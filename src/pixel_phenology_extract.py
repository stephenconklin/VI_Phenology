#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# pixel_phenology_extract.py
# Per-pixel phenological metric extraction from CF-1.8 datacubes.
#
# Reads one or more per-pixel datacubes produced by the netcdf_datacube pipeline
# and outputs, per (VI, region), a CF-1.8 NetCDF containing 19 phenological
# metric bands on the same x/y grid as the input, plus a summary CSV with
# spatial statistics (mean, std, p05, p50, p95, n_valid_pixels) per metric.
#
# Processing model
# ────────────────
# 1. For each input datacube (one VI × one region):
#    a. Open with xarray (lazy); apply optional date-range filter on time axis.
#    b. Warn if uncompressed array would exceed MEM_WARN_GB.
#    c. Load the full (time, y, x) array into a numpy float32 array.
#    d. Build the Whittaker D-matrix once for the full time axis (all pixels share
#       the same daily grid after time-axis standardisation).
#    e. Dispatch y-row chunks to a ThreadPoolExecutor (threads share the in-memory
#       array; scipy sparse solver releases the GIL for true parallelism).
#    f. Each thread processes its chunk pixel-by-pixel:
#         - Map observations onto the daily grid (NaN → weight 0).
#         - Solve (W + λ D^T D) z = W y for the smooth daily series.
#         - Compute 19 per-year metrics, aggregate mean/std across years.
#    g. Assemble 19-band output array; write CF-1.8 NetCDF with compression.
#    h. Write summary CSV.
#
# Metric bands (19)
# ─────────────────
#   peak_ndvi_mean, peak_ndvi_std
#   peak_doy_mean,  peak_doy_std
#   integrated_ndvi_mean, integrated_ndvi_std
#   greenup_rate_mean, greenup_rate_std
#   floor_ndvi_mean, ceiling_ndvi_mean          ← derived from curve min/max
#   season_length_mean, season_length_std
#   cv                                          ← std/mean of raw obs (whole-series)
#   interannual_peak_range, interannual_peak_std
#   n_peaks_mean
#   peak_separation_mean
#   relative_peak_amplitude_mean
#   valley_depth_mean
#
# Authors: Stephen Conklin <stephenconklin@gmail.com>
#          G. Burch Fisher, PhD — conceptual guidance and original code
#                                 adapted for per-pixel metric extraction
# License: MIT

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

import netCDF4 as nc4
import numpy as np
import pandas as pd
import xarray as xr
from scipy.signal import find_peaks as _find_peaks
from scipy.sparse import diags as sp_diags
from scipy.sparse.linalg import spsolve

from io_utils import setup_log_file

logger = logging.getLogger(__name__)

# Warn when the uncompressed input array would exceed this threshold.
_MEM_WARN_GB = 8.0

# Number of y-rows per thread chunk.
_Y_CHUNK_ROWS = 50

# Ordered list of the 18 output metric names.
METRIC_NAMES = [
    "peak_ndvi_mean",
    "peak_ndvi_std",
    "peak_doy_mean",
    "peak_doy_std",
    "integrated_ndvi_mean",
    "integrated_ndvi_std",
    "greenup_rate_mean",
    "greenup_rate_std",
    "floor_ndvi_mean",
    "ceiling_ndvi_mean",
    "season_length_mean",
    "season_length_std",
    "cv",
    "interannual_peak_range",
    "interannual_peak_std",
    "n_peaks_mean",
    "peak_separation_mean",
    "relative_peak_amplitude_mean",
    "valley_depth_mean",
]

_MIN_AMPLITUDE = 1e-6


# ---------------------------------------------------------------------------
# Whittaker smoother
# ---------------------------------------------------------------------------

def _build_whittaker_system(n: int, lam: float):
    """Pre-build the λ D^T D penalty term for a daily grid of length n.

    Returns the sparse (n × n) matrix lam_DTD so each pixel only needs to
    add the diagonal weight matrix W and solve (W + lam_DTD) z = W y.
    """
    e = np.ones(n)
    D = sp_diags(
        [e[:-2], -2 * e[:-1], e],
        offsets=[0, 1, 2],
        shape=(n - 2, n),
        format='csc',
    )
    return lam * D.T @ D


def _whittaker_smooth_pixel(
    daily_y: np.ndarray,
    daily_w: np.ndarray,
    lam_DTD,
) -> np.ndarray:
    """Solve the Whittaker system for one pixel.

    Args:
        daily_y:  float64 array of length n (0 where no observation).
        daily_w:  float64 weight array of length n (1 = observed, 0 = gap).
        lam_DTD:  precomputed λ D^T D sparse matrix.

    Returns:
        Smoothed float64 array of length n, or daily_y unchanged on failure.
    """
    W = sp_diags(daily_w, format='csc')
    A = W + lam_DTD
    b = daily_w * daily_y
    try:
        return spsolve(A, b)
    except Exception:
        return daily_y.copy()


# ---------------------------------------------------------------------------
# Per-pixel metric extraction
# ---------------------------------------------------------------------------

def _extract_pixel_metrics(
    pixel_ts: np.ndarray,
    times: pd.DatetimeIndex,
    lam_DTD,
    config: dict,
) -> dict:
    """Compute all 18 phenological metrics for one pixel time series.

    Args:
        pixel_ts: float32/float64 array of shape (n_time,); NaN = masked.
        times:    DatetimeIndex aligned with pixel_ts.
        lam_DTD:  precomputed Whittaker penalty matrix (shared across pixels).
        config:   dict with keys: min_valid_obs, min_valid_obs_per_year, vi_min, vi_max,
                  peak_prominence, peak_min_distance_days, season_threshold.

    Returns:
        dict of metric_name → float scalar (np.nan if not computable).
    """
    nan_result = {k: np.nan for k in METRIC_NAMES}

    # Validity check.
    vi_min = config["vi_min"]
    vi_max = config["vi_max"]
    valid_mask = (
        ~np.isnan(pixel_ts)
        & (pixel_ts >= vi_min)
        & (pixel_ts <= vi_max)
    )
    if valid_mask.sum() < config["min_valid_obs"]:
        return nan_result

    # Coefficient of variation from raw observations (whole-series).
    raw_vals = pixel_ts[valid_mask].astype(np.float64)
    mean_raw = float(np.mean(raw_vals))
    cv = float(np.std(raw_vals) / mean_raw) if mean_raw > 0 else np.nan

    # Map observations onto the daily grid spanning the full time axis.
    start_date = times[0]
    end_date = times[-1]
    n_days = (end_date - start_date).days + 1
    all_dates = pd.date_range(start=start_date, end=end_date, freq='D')

    daily_y = np.zeros(n_days, dtype=np.float64)
    daily_w = np.zeros(n_days, dtype=np.float64)

    for t_idx, ts in enumerate(times):
        if valid_mask[t_idx]:
            d = (ts - start_date).days
            if daily_w[d] > 0:
                # Same-day duplicate — take mean.
                daily_y[d] = (daily_y[d] + float(pixel_ts[t_idx])) / 2.0
            else:
                daily_y[d] = float(pixel_ts[t_idx])
                daily_w[d] = 1.0

    # Whittaker smooth.
    if n_days < 3 or lam_DTD is None:
        # Fallback: linear interpolation between observations.
        smoothed = daily_y.copy()
    else:
        smoothed = _whittaker_smooth_pixel(daily_y, daily_w, lam_DTD)
        smoothed = np.clip(smoothed, vi_min, vi_max)

    # Build a date-indexed DataFrame for the smoothed series.
    smooth_df = pd.DataFrame({
        "date": all_dates,
        "ndvi": smoothed.astype(np.float32),
    }).set_index("date")

    years = sorted(smooth_df.index.year.unique())

    # Per-year accumulators.
    annual = {
        "peak_ndvi":   [],
        "peak_doy":    [],
        "integrated":  [],
        "greenup":     [],
        "floor":       [],
        "ceiling":     [],
        "season_len":  [],
        "n_peaks":     [],
        "peak_sep":    [],
        "rel_amp":     [],
        "valley":      [],
    }

    peak_prominence = config["peak_prominence"]
    peak_min_dist = config["peak_min_distance_days"]
    season_thr = config["season_threshold"]
    min_obs_per_year = config["min_valid_obs_per_year"]

    for yr in years:
        yr_s = smooth_df[smooth_df.index.year == yr]["ndvi"]
        if len(yr_s) < 30:
            continue

        # Count valid observations in this annual window; skip if too sparse.
        yr_obs = int(daily_w[all_dates.year == yr].sum())
        if yr_obs < min_obs_per_year:
            continue

        y = yr_s.values.astype(np.float64)
        doys = yr_s.index.dayofyear.values

        # Peak.
        peak_idx = int(np.argmax(y))
        annual["peak_ndvi"].append(float(y[peak_idx]))
        annual["peak_doy"].append(int(doys[peak_idx]))

        # Integrated NDVI (trapezoidal, using integer day indices as x-axis).
        annual["integrated"].append(float(np.trapezoid(y)))

        # Floor and ceiling from the curve (no DOY windows).
        floor_val = float(np.nanmin(y))
        ceil_val = float(np.nanmax(y))
        annual["floor"].append(floor_val)
        annual["ceiling"].append(ceil_val)

        # Green-up rate: slope from curve minimum to curve maximum.
        floor_idx = int(np.argmin(y))
        if floor_idx < peak_idx:
            delta_ndvi = float(y[peak_idx] - y[floor_idx])
            delta_days = int(doys[peak_idx] - doys[floor_idx])
            rate = delta_ndvi / delta_days if delta_days > 0 else np.nan
            annual["greenup"].append(rate)

        # Season length: days above floor + season_thr * amplitude.
        amplitude = ceil_val - floor_val
        if amplitude >= _MIN_AMPLITUDE:
            threshold = floor_val + season_thr * amplitude
            above_dates = yr_s.index[y >= threshold]
            if len(above_dates) >= 2:
                annual["season_len"].append(
                    float((above_dates[-1] - above_dates[0]).days)
                )

        # Bimodality.
        peaks, _ = _find_peaks(y, prominence=peak_prominence, distance=peak_min_dist)
        n_p = int(len(peaks))
        annual["n_peaks"].append(n_p)

        if n_p >= 2:
            sorted_peaks = peaks[np.argsort(y[peaks])[::-1]]
            p1, p2 = sorted_peaks[0], sorted_peaks[1]
            sep = float(abs(doys[p1] - doys[p2]))
            annual["peak_sep"].append(sep)
            h1, h2 = float(y[p1]), float(y[p2])
            if max(h1, h2) > 0:
                annual["rel_amp"].append(float(min(h1, h2) / max(h1, h2)))
            lo, hi = min(p1, p2), max(p1, p2)
            valley = float(np.nanmin(y[lo : hi + 1]))
            mean_pk = (h1 + h2) / 2.0
            if mean_pk > 0:
                annual["valley"].append(float((mean_pk - valley) / mean_pk))
        else:
            annual["peak_sep"].append(np.nan)
            annual["rel_amp"].append(np.nan)
            annual["valley"].append(np.nan)

    def _safe_mean(lst):
        a = [v for v in lst if not np.isnan(v)]
        return float(np.mean(a)) if a else np.nan

    def _safe_std(lst):
        a = [v for v in lst if not np.isnan(v)]
        return float(np.std(a)) if a else np.nan

    peak_list = annual["peak_ndvi"]
    interannual_range = (
        float(np.nanmax(peak_list) - np.nanmin(peak_list)) if peak_list else np.nan
    )

    return {
        "peak_ndvi_mean":               _safe_mean(annual["peak_ndvi"]),
        "peak_ndvi_std":                _safe_std(annual["peak_ndvi"]),
        "peak_doy_mean":                _safe_mean(annual["peak_doy"]),
        "peak_doy_std":                 _safe_std(annual["peak_doy"]),
        "integrated_ndvi_mean":         _safe_mean(annual["integrated"]),
        "integrated_ndvi_std":          _safe_std(annual["integrated"]),
        "greenup_rate_mean":            _safe_mean(annual["greenup"]),
        "greenup_rate_std":             _safe_std(annual["greenup"]),
        "floor_ndvi_mean":              _safe_mean(annual["floor"]),
        "ceiling_ndvi_mean":            _safe_mean(annual["ceiling"]),
        "season_length_mean":           _safe_mean(annual["season_len"]),
        "season_length_std":            _safe_std(annual["season_len"]),
        "cv":                           cv,
        "interannual_peak_range":       interannual_range,
        "interannual_peak_std":         _safe_std(annual["peak_ndvi"]),
        "n_peaks_mean":                 _safe_mean(annual["n_peaks"]),
        "peak_separation_mean":         _safe_mean(annual["peak_sep"]),
        "relative_peak_amplitude_mean": _safe_mean(annual["rel_amp"]),
        "valley_depth_mean":            _safe_mean(annual["valley"]),
    }


# ---------------------------------------------------------------------------
# Thread worker
# ---------------------------------------------------------------------------

def _process_y_chunk(
    ndvi_chunk: np.ndarray,
    times: pd.DatetimeIndex,
    lam_DTD,
    config: dict,
) -> np.ndarray:
    """Process all pixels in a y-row chunk.

    Args:
        ndvi_chunk: float32 array of shape (n_time, n_y_chunk, n_x).
        times:      DatetimeIndex of length n_time.
        lam_DTD:    precomputed Whittaker penalty matrix.
        config:     per-pixel config dict.

    Returns:
        float32 array of shape (n_metrics, n_y_chunk, n_x).
    """
    n_metrics = len(METRIC_NAMES)
    n_time, n_y, n_x = ndvi_chunk.shape
    out = np.full((n_metrics, n_y, n_x), np.nan, dtype=np.float32)

    for iy in range(n_y):
        for ix in range(n_x):
            pixel_ts = ndvi_chunk[:, iy, ix].astype(np.float64)
            if np.all(np.isnan(pixel_ts)):
                continue
            metrics = _extract_pixel_metrics(pixel_ts, times, lam_DTD, config)
            for im, name in enumerate(METRIC_NAMES):
                out[im, iy, ix] = metrics.get(name, np.nan)

    return out


# ---------------------------------------------------------------------------
# Per-datacube pipeline
# ---------------------------------------------------------------------------

def process_datacube(
    datacube_path: Path,
    output_dir: Path,
    config: dict,
    n_workers: int,
    start_date: str | None,
    end_date: str | None,
) -> None:
    """Extract 18 per-pixel metrics from one datacube and write outputs.

    Args:
        datacube_path: Path to a *_datacube.nc file.
        output_dir:    Directory to write outputs (created if needed).
        config:        Processing config dict (vi_min, vi_max, min_valid_obs,
                       smooth_lambda, peak_prominence, peak_min_distance_days,
                       season_threshold).
        n_workers:     Number of threads.
        start_date:    Optional YYYY-MM-DD lower bound (inclusive).
        end_date:      Optional YYYY-MM-DD upper bound (inclusive).
    """
    # ── Parse VI and region label from filename ───────────────────────────────
    stem = datacube_path.stem  # e.g. "NDVI_MyRegion_datacube" after stripping .nc
    if not stem.endswith("_datacube"):
        logger.warning(
            "Unexpected datacube filename '%s' — expected *_datacube.nc; "
            "proceeding anyway.", datacube_path.name
        )
    # Strip trailing _datacube if present.
    base = stem[: -len("_datacube")] if stem.endswith("_datacube") else stem

    # VI is the first underscore-separated token; region_label is the rest.
    parts = base.split("_", 1)
    vi_name = parts[0].upper()
    region_label = parts[1] if len(parts) > 1 else "unknown_region"

    logger.info("Processing datacube: VI=%s  region=%s  path=%s",
                vi_name, region_label, datacube_path)

    # ── Open and date-filter ──────────────────────────────────────────────────
    ds = xr.open_dataset(datacube_path, chunks={})

    # Detect VI variable (first non-coordinate, non-spatial_ref data variable).
    vi_var = None
    for vname in ds.data_vars:
        if vname not in ("spatial_ref",):
            vi_var = vname
            break
    if vi_var is None:
        logger.error("No VI variable found in %s — skipping.", datacube_path)
        return

    da = ds[vi_var]

    # Apply date range filter before loading into memory.
    if start_date or end_date:
        time_sel = {}
        if start_date:
            time_sel["time"] = slice(start_date, None)
        if end_date:
            existing = time_sel.get("time", slice(None, None))
            time_sel["time"] = slice(existing.start, end_date)
        da = da.sel(**time_sel)

    times_raw = da.time.values
    times = pd.DatetimeIndex(pd.to_datetime(times_raw))

    n_time = len(times)
    n_y = da.sizes.get("y", da.sizes.get("lat", 0))
    n_x = da.sizes.get("x", da.sizes.get("lon", 0))
    y_coords = da.coords["y"].values if "y" in da.coords else da.coords["lat"].values
    x_coords = da.coords["x"].values if "x" in da.coords else da.coords["lon"].values

    # Clamp valid range from config.
    vi_key = vi_name.lower()
    vi_min = config.get(f"vi_min_{vi_key}", config.get("vi_min", -1.0))
    vi_max = config.get(f"vi_max_{vi_key}", config.get("vi_max", 2.0))

    # Memory check.
    n_bytes = n_time * n_y * n_x * 4
    if n_bytes > _MEM_WARN_GB * 1e9:
        logger.warning(
            "Datacube is large (%.1f GB uncompressed, %d×%d spatial, %d time steps). "
            "Consider using --start-date/--end-date to reduce temporal extent.",
            n_bytes / 1e9, n_y, n_x, n_time,
        )

    logger.info(
        "Loading array: time=%d  y=%d  x=%d  (%.2f GB uncompressed)",
        n_time, n_y, n_x, n_bytes / 1e9,
    )
    ndvi_np = da.values.astype(np.float32)  # shape: (time, y, x)

    # Build Whittaker penalty matrix once for the daily grid (not n_time).
    # n_days = calendar days from first to last acquisition; always >= n_time since
    # HLS does not observe every day. W in _whittaker_smooth_pixel has size n_days,
    # so lam_DTD must match that dimension.
    n_days = (times[-1] - times[0]).days + 1
    lam = config["smooth_lambda"]
    if n_days >= 3:
        try:
            lam_DTD = _build_whittaker_system(n_days, lam)
            logger.debug(
                "Built Whittaker D^T D matrix: n_days=%d, n_time=%d, λ=%.1f",
                n_days, n_time, lam,
            )
        except Exception as exc:
            logger.warning("Could not build Whittaker matrix (%s); using linear fill.", exc)
            lam_DTD = None
    else:
        lam_DTD = None

    per_pixel_cfg = {
        "vi_min":                   vi_min,
        "vi_max":                   vi_max,
        "min_valid_obs":            config["min_valid_obs"],
        "min_valid_obs_per_year":   config["min_valid_obs_per_year"],
        "peak_prominence":          config["peak_prominence"],
        "peak_min_distance_days":   config["peak_min_distance_days"],
        "season_threshold":         config["season_threshold"],
    }

    # ── Parallel pixel processing ─────────────────────────────────────────────
    n_metrics = len(METRIC_NAMES)
    out_array = np.full((n_metrics, n_y, n_x), np.nan, dtype=np.float32)

    y_chunks = list(range(0, n_y, _Y_CHUNK_ROWS))
    n_chunks = len(y_chunks)
    logger.info("Dispatching %d y-row chunks to %d threads ...", n_chunks, n_workers)

    futures = {}
    with logging_redirect_tqdm():
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            for chunk_start in y_chunks:
                chunk_end = min(chunk_start + _Y_CHUNK_ROWS, n_y)
                ndvi_chunk = ndvi_np[:, chunk_start:chunk_end, :]
                future = executor.submit(
                    _process_y_chunk, ndvi_chunk, times, lam_DTD, per_pixel_cfg
                )
                futures[future] = (chunk_start, chunk_end)

            with tqdm(total=n_chunks, desc=f"{vi_name}/{region_label}",
                      unit="chunk", dynamic_ncols=True) as pbar:
                for future in as_completed(futures):
                    chunk_start, chunk_end = futures[future]
                    result = future.result()
                    out_array[:, chunk_start:chunk_end, :] = result
                    pbar.update(1)

    # ── Write output NetCDF ───────────────────────────────────────────────────
    region_out_dir = output_dir / region_label
    region_out_dir.mkdir(parents=True, exist_ok=True)
    out_nc_path = region_out_dir / f"{vi_name}_{region_label}_pixel_metrics.nc"

    logger.info("Writing pixel metrics NetCDF: %s", out_nc_path)
    with nc4.Dataset(str(out_nc_path), "w", format="NETCDF4") as ncout:
        # Dimensions.
        y_dim = "y" if "y" in da.coords else "lat"
        x_dim = "x" if "x" in da.coords else "lon"
        ncout.createDimension(y_dim, n_y)
        ncout.createDimension(x_dim, n_x)

        # Coordinate variables.
        y_var = ncout.createVariable(y_dim, "f8", (y_dim,))
        y_var[:] = y_coords
        x_var = ncout.createVariable(x_dim, "f8", (x_dim,))
        x_var[:] = x_coords

        # Copy CRS if present.
        if "spatial_ref" in ds.data_vars:
            sr = ncout.createVariable("spatial_ref", "i4")
            sr.setncatts(
                {k: ds["spatial_ref"].attrs[k] for k in ds["spatial_ref"].attrs}
            )

        # Metric bands.
        for im, name in enumerate(METRIC_NAMES):
            v = ncout.createVariable(
                name, "f4", (y_dim, x_dim),
                zlib=True, complevel=4, fill_value=np.float32(np.nan),
            )
            v[:] = out_array[im]
            v.long_name = name
            v.grid_mapping = "spatial_ref"

        # Global attributes.
        ncout.Conventions = "CF-1.8"
        ncout.history = (
            f"Created {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')} "
            "by pixel_phenology_extract.py"
        )
        ncout.region = region_label
        ncout.vi = vi_name
        ncout.source_datacube = str(datacube_path)
        ncout.whittaker_lambda = float(lam)
        ncout.peak_prominence = float(config["peak_prominence"])
        ncout.peak_min_distance_days = int(config["peak_min_distance_days"])
        ncout.season_threshold = float(config["season_threshold"])
        ncout.min_valid_obs = int(config["min_valid_obs"])
        if start_date:
            ncout.start_date = start_date
        if end_date:
            ncout.end_date = end_date

    logger.info("Saved → %s", out_nc_path)

    # ── Write summary CSV ─────────────────────────────────────────────────────
    rows = []
    for im, name in enumerate(METRIC_NAMES):
        vals = out_array[im].ravel()
        vals = vals[~np.isnan(vals)]
        rows.append({
            "metric":         name,
            "mean":           float(np.mean(vals))         if len(vals) > 0 else np.nan,
            "std":            float(np.std(vals))          if len(vals) > 0 else np.nan,
            "p05":            float(np.percentile(vals, 5))  if len(vals) > 0 else np.nan,
            "p50":            float(np.percentile(vals, 50)) if len(vals) > 0 else np.nan,
            "p95":            float(np.percentile(vals, 95)) if len(vals) > 0 else np.nan,
            "n_valid_pixels": int(len(vals)),
        })
    csv_path = region_out_dir / f"{vi_name}_{region_label}_pixel_metrics_summary.csv"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    logger.info("Saved summary CSV → %s", csv_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_valid_range(s: str, vi: str) -> tuple:
    parts = s.split(",")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--valid-range-{vi.lower()} must be MIN,MAX, got: {s!r}"
        )
    return float(parts[0]), float(parts[1])


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Pixel Phenology: extract 18 per-pixel phenological metrics from "
            "CF-1.8 datacubes produced by the netcdf_datacube pipeline."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # --- Input ---
    parser.add_argument(
        "--input-datacubes", nargs="+", required=True, metavar="PATH",
        help=(
            "Path(s) to *_datacube.nc files produced by the netcdf_datacube pipeline. "
            "VI and region label are inferred from each filename "
            "({VI}_{region_label}_datacube.nc)."
        ),
    )

    # --- Output ---
    parser.add_argument(
        "--output-dir", required=True,
        help="Root output directory. Per-region subdirectories are created automatically.",
    )

    # --- Valid ranges ---
    parser.add_argument("--valid-range-ndvi", default="-0.1,1.0", metavar="MIN,MAX",
                        help="Valid range for NDVI pixels")
    parser.add_argument("--valid-range-evi2", default="-1,2", metavar="MIN,MAX",
                        help="Valid range for EVI2 pixels")
    parser.add_argument("--valid-range-nirv", default="-0.5,1", metavar="MIN,MAX",
                        help="Valid range for NIRv pixels")

    # --- Whittaker smoother ---
    parser.add_argument(
        "--smooth-lambda", type=float, default=100.0, metavar="LAMBDA",
        help=(
            "Whittaker smoothing strength λ (default: 100). "
            "Higher = smoother curve. Typical range: 10 (tight) – 1000 (very smooth)."
        ),
    )

    # --- Pixel validity ---
    parser.add_argument(
        "--min-valid-obs", type=int, default=20, metavar="N",
        help=(
            "Minimum valid observations over the full record required to compute metrics "
            "for a pixel (default: 20). Pixels with fewer observations are set to NaN."
        ),
    )
    parser.add_argument(
        "--min-valid-obs-per-year", type=int, default=5, metavar="N",
        help=(
            "Minimum valid observations within an annual window for that year's metrics "
            "to be included in the pixel aggregate (default: 5). Years with fewer "
            "observations are skipped rather than contributing unreliable values."
        ),
    )

    # --- Bimodality ---
    parser.add_argument(
        "--peak-prominence", type=float, default=0.05, metavar="NDVI",
        help=(
            "Minimum NDVI prominence for a peak to count as bimodal (default: 0.05). "
            "Floor and ceiling NDVI are derived directly from the curve (no DOY windows)."
        ),
    )
    parser.add_argument(
        "--peak-min-distance", type=int, default=45, metavar="DAYS",
        help="Minimum separation (days) between detected peaks (default: 45).",
    )

    # --- Season length ---
    parser.add_argument(
        "--season-threshold", type=float, default=0.20, metavar="FRACTION",
        help=(
            "Amplitude fraction above floor for season-length calculation (default: 0.20). "
            "Season length = days above floor + threshold × (ceiling − floor)."
        ),
    )

    # --- Date range ---
    parser.add_argument(
        "--start-date", default=None, metavar="YYYY-MM-DD",
        help="Only use time steps on or after this date (inclusive).",
    )
    parser.add_argument(
        "--end-date", default=None, metavar="YYYY-MM-DD",
        help="Only use time steps on or before this date (inclusive).",
    )

    # --- Parallelization ---
    parser.add_argument(
        "--workers", type=int, default=8, metavar="N",
        help=(
            "Number of parallel threads for pixel processing (default: 8). "
            "Threads share the in-memory array; scipy sparse solver releases the GIL."
        ),
    )

    # --- Logging ---
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    setup_log_file(output_dir, "pixel_phenology", args.log_level)

    # Parse valid ranges.
    ndvi_min, ndvi_max = _parse_valid_range(args.valid_range_ndvi, "NDVI")
    evi2_min, evi2_max = _parse_valid_range(args.valid_range_evi2, "EVI2")
    nirv_min, nirv_max = _parse_valid_range(args.valid_range_nirv, "NIRv")

    config = {
        "smooth_lambda":          args.smooth_lambda,
        "min_valid_obs":          args.min_valid_obs,
        "min_valid_obs_per_year": args.min_valid_obs_per_year,
        "peak_prominence":        args.peak_prominence,
        "peak_min_distance_days": args.peak_min_distance,
        "season_threshold":       args.season_threshold,
        # Default fallback range (overridden per-datacube by VI name).
        "vi_min": -1.0,
        "vi_max":  2.0,
        # Per-VI ranges.
        "vi_min_ndvi": ndvi_min, "vi_max_ndvi": ndvi_max,
        "vi_min_evi2": evi2_min, "vi_max_evi2": evi2_max,
        "vi_min_nirv": nirv_min, "vi_max_nirv": nirv_max,
    }

    datacube_paths = [Path(p) for p in args.input_datacubes]
    logger.info("Pixel Phenology pipeline starting")
    logger.info("  Datacubes     : %d file(s)", len(datacube_paths))
    logger.info("  Output dir    : %s", output_dir)
    logger.info("  Workers       : %d threads", args.workers)
    logger.info("  Smooth λ      : %.1f", args.smooth_lambda)
    if args.start_date or args.end_date:
        logger.info(
            "  Date range    : %s → %s",
            args.start_date or "start of record",
            args.end_date or "end of record",
        )

    for dc_path in datacube_paths:
        if not dc_path.exists():
            logger.error("Datacube not found: %s — skipping.", dc_path)
            continue
        process_datacube(
            datacube_path=dc_path,
            output_dir=output_dir,
            config=config,
            n_workers=args.workers,
            start_date=args.start_date,
            end_date=args.end_date,
        )

    logger.info("Done. All outputs written to: %s", output_dir)


if __name__ == "__main__":
    main()
