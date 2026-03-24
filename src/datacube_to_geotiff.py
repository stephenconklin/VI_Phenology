#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# datacube_to_geotiff.py
# Per-pixel temporal statistics from CF-1.8 VI datacubes, delivered as GeoTiffs.
#
# Reads one or more per-pixel datacubes produced by the netcdf_datacube pipeline
# and writes three multi-band GeoTiffs per (VI, region):
#
#   {VI}_{region_label}_per_year.tif
#       N_years × 3 bands.  For each calendar year: median, 5th percentile,
#       and 95th percentile of all valid observations in that year.
#       Band names: year2020_median, year2020_p05, year2020_p95, ...
#
#   {VI}_{region_label}_per_month.tif
#       12 × 3 = 36 bands.  "Per-year then average years" method: for each
#       (year, month) compute per-year percentiles, then average across years.
#       Band names: month01_median, month01_p05, month01_p95, ... month12_p95
#
#   {VI}_{region_label}_per_doy.tif
#       365 × 3 = 1095 bands.  For each DOY (1–365), pool all valid
#       observations across all years and compute percentiles.  DOYs with no
#       observations receive the CF/NetCDF4 float32 fill value (~80% of bands
#       at HLS ~5-day cadence).  Use --skip-per-doy for very large datacubes.
#       Band names: doy001_median, doy001_p05, doy001_p95, ... doy365_p95
#
# NoData value: 9.96920996838687e+36 (NC_FILL_FLOAT — CF/NetCDF4 float32)
# Compression : LZW + predictor=2 (horizontal differencing)  |  Tiling: 256×256  |  BigTIFF: IF_SAFER
#
# Authors: Stephen Conklin <stephenconklin@gmail.com>
# License: MIT

import argparse
import logging
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
import rioxarray  # noqa: F401 — activates .rio accessor on xarray objects
import xarray as xr
from rasterio.crs import CRS

from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from io_utils import read_netcdf_crs, setup_log_file

logger = logging.getLogger(__name__)

# All-NaN slices are expected (pixels outside the polygon, or DOYs/months with
# no observations). Suppress the RuntimeWarning at module level so it stays
# silent even when multiple datacubes are processed in parallel threads.
warnings.filterwarnings(
    "ignore",
    message="All-NaN slice encountered",
    category=RuntimeWarning,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# CF/NetCDF4 float32 fill value (NC_FILL_FLOAT).  Used as GeoTiff nodata so
# that all pipeline outputs share a consistent sentinel value.
_FILL_F4 = np.float32(9.96920996838687e+36)

# Warn when the uncompressed input array would exceed this size.
_MEM_WARN_GB = 8.0

# Warn when the per-DOY output GeoTiff would exceed this size.
_DOY_WARN_GB = 4.0


# ---------------------------------------------------------------------------
# Rasterio profile helper
# ---------------------------------------------------------------------------

def _build_rasterio_profile(
    n_bands: int,
    crs_wkt: str,
    y_coords: np.ndarray,
    x_coords: np.ndarray,
) -> dict:
    """Build a rasterio write profile from datacube coordinate arrays.

    Derives the affine transform from the y/x coordinate arrays, which are
    assumed to be a regular 30-m projected grid (UTM).

    Args:
        n_bands:   Total number of bands in the output file.
        crs_wkt:   WKT string for the projected CRS.
        y_coords:  1-D float64 northing coordinate array (length n_y).
        x_coords:  1-D float64 easting coordinate array (length n_x).

    Returns:
        rasterio profile dict suitable for ``rasterio.open(..., "w", **profile)``.
    """
    n_y = len(y_coords)
    n_x = len(x_coords)

    # Cell size from first spacing; fall back to ±30 m if only one pixel.
    cell_x = float(x_coords[1] - x_coords[0]) if n_x > 1 else 30.0
    cell_y = float(y_coords[1] - y_coords[0]) if n_y > 1 else -30.0

    # Upper-left corner of the upper-left pixel.
    west  = float(x_coords[0]) - cell_x / 2.0
    north = float(y_coords[0]) - cell_y / 2.0  # y_coords[0] is northernmost

    transform = rasterio.transform.from_origin(
        west, north, abs(cell_x), abs(cell_y)
    )

    return {
        "driver":     "GTiff",
        "dtype":      "float32",
        "nodata":     float(_FILL_F4),
        "width":      n_x,
        "height":     n_y,
        "count":      n_bands,
        "crs":        CRS.from_wkt(crs_wkt),
        "transform":  transform,
        "compress":   "lzw",
        "predictor":  2,
        "tiled":      True,
        "blockxsize": 256,
        "blockysize": 256,
        "bigtiff":    "IF_SAFER",
    }


# ---------------------------------------------------------------------------
# Streaming GeoTiff writer
# ---------------------------------------------------------------------------

def _write_geotiff(
    band_iter,
    n_bands: int,
    crs_wkt: str,
    y_coords: np.ndarray,
    x_coords: np.ndarray,
    out_path: Path,
    desc: str = "",
) -> None:
    """Write bands from an iterator to a GeoTiff file.

    Writes one band at a time — peak additional memory is a single
    (n_y, n_x) float32 array regardless of total band count.  This keeps the
    per-DOY product (1095 bands) from requiring a 17 GB output array in RAM.

    Args:
        band_iter: Iterator yielding (float32 ndarray (n_y, n_x), str) tuples
                   of (band_data, band_description).
        n_bands:   Total number of bands (must be known before opening the file).
        crs_wkt:   CRS WKT string read from the source datacube.
        y_coords:  1-D float64 northing array (length n_y).
        x_coords:  1-D float64 easting array (length n_x).
        out_path:  Destination path for the GeoTiff file.
        desc:      Progress bar label (e.g. "NDVI/G5_25 per_doy").
    """
    profile = _build_rasterio_profile(n_bands, crs_wkt, y_coords, x_coords)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with logging_redirect_tqdm():
        with rasterio.open(str(out_path), "w", **profile) as dst:
            with tqdm(total=n_bands, desc=desc, unit="band",
                      dynamic_ncols=True) as pbar:
                for band_idx, (band_data, band_desc) in enumerate(band_iter, start=1):
                    dst.write(band_data, band_idx)
                    dst.set_band_description(band_idx, band_desc)
                    pbar.update(1)

    logger.info("Saved → %s  (%d bands)", out_path, n_bands)


# ---------------------------------------------------------------------------
# Band iterators — one per temporal aggregation
# ---------------------------------------------------------------------------

def _iter_per_year_bands(da: xr.DataArray, times: pd.DatetimeIndex,
                         vi_min: float, vi_max: float):
    """Yield (band_data, band_desc) for per-year statistics.

    Loads one calendar year's time steps at a time — peak memory is
    (n_yr_obs × n_y × n_x × 4 B), not the full datacube.

    Args:
        da:     Lazy xarray DataArray with dimensions (time, y, x).
        times:  DatetimeIndex of length n_time.
        vi_min: Lower bound of valid VI range; values below → NaN.
        vi_max: Upper bound of valid VI range; values above → NaN.

    Yields:
        Tuples of (float32 ndarray (n_y, n_x), str band_description).
        Total yield: n_years × 3 tuples.
    """
    year_vals    = times.year.values
    unique_years = np.unique(year_vals)

    for yr in unique_years:
        idx = np.where(year_vals == yr)[0]
        sl  = da.isel(time=idx).values.astype(np.float32)  # load this year only
        sl[(sl < vi_min) | (sl > vi_max)] = np.nan
        all_nan_mask = np.all(np.isnan(sl), axis=0)

        for pct, suffix in ((50, "median"), (5, "p05"), (95, "p95")):
            band = np.nanpercentile(sl, pct, axis=0).astype(np.float32)
            band[all_nan_mask] = _FILL_F4
            yield band, f"year{yr}_{suffix}"


def _iter_per_month_bands(da: xr.DataArray, times: pd.DatetimeIndex,
                          vi_min: float, vi_max: float):
    """Yield (band_data, band_desc) for per-month statistics.

    Uses a "per-year then average years" approach:
      1. For each (year, month) with observations: compute per-year percentiles.
      2. For each month: average the per-year percentiles across contributing years.
    Loads one (year, month) slice at a time — peak memory is tiny.
    Pixels with no valid observations in a given month receive _FILL_F4.

    Args:
        da:     Lazy xarray DataArray with dimensions (time, y, x).
        times:  DatetimeIndex of length n_time.
        vi_min: Lower bound of valid VI range; values below → NaN.
        vi_max: Upper bound of valid VI range; values above → NaN.

    Yields:
        Tuples of (float32 ndarray (n_y, n_x), str band_description).
        Total yield: 36 tuples (12 months × 3 statistics).
    """
    n_y = da.sizes.get("y", da.sizes.get("lat", 0))
    n_x = da.sizes.get("x", da.sizes.get("lon", 0))
    year_vals  = times.year.values
    month_vals = times.month.values

    for month in range(1, 13):
        month_mask = (month_vals == month)
        unique_years_in_month = np.unique(year_vals[month_mask])

        per_yr_50, per_yr_05, per_yr_95 = [], [], []

        for yr in unique_years_in_month:
            idx = np.where(month_mask & (year_vals == yr))[0]
            if idx.size == 0:
                continue
            sl = da.isel(time=idx).values.astype(np.float32)  # load this month-year only
            sl[(sl < vi_min) | (sl > vi_max)] = np.nan
            per_yr_50.append(np.nanpercentile(sl, 50, axis=0))
            per_yr_05.append(np.nanpercentile(sl,  5, axis=0))
            per_yr_95.append(np.nanpercentile(sl, 95, axis=0))

        for year_stats, suffix in (
            (per_yr_50, "median"), (per_yr_05, "p05"), (per_yr_95, "p95")
        ):
            if year_stats:
                band = np.nanmean(
                    np.stack(year_stats, axis=0), axis=0
                ).astype(np.float32)
                band[np.isnan(band)] = _FILL_F4
            else:
                band = np.full((n_y, n_x), _FILL_F4, dtype=np.float32)
            yield band, f"month{month:02d}_{suffix}"


def _iter_per_doy_bands(da: xr.DataArray, times: pd.DatetimeIndex,
                        vi_min: float, vi_max: float):
    """Yield (band_data, band_desc) for per-DOY statistics.

    Pools all valid observations across all years at each DOY 1–365.
    DOY 366 (leap day) is folded into DOY 365.  DOYs with no observations
    at all receive _FILL_F4 for all pixels (~80% of bands at HLS ~5-day cadence).

    Loads only the ~5 time steps for each DOY — peak memory is
    (n_doy_obs × n_y × n_x × 4 B), not the full datacube.

    Args:
        da:     Lazy xarray DataArray with dimensions (time, y, x).
        times:  DatetimeIndex of length n_time.
        vi_min: Lower bound of valid VI range; values below → NaN.
        vi_max: Upper bound of valid VI range; values above → NaN.

    Yields:
        Tuples of (float32 ndarray (n_y, n_x), str band_description).
        Total yield: 1095 tuples (365 DOYs × 3 statistics).
    """
    n_y = da.sizes.get("y", da.sizes.get("lat", 0))
    n_x = da.sizes.get("x", da.sizes.get("lon", 0))
    doy_vals    = np.clip(times.dayofyear.values, 1, 365)  # fold DOY 366 → 365
    nodata_band = np.full((n_y, n_x), _FILL_F4, dtype=np.float32)

    for doy in range(1, 366):
        idx = np.where(doy_vals == doy)[0]

        if idx.size == 0:
            for suffix in ("median", "p05", "p95"):
                yield nodata_band.copy(), f"doy{doy:03d}_{suffix}"
        else:
            sl = da.isel(time=idx).values.astype(np.float32)  # load this DOY only
            sl[(sl < vi_min) | (sl > vi_max)] = np.nan
            all_nan_mask = np.all(np.isnan(sl), axis=0)

            for pct, suffix in ((50, "median"), (5, "p05"), (95, "p95")):
                band = np.nanpercentile(sl, pct, axis=0).astype(np.float32)
                band[all_nan_mask] = _FILL_F4
                yield band, f"doy{doy:03d}_{suffix}"


# ---------------------------------------------------------------------------
# Per-datacube pipeline
# ---------------------------------------------------------------------------

def process_datacube(
    datacube_path: Path,
    output_dir: Path,
    config: dict,
    start_date: str | None,
    end_date: str | None,
    skip_per_year: bool = False,
    skip_per_month: bool = False,
    skip_per_doy: bool = False,
) -> None:
    """Extract per-year/month/DOY statistics from one datacube and write GeoTiffs.

    Args:
        datacube_path:  Path to a *_datacube.nc file.
        output_dir:     Root output directory; per-region subdir is created.
        config:         Dict with vi_min_{vi} / vi_max_{vi} keys for valid-range
                        masking, plus fallback vi_min / vi_max.
        start_date:     Optional YYYY-MM-DD lower bound (inclusive).
        end_date:       Optional YYYY-MM-DD upper bound (inclusive).
        skip_per_year:  If True, skip the per-year GeoTiff.
        skip_per_month: If True, skip the per-month GeoTiff.
        skip_per_doy:   If True, skip the per-DOY GeoTiff.
    """
    # ── Parse VI name and region_label from filename ──────────────────────────
    stem = datacube_path.stem   # e.g. "NDVI_MyRegion_datacube"
    if not stem.endswith("_datacube"):
        logger.warning(
            "Unexpected datacube filename '%s' — expected *_datacube.nc; "
            "proceeding anyway.", datacube_path.name,
        )
    base  = stem[: -len("_datacube")] if stem.endswith("_datacube") else stem
    parts = base.split("_", 1)
    vi_name      = parts[0].upper()
    region_label = parts[1] if len(parts) > 1 else "unknown_region"

    logger.info(
        "Processing datacube: VI=%s  region=%s  path=%s",
        vi_name, region_label, datacube_path,
    )

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

    if start_date or end_date:
        time_sel: dict = {}
        if start_date:
            time_sel["time"] = slice(start_date, None)
        if end_date:
            existing = time_sel.get("time", slice(None, None))
            time_sel["time"] = slice(existing.start, end_date)
        da = da.sel(**time_sel)

    times    = pd.DatetimeIndex(pd.to_datetime(da.time.values))
    n_time   = len(times)
    n_y      = da.sizes.get("y", da.sizes.get("lat", 0))
    n_x      = da.sizes.get("x", da.sizes.get("lon", 0))
    y_coords = da.coords["y"].values if "y" in da.coords else da.coords["lat"].values
    x_coords = da.coords["x"].values if "x" in da.coords else da.coords["lon"].values

    # Read CRS WKT from the spatial_ref variable.
    try:
        crs_wkt = read_netcdf_crs(ds, nc_name=datacube_path.name)
    except Exception as exc:
        logger.error(
            "Could not read CRS from %s: %s — skipping.", datacube_path, exc
        )
        return

    # ── Valid-range config for this VI ────────────────────────────────────────
    vi_key = vi_name.lower()
    vi_min = config.get(f"vi_min_{vi_key}", config.get("vi_min", -1.0))
    vi_max = config.get(f"vi_max_{vi_key}", config.get("vi_max",  2.0))

    # ── Memory note ───────────────────────────────────────────────────────────
    n_bytes = n_time * n_y * n_x * 4
    if n_bytes > _MEM_WARN_GB * 1e9:
        logger.warning(
            "Large datacube (%.1f GB uncompressed, %d×%d spatial, %d time steps). "
            "Processing in temporal slices — peak memory is bounded per product.",
            n_bytes / 1e9, n_y, n_x, n_time,
        )

    logger.info(
        "Datacube: time=%d  y=%d  x=%d  (%.2f GB uncompressed) — lazy load",
        n_time, n_y, n_x, n_bytes / 1e9,
    )

    # ── Output directory ──────────────────────────────────────────────────────
    region_out_dir = output_dir / region_label
    region_out_dir.mkdir(parents=True, exist_ok=True)

    prefix = f"{vi_name}_{region_label}"

    # ── Per-year GeoTiff ──────────────────────────────────────────────────────
    if not skip_per_year:
        n_years    = len(np.unique(times.year))
        n_yr_bands = n_years * 3
        out_path   = region_out_dir / f"{prefix}_per_year.tif"
        logger.info(
            "Writing per-year GeoTiff: %d bands (%d years × 3 stats) → %s",
            n_yr_bands, n_years, out_path,
        )
        _write_geotiff(
            _iter_per_year_bands(da, times, vi_min, vi_max),
            n_yr_bands,
            crs_wkt, y_coords, x_coords, out_path,
            desc=f"{vi_name}/{region_label} per_year",
        )

    # ── Per-month GeoTiff ─────────────────────────────────────────────────────
    if not skip_per_month:
        out_path = region_out_dir / f"{prefix}_per_month.tif"
        logger.info("Writing per-month GeoTiff: 36 bands → %s", out_path)
        _write_geotiff(
            _iter_per_month_bands(da, times, vi_min, vi_max),
            36,
            crs_wkt, y_coords, x_coords, out_path,
            desc=f"{vi_name}/{region_label} per_month",
        )

    # ── Per-DOY GeoTiff ───────────────────────────────────────────────────────
    if not skip_per_doy:
        doy_bytes = 1095 * n_y * n_x * 4
        if doy_bytes > _DOY_WARN_GB * 1e9:
            logger.warning(
                "Per-DOY GeoTiff will be approximately %.1f GB on disk (uncompressed). "
                "Use --skip-per-doy to suppress this output for very large datacubes.",
                doy_bytes / 1e9,
            )
        out_path = region_out_dir / f"{prefix}_per_doy.tif"
        logger.info("Writing per-DOY GeoTiff: 1095 bands → %s", out_path)
        _write_geotiff(
            _iter_per_doy_bands(da, times, vi_min, vi_max),
            1095,
            crs_wkt, y_coords, x_coords, out_path,
            desc=f"{vi_name}/{region_label} per_doy",
        )

    logger.info("Completed: %s / %s", vi_name, region_label)


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
            "datacube_to_geotiff: write per-year, per-month, and per-DOY VI "
            "statistics as multi-band GeoTiffs from CF-1.8 datacubes produced "
            "by the netcdf_datacube pipeline."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # --- Input ---
    parser.add_argument(
        "--input-datacubes", nargs="+", required=True, metavar="PATH",
        help=(
            "Path(s) to *_datacube.nc files produced by the netcdf_datacube "
            "pipeline, or a directory containing them (searched recursively). "
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
                        help="Valid range for NDVI pixels.")
    parser.add_argument("--valid-range-evi2", default="-1,2",     metavar="MIN,MAX",
                        help="Valid range for EVI2 pixels.")
    parser.add_argument("--valid-range-nirv", default="-0.5,1",   metavar="MIN,MAX",
                        help="Valid range for NIRv pixels.")

    # --- Date range ---
    parser.add_argument(
        "--start-date", default=None, metavar="YYYY-MM-DD",
        help="Only use time steps on or after this date (inclusive).",
    )
    parser.add_argument(
        "--end-date", default=None, metavar="YYYY-MM-DD",
        help="Only use time steps on or before this date (inclusive).",
    )

    # --- Output toggles ---
    parser.add_argument(
        "--skip-per-year", action="store_true", default=False,
        help="Skip the per-year GeoTiff output.",
    )
    parser.add_argument(
        "--skip-per-month", action="store_true", default=False,
        help="Skip the per-month GeoTiff output.",
    )
    parser.add_argument(
        "--skip-per-doy", action="store_true", default=False,
        help=(
            "Skip the per-DOY GeoTiff output. "
            "Recommended for large datacubes (1095 bands; can exceed 4 GB)."
        ),
    )

    # --- Parallelization ---
    parser.add_argument(
        "--workers", type=int, default=4, metavar="N",
        help="Number of parallel threads for processing multiple datacubes concurrently.",
    )

    # --- Logging ---
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level.",
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

    setup_log_file(output_dir, "datacube_to_geotiff", args.log_level)

    # Parse valid ranges.
    ndvi_min, ndvi_max = _parse_valid_range(args.valid_range_ndvi, "NDVI")
    evi2_min, evi2_max = _parse_valid_range(args.valid_range_evi2, "EVI2")
    nirv_min, nirv_max = _parse_valid_range(args.valid_range_nirv, "NIRv")

    config = {
        "vi_min": -1.0, "vi_max": 2.0,
        "vi_min_ndvi": ndvi_min, "vi_max_ndvi": ndvi_max,
        "vi_min_evi2": evi2_min, "vi_max_evi2": evi2_max,
        "vi_min_nirv": nirv_min, "vi_max_nirv": nirv_max,
    }

    # Resolve input paths: expand any directory entries recursively.
    datacube_paths = []
    for raw_path in args.input_datacubes:
        p = Path(raw_path)
        if p.is_dir():
            found = sorted(p.rglob("*_datacube.nc"))
            if not found:
                logger.warning("No *_datacube.nc files found in directory: %s", p)
            else:
                logger.info("Found %d datacube(s) in %s", len(found), p)
            datacube_paths.extend(found)
        else:
            datacube_paths.append(p)

    if not datacube_paths:
        logger.error("No datacube files to process. Exiting.")
        sys.exit(1)

    logger.info("datacube_to_geotiff pipeline starting")
    logger.info("  Datacubes     : %d file(s)", len(datacube_paths))
    logger.info("  Output dir    : %s", output_dir)
    logger.info("  Workers       : %d threads", args.workers)
    logger.info("  Per-year      : %s", "skip" if args.skip_per_year  else "write")
    logger.info("  Per-month     : %s", "skip" if args.skip_per_month else "write")
    logger.info("  Per-DOY       : %s", "skip" if args.skip_per_doy   else "write")
    if args.start_date or args.end_date:
        logger.info(
            "  Date range    : %s → %s",
            args.start_date or "start of record",
            args.end_date   or "end of record",
        )

    def _run(dc_path: Path) -> None:
        if not dc_path.exists():
            logger.error("Datacube not found: %s — skipping.", dc_path)
            return
        process_datacube(
            datacube_path=dc_path,
            output_dir=output_dir,
            config=config,
            start_date=args.start_date,
            end_date=args.end_date,
            skip_per_year=args.skip_per_year,
            skip_per_month=args.skip_per_month,
            skip_per_doy=args.skip_per_doy,
        )

    if len(datacube_paths) == 1 or args.workers <= 1:
        for dc_path in datacube_paths:
            _run(dc_path)
    else:
        futures: dict = {}
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for dc_path in datacube_paths:
                futures[executor.submit(_run, dc_path)] = dc_path
            for fut in as_completed(futures):
                dc_path = futures[fut]
                exc = fut.exception()
                if exc:
                    logger.error("Failed processing %s: %s", dc_path, exc)

    logger.info("Done. All outputs written to: %s", output_dir)


if __name__ == "__main__":
    main()
