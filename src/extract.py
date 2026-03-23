#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# extract.py
# Layer 0 + 1: Read VI NetCDFs, apply spatial mask, aggregate to time series.
#
# Layer 0: Raw aggregated observations (actual observation dates only).
#          Spatial mean + std + count of valid pixels within the ROI.
# Layer 1: Raw observations reindexed to a complete daily DatetimeIndex.
#          Non-observation days are NaN — gaps are preserved, not filled.
#
# Returns a dict keyed by (vi, region_label) → pd.DataFrame with columns:
#   date (datetime64), vi_raw (float32), vi_count (int32), vi_std (float32),
#   vi_daily (float32)
#
# Author:  Stephen Conklin <stephenconklin@gmail.com>
# License: MIT

import logging
import sys

import numpy as np
import pandas as pd
import xarray as xr
import rioxarray  # noqa: F401 — activates .rio accessor
import geopandas as gpd
from pathlib import Path
from typing import Optional
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from phenology_config import PhenologyConfig
from io_utils import sanitize_label, load_shapefile_regions, read_netcdf_crs

logger = logging.getLogger(__name__)

try:
    from rioxarray.exceptions import NoDataInBounds
except ImportError:
    # Older rioxarray versions: fall back to a catch-all for clip failures.
    NoDataInBounds = Exception  # type: ignore[misc,assignment]


# ---------------------------------------------------------------------------
# NetCDF discovery
# ---------------------------------------------------------------------------

def discover_netcdfs(netcdf_dir: Path, vi: str) -> list:
    """Glob for T{TILE}_{VI}.nc files in netcdf_dir for the given VI.

    Returns a sorted list of Path objects. Raises FileNotFoundError if the
    directory does not exist. Returns an empty list (with a warning) if no
    files match.
    """
    from io_utils import discover_netcdfs_for_vi
    return discover_netcdfs_for_vi(netcdf_dir, vi)


# ---------------------------------------------------------------------------
# Spatial masking
# ---------------------------------------------------------------------------

def clip_netcdf_to_roi(nc_path: Path, roi_gdf: gpd.GeoDataFrame) -> Optional[xr.DataArray]:
    """Open a NetCDF, detect its CRS, reproject the ROI geometry to match,
    and return a spatially clipped DataArray.

    Uses rioxarray .rio.clip() for the clip operation.
    Reads CRS from the 'spatial_ref' variable WKT (CF-1.8 grid mapping).
    Returns None if the ROI does not intersect the tile extent.

    The file is opened with chunks={} to use the file's native chunk layout.
    The synchronous dask scheduler then processes one native chunk at a time,
    keeping memory bounded without causing I/O amplification from sub-chunk reads.
    """
    vi_name = nc_path.stem.rsplit('_', 1)[-1]
    ds = xr.open_dataset(nc_path, chunks={})
    da = ds[vi_name]

    # Read CRS from spatial_ref variable (CF-1.8 grid mapping).
    wkt = read_netcdf_crs(ds, nc_path.name)
    da = da.rio.write_crs(wkt)

    roi_reprojected = roi_gdf.to_crs(da.rio.crs)

    try:
        clipped = da.rio.clip(roi_reprojected.geometry, all_touched=True, drop=True)
        n_y, n_x = clipped.sizes.get('y', 0), clipped.sizes.get('x', 0)
        logger.debug(
            "Tile %s: clipped to %d×%d px within ROI", nc_path.name, n_y, n_x
        )
        return clipped
    except NoDataInBounds:
        logger.debug("Tile %s: no overlap with ROI — skipping", nc_path.name)
        return None
    except Exception as e:
        logger.error(
            "Unexpected error clipping tile '%s': %s: %s",
            nc_path.name, type(e).__name__, e,
        )
        raise


def open_full_extent(nc_path: Path) -> xr.DataArray:
    """Open a NetCDF without spatial clipping.

    Returns the full DataArray for the VI variable.
    Used when no shapefile is provided.

    Opened with chunks={} to use the file's native chunk layout.
    """
    vi_name = nc_path.stem.rsplit('_', 1)[-1]
    ds = xr.open_dataset(nc_path, chunks={})
    da = ds[vi_name]

    wkt = read_netcdf_crs(ds, nc_path.name)
    da = da.rio.write_crs(wkt)
    return da


# ---------------------------------------------------------------------------
# Spatial aggregation (Layer 0)
# ---------------------------------------------------------------------------

def aggregate_spatial(da: xr.DataArray, vmin: float, vmax: float) -> pd.DataFrame:
    """Apply valid-range masking and compute spatial statistics per time step.

    For each time step:
      - Pixels outside [vmin, vmax] are set to NaN (outlier masking)
      - vi_raw:   spatial mean of valid pixels
      - vi_count: number of valid pixels contributing to the mean
      - vi_std:   spatial standard deviation of valid pixels

    This function is a public API for single-tile aggregation. When pooling
    pixels across multiple tiles, use aggregate_across_tiles(), which operates
    on raw pixel values directly to ensure a correctly weighted pooled mean and
    standard deviation (rather than averaging per-tile statistics).

    Args:
        da:    DataArray with dims (time, y, x). Time decoded to datetime64.
        vmin:  Minimum valid VI value (inclusive).
        vmax:  Maximum valid VI value (inclusive).

    Returns:
        pd.DataFrame with columns [date, vi_raw, vi_count, vi_std],
        indexed by actual observation dates only (Layer 0).
    """
    masked = da.where((da >= vmin) & (da <= vmax))
    dates = pd.to_datetime(da['time'].values)
    vi_raw = masked.mean(dim=['y', 'x']).values.astype(np.float32)
    vi_count = masked.count(dim=['y', 'x']).values.astype(np.int32)
    vi_std = masked.std(dim=['y', 'x']).values.astype(np.float32)
    return pd.DataFrame({
        'date': dates,
        'vi_raw': vi_raw,
        'vi_count': vi_count,
        'vi_std': vi_std,
    })


# ---------------------------------------------------------------------------
# Per-tile worker (module-level — must not be nested for multiprocessing pickling)
# ---------------------------------------------------------------------------

def _process_one_tile(args: tuple) -> dict:
    """Worker function for parallel tile aggregation.

    Must be defined at module top level (not nested inside a function or class)
    to be picklable by multiprocessing on all platforms.

    Imports dask and sets scheduler='synchronous' to force synchronous evaluation
    inside the worker process. This prevents each worker from spawning its own
    thread pool that would compete with other workers for CPU cores — the same
    pattern used in HLS_VI_Pipeline Step 04.

    Args:
        args: Tuple of (nc_path, roi_gdf, vmin, vmax, start_date, end_date,
              pixel_coords). pixel_coords is a set of (y_round, x_round) tuples
              or None (use all pixels).

    Returns:
        dict with keys:
            tile_name     str
            status        'ok' | 'skip' | 'error'
            message       str  — diagnostic message for main-process logging
            date_stats    {Timestamp: [sum, sum_sq, count]} | None
            n_dates       int  — total observation dates in tile
            n_valid_dates int  — dates with at least one valid pixel
            total_pixels  int  — cumulative valid-pixel count across all dates
    """
    import dask
    nc_path, roi_gdf, vmin, vmax, start_date, end_date, pixel_coords = args

    try:
        if roi_gdf is not None:
            da = clip_netcdf_to_roi(nc_path, roi_gdf)
            if da is None:
                return {
                    'tile_name': nc_path.name, 'status': 'skip',
                    'message': f"Tile {nc_path.name}: no overlap with ROI",
                    'date_stats': None, 'n_dates': 0, 'n_valid_dates': 0, 'total_pixels': 0,
                }
        else:
            da = open_full_extent(nc_path)

        # Apply optional date range filter before any compute — keeps memory low.
        if start_date or end_date:
            da = da.sel(time=slice(start_date, end_date))
            if da.sizes['time'] == 0:
                return {
                    'tile_name': nc_path.name, 'status': 'skip',
                    'message': (
                        f"Tile {nc_path.name}: no time steps within "
                        f"[{start_date or 'start'}, {end_date or 'end'}]"
                    ),
                    'date_stats': None, 'n_dates': 0, 'n_valid_dates': 0, 'total_pixels': 0,
                }

        masked = da.where((da >= vmin) & (da <= vmax))

        # Apply pixel sample mask when a sampled coordinate set is provided.
        # Build a 2D boolean mask in O(N_sample) time using dict lookups.
        if pixel_coords is not None:
            y_arr = np.round(da.coords['y'].values.astype(np.float64), 1)
            x_arr = np.round(da.coords['x'].values.astype(np.float64), 1)
            y_to_idx = {float(y): i for i, y in enumerate(y_arr)}
            x_to_idx = {float(x): i for i, x in enumerate(x_arr)}
            pixel_mask_2d = np.zeros((len(y_arr), len(x_arr)), dtype=bool)
            for (y_r, x_r) in pixel_coords:
                iy = y_to_idx.get(float(y_r))
                ix = x_to_idx.get(float(x_r))
                if iy is not None and ix is not None:
                    pixel_mask_2d[iy, ix] = True
            if not pixel_mask_2d.any():
                return {
                    'tile_name': nc_path.name, 'status': 'skip',
                    'message': f"Tile {nc_path.name}: no sampled pixels within tile extent",
                    'date_stats': None, 'n_dates': 0, 'n_valid_dates': 0, 'total_pixels': 0,
                }
            pixel_mask_da = xr.DataArray(pixel_mask_2d, dims=['y', 'x'])
            masked = masked.where(pixel_mask_da)

        # Compute with synchronous scheduler — prevents nested thread pools inside
        # the worker process from competing with other parallel workers for cores.
        with dask.config.set(scheduler='synchronous'):
            tile_sum    = masked.sum(dim=['y', 'x'], skipna=True).values       # (time,)
            tile_count  = masked.count(dim=['y', 'x']).values                  # (time,)
            tile_sum_sq = (masked ** 2).sum(dim=['y', 'x'], skipna=True).values  # (time,)

        # Normalize to midnight — HLS tiles can contain L30 and S30 acquisitions
        # on the same calendar date. Accumulate rather than overwrite so same-day
        # entries are pooled before being returned to the main process.
        dates = pd.to_datetime(da['time'].values).normalize()

        date_stats: dict = {}
        n_valid_dates = 0
        total_pixels = 0
        for i, date in enumerate(dates):
            n = int(tile_count[i])
            s  = float(tile_sum[i])
            sq = float(tile_sum_sq[i])
            if date not in date_stats:
                date_stats[date] = [s, sq, n]
            else:
                date_stats[date][0] += s
                date_stats[date][1] += sq
                date_stats[date][2] += n
            if n > 0:
                n_valid_dates += 1
                total_pixels += n

        return {
            'tile_name': nc_path.name, 'status': 'ok',
            'date_stats': date_stats,
            'n_dates': len(dates), 'n_valid_dates': n_valid_dates, 'total_pixels': total_pixels,
        }

    except Exception as e:
        return {
            'tile_name': nc_path.name, 'status': 'error',
            'message': f"{type(e).__name__}: {e}",
            'date_stats': None, 'n_dates': 0, 'n_valid_dates': 0, 'total_pixels': 0,
        }


# ---------------------------------------------------------------------------
# Pixel selection worker (module-level — must not be nested for pickling)
# ---------------------------------------------------------------------------

def _compute_pixel_stats_one_tile(args: tuple) -> dict:
    """Worker: compute per-pixel temporal statistics used for pixel selection.

    For each pixel that has at least one valid observation, returns the sum of
    valid NDVI values and the count of valid timesteps — sufficient to compute
    temporal mean NDVI and valid fraction after combining across tiles.

    Must be defined at module top level (not nested) to be picklable by
    multiprocessing on all platforms.

    Args:
        args: Tuple of (nc_path, roi_gdf, vmin, vmax, start_date, end_date).

    Returns:
        dict with keys:
            tile_name  str
            status     'ok' | 'skip' | 'error'
            n_time     int  — total time steps in this tile
            pixels     list of (y_round, x_round, ndvi_sum, count) | None
            message    str  — diagnostic (skip/error only)
    """
    import dask
    nc_path, roi_gdf, vmin, vmax, start_date, end_date = args

    try:
        if roi_gdf is not None:
            da = clip_netcdf_to_roi(nc_path, roi_gdf)
            if da is None:
                return {
                    'tile_name': nc_path.name, 'status': 'skip',
                    'message': f"Tile {nc_path.name}: no overlap with ROI",
                    'n_time': 0, 'pixels': None,
                }
        else:
            da = open_full_extent(nc_path)

        if start_date or end_date:
            da = da.sel(time=slice(start_date, end_date))
            if da.sizes['time'] == 0:
                return {
                    'tile_name': nc_path.name, 'status': 'skip',
                    'message': f"Tile {nc_path.name}: no time steps in date range",
                    'n_time': 0, 'pixels': None,
                }

        n_time = int(da.sizes['time'])
        masked = da.where((da >= vmin) & (da <= vmax))

        with dask.config.set(scheduler='synchronous'):
            pixel_sum   = masked.sum(dim='time', skipna=True).values   # (y, x)
            pixel_count = masked.count(dim='time').values               # (y, x)

        # Round coordinates to 1 decimal place for reliable cross-tile matching.
        y_coords = np.round(da.coords['y'].values.astype(np.float64), 1)
        x_coords = np.round(da.coords['x'].values.astype(np.float64), 1)

        # Vectorised: only include pixels with at least one valid observation.
        valid_iy, valid_ix = np.where(pixel_count > 0)
        pixels = list(zip(
            y_coords[valid_iy].tolist(),
            x_coords[valid_ix].tolist(),
            pixel_sum[valid_iy, valid_ix].tolist(),
            pixel_count[valid_iy, valid_ix].astype(int).tolist(),
        ))

        return {'tile_name': nc_path.name, 'status': 'ok', 'n_time': n_time, 'pixels': pixels}

    except Exception as e:
        return {
            'tile_name': nc_path.name, 'status': 'error',
            'message': f"{type(e).__name__}: {e}",
            'n_time': 0, 'pixels': None,
        }


def select_pixel_sample(
    nc_paths: list,
    roi_gdf: Optional[gpd.GeoDataFrame],
    vmin: float,
    vmax: float,
    n_sample: Optional[int],
    random_seed: Optional[int],
    min_ndvi_mean: Optional[float],
    min_quality_frac: float,
    start_date: Optional[str],
    end_date: Optional[str],
    n_workers: int,
) -> set:
    """Select a spatially consistent pixel sample across all tiles.

    Phase A of the pixel-sampling flow. Runs a parallel pass over all tiles to
    compute per-pixel temporal mean NDVI and valid fraction, then applies
    optional filters and draws a random sample of N pixels.

    Pixels are identified by (y_round, x_round) coordinate tuples (rounded to
    1 decimal place). Same-CRS tiles share an identical 30-m grid, so
    coordinates are directly comparable across tiles. Cross-CRS tiles will not
    share coordinates; their pixels are sampled independently and the union is
    returned — which is the same graceful behaviour as the current un-sampled
    extraction.

    Args:
        nc_paths:         List of NetCDF Paths for a single VI.
        roi_gdf:          GeoDataFrame of the ROI, or None for full extent.
        vmin, vmax:       Valid VI range (inclusive).
        n_sample:         Number of pixels to randomly sample; None = keep all.
        random_seed:      Seed for np.random.default_rng; None = random.
        min_ndvi_mean:    Exclude pixels with temporal mean NDVI below this.
        min_quality_frac: Exclude pixels valid in fewer than this fraction of
                          timesteps (0.0 = no filter).
        start_date:       Optional date lower bound (YYYY-MM-DD).
        end_date:         Optional date upper bound (YYYY-MM-DD).
        n_workers:        Number of parallel worker processes.

    Returns:
        Set of (y_round, x_round) float tuples representing sampled pixels.
        Returns an empty set if no pixels pass the filters.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed

    work_items = [
        (nc_path, roi_gdf, vmin, vmax, start_date, end_date)
        for nc_path in nc_paths
    ]

    # Accumulate per-pixel stats across tiles.
    # pixel_dict: (y_r, x_r) -> [ndvi_sum, valid_count, total_n_time]
    pixel_dict: dict = {}

    with logging_redirect_tqdm(), ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(_compute_pixel_stats_one_tile, item): item
                   for item in work_items}
        for future in tqdm(as_completed(futures), total=len(futures),
                           desc="Phase A tiles", unit="tile", leave=False):
            result = future.result()
            if result['status'] != 'ok' or not result['pixels']:
                continue
            n_time = result['n_time']
            for (y_r, x_r, ndvi_sum, count) in result['pixels']:
                key = (y_r, x_r)
                if key not in pixel_dict:
                    pixel_dict[key] = [0.0, 0, 0]
                pixel_dict[key][0] += ndvi_sum
                pixel_dict[key][1] += count
                pixel_dict[key][2] += n_time

    if not pixel_dict:
        logger.warning("select_pixel_sample: no valid pixels found across all tiles.")
        return set()

    all_keys = list(pixel_dict.keys())
    totals = np.array([[v[0], v[1], v[2]] for v in pixel_dict.values()])
    # Avoid division by zero for total_count (should not happen since we skip count==0 above).
    valid_count = totals[:, 1].astype(float)
    mean_ndvi  = np.where(valid_count > 0, totals[:, 0] / valid_count, np.nan)
    valid_frac = np.where(totals[:, 2] > 0, valid_count / totals[:, 2], 0.0)

    # Apply filters.
    keep = np.ones(len(all_keys), dtype=bool)
    if min_ndvi_mean is not None:
        keep &= (mean_ndvi >= min_ndvi_mean)
    if min_quality_frac > 0.0:
        keep &= (valid_frac >= min_quality_frac)

    eligible_keys = [all_keys[i] for i in range(len(all_keys)) if keep[i]]
    n_eligible = len(eligible_keys)
    logger.info(
        "  Pixel selection: %d total, %d eligible (min_ndvi_mean=%s, min_quality_frac=%.2f)",
        len(all_keys), n_eligible,
        f"{min_ndvi_mean:.3f}" if min_ndvi_mean is not None else "none",
        min_quality_frac,
    )

    if n_eligible == 0:
        logger.warning("  No pixels passed selection filters — region will be skipped.")
        return set()

    if n_sample is not None and n_sample < n_eligible:
        rng = np.random.default_rng(random_seed)
        chosen = rng.choice(n_eligible, size=n_sample, replace=False)
        selected = {eligible_keys[int(i)] for i in chosen}
        logger.info(
            "  Randomly sampled %d/%d pixels (seed=%s)",
            n_sample, n_eligible, random_seed,
        )
    else:
        selected = set(eligible_keys)
        if n_sample is not None:
            logger.info(
                "  Requested %d samples but only %d eligible — using all eligible pixels.",
                n_sample, n_eligible,
            )

    return selected


# ---------------------------------------------------------------------------
# Multi-tile aggregation
# ---------------------------------------------------------------------------

def aggregate_across_tiles(
    nc_paths: list,
    roi_gdf: Optional[gpd.GeoDataFrame],
    vmin: float,
    vmax: float,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    n_workers: int = 4,
    n_sample: Optional[int] = None,
    random_seed: Optional[int] = None,
    min_ndvi_mean: Optional[float] = None,
    min_quality_frac: float = 0.0,
) -> pd.DataFrame:
    """Aggregate Layer 0 observations across multiple tiles for a single VI.

    Tiles are processed in parallel using concurrent.futures.ProcessPoolExecutor.
    Each worker processes one tile independently and returns a per-tile stats dict.
    The main process merges all per-tile dicts then computes the pooled mean and
    standard deviation — identical math to the former sequential implementation.

    Pooling is done via raw sum / sum-of-squares / count accumulators rather than
    averaging per-tile statistics, which ensures a correctly weighted pooled mean
    and unbiased pooled standard deviation.

    When pixel sampling is requested (n_sample, min_ndvi_mean, or min_quality_frac
    are set), a Phase A pixel selection pass runs first via select_pixel_sample().
    The resulting pixel coordinate set is passed to each extraction worker so that
    only the sampled pixels contribute to the spatial mean at each time step.
    This ensures the same spatial sample is used consistently across the full
    time series, eliminating date-to-date variation in which pixels are included.

    Args:
        nc_paths:         List of NetCDF Paths for a single VI.
        roi_gdf:          GeoDataFrame of the ROI, or None for full extent.
        vmin:             Minimum valid VI value (inclusive).
        vmax:             Maximum valid VI value (inclusive).
        start_date:       Optional ISO-8601 date string (YYYY-MM-DD).
        end_date:         Optional ISO-8601 date string (YYYY-MM-DD).
        n_workers:        Number of parallel worker processes (default 4).
        n_sample:         Number of pixels to randomly sample; None = all pixels.
        random_seed:      RNG seed for reproducibility; None = random.
        min_ndvi_mean:    Exclude pixels whose temporal mean NDVI is below this.
        min_quality_frac: Exclude pixels valid in fewer than this fraction of
                          timesteps (0.0 = no filter).

    Returns:
        pd.DataFrame with columns [date, vi_raw, vi_count, vi_std] (Layer 0).
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed

    # ── Phase A: pixel selection (only when sampling or filtering is requested) ─
    pixel_coords: Optional[set] = None
    if n_sample is not None or min_ndvi_mean is not None or min_quality_frac > 0.0:
        logger.info("  Phase A: selecting pixel sample ...")
        pixel_coords = select_pixel_sample(
            nc_paths, roi_gdf, vmin, vmax,
            n_sample=n_sample, random_seed=random_seed,
            min_ndvi_mean=min_ndvi_mean, min_quality_frac=min_quality_frac,
            start_date=start_date, end_date=end_date,
            n_workers=n_workers,
        )
        if len(pixel_coords) == 0:
            return pd.DataFrame(columns=['date', 'vi_raw', 'vi_count', 'vi_std'])
        logger.info("  Phase B: extracting time series from %d pixel(s) ...", len(pixel_coords))

    work_items = [
        (nc_path, roi_gdf, vmin, vmax, start_date, end_date, pixel_coords)
        for nc_path in nc_paths
    ]
    total = len(work_items)
    logger.info(
        "  Dispatching %d tile(s) across %d worker process(es)", total, min(n_workers, total)
    )

    # Accumulate per-date: date -> [sum, sum_of_squares, count]
    date_stats: dict = {}
    n_tiles_used = 0

    with logging_redirect_tqdm(), ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(_process_one_tile, item): item for item in work_items}
        for future in tqdm(as_completed(futures), total=len(futures),
                           desc="Tiles", unit="tile", leave=False):
            result = future.result()
            tile_name = result['tile_name']

            if result['status'] == 'skip':
                logger.debug("  %s", result['message'])
                continue
            elif result['status'] == 'error':
                logger.error("  Tile %s: %s", tile_name, result['message'])
                continue

            # Merge this tile's per-date stats into the pooled accumulator.
            for date, (s, sq, n) in result['date_stats'].items():
                if date not in date_stats:
                    date_stats[date] = [0.0, 0.0, 0]
                date_stats[date][0] += s
                date_stats[date][1] += sq
                date_stats[date][2] += n

            n_tiles_used += 1
            logger.info(
                "  Tile %s: %d obs dates, %d with valid pixels, %s total valid pixel-obs",
                tile_name, result['n_dates'], result['n_valid_dates'],
                f"{result['total_pixels']:,}",
            )

    if n_tiles_used == 0:
        logger.warning("No tiles contributed valid data. ROI may not intersect any tile.")
        return pd.DataFrame(columns=['date', 'vi_raw', 'vi_count', 'vi_std'])

    # Build result DataFrame from pooled accumulators.
    rows = []
    for date in sorted(date_stats):
        s, sq, n = date_stats[date]
        if n == 0:
            rows.append({'date': date, 'vi_raw': np.nan, 'vi_count': 0, 'vi_std': np.nan})
        elif n == 1:
            rows.append({'date': date, 'vi_raw': float(s), 'vi_count': 1, 'vi_std': np.nan})
        else:
            mean = s / n
            # Pooled sample variance from sum-of-squares (numerically stable).
            var = max((sq - n * mean ** 2) / (n - 1), 0.0)
            rows.append({
                'date': date,
                'vi_raw': float(mean),
                'vi_count': n,
                'vi_std': float(np.sqrt(var)),
            })

    df = pd.DataFrame(rows)
    df['vi_raw'] = df['vi_raw'].astype(np.float32)
    df['vi_count'] = df['vi_count'].astype(np.int32)
    df['vi_std'] = df['vi_std'].astype(np.float32)

    valid_obs = int((df['vi_count'] > 0).sum())
    logger.info(
        "  Pooled result: %d total dates, %d with valid pixels, "
        "vi_raw range [%.4f, %.4f]",
        len(df), valid_obs,
        float(df['vi_raw'].min()), float(df['vi_raw'].max()),
    )
    return df


# ---------------------------------------------------------------------------
# Datacube aggregation (Layer 0 from pre-clipped datacube)
# ---------------------------------------------------------------------------

def aggregate_from_datacube(
    dc_path: Path,
    vi: str,
    vmin: float,
    vmax: float,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    n_sample: Optional[int] = None,
    random_seed: Optional[int] = None,
    min_ndvi_mean: Optional[float] = None,
    min_quality_frac: float = 0.0,
    use_median: bool = False,
) -> pd.DataFrame:
    """Aggregate Layer 0 observations from a pre-clipped per-pixel datacube.

    Equivalent to aggregate_across_tiles() but reads from a single merged
    datacube file instead of re-clipping source tiles.  No parallel workers
    are needed — spatial clipping is already embedded in the datacube.

    Pixel sampling (n_sample, min_ndvi_mean, min_quality_frac) is supported:
    per-pixel temporal statistics are computed in-process from the 2D arrays
    and the same pixel mask is applied before spatial aggregation, so the
    result is numerically identical to the tile-based sampling path for
    same-CRS regions.

    Args:
        dc_path:          Path to a {VI}_{region_label}_datacube.nc file.
        vi:               VI variable name (e.g. 'NDVI').
        vmin:             Minimum valid VI value (inclusive).
        vmax:             Maximum valid VI value (inclusive).
        start_date:       Optional ISO-8601 date string (YYYY-MM-DD).
        end_date:         Optional ISO-8601 date string (YYYY-MM-DD).
        n_sample:         Number of pixels to randomly sample; None = all pixels.
        random_seed:      RNG seed for reproducibility; None = random.
        min_ndvi_mean:    Exclude pixels whose temporal mean NDVI is below this.
        min_quality_frac: Exclude pixels valid in fewer than this fraction of
                          timesteps (0.0 = no filter).

    Returns:
        pd.DataFrame with columns [date, vi_raw, vi_count, vi_std] (Layer 0).
        Only rows where vi_count > 0 are included (observation dates only).
    """
    import dask

    _empty = pd.DataFrame(columns=['date', 'vi_raw', 'vi_count', 'vi_std'])

    try:
        ds = xr.open_dataset(dc_path, chunks={})
    except Exception as e:
        logger.error("aggregate_from_datacube: cannot open '%s': %s", dc_path.name, e)
        return _empty

    if vi not in ds:
        logger.error(
            "aggregate_from_datacube: variable '%s' not found in '%s' "
            "(available: %s) — skipping.",
            vi, dc_path.name, list(ds.data_vars),
        )
        return _empty

    da = ds[vi]

    # Apply date range filter.
    if start_date or end_date:
        da = da.sel(time=slice(start_date, end_date))
    if da.sizes['time'] == 0:
        logger.warning(
            "aggregate_from_datacube: no time steps remain after date filtering "
            "('%s')", dc_path.name,
        )
        return _empty

    # Apply valid-range mask.
    masked = da.where((da >= vmin) & (da <= vmax))

    n_time = int(da.sizes['time'])
    y_coords = np.round(da.coords['y'].values.astype(np.float64), 1)
    x_coords = np.round(da.coords['x'].values.astype(np.float64), 1)

    # ── Pixel sampling / filtering (optional) ───────────────────────────────
    do_sampling = (
        n_sample is not None or min_ndvi_mean is not None or min_quality_frac > 0.0
    )
    if do_sampling:
        need_stats = min_ndvi_mean is not None or min_quality_frac > 0.0
        if need_stats:
            logger.info(
                "  aggregate_from_datacube: computing per-pixel stats for sampling "
                "(%s)", dc_path.name,
            )
            with dask.config.set(scheduler='synchronous'):
                pixel_sum_2d   = masked.sum(dim='time', skipna=True).values    # (y, x)
                pixel_count_2d = masked.count(dim='time').values                # (y, x)

            # Build pixel_dict: {(y_r, x_r): [ndvi_sum, count, n_time]}
            valid_iy, valid_ix = np.where(pixel_count_2d > 0)
            pixel_dict: dict = {}
            for iy, ix in zip(valid_iy, valid_ix):
                key = (float(y_coords[iy]), float(x_coords[ix]))
                pixel_dict[key] = [
                    float(pixel_sum_2d[iy, ix]),
                    int(pixel_count_2d[iy, ix]),
                    n_time,
                ]

            # Apply filters.
            eligible_keys = []
            for key, (ndvi_sum, count, n_t) in pixel_dict.items():
                mean_ndvi = ndvi_sum / count
                valid_frac = count / n_t
                if min_ndvi_mean is not None and mean_ndvi < min_ndvi_mean:
                    continue
                if valid_frac < min_quality_frac:
                    continue
                eligible_keys.append(key)

            n_eligible = len(eligible_keys)
            logger.info(
                "  Pixel selection: %d total, %d eligible (min_ndvi_mean=%s, "
                "min_quality_frac=%.2f)",
                len(pixel_dict), n_eligible,
                f"{min_ndvi_mean:.3f}" if min_ndvi_mean is not None else "none",
                min_quality_frac,
            )
        else:
            # No filters active — enumerate pixel coordinates directly from
            # the coordinate arrays (no data read required).
            eligible_keys = [
                (float(y_coords[iy]), float(x_coords[ix]))
                for iy in range(len(y_coords))
                for ix in range(len(x_coords))
            ]
            n_eligible = len(eligible_keys)
            logger.info(
                "  Pixel selection: %d total pixels available, no filters active "
                "(%s)", n_eligible, dc_path.name,
            )

        if n_eligible == 0:
            logger.warning(
                "aggregate_from_datacube: no eligible pixels after filtering — skipping."
            )
            return _empty

        # Draw random sample if requested.
        if n_sample is not None and n_sample < n_eligible:
            rng = np.random.default_rng(random_seed)
            chosen_indices = rng.choice(n_eligible, size=n_sample, replace=False)
            pixel_coords = {eligible_keys[i] for i in chosen_indices}
            logger.info(
                "  Sampled %d pixels from %d eligible (seed=%s)",
                n_sample, n_eligible,
                str(random_seed) if random_seed is not None else "random",
            )
        else:
            pixel_coords = set(eligible_keys)

        # Build 2D boolean mask and apply.
        y_to_idx = {float(y): i for i, y in enumerate(y_coords)}
        x_to_idx = {float(x): i for i, x in enumerate(x_coords)}
        pixel_mask_2d = np.zeros((len(y_coords), len(x_coords)), dtype=bool)
        for (y_r, x_r) in pixel_coords:
            iy = y_to_idx.get(y_r)
            ix = x_to_idx.get(x_r)
            if iy is not None and ix is not None:
                pixel_mask_2d[iy, ix] = True

        pixel_mask_da = xr.DataArray(pixel_mask_2d, dims=['y', 'x'],
                                     coords={'y': da.coords['y'], 'x': da.coords['x']})
        masked = masked.where(pixel_mask_da)

    # Normalize timestamps to midnight — HLS datacubes can contain both Landsat
    # (L30) and Sentinel-2 (S30) acquisitions on the same calendar date, producing
    # duplicate time steps. Moved before the aggregation loop so both mean and
    # median branches can group same-day acquisitions correctly.
    dates = pd.to_datetime(da.coords['time'].values).normalize()

    # ── Spatial aggregation per time step ───────────────────────────────────
    dc_desc = dc_path.stem.replace("_datacube", "")
    rows = []

    if use_median:
        # Median mode: collect all valid pixel values per calendar date, then
        # compute spatial median and IQR from the full per-day distribution.
        # Same-day acquisitions (L30 + S30) are pooled before computing median,
        # matching the correctness of the mean branch's sum pooling.
        # vi_std holds IQR (Q75 − Q25) — same units as VI, robust spread measure.
        date_pixels: dict = {}  # date -> list[float]
        with logging_redirect_tqdm(), dask.config.set(scheduler='synchronous'):
            for t in tqdm(range(n_time), desc=dc_desc, unit="step", leave=False):
                date = dates[t]
                s2d = masked.isel(time=t).values
                valid = ~np.isnan(s2d)
                if int(valid.sum()) > 0:
                    if date not in date_pixels:
                        date_pixels[date] = []
                    date_pixels[date].extend(s2d[valid].tolist())

        for date in sorted(date_pixels):
            vals = np.asarray(date_pixels[date], dtype=np.float64)
            n = len(vals)
            iqr = float(np.percentile(vals, 75) - np.percentile(vals, 25)) if n > 1 else np.nan
            rows.append({
                'date': date,
                'vi_raw': float(np.median(vals)),
                'vi_count': n,
                'vi_std': iqr,
            })

    else:
        # Mean mode: accumulate sum / count / sum_of_squares per calendar date,
        # then compute pooled spatial mean and sample standard deviation.
        agg_sum    = np.zeros(n_time, dtype=np.float64)
        agg_count  = np.zeros(n_time, dtype=np.int64)
        agg_sum_sq = np.zeros(n_time, dtype=np.float64)
        with logging_redirect_tqdm(), dask.config.set(scheduler='synchronous'):
            for t in tqdm(range(n_time), desc=dc_desc, unit="step", leave=False):
                s2d = masked.isel(time=t).values
                valid = ~np.isnan(s2d)
                c = int(valid.sum())
                agg_count[t] = c
                if c > 0:
                    v = s2d[valid].astype(np.float64)
                    agg_sum[t]    = v.sum()
                    agg_sum_sq[t] = (v * v).sum()

        date_accum: dict = {}   # date -> [sum, count, sum_sq]
        for i, date in enumerate(dates):
            n = int(agg_count[i])
            if n == 0:
                continue
            s  = float(agg_sum[i])
            sq = float(agg_sum_sq[i])
            if date not in date_accum:
                date_accum[date] = [s, n, sq]
            else:
                date_accum[date][0] += s
                date_accum[date][1] += n
                date_accum[date][2] += sq

        for date, (s, n, sq) in sorted(date_accum.items()):
            mean = s / n
            std = float(np.sqrt(max((sq - n * mean ** 2) / (n - 1), 0.0))) if n > 1 else np.nan
            rows.append({'date': date, 'vi_raw': float(mean), 'vi_count': n, 'vi_std': std})

    if not rows:
        logger.warning(
            "aggregate_from_datacube: no valid observations in '%s'.", dc_path.name,
        )
        return _empty

    df = pd.DataFrame(rows)
    df['vi_raw']   = df['vi_raw'].astype(np.float32)
    df['vi_count'] = df['vi_count'].astype(np.int32)
    df['vi_std']   = df['vi_std'].astype(np.float32)

    logger.info(
        "  Datacube result: %d valid observation dates, vi_raw range [%.4f, %.4f]",
        len(df), float(df['vi_raw'].min()), float(df['vi_raw'].max()),
    )
    return df


# ---------------------------------------------------------------------------
# Daily reindex (Layer 1)
# ---------------------------------------------------------------------------

def reindex_to_daily(obs_df: pd.DataFrame) -> pd.DataFrame:
    """Reindex a Layer 0 observation DataFrame to a complete daily DatetimeIndex.

    Non-observation days receive:
      vi_raw   = NaN
      vi_count = 0
      vi_std   = NaN
      vi_daily = NaN  (same as vi_raw, explicit column for clarity)

    Observation days receive vi_daily = vi_raw.

    Returns a DataFrame spanning from the first to last observation date,
    with one row per calendar day.
    """
    if obs_df.empty:
        logger.warning("reindex_to_daily: received empty observation DataFrame")
        return obs_df.assign(vi_daily=pd.Series(dtype=np.float32))

    obs_df = obs_df.set_index('date')
    daily_index = pd.date_range(obs_df.index.min(), obs_df.index.max(), freq='D')
    daily_df = obs_df.reindex(daily_index)

    daily_df['vi_count'] = daily_df['vi_count'].fillna(0).astype(np.int32)
    # Cast to float32 before creating vi_daily so both columns share the same dtype.
    daily_df['vi_raw'] = daily_df['vi_raw'].astype(np.float32)
    daily_df['vi_std'] = daily_df['vi_std'].astype(np.float32)
    daily_df['vi_daily'] = daily_df['vi_raw'].copy()   # float32; NaN on non-obs days

    n_obs = int((daily_df['vi_count'] > 0).sum())
    n_total = len(daily_df)
    logger.debug(
        "reindex_to_daily: %d obs days → %d daily rows (%d gap days)",
        n_obs, n_total, n_total - n_obs,
    )

    daily_df.index.name = 'date'
    return daily_df.reset_index()


# ---------------------------------------------------------------------------
# Region enumeration
# ---------------------------------------------------------------------------

def enumerate_regions(config: PhenologyConfig) -> list:
    """Return a flat list of (region_label, roi_gdf) pairs for all configured shapefiles.

    When config.shapefiles is None: returns [('full_extent', None)].
    When shapefiles are set: expands each shapefile into its constituent regions —
    one per unique field value when shapefile_field is set, one dissolved region otherwise.

    Validates that each shapefile path exists and that any configured field is present.
    Calls sys.exit(1) on the first error encountered (same behaviour as extract_timeseries).

    Args:
        config: PhenologyConfig with shapefiles, shapefile_field, etc.

    Returns:
        Ordered list of (region_label: str, roi_gdf: Optional[GeoDataFrame]) pairs.
    """
    if not config.shapefiles:
        return [('full_extent', None)]

    regions = []
    for sf_index, shapefile in enumerate(config.shapefiles):
        if not shapefile.exists():
            logger.error("Shapefile not found: %s", shapefile)
            sys.exit(1)

        field = config.field_for_shapefile(sf_index)
        try:
            region_pairs = load_shapefile_regions(shapefile, field)
        except ValueError as e:
            logger.error("%s", e)
            sys.exit(1)

        logger.info(
            "Shapefile '%s': %d region(s)%s",
            shapefile.name, len(region_pairs),
            f" (split by field '{field}')" if field else " (dissolved)",
        )
        for region_label, _ in region_pairs:
            config.register_region(region_label, shapefile.stem)
        regions.extend(region_pairs)

    return regions


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def extract_timeseries(config: PhenologyConfig) -> dict:
    """Main entry point for Layers 0 + 1.

    Iterates over all (VI, shapefile) combinations defined in config.
    Discovers relevant NetCDF files, loads and clips to ROI (if provided),
    aggregates spatially, and reindexes to daily.

    Returns:
        dict keyed by (vi, region_label) → pd.DataFrame with columns:
            date (datetime64[ns])
            vi_raw   (float32) — Layer 0: NaN on non-obs days
            vi_count (int32)   — Layer 0: 0 on non-obs days
            vi_std   (float32) — Layer 0: NaN on non-obs days
            vi_daily (float32) — Layer 1: same as vi_raw (explicit daily column)
    """
    result = {}
    shapefiles = config.shapefiles if config.shapefiles else [None]

    for vi in config.vi_list:
        nc_paths = discover_netcdfs(config.netcdf_dir, vi)
        if not nc_paths:
            logger.warning("Skipping %s — no matching NetCDF files found in %s", vi, config.netcdf_dir)
            continue

        vmin, vmax = config.valid_range_for(vi)
        logger.debug("%s valid range: [%.4f, %.4f]", vi, vmin, vmax)

        for sf_index, shapefile in enumerate(shapefiles):
            if shapefile is not None:
                if not shapefile.exists():
                    logger.error("Shapefile not found: %s", shapefile)
                    sys.exit(1)
                field = config.field_for_shapefile(sf_index)
                try:
                    region_pairs = load_shapefile_regions(shapefile, field)
                except ValueError as e:
                    logger.error("%s", e)
                    sys.exit(1)
            else:
                region_pairs = [("full_extent", None)]

            for region_label, roi_gdf in region_pairs:
                logger.info(
                    "Processing %s / %s  (%d tile(s), valid range [%.4f, %.4f])",
                    vi, region_label, len(nc_paths), vmin, vmax,
                )

                obs_df = aggregate_across_tiles(
                    nc_paths, roi_gdf, vmin, vmax,
                    start_date=config.start_date,
                    end_date=config.end_date,
                    n_workers=config.n_workers,
                )
                if obs_df.empty:
                    logger.warning(
                        "%s / %s: no valid observations extracted — skipping this combination.",
                        vi, region_label,
                    )
                    continue

                daily_df = reindex_to_daily(obs_df)
                result[(vi, region_label)] = daily_df

    return result
