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
        args: Tuple of (nc_path, roi_gdf, vmin, vmax, start_date, end_date).

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
    nc_path, roi_gdf, vmin, vmax, start_date, end_date = args

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

        # Compute with synchronous scheduler — prevents nested thread pools inside
        # the worker process from competing with other parallel workers for cores.
        with dask.config.set(scheduler='synchronous'):
            tile_sum    = masked.sum(dim=['y', 'x'], skipna=True).values       # (time,)
            tile_count  = masked.count(dim=['y', 'x']).values                  # (time,)
            tile_sum_sq = (masked ** 2).sum(dim=['y', 'x'], skipna=True).values  # (time,)

        dates = pd.to_datetime(da['time'].values)

        date_stats: dict = {}
        n_valid_dates = 0
        total_pixels = 0
        for i, date in enumerate(dates):
            n = int(tile_count[i])
            date_stats[date] = [float(tile_sum[i]), float(tile_sum_sq[i]), n]
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
) -> pd.DataFrame:
    """Aggregate Layer 0 observations across multiple tiles for a single VI.

    Tiles are processed in parallel using concurrent.futures.ProcessPoolExecutor.
    Each worker processes one tile independently and returns a per-tile stats dict.
    The main process merges all per-tile dicts then computes the pooled mean and
    standard deviation — identical math to the former sequential implementation.

    Pooling is done via raw sum / sum-of-squares / count accumulators rather than
    averaging per-tile statistics, which ensures a correctly weighted pooled mean
    and unbiased pooled standard deviation.

    Args:
        nc_paths:   List of NetCDF Paths for a single VI.
        roi_gdf:    GeoDataFrame of the ROI, or None for full extent.
        vmin:       Minimum valid VI value (inclusive).
        vmax:       Maximum valid VI value (inclusive).
        start_date: Optional ISO-8601 date string (YYYY-MM-DD); only dates on or
                    after this value are processed. None = no lower bound.
        end_date:   Optional ISO-8601 date string (YYYY-MM-DD); only dates on or
                    before this value are processed. None = no upper bound.
        n_workers:  Number of parallel worker processes (default 4). Set to 1 to
                    process tiles sequentially (useful for debugging).

    Returns:
        pd.DataFrame with columns [date, vi_raw, vi_count, vi_std] (Layer 0).
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed

    work_items = [
        (nc_path, roi_gdf, vmin, vmax, start_date, end_date)
        for nc_path in nc_paths
    ]
    total = len(work_items)
    logger.info(
        "  Dispatching %d tile(s) across %d worker process(es)", total, min(n_workers, total)
    )

    # Accumulate per-date: date -> [sum, sum_of_squares, count]
    date_stats: dict = {}
    n_tiles_used = 0

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(_process_one_tile, item): item for item in work_items}
        for future in as_completed(futures):
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
