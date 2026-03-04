#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# io_utils.py
# Parquet and file I/O utilities for VI Phenology.
#
# Author:  Stephen Conklin <stephenconklin@gmail.com>
# License: MIT

import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from typing import Optional

from phenology_config import PhenologyConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# File discovery
# ---------------------------------------------------------------------------

def discover_netcdfs_for_vi(netcdf_dir: Path, vi: str) -> list:
    """Glob for T{TILE}_{VI}.nc files in netcdf_dir.

    Matches the HLS_VI_Pipeline naming convention: T{TILE}_{VI}.nc
    Example: T14SMJ_NDVI.nc

    Args:
        netcdf_dir: Directory to search.
        vi:         VI name (e.g. 'NDVI', 'EVI2', 'NIRv').

    Returns:
        Sorted list of Path objects. Empty list if none found (with a warning logged).
    """
    if not netcdf_dir.exists():
        raise FileNotFoundError(f"NetCDF directory not found: {netcdf_dir}")
    paths = sorted(netcdf_dir.glob(f"T*_{vi}.nc"))
    if paths:
        logger.debug(
            "discover_netcdfs_for_vi: found %d file(s) matching 'T*_%s.nc' in %s",
            len(paths), vi, netcdf_dir,
        )
    else:
        logger.warning(
            "discover_netcdfs_for_vi: no files matching 'T*_%s.nc' found in %s",
            vi, netcdf_dir,
        )
    return paths


# ---------------------------------------------------------------------------
# Parquet I/O
# ---------------------------------------------------------------------------

def save_parquet(raw: dict, smoothed: Optional[dict], config: PhenologyConfig) -> dict:
    """Merge Layer 1 (raw) and Layer 2 (smooth) data and save to Parquet.

    One file per (vi, region_label). Columns written:
        date           datetime64[ns]
        vi_raw         float32   — NaN on non-observation days
        vi_count       int32     — 0 on non-observation days
        vi_std         float32   — NaN on non-observation days
        vi_daily       float32   — Layer 1 daily series (NaN gaps preserved)
        vi_smooth      float32   — Layer 2 smooth (omitted if smoothed is None)
        vi_smooth_flag str       — provenance flag (omitted if smoothed is None)

    Output path: config.output_dir / '{vi}_{region_label}_timeseries.parquet'

    Args:
        raw:      dict from extract_timeseries()
        smoothed: dict from smooth_timeseries(), or None
        config:   PhenologyConfig

    Returns:
        dict keyed by (vi, shapefile_stem) → list[pd.DataFrame] for use by
        write_combined_parquet(). Each DataFrame has a leading 'region' column
        and contains the full daily series (all rows, including NaN gap days).
    """
    accumulated: dict = {}

    for (vi, region_label), df in raw.items():
        out_dir = config.output_dir_for(region_label)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_df = df.copy()
        if smoothed is not None and (vi, region_label) in smoothed:
            s_df = smoothed[(vi, region_label)]
            out_df = out_df.merge(
                s_df[['date', 'vi_smooth', 'vi_smooth_flag']], on='date', how='left'
            )
        filename = f"{vi}_{region_label}_timeseries.parquet"
        out_path = out_dir / filename
        out_df.to_parquet(out_path, index=False, engine='pyarrow')
        logger.info(
            "Saved Parquet: %s  (%d rows, columns: %s)",
            out_path, len(out_df), list(out_df.columns),
        )

        # Accumulate for combined Parquet — add region identifier column.
        row_df = out_df.copy()
        row_df.insert(0, 'region', region_label)
        shapefile_stem = config._region_shapefile_map.get(region_label, region_label)
        accumulated.setdefault((vi, shapefile_stem), []).append(row_df)

    return accumulated


def save_observations_csv(
    raw: dict, smoothed: Optional[dict], config: PhenologyConfig
) -> dict:
    """Save per-region observations-only CSV and return data for combined CSV assembly.

    Only rows where vi_count > 0 are written — actual HLS acquisition dates only.
    No gap-filled, interpolated, or extrapolated rows are included.

    Columns written:
        date       datetime      — observation date
        vi_raw     float32       — spatially averaged VI over ROI
        vi_count   int32         — number of valid pixels contributing to the mean
        vi_std     float32       — spatial standard deviation of VI over ROI
        vi_smooth  float32       — smooth curve value at this observation date
                                   (omitted when smoothed is None)

    Output path: config.output_dir_for(region_label) / '{vi}_{region_label}_observations.csv'

    Args:
        raw:      dict from extract_timeseries() — Layers 0+1
        smoothed: dict from smooth_timeseries() — Layer 2, or None
        config:   PhenologyConfig

    Returns:
        dict keyed by (vi, shapefile_stem) → list[pd.DataFrame] for use by
        write_combined_observations_csv(). Each DataFrame has a leading 'region' column.
    """
    accumulated: dict = {}

    for (vi, region_label), df in raw.items():
        obs = df[df['vi_count'] > 0].copy()
        cols = ['date', 'vi_raw', 'vi_count', 'vi_std']

        if smoothed is not None and (vi, region_label) in smoothed:
            s_df = smoothed[(vi, region_label)]
            obs = obs.merge(s_df[['date', 'vi_smooth']], on='date', how='left')
            cols.append('vi_smooth')

        out_dir = config.output_dir_for(region_label)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{vi}_{region_label}_observations.csv"
        obs[cols].to_csv(out_path, index=False, date_format='%Y-%m-%d')
        logger.info(
            "Saved observations CSV: %s  (%d rows)", out_path, len(obs)
        )

        # Accumulate for combined CSV — add region identifier column.
        row_df = obs[cols].copy()
        row_df.insert(0, 'region', region_label)
        shapefile_stem = config._region_shapefile_map.get(region_label, region_label)
        accumulated.setdefault((vi, shapefile_stem), []).append(row_df)

    return accumulated


def write_combined_observations_csv(all_obs: dict, config: PhenologyConfig) -> None:
    """Write a combined observations CSV per shapefile, stacking all regions.

    Produced only when a shapefile contributes more than one region (i.e. when
    --shapefile-field is active and yields multiple distinct field values).
    Skipped for full_extent runs and dissolved single-region shapefiles.

    Columns: region, date, vi_raw, vi_count, vi_std[, vi_smooth]
    Rows sorted by (region, date).

    Output path: config.output_dir / {shapefile_stem} / '{VI}_{shapefile_stem}_timeseries.csv'

    Args:
        all_obs: dict returned by (and accumulated across calls to) save_observations_csv()
        config:  PhenologyConfig
    """
    if not config.shapefiles:
        return

    for (vi, shapefile_stem), dfs in all_obs.items():
        if len(dfs) <= 1:
            # Single region — combined CSV would duplicate the per-region file.
            continue

        combined = (
            pd.concat(dfs, ignore_index=True)
            .sort_values(['region', 'date'])
            .reset_index(drop=True)
        )
        out_dir = config.output_dir / shapefile_stem
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{vi}_{shapefile_stem}_timeseries.csv"
        combined.to_csv(out_path, index=False, date_format='%Y-%m-%d')
        logger.info(
            "Saved combined observations CSV (%d region(s), %d rows): %s",
            combined['region'].nunique(), len(combined), out_path,
        )


def write_combined_parquet(all_parquet: dict, config: PhenologyConfig) -> None:
    """Write a combined full-daily-series Parquet per shapefile, stacking all regions.

    Produced only when a shapefile contributes more than one region (i.e. when
    --shapefile-field is active and yields multiple distinct field values).
    Skipped for full_extent runs and dissolved single-region shapefiles.

    Unlike the observations CSV, this file contains ALL daily rows including NaN
    gap days, vi_smooth, and vi_smooth_flag — the complete Layer 1 + Layer 2 record
    for all regions in one file.

    Columns: region, date, vi_raw, vi_count, vi_std, vi_daily[, vi_smooth, vi_smooth_flag]
    Rows sorted by (region, date).

    Output path: config.output_dir / {shapefile_stem} / '{VI}_{shapefile_stem}_timeseries.parquet'

    Args:
        all_parquet: dict returned by (and accumulated across calls to) save_parquet()
        config:      PhenologyConfig
    """
    if not config.shapefiles:
        return

    for (vi, shapefile_stem), dfs in all_parquet.items():
        if len(dfs) <= 1:
            # Single region — combined Parquet would duplicate the per-region file.
            continue

        combined = (
            pd.concat(dfs, ignore_index=True)
            .sort_values(['region', 'date'])
            .reset_index(drop=True)
        )
        out_dir = config.output_dir / shapefile_stem
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{vi}_{shapefile_stem}_timeseries.parquet"
        combined.to_parquet(out_path, index=False, engine='pyarrow')
        logger.info(
            "Saved combined Parquet (%d region(s), %d rows, columns: %s): %s",
            combined['region'].nunique(), len(combined), list(combined.columns), out_path,
        )


def load_parquet(parquet_path: Path) -> pd.DataFrame:
    """Load a previously saved phenology Parquet file.

    Returns a pd.DataFrame with the same schema as saved by save_parquet().
    The 'date' column is restored as datetime64[ns].
    """
    logger.debug("Loading Parquet: %s", parquet_path)
    df = pd.read_parquet(parquet_path, engine='pyarrow')
    df['date'] = pd.to_datetime(df['date'])
    logger.debug("Loaded %d rows from %s", len(df), parquet_path)
    return df


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def validate_parquet_schema(df: pd.DataFrame) -> bool:
    """Check that a loaded DataFrame has the expected phenology Parquet columns.

    Required columns: date, vi_raw, vi_count, vi_std, vi_daily
    Optional columns: vi_smooth, vi_smooth_flag

    Returns True if valid, False (with a logged warning) if not.
    """
    required = {'date', 'vi_raw', 'vi_count', 'vi_std', 'vi_daily'}
    missing = required - set(df.columns)
    if missing:
        logger.warning(
            "validate_parquet_schema: missing required columns: %s", sorted(missing)
        )
        return False
    logger.debug("validate_parquet_schema: schema OK (columns: %s)", list(df.columns))
    return True
