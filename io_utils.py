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

def save_parquet(raw: dict, smoothed: Optional[dict], config: PhenologyConfig):
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
    """
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
