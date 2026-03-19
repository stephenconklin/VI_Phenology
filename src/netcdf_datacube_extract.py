#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# netcdf_datacube_extract.py
# Extract per-pixel VI time series as CF-1.8 compliant netCDF datacubes.
#
# For each (VI, polygon region) combination, all overlapping HLS MGRS tiles are
# clipped to the polygon boundary and merged into a single datacube per region.
# Valid-range masking is applied before output (out-of-range pixels → NaN).
#
# Tile merge strategy (controlled by --no-merge-same-crs / --no-merge-cross-crs):
#
#   Same CRS, merge enabled (default):
#     Adjacent HLS MGRS tiles in the same UTM zone share an identical 30-m pixel
#     grid — they are aligned at the pixel level. Tiles are mosaiced via a
#     direct netCDF4-python write loop (one HDF5 chunk per time step), keeping
#     peak memory bounded regardless of datacube size. No resampling — pixel
#     values are unmodified. Output: one CF-1.8 netCDF per region.
#
#   Cross-CRS, merge enabled (default):
#     Tiles spanning different UTM zones cannot share a pixel grid. Minority tiles
#     (those covering less of the polygon) are reprojected to the dominant CRS
#     (the CRS covering the most pixels) using bilinear resampling, then mosaiced.
#     Bilinear reprojection between adjacent UTM zones introduces sub-pixel mixing
#     comparable to the sensor point spread function — scientifically acceptable
#     for VI analysis at 30 m. The target CRS and resampling method are recorded
#     in the output file's global attributes.
#     Output: one CF-1.8 netCDF per region.
#
#   Merge disabled (--no-merge-same-crs and/or --no-merge-cross-crs):
#     One file per tile per region, each in its native UTM CRS. No reprojection.
#     Output: {VI}_{region_label}_{tile_id}_datacube.nc per tile.
#
# Processing model:
#   Phase 1 (parallel): each tile is clipped and written to a temp netCDF.
#   Phase 2 (main process): temp files are merged and final datacube(s) written.
#   Cleanup: temp files are removed regardless of Phase 2 success or failure.
#
# Author:  Stephen Conklin <stephenconklin@gmail.com>
# License: MIT

import argparse
import logging
import shutil
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

import geopandas as gpd
import netCDF4 as nc4
import numpy as np
import pandas as pd
import xarray as xr
import rioxarray  # noqa: F401 — activates .rio accessor

from io_utils import (
    discover_netcdfs_for_vi,
    sanitize_label,
    load_shapefile_regions,
    parse_valid_range,
    read_netcdf_crs,
    setup_log_file,
)

logger = logging.getLogger(__name__)

# Number of time steps read from each temp file per iteration during Phase 2
# merge.  Bounds peak memory to roughly:
#   _PHASE2_READ_CHUNK × tile_ny × tile_nx × 4 bytes
# e.g. 20 × 994 × 2404 × 4 B ≈ 190 MB for the largest BioSCape tiles.
_PHASE2_READ_CHUNK = 20

try:
    from rioxarray.exceptions import NoDataInBounds
except ImportError:
    NoDataInBounds = Exception  # type: ignore[misc,assignment]


# ---------------------------------------------------------------------------
# CF attribute helper
# ---------------------------------------------------------------------------

def _apply_cf_attrs(
    ds_out: xr.Dataset,
    vi: str,
    region_label: str,
    tile_ids: list,
    vmin: float,
    vmax: float,
    reprojected: bool,
    target_crs_label: str = "",
) -> None:
    """Add CF-1.8 variable and global attributes to an output Dataset in-place."""
    # rioxarray stores grid_mapping in encoding; setting it in attrs too causes
    # xarray's CF encoder to raise "already exists in attrs".  Remove from
    # encoding first so our explicit attrs value is the sole source of truth.
    ds_out[vi].encoding.pop('grid_mapping', None)
    ds_out[vi].attrs.update({
        'long_name': f'{vi} vegetation index',
        'valid_min': float(vmin),
        'valid_max': float(vmax),
        'grid_mapping': 'spatial_ref',
    })
    history = (
        f"Created {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')} "
        f"by netcdf_datacube_extract.py; "
        f"clipped to region='{region_label}', "
        f"valid_range=[{vmin}, {vmax}]"
    )
    if reprojected:
        history += (
            f"; minority tiles reprojected to {target_crs_label} "
            "via bilinear resampling"
        )
    ds_out.attrs.update({
        'Conventions': 'CF-1.8',
        'history': history,
        'tiles': ', '.join(tile_ids),
        'region': region_label,
        'vi': vi,
    })
    if reprojected:
        ds_out.attrs['resampling_method'] = 'bilinear'
        ds_out.attrs['target_crs'] = target_crs_label


# ---------------------------------------------------------------------------
# Per-tile worker (module-level for multiprocessing picklability)
# ---------------------------------------------------------------------------

def _extract_datacube_one_tile(args: tuple) -> dict:
    """Clip one tile to the ROI, apply valid-range mask, write a temp netCDF.

    Must be at module top level (not nested) to be picklable by multiprocessing.

    Uses dask scheduler='synchronous' inside the worker to prevent nested
    thread pools from competing with other parallel workers for CPU cores.

    Args:
        args: Tuple of (nc_path, roi_gdf, vmin, vmax, start_date, end_date,
                        temp_path).

    Returns:
        dict with keys: tile_name, tile_id, status ('ok'|'skip'|'error'),
                        message, temp_path, n_times, shape (y, x).
    """
    import dask

    nc_path, roi_gdf, vmin, vmax, start_date, end_date, temp_path = args
    tile_id = nc_path.stem.rsplit('_', 1)[0]   # e.g. 'T34HBH'
    vi_name = nc_path.stem.rsplit('_', 1)[-1]  # e.g. 'NDVI'

    try:
        ds = xr.open_dataset(nc_path, chunks={})
        da = ds[vi_name]

        wkt = read_netcdf_crs(ds, nc_path.name)
        da = da.rio.write_crs(wkt)

        if roi_gdf is not None:
            roi_repr = roi_gdf.to_crs(da.rio.crs)
            try:
                da = da.rio.clip(roi_repr.geometry, all_touched=True, drop=True)
            except NoDataInBounds:
                return {
                    'tile_name': nc_path.name, 'tile_id': tile_id,
                    'status': 'skip',
                    'message': f"{nc_path.name}: no overlap with ROI",
                    'temp_path': None, 'n_times': 0, 'shape': (0, 0),
                }

        if start_date or end_date:
            da = da.sel(time=slice(start_date, end_date))
            if da.sizes['time'] == 0:
                return {
                    'tile_name': nc_path.name, 'tile_id': tile_id,
                    'status': 'skip',
                    'message': (
                        f"{nc_path.name}: no time steps within "
                        f"[{start_date or 'start'}, {end_date or 'end'}]"
                    ),
                    'temp_path': None, 'n_times': 0, 'shape': (0, 0),
                }

        # Apply valid-range mask; out-of-range pixels → NaN.
        da = da.where((da >= vmin) & (da <= vmax))

        # Deduplicate timestamps: multiple HLS sensors (Landsat 8/9,
        # Sentinel-2A/2B) can observe the same tile on the same calendar day.
        # The HLS time axis uses integer days (day-level precision only), so
        # both acquisitions share an identical midnight timestamp — there is no
        # sub-day information to distinguish them. combine_first in Phase 2
        # requires a strictly monotonic time index and will crash if duplicates
        # are present. For each duplicate group, merge pixel-by-pixel using
        # combine_first: first non-NaN value at each pixel wins. This preserves
        # valid data from all same-day acquisitions across the spatial footprint
        # without averaging or discarding any valid pixel values.
        if pd.DatetimeIndex(da.time.values).duplicated().any():
            groups: dict = {}
            for i, t in enumerate(da.time.values):
                groups.setdefault(t, []).append(i)
            merged_steps = []
            for t in sorted(groups.keys()):
                indices = groups[t]
                if len(indices) == 1:
                    merged_steps.append(da.isel(time=indices))  # list → keeps time dim
                else:
                    # Pixel-level spatial merge: first non-NaN wins.
                    # da.isel(time=int) drops time to a 2D (y, x) slice.
                    frame = da.isel(time=indices[0])
                    for idx in indices[1:]:
                        frame = frame.combine_first(da.isel(time=idx))
                    merged_steps.append(
                        frame.assign_coords(time=t).expand_dims('time')
                    )
            da = xr.concat(merged_steps, dim='time')

        # Write temp file preserving spatial_ref for CRS detection in Phase 2.
        ds_tmp = da.to_dataset(name=vi_name)
        ds_tmp['spatial_ref'] = ds['spatial_ref']

        temp_path.parent.mkdir(parents=True, exist_ok=True)
        with dask.config.set(scheduler='synchronous'):
            ds_tmp.to_netcdf(temp_path)

        n_y = da.sizes.get('y', 0)
        n_x = da.sizes.get('x', 0)
        n_t = da.sizes.get('time', 0)
        return {
            'tile_name': nc_path.name, 'tile_id': tile_id,
            'status': 'ok', 'temp_path': temp_path,
            'n_times': n_t, 'shape': (n_y, n_x),
        }

    except Exception as e:
        return {
            'tile_name': nc_path.name, 'tile_id': tile_id,
            'status': 'error',
            'message': f"{type(e).__name__}: {e}",
            'temp_path': None, 'n_times': 0, 'shape': (0, 0),
        }


# ---------------------------------------------------------------------------
# Bounding-box pre-filter (main process, before worker dispatch)
# ---------------------------------------------------------------------------

def _prefilter_tiles_by_bbox(
    nc_paths: list,
    roi_gdf: gpd.GeoDataFrame,
    vi: str,
) -> list:
    """Return only tiles whose spatial extent intersects the ROI bounding box.

    Reads just the first and last values of each tile's x/y coordinate arrays
    (4 scalar reads per file, no data decompression) to determine the tile
    bounding box, then tests against the ROI bounding box reprojected into the
    tile's native CRS.  Runs sequentially in the main process before workers
    are dispatched — far cheaper than dispatching all tiles and letting workers
    fail with NoDataInBounds.

    Tiles that cannot be opened or whose CRS cannot be read are included in the
    output as a safe fallback (they will be handled normally by the worker).

    Args:
        nc_paths: Full list of candidate tile paths.
        roi_gdf:  ROI GeoDataFrame (any CRS).
        vi:       VI variable name — dropped when opening so only coordinate
                  metadata is loaded.

    Returns:
        Filtered list of nc_paths that may overlap the ROI.
    """
    from pyproj import CRS as ProjCRS

    filtered = []
    n_skipped = 0

    for nc_path in nc_paths:
        try:
            # Open metadata only — skip the large data variable and sensor array.
            ds = xr.open_dataset(nc_path, chunks={}, drop_variables=[vi, 'sensor'])
            wkt = read_netcdf_crs(ds, nc_path.name)

            # Tile bounding box in native CRS (first/last coordinate values only).
            x_min = float(ds.x.values[0])
            x_max = float(ds.x.values[-1])
            y_min = min(float(ds.y.values[0]), float(ds.y.values[-1]))
            y_max = max(float(ds.y.values[0]), float(ds.y.values[-1]))
            ds.close()

            # Reproject ROI bounding box into tile CRS for comparison.
            crs_obj = ProjCRS.from_wkt(wkt)
            tile_crs = crs_obj.to_epsg(min_confidence=20) or wkt
            roi_repr = roi_gdf.to_crs(tile_crs)
            rx_min, ry_min, rx_max, ry_max = roi_repr.total_bounds

            if rx_max >= x_min and rx_min <= x_max and ry_max >= y_min and ry_min <= y_max:
                filtered.append(nc_path)
            else:
                n_skipped += 1
                logger.debug(
                    "  Pre-filter: %s — bbox no overlap with ROI, skipping",
                    nc_path.name,
                )

        except Exception:
            # On any error include the tile; the worker will handle it normally.
            filtered.append(nc_path)

    if n_skipped:
        logger.info(
            "  Pre-filter: %d/%d tile(s) intersect ROI bbox (%d skipped)",
            len(filtered), len(nc_paths), n_skipped,
        )

    return filtered


# ---------------------------------------------------------------------------
# Phase 1: parallel tile extraction to temp files
# ---------------------------------------------------------------------------

def _extract_tiles_to_temp(
    nc_paths: list,
    roi_gdf: Optional[gpd.GeoDataFrame],
    vi: str,
    region_label: str,
    vmin: float,
    vmax: float,
    output_dir: Path,
    start_date: Optional[str],
    end_date: Optional[str],
    n_workers: int,
) -> list:
    """Clip all overlapping tiles in parallel, writing each to a temp netCDF.

    Temp files are stored in {output_dir}/{region_label}/_tmp/.

    Returns a list of (tile_id, temp_path) for all successfully clipped tiles.
    """
    # Pre-filter tiles by bounding box before dispatching to workers.
    # Reads only x/y coordinate min/max from each file — no data decompression.
    # Tiles with no bbox overlap are excluded before any worker is spawned.
    if roi_gdf is not None:
        nc_paths = _prefilter_tiles_by_bbox(nc_paths, roi_gdf, vi)

    if not nc_paths:
        logger.warning("  [Phase 1] No tiles intersect ROI bbox — skipping dispatch")
        return []

    tmp_dir = output_dir / region_label / '_tmp'
    work_items = []
    for nc_path in nc_paths:
        tile_id = nc_path.stem.rsplit('_', 1)[0]
        temp_path = tmp_dir / f"{vi}_{tile_id}_clip.nc"
        work_items.append((nc_path, roi_gdf, vmin, vmax, start_date, end_date, temp_path))

    logger.info(
        "  [Phase 1] Dispatching %d tile(s) across %d worker(s)",
        len(work_items), min(n_workers, len(work_items)),
    )

    clipped = []
    with ProcessPoolExecutor(max_workers=min(n_workers, len(work_items))) as executor:
        futures = {executor.submit(_extract_datacube_one_tile, item): item
                   for item in work_items}
        for future in as_completed(futures):
            result = future.result()
            if result['status'] == 'ok':
                clipped.append((result['tile_id'], result['temp_path']))
                logger.info(
                    "  Tile %s: clipped (%d×%d px, %d time steps)",
                    result['tile_name'], *result['shape'], result['n_times'],
                )
            elif result['status'] == 'skip':
                logger.info("  %s", result['message'])
            else:
                logger.error("  Tile %s: %s", result['tile_name'], result['message'])

    return clipped


# ---------------------------------------------------------------------------
# Phase 2 helpers
# ---------------------------------------------------------------------------

def _write_mosaic_nc4(
    tile_write_infos: list,
    dominant_wkt: str,
    ref_y: float,
    ref_x: float,
    vi: str,
    region_label: str,
    out_path: Path,
    vmin: float,
    vmax: float,
    reprojected_any: bool,
    target_crs_label: str,
    all_tile_ids: list,
) -> tuple:
    """Write a merged multi-tile datacube via a direct netCDF4-python write loop.

    Replaces the former ``xarray.DataArray.combine_first + to_netcdf`` approach,
    which materialised the full spatial-union DataArray (potentially 30+ GB for
    large multi-tile regions) before writing a single byte.

    This function reads at most ``_PHASE2_READ_CHUNK`` time steps × one tile's
    spatial footprint at a time, keeping peak memory bounded to a few hundred MB
    regardless of datacube size or number of tiles.

    Mosaic semantics
    ----------------
    Tiles are written in order (``tile_write_infos[0]`` first).  Where two tiles
    share the same time step and spatial position (the ~163-pixel MGRS overlap
    zone), the **last-written tile wins**.  The former ``combine_first`` gave
    first-wins; for co-acquired HLS pixels in the overlap zone the values are
    effectively identical, so the distinction is not scientifically meaningful.

    HDF5 chunk layout
    -----------------
    ``chunksizes=(1, ny, nx)``: one complete 2-D spatial layer per HDF5 chunk.
    Downstream ``xr.open_dataset(chunks={})`` therefore produces one dask chunk
    per time step — the natural access pattern for both the phenology pipeline
    and per-time-step analysis.  For the first tile written, each HDF5 chunk
    starts empty (fill_value) and is written fresh (no read-modify-write
    overhead).  For subsequent tiles that share the same time step in the overlap
    zone, HDF5 reads the existing chunk, splices in the new spatial slice, and
    re-compresses — this is unavoidable but affects only the small overlap zone.

    Args:
        tile_write_infos : list of dicts, each containing:
            ``tile_id``        str   — MGRS tile identifier.
            ``da``             xr.DataArray — in dominant CRS (reprojected if
                               needed); used for coordinate mapping only.
            ``temp_path``      Path  — Phase 1 temp file, re-opened with
                               explicit time chunking for the actual data read.
            ``needs_reproject`` bool — whether to re-apply bilinear reprojection
                               when re-opening the temp file inside this function.
            ``src_wkt``        str  — original CRS WKT of the temp file
                               (needed to attach CRS before reprojection).
        dominant_wkt     : WKT of the output (dominant) CRS.
        ref_y, ref_x     : coordinate of the dominant tile's first y/x pixel,
                           used as the snapping reference for cross-CRS tiles.
        vi               : variable name (e.g. 'NDVI').
        region_label     : human-readable region name for CF attributes.
        out_path         : destination file path.
        vmin, vmax       : valid_range for CF variable attributes.
        reprojected_any  : True if any tile was bilinearly reprojected.
        target_crs_label : e.g. 'EPSG:32734'; written to global attrs when
                           ``reprojected_any`` is True.
        all_tile_ids     : source tile IDs in declaration order, for 'tiles'
                           global attribute.

    Returns:
        (n_t, n_y, n_x) — the dimension sizes of the file that was written.
    """
    import rasterio.enums
    from pyproj import CRS as ProjCRS

    # ── 1. Build union coordinate arrays ────────────────────────────────────
    # For same-CRS tiles: all coords are exact 30-m UTM multiples sharing an
    # identical grid — np.unique is lossless.
    # For cross-CRS tiles (post-reproject): bilinear reprojection can shift
    # pixel centres by a sub-metre amount relative to the dominant grid.  We
    # snap those coords to the nearest 30-m grid point (aligned to the dominant
    # tile's coordinate origin) so that the union grid is regular and both tiles
    # index cleanly into it without sub-pixel seams.

    def _snap(coords: np.ndarray, ref: float, res: float = 30.0) -> np.ndarray:
        """Round coords to the nearest multiple of res anchored at ref."""
        return np.round((coords - ref) / res) * res + ref

    all_y_raw = np.concatenate([info['da'].y.values for info in tile_write_infos])
    all_x_raw = np.concatenate([info['da'].x.values for info in tile_write_infos])
    all_t_raw = np.concatenate([info['da'].time.values for info in tile_write_infos])

    union_y = np.sort(np.unique(_snap(all_y_raw, ref_y)))[::-1]  # descending N→S
    union_x = np.sort(np.unique(_snap(all_x_raw, ref_x)))        # ascending  W→E
    union_t = np.sort(np.unique(all_t_raw))                       # chronological

    ny, nx, nt = len(union_y), len(union_x), len(union_t)

    # time → integer days since 1970-01-01 (CF-1.8 convention)
    epoch    = pd.Timestamp('1970-01-01')
    time_days = (pd.DatetimeIndex(union_t) - epoch).days.values.astype(np.int32)

    # ── 2. Resolve grid_mapping_name from the dominant CRS WKT ──────────────
    try:
        _grid_map = ProjCRS.from_wkt(dominant_wkt).to_cf().get(
            'grid_mapping_name', 'transverse_mercator'
        )
    except Exception:
        _grid_map = 'transverse_mercator'

    # ── 3. Create output netCDF4 file and write coordinates / metadata ───────
    # chunksizes=(1, ny, nx): one complete 2-D raster layer per HDF5 chunk.
    # This matches the read pattern of downstream xr.open_dataset(chunks={})
    # and eliminates partial-chunk writes for the first tile (fresh empty chunks
    # are cheaper to write than read-modify-write on existing chunks).
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with nc4.Dataset(str(out_path), 'w', format='NETCDF4') as dst:

        # Dimensions
        dst.createDimension('time', nt)
        dst.createDimension('y', ny)
        dst.createDimension('x', nx)

        # time variable
        tv = dst.createVariable('time', 'i4', ('time',))
        tv[:] = time_days
        tv.units     = 'days since 1970-01-01'
        tv.standard_name = 'time'
        tv.calendar  = 'proleptic_gregorian'
        tv.axis      = 'T'

        # y (northing)
        yv = dst.createVariable('y', 'f8', ('y',))
        yv[:] = union_y
        yv.units         = 'metre'
        yv.standard_name = 'projection_y_coordinate'
        yv.long_name     = 'Northing'
        yv.axis          = 'Y'

        # x (easting)
        xv = dst.createVariable('x', 'f8', ('x',))
        xv[:] = union_x
        xv.units         = 'metre'
        xv.standard_name = 'projection_x_coordinate'
        xv.long_name     = 'Easting'
        xv.axis          = 'X'

        # spatial_ref (CF-1.8 grid mapping variable)
        crv = dst.createVariable('spatial_ref', 'i4')
        crv[:] = np.int32(0)
        crv.crs_wkt          = dominant_wkt
        crv.spatial_ref      = dominant_wkt   # legacy GDAL / rioxarray compat
        crv.grid_mapping_name = _grid_map
        crv.long_name        = 'CRS definition'

        # VI data variable — zlib + complevel=4 matches current final-output
        # convention; fill_value=NaN used throughout the phenology pipeline.
        # chunksizes: one complete 2-D layer per HDF5 chunk (see docstring).
        vi_var = dst.createVariable(
            vi, 'f4', ('time', 'y', 'x'),
            zlib=True, complevel=4,
            fill_value=np.float32(np.nan),
            chunksizes=(1, ny, nx),
        )
        vi_var.long_name   = f'{vi} vegetation index'
        vi_var.valid_min   = float(vmin)
        vi_var.valid_max   = float(vmax)
        vi_var.grid_mapping = 'spatial_ref'

        # Global CF-1.8 attributes
        history = (
            f"Created {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')} "
            f"by netcdf_datacube_extract.py; "
            f"clipped to region='{region_label}', "
            f"valid_range=[{vmin}, {vmax}]"
        )
        if reprojected_any:
            history += (
                f"; minority tiles reprojected to {target_crs_label} "
                "via bilinear resampling"
            )
        dst.Conventions = 'CF-1.8'
        dst.history     = history
        dst.tiles       = ', '.join(all_tile_ids)
        dst.region      = region_label
        dst.vi          = vi
        if reprojected_any:
            dst.resampling_method = 'bilinear'
            dst.target_crs        = target_crs_label

        # ── 4. Write tile data — time-chunk by time-chunk ───────────────────
        #
        # For each tile we:
        #   a) Re-open the Phase 1 temp file with chunks={'time': _PHASE2_READ_CHUNK}
        #      so dask fetches exactly that many time steps per compute() call.
        #   b) Re-apply bilinear reprojection if required (lazy; only the
        #      current _PHASE2_READ_CHUNK steps are computed at once).
        #   c) Snap the tile's y/x coords to the union grid and locate its
        #      position with np.searchsorted — O(log n), exact for same-CRS
        #      tiles, snap-corrected for reprojected tiles.
        #   d) Locate each of the tile's time steps in union_t (searchsorted).
        #   e) Read _PHASE2_READ_CHUNK steps at a time (.values triggers compute),
        #      then write each step individually using integer + slice indexing —
        #      the simplest, most transparent write path supported by netCDF4-python.
        #
        # Peak memory per iteration ≈ _PHASE2_READ_CHUNK × tile_ny × tile_nx × 4 B.

        for info in tile_write_infos:
            tile_id      = info['tile_id']
            temp_path    = info['temp_path']
            needs_repr   = info['needs_reproject']
            src_wkt      = info['src_wkt']

            # Re-open with explicit time chunking (avoids loading entire temp
            # file when slicing — critical for large contiguous HDF5 layouts).
            with xr.open_dataset(temp_path,
                                  chunks={'time': _PHASE2_READ_CHUNK}) as ds_tile:
                da_tile = ds_tile[vi]

                if needs_repr:
                    da_tile = da_tile.rio.write_crs(src_wkt)
                    da_tile = da_tile.rio.reproject(
                        dominant_wkt,
                        resampling=rasterio.enums.Resampling.bilinear,
                    )

                # Snap tile coords to union grid and find integer positions.
                tile_y_snapped = _snap(da_tile.y.values, ref_y)
                tile_x_snapped = _snap(da_tile.x.values, ref_x)
                tile_times     = da_tile.time.values
                tile_ny        = len(tile_y_snapped)
                tile_nx        = len(tile_x_snapped)
                n_tile_t       = len(tile_times)

                # y is descending in union_y; negate both arrays so searchsorted
                # operates on an ascending sequence (correct for all UTM zones).
                y_idx = np.searchsorted(-union_y, -tile_y_snapped)
                x_idx = np.searchsorted(union_x,   tile_x_snapped)

                # Guard: indices must be contiguous for axis-aligned HLS tiles.
                # Non-contiguous indices indicate a grid-alignment bug and would
                # silently produce a corrupt mosaic — raise instead.
                if tile_ny > 1 and not np.all(np.diff(y_idx) == 1):
                    raise ValueError(
                        f"Tile {tile_id}: y-indices in union grid are non-contiguous "
                        f"(unique diffs: {np.unique(np.diff(y_idx)).tolist()}). "
                        "Expected contiguous for axis-aligned HLS tiles — "
                        "check coordinate snapping or CRS alignment."
                    )
                if tile_nx > 1 and not np.all(np.diff(x_idx) == 1):
                    raise ValueError(
                        f"Tile {tile_id}: x-indices in union grid are non-contiguous "
                        f"(unique diffs: {np.unique(np.diff(x_idx)).tolist()}). "
                        "Expected contiguous for axis-aligned HLS tiles — "
                        "check coordinate snapping or CRS alignment."
                    )

                y_start = int(y_idx[0])
                x_start = int(x_idx[0])
                y_end   = y_start + tile_ny
                x_end   = x_start + tile_nx

                # Locate each of this tile's time steps in the union time axis.
                t_global = np.searchsorted(union_t, tile_times)

                logger.info(
                    "    Writing tile %s → union y[%d:%d] x[%d:%d], %d steps",
                    tile_id, y_start, y_end, x_start, x_end, n_tile_t,
                )

                # Read and write in time chunks of _PHASE2_READ_CHUNK.
                # Sequential reads from the temp file are efficient on both
                # local and external drives; writing one step at a time keeps
                # the nc4 indexing simple and avoids numpy broadcast overhead.
                for t_start_local in range(0, n_tile_t, _PHASE2_READ_CHUNK):
                    t_end_local = min(t_start_local + _PHASE2_READ_CHUNK, n_tile_t)

                    # Triggers dask compute for exactly this time window.
                    chunk = da_tile.isel(
                        time=slice(t_start_local, t_end_local)
                    ).values  # shape: (chunk_t, tile_ny, tile_nx), dtype float32

                    for dt in range(t_end_local - t_start_local):
                        t_out = int(t_global[t_start_local + dt])
                        vi_var[t_out, y_start:y_end, x_start:x_end] = chunk[dt]

    return nt, ny, nx


# ---------------------------------------------------------------------------
# Phase 2: merge temp files and write final datacube(s)
# ---------------------------------------------------------------------------

def _merge_and_write_datacube(
    clipped_tiles: list,
    vi: str,
    region_label: str,
    output_dir: Path,
    merge_same_crs: bool,
    merge_cross_crs: bool,
    vmin: float,
    vmax: float,
) -> list:
    """Merge clipped temp files and write the final datacube netCDF file(s).

    Returns list of final output Paths written.

    Merge decisions:
      1 tile              → single file, native CRS (no merge step)
      N tiles, same CRS, merge_same_crs=True  → _write_mosaic_nc4, one file
      N tiles, mixed CRS, merge_cross_crs=True → reproject + _write_mosaic_nc4, one file
      otherwise           → one file per tile, native CRS, no reprojection

    Mosaic write (_write_mosaic_nc4):
      - Reads _PHASE2_READ_CHUNK time steps at a time per tile; memory-bounded
      - Time union: NaN where a tile has no acquisition on a given date
      - No resampling for same-CRS tiles (pixel-perfect alignment)
      - Last-written tile wins for the MGRS overlap zone (~163 px at 30 m);
        scientifically equivalent to first-wins for co-acquired HLS pixels
    """
    import dask
    import rasterio.enums
    from pyproj import CRS as ProjCRS

    out_region_dir = output_dir / region_label
    out_region_dir.mkdir(parents=True, exist_ok=True)

    # Open all temp files lazily and detect each tile's CRS.
    tile_info = []
    for tile_id, temp_path in clipped_tiles:
        ds = xr.open_dataset(temp_path, chunks={})
        da = ds[vi]
        wkt = read_netcdf_crs(ds, temp_path.name)
        da = da.rio.write_crs(wkt)
        # Normalise to an EPSG integer key for grouping.
        # HLS 2.0 GeoTIFFs often embed a non-standard datum name
        # ("Not specified (based on WGS 84 spheroid)") which causes
        # pyproj's default to_epsg() (min_confidence=70) to return None
        # even though the CRS is functionally EPSG:326xx.  Lowering the
        # confidence threshold to 20 reliably resolves these WKTs to their
        # correct EPSG integer so that same-UTM-zone tiles are always
        # grouped together.  Fall back to crs_obj.name (a short readable
        # label) if EPSG resolution fails — never use the raw WKT as a key
        # because identical CRS definitions from different sources can
        # produce non-equal WKT strings, causing false cross-CRS grouping.
        try:
            crs_obj = ProjCRS.from_wkt(wkt)
            epsg = crs_obj.to_epsg(min_confidence=20)
            crs_key = epsg if epsg is not None else crs_obj.name
        except Exception:
            crs_key = wkt
        tile_info.append({
            'tile_id': tile_id,
            'temp_path': temp_path,
            'da': da,
            'spatial_ref': ds['spatial_ref'],
            'crs_key': crs_key,
            'wkt': wkt,
        })

    # Group tiles by CRS.
    crs_groups: dict = {}
    for info in tile_info:
        crs_groups.setdefault(info['crs_key'], []).append(info)

    n_crs   = len(crs_groups)
    n_tiles = len(tile_info)

    logger.info(
        "  [Phase 2] %d tile(s) across %d CRS group(s): %s",
        n_tiles, n_crs,
        {k: [i['tile_id'] for i in v] for k, v in crs_groups.items()},
    )

    # Determine merge strategy.
    if n_tiles == 1:
        do_merge = True
        reason = "single tile"
    elif n_crs == 1 and merge_same_crs:
        do_merge = True
        crs_key_0 = list(crs_groups.keys())[0]
        crs_label = f"EPSG:{crs_key_0}" if isinstance(crs_key_0, int) else str(crs_key_0)
        reason = (
            f"all {n_tiles} tiles share the same CRS "
            f"({crs_label}) — "
            "direct nc4 write loop mosaic, no resampling"
        )
    elif n_crs > 1 and merge_cross_crs:
        do_merge = True
        reason = (
            f"tiles span {n_crs} CRS zones — "
            "reprojecting minority tiles via bilinear resampling, then merging"
        )
    else:
        do_merge = False
        reason = "per-tile output requested"
        if n_crs > 1:
            logger.warning(
                "  %s / %s: polygon spans %d CRS zones and --no-merge-cross-crs "
                "is set — writing per-tile files in native CRS",
                vi, region_label, n_crs,
            )

    logger.info(
        "  Merge strategy: %s (%s)", "merge" if do_merge else "per-tile", reason
    )

    # ── Per-tile output (no merge) ─────────────────────────────────────────
    if not do_merge:
        out_paths = []
        for info in tile_info:
            out_path = (
                out_region_dir / f"{vi}_{region_label}_{info['tile_id']}_datacube.nc"
            )
            ds_out = info['da'].to_dataset(name=vi)
            ds_out['spatial_ref'] = info['spatial_ref']
            _apply_cf_attrs(
                ds_out, vi, region_label,
                tile_ids=[info['tile_id']],
                vmin=vmin, vmax=vmax,
                reprojected=False,
            )
            with dask.config.set(scheduler='synchronous'):
                ds_out.to_netcdf(out_path, encoding={vi: {'zlib': True, 'complevel': 4}})
            n_y = info['da'].sizes.get('y', 0)
            n_x = info['da'].sizes.get('x', 0)
            n_t = info['da'].sizes.get('time', 0)
            logger.info(
                "  Saved per-tile datacube: %s  (%d time steps, %d×%d px)",
                out_path, n_t, n_y, n_x,
            )
            out_paths.append(out_path)
        return out_paths

    # ── Merged output ──────────────────────────────────────────────────────
    # Dominant CRS = the group whose tiles cover the most total pixels.
    dominant_crs_key = max(
        crs_groups,
        key=lambda k: sum(
            t['da'].sizes.get('y', 0) * t['da'].sizes.get('x', 0)
            for t in crs_groups[k]
        ),
    )
    dominant_info    = crs_groups[dominant_crs_key][0]
    dominant_wkt     = dominant_info['wkt']
    target_crs_label = (
        f"EPSG:{dominant_crs_key}"
        if isinstance(dominant_crs_key, int)
        else str(dominant_crs_key)[:60]
    )

    # Coordinate origin for snapping reprojected tiles to the dominant grid.
    # For same-CRS tiles snapping is a mathematical no-op (all coords are
    # already exact 30-m multiples sharing the same UTM origin).
    ref_y = float(dominant_info['da'].y.values[0])
    ref_x = float(dominant_info['da'].x.values[0])

    # Build tile_write_infos — one entry per tile in mosaic order.
    # For cross-CRS minority tiles, reproject lazily here to obtain the
    # output coordinate arrays needed for union-grid computation.  The
    # reprojection is re-applied (with explicit time chunking) inside
    # _write_mosaic_nc4 for the actual data write.
    tile_write_infos: list = []
    reprojected_tile_ids: list = []
    all_tile_ids: list = []

    for crs_key, group in crs_groups.items():
        for info in group:
            all_tile_ids.append(info['tile_id'])
            needs_repr = (crs_key != dominant_crs_key)
            if needs_repr:
                logger.info(
                    "  Reprojecting tile %s (CRS %s → %s) via bilinear resampling",
                    info['tile_id'], info['crs_key'], target_crs_label,
                )
                # Lazy reproject — only coordinate arrays (.y/.x) are read
                # here; VI data is not computed until the write loop below.
                da_for_coords = info['da'].rio.reproject(
                    dominant_wkt,
                    resampling=rasterio.enums.Resampling.bilinear,
                )
                reprojected_tile_ids.append(info['tile_id'])
            else:
                da_for_coords = info['da']

            tile_write_infos.append({
                'tile_id':        info['tile_id'],
                'da':             da_for_coords,   # coordinates only; data read inside helper
                'temp_path':      info['temp_path'],
                'needs_reproject': needs_repr,
                'src_wkt':        info['wkt'],      # original CRS for re-open + reproject
            })

    reprojected_any = len(reprojected_tile_ids) > 0

    out_path = out_region_dir / f"{vi}_{region_label}_datacube.nc"

    logger.info(
        "  Writing mosaic: %d tile(s) → %s",
        len(tile_write_infos), out_path.name,
    )
    n_t, n_y, n_x = _write_mosaic_nc4(
        tile_write_infos = tile_write_infos,
        dominant_wkt     = dominant_wkt,
        ref_y            = ref_y,
        ref_x            = ref_x,
        vi               = vi,
        region_label     = region_label,
        out_path         = out_path,
        vmin             = vmin,
        vmax             = vmax,
        reprojected_any  = reprojected_any,
        target_crs_label = target_crs_label if reprojected_any else "",
        all_tile_ids     = all_tile_ids,
    )

    repr_note = (
        f", tiles {reprojected_tile_ids} bilinearly reprojected to {target_crs_label}"
        if reprojected_any else ""
    )
    logger.info(
        "  Saved merged datacube: %s  (%d time steps, %d×%d px%s)",
        out_path, n_t, n_y, n_x, repr_note,
    )
    return [out_path]


# ---------------------------------------------------------------------------
# Temp file cleanup
# ---------------------------------------------------------------------------

def _cleanup_temps(output_dir: Path, region_label: str) -> None:
    """Delete the _tmp directory for a region after Phase 2 completes."""
    tmp_dir = output_dir / region_label / '_tmp'
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
        logger.debug("  Cleaned up temp directory: %s", tmp_dir)


# ---------------------------------------------------------------------------
# Main extraction entry point (one VI × region)
# ---------------------------------------------------------------------------

def extract_datacubes(
    nc_paths: list,
    roi_gdf: Optional[gpd.GeoDataFrame],
    region_label: str,
    vi: str,
    vmin: float,
    vmax: float,
    output_dir: Path,
    start_date: Optional[str],
    end_date: Optional[str],
    n_workers: int,
    merge_same_crs: bool,
    merge_cross_crs: bool,
) -> int:
    """Two-phase extraction + merge for one (VI, region) combination.

    Phase 1 (parallel): each overlapping tile is clipped to the ROI and
    written to a temp netCDF in {output_dir}/{region_label}/_tmp/.

    Phase 2 (main process): temp files are merged per the configured strategy
    and final datacube file(s) are written.

    Temp files are deleted on completion or on error (try/finally).

    Returns the number of final datacube files written.
    """
    clipped = _extract_tiles_to_temp(
        nc_paths=nc_paths,
        roi_gdf=roi_gdf,
        vi=vi,
        region_label=region_label,
        vmin=vmin,
        vmax=vmax,
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        n_workers=n_workers,
    )

    if not clipped:
        logger.warning(
            "  %s / %s: no tiles contributed data — skipping merge", vi, region_label
        )
        return 0

    try:
        out_paths = _merge_and_write_datacube(
            clipped_tiles=clipped,
            vi=vi,
            region_label=region_label,
            output_dir=output_dir,
            merge_same_crs=merge_same_crs,
            merge_cross_crs=merge_cross_crs,
            vmin=vmin,
            vmax=vmax,
        )
    finally:
        _cleanup_temps(output_dir, region_label)

    return len(out_paths)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "netCDF Datacube Extract: produce CF-1.8 compliant per-pixel VI "
            "datacubes clipped to polygon regions. "
            "Outputs one netCDF per region by default (tiles merged); "
            "use --no-merge-* flags to keep per-tile files instead."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--netcdf-dir", required=True,
        help="Directory containing VI NetCDF files (T{TILE}_{VI}.nc)",
    )
    parser.add_argument(
        "--vi", nargs="+", default=["NDVI"],
        choices=["NDVI", "EVI2", "NIRv"],
        help="Vegetation indices to extract",
    )
    parser.add_argument(
        "--shapefile", nargs="+", default=None,
        help="Polygon shapefile(s) defining extraction boundaries",
    )
    parser.add_argument(
        "--shapefile-field", nargs="+", default=None, metavar="FIELDNAME",
        help=(
            "Attribute field(s) to split shapefile(s) by. One value per shapefile "
            "in positional order as --shapefile. Use 'none' to dissolve. "
            "Count must match --shapefile exactly."
        ),
    )
    parser.add_argument("--valid-range-ndvi", default="-1,1",   metavar="MIN,MAX",
                        help="Valid range for NDVI")
    parser.add_argument("--valid-range-evi2", default="-1,2",   metavar="MIN,MAX",
                        help="Valid range for EVI2")
    parser.add_argument("--valid-range-nirv", default="-0.5,1", metavar="MIN,MAX",
                        help="Valid range for NIRv")
    parser.add_argument(
        "--output-dir", required=True,
        help="Root directory for output netCDF files",
    )
    parser.add_argument(
        "--workers", type=int, default=8, metavar="N",
        help="Parallel worker processes for tile extraction (Phase 1)",
    )
    parser.add_argument("--start-date", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--end-date",   default=None, metavar="YYYY-MM-DD")

    # Merge options
    parser.add_argument(
        "--no-merge-same-crs", action="store_true",
        help=(
            "Do not merge tiles that share the same CRS. "
            "Writes one file per tile in the tile's native UTM CRS — "
            "no resampling, no spatial modification. "
            "Default: merge all same-CRS tiles into one datacube per region."
        ),
    )
    parser.add_argument(
        "--no-merge-cross-crs", action="store_true",
        help=(
            "Do not reproject or merge tiles that span different UTM zones. "
            "Writes one file per tile in each tile's native CRS. "
            "Default: reproject minority tiles to the dominant CRS via bilinear "
            "resampling, then merge into one datacube per region."
        ),
    )

    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    parser.add_argument(
        "--no-logfile", action="store_true",
        help="Disable automatic log file in --output-dir",
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

    if not args.no_logfile:
        setup_log_file(output_dir, "netcdf_datacube", args.log_level)

    netcdf_dir = Path(args.netcdf_dir)
    if not netcdf_dir.exists():
        logger.error("NetCDF directory not found: %s", netcdf_dir)
        sys.exit(1)

    valid_ranges = {
        "NDVI": parse_valid_range(args.valid_range_ndvi, "NDVI"),
        "EVI2": parse_valid_range(args.valid_range_evi2, "EVI2"),
        "NIRv":  parse_valid_range(args.valid_range_nirv,  "NIRv"),
    }

    shapefiles      = [Path(s) for s in args.shapefile] if args.shapefile else []
    shapefile_fields = args.shapefile_field or []
    if shapefile_fields and len(shapefile_fields) != len(shapefiles):
        logger.error(
            "--shapefile-field has %d value(s) but --shapefile has %d path(s) — "
            "counts must match exactly",
            len(shapefile_fields), len(shapefiles),
        )
        sys.exit(1)

    merge_same_crs  = not args.no_merge_same_crs
    merge_cross_crs = not args.no_merge_cross_crs

    logger.info("netCDF Datacube Extract starting")
    logger.info("  NetCDF dir      : %s", netcdf_dir)
    logger.info("  VIs             : %s", ", ".join(args.vi))
    logger.info("  Output dir      : %s", output_dir)
    logger.info("  Workers         : %d", args.workers)
    logger.info("  Merge same-CRS  : %s", merge_same_crs)
    logger.info("  Merge cross-CRS : %s", merge_cross_crs)
    if args.start_date or args.end_date:
        logger.info(
            "  Date range      : %s → %s",
            args.start_date or "beginning", args.end_date or "end",
        )

    # Each entry: (region_label, roi_gdf, region_output_dir)
    # region_output_dir mirrors the phenology pipeline nesting:
    #   no shapefile  → output_dir/
    #   shapefile     → output_dir/{shapefile_stem}/{region_label}/
    if not shapefiles:
        regions = [('full_extent', None, output_dir)]
    else:
        regions = []
        for sf_idx, sf_path in enumerate(shapefiles):
            if not sf_path.exists():
                logger.error("Shapefile not found: %s", sf_path)
                sys.exit(1)
            field_raw = shapefile_fields[sf_idx] if shapefile_fields else None
            field = None if (field_raw is None or field_raw.lower() == 'none') else field_raw
            try:
                pairs = load_shapefile_regions(sf_path, field)
            except ValueError as e:
                logger.error("%s", e)
                sys.exit(1)
            logger.info(
                "Shapefile '%s': %d region(s)%s",
                sf_path.name, len(pairs),
                f" (split by field '{field}')" if field else " (dissolved)",
            )
            sf_output_dir = output_dir / sf_path.stem
            regions.extend(
                (label, gdf, sf_output_dir) for label, gdf in pairs
            )

    total_written = 0

    for vi in args.vi:
        nc_paths = discover_netcdfs_for_vi(netcdf_dir, vi)
        if not nc_paths:
            logger.warning("Skipping %s — no matching NetCDF files in %s", vi, netcdf_dir)
            continue

        vmin, vmax = valid_ranges[vi]

        for region_label, roi_gdf, region_output_dir in regions:
            logger.info(
                "══ %s / %s — %d tile(s), valid range [%.4f, %.4f] ══",
                vi, region_label, len(nc_paths), vmin, vmax,
            )
            n = extract_datacubes(
                nc_paths=nc_paths,
                roi_gdf=roi_gdf,
                region_label=region_label,
                vi=vi,
                vmin=vmin,
                vmax=vmax,
                output_dir=region_output_dir,
                start_date=args.start_date,
                end_date=args.end_date,
                n_workers=args.workers,
                merge_same_crs=merge_same_crs,
                merge_cross_crs=merge_cross_crs,
            )
            logger.info("  %s / %s: %d datacube file(s) written", vi, region_label, n)
            total_written += n

    if total_written == 0:
        logger.error(
            "No datacube files written. Check --netcdf-dir contains T*_{VI}.nc files "
            "and that any shapefile intersects the data extent."
        )
        sys.exit(1)

    logger.info(
        "Done. %d datacube file(s) written to: %s", total_written, output_dir
    )


if __name__ == "__main__":
    main()
