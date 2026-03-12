# VI Phenology Project Memory

## Project Status
All processing stubs implemented and end-to-end pipeline runs successfully.
Two fully implemented pipelines: phenology (vi_phenology.py) and netcdf_datacube (netcdf_datacube_extract.py).

## Two Pipelines
Selected via `PIPELINE` variable in `run_phenology.sh`:
- `PIPELINE="phenology"` → `src/vi_phenology.py` — ROI-mean time series, smoothing, metrics, plots
- `PIPELINE="netcdf_datacube"` → `src/netcdf_datacube_extract.py` — per-pixel CF-1.8 datacubes

Both share the same input config (NETCDF_DIR, VI, SHAPEFILE, SHAPEFILE_FIELD, VALID_RANGE_*, WORKERS, dates).

## Key File Roles
- `vi_phenology.py` / `phenology_config.py` — fully implemented CLI + config
- `extract.py` — Layers 0+1 (implemented)
- `smooth.py` — Layer 2 (implemented)
- `metrics.py` — Layer 3 (implemented)
- `plot.py` — PNG (matplotlib) + HTML (plotly) (implemented)
- `io_utils.py` — Shared utilities: Parquet I/O, NetCDF file discovery, sanitize_label,
  load_shapefile_regions, parse_valid_range, read_netcdf_crs, setup_log_file
- `netcdf_datacube_extract.py` — standalone CLI, two-phase parallel extraction, CF-1.8 output

## Key Features Added
- `--workers N`: tile-level parallelism via ProcessPoolExecutor (default 8)
- `--start-date`/`--end-date`: date range filtering applied at NetCDF level
- `--shapefile-field FIELDNAME`: split shapefile by attribute → one time series per unique value
- Per-region output subdirectories: `output_dir/{region_label}/`
- `chunks={}` on all xr.open_dataset calls → dask-backed lazy loading
- Streaming per-region loop (phenology): one region fully processed before next begins
- `--no-logfile`: timestamped log file written to OUTPUT_DIR by default
- 7 output toggles (phenology): SAVE_PARQUET, SAVE_OBSERVATIONS_CSV, SAVE_COMBINED_OUTPUTS,
  PLOT_ANNUAL, PLOT_TIMESERIES, PLOT_ANOMALY, PLOT_MULTI_VI (all default true in PhenologyConfig)
- `--mode per_pixel` stub removed from phenology pipeline (retired)
- `PIPELINE` selector in run_phenology.sh routes to either pipeline

## netCDF Datacube Pipeline Architecture
- Phase 1 (parallel): workers clip tiles to ROI → write temp netCDF to `{output_dir}/{region}/_tmp/`
  Workers return only small status dicts (not arrays) — no large data across process boundary
- Phase 2 (main process): detect CRS per tile using pyproj.CRS.to_epsg() (EPSG int comparison,
  more robust than WKT strings); group by CRS; apply merge strategy; write final datacube(s)
- Cleanup: try/finally guarantees _tmp/ deletion even if Phase 2 raises
- Merge same-CRS: combine_first mosaic, first-wins overlap zone (~163 px), time union
- Merge cross-CRS: bilinear reproject minority tiles to dominant CRS, then combine_first
- Dominant CRS = CRS group with most total pixels (y × x) in the polygon
- Output: `{VI}_{region}_datacube.nc` (merged) or `{VI}_{region}_{tile_id}_datacube.nc` (per-tile)
- CF-1.8 attrs: Conventions, history, tiles, region, vi, target_crs*, resampling_method* (*cross-CRS only)

## year_start_doy Guidance
Set to the VI MINIMUM, not peak. Depends on biome/rainfall:
- Cape fynbos (winter-rainfall, peak Jun–Aug, min Dec–Jan): `year_start_doy=1`
- Savanna/Highveld (summer-rainfall, peak Dec–Jan, min Jun–Jul): `year_start_doy=182`
- year_start_doy ONLY affects metrics windows (metrics.py split_by_year). Annual plot always shows calendar Jan–Dec.

## io_utils.py — Shared Utilities (consolidated 2026-03-12)
Five functions moved from extract.py, netcdf_datacube_extract.py, vi_phenology.py into io_utils.py:
- `sanitize_label(value)` — filesystem-safe region label; raises ValueError on collision (load_shapefile_regions)
- `load_shapefile_regions(path, field)` — dissolve or split shapefile; detects sanitized-label collisions
- `parse_valid_range(raw, vi)` — parse "min,max" CLI arg; sys.exit(1) on failure
- `read_netcdf_crs(ds, nc_name)` — reads WKT from spatial_ref var; checks crs_wkt then spatial_ref attr
- `setup_log_file(output_dir, prefix, log_level)` — attaches timestamped FileHandler to root logger

## netCDF Datacube: Duplicate Timestamp Handling (fixed 2026-03-12)
HLS time axis uses integer days (day-level precision only). Multiple sensors (Landsat 8/9,
Sentinel-2A/2B) observing the same tile on the same calendar day produce identical midnight
timestamps. combine_first in Phase 2 crashes on non-monotonic time indexes.
Fix (in _extract_datacube_one_tile, after valid-range mask):
- Detect duplicates via pd.DatetimeIndex(da.time.values).duplicated().any()
- For each duplicate group: pixel-level combine_first across all same-day frames (2D slices)
- First non-NaN value at each (y, x) wins — preserves all valid pixels from all acquisitions
- Result reassembled with xr.concat; fully lazy (no intermediate .compute())

## Known Issues (from code review 2026-03-03)
Critical:
1. smooth.py S-G window can exceed n_bins when data is sparse → crash in savgol_filter
2. metrics.py los_days typed as int but assigned np.nan — type inconsistency

## NetCDF Format (confirmed)
- CRS: check `ds['spatial_ref'].attrs.get('crs_wkt')` first, then `attrs.get('spatial_ref')`
- Time: decoded to datetime64[ns] by xarray (no manual decoding needed)
- VI var name: `nc_path.stem.rsplit('_', 1)[-1]` → e.g. 'NDVI'
- File naming: `T{TILE}_{VI}.nc` (e.g. T34HBH_NDVI.nc)

## Environment
- Conda env: `vi_phenology` (Python 3.11)
- NumPy 2.x: use `np.trapezoid` not `np.trapz`

## Pending Upstream Fix
See `memory/pending_upstream_fix.md` — fix needed in HLS_VI_Pipeline `src/03_hls_netcdf_build.py`
line 268 to write authority-tagged WKT so `pyproj.CRS.to_epsg()` resolves in netcdf_datacube_extract.py.

## Commit Style
No `Co-Authored-By: Claude` trailers.
