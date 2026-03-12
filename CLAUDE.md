# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commit Style

Do NOT include `Co-Authored-By: Claude` trailers in any commit messages for this project.

## Environment Setup

```bash
conda env create -f environment.yml
conda activate vi_phenology
```

## Two Pipelines — Overview

This repository contains two independent processing pipelines, selected via the `PIPELINE`
variable in `run_phenology.sh`:

| `PIPELINE` | Script | Purpose |
|---|---|---|
| `phenology` (default) | `src/vi_phenology.py` | ROI-mean time series, smoothing, metrics, plots |
| `netcdf_datacube` | `src/netcdf_datacube_extract.py` | Per-pixel CF-1.8 datacubes clipped to polygon regions |

Both pipelines share the same `--netcdf-dir`, `--vi`, `--shapefile`, `--shapefile-field`,
`--valid-range-*`, `--workers`, `--start-date`, `--end-date`, and `--no-logfile` inputs.

## Running the Tool

### Recommended — `run_phenology.sh`

Edit the variables at the top and run `./run_phenology.sh`. Inline comments document every option.

### Phenology pipeline (direct CLI)

```bash
python src/vi_phenology.py \
  --netcdf-dir /path/to/netcdfs \
  --vi NDVI EVI2 \
  --shapefile roi.gpkg \
  --shapefile-field Name \
  --valid-range-ndvi "-0.1,1.0" \
  --output-dir ./outputs \
  --smooth-method savgol \
  --smooth-window 15 \
  --smooth-polyorder 3 \
  --plot-style combined \
  --plot-format png html \
  --metrics \
  --workers 4 \
  --start-date 2022-01-01 \
  --end-date   2024-12-31
```

Use `python src/vi_phenology.py --help` for the full argument reference.

### netCDF datacube pipeline (direct CLI)

```bash
python src/netcdf_datacube_extract.py \
  --netcdf-dir /path/to/netcdfs \
  --vi NDVI EVI2 \
  --shapefile roi.gpkg \
  --shapefile-field Name \
  --valid-range-ndvi "-0.1,1.0" \
  --output-dir ./outputs \
  --workers 4 \
  --start-date 2022-01-01 \
  --end-date   2024-12-31
```

Use `python src/netcdf_datacube_extract.py --help` for the full argument reference.

## Implementation Status

All modules are **fully implemented**. The end-to-end pipeline runs successfully across
all layers (extraction → smoothing → metrics → plots). No stubs remain.

`netcdf_datacube_extract.py` is a standalone module — it does not depend on
`PhenologyConfig` or any other phenology-pipeline module.

## Project Purpose

### Phenology pipeline (`vi_phenology.py`)

Reads CF-1.8 compliant VI NetCDF time-series files and produces per-region
spatially aggregated time series:
- ROI-mean aggregated time series (Parquet + observations CSV)
- Smoothed gap-filled daily series
- Phenological metrics tables (CSV)
- Phenology plots (PNG static + HTML interactive)

### netCDF datacube pipeline (`netcdf_datacube_extract.py`)

Clips CF-1.8 VI NetCDF files to polygon boundaries and delivers per-pixel
CF-1.8 compliant datacubes — preserving the full spatial dimension for
downstream scientific analysis:
- One merged datacube per region (default) or one file per tile
- Same-CRS tiles: mosaiced without resampling (`combine_first`, first-wins for overlap zone)
- Cross-CRS tiles: minority tiles bilinearly reprojected to the dominant CRS before merging
- CF-1.8 global attributes: `Conventions`, `history`, `tiles`, `region`, `vi`,
  `target_crs`, `resampling_method` (when reprojection occurs)

## Architecture: Phenology Layered Output Design

Processing is structured as four sequential layers:

```
Layer 0  Raw aggregated observations (actual observation dates only)
   ↓
Layer 1  Reindexed to a complete daily DatetimeIndex (NaN on non-observation days)
   ↓
Layer 2  Gap-filled + smoothed daily series (continuous, no NaN gaps)
   ↓
Layer 3  Phenological metrics derived from Layer 2 smooth curve
```

Layers 0+1 are always computed. Layer 2 is skipped when `--smooth-method none`.
Layer 3 requires Layer 2 and `--metrics` flag.

## Architecture: netCDF Datacube Two-Phase Model

```
Phase 1 (parallel workers)
  For each tile:
    Open NetCDF → clip to ROI → apply valid-range mask → write temp netCDF
    (workers return only small status dicts — never large arrays across process boundary)
   ↓
Phase 2 (main process)
  Detect CRS per tile (pyproj.CRS.to_epsg() for normalized comparison)
  Group tiles by CRS
  Apply merge strategy:
    1 tile           → single file, native CRS
    N tiles same CRS, merge_same_crs=True  → combine_first mosaic, no resampling
    N tiles mixed CRS, merge_cross_crs=True → reproject minority tiles (bilinear) + combine_first
    otherwise        → one file per tile, native CRS
  Write final datacube(s)
   ↓
Cleanup (try/finally — runs even if Phase 2 raises)
  Delete {output_dir}/{region_label}/_tmp/
```

### Merge Strategy Details

| Condition | Behavior | Output |
|---|---|---|
| 1 tile | Direct file write, native CRS | 1 file |
| N tiles, same CRS, `MERGE_SAME_CRS=true` | `combine_first` mosaic, no resampling | 1 file |
| N tiles, mixed CRS, `MERGE_CROSS_CRS=true` | Bilinear reproject minority → `combine_first` | 1 file |
| `MERGE_SAME_CRS=false` | One file per tile, native CRS | N files |
| `MERGE_CROSS_CRS=false` | One file per tile, native CRS, no reprojection | N files |

Dominant CRS = the CRS group with the most total pixels (y × x) within the polygon.

## Pipeline Execution Model

### Phenology pipeline

`vi_phenology.py` processes one region at a time. For each region:
1. All configured VIs are extracted (Layers 0+1) → `region_raw` dict
2. Smoothing applied (Layer 2) → `region_smoothed` dict
3. Parquet written to disk (if `config.save_parquet`)
4. Observations CSV written to disk (if `config.save_observations_csv`)
5. Metrics computed and per-region CSV written to disk (if `config.compute_metrics`)
6. Plots generated and written to disk (if any plot toggle is enabled)
7. `region_raw` and `region_smoothed` go out of scope (GC-eligible)
8. Next region begins

Combined shapefile outputs (`write_combined_metrics`, `write_combined_parquet`,
`write_combined_observations_csv`) wait until all regions are complete.
These are gated by `config.save_combined_outputs` (for Parquet + observations CSV)
and `config.compute_metrics` (for the metrics CSV).

### Output Toggles (phenology pipeline only)

All output types are enabled by default. Disable individually via CLI flags or
`run_phenology.sh` variables:

| Config field | CLI flag | `run_phenology.sh` | Controls |
|---|---|---|---|
| `save_parquet` | `--no-parquet` | `SAVE_PARQUET=false` | Per-region Parquet time series |
| `save_observations_csv` | `--no-observations-csv` | `SAVE_OBSERVATIONS_CSV=false` | Per-region observations CSV |
| `save_combined_outputs` | `--no-combined-outputs` | `SAVE_COMBINED_OUTPUTS=false` | Combined shapefile Parquet + observations CSV |
| `plot_annual` | `--no-plot-annual` | `PLOT_ANNUAL=false` | Annual DOY overlay plot |
| `plot_timeseries` | `--no-plot-timeseries` | `PLOT_TIMESERIES=false` | Full calendar time-series plot |
| `plot_anomaly` | `--no-plot-anomaly` | `PLOT_ANOMALY=false` | Anomaly (departure from mean) plot |
| `plot_multi_vi` | `--no-plot-multi-vi` | `PLOT_MULTI_VI=false` | Multi-VI comparison panel |

These are stored as boolean fields in `PhenologyConfig` (all default `True`).

## Inter-Module Data Contract

The primary data bus between phenology layers is a `dict` keyed by `(vi, region_label)` tuple,
where each value is a `pd.DataFrame`. Columns accumulate across layers:

| Column | Type | Added by | Notes |
|--------|------|----------|-------|
| `date` | datetime64[ns] | extract | Index column |
| `vi_raw` | float32 | extract (L0) | NaN on non-obs days |
| `vi_count` | int32 | extract (L0) | 0 on non-obs days |
| `vi_std` | float32 | extract (L0) | NaN on non-obs days |
| `vi_daily` | float32 | extract (L1) | Same as vi_raw; explicit daily column |
| `vi_smooth` | float32 | smooth (L2) | Absent if smooth_method='none' |
| `vi_smooth_flag` | str | smooth (L2) | `observed`\|`interpolated`\|`extrapolated` |

`smooth_timeseries()` returns a **new dict** with the same keys; DataFrames are extended
copies (not in-place mutation). `generate_plots()` receives both `raw` and `smoothed`
dicts; `smoothed` may be `None` when smooth_method is 'none'.

## Module Responsibilities

| Module | Role |
|--------|------|
| `vi_phenology.py` | CLI entrypoint (argparse); streaming per-region phenology pipeline orchestration |
| `phenology_config.py` | `PhenologyConfig` dataclass; built from parsed CLI args |
| `extract.py` | Layers 0+1 — NetCDF discovery, region enumeration, spatial masking, ROI aggregation, daily reindex |
| `smooth.py` | Layer 2 — gap-fill and smooth; smoothing-on-obs-dates-first strategy |
| `metrics.py` | Layer 3 — SOS, POS, EOS, LOS, IVI, greening/senescence rates |
| `plot.py` | Matplotlib static (PNG) + Plotly interactive (HTML) phenology plots |
| `io_utils.py` | Shared utilities: Parquet I/O, NetCDF file discovery, `sanitize_label`, `load_shapefile_regions`, `parse_valid_range`, `read_netcdf_crs`, `setup_log_file` |
| `netcdf_datacube_extract.py` | Standalone CLI: per-pixel CF-1.8 datacube extraction with two-phase parallel tile processing and CRS-aware merge |

## Key Design Decisions

### Smoothing: Obs-First Strategy
Do NOT apply smoothing directly to a NaN-filled daily series — this causes artifacts at
gap boundaries. Instead:
1. Apply smoothing to raw observation dates only (irregular spacing is fine for LOESS;
   bin to observation-density grid for S-G)
2. Interpolate the smoothed-but-sparse result to a complete daily axis
3. Tag each value with a provenance flag: `observed` | `interpolated` | `extrapolated`

### Savitzky-Golay Binning
S-G requires uniformly spaced input. Bin observations to the **median inter-observation
spacing** before applying the filter, then interpolate the filtered result back to the
full daily axis. Do not use a fixed 1-day grid for binning — observation density varies.

### Harmonic Fit Formula
`VI(t) = a0 + Σ_k [ a_k * cos(2π k t / T) + b_k * sin(2π k t / T) ]`
where `T = 365.25` days, `t` = day-of-year. Default `n_harmonics = 3`.

### Smoothing Methods (`--smooth-method`)
| Flag | Method | Notes |
|------|--------|-------|
| `savgol` | Savitzky-Golay | Default; fast; `--smooth-window` + `--smooth-polyorder` |
| `loess` | LOESS/LOWESS | Adaptive to irregular spacing; `--smooth-window` |
| `linear` | Linear interpolation | No smoothing, just gap-fill |
| `harmonic` | Fourier/harmonic fit | Best for multi-year trend decomposition |
| `none` | Skip Layer 2 | Only Layers 0+1 produced |

### Parquet Schema
One file per `(vi, region_label)`. See Inter-Module Data Contract above for column definitions.

### Phenological Metrics (Layer 3)
Per year per region per VI:
- **SOS** — Start of Season: VI crosses `baseline + sos_threshold * amplitude` going up
- **POS** — Peak of Season: date + value of annual maximum
- **EOS** — End of Season: VI drops back through same threshold
- **LOS** — Length of Season: EOS − SOS (days)
- **IVI** — Integrated VI: trapezoidal area under curve between SOS and EOS
- **Greening rate** — `(VI_pos - VI_sos) / (pos_date - sos_date).days`
- **Senescence rate** — `(VI_eos - VI_pos) / (eos_date - pos_date).days` (negative for declining)

`baseline = annual minimum; amplitude = peak - baseline`

`compute_metrics()` returns the full `pd.DataFrame` of all rows. `vi_phenology.py` captures
this and passes it to `write_combined_metrics()`, which writes `{VI}_{shapefile_stem}_metrics.csv`
at the root of `output_dir` when `--shapefile-field` is set. Per-region CSVs are always written
by `compute_metrics()` regardless. Dissolved shapefiles (field value `'none'`) are skipped by
`write_combined_metrics()`.

### Valid Range Application
Apply `--valid-range-{vi}` at extraction time (Layer 0 for phenology; Phase 1 worker for datacube),
before spatial aggregation or output. Pixels outside `[vmin, vmax]` → NaN.

### Multi-Tile Handling — Phenology Pipeline
If a shapefile spans multiple MGRS tiles, pool all valid pixels from all overlapping tiles
**per observation date** before computing the spatial mean (not concatenate-then-average).
Read CRS from `spatial_ref` variable WKT (CF-1.8 grid mapping convention).

### Multi-Tile Handling — Datacube Pipeline
Phase 2 reads CRS from each temp file's `spatial_ref` variable WKT, normalizes to EPSG
integers using `pyproj.CRS.from_wkt(wkt).to_epsg()` (more robust than WKT string comparison),
and groups tiles by CRS. The merge strategy is then applied as described in the Two-Phase
Model section above.

For same-CRS merges: adjacent HLS MGRS tiles in the same UTM zone share an **identical 30-m
pixel grid** — no resampling is needed. `combine_first` is first-wins for the ~163-pixel
MGRS overlap zone and provides time union across all tile acquisition dates.

For cross-CRS merges: bilinear reprojection between adjacent UTM zones introduces sub-pixel
mixing comparable to the sensor point spread function — scientifically acceptable for VI
analysis at 30 m. The target CRS and `resampling_method='bilinear'` are written to the
output file's global attributes.

### Annual Windows
`--year-start-doy` (default 1 = Jan 1) allows non-calendar-year seasons. Set it to the
**VI minimum**, not the peak — the correct value depends on biome and rainfall regime:
- Northern Hemisphere temperate / Cape fynbos (winter-rainfall): `1` (Jan 1, summer drought)
- Southern Hemisphere summer-rainfall (Savanna, Highveld): `182` (Jul 1, austral winter)

`year_start_doy` only affects `split_by_year()` in `metrics.py`. It does NOT affect the
annual phenology plot, which always groups by calendar year and displays DOY 1–365 (Jan–Dec).

### Tile-Level Parallelism
Both pipelines use `concurrent.futures.ProcessPoolExecutor` (`--workers N`, default 8).
Worker functions (`_process_one_tile()` in `extract.py`, `_extract_datacube_one_tile()` in
`netcdf_datacube_extract.py`) **must** remain at module top level (not nested) to be
picklable by multiprocessing on all platforms.

Inside each worker, dask computation is wrapped in `with dask.config.set(scheduler='synchronous'):`.
This is critical — without it, each worker spawns its own dask thread pool, causing all
workers to compete for the same cores and eliminating the parallelism benefit.

### Memory-Safe NetCDF Chunking
All `xr.open_dataset()` calls use `chunks={}` to use the file's native chunk layout.
With `dask.config.set(scheduler='synchronous')`, dask processes one native chunk at a time
sequentially, keeping peak memory bounded at the native chunk size × the spatial footprint.

Do NOT use `chunks={'time': 1}` — if the file's native time chunks are larger than 1,
xarray must decompress the full native chunk just to extract a single time step, causing
severe I/O amplification.

### Datacube Temp File Strategy
Phase 1 workers write to `{output_dir}/{region_label}/_tmp/{VI}_{tile_id}_clip.nc`.
Workers return only small status dicts — they never return DataArrays across the process
boundary. This keeps inter-process communication cheap regardless of datacube size.
Phase 2 opens temp files lazily (`chunks={}`), performs the merge, and writes the final
output. `_cleanup_temps()` is called in a `try/finally` block to guarantee temp directory
removal even if Phase 2 raises an exception.

## Key Implementation Notes

### `rioxarray` Side-Effect Import
Both `extract.py` and `netcdf_datacube_extract.py` import `rioxarray` as `# noqa: F401`
to activate the `.rio` accessor on xarray objects. This import must be present even though
`rioxarray` is never referenced directly by name — without it, `.rio.clip()`,
`.rio.write_crs()`, and `.rio.reproject()` do not exist.

### `matplotlib` Backend
`plot.py` calls `matplotlib.use("Agg")` at module level (before any other matplotlib
import) to force the non-interactive PNG backend. Do not move or remove this call.

### NetCDF Discovery — Canonical Location
`io_utils.discover_netcdfs_for_vi()` is the canonical implementation for finding
`T{TILE}_{VI}.nc` files. Both pipelines use it: `discover_netcdfs()` in `extract.py`
delegates to it; `netcdf_datacube_extract.py` imports and calls it directly.

### Supported VIs
`--vi` choices are hard-coded in both CLI entry points (`choices=["NDVI", "EVI2", "NIRv"]`).
Adding a new VI requires updating that list in both files, adding a `--valid-range-{vi}`
argument, and adding its entry to the `valid_ranges` dict in `main()` of each.

### `--shapefile` and `--shapefile-field`
`--shapefile` is `nargs="+"` — multiple shapefiles produce independent outputs per file.

`--shapefile-field` is also `nargs="+"` and accepts one field name per shapefile, in the
same positional order as `--shapefile`. The count must match exactly — a mismatch raises
a hard error. Use the special value `none` (case-insensitive) to dissolve a specific
shapefile instead of splitting it:

```
--shapefile flights.shp tiles.geojson --shapefile-field box_nr none
```

When `--shapefile-field` is omitted entirely, all shapefiles are dissolved.

In the phenology pipeline, field handling is in `PhenologyConfig.field_for_shapefile(index)`.
In the datacube pipeline, it is handled inline in `main()` and via `_load_regions()`.

Both pipelines sanitize field values via `_sanitize_label()` (spaces/special chars → underscores).
The phenology version lives in `extract.py`; the datacube version is inlined in
`netcdf_datacube_extract.py` to avoid coupling.

## Input NetCDF Format

Expected dimensions: `time` (decoded to datetime64[ns] by xarray), `y`, `x` (meters)
CRS stored in `spatial_ref` variable — check for `crs_wkt` attr first, then `spatial_ref` attr
One file per tile+VI: `T{TILE}_{VI}.nc` (HLS_VI_Pipeline naming convention)

## Output File Naming

### Phenology pipeline

| Output | Pattern | Location |
|--------|---------|----------|
| Parquet time series | `{VI}_{region_label}_timeseries.parquet` | per-region subdirectory |
| Combined shapefile Parquet | `{VI}_{shapefile_stem}_timeseries.parquet` | shapefile root folder |
| Observations CSV | `{VI}_{region_label}_observations.csv` | per-region subdirectory |
| Combined observations CSV | `{VI}_{shapefile_stem}_timeseries.csv` | shapefile root folder |
| Per-region metrics CSV | `{VI}_{region_label}_metrics.csv` | per-region subdirectory |
| Combined shapefile metrics CSV | `{VI}_{shapefile_stem}_metrics.csv` | shapefile root folder |
| Annual phenology plot | `{VI}_{region_label}_annual.{ext}` | per-region subdirectory |
| Full time-series plot | `{VI}_{region_label}_timeseries.{ext}` | per-region subdirectory |
| Anomaly plot | `{VI}_{region_label}_anomaly.{ext}` | per-region subdirectory |
| Multi-VI comparison | `{region_label}_multi_vi.{ext}` | per-region subdirectory |
| Log file | `vi_phenology_{YYYYMMDD_HHMMSS}.log` | `--output-dir` root |

### netCDF datacube pipeline

| Output | Pattern | Location |
|--------|---------|----------|
| Merged datacube | `{VI}_{region_label}_datacube.nc` | `{output_dir}/{shapefile_stem}/{region_label}/` |
| Per-tile datacube (no merge) | `{VI}_{region_label}_{tile_id}_datacube.nc` | `{output_dir}/{shapefile_stem}/{region_label}/` |
| Full-extent datacube (no shapefile) | `{VI}_full_extent_datacube.nc` | `{output_dir}/full_extent/` |
| Temp clip files | `{VI}_{tile_id}_clip.nc` | `{output_dir}/{shapefile_stem}/{region_label}/_tmp/` (deleted after Phase 2) |
| Log file | `netcdf_datacube_{YYYYMMDD_HHMMSS}.log` | `--output-dir` root |

`region_label` is determined as follows for both pipelines:

| Scenario | `region_label` |
|---|---|
| No shapefile | `full_extent` |
| Shapefile, no `--shapefile-field` | Shapefile filename stem |
| Shapefile + `--shapefile-field` | Sanitized field value (spaces/specials → `_`) |
