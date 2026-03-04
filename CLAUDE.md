# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commit Style

Do NOT include `Co-Authored-By: Claude` trailers in any commit messages for this project.

## Environment Setup

```bash
conda env create -f environment.yml
conda activate vi_phenology
```

## Running the Tool

```bash
python vi_phenology.py \
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

The recommended way to run is via `run_phenology.sh` — edit its variables and run
`./run_phenology.sh`. Use `python vi_phenology.py --help` for the full argument reference.

## Implementation Status

All modules are **fully implemented**. The end-to-end pipeline runs successfully across
all layers (extraction → smoothing → metrics → plots). No stubs remain.

## Project Purpose

Standalone phenology analysis tool that reads CF-1.8 compliant vegetation index (VI)
NetCDF time-series files (e.g., output from HLS_VI_Pipeline steps 01–03) and produces:
- Aggregated time-series data (Parquet)
- Phenological metrics tables (CSV)
- Phenology plots (PNG static + HTML interactive)

The tool is intentionally decoupled from HLS_VI_Pipeline — it accepts any CF-1.8 NetCDF
with `time` (days since 1970-01-01), `y`, `x` dimensions and a VI data variable.

## Architecture: Layered Output Design

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

## Pipeline Execution Model

`vi_phenology.py` processes one region at a time. For each region:
1. All configured VIs are extracted (Layers 0+1) → `region_raw` dict
2. Smoothing applied (Layer 2) → `region_smoothed` dict
3. Parquet written to disk
4. Metrics computed and per-region CSV written to disk
5. Plots generated and written to disk
6. `region_raw` and `region_smoothed` go out of scope (GC-eligible)
7. Next region begins

The combined shapefile metrics CSV (`write_combined_metrics`) is the only output that
waits until all regions are complete — it requires the full accumulated `metrics_df`.

Region enumeration is done upfront by `extract.enumerate_regions(config)`, which
expands all shapefiles into their constituent (region_label, roi_gdf) pairs and
validates that each shapefile path exists.

## Inter-Module Data Contract

The primary data bus between layers is a `dict` keyed by `(vi, region_label)` tuple,
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
| `vi_phenology.py` | CLI entrypoint (argparse); streaming per-region pipeline orchestration |
| `phenology_config.py` | `PhenologyConfig` dataclass; built from parsed CLI args |
| `extract.py` | Layers 0+1 — NetCDF discovery, region enumeration, spatial masking, ROI aggregation, daily reindex |
| `smooth.py` | Layer 2 — gap-fill and smooth; smoothing-on-obs-dates-first strategy |
| `metrics.py` | Layer 3 — SOS, POS, EOS, LOS, IVI, greening/senescence rates |
| `plot.py` | Matplotlib static (PNG) + Plotly interactive (HTML) phenology plots |
| `io_utils.py` | Parquet read/write, NetCDF file discovery helpers |

## Key Design Decisions

### Spatial Aggregation Modes
- `roi_mean` (default): mask pixels to shapefile, aggregate spatially to one time series per region
- `per_pixel`: preserve spatial dimensions (output is larger; Zarr recommended for future extension)

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
Apply `--valid-range-{vi}` at extraction time (Layer 0), before spatial aggregation.
Pixels outside `[vmin, vmax]` → NaN before computing the spatial mean.

### Multi-Tile Handling
If a shapefile spans multiple MGRS tiles, pool all valid pixels from all overlapping tiles
**per observation date** before computing the spatial mean (not concatenate-then-average).
Read CRS from `spatial_ref` variable WKT (CF-1.8 grid mapping convention).

### Annual Windows
`--year-start-doy` (default 1 = Jan 1) allows non-calendar-year seasons. Set it to the
**VI minimum**, not the peak — the correct value depends on biome and rainfall regime:
- Northern Hemisphere temperate / Cape fynbos (winter-rainfall): `1` (Jan 1, summer drought)
- Southern Hemisphere summer-rainfall (Savanna, Highveld): `182` (Jul 1, austral winter)

`year_start_doy` only affects `split_by_year()` in `metrics.py`. It does NOT affect the
annual phenology plot, which always groups by calendar year and displays DOY 1–365 (Jan–Dec).

### Tile-Level Parallelism
Tile extraction uses `concurrent.futures.ProcessPoolExecutor` (`--workers N`, default 8).
The worker function `_process_one_tile()` in `extract.py` **must** remain at module top
level (not nested) to be picklable by multiprocessing on all platforms.

Inside each worker, dask computation is wrapped in `with dask.config.set(scheduler='synchronous'):`.
This is critical — without it, each worker spawns its own dask thread pool, causing all
workers to compete for the same cores and eliminating the parallelism benefit. This is the
same pattern used in HLS_VI_Pipeline Steps 04–10.

### Memory-Safe NetCDF Chunking
Both `clip_netcdf_to_roi()` and `open_full_extent()` open files with `chunks={}` to use
the file's native chunk layout. With `dask.config.set(scheduler='synchronous')`, dask
processes one native chunk at a time sequentially, keeping peak memory bounded at the
native chunk size × the spatial footprint.

Do NOT use `chunks={'time': 1}` — if the file's native time chunks are larger than 1,
xarray must decompress the full native chunk just to extract a single time step, causing
severe I/O amplification (the file is read once per time step instead of once per native
chunk). Use `chunks={}` and let the native storage layout determine the memory footprint.

## Key Implementation Notes

### `rioxarray` Side-Effect Import
`extract.py` imports `rioxarray` as `# noqa: F401` to activate the `.rio` accessor on
xarray objects. This import must be present even though `rioxarray` is never referenced
directly by name — without it, `.rio.clip()` and `.rio.write_crs()` do not exist.

### `matplotlib` Backend
`plot.py` calls `matplotlib.use("Agg")` at module level (before any other matplotlib
import) to force the non-interactive PNG backend. Do not move or remove this call.

### NetCDF Discovery — Canonical Location
`io_utils.discover_netcdfs_for_vi()` is the canonical implementation for finding
`T{TILE}_{VI}.nc` files. `discover_netcdfs()` in `extract.py` delegates to it.

### Supported VIs
`--vi` choices are hard-coded in `vi_phenology.py` (`choices=["NDVI", "EVI2", "NIRv"]`).
Adding a new VI requires updating that list, adding a `--valid-range-{vi}` argument,
and adding its entry to the `valid_ranges` dict in `main()`.

### `--shapefile` and `--shapefile-field`
`--shapefile` is `nargs="+"` — multiple shapefiles produce independent time series per
file, each written to its own subdirectory.

`--shapefile-field` is also `nargs="+"` and accepts one field name per shapefile, in the
same positional order as `--shapefile`. The count must match exactly — a mismatch raises
a hard error in `PhenologyConfig.__post_init__`. Use the special value `none`
(case-insensitive) to dissolve a specific shapefile instead of splitting it:

```
--shapefile flights.shp tiles.geojson --shapefile-field box_nr none
```

When `--shapefile-field` is omitted entirely, all shapefiles are dissolved.

The field value for each shapefile is looked up via `PhenologyConfig.field_for_shapefile(index)`,
which returns `None` for dissolved shapefiles (value `'none'`) and the field name otherwise.

Splitting is implemented in `extract.load_shapefile_regions(shapefile_path, field=None)`,
which returns a list of `(region_label, GeoDataFrame)` pairs. Field values are sanitized
(spaces/special chars → underscores) via `extract._sanitize_label()`.

## Input NetCDF Format

Expected dimensions: `time` (int32, "days since 1970-01-01"), `y`, `x` (meters)
CRS stored in `spatial_ref` variable as WKT (CF-1.8 grid mapping convention)
One file per tile+VI: `T{TILE}_{VI}.nc` (HLS_VI_Pipeline naming convention)
Tool can accept any NetCDF matching the dimension/CRS structure above.

## Output File Naming

| Output | Pattern | Location |
|--------|---------|----------|
| Parquet time series | `{VI}_{region_label}_timeseries.parquet` | per-region subdirectory |
| Per-region metrics CSV | `{VI}_{region_label}_metrics.csv` | per-region subdirectory |
| Combined shapefile metrics CSV | `{VI}_{shapefile_stem}_metrics.csv` | `output_dir` root |
| Annual phenology plot | `{VI}_{region_label}_annual.{ext}` | per-region subdirectory |
| Full time-series plot | `{VI}_{region_label}_timeseries.{ext}` | per-region subdirectory |
| Anomaly plot | `{VI}_{region_label}_anomaly.{ext}` | per-region subdirectory |
| Multi-VI comparison | `{region_label}_multi_vi.{ext}` | per-region subdirectory |

Combined metrics CSV is only written when `--shapefile-field` is set and the shapefile was
not dissolved (i.e., the field value is not `'none'`).

`region_label` is determined as follows:

| Scenario | `region_label` |
|---|---|
| No shapefile | `full_extent` |
| Shapefile, no `--shapefile-field` | Shapefile filename stem |
| Shapefile + `--shapefile-field` | Sanitized field value (spaces/specials → `_`) |

Field-value sanitization is handled by `extract._sanitize_label()`.
