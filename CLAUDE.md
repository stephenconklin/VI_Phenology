# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commit Style

Do NOT include `Co-Authored-By: Claude` trailers in any commit messages for this project.

## Environment Setup

```bash
conda env create -f environment.yml
conda activate vi_phenology
```

## Three Pipelines — Overview

This repository contains three independent processing pipelines, selected via the `PIPELINE`
variable in `run_phenology.sh`:

| `PIPELINE` | Script | Purpose |
|---|---|---|
| `phenology` | `src/vi_phenology.py` | ROI-mean time series, smoothing, metrics, plots |
| `netcdf_datacube` | `src/netcdf_datacube_extract.py` | Per-pixel CF-1.8 datacubes clipped to polygon regions |
| `pixel_phenology` | `src/pixel_phenology_extract.py` | 19 per-pixel metric maps from existing datacubes |

The `phenology` and `netcdf_datacube` pipelines share the same `--netcdf-dir`, `--vi`,
`--shapefile`, `--shapefile-field`, `--valid-range-*`, `--workers`, `--start-date`,
and `--end-date` inputs.

The `phenology` pipeline additionally supports `--input-datacubes` as an alternative to
`--netcdf-dir` + `--shapefile`. When datacubes are provided, no tile discovery or spatial
clipping is needed — VI and region_label are inferred from each filename
(`{VI}_{region_label}_datacube.nc`). Use this for faster repeated runs (different smoothing,
thresholds, or plots) after the `netcdf_datacube` pipeline has already produced the datacubes.
`--input-datacubes` accepts individual file paths or a **directory path**; when a directory
is given, all `*_datacube.nc` files found recursively within it are used. This matches the
nested output layout of `netcdf_datacube` (`{output_dir}/{shapefile_stem}/{region_label}/`)
so pointing at the shapefile subfolder picks up all regions automatically.

The `pixel_phenology` pipeline takes `--input-datacubes` (paths to `*_datacube.nc` files
produced by `netcdf_datacube`) as its primary input and does not use `--netcdf-dir` or
shapefiles — the spatial clipping is already embedded in the datacube.
`--input-datacubes` accepts individual file paths or a **directory path**; when a directory
is given, all `*_datacube.nc` files found recursively within it are used. `PIXEL_INPUT_DATACUBES`
in `run_phenology.sh` supports the same two forms.

## Running the Tool

### Recommended — `run_phenology.sh`

Edit the variables at the top and run `./run_phenology.sh`. Inline comments document every option.

### Phenology pipeline (direct CLI)

**Standard mode** — discovers and clips source VI NetCDF tiles:
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
  --min-valid-obs 20 \
  --min-valid-obs-per-year 5 \
  --sample-pixels 500 \
  --random-seed 42 \
  --min-ndvi-mean 0.10 \
  --min-quality-frac 0.0 \
  --workers 4 \
  --start-date 2022-01-01 \
  --end-date   2024-12-31
```

**Datacube mode** — reads pre-clipped datacubes (faster for repeated runs); accepts a
directory (all `*_datacube.nc` files found recursively) or individual file paths:
```bash
# Directory — picks up all regions under the shapefile subfolder automatically:
python src/vi_phenology.py \
  --input-datacubes /path/to/outputs/LVIS_flightboxes_final \
  --vi NDVI \
  --valid-range-ndvi "-0.1,1.0" \
  --output-dir ./outputs \
  --smooth-method whittaker \
  --smooth-lambda 100 \
  --plot-style combined \
  --plot-format png html \
  --metrics \
  --min-valid-obs 20 \
  --min-valid-obs-per-year 5

# Individual files:
python src/vi_phenology.py \
  --input-datacubes /path/to/NDVI_G5_1_datacube.nc /path/to/NDVI_G5_12_datacube.nc \
  --vi NDVI \
  --valid-range-ndvi "-0.1,1.0" \
  --output-dir ./outputs \
  --smooth-method whittaker \
  --smooth-lambda 100 \
  --plot-style combined \
  --plot-format png html \
  --metrics \
  --min-valid-obs 20 \
  --min-valid-obs-per-year 5
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

### Pixel phenology pipeline (direct CLI)

Reads `*_datacube.nc` files produced by `netcdf_datacube_extract.py`. VI and region label
are inferred from the filename (`{VI}_{region_label}_datacube.nc`).

```bash
python src/pixel_phenology_extract.py \
  --input-datacubes /path/to/NDVI_MyRegion_datacube.nc \
  --output-dir ./pixel_metrics \
  --smooth-lambda 100 \
  --min-valid-obs 20 \
  --min-valid-obs-per-year 5 \
  --peak-prominence 0.05 \
  --peak-min-distance 45 \
  --season-threshold 0.20 \
  --valid-range-ndvi "-0.1,1.0" \
  --workers 8 \
  --start-date 2020-01-01 \
  --end-date   2024-12-31
```

Use `python src/pixel_phenology_extract.py --help` for the full argument reference.

## Implementation Status

All modules are **fully implemented**. The end-to-end pipeline runs successfully across
all layers (extraction → smoothing → metrics → plots). No stubs remain.

`netcdf_datacube_extract.py` and `pixel_phenology_extract.py` are standalone modules —
neither depends on `PhenologyConfig` or any other phenology-pipeline module.

## Project Purpose

### Phenology pipeline (`vi_phenology.py`)

Reads CF-1.8 compliant VI NetCDF time-series files and produces per-region
spatially aggregated time series:
- ROI-mean aggregated time series (observations CSV)
- Smoothed gap-filled daily series
- Phenological metrics tables (CSV)
- Phenology plots (PNG static + HTML interactive)

### netCDF datacube pipeline (`netcdf_datacube_extract.py`)

Clips CF-1.8 VI NetCDF files to polygon boundaries and delivers per-pixel
CF-1.8 compliant datacubes — preserving the full spatial dimension for
downstream scientific analysis:
- One merged datacube per region (default) or one file per tile
- Same-CRS tiles: mosaiced without resampling via direct `netCDF4-python` write loop (memory-bounded)
- Cross-CRS tiles: minority tiles bilinearly reprojected to the dominant CRS before merging
- CF-1.8 global attributes: `Conventions`, `history`, `tiles`, `region`, `vi`,
  `target_crs`, `resampling_method` (when reprojection occurs)

### Pixel phenology pipeline (`pixel_phenology_extract.py`)

Reads per-pixel datacubes (from `netcdf_datacube_extract.py`) and produces per-pixel
phenological metric maps — one CF-1.8 NetCDF per (VI, region) with 19 metric bands:
- Whittaker smoothing applied per-pixel (λ D^T D penalty matrix precomputed once per
  datacube; all pixels share the same time axis)
- 19 metrics per pixel: peak NDVI/DOY (mean+std), integrated NDVI (mean+std),
  green-up rate (mean+std), floor NDVI, ceiling NDVI, season length (mean+std), CV,
  interannual peak range+std, n_peaks, peak separation, relative peak amplitude,
  valley depth
- Floor and ceiling NDVI derived directly from the annual smooth curve (no DOY windows)
- Parallelised via `ThreadPoolExecutor` over y-row chunks; scipy sparse solver releases
  GIL for true multi-core throughput
- Outputs: `{VI}_{region_label}_pixel_metrics.nc` (compressed, CF-1.8) +
  `{VI}_{region_label}_pixel_metrics_summary.csv` (spatial stats per metric)

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
Pre-filter (main process, sequential, before worker dispatch)
  For each tile: read x/y coordinate min/max only (4 scalar values, no data decompression)
  Reproject ROI bounding box to tile CRS → bounding-box intersection test
  Exclude non-overlapping tiles before any worker is spawned
   ↓
Phase 1 (parallel workers)
  For each bbox-passing tile:
    Open NetCDF → clip to ROI → apply valid-range mask → write temp netCDF
    (workers return only small status dicts — never large arrays across process boundary)
   ↓
Phase 2 (main process)
  Detect CRS per tile (pyproj.CRS.to_epsg() for normalized comparison)
  Group tiles by CRS
  Apply merge strategy:
    1 tile           → single file, native CRS
    N tiles same CRS, merge_same_crs=True  → _write_mosaic_nc4 (nc4 write loop), no resampling
    N tiles mixed CRS, merge_cross_crs=True → reproject minority tiles (bilinear) + _write_mosaic_nc4
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
| N tiles, same CRS, `MERGE_SAME_CRS=true` | `_write_mosaic_nc4` write loop, no resampling | 1 file |
| N tiles, mixed CRS, `MERGE_CROSS_CRS=true` | Bilinear reproject minority → `_write_mosaic_nc4` | 1 file |
| `MERGE_SAME_CRS=false` | One file per tile, native CRS | N files |
| `MERGE_CROSS_CRS=false` | One file per tile, native CRS, no reprojection | N files |

Dominant CRS = the CRS group with the most total pixels (y × x) within the polygon.

## Architecture: Pixel Phenology Pipeline

```
For each input datacube ({VI}_{region_label}_datacube.nc):
   ↓
Open with xarray (lazy); apply --start-date/--end-date filter on time axis
Warn if uncompressed size > 8 GB
Load full (time, y, x) array into numpy float32
   ↓
Precompute λ D^T D penalty matrix once (all pixels share the same n_days)
   ↓
ThreadPoolExecutor: dispatch y-row chunks (_Y_CHUNK_ROWS = 50 rows per chunk)
   Each thread:
     For each pixel in its y-row chunk:
       Map valid obs onto daily grid (NaN → weight 0)
       Solve (W + λ D^T D) z = W y  [Whittaker smooth]
       Per year: peak, IVI, floor, ceiling, greenup_rate, season_length, bimodality
       Aggregate: mean/std across years; CV from raw obs
     Return (n_metrics, n_y_chunk, n_x) float32 array
   ↓
Assemble full (n_metrics, n_y, n_x) output array
   ↓
Write {VI}_{region_label}_pixel_metrics.nc  (CF-1.8, zlib complevel=4)
Write {VI}_{region_label}_pixel_metrics_summary.csv  (mean, std, p05, p50, p95, n_valid_pixels)
```

**Threading rationale:** `scipy.sparse.linalg.spsolve` releases the GIL, so
`ThreadPoolExecutor` achieves true multi-core parallelism while sharing the in-memory
array without inter-process serialisation overhead.

**Memory note:** The full (time, y, x) array is loaded once into RAM. For large datacubes,
use `--start-date`/`--end-date` to reduce the time axis before loading.

## Pipeline Execution Model

### Phenology pipeline

`vi_phenology.py` processes one region at a time. For each region:
1. All configured VIs are extracted (Layers 0+1) → `region_raw` dict
2. Smoothing applied (Layer 2) → `region_smoothed` dict
3. Observations CSV written to disk (if `config.save_observations_csv`)
4. Metrics computed and per-region CSV written to disk (if `config.compute_metrics`)
5. Plots generated and written to disk (if any plot toggle is enabled)
6. `region_raw` and `region_smoothed` go out of scope (GC-eligible)
7. Next region begins

Combined shapefile outputs (`write_combined_metrics`, `write_combined_observations_csv`)
wait until all regions are complete.
These are gated by `config.save_combined_outputs` (for observations CSV)
and `config.compute_metrics` (for the metrics CSV).

### Output Toggles (phenology pipeline only)

All output types are enabled by default. Disable individually via CLI flags or
`run_phenology.sh` variables:

| Config field | CLI flag | `run_phenology.sh` | Controls |
|---|---|---|---|
| `save_observations_csv` | `--no-observations-csv` | `SAVE_OBSERVATIONS_CSV=false` | Per-region observations CSV |
| `save_combined_outputs` | `--no-combined-outputs` | `SAVE_COMBINED_OUTPUTS=false` | Combined shapefile observations CSV |
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
| `vi_phenology.py` | CLI entrypoint (argparse); streaming per-region phenology pipeline orchestration; `_enumerate_datacube_regions()` for datacube input mode |
| `phenology_config.py` | `PhenologyConfig` dataclass; built from parsed CLI args; `netcdf_dir` and `input_datacubes` are mutually exclusive optional fields |
| `extract.py` | Layers 0+1 — NetCDF discovery, region enumeration, spatial masking, ROI aggregation, daily reindex; `aggregate_from_datacube()` for datacube input mode; pixel selection (Phase A: `_compute_pixel_stats_one_tile`, `select_pixel_sample`) |
| `smooth.py` | Layer 2 — gap-fill and smooth; supports savgol, loess, linear, harmonic, whittaker |
| `metrics.py` | Layer 3 — SOS, POS, EOS, LOS, IVI, greening/senescence rates + extended metrics (floor/ceiling, season length, bimodality, CV) |
| `plot.py` | Matplotlib static (PNG) + Plotly interactive (HTML) phenology plots |
| `io_utils.py` | Shared utilities: observations CSV I/O, NetCDF file discovery, `sanitize_label`, `load_shapefile_regions`, `parse_valid_range`, `read_netcdf_crs`, `setup_log_file` |
| `netcdf_datacube_extract.py` | Standalone CLI: per-pixel CF-1.8 datacube extraction with two-phase parallel tile processing and CRS-aware merge |
| `pixel_phenology_extract.py` | Standalone CLI: per-pixel Whittaker smoothing + 19-metric extraction from datacubes; ThreadPoolExecutor over y-row chunks |

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
| `whittaker` | Whittaker smoother | Penalised least-squares; `--smooth-lambda` (default 100); handles irregular HLS cadence natively without binning |
| `none` | Skip Layer 2 | Only Layers 0+1 produced |

### Whittaker Smoother (`smooth_whittaker`)
Solves `(W + λ D^T D) z = W y` where W is the diagonal observation-weight matrix
(1 = observed, 0 = gap) and D is the 2nd-order difference matrix (penalises curvature).
Unlike S-G, the full daily grid is the working domain — no binning to uniform spacing
required. This makes it especially robust to HLS's variable revisit cadence and long
cloud-gap periods.

`--smooth-lambda` controls smoothing strength. Typical values:
- `10–50`: tight, follows observations closely
- `100` (default): balanced smoothing
- `300–1000`: very smooth; appropriate for coarse biome-level characterisation

`smooth_whittaker` follows the same obs-first API as other methods in `smooth.py`: takes
an observation-date-only Series, returns a complete daily Series. Falls back to linear
interpolation if n_days < 3 or the sparse solver fails.

### Phenological Metrics (Layer 3)
Per year per region per VI. The metrics CSV has one row per (vi, region, year).

**Core metrics** (existing):
- **SOS** — Start of Season: VI crosses `baseline + sos_threshold * amplitude` going up
- **POS** — Peak of Season: date + value of annual maximum
- **EOS** — End of Season: VI drops back through same threshold
- **LOS** — Length of Season: EOS − SOS (days)
- **IVI** — Integrated VI: trapezoidal area under curve between SOS and EOS
- **Greening rate** — `(VI_pos - VI_sos) / (pos_date - sos_date).days`
- **Senescence rate** — `(VI_eos - VI_pos) / (eos_date - pos_date).days` (negative for declining)

**Extended metrics** (added in feature/metrics_and_pixel_pipeline):
- **floor_ndvi** — annual minimum of the smooth curve (dry-season trough, derived directly from curve)
- **ceiling_ndvi** — annual maximum of the smooth curve (= pos_value; wet-season peak)
- **season_length_days** — days above `floor + sos_threshold × amplitude`; uses actual dates (cross-year safe)
- **greenup_rate** — `(ceiling − floor) / (peak_date − floor_date).days`; floor location from curve minimum
- **n_peaks** — count of peaks detected by `scipy.signal.find_peaks` (prominence + distance thresholds)
- **peak_separation_days** — calendar days between the two tallest peaks (NaN if n_peaks < 2)
- **relative_peak_amplitude** — `min(h1,h2) / max(h1,h2)` ratio (NaN if n_peaks < 2)
- **valley_depth** — normalised trough depth between peaks: `((h1+h2)/2 − valley) / ((h1+h2)/2)` (NaN if n_peaks < 2)
- **cv** — coefficient of variation of raw (unsmoothed) observations across the **full** time series; same value on every year row for a given (vi, region)

**Floor/ceiling design note:** floor and ceiling are computed from the annual smooth curve
minimum and maximum — no biome-specific DOY windows are used or needed. This makes the
pipeline self-calibrating across biomes (fynbos, savanna, grassland) without configuration.

**Bimodality parameters** (only active when `--metrics` flag is set):
- `--peak-prominence` (default 0.05): min NDVI prominence for a peak to be counted
- `--peak-min-distance` (default 45): min separation in days between peaks

`baseline = annual minimum; amplitude = peak - baseline`

`compute_metrics()` returns the full `pd.DataFrame` of all rows. `vi_phenology.py` captures
this and passes it to `write_combined_metrics()`, which writes `{VI}_{shapefile_stem}_metrics.csv`
at the root of `output_dir` when `--shapefile-field` is set. Per-region CSVs are always written
by `compute_metrics()` regardless. Dissolved shapefiles (field value `'none'`) are skipped by
`write_combined_metrics()`.

### Observation Count Thresholds (phenology + pixel_phenology)

Two complementary thresholds gate metric computation at different granularities:

| Parameter | CLI flag | `run_phenology.sh` | Applied in | Effect |
|---|---|---|---|---|
| `min_valid_obs` | `--min-valid-obs` | `MIN_VALID_OBS` / `PIXEL_MIN_VALID_OBS` | Phenology: `vi_phenology.py` main loop; Pixel: `_extract_pixel_metrics` | Skip region/pixel entirely if total obs < N |
| `min_valid_obs_per_year` | `--min-valid-obs-per-year` | `MIN_VALID_OBS_PER_YEAR` / `PIXEL_MIN_VALID_OBS_PER_YEAR` | Phenology: `metrics.py compute_metrics()`; Pixel: `_extract_pixel_metrics` per-year loop | Skip individual annual window if obs < N; no NaN row written |

Default values: `min_valid_obs=20`, `min_valid_obs_per_year=5`. Increase per-year threshold to 8–10 for stricter quality control in cloudy regions.

For the phenology pipeline, `min_valid_obs` counts rows in the Layer 0 observation DataFrame (one row per observation date). For the pixel pipeline, it counts valid (non-NaN, in-range) timesteps in the per-pixel array.

### Pixel Sampling — Phenology Pipeline Only

Randomly samples a fixed set of N pixels once per (VI, region) and uses those same pixels consistently across the entire time series. Eliminates date-to-date variation in the spatial sample caused by cloud masking — different dates no longer use different pixel sets.

**Two-phase architecture in `aggregate_across_tiles()`:**

```
Phase A  (select_pixel_sample — only when sampling/filtering is requested)
  _compute_pixel_stats_one_tile workers (parallel, same ProcessPoolExecutor pattern):
    Clip tile → apply valid range → compute per-pixel temporal NDVI sum + valid count
    Return flat pixel list: (y_round, x_round, ndvi_sum, count)
  Main process combines tiles, deduplicates by (y_round, x_round), computes:
    mean_ndvi  = total_ndvi_sum / total_count
    valid_frac = total_count / total_n_time
  Apply filters: min_ndvi_mean, min_quality_frac
  Draw N random pixels: np.random.default_rng(seed).choice(eligible_keys, N)
  Returns: set of (y_round, x_round) tuples
   ↓
Phase B  (extraction — always runs)
  _process_one_tile workers receive pixel_coords set
  After valid-range masking, build 2D boolean pixel mask in O(N_sample) via dict lookup:
    y_to_idx = {y: i for i, y in enumerate(y_arr)}
    x_to_idx = {x: i for i, x in enumerate(x_arr)}
    pixel_mask_2d[iy, ix] = True for each (y_r, x_r) in pixel_coords
  Apply mask with xr.DataArray.where() before spatial aggregation
```

**Pixel coordinates:** rounded to 1 decimal place for reliable cross-tile matching. Same-CRS tiles share an identical 30-m grid, so coordinates are directly comparable. Cross-CRS tiles have incompatible coordinate spaces — pixels sampled from tile A (EPSG:32734) are silently absent from tile B (EPSG:32733), which degrades gracefully (tile B still contributes all its eligible pixels).

**Parameters (phenology pipeline only):**

| CLI flag | `run_phenology.sh` | Default | Effect |
|---|---|---|---|
| `--sample-pixels N` | `SAMPLE_PIXELS` (commented out) | None = all pixels | Number of pixels to randomly sample per region |
| `--random-seed SEED` | `RANDOM_SEED` (commented out) | None = random | RNG seed for reproducible pixel samples |
| `--min-ndvi-mean VAL` | `MIN_NDVI_MEAN` (commented out) | None = no filter | Exclude pixels below this temporal mean NDVI |
| `--min-quality-frac FRAC` | `MIN_QUALITY_FRAC` (commented out) | None = no filter | Min fraction of valid timesteps for a pixel to be eligible |

Phase A only runs when at least one of `n_sample`, `min_ndvi_mean`, or `min_quality_frac > 0` is set. When all are at defaults, extraction is identical to the original wall-to-wall approach.

### Valid Range Application
Apply `--valid-range-{vi}` at extraction time (Layer 0 for phenology; Phase 1 worker for datacube),
before spatial aggregation or output. Pixels outside `[vmin, vmax]` → NaN.

### Multi-Tile Handling — Phenology Pipeline
If a shapefile spans multiple MGRS tiles, pool all valid pixels from all overlapping tiles
**per observation date** before computing the spatial mean (not concatenate-then-average).
Read CRS from `spatial_ref` variable WKT (CF-1.8 grid mapping convention).

### Multi-Tile Handling — Datacube Pipeline
Phase 2 reads CRS from each temp file's `spatial_ref` variable WKT, normalizes to EPSG
integers using `pyproj.CRS.from_wkt(wkt).to_epsg(min_confidence=20)` (more robust than
WKT string comparison), and groups tiles by CRS. The merge strategy is then applied as
described in the Two-Phase Model section above.

**HLS 2.0 CRS quirks:** Two separate quirks apply to HLS v2.0 source data, both fixed
upstream in `03_hls_netcdf_build.py` of HLS_VI_Pipeline:

1. **Non-standard datum name** — HLS v2.0 tiles embed `"Not specified (based on WGS 84
   spheroid)"` as the datum name. pyproj's default `to_epsg()` (min_confidence=70) returns
   `None` for these WKTs. `_merge_and_write_datacube()` uses `min_confidence=20` to
   reliably resolve them to their EPSG integer so same-UTM-zone tiles group correctly. The
   fallback when EPSG resolution still fails is `crs_obj.name` (never the raw WKT string,
   which can differ between GDAL versions and cause false cross-CRS grouping).

2. **Southern hemisphere tiles stored as UTM North** — HLS v2.0 GeoTIFFs for tiles south
   of the equator use a UTM North zone (EPSG:326xx, false_northing=0) with negative
   northings instead of the standard UTM South convention (EPSG:327xx,
   false_northing=10,000,000). `03_hls_netcdf_build.py` now corrects this automatically
   (EPSG + 100, y-coords + 10,000,000 m). NetCDF files from a corrected pipeline run carry
   EPSG:327xx CRS with positive northings. Older files with the UTM North / negative-
   northing convention remain spatially consistent for all VI_Phenology operations (both
   tile and ROI are projected in the same UTM North space), but will carry an incorrect
   northern hemisphere CRS label in any output datacube; rebuilding with step 03 resolves
   this fully.

For same-CRS merges: adjacent HLS MGRS tiles in the same UTM zone share an **identical 30-m
pixel grid** — no resampling is needed. `_write_mosaic_nc4` uses last-written-wins for the
~163-pixel MGRS overlap zone (scientifically equivalent to first-wins for co-acquired pixels)
and provides time union across all tile acquisition dates.

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

**Phase 1 workers** (`_extract_datacube_one_tile`): open source files with `chunks={}` to
use the file's native HDF5 chunk layout. With `dask.config.set(scheduler='synchronous')`,
dask processes one native chunk at a time, keeping peak memory bounded at native chunk
size × spatial footprint. Do NOT use `chunks={'time': 1}` here — if native time chunks
are larger than 1, xarray must decompress the full native chunk per time step, causing
severe I/O amplification.

**Phase 2 (`_write_mosaic_nc4`)**: re-opens each temp file with
`chunks={'time': _PHASE2_READ_CHUNK}` (default 20). This overrides the native chunk
layout so exactly 20 time steps are decompressed per dask compute call, bounding peak
memory to `~_PHASE2_READ_CHUNK × tile_ny × tile_nx × 4 B` (~190 MB for the largest
BioSCape tiles). The `chunks={}` convention is intentionally NOT used here — for temp
files with large or contiguous native chunks, `chunks={}` would produce a single giant
dask chunk that materialises the full DataArray into memory, causing swap thrash.

### Datacube Temp File Strategy
Phase 1 workers write to `{output_dir}/{region_label}/_tmp/{VI}_{tile_id}_clip.nc`.
Workers return only small status dicts — they never return DataArrays across the process
boundary. This keeps inter-process communication cheap regardless of datacube size.

Phase 2 (`_write_mosaic_nc4`) re-opens each temp file with explicit time chunking,
builds the union y/x/time coordinate arrays, creates the output file via `nc4.Dataset`,
and writes one time step at a time per tile using simple integer + slice indexing.
`_cleanup_temps()` is called in a `try/finally` block to guarantee temp directory removal
even if Phase 2 raises an exception.

Temp files are written **without compression** — they are written once, read once by
Phase 2, then deleted. Compression would add significant CPU overhead for no lasting
benefit, particularly on external drives.
Final output datacubes use `zlib=True, complevel=4` (set via `nc4.createVariable`) —
VI data compresses well (NaN-heavy, spatially smooth) and typically yields 5–10× size
reduction vs. uncompressed NetCDF4. HDF5 `chunksizes=(1, ny, nx)` gives one complete
2-D spatial layer per chunk — efficient for both write (one step at a time) and
downstream `xr.open_dataset(chunks={})` reads.

## Key Implementation Notes

### NetCDF Output Compression

**Phase 1 temp files** (`_extract_datacube_one_tile`): written via xarray `to_netcdf(temp_path)`
with **no encoding** — uncompressed, written once, read once by Phase 2, then deleted.
Compression here would add CPU overhead with no lasting benefit.

**Final output datacubes** (`_write_mosaic_nc4` and per-tile path): compression is set
directly on the `nc4.createVariable` call (merged path) or via xarray encoding (per-tile path):
```python
# Merged output — via nc4.Dataset.createVariable:
nc4.createVariable(vi, 'f4', ..., zlib=True, complevel=4, chunksizes=(1, ny, nx))

# Per-tile output — via xarray to_netcdf encoding:
encoding={vi: {'zlib': True, 'complevel': 4}}
```
Do NOT call `to_netcdf()` without `encoding` on the per-tile path — xarray's default is
uncompressed NetCDF4, which produces files orders of magnitude larger than necessary
(e.g. a 853×4611 px, 1465-step datacube would be ~23 GB uncompressed vs. ~2–5 GB compressed).

### NumPy 2.x Compatibility
Use `np.trapezoid` (not `np.trapz`, which was removed in NumPy 2.0). The conda env
uses NumPy 2.x — `np.trapz` will raise `AttributeError` at runtime.

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
In the datacube pipeline, it is handled inline in `main()`.

Both pipelines sanitize field values via `sanitize_label()` (spaces/special chars → underscores),
which lives in `io_utils.py` and is imported by both.

## Input NetCDF Format

Expected dimensions: `time` (decoded to datetime64[ns] by xarray), `y`, `x` (meters)
CRS stored in `spatial_ref` variable — check for `crs_wkt` attr first, then `spatial_ref` attr
One file per tile+VI: `T{TILE}_{VI}.nc` (HLS_VI_Pipeline naming convention)

## Output File Naming

### Phenology pipeline

| Output | Pattern | Location |
|--------|---------|----------|
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

### Pixel phenology pipeline

| Output | Pattern | Location |
|--------|---------|----------|
| Pixel metric map | `{VI}_{region_label}_pixel_metrics.nc` | `{output_dir}/{region_label}/` |
| Summary CSV | `{VI}_{region_label}_pixel_metrics_summary.csv` | `{output_dir}/{region_label}/` |
| Overview PNG | `{VI}_{region_label}_pixel_metrics_overview.png` | `{output_dir}/{region_label}/` |
| Overview HTML | `{VI}_{region_label}_pixel_metrics_overview.html` | `{output_dir}/{region_label}/` |
| Log file | `pixel_phenology_{YYYYMMDD_HHMMSS}.log` | `--output-dir` root |

Overview outputs are generated by default. Disable individually via `--no-overview-figure`
and `--no-overview-html` CLI flags, or `PIXEL_OVERVIEW_FIGURE=false` /
`PIXEL_OVERVIEW_HTML=false` in `run_phenology.sh`.

The PNG is a print-quality 4×5 panel sheet with geographic aspect ratio preserved.
The HTML is an interactive Plotly viewer (2-column × 10-row) with hover-enabled pixel
coordinates and values. Both use `_FillValue=9.96920996838687e+36` (CF/NetCDF4 float32
standard) — **do not** use `np.nan` as `_FillValue` in output NetCDF files, as NaN
fails equality-comparison tests in tools such as Panoply (`NaN != NaN` in IEEE 754).

`VI` and `region_label` are parsed from the input datacube filename:
`{VI}_{region_label}_datacube.nc` → first underscore-separated token = VI, remainder = region_label.

`region_label` is determined as follows for both pipelines:

| Scenario | `region_label` |
|---|---|
| No shapefile | `full_extent` |
| Shapefile, no `--shapefile-field` | Shapefile filename stem |
| Shapefile + `--shapefile-field` | Sanitized field value (spaces/specials → `_`) |
