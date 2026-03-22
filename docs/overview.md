# VI Phenology Guide

[![Python](https://img.shields.io/badge/python-3.10--3.12-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS-lightgrey.svg)]()

A four-pipeline vegetation index analysis toolkit built around per-pixel CF-1.8 datacubes.
The `netcdf_datacube` pipeline is the recommended starting point — it clips source NetCDF
tiles to your polygon regions once and produces the per-pixel datacubes that power all
downstream analysis. From those datacubes, run `phenology` for ROI-mean time series,
smoothing, and plots; `pixel_phenology` for spatially explicit per-pixel metric maps;
`datacube_to_geotiff` for model-ready raster statistics; or any combination.

Designed to work natively with output from [HLS_VI_Pipeline](https://github.com/stephenconklin/HLS_VI_Pipeline),
but accepts any CF-1.8 NetCDF with `time`, `y`, `x` dimensions and a VI data variable.

---

## Four Pipelines

| Pipeline | Role | Set in `config.local.env` |
|---|---|---|
| **netcdf_datacube** | **Foundation** — clip source tiles to polygon regions; produce per-pixel CF-1.8 datacubes for downstream use | `PIPELINE="netcdf_datacube"` |
| **phenology** | ROI-mean time series, smoothing, phenological metrics, and plots — reads datacubes or raw tiles | `PIPELINE="phenology"` |
| **pixel_phenology** | 19 per-pixel phenological metric maps — reads datacubes produced by `netcdf_datacube` | `PIPELINE="pixel_phenology"` |
| **datacube_to_geotiff** | Per-year / per-month / per-DOY summary statistics as multi-band GeoTiffs — reads datacubes produced by `netcdf_datacube` | `PIPELINE="datacube_to_geotiff"` |

`netcdf_datacube` and `phenology` share the same tile-based input configuration:
`NETCDF_DIR`, `VI`, `SHAPEFILE`, `SHAPEFILE_FIELD`, `VALID_RANGE_*`, `WORKERS`,
`START_DATE`, `END_DATE`. The three datacube-reading pipelines (`phenology` in datacube
mode, `pixel_phenology`, and `datacube_to_geotiff`) take `--input-datacubes` (datacubes
produced by `netcdf_datacube`) and do not use `--netcdf-dir` or shapefiles — the spatial
clipping is already embedded in the datacube files.

---

## Typical Workflows

### Step 1 — Produce datacubes (recommended for all workflows)

```
PIPELINE="netcdf_datacube"
```

Clips source tiles to your polygon boundaries. Produces one `*_datacube.nc` file
per (VI, region). **Run this once.** All subsequent analysis reads from these files —
no re-clipping of source tiles required.

---

### Step 2 — Choose your analysis

From the same datacubes, run either or both downstream pipelines:

**ROI-mean phenology** — aggregate pixels to a regional mean, smooth, compute
metrics, and generate plots:

```
PIPELINE="phenology"    (set PHENOLOGY_INPUT_DATACUBES to your datacube directory)
```

**Per-pixel metric maps** — 19 spatially explicit metric bands mapped across every pixel:

```
PIPELINE="pixel_phenology"    (set PIXEL_INPUT_DATACUBES to the same directory)
```

Both pipelines can be pointed at the same datacube output directory. Running
`netcdf_datacube` once gives you access to both.

---

### Single-pass phenology (no intermediate datacubes)

If you need a one-off phenology run and don't plan to iterate or compute pixel
metrics, you can skip the datacube step entirely:

```
PIPELINE="phenology"    (set NETCDF_DIR + SHAPEFILE)
```

Discovers tiles, clips, aggregates, smooths, and plots in one pass. Tile clipping
runs every time, so this is slower for iterative work but requires no intermediate
storage.

---

## Supported Vegetation Indices

| VI | Name |
|----|------|
| NDVI | Normalized Difference Vegetation Index |
| EVI2 | Two-band Enhanced Vegetation Index |
| NIRv | Near-Infrared Reflectance of Vegetation |

Multiple VIs can be processed in a single run (`--vi NDVI EVI2 NIRv`).

---

## Features

### netCDF Datacube Pipeline
- Per-pixel CF-1.8 compliant datacubes clipped to polygon boundaries
- Same-CRS multi-tile merging: pixel-perfect, memory-bounded mosaic — no resampling
- Cross-CRS multi-tile merging: bilinear reprojection of minority tiles to dominant CRS before merge
- Configurable per-tile or merged output via `MERGE_SAME_CRS` / `MERGE_CROSS_CRS`
- Full CF-1.8 metadata: `Conventions`, `history`, `tiles`, `region`, `vi`, `target_crs`, `resampling_method`
- Output feeds both `phenology` (datacube input mode) and `pixel_phenology` directly

### Phenology Pipeline
- Two input modes: standard (`--netcdf-dir` + `--shapefile`) or datacube (`--input-datacubes`)
- Layered processing: raw observations → daily time axis → smoothed gap-filled series → phenological metrics
- Multiple smoothing methods: Savitzky-Golay, LOESS, linear interpolation, harmonic fit, Whittaker (`--smooth-lambda`)
- Core phenological metrics: SOS, POS, EOS, LOS, IVI, greening rate, senescence rate
- Extended metrics: `floor_ndvi`, `ceiling_ndvi`, `season_length_days`, `greenup_rate`, `n_peaks`, `peak_separation_days`, `relative_peak_amplitude`, `valley_depth`, `cv`
- Observation count thresholds: `--min-valid-obs`, `--min-valid-obs-per-year`
- Pixel sampling: `--sample-pixels`, `--random-seed`, `--min-ndvi-mean`, `--min-quality-frac`
- Annual DOY overlay plot, full time-series plot, anomaly plot, multi-VI comparison panel
- Granular output toggles — disable any combination of outputs in `config.local.env`
- Output formats: CSV (observations and metrics), PNG + interactive HTML plots
- Combined per-shapefile observations CSV and metrics CSV when splitting by attribute field

### Pixel Phenology Pipeline
- Reads datacubes produced by `netcdf_datacube` — accepts a directory or individual file paths
- Whittaker smoothing applied per pixel — handles HLS's irregular revisit cadence natively
- 19 metric bands: peak NDVI/DOY, integrated NDVI, green-up rate (mean + std), floor/ceiling NDVI,
  season length, CV, interannual peak range/std, bimodality metrics (n_peaks, separation, amplitude, valley depth)
- Output per (VI, region): CF-1.8 NetCDF metric map + summary CSV + print-quality 4×5 overview PNG + interactive Plotly HTML (hover shows pixel coordinates and values)
- Parallelised via `ThreadPoolExecutor`; scipy sparse solver releases GIL for true multi-core throughput
- Overview outputs generated by default; disable with `--no-overview-figure` / `--no-overview-html`

### datacube_to_geotiff Pipeline
- Reads datacubes produced by `netcdf_datacube` — accepts a directory or individual file paths
- Three GeoTiff products per (VI, region): per-year (N_years × 3 bands), per-month (36 bands), per-DOY (1095 bands)
- Statistics: median, 5th percentile, 95th percentile at each temporal resolution
- Per-month uses a per-year-then-average method to prevent observation-density bias across years
- LZW-compressed, 256×256 tiled, BigTIFF when > 4 GB; NoData = CF float32 fill value
- Band descriptions readable via `gdalinfo -mdd all` or `rasterio.open().descriptions`
- Streaming band-by-band write — constant peak memory regardless of output size
- Skip any product individually with `--skip-per-year`, `--skip-per-month`, `--skip-per-doy`

### Shared Features
- Spatial subsetting via any GeoPandas-readable vector format (`.shp`, `.gpkg`, `.geojson`, etc.)
- Per-feature splitting: one independent output per attribute value in a shapefile
- Multiple shapefiles in a single run, each with its own optional field splitting
- Date range filtering applied at the NetCDF level before any aggregation
- Valid-range filtering consistent with HLS_VI_Pipeline configuration
- Parallel tile extraction via `concurrent.futures.ProcessPoolExecutor`
- Automatic timestamped log file written to `--output-dir`

---

## Performance

Tile-level extraction is parallelized using `concurrent.futures.ProcessPoolExecutor`. Each
NetCDF tile is processed in a dedicated worker process.

Control the worker count with `--workers N` (default: 8). Set to 1 for fully sequential
processing — useful for debugging or on memory-constrained machines.

| Workers | 23 tiles | Approx. time |
|---------|----------|--------------|
| 1 (sequential) | — | ~10 min |
| 4 | — | ~2.5 min |
| 8 | — | ~1.5 min |

---

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/stephenconklin/VI_Phenology.git
cd VI_Phenology
```

### 2. Create the Conda Environment

```bash
conda env create -f environment.yml
conda activate vi_phenology
```

### 3. Create Your Local Configuration

Configuration is split across two files:

| File | Purpose | Committed to git? |
|---|---|---|
| `config.env` | Base template — all variables with defaults and inline documentation | Yes |
| `config.local.env` | Your project-specific overrides (actual paths, active pipeline) | **No** (gitignored) |
| `run_phenology.sh` | Execution engine — sources both files, dispatches the selected pipeline | Yes |

Copy `config.env` to `config.local.env` and set your paths and active pipeline:

```bash
cp config.env config.local.env
# then edit config.local.env in your editor
```

`config.local.env` only needs to contain the variables you are overriding — everything
else falls back to `config.env`. A minimal `config.local.env` looks like:

```bash
# config.local.env — my BioSCape project
PIPELINE="netcdf_datacube"

OUTPUT_DIR="/path/to/my/outputs"
NETCDF_DIR="/path/to/my/netcdfs"
VI="NDVI"
SHAPEFILE="/path/to/roi.gpkg"
SHAPEFILE_FIELD="box_nr"
```

To maintain multiple project configurations, keep named copies (e.g.
`config.local.BioSCape.env`, `config.local.Durango.env`) alongside `config.local.env`.
Copy or symlink the active one before each run.

---

## Quickstart

### Recommended — `run_phenology.sh`

After creating `config.local.env` (see Setup above):

```bash
./run_phenology.sh
```

All variables are documented with inline comments in `config.env`.

### Direct CLI — netCDF Datacube Pipeline

Run this first. Clips source tiles to your polygon regions and produces
`*_datacube.nc` files that feed both downstream pipelines:

```bash
python src/netcdf_datacube_extract.py \
  --netcdf-dir /path/to/netcdfs \
  --vi NDVI EVI2 \
  --shapefile /path/to/roi.gpkg \
  --shapefile-field Name \
  --output-dir ./outputs \
  --workers 8
```

```bash
python src/netcdf_datacube_extract.py --help
```

For full details on the datacube pipeline, see [netCDF Datacube Pipeline](datacube.md).

### Direct CLI — Phenology Pipeline

**Datacube input mode** — reads pre-clipped datacubes produced by `netcdf_datacube`
(recommended; skips tile discovery on every re-run):

```bash
python src/vi_phenology.py \
  --input-datacubes /path/to/outputs/my_shapefile_stem \
  --vi NDVI \
  --output-dir ./outputs \
  --smooth-method whittaker \
  --smooth-lambda 100 \
  --plot-style combined \
  --plot-format png html \
  --metrics
```

**Standard mode** — discovers and clips source tiles on each run (single-pass, no
intermediate datacubes needed):

```bash
python src/vi_phenology.py \
  --netcdf-dir /path/to/netcdfs \
  --vi NDVI EVI2 \
  --shapefile /path/to/roi.gpkg \
  --shapefile-field Name \
  --output-dir ./outputs \
  --smooth-method whittaker \
  --smooth-lambda 100 \
  --plot-style combined \
  --plot-format png html \
  --metrics \
  --workers 8
```

```bash
python src/vi_phenology.py --help
```

For the full argument reference, see the [Phenology Pipeline CLI Reference](cli_reference.md).

### Direct CLI — Pixel Phenology Pipeline

Reads datacubes produced by `netcdf_datacube` and computes 19 per-pixel metric maps:

```bash
python src/pixel_phenology_extract.py \
  --input-datacubes /path/to/NDVI_MyRegion_datacube.nc \
  --output-dir ./pixel_metrics \
  --smooth-lambda 100 \
  --min-valid-obs 20 \
  --min-valid-obs-per-year 5 \
  --workers 8
```

```bash
python src/pixel_phenology_extract.py --help
```

### Direct CLI — datacube_to_geotiff Pipeline

Reads datacubes produced by `netcdf_datacube` and writes multi-band GeoTiffs:

```bash
python src/datacube_to_geotiff.py \
  --input-datacubes /path/to/NDVI_MyRegion_datacube.nc \
  --output-dir ./geotiff_stats \
  --workers 4
```

```bash
python src/datacube_to_geotiff.py --help
```

For full details, see [datacube_to_geotiff Pipeline](datacube_to_geotiff.md).

---

## Authors

**Stephen Conklin**, Geospatial Analyst — Pipeline architecture, orchestration, and all original code.
[https://github.com/stephenconklin](https://github.com/stephenconklin)

**G. Burch Fisher, PhD**, Research Scientist — Conceptual guidance and original code adapted for:
- `src/pixel_phenology_extract.py` (Per-pixel phenological metric extraction from CF-1.8 datacubes)

**AI Assistance:** This tool was developed with the assistance of Anthropic Claude / Claude Code. These tools assisted
with code generation and refinement under the direction and review of the author.

---

## License

MIT
