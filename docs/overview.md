# VI Phenology Guide

[![Python](https://img.shields.io/badge/python-3.10--3.12-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS-lightgrey.svg)]()

A three-pipeline vegetation index analysis toolkit. Reads CF-1.8 compliant NetCDF files
and delivers aggregated phenology time series, per-pixel CF-1.8 datacubes clipped to
polygon regions, or per-pixel phenological metric maps from existing datacubes.

Designed to work natively with output from [HLS_VI_Pipeline](https://github.com/stephenconklin/HLS_VI_Pipeline),
but accepts any CF-1.8 NetCDF with `time`, `y`, `x` dimensions and a VI data variable.

---

## Three Pipelines

| Pipeline | Purpose | Select in `run_phenology.sh` |
|---|---|---|
| **phenology** | ROI-mean time series, smoothing, phenological metrics, plots | `PIPELINE="phenology"` (default) |
| **netcdf_datacube** | Per-pixel CF-1.8 datacubes clipped to polygon regions | `PIPELINE="netcdf_datacube"` |
| **pixel_phenology** | 18 per-pixel metric maps from existing datacubes | `PIPELINE="pixel_phenology"` |

The `phenology` and `netcdf_datacube` pipelines share the same input configuration:
`NETCDF_DIR`, `VI`, `SHAPEFILE`, `SHAPEFILE_FIELD`, `VALID_RANGE_*`, `WORKERS`,
`START_DATE`, `END_DATE`. The `pixel_phenology` pipeline takes `--input-datacubes`
(paths to `*_datacube.nc` files produced by `netcdf_datacube`) as its primary input
and does not use `--netcdf-dir` or shapefiles.

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

### Phenology Pipeline
- Layered processing: raw observations → daily time axis → smoothed gap-filled series → phenological metrics
- Multiple smoothing methods: Savitzky-Golay, LOESS, linear interpolation, harmonic fit, Whittaker (`--smooth-lambda`)
- Core phenological metrics: SOS, POS, EOS, LOS, IVI, greening rate, senescence rate
- Extended metrics: `floor_ndvi`, `ceiling_ndvi`, `season_length_days`, `greenup_rate`, `n_peaks`, `peak_separation_days`, `relative_peak_amplitude`, `valley_depth`, `cv`
- Observation count thresholds: `--min-valid-obs` (skip region if total obs < N), `--min-valid-obs-per-year` (skip annual window if obs < N)
- Pixel sampling: `--sample-pixels`, `--random-seed`, `--min-ndvi-mean`, `--min-quality-frac`
- Datacube input mode: `--input-datacubes` accepts pre-clipped datacubes (individual files or a directory) — faster for repeated runs without re-running tile discovery
- Annual DOY overlay plot, full time-series plot, anomaly plot, multi-VI comparison panel
- Granular output toggles — disable any combination of outputs in `run_phenology.sh`
- Output formats: CSV (observations and metrics), PNG + interactive HTML plots
- Combined per-shapefile observations CSV and metrics CSV when splitting by attribute field

### netCDF Datacube Pipeline
- Per-pixel CF-1.8 compliant datacubes clipped to polygon boundaries
- Same-CRS multi-tile merging: pixel-perfect, memory-bounded mosaic — no resampling
- Cross-CRS multi-tile merging: bilinear reprojection of minority tiles to dominant CRS before merge
- Configurable per-tile or merged output via `MERGE_SAME_CRS` / `MERGE_CROSS_CRS`
- Full CF-1.8 metadata: `Conventions`, `history`, `tiles`, `region`, `vi`, `target_crs`, `resampling_method`

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

---

## Quickstart

### Recommended — `run_phenology.sh`

Edit the configuration variables at the top of the script to match your paths, select
your pipeline (`PIPELINE="phenology"`, `PIPELINE="netcdf_datacube"`, or
`PIPELINE="pixel_phenology"`), then:

```bash
./run_phenology.sh
```

All parameters are documented with inline comments inside the script.

### Direct CLI — Phenology Pipeline

```bash
python src/vi_phenology.py \
  --netcdf-dir /path/to/netcdfs \
  --vi NDVI EVI2 \
  --shapefile /path/to/roi.gpkg \
  --output-dir ./outputs \
  --smooth-method savgol \
  --smooth-window 15 \
  --smooth-polyorder 3 \
  --plot-style combined \
  --plot-format png html \
  --metrics \
  --workers 8
```

```bash
python src/vi_phenology.py --help
```

For the full argument reference, see the [CLI Reference](cli_reference.md).

### Direct CLI — netCDF Datacube Pipeline

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

### Direct CLI — Pixel Phenology Pipeline

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

---

## Authors

**Stephen Conklin**, Geospatial Analyst — Pipeline architecture, orchestration, and all original code.
[https://github.com/stephenconklin](https://github.com/stephenconklin)

**AI Assistance:** This tool was developed with the assistance of Anthropic Claude / Claude Code. These tools assisted
with code generation and refinement under the direction and review of the author.

---

## License

MIT
