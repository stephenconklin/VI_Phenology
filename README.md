# VI_Phenology

Phenology analysis tool for vegetation index (VI) time-series data. Reads CF-1.8 compliant
NetCDF files, extracts and smooths temporal profiles, computes phenological metrics, and
generates publication-ready plots.

Designed to work natively with output from [HLS_VI_Pipeline](https://github.com/stephenconklin/HLS_VI_Pipeline),
but accepts any CF-1.8 NetCDF with `time`, `y`, `x` dimensions and a VI data variable.

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

- Layered processing pipeline: raw observations → daily time axis → smoothed gap-filled series → phenological metrics
- Multiple smoothing methods: Savitzky-Golay, LOESS, linear interpolation, harmonic fit
- Spatial subsetting via any GeoPandas-readable vector format (`.shp`, `.gpkg`, `.geojson`, etc.)
- Per-feature splitting: produce one independent time series per attribute value in a shapefile
- Multiple shapefiles in a single run, each with its own optional field splitting
- Phenological metrics: SOS, POS, EOS, LOS, IVI, greening rate, senescence rate
- Combined per-shapefile metrics CSV when splitting by attribute field
- Output formats: Parquet (time series), CSV (metrics), PNG static + interactive HTML plots
- Date range filtering applied at the NetCDF level before any aggregation
- Valid-range filtering consistent with HLS_VI_Pipeline configuration
- Parallel tile extraction via `concurrent.futures.ProcessPoolExecutor`

---

## Performance

Tile-level extraction is parallelized using `concurrent.futures.ProcessPoolExecutor`. Each
NetCDF tile is processed in a dedicated worker process; the main process pools pixel statistics
across tiles to compute the correct weighted mean and standard deviation.

Control the worker count with `--workers N` (default: 8). Set to 1 for fully sequential
processing — useful for debugging or on memory-constrained machines.

| Workers | 23 tiles | Approx. time |
|---------|----------|--------------|
| 1 (sequential) | — | ~10 min |
| 4 | — | ~2.5 min |
| 8 | — | ~1.5 min |

---

## Setup

```bash
conda env create -f environment.yml
conda activate vi_phenology
```

---

## Usage

### Quickstart — `run_phenology.sh`

The recommended way to run the tool. Edit the configuration variables at the top of the script
to match your paths and options, then:

```bash
./run_phenology.sh
```

All parameters are documented with inline comments inside the script.

### Direct CLI

```bash
python vi_phenology.py \
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
python vi_phenology.py --help
```

---

## CLI Reference

### Input

| Argument | Default | Description |
|----------|---------|-------------|
| `--netcdf-dir PATH` | *(required)* | Directory containing `T{TILE}_{VI}.nc` files |
| `--vi VI [VI ...]` | `NDVI` | Vegetation indices to process: `NDVI` `EVI2` `NIRv` |
| `--shapefile PATH [PATH ...]` | — | Shapefile(s) for spatial subsetting. Omit to process the full NetCDF extent. |
| `--shapefile-field FIELD [FIELD ...]` | — | Attribute field(s) to split shapefile(s) by — one per shapefile in positional order. Use `none` to dissolve a specific file instead of splitting it. Count must match `--shapefile` exactly. |
| `--valid-range-ndvi MIN,MAX` | `-1,1` | Valid range for NDVI |
| `--valid-range-evi2 MIN,MAX` | `-1,2` | Valid range for EVI2 |
| `--valid-range-nirv MIN,MAX` | `-0.5,1` | Valid range for NIRv |

### Output

| Argument | Default | Description |
|----------|---------|-------------|
| `--output-dir PATH` | *(required)* | Output directory (created if it does not exist) |

### Smoothing

| Argument | Default | Description |
|----------|---------|-------------|
| `--smooth-method METHOD` | `savgol` | Smoothing method: `savgol` `loess` `linear` `harmonic` `none` |
| `--smooth-window DAYS` | `15` | Smoothing window in days (savgol and loess) |
| `--smooth-polyorder N` | `3` | Polynomial order for Savitzky-Golay (must be < window length) |

### Phenological Metrics

| Argument | Default | Description |
|----------|---------|-------------|
| `--metrics` | off | Compute and export phenological metrics (requires a smoothing method) |
| `--sos-threshold FRACTION` | `0.20` | Amplitude fraction for SOS/EOS detection (e.g. `0.20` = 20% of annual amplitude) |
| `--year-start-doy DOY` | `1` | Day of year to begin each annual phenology window (1–365). Use `1` for Northern Hemisphere (Jan 1). Use `182` (Jul 1) or another austral-winter DOY for Southern Hemisphere data. |

### Plotting

| Argument | Default | Description |
|----------|---------|-------------|
| `--plot-style STYLE` | `combined` | `raw`: observation scatter only · `smooth`: smooth curve only · `combined`: smooth + scatter |
| `--plot-format FORMAT [FORMAT ...]` | `png` | Output format(s): `png` and/or `html` |

### Performance

| Argument | Default | Description |
|----------|---------|-------------|
| `--workers N` | `8` | Parallel worker processes for tile extraction. Set to `1` for sequential mode. |
| `--start-date YYYY-MM-DD` | — | Only include observations on or after this date |
| `--end-date YYYY-MM-DD` | — | Only include observations on or before this date |

### Diagnostics

| Argument | Default | Description |
|----------|---------|-------------|
| `--log-level LEVEL` | `INFO` | Verbosity: `DEBUG` `INFO` `WARNING` `ERROR` |
| `--no-logfile` | off | Disable automatic log file creation in `--output-dir` |

---

## Spatial Input

`--shapefile` accepts any vector format readable by GeoPandas/Fiona:

| Format | Extension(s) |
|--------|-------------|
| ESRI Shapefile | `.shp` (`.dbf`, `.prj`, `.shx` sidecars must be present) |
| GeoPackage | `.gpkg` |
| GeoJSON | `.geojson`, `.json` |
| KML / KMZ | `.kml`, `.kmz` |
| FlatGeobuf | `.fgb` |
| File Geodatabase | `.gdb` |

Omit `--shapefile` entirely to process the full spatial extent of the NetCDF files.

### Multiple shapefiles

Pass multiple paths to produce an independent time series and output set per shapefile:

```bash
python vi_phenology.py \
  --shapefile /path/to/region1.gpkg /path/to/region2.geojson \
  ...
```

### Per-feature splitting — `--shapefile-field`

To produce one independent time series per feature value in a shapefile, provide the name
of an attribute column:

```bash
python vi_phenology.py \
  --shapefile biomes.gpkg \
  --shapefile-field Name \
  ...
```

Each unique non-null value in the `Name` column becomes its own region. Field values are
sanitized for filesystem use: spaces and special characters are replaced with underscores.
For example, `"Cape Fynbos"` becomes the region label (and subdirectory) `Cape_Fynbos`.

When `--shapefile-field` is omitted, all features in a file are dissolved into a single
geometry and the shapefile's filename stem is used as the region label.

### Multiple shapefiles with per-field splitting

When passing multiple shapefiles, provide one field name per shapefile in the same positional
order. Use `none` to dissolve a specific shapefile rather than splitting it:

```bash
# Split first shapefile by 'box_nr', dissolve the second
python vi_phenology.py \
  --shapefile flights.shp tiles.geojson \
  --shapefile-field box_nr none \
  ...

# Split both shapefiles by their respective fields
python vi_phenology.py \
  --shapefile flights.shp tiles.geojson \
  --shapefile-field box_nr tile_id \
  ...
```

The number of `--shapefile-field` values must match the number of `--shapefile` paths exactly.
A mismatch is a hard error.

In `run_phenology.sh`, set `SHAPEFILE_FIELD` to activate field splitting:

```bash
SHAPEFILE="flights.shp tiles.geojson"
SHAPEFILE_FIELD="box_nr none"   # comment out entirely to dissolve all
```

### Region label derivation

The region label controls both the output subdirectory name and all output file stems:

| Scenario | Region label |
|---|---|
| No shapefile | `full_extent` |
| Shapefile, no `--shapefile-field` | Shapefile filename stem (e.g. `biomes`) |
| Shapefile + `--shapefile-field` | Sanitized field value (e.g. `Cape_Fynbos`) |

---

## Smoothing

Smoothing is applied using an **obs-first** strategy: the filter is applied to raw observation
dates only (not to the gap-filled daily series), then the result is interpolated to a complete
daily axis. This avoids artifacts at gap boundaries.

| Method | Flag | Notes |
|--------|------|-------|
| Savitzky-Golay | `savgol` | Default. Uses `--smooth-window` + `--smooth-polyorder`. Bins observations to median inter-observation spacing before filtering. |
| LOESS/LOWESS | `loess` | Adaptive to irregular observation spacing. Uses `--smooth-window`. |
| Linear | `linear` | Gap-fill only — connects observations with straight lines, no smoothing applied. |
| Harmonic | `harmonic` | Fourier fit: `VI(t) = a₀ + Σ [aₖ cos(2πkt/T) + bₖ sin(2πkt/T)]` with T=365.25 days. Best for multi-year trend decomposition. |
| None | `none` | Skip Layer 2 entirely. Only Layers 0+1 (raw + daily reindex) are produced. `--metrics` requires a smoothing method. |

Each smoothed value carries a provenance flag in `vi_smooth_flag`:
- `observed` — on an actual observation date
- `interpolated` — between first and last observation
- `extrapolated` — before first or after last observation

---

## Phenological Metrics

Computed per year, per region, per VI from the smoothed daily series. Enable with `--metrics`.

### Metrics

| Metric | Column | Description |
|--------|--------|-------------|
| SOS | `sos_date`, `sos_doy` | Start of Season: first date VI crosses the amplitude threshold going up |
| POS | `pos_date`, `pos_doy`, `pos_value` | Peak of Season: date and value of annual maximum |
| EOS | `eos_date`, `eos_doy` | End of Season: last date VI is still above the amplitude threshold |
| LOS | `los_days` | Length of Season: EOS − SOS in days |
| IVI | `ivi` | Integrated VI: trapezoidal area under the smooth curve between SOS and EOS |
| Greening rate | `greening_rate` | Mean slope (VI/day) from SOS to POS |
| Senescence rate | `senescence_rate` | Mean slope (VI/day) from POS to EOS (negative for a declining curve) |

### SOS/EOS threshold

SOS and EOS are determined relative to the annual amplitude:

```
baseline  = annual minimum VI
amplitude = annual peak − baseline
threshold = baseline + sos_threshold × amplitude
```

`--sos-threshold` controls the fraction of amplitude (default `0.20` = 20%).

### Annual windows

`--year-start-doy` sets the day of year at which each annual phenology window begins
(default `1` = January 1). The correct value depends on **biome and rainfall regime**, not
simply on hemisphere:

| Biome / regime | Peak VI | VI minimum | Recommended `--year-start-doy` |
|---|---|---|---|
| Northern Hemisphere temperate | Jun–Aug | Dec–Jan | `1` (January 1) |
| Southern Hemisphere summer-rainfall (Savanna, Highveld) | Dec–Jan | Jun–Jul | `182` (July 1) |
| Southern Hemisphere winter-rainfall (Cape fynbos, Mediterranean) | Jun–Aug | Dec–Jan | `1` (January 1) |

The rule is: **place the window boundary at the VI minimum** for the target region. Placing
it at or near the seasonal peak will cause SOS/EOS detection to fail or produce nonsensical
results.

---

## Output

### File structure

Outputs are organized into per-region subdirectories when shapefiles are provided:

```
outputs/                                         ← --output-dir
├── Cape_Fynbos/                                 ← one subdirectory per region
│   ├── NDVI_Cape_Fynbos_timeseries.parquet
│   ├── NDVI_Cape_Fynbos_metrics.csv
│   ├── NDVI_Cape_Fynbos_timeseries.png
│   ├── NDVI_Cape_Fynbos_timeseries.html
│   ├── NDVI_Cape_Fynbos_annual.png
│   ├── NDVI_Cape_Fynbos_annual.html
│   ├── NDVI_Cape_Fynbos_anomaly.png
│   └── Cape_Fynbos_multi_vi.png
├── Succulent_Karoo/
│   └── ...
└── NDVI_biomes_metrics.csv                      ← combined metrics (one per shapefile × VI)
```

When no shapefile is provided (full-extent mode), all outputs go directly into `--output-dir`
and the region label is `full_extent`.

### Output files

| File | Description |
|------|-------------|
| `{VI}_{region}_timeseries.parquet` | Daily time series: raw + smoothed VI columns, provenance flags |
| `{VI}_{region}_metrics.csv` | Phenological metrics per year for this region |
| `{VI}_{shapefile_stem}_metrics.csv` | Combined metrics for all regions in a shapefile (when `--shapefile-field` is set) |
| `{VI}_{region}_timeseries.png/html` | Full temporal range: smooth curve + observation scatter + ±1 std band |
| `{VI}_{region}_annual.png/html` | VI vs day-of-year, one line per year |
| `{VI}_{region}_anomaly.png/html` | Per-year deviation from multi-year mean (requires ≥ 2 calendar years) |
| `{region}_multi_vi.png/html` | Side-by-side NDVI / EVI2 / NIRv comparison (requires > 1 VI) |

### Parquet schema

| Column | Type | Description |
|--------|------|-------------|
| `date` | datetime64[ns] | Calendar date |
| `vi_raw` | float32 | Spatially aggregated VI on observation days; NaN on non-observation days |
| `vi_count` | int32 | Valid pixel count contributing to `vi_raw`; 0 on non-observation days |
| `vi_std` | float32 | Spatial standard deviation of valid pixels; NaN on non-observation days |
| `vi_daily` | float32 | Daily reindex of `vi_raw` (NaN gaps preserved) |
| `vi_smooth` | float32 | Smoothed, gap-filled daily values (absent when `--smooth-method none`) |
| `vi_smooth_flag` | str | Provenance: `observed` · `interpolated` · `extrapolated` |

### Combined metrics CSV

When `--shapefile-field` is set, a combined metrics CSV is written at the root of `--output-dir`
for each shapefile × VI combination. This file contains one row per (region, year), with the
existing `region` column identifying the source field value. Individual per-region CSVs are
still written alongside.

File naming: `{VI}_{shapefile_stem}_metrics.csv`

### Date range filtering

Use `--start-date` and `--end-date` to restrict processing to a specific time window. Filtering
is applied at the NetCDF level before any spatial aggregation, keeping memory use low even on
large multi-year datasets:

```bash
python vi_phenology.py \
  --netcdf-dir /path/to/netcdfs \
  --vi NDVI \
  --output-dir ./outputs \
  --start-date 2021-01-01 \
  --end-date   2023-12-31 \
  --metrics
```

Either bound can be omitted to apply only a lower or upper limit.

---

## Logging

All progress, warnings, and errors are written to the terminal (stderr) using Python's standard
`logging` module. Each message includes a timestamp, severity level, and source module:

```
2026-03-03 15:31:00  INFO      [extract]  Tile T34HBH_NDVI.nc: 56 obs dates, 56 with valid pixels, 78,949,714 total valid pixel-obs
2026-03-03 15:37:35  INFO      [metrics]  NDVI / Cape_Fynbos / 2025: SOS=2025-10-02 (DOY 275), POS=2025-12-31 (DOY 365, val=0.5659), LOS=90 d
2026-03-03 15:37:35  WARNING   [extract]  Tile T34HXX_NDVI.nc: no overlap with ROI — skipping
```

### Log file

By default, a timestamped log file is automatically written to `--output-dir` alongside all
other outputs:

```
outputs/vi_phenology_20260303_153100.log
```

The log file receives the same messages as the terminal at the same verbosity level. To disable
automatic log file creation:

```bash
python vi_phenology.py --no-logfile ...
```

### Verbosity levels

| Level | What you see |
|-------|-------------|
| `WARNING` | Only warnings and errors |
| `INFO` *(default)* | Per-tile pixel counts, smoothing stats, metric values, saved file paths |
| `DEBUG` | Adds clip geometry details, bin sizes, S-G window calculations, per-reindex statistics |

```bash
python vi_phenology.py --log-level WARNING ...   # quiet
python vi_phenology.py --log-level DEBUG ...     # full diagnostics
```

---

## Authors

**Stephen Conklin**, Geospatial Analyst — Pipeline architecture, orchestration, and all original code.
[https://github.com/stephenconklin](https://github.com/stephenconklin)

### AI Assistance

This tool was developed with the assistance of Anthropic Claude / Claude Code. These tools assisted
with code generation and refinement under the direction and review of the author.

---

## License

MIT
