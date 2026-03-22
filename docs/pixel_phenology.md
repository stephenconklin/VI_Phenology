# Pixel Phenology Pipeline

The `pixel_phenology` pipeline reads per-pixel datacubes produced by the
[netCDF Datacube Pipeline](datacube.md) and computes 19 phenological metrics for every
pixel on the same spatial grid. The output is one CF-1.8 NetCDF per (VI, region) containing
one spatial layer per metric, plus a summary CSV with spatial statistics per metric.

---

## When to Use This Pipeline

Use the pixel phenology pipeline when you need:
- Spatially explicit phenological metric maps (one value per 30-m pixel per metric)
- Per-pixel interannual variability (std, CV, peak range) across a multi-year record
- Bimodality detection (n_peaks, peak separation, valley depth) mapped across a region

Use the [phenology pipeline](cli_reference.md) when you need ROI-mean time series,
smoothed curves aggregated over a region, or interactive HTML plots.

The pixel phenology pipeline always requires datacubes from the `netcdf_datacube` pipeline
as its input — run that pipeline first if datacubes do not yet exist.

---

## Selecting the Pipeline

In `run_phenology.sh`, set the `PIPELINE` variable at the top of the file:

```bash
PIPELINE="pixel_phenology"   # per-pixel metric maps (this pipeline)
PIPELINE="netcdf_datacube"   # produce the input datacubes first
PIPELINE="phenology"         # ROI-mean time series, metrics, plots
```

---

## Processing Model

```
For each input datacube ({VI}_{region_label}_datacube.nc):

  1. Open with xarray (lazy); apply optional --start-date / --end-date filter
     Warn if uncompressed array size > 8 GB

  2. Load full (time, y, x) array into RAM as float32

  3. Precompute λ D^T D Whittaker penalty matrix once
     (all pixels share the same n_days — computed once, reused for every pixel)

  4. ThreadPoolExecutor: dispatch y-row chunks (50 rows per chunk)
     Each thread, for every pixel in its chunk:
       a. Apply valid-range mask (vi_min, vi_max → NaN)
       b. Skip pixel if valid obs < min_valid_obs → all metrics = NaN
       c. Map valid observations onto the daily grid (NaN → weight 0)
       d. Solve (W + λ D^T D) z = W y  [Whittaker smooth]
       e. For each year with ≥ min_valid_obs_per_year observations:
            peak NDVI + DOY, integrated NDVI, floor, ceiling,
            green-up rate, season length, bimodality
       f. Aggregate across years: mean / std for annual metrics
       g. Compute whole-series metrics: CV, interannual peak range + std

  5. Assemble (19, n_y, n_x) output array from all chunks

  6. Write {VI}_{region_label}_pixel_metrics.nc  (CF-1.8, zlib complevel=4)
  7. Write {VI}_{region_label}_pixel_metrics_summary.csv
```

**Threading rationale:** `scipy.sparse.linalg.spsolve` releases the Python GIL, so
`ThreadPoolExecutor` achieves true multi-core parallelism while all threads share the
single in-memory array without inter-process serialisation overhead.

**Memory note:** The full `(time, y, x)` float32 array is loaded into RAM once. Use
`--start-date` / `--end-date` to reduce the time axis if the datacube is large.

---

## Input

`PIXEL_INPUT_DATACUBES` / `--input-datacubes` accepts two forms:

| Form | Example |
|---|---|
| **Directory** — all `*_datacube.nc` files found recursively | `PIXEL_INPUT_DATACUBES="${OUTPUT_DIR}/LVIS_flightboxes_final"` |
| **File path(s)** — space-separated list of individual files | `PIXEL_INPUT_DATACUBES="/path/to/NDVI_G5_1_datacube.nc /path/to/NDVI_G5_12_datacube.nc"` |

When a directory is given, files are discovered with `find … -name "*_datacube.nc" | sort`
and an error is raised if none are found.

VI and `region_label` are parsed from each filename:
`{VI}_{region_label}_datacube.nc` → first underscore-separated token = VI, remainder = region_label.

---

## Whittaker Smoothing

The pipeline uses the Whittaker smoother exclusively. The penalty matrix
`λ D^T D` is precomputed once for the full time axis and reused for every pixel,
making per-pixel solve cost dominated by the sparse linear system rather than matrix
construction.

The smoother solves:

```
(W + λ D^T D) z = W y
```

where `W` is the diagonal weight matrix (1 = observed, 0 = gap), `D` is the
2nd-order difference matrix, and `λ` is the smoothing strength.

| `PIXEL_SMOOTH_LAMBDA` | CLI `--smooth-lambda` | Default | Effect |
|---|---|---|---|
| `10–50` | — | — | Tight; follows observations closely |
| `100` | — | ✓ default | Balanced smoothing |
| `300–1000` | — | — | Very smooth; coarse biome-level characterisation |

If the solver fails for a pixel, that pixel falls back to a zero-filled result and its
metrics are set to NaN.

---

## Metrics (19)

All metrics are derived from the per-pixel Whittaker-smoothed daily series.
Floor and ceiling are computed from the curve's annual minimum and maximum —
no biome-specific DOY windows are used.

### Annual metrics (mean + std aggregated across years)

| Metric band | Description |
|---|---|
| `peak_ndvi_mean` | Mean annual peak VI value |
| `peak_ndvi_std` | Interannual std of peak VI |
| `peak_doy_mean` | Mean day-of-year of annual peak |
| `peak_doy_std` | Interannual std of peak DOY |
| `integrated_ndvi_mean` | Mean integrated VI (trapezoidal, NDVI-days per year) |
| `integrated_ndvi_std` | Interannual std of integrated VI |
| `greenup_rate_mean` | Mean green-up rate: `(ceiling − floor) / (peak_date − floor_date)` in NDVI/day |
| `greenup_rate_std` | Interannual std of green-up rate |
| `floor_ndvi_mean` | Mean annual curve minimum (dry-season trough) |
| `ceiling_ndvi_mean` | Mean annual curve maximum (= peak VI) |
| `season_length_mean` | Mean days above `floor + season_threshold × amplitude` |
| `season_length_std` | Interannual std of season length |
| `n_peaks_mean` | Mean number of peaks per year detected by `scipy.signal.find_peaks` |
| `peak_separation_mean` | Mean DOY distance between the two tallest peaks (NaN if n_peaks < 2) |
| `relative_peak_amplitude_mean` | Mean `min(h1, h2) / max(h1, h2)` between two tallest peaks (NaN if n_peaks < 2) |
| `valley_depth_mean` | Mean normalised trough depth: `((h1+h2)/2 − valley) / ((h1+h2)/2)` (NaN if n_peaks < 2) |

### Whole-series metrics (single value per pixel)

| Metric band | Description |
|---|---|
| `cv` | Coefficient of variation of raw (unsmoothed) valid observations across the full record |
| `interannual_peak_range` | `max(annual_peak) − min(annual_peak)` across all years |
| `interannual_peak_std` | Std of annual peak VI across all years |

### Observation thresholds

| Parameter | `run_phenology.sh` | CLI flag | Default | Effect |
|---|---|---|---|---|
| Total obs | `PIXEL_MIN_VALID_OBS` | `--min-valid-obs` | `20` | Pixel skipped entirely (all metrics = NaN) if total valid obs < N |
| Per-year obs | `PIXEL_MIN_VALID_OBS_PER_YEAR` | `--min-valid-obs-per-year` | `5` | Annual window skipped if obs < N; does not contribute to mean/std |

### Bimodality parameters

| Parameter | `run_phenology.sh` | CLI flag | Default | Description |
|---|---|---|---|---|
| Peak prominence | `PIXEL_PEAK_PROMINENCE` | `--peak-prominence` | `0.05` | Minimum NDVI prominence for a peak to be counted |
| Peak min distance | `PIXEL_PEAK_MIN_DISTANCE` | `--peak-min-distance` | `45` | Minimum separation in days between detected peaks |
| Season threshold | `PIXEL_SEASON_THRESHOLD` | `--season-threshold` | `0.20` | Amplitude fraction above floor for season-length calculation |

---

## Output

### File Naming

| File | Location |
|---|---|
| `{VI}_{region_label}_pixel_metrics.nc` | `{PIXEL_OUTPUT_DIR}/{region_label}/` |
| `{VI}_{region_label}_pixel_metrics_summary.csv` | `{PIXEL_OUTPUT_DIR}/{region_label}/` |
| `pixel_phenology_{YYYYMMDD_HHMMSS}.log` | `{PIXEL_OUTPUT_DIR}/` |

### File Structure Example

```
pixel_metrics/                                         ← PIXEL_OUTPUT_DIR
├── pixel_phenology_20260320_153100.log
├── G5_1/
│   ├── NDVI_G5_1_pixel_metrics.nc
│   └── NDVI_G5_1_pixel_metrics_summary.csv
└── G5_12/
    ├── NDVI_G5_12_pixel_metrics.nc
    └── NDVI_G5_12_pixel_metrics_summary.csv
```

### Output NetCDF Structure

Each output file is a CF-1.8 compliant netCDF4 with:

**Dimensions:**
- `y` — northing in meters (UTM), same grid as input datacube
- `x` — easting in meters (UTM), same grid as input datacube

**Variables:**
- One float32 `(y, x)` variable per metric — 19 total, zlib-compressed (level 4)
- `spatial_ref` — scalar CRS container (copied from input datacube)

**Global attributes:**

| Attribute | Description |
|---|---|
| `Conventions` | `'CF-1.8'` |
| `history` | Creation timestamp |
| `region` | Region label |
| `vi` | VI name (e.g. `'NDVI'`) |
| `source_datacube` | Absolute path to the input datacube |
| `whittaker_lambda` | λ value used for smoothing |
| `peak_prominence` | Bimodality prominence threshold |
| `peak_min_distance_days` | Bimodality minimum peak separation |
| `season_threshold` | Season-length amplitude fraction |
| `min_valid_obs` | Minimum obs threshold used |
| `start_date` / `end_date` | Present only when `--start-date` / `--end-date` were set |

### Summary CSV

`{VI}_{region_label}_pixel_metrics_summary.csv` contains one row per metric with spatial
statistics computed across all non-NaN pixels:

| Column | Description |
|---|---|
| `metric` | Metric name |
| `mean` | Spatial mean across valid pixels |
| `std` | Spatial std |
| `p05` | 5th percentile |
| `p50` | Median |
| `p95` | 95th percentile |
| `n_valid_pixels` | Count of pixels with a non-NaN value for this metric |

---

## CLI Reference

```
python src/pixel_phenology_extract.py --help
```

| Argument | `run_phenology.sh` variable | Default | Description |
|---|---|---|---|
| `--input-datacubes PATH [PATH ...]` | `PIXEL_INPUT_DATACUBES` | *(required)* | Path(s) to `*_datacube.nc` files, or a directory (all `*_datacube.nc` files found recursively) |
| `--output-dir PATH` | `PIXEL_OUTPUT_DIR` | *(required)* | Root output directory |
| `--smooth-lambda LAMBDA` | `PIXEL_SMOOTH_LAMBDA` | `100.0` | Whittaker smoothing strength λ (10–1000; higher = smoother) |
| `--min-valid-obs N` | `PIXEL_MIN_VALID_OBS` | `20` | Min valid obs over full record; fewer → pixel set to NaN |
| `--min-valid-obs-per-year N` | `PIXEL_MIN_VALID_OBS_PER_YEAR` | `5` | Min valid obs per annual window; fewer → that year skipped |
| `--peak-prominence NDVI` | `PIXEL_PEAK_PROMINENCE` | `0.05` | Min NDVI prominence for bimodality peak detection |
| `--peak-min-distance DAYS` | `PIXEL_PEAK_MIN_DISTANCE` | `45` | Min separation in days between detected peaks |
| `--season-threshold FRACTION` | `PIXEL_SEASON_THRESHOLD` | `0.20` | Amplitude fraction above floor for season-length calculation |
| `--valid-range-ndvi MIN,MAX` | `VALID_RANGE_NDVI` | `-1,1` | Valid range for NDVI |
| `--valid-range-evi2 MIN,MAX` | `VALID_RANGE_EVI2` | `-1,2` | Valid range for EVI2 |
| `--valid-range-nirv MIN,MAX` | `VALID_RANGE_NIRV` | `-0.5,1` | Valid range for NIRv |
| `--workers N` | `WORKERS` | `8` | Number of parallel threads for pixel processing |
| `--start-date YYYY-MM-DD` | `START_DATE` | — | Include only time steps on or after this date |
| `--end-date YYYY-MM-DD` | `END_DATE` | — | Include only time steps on or before this date |
| `--log-level LEVEL` | — | `INFO` | `DEBUG` `INFO` `WARNING` `ERROR` |
