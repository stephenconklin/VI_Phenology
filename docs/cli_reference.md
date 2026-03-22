# Phenology Pipeline — CLI Reference

This page covers the **phenology pipeline** CLI (`src/vi_phenology.py`).

| Other pipeline CLIs | |
|---|---|
| netCDF datacube pipeline | [netCDF Datacube Pipeline → CLI Reference](datacube.md#cli-reference) |
| Pixel phenology pipeline | [Pixel Phenology Pipeline → CLI Reference](pixel_phenology.md#cli-reference) |

The recommended way to run any pipeline is via `run_phenology.sh` — set variables in
`config.local.env` and run `./run_phenology.sh`. All variables are documented with inline
comments in `config.env`. See [Overview — Setup](overview.md#setup) for the config file model.

---

## Input

| Argument | Default | Description |
|----------|---------|-------------|
| `--netcdf-dir PATH` | *(required unless --input-datacubes is used)* | Directory containing `T{TILE}_{VI}.nc` files |
| `--input-datacubes PATH [PATH ...]` | — | Alternative to `--netcdf-dir` + `--shapefile`. Accepts individual `*_datacube.nc` file paths **or** a single directory path (all `*_datacube.nc` files found recursively). VI and `region_label` are inferred from each filename (`{VI}_{region_label}_datacube.nc`). When this is set, `--netcdf-dir`, `--shapefile`, and `--shapefile-field` are ignored. |
| `--vi VI [VI ...]` | `NDVI` | Vegetation indices to process: `NDVI` `EVI2` `NIRv` |
| `--shapefile PATH [PATH ...]` | — | Shapefile(s) for spatial subsetting. Omit to process the full NetCDF extent. Ignored when `--input-datacubes` is used. |
| `--shapefile-field FIELD [FIELD ...]` | — | Attribute field(s) to split shapefile(s) by — one per shapefile in positional order. Use `none` to dissolve a specific file instead of splitting it. Count must match `--shapefile` exactly. Ignored when `--input-datacubes` is used. |
| `--valid-range-ndvi MIN,MAX` | `-1,1` | Valid range for NDVI |
| `--valid-range-evi2 MIN,MAX` | `-1,2` | Valid range for EVI2 |
| `--valid-range-nirv MIN,MAX` | `-0.5,1` | Valid range for NIRv |

---

## Output

| Argument | Default | Description |
|----------|---------|-------------|
| `--output-dir PATH` | *(required)* | Output directory (created if it does not exist) |

---

## Smoothing

| Argument | Default | Description |
|----------|---------|-------------|
| `--smooth-method METHOD` | `savgol` | Smoothing method: `savgol` `loess` `linear` `harmonic` `whittaker` `none` |
| `--smooth-window DAYS` | `15` | Smoothing window in days (savgol and loess) |
| `--smooth-polyorder N` | `3` | Polynomial order for Savitzky-Golay (must be < window length) |
| `--smooth-lambda LAMBDA` | `100.0` | Whittaker smoother penalty strength (only used with `--smooth-method whittaker`). Lower values (10–50) follow observations closely; higher values (300–1000) produce a very smooth curve. |

For full details on each method, see [Smoothing](smoothing.md).

---

## Phenological Metrics

| Argument | Default | Description |
|----------|---------|-------------|
| `--metrics` | off | Compute and export phenological metrics (requires a smoothing method) |
| `--sos-threshold FRACTION` | `0.20` | Amplitude fraction for SOS/EOS detection (e.g. `0.20` = 20% of annual amplitude) |
| `--year-start-doy DOY` | `1` | Day of year to begin each annual phenology window (1–365). Use `1` for Northern Hemisphere (Jan 1). Use `182` (Jul 1) or another austral-winter DOY for Southern Hemisphere data. |
| `--peak-prominence NDVI` | `0.05` | Minimum NDVI prominence for a peak to be counted by bimodality detection (only active when `--metrics` is set) |
| `--peak-min-distance DAYS` | `45` | Minimum separation in days between detected peaks (only active when `--metrics` is set) |

For full details on metrics and annual window configuration, see [Phenological Metrics](metrics.md).

---

## Observation Count Thresholds

| Argument | Default | Description |
|----------|---------|-------------|
| `--min-valid-obs N` | `20` | Minimum valid observations over the full record. Regions with fewer observations are skipped entirely. |
| `--min-valid-obs-per-year N` | `5` | Minimum valid observations within an annual window. Annual windows with fewer observations are skipped (no NaN row written). |

---

## Pixel Sampling

Randomly samples a fixed set of N pixels per region — used consistently across the full time series to eliminate date-to-date variation caused by cloud masking. Only active when at least one of `--sample-pixels`, `--min-ndvi-mean`, or `--min-quality-frac > 0` is set.

| Argument | Default | Description |
|----------|---------|-------------|
| `--sample-pixels N` | None (all pixels) | Number of pixels to randomly sample per region |
| `--random-seed SEED` | None (random) | RNG seed for reproducible pixel samples |
| `--min-ndvi-mean VAL` | None (no filter) | Exclude pixels whose temporal mean NDVI is below this value |
| `--min-quality-frac FRAC` | `0.0` (no filter) | Minimum fraction of timesteps a pixel must be valid (non-NaN, in-range) to be eligible |

---

## Plotting

| Argument | Default | Description |
|----------|---------|-------------|
| `--plot-style STYLE` | `combined` | `raw`: observation scatter only · `smooth`: smooth curve only · `combined`: smooth + scatter |
| `--plot-format FORMAT [FORMAT ...]` | `png` | Output format(s): `png` and/or `html` |

---

## Output Toggles

All output types are enabled by default. Use these flags to disable specific outputs:

| Argument | `config.local.env` variable | Controls |
|----------|------|---------|
| `--no-observations-csv` | `SAVE_OBSERVATIONS_CSV=false` | Per-region observations-only CSV files |
| `--no-combined-outputs` | `SAVE_COMBINED_OUTPUTS=false` | Combined shapefile observations CSV |
| `--no-plot-annual` | `PLOT_ANNUAL=false` | Annual DOY overlay plot |
| `--no-plot-timeseries` | `PLOT_TIMESERIES=false` | Full calendar time-series plot |
| `--no-plot-anomaly` | `PLOT_ANOMALY=false` | Anomaly (departure from multi-year mean) plot |
| `--no-plot-multi-vi` | `PLOT_MULTI_VI=false` | Multi-VI comparison panel (requires > 1 VI) |

For output file details, see [Output](output.md).

---

## Performance

| Argument | Default | Description |
|----------|---------|-------------|
| `--workers N` | `8` | Parallel worker processes for tile extraction. Set to `1` for sequential mode. |
| `--start-date YYYY-MM-DD` | — | Only include observations on or after this date |
| `--end-date YYYY-MM-DD` | — | Only include observations on or before this date |

---

## Diagnostics

| Argument | Default | Description |
|----------|---------|-------------|
| `--log-level LEVEL` | `INFO` | Verbosity: `DEBUG` `INFO` `WARNING` `ERROR` |
