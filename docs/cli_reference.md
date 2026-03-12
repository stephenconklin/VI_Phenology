# CLI Reference

This page covers the **phenology pipeline** CLI (`src/vi_phenology.py`).
For the netCDF datacube pipeline CLI, see [netCDF Datacube Pipeline](datacube.md).

The recommended way to run either pipeline is via `run_phenology.sh` — edit its variables
and run `./run_phenology.sh`. All parameters are documented with inline comments inside the script.

---

## Input

| Argument | Default | Description |
|----------|---------|-------------|
| `--netcdf-dir PATH` | *(required)* | Directory containing `T{TILE}_{VI}.nc` files |
| `--vi VI [VI ...]` | `NDVI` | Vegetation indices to process: `NDVI` `EVI2` `NIRv` |
| `--shapefile PATH [PATH ...]` | — | Shapefile(s) for spatial subsetting. Omit to process the full NetCDF extent. |
| `--shapefile-field FIELD [FIELD ...]` | — | Attribute field(s) to split shapefile(s) by — one per shapefile in positional order. Use `none` to dissolve a specific file instead of splitting it. Count must match `--shapefile` exactly. |
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
| `--smooth-method METHOD` | `savgol` | Smoothing method: `savgol` `loess` `linear` `harmonic` `none` |
| `--smooth-window DAYS` | `15` | Smoothing window in days (savgol and loess) |
| `--smooth-polyorder N` | `3` | Polynomial order for Savitzky-Golay (must be < window length) |

For full details on each method, see [Smoothing](smoothing.md).

---

## Phenological Metrics

| Argument | Default | Description |
|----------|---------|-------------|
| `--metrics` | off | Compute and export phenological metrics (requires a smoothing method) |
| `--sos-threshold FRACTION` | `0.20` | Amplitude fraction for SOS/EOS detection (e.g. `0.20` = 20% of annual amplitude) |
| `--year-start-doy DOY` | `1` | Day of year to begin each annual phenology window (1–365). Use `1` for Northern Hemisphere (Jan 1). Use `182` (Jul 1) or another austral-winter DOY for Southern Hemisphere data. |

For full details on metrics and annual window configuration, see [Phenological Metrics](metrics.md).

---

## Plotting

| Argument | Default | Description |
|----------|---------|-------------|
| `--plot-style STYLE` | `combined` | `raw`: observation scatter only · `smooth`: smooth curve only · `combined`: smooth + scatter |
| `--plot-format FORMAT [FORMAT ...]` | `png` | Output format(s): `png` and/or `html` |

---

## Output Toggles

All output types are enabled by default. Use these flags to disable specific outputs:

| Argument | `run_phenology.sh` variable | Controls |
|----------|------|---------|
| `--no-parquet` | `SAVE_PARQUET=false` | Per-region Parquet time-series files |
| `--no-observations-csv` | `SAVE_OBSERVATIONS_CSV=false` | Per-region observations-only CSV files |
| `--no-combined-outputs` | `SAVE_COMBINED_OUTPUTS=false` | Combined shapefile Parquet + observations CSV |
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
| `--no-logfile` | off | Disable automatic log file creation in `--output-dir` |
