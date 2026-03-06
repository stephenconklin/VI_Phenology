# Output

---

## File Structure

Outputs are organized into per-region subdirectories when shapefiles are provided:

```
outputs/                                         ← --output-dir
└── biomes/                                      ← shapefile stem folder
    ├── Cape_Fynbos/                             ← one subfolder per field value
    │   ├── NDVI_Cape_Fynbos_timeseries.parquet
    │   ├── NDVI_Cape_Fynbos_observations.csv
    │   ├── NDVI_Cape_Fynbos_metrics.csv
    │   ├── NDVI_Cape_Fynbos_timeseries.png
    │   ├── NDVI_Cape_Fynbos_timeseries.html
    │   ├── NDVI_Cape_Fynbos_annual.png
    │   ├── NDVI_Cape_Fynbos_annual.html
    │   ├── NDVI_Cape_Fynbos_anomaly.png
    │   └── Cape_Fynbos_multi_vi.png
    ├── Succulent_Karoo/
    │   └── ...
    ├── NDVI_biomes_timeseries.parquet           ← all regions stacked, full daily series
    ├── NDVI_biomes_timeseries.csv               ← all regions stacked, observations only
    └── NDVI_biomes_metrics.csv                  ← combined metrics (field-split runs only)
```

When a shapefile is dissolved (no `--shapefile-field`), outputs go into
`outputs/{shapefile_stem}/` with no further nesting.

When no shapefile is provided (full-extent mode), all outputs go directly into `--output-dir`
and the region label is `full_extent`.

---

## Output Files

| File | Description |
|------|-------------|
| `{VI}_{region}_timeseries.parquet` | Complete daily time series (all rows including gap days): raw + smoothed VI columns, provenance flags |
| `{VI}_{shapefile_stem}_timeseries.parquet` | All regions stacked with `region` column — same full daily series as above. Written to shapefile root folder when `--shapefile-field` yields multiple regions. |
| `{VI}_{region}_observations.csv` | **Actual HLS observations only** — date, vi_raw, vi_count, vi_std, vi_smooth (at obs dates). No gap-filled rows. |
| `{VI}_{shapefile_stem}_timeseries.csv` | All regions stacked with `region` column — same columns as observations CSV above. Written to shapefile root folder when `--shapefile-field` yields multiple regions. |
| `{VI}_{region}_metrics.csv` | Phenological metrics per year for this region |
| `{VI}_{shapefile_stem}_metrics.csv` | Combined metrics for all regions in a shapefile (when `--shapefile-field` is set) |
| `{VI}_{region}_timeseries.png/html` | Full temporal range: smooth curve + observation scatter + ±1 std band |
| `{VI}_{region}_annual.png/html` | VI vs month, one line per year + multi-year mean overlay |
| `{VI}_{region}_anomaly.png/html` | Per-year deviation from multi-year mean (requires ≥ 2 calendar years) |
| `{region}_multi_vi.png/html` | Side-by-side NDVI / EVI2 / NIRv comparison (requires > 1 VI) |

---

## Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| `date` | datetime64[ns] | Calendar date |
| `vi_raw` | float32 | Spatially aggregated VI on observation days; NaN on non-observation days |
| `vi_count` | int32 | Valid pixel count contributing to `vi_raw`; 0 on non-observation days |
| `vi_std` | float32 | Spatial standard deviation of valid pixels; NaN on non-observation days |
| `vi_daily` | float32 | Daily reindex of `vi_raw` (NaN gaps preserved) |
| `vi_smooth` | float32 | Smoothed, gap-filled daily values (absent when `--smooth-method none`) |
| `vi_smooth_flag` | str | Provenance: `observed` · `interpolated` · `extrapolated` |

---

## Date Range Filtering

Use `--start-date` and `--end-date` to restrict processing to a specific time window. Filtering
is applied at the NetCDF level before any spatial aggregation, keeping memory use low even on
large multi-year datasets:

```bash
python src/vi_phenology.py \
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

### Log File

By default, a timestamped log file is automatically written to `--output-dir`:

```
outputs/vi_phenology_20260303_153100.log
```

To disable automatic log file creation:

```bash
python src/vi_phenology.py --no-logfile ...
```

### Verbosity Levels

| Level | What you see |
|-------|-------------|
| `WARNING` | Only warnings and errors |
| `INFO` *(default)* | Per-tile pixel counts, smoothing stats, metric values, saved file paths |
| `DEBUG` | Adds clip geometry details, bin sizes, S-G window calculations, per-reindex statistics |

```bash
python src/vi_phenology.py --log-level WARNING ...   # quiet
python src/vi_phenology.py --log-level DEBUG ...     # full diagnostics
```
