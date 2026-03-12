# Output

---

## Phenology Pipeline

### File Structure

Outputs are organized into per-region subdirectories when shapefiles are provided:

```
outputs/                                         ← --output-dir
├── vi_phenology_20260303_153100.log             ← log file at output-dir root
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

### Output Files

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

### Output Toggles

All output types are enabled by default. Disable any combination in `run_phenology.sh`:

```bash
SAVE_PARQUET=false            # skip per-region Parquet files
SAVE_OBSERVATIONS_CSV=false   # skip per-region observations CSV files
SAVE_COMBINED_OUTPUTS=false   # skip combined shapefile Parquet + observations CSV
PLOT_ANNUAL=false             # skip annual DOY overlay plot
PLOT_TIMESERIES=false         # skip full time-series plot
PLOT_ANOMALY=false            # skip anomaly plot
PLOT_MULTI_VI=false           # skip multi-VI comparison panel
```

Or via CLI flags: `--no-parquet`, `--no-observations-csv`, `--no-combined-outputs`,
`--no-plot-annual`, `--no-plot-timeseries`, `--no-plot-anomaly`, `--no-plot-multi-vi`.

See [CLI Reference](cli_reference.md) for the full toggle table.

---

### Parquet Schema

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

## netCDF Datacube Pipeline

### File Structure

```
outputs/                                              ← --output-dir
├── netcdf_datacube_20260312_153100.log               ← log file at output-dir root
└── Parks_and_OpenSpace/                              ← shapefile stem
    ├── Mesa_Verde/                                   ← one subfolder per region
    │   ├── NDVI_Mesa_Verde_datacube.nc               ← merged datacube (default)
    │   └── EVI2_Mesa_Verde_datacube.nc
    └── Ridgway_State_Park/
        └── NDVI_Ridgway_State_Park_datacube.nc
```

When no shapefile is provided (full-extent mode), outputs go into
`outputs/full_extent/` directly.

When `--no-merge-same-crs` or `--no-merge-cross-crs` is set, each tile writes its
own file:

```
outputs/
└── Parks_and_OpenSpace/
    └── Mesa_Verde/
        ├── NDVI_Mesa_Verde_T13SDA_datacube.nc
        └── NDVI_Mesa_Verde_T13SDB_datacube.nc
```

---

### Output Files

| File | Description |
|------|-------------|
| `{VI}_{region}_datacube.nc` | Merged per-pixel datacube: all contributing tiles merged into one CF-1.8 netCDF |
| `{VI}_{region}_{tile_id}_datacube.nc` | Per-tile datacube when merge is disabled (native CRS, no reprojection) |
| `netcdf_datacube_{timestamp}.log` | Timestamped log file |

---

### NetCDF Schema

**Dimensions:** `time`, `y` (northing, meters), `x` (easting, meters)

**Variables:**
- `{VI}` — float32 `(time, y, x)`, NaN where no valid data
- `spatial_ref` — scalar CRS container (CF-1.8 grid mapping convention)

**Global attributes:**

| Attribute | Present when | Description |
|---|---|---|
| `Conventions` | always | `'CF-1.8'` |
| `history` | always | Creation timestamp, region, valid range |
| `tiles` | always | Source tile IDs (comma-separated, e.g. `'T13SDA, T13SDB'`) |
| `region` | always | Region label |
| `vi` | always | VI name (e.g. `'NDVI'`) |
| `target_crs` | cross-CRS merge only | Target CRS for reprojected tiles (e.g. `'EPSG:32613'`) |
| `resampling_method` | cross-CRS merge only | `'bilinear'` |

For full details on the datacube pipeline, see [netCDF Datacube Pipeline](datacube.md).

---

## Date Range Filtering

Use `--start-date` and `--end-date` to restrict processing to a specific time window.
Filtering is applied at the NetCDF level before any spatial aggregation or clipping,
keeping memory use low even on large multi-year datasets.

Works identically in both pipelines:

```bash
# Phenology pipeline
python src/vi_phenology.py \
  --netcdf-dir /path/to/netcdfs \
  --vi NDVI \
  --output-dir ./outputs \
  --start-date 2021-01-01 \
  --end-date   2023-12-31 \
  --metrics

# Datacube pipeline
python src/netcdf_datacube_extract.py \
  --netcdf-dir /path/to/netcdfs \
  --vi NDVI \
  --output-dir ./outputs \
  --start-date 2021-01-01 \
  --end-date   2023-12-31
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
outputs/vi_phenology_20260303_153100.log        ← phenology pipeline
outputs/netcdf_datacube_20260303_153100.log     ← datacube pipeline
```

To disable automatic log file creation:

```bash
python src/vi_phenology.py --no-logfile ...
python src/netcdf_datacube_extract.py --no-logfile ...
```

Or in `run_phenology.sh`:
```bash
NO_LOGFILE=true
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
