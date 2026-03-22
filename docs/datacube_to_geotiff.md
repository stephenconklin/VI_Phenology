# datacube_to_geotiff Pipeline

> **Prerequisite:** This pipeline reads datacubes produced by the
> [netCDF Datacube Pipeline](datacube.md). Run that pipeline first to produce
> `*_datacube.nc` files, then point `GEOTIFF_INPUT_DATACUBES` at those files or
> their parent directory.

The `datacube_to_geotiff` pipeline reads per-pixel datacubes and writes three
multi-band GeoTiffs per (VI, region) — one summarising the record year by year,
one month by month, and one day-of-year by day-of-year. These products are
designed for delivery to downstream spatial models that expect standard raster
inputs rather than CF-1.8 netCDF files.

---

## When to Use This Pipeline

Use the datacube_to_geotiff pipeline when you need:
- Multi-band GeoTiff inputs for machine-learning or statistical models
- Per-year, per-month, or per-DOY statistics as analysis-ready rasters
- A format compatible with GDAL-based tools, ArcGIS, QGIS, or Google Earth Engine

Use the [pixel phenology pipeline](pixel_phenology.md) when you need
spatially explicit phenological metric maps (SOS, POS, green-up rate, etc.)
derived from the Whittaker-smoothed time series.

---

## Selecting the Pipeline

Set `PIPELINE` in `config.local.env`:

```bash
PIPELINE="datacube_to_geotiff"
```

And point it at the datacubes:

```bash
GEOTIFF_INPUT_DATACUBES="${OUTPUT_DIR}/my_shapefile_stem"   # directory
# or
GEOTIFF_INPUT_DATACUBES="/path/to/NDVI_MyRegion_datacube.nc"   # single file
```

See [Overview — Setup](overview.md#setup) for the full config file model.

---

## Output Products

Three GeoTiffs are written per (VI, region), each summarising the input
time series at a different temporal resolution.

### Per-Year (`*_per_year.tif`)

One group of 3 bands per calendar year present in the datacube:

| Band | Statistic |
|------|-----------|
| `year{YYYY}_median` | Median of all valid observations in that calendar year |
| `year{YYYY}_p05` | 5th percentile |
| `year{YYYY}_p95` | 95th percentile |

Band count: **N_years × 3** (e.g. 15 bands for a 5-year record).

### Per-Month (`*_per_month.tif`)

36 bands: 12 calendar months × 3 statistics. Uses a **per-year-then-average**
method: for each calendar year, the median/p05/p95 across all valid observations
in that month are computed; these annual values are then averaged across all
years. This prevents years with more observations from dominating the result.

| Bands | Description |
|-------|-------------|
| `month01_median` … `month12_median` | Mean-of-annual-medians per calendar month |
| `month01_p05` … `month12_p05` | Mean of annual 5th percentiles |
| `month01_p95` … `month12_p95` | Mean of annual 95th percentiles |

Band count: **36** (always, regardless of date range).

### Per-DOY (`*_per_doy.tif`)

1095 bands: 365 day-of-year values × 3 statistics. Raw observations from all
years are pooled at each DOY before computing statistics. At HLS's ~5-day
revisit cadence, approximately 80% of DOY bands will be all-NoData for a
given pixel — only DOYs with actual acquisitions carry values.

| Bands | Description |
|-------|-------------|
| `doy001_median` … `doy365_median` | Median across all years at each DOY |
| `doy001_p05` … `doy365_p05` | 5th percentile |
| `doy001_p95` … `doy365_p95` | 95th percentile |

Band count: **1095** (always). For large regions, use `--skip-per-doy`
(or `GEOTIFF_PER_DOY=false`) — this product can exceed 4 GB.

---

## GeoTiff Format

| Property | Value |
|----------|-------|
| Format | GeoTiff (BigTIFF when > 4 GB) |
| Compression | LZW |
| Tiling | 256 × 256 pixels |
| Data type | float32 |
| NoData | `9.96920996838687e+36` (CF/NetCDF4 float32 fill value, `NC_FILL_FLOAT`) |
| CRS | Native CRS of the input datacube (UTM, meters) |
| Band descriptions | Set via GDAL standard field; readable with `gdalinfo -mdd all` or `rasterio.open().descriptions` |

Band descriptions are accessible in Python:
```python
import rasterio
with rasterio.open("NDVI_MyRegion_per_year.tif") as src:
    print(src.descriptions)   # ('year2020_median', 'year2020_p05', 'year2020_p95', ...)
```

---

## File Structure

```
outputs/                                              ← GEOTIFF_OUTPUT_DIR
├── datacube_to_geotiff_20260320_153100.log           ← log file at output-dir root
└── Mesa_Verde/                                       ← one subfolder per region
    ├── NDVI_Mesa_Verde_per_year.tif
    ├── NDVI_Mesa_Verde_per_month.tif
    └── NDVI_Mesa_Verde_per_doy.tif
```

VI and `region_label` are parsed from the input datacube filename:
`{VI}_{region_label}_datacube.nc` → first underscore-separated token = VI, remainder = region_label.

### File Naming

| File | Location |
|------|----------|
| `{VI}_{region_label}_per_year.tif` | `{GEOTIFF_OUTPUT_DIR}/{region_label}/` |
| `{VI}_{region_label}_per_month.tif` | `{GEOTIFF_OUTPUT_DIR}/{region_label}/` |
| `{VI}_{region_label}_per_doy.tif` | `{GEOTIFF_OUTPUT_DIR}/{region_label}/` |
| `datacube_to_geotiff_{YYYYMMDD_HHMMSS}.log` | `{GEOTIFF_OUTPUT_DIR}/` |

---

## Processing Model

```
For each input datacube ({VI}_{region_label}_datacube.nc):

  1. Open with xarray (lazy); apply optional --start-date / --end-date filter
     Warn if uncompressed array size > 8 GB
     Warn if per-DOY output size estimate > 4 GB

  2. Apply valid-range mask (vi_min, vi_max → NaN)

  3. Write per_year.tif  (unless --skip-per-year)
     For each calendar year: compute median, p05, p95 → stream one band at a time

  4. Write per_month.tif  (unless --skip-per-month)
     Per-year-then-average: per-year percentiles first, then average across years

  5. Write per_doy.tif  (unless --skip-per-doy)
     Pool all years at each DOY: compute median, p05, p95

  Parallelised via ThreadPoolExecutor across input datacubes.
  Each GeoTiff is written one band at a time — peak memory = one spatial band.
```

---

## CLI Reference

```
python src/datacube_to_geotiff.py --help
```

| Argument | `config.local.env` variable | Default | Description |
|---|---|---|---|
| `--input-datacubes PATH [PATH ...]` | `GEOTIFF_INPUT_DATACUBES` | *(required)* | Path(s) to `*_datacube.nc` files, or a directory (all `*_datacube.nc` files found recursively) |
| `--output-dir PATH` | `GEOTIFF_OUTPUT_DIR` | `${OUTPUT_DIR}/geotiff_stats` | Root output directory |
| `--valid-range-ndvi MIN,MAX` | `VALID_RANGE_NDVI` | `-0.1,1.0` | Valid range for NDVI |
| `--valid-range-evi2 MIN,MAX` | `VALID_RANGE_EVI2` | `-1,2` | Valid range for EVI2 |
| `--valid-range-nirv MIN,MAX` | `VALID_RANGE_NIRV` | `-0.5,1` | Valid range for NIRv |
| `--start-date YYYY-MM-DD` | `START_DATE` | — | Include only time steps on or after this date |
| `--end-date YYYY-MM-DD` | `END_DATE` | — | Include only time steps on or before this date |
| `--skip-per-year` | `GEOTIFF_PER_YEAR=false` | *(per-year written)* | Skip the per-year GeoTiff |
| `--skip-per-month` | `GEOTIFF_PER_MONTH=false` | *(per-month written)* | Skip the per-month GeoTiff |
| `--skip-per-doy` | `GEOTIFF_PER_DOY=false` | *(per-DOY written)* | Skip the per-DOY GeoTiff (recommended for large regions) |
| `--workers N` | `WORKERS` | `4` | Parallel threads for processing multiple datacubes concurrently |
| `--log-level LEVEL` | — | `INFO` | `DEBUG` `INFO` `WARNING` `ERROR` |
