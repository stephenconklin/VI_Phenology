# netCDF Datacube Pipeline

The `netcdf_datacube` pipeline extracts per-pixel VI time series as CF-1.8 compliant
netCDF datacubes clipped to polygon regions. Unlike the phenology pipeline, which
spatially aggregates pixels into a single ROI-mean time series per region, this pipeline
**preserves the full spatial dimension** — every pixel within the polygon boundary is
retained for downstream scientific analysis.

---

## When to Use This Pipeline

Use the datacube pipeline when you need:
- Per-pixel VI values for input into spatial models (e.g., biodiversity, habitat, carbon models)
- CF-1.8 compliant datacubes for interoperability with other geospatial tools
- A clipped spatial subset of the source HLS VI tiles in a standardized format

Use the [phenology pipeline](cli_reference.md) when you need aggregated time series,
smoothed curves, phenological metrics, or plots.

---

## Selecting the Pipeline

In `run_phenology.sh`, set the `PIPELINE` variable at the top of the file:

```bash
PIPELINE="netcdf_datacube"   # CF-1.8 datacubes (this pipeline)
PIPELINE="phenology"         # ROI-mean time series, metrics, plots (default)
```

All input variables (`NETCDF_DIR`, `VI`, `SHAPEFILE`, `SHAPEFILE_FIELD`,
`VALID_RANGE_*`, `WORKERS`, `START_DATE`, `END_DATE`) are shared between both pipelines.

---

## Processing Model

Extraction runs in two phases:

```
Phase 1 — Parallel tile extraction
  For each overlapping tile:
    Open NetCDF → clip to polygon boundary → apply valid-range mask → write temp file

Phase 2 — Merge and write (main process)
  Detect CRS for each temp file
  Group tiles by CRS
  Apply merge strategy → write final datacube(s)

Cleanup — temp files deleted (always, even on error)
```

Temp files are stored in `{output_dir}/{region_label}/_tmp/` and are always removed
after Phase 2 completes, whether it succeeded or failed.

---

## Multi-Tile and Multi-CRS Handling

When a polygon spans multiple HLS MGRS tiles, the pipeline automatically detects the
number of CRS zones involved and selects the appropriate merge strategy.

### Same-CRS Tiles (polygon within one UTM zone)

Adjacent HLS MGRS tiles in the same UTM zone share an **identical 30-m pixel grid**.
No resampling is needed — tiles are mosaiced using `xarray.DataArray.combine_first()`:

- **First-wins** for pixels in the MGRS overlap zone (~163 pixels at 30 m)
- **Time union**: the output datacube covers all acquisition dates from all tiles;
  pixels from tiles that have no data on a given date are NaN
- **No resampling** — pixel values are unmodified

### Cross-CRS Tiles (polygon spans a UTM zone boundary)

When a polygon crosses a UTM zone boundary, tiles are in different coordinate systems.
The dominant CRS (the CRS group with the most total pixels within the polygon) is
selected as the output CRS. Minority tiles are reprojected to the dominant CRS using
**bilinear resampling**, then mosaiced via `combine_first`.

Bilinear reprojection between adjacent UTM zones introduces sub-pixel mixing comparable
to the sensor point spread function — scientifically acceptable for VI analysis at 30 m.

The output file's global attributes document the merge:

```
target_crs:        EPSG:32634
resampling_method: bilinear
```

### Merge Options

| `run_phenology.sh` variable | CLI flag | Default | Effect |
|---|---|---|---|
| `MERGE_SAME_CRS=true` | *(default)* | on | Merge same-CRS tiles into one datacube per region |
| `MERGE_SAME_CRS=false` | `--no-merge-same-crs` | — | One file per tile, native CRS |
| `MERGE_CROSS_CRS=true` | *(default)* | on | Reproject + merge cross-CRS tiles |
| `MERGE_CROSS_CRS=false` | `--no-merge-cross-crs` | — | One file per tile, native CRS, no reprojection |

Setting both to `false` always produces one netCDF per tile per region.

---

## Output

### File Naming

| Condition | Output filename | Location |
|---|---|---|
| Single tile, or merged output | `{VI}_{region_label}_datacube.nc` | `{output_dir}/{shapefile_stem}/{region_label}/` |
| Per-tile output (merge disabled) | `{VI}_{region_label}_{tile_id}_datacube.nc` | `{output_dir}/{shapefile_stem}/{region_label}/` |
| No shapefile (full extent) | `{VI}_full_extent_datacube.nc` | `{output_dir}/full_extent/` |
| Log file | `netcdf_datacube_{YYYYMMDD_HHMMSS}.log` | `{output_dir}/` |

### File Structure Example

```
outputs/                                              ← --output-dir
├── netcdf_datacube_20260312_153100.log               ← log file at output-dir root
└── Parks_and_OpenSpace/                              ← shapefile stem
    ├── Mesa_Verde/                                   ← one subfolder per region
    │   ├── NDVI_Mesa_Verde_datacube.nc               ← merged (2 same-CRS tiles)
    │   └── EVI2_Mesa_Verde_datacube.nc
    └── Ridgway_State_Park/
        ├── NDVI_Ridgway_State_Park_datacube.nc
        └── EVI2_Ridgway_State_Park_datacube.nc
```

When `--no-merge-same-crs` is set:

```
outputs/
└── Parks_and_OpenSpace/
    └── Mesa_Verde/
        ├── NDVI_Mesa_Verde_T13SDA_datacube.nc
        ├── NDVI_Mesa_Verde_T13SDB_datacube.nc
        └── EVI2_Mesa_Verde_T13SDA_datacube.nc
```

### Output NetCDF Structure

Each output file is a CF-1.8 compliant netCDF4 with:

**Dimensions:**
- `time` — acquisition dates (datetime64, decoded from "days since 1970-01-01")
- `y` — northing in meters (UTM)
- `x` — easting in meters (UTM)

**Variables:**
- `{VI}` — float32, dimensions `(time, y, x)`, NaN where no valid data
- `spatial_ref` — scalar, CRS container variable (CF-1.8 grid mapping convention)

**Global attributes:**

| Attribute | Always present | Description |
|---|---|---|
| `Conventions` | yes | `'CF-1.8'` |
| `history` | yes | Creation timestamp, region name, valid_range |
| `tiles` | yes | Source tile IDs (comma-separated) |
| `region` | yes | Region label |
| `vi` | yes | VI name (e.g. `'NDVI'`) |
| `target_crs` | cross-CRS merge only | Target CRS (e.g. `'EPSG:32634'`) |
| `resampling_method` | cross-CRS merge only | `'bilinear'` |

**Variable attributes:**
- `long_name`: e.g. `'NDVI vegetation index'`
- `valid_min` / `valid_max`: applied valid range values
- `grid_mapping`: `'spatial_ref'`

---

## CLI Reference

```
python src/netcdf_datacube_extract.py --help
```

| Argument | Default | Description |
|---|---|---|
| `--netcdf-dir PATH` | *(required)* | Directory containing `T{TILE}_{VI}.nc` files |
| `--vi VI [VI ...]` | `NDVI` | Vegetation indices: `NDVI` `EVI2` `NIRv` |
| `--shapefile PATH [PATH ...]` | — | Polygon shapefile(s). Omit for full extent. |
| `--shapefile-field FIELD [FIELD ...]` | — | Attribute field(s) to split by. Count must match `--shapefile`. |
| `--valid-range-ndvi MIN,MAX` | `-1,1` | Valid range for NDVI |
| `--valid-range-evi2 MIN,MAX` | `-1,2` | Valid range for EVI2 |
| `--valid-range-nirv MIN,MAX` | `-0.5,1` | Valid range for NIRv |
| `--output-dir PATH` | *(required)* | Root output directory |
| `--workers N` | `8` | Parallel worker processes for Phase 1 tile extraction |
| `--start-date YYYY-MM-DD` | — | Include only observations on or after this date |
| `--end-date YYYY-MM-DD` | — | Include only observations on or before this date |
| `--no-merge-same-crs` | off | Disable same-CRS tile merging; write per-tile files |
| `--no-merge-cross-crs` | off | Disable cross-CRS reprojection + merge; write per-tile files |
| `--log-level LEVEL` | `INFO` | `DEBUG` `INFO` `WARNING` `ERROR` |
| `--no-logfile` | off | Disable automatic log file creation |

---

## Notes on CRS Detection

CRS comparison uses `pyproj.CRS.from_wkt(wkt).to_epsg()` rather than raw WKT string
comparison. EPSG integer comparison is more robust — two WKT strings that represent the
same CRS may differ in formatting or authority prefix. When `to_epsg()` cannot resolve
a WKT to an EPSG code, the raw WKT string is used as the group key.

---

## Notes on Same-Day Multi-Sensor Acquisitions

HLS combines Landsat 8, Landsat 9, Sentinel-2A, and Sentinel-2B. When two or more of
these sensors observe the same MGRS tile on the same calendar day, the source NetCDF
contains duplicate time steps with identical timestamps (the HLS time axis uses integer
day precision — there is no sub-day information to distinguish them).

The datacube pipeline resolves this automatically in Phase 1, before writing the temp
file for each tile. For each group of same-day duplicates, the acquisitions are merged
**pixel by pixel**: the first non-NaN value at each `(y, x)` location wins. This
preserves valid data from all same-day acquisitions across the spatial footprint — no
valid pixel is discarded and no values are averaged.

As a result, the output datacube's time axis may have **fewer time steps** than the
source NetCDF tile — one step per unique calendar date rather than one step per
acquisition. The `tiles` global attribute records the source tile IDs, but the number
of acquisitions per sensor is not separately tracked.
