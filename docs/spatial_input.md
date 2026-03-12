# Spatial Input

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

The same `--shapefile` and `--shapefile-field` arguments work identically in both
the phenology pipeline and the netCDF datacube pipeline.

---

## Multiple Shapefiles

Pass multiple paths to produce an independent output set per shapefile:

```bash
python src/vi_phenology.py \
  --shapefile /path/to/region1.gpkg /path/to/region2.geojson \
  ...
```

---

## Per-Feature Splitting — `--shapefile-field`

To produce one independent output per feature value in a shapefile, provide the name
of an attribute column:

```bash
python src/vi_phenology.py \
  --shapefile biomes.gpkg \
  --shapefile-field Name \
  ...
```

Each unique non-null value in the `Name` column becomes its own region. Field values are
sanitized for filesystem use: spaces and special characters are replaced with underscores.
For example, `"Cape Fynbos"` becomes the region label (and subdirectory) `Cape_Fynbos`.

When `--shapefile-field` is omitted, all features in a file are dissolved into a single
geometry and the shapefile's filename stem is used as the region label.

> **Label collision error:** If two distinct field values sanitize to the same string
> (e.g. `"My Park"` and `"My_Park"` both become `My_Park`), the pipeline exits with an
> error identifying the conflicting values. Rename one of them in the attribute table to
> make the labels unique before re-running.

---

## Multiple Shapefiles with Per-Field Splitting

When passing multiple shapefiles, provide one field name per shapefile in the same positional
order. Use `none` to dissolve a specific shapefile rather than splitting it:

```bash
# Split first shapefile by 'box_nr', dissolve the second
python src/vi_phenology.py \
  --shapefile flights.shp tiles.geojson \
  --shapefile-field box_nr none \
  ...

# Split both shapefiles by their respective fields
python src/vi_phenology.py \
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

---

## Region Label Derivation

The region label controls both the output subdirectory name and all output file stems:

| Scenario | Region label |
|---|---|
| No shapefile | `full_extent` |
| Shapefile, no `--shapefile-field` | Shapefile filename stem (e.g. `biomes`) |
| Shapefile + `--shapefile-field` | Sanitized field value (e.g. `Cape_Fynbos`) |

For output directory structure and file naming, see [Output](output.md).

---

## Multi-Tile Polygon Handling

When a polygon spans multiple HLS MGRS tiles, both pipelines detect overlapping tiles
automatically and pool data from all of them. The merge behavior differs between pipelines:

### Phenology Pipeline

All valid pixels from all overlapping tiles are pooled **per observation date** before
computing the spatial mean for that region. No spatial resampling occurs — the ROI-mean
aggregation naturally handles tiles with different acquisition dates by combining the
pixel pools per date.

### netCDF Datacube Pipeline

The datacube pipeline preserves the full spatial dimension and must explicitly merge
tile arrays. The merge strategy depends on the CRS relationships between tiles:

#### Same UTM Zone (same CRS)

Adjacent HLS MGRS tiles in the same UTM zone share an **identical 30-m pixel grid**.
Tiles are mosaiced without resampling using `xarray.DataArray.combine_first()`:

- First-wins for the ~163-pixel MGRS overlap zone at 30 m
- Time union: the merged datacube covers all acquisition dates from all tiles
- No resampling — pixel values are unmodified

**Enable/disable** in `run_phenology.sh`:

```bash
MERGE_SAME_CRS=true    # merge into one datacube per region (default)
MERGE_SAME_CRS=false   # one file per tile, native CRS
```

#### Different UTM Zones (cross-CRS)

When a polygon crosses a UTM zone boundary, the tiles are in different coordinate
systems and cannot share a pixel grid without reprojection.

The dominant CRS (the CRS group covering the most pixels within the polygon) is
selected as the target. Minority tiles are bilinearly reprojected to the dominant CRS,
then mosaiced via `combine_first`.

Bilinear reprojection between adjacent UTM zones introduces sub-pixel mixing comparable
to the sensor point spread function — scientifically acceptable for VI analysis at 30 m.
The output file's global attributes record the target CRS and resampling method.

**Enable/disable** in `run_phenology.sh`:

```bash
MERGE_CROSS_CRS=true   # reproject + merge into one datacube (default)
MERGE_CROSS_CRS=false  # one file per tile, native CRS, no reprojection
```

#### Merge Strategy Summary

| Condition | Result |
|---|---|
| 1 tile | Single file, native CRS |
| N tiles, same CRS, `MERGE_SAME_CRS=true` | 1 merged file, no resampling |
| N tiles, same CRS, `MERGE_SAME_CRS=false` | N files, one per tile |
| N tiles, mixed CRS, `MERGE_CROSS_CRS=true` | 1 merged file, minority tiles bilinearly reprojected |
| N tiles, mixed CRS, `MERGE_CROSS_CRS=false` | N files, one per tile, native CRS |

For complete datacube pipeline documentation, see [netCDF Datacube Pipeline](datacube.md).
