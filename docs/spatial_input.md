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

---

## Multiple Shapefiles

Pass multiple paths to produce an independent time series and output set per shapefile:

```bash
python vi_phenology.py \
  --shapefile /path/to/region1.gpkg /path/to/region2.geojson \
  ...
```

---

## Per-Feature Splitting — `--shapefile-field`

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

---

## Multiple Shapefiles with Per-Field Splitting

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

---

## Region Label Derivation

The region label controls both the output subdirectory name and all output file stems:

| Scenario | Region label |
|---|---|
| No shapefile | `full_extent` |
| Shapefile, no `--shapefile-field` | Shapefile filename stem (e.g. `biomes`) |
| Shapefile + `--shapefile-field` | Sanitized field value (e.g. `Cape_Fynbos`) |

For output directory structure and file naming, see [Output](output.md).
