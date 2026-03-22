# Spatial Input

This page covers polygon-based spatial input for the **phenology** and **netCDF datacube**
pipelines. Both accept `--shapefile` to clip processing to one or more polygon regions.

> **Pixel phenology pipeline:** Does **not** use `--shapefile` or `--netcdf-dir`. It
> reads pre-clipped datacubes produced by the netCDF datacube pipeline — the spatial
> boundary is already embedded in those files. See
> [Pixel Phenology Pipeline](pixel_phenology.md).

---

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

In `config.local.env`, set `SHAPEFILE_FIELD` to activate field splitting:

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

The datacube pipeline preserves the full spatial dimension and merges tiles based on
their CRS relationships: same-UTM-zone tiles are mosaiced without resampling; tiles
spanning a UTM zone boundary are bilinearly reprojected to the dominant CRS before
merging. Merge behavior is controlled by `MERGE_SAME_CRS` and `MERGE_CROSS_CRS` in
`config.local.env`.

For the full merge algorithm, CRS detection logic, and merge options, see
[netCDF Datacube Pipeline — Multi-Tile and Multi-CRS Handling](datacube.md#multi-tile-and-multi-crs-handling).
