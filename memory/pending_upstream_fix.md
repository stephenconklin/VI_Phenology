---
name: HLS_VI_Pipeline Step 03 — southern hemisphere CRS fix (implemented 2026-03-18)
description: HLS v2.0 tiles south of the equator were stored with UTM North CRS and negative northings; fixed in 03_hls_netcdf_build.py — existing NetCDF files need to be regenerated
type: project
---

## Status
**Fixed** in `HLS_VI_Pipeline/src/03_hls_netcdf_build.py` (2026-03-18).

## Problem (now fixed upstream)
HLS v2.0 GeoTIFFs for southern hemisphere tiles used a UTM North zone (EPSG:326xx,
false_northing=0) with **negative northings** instead of the standard UTM South
convention (EPSG:327xx, false_northing=10,000,000). All BioSCape / southern Africa
tiles were affected. Confirmed by diagnostic on actual netCDF files:
- T34HBH: CRS = "WGS 84 / UTM zone 34N", EPSG:32634, y range = -3,809,745 → -3,699,975 m
- T35HLC: CRS = "UTM Zone 35, Northern Hemisphere", EPSG:32635, y range negative
- pyproj Transformer still yielded correct lat/lon (lat=-33.9°), but GIS tools and
  CF-1.8 validators saw the "Northern Hemisphere" label and negative northings.

## Fix (in 03_hls_netcdf_build.py)
After reading the source GeoTIFF CRS and computing pixel-center y-coordinates,
`HLSNetCDFAggregator.run()` now checks: if `to_epsg(min_confidence=20)` returns a
UTM North code (32601–32660) AND `y_coords.mean() < 0`, it replaces the CRS with the
UTM South equivalent (`_south_epsg = _epsg + 100`, e.g. 32634 → 32734) and shifts
y-coordinates by +10,000,000 m. The correction is logged with a `[CRS fix]` prefix.

## Impact on existing NetCDF files
Existing NetCDF files built before this fix carry UTM North CRS and negative northings.
VI_Phenology spatial operations remain internally consistent for these files (tile and
ROI are both in UTM North space), but output datacubes carry the wrong hemisphere label.
**Rebuilding with step 03 is required to produce CF-1.8 correct files.**

**Why:** Fix must be upstream (in the netCDF files themselves) so all consumers
(QGIS, VI_Phenology, any third-party tool) see the correct CRS.
**How to apply:** Rerun `STEPS="netcdf" bash hls_pipeline.sh` for affected tiles.
