---
name: Pending upstream fix — HLS_VI_Pipeline Step 03 CRS WKT
description: Fix needed in HLS_VI_Pipeline/src/03_hls_netcdf_build.py to write authority-tagged WKT so pyproj.CRS.to_epsg() resolves correctly in netcdf_datacube_extract.py
type: project
---

## Fix Location
`HLS_VI_Pipeline/src/03_hls_netcdf_build.py`, line 268

## Problem
`crs.to_wkt()` (rasterio) produces non-authority-tagged WKT1 for HLS GeoTIFFs.
`pyproj.CRS.from_wkt(wkt).to_epsg()` in `netcdf_datacube_extract.py` returns `None`
for this WKT, so CRS grouping in Phase 2 falls back to raw WKT strings instead of
clean EPSG integers. Functionally correct but verbose in logs.

## Fix
Add import: `from pyproj import CRS as ProjCRS`

Replace line 268:
```python
# Before
crs_wkt = crs.to_wkt()

# After
try:
    crs_wkt = ProjCRS.from_user_input(crs).to_wkt()
except Exception:
    crs_wkt = crs.to_wkt()
```

Full details in `HLS_VI_Pipeline/memory/MEMORY.md`.
