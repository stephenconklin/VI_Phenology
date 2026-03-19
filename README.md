# VI_Phenology

[![Python](https://img.shields.io/badge/python-3.10--3.12-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS-lightgrey.svg)]()

Vegetation index (VI) analysis toolkit for CF-1.8 compliant NetCDF time-series data.
Designed to work natively with output from [HLS_VI_Pipeline](https://github.com/stephenconklin/HLS_VI_Pipeline),
but accepts any CF-1.8 NetCDF with `time`, `y`, `x` dimensions and a VI data variable.

---

## Two Pipelines

| Pipeline | Script | Purpose |
|---|---|---|
| `phenology` | `src/vi_phenology.py` | ROI-mean time series, smoothing, phenological metrics, and plots |
| `netcdf_datacube` | `src/netcdf_datacube_extract.py` | Per-pixel CF-1.8 datacubes clipped to polygon regions |

Both pipelines support multi-tile inputs, parallel processing (`--workers`), date range
filtering, and per-region shapefile splitting. Select the pipeline via the `PIPELINE`
variable in `run_phenology.sh`, or invoke each script directly from the CLI.

### Phenology pipeline (`vi_phenology.py`)

Extracts spatially aggregated ROI-mean time series and produces:
- Smoothed, gap-filled daily VI profiles (Savitzky-Golay, LOESS, harmonic, or linear)
- Phenological metrics: SOS, POS, EOS, LOS, IVI, greening/senescence rates
- Annual DOY overlay, full time-series, anomaly, and multi-VI comparison plots (PNG + HTML)
- Parquet time-series files and observations CSVs (per-region and combined)

### netCDF datacube pipeline (`netcdf_datacube_extract.py`)

Clips source tiles to polygon boundaries and delivers per-pixel CF-1.8 datacubes:
- Preserves full spatial dimensions for downstream scientific analysis
- Same-CRS tiles mosaiced via a memory-bounded write loop (no resampling)
- Cross-CRS tiles bilinearly reprojected to the dominant CRS before merging
- CF-1.8 global attributes written to all output files

---

## Documentation

Full documentation — setup, CLI reference, spatial input, smoothing methods, phenological
metrics, and output files — is available at:

**[https://vi-phenology.readthedocs.io/en/latest/overview.html](https://vi-phenology.readthedocs.io/en/latest/overview.html)**

---

## Authors

**Stephen Conklin**, Geospatial Analyst — Pipeline architecture, orchestration, and all original code.
[https://github.com/stephenconklin](https://github.com/stephenconklin)

**AI Assistance:** This tool was developed with the assistance of Anthropic Claude / Claude Code. These tools assisted
with code generation and refinement under the direction and review of the author.

---

## License

MIT
