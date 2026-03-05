# VI_Phenology

[![Python](https://img.shields.io/badge/python-3.10--3.12-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS-lightgrey.svg)]()

Phenology analysis tool for vegetation index (VI) time-series data. Reads CF-1.8 compliant
NetCDF files, extracts and smooths temporal profiles, computes phenological metrics, and
generates annual and time-series plots (PNG static + interactive HTML).

Designed to work natively with output from [HLS_VI_Pipeline](https://github.com/stephenconklin/HLS_VI_Pipeline),
but accepts any CF-1.8 NetCDF with `time`, `y`, `x` dimensions and a VI data variable.

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
