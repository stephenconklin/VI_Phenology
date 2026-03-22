# VI_Phenology

[![Python](https://img.shields.io/badge/python-3.10--3.12-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS-lightgrey.svg)]()

Vegetation index (VI) analysis toolkit for CF-1.8 compliant NetCDF time-series data.
Designed to work natively with output from [HLS_VI_Pipeline](https://github.com/stephenconklin/HLS_VI_Pipeline),
but accepts any CF-1.8 NetCDF with `time`, `y`, `x` dimensions and a VI data variable.

---

## Three Pipelines

| Pipeline | Script | Purpose |
|---|---|---|
| `phenology` | `src/vi_phenology.py` | ROI-mean time series, smoothing, phenological metrics, and plots |
| `netcdf_datacube` | `src/netcdf_datacube_extract.py` | Per-pixel CF-1.8 datacubes clipped to polygon regions |
| `pixel_phenology` | `src/pixel_phenology_extract.py` | 19 per-pixel phenological metric maps from existing datacubes |

The `phenology` and `netcdf_datacube` pipelines support multi-tile inputs, parallel
processing (`--workers`), date range filtering, and per-region shapefile splitting.
Select the pipeline via the `PIPELINE` variable in `run_phenology.sh`, or invoke each
script directly from the CLI.

---

## Typical Workflows

### Workflow A — ROI-mean phenology (most common)

```
Step 1 (recommended): PIPELINE="netcdf_datacube"
  Clips source tiles to your polygon → produces *_datacube.nc files per region

Step 2: PIPELINE="phenology"  (--input-datacubes mode)
  Reads those datacubes directly — skips tile clipping on every re-run
  Change smoothing, thresholds, or plot settings without re-processing source tiles
```

Or in a single pass without intermediate datacubes:
```
  PIPELINE="phenology"  (--netcdf-dir + --shapefile mode)
  Discovers tiles, clips, aggregates, smooths, and plots in one run
```

### Workflow B — Per-pixel phenological metric maps

```
Step 1 (required): PIPELINE="netcdf_datacube"
  Clips source tiles → *_datacube.nc files

Step 2: PIPELINE="pixel_phenology"
  Reads those datacubes → 19-band metric map NetCDF per region
```

> `pixel_phenology` requires datacubes from `netcdf_datacube` — there is no direct
> path from raw tiles to pixel metric maps.

### Workflow C — Per-pixel datacubes for external analysis

```
  PIPELINE="netcdf_datacube"
  Clips source tiles → *_datacube.nc files ready for your own downstream tools
```

---

### Phenology pipeline (`vi_phenology.py`)

Extracts spatially aggregated ROI-mean time series and produces:
- Smoothed, gap-filled daily VI profiles (Savitzky-Golay, LOESS, harmonic, linear, or Whittaker)
- Phenological metrics per year: SOS, POS, EOS, LOS, IVI, greening/senescence rates
- Extended metrics per year: floor NDVI, ceiling NDVI, season length, green-up rate, bimodality (n_peaks, peak separation, relative amplitude, valley depth), and whole-series CV — all derived directly from the smooth curve, no seasonal DOY windows required
- Annual DOY overlay, full time-series, anomaly, and multi-VI comparison plots (PNG + HTML)
- Observations CSVs (per-region and combined)
- **Pixel sampling** (`--sample-pixels N`): randomly draw N pixels per region once and use them consistently across the full time series — eliminates date-to-date spatial sampling variation from cloud masking
- **Pixel filtering** (`--min-ndvi-mean`, `--min-quality-frac`): exclude bare soil / persistently cloud-covered pixels before sampling
- **Observation thresholds** (`--min-valid-obs`, `--min-valid-obs-per-year`): skip data-poor regions or individual sparse years rather than fitting unreliable phenological metrics
- **Datacube input mode** (`--input-datacubes`): read pre-clipped datacubes produced by the `netcdf_datacube` pipeline instead of re-clipping source tiles — eliminates tile discovery and parallel clip overhead for repeated runs with different smoothing settings, thresholds, or plot styles; accepts a directory path (all `*_datacube.nc` files found recursively) or individual file paths

### netCDF datacube pipeline (`netcdf_datacube_extract.py`)

Clips source tiles to polygon boundaries and delivers per-pixel CF-1.8 datacubes (time × y × x):
- Retains the full time axis across all tile acquisition dates — each pixel carries a complete VI time series
- Preserves full spatial resolution for downstream per-pixel analysis (trend, anomaly, classification, etc.)
- Same-CRS tiles mosaiced via a memory-bounded write loop (no resampling)
- Cross-CRS tiles bilinearly reprojected to the dominant CRS before merging
- CF-1.8 global attributes written to all output files

### Pixel phenology pipeline (`pixel_phenology_extract.py`)

Reads per-pixel datacubes (output of the `netcdf_datacube` pipeline) and produces
per-pixel phenological metric maps:
- Whittaker smoothing applied per pixel — handles HLS's irregular revisit cadence natively, no binning required
- 19 metric bands output as a CF-1.8 NetCDF on the same x/y grid as the input datacube:

  | Metric group | Bands |
  |---|---|
  | Peak | `peak_ndvi_mean`, `peak_ndvi_std`, `peak_doy_mean`, `peak_doy_std` |
  | Productivity | `integrated_ndvi_mean`, `integrated_ndvi_std`, `greenup_rate_mean`, `greenup_rate_std` |
  | Seasonality | `floor_ndvi_mean`, `ceiling_ndvi_mean`, `season_length_mean`, `season_length_std` |
  | Variability | `cv`, `interannual_peak_range`, `interannual_peak_std` |
  | Bimodality | `n_peaks_mean`, `peak_separation_mean`, `relative_peak_amplitude_mean`, `valley_depth_mean` |

- Floor and ceiling NDVI derived from the curve itself — no biome-specific DOY window configuration needed
- Summary CSV with spatial statistics (mean, std, p05, p50, p95) per metric
- Parallelised via `ThreadPoolExecutor`; scipy sparse solver releases GIL for true multi-core throughput

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
