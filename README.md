# VI_Phenology

[![Python](https://img.shields.io/badge/python-3.10--3.12-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-linux%20%7C%20macOS-lightgrey.svg)]()
[![Docs](https://readthedocs.org/projects/vi-phenology/badge/?version=latest)](https://vi-phenology.readthedocs.io/en/latest/)
[![Data: HLS_VI_Pipeline](https://img.shields.io/badge/data-HLS__VI__Pipeline-blue.svg)](https://github.com/stephenconklin/HLS_VI_Pipeline)

Vegetation index (VI) analysis toolkit for CF-1.8 compliant NetCDF time-series data.
Designed to work natively with output from [HLS_VI_Pipeline](https://github.com/stephenconklin/HLS_VI_Pipeline),
but accepts any CF-1.8 NetCDF with `time`, `y`, `x` dimensions and a VI data variable.

---

## Four Pipelines

| Pipeline | Script | Purpose |
|---|---|---|
| `netcdf_datacube` | `src/netcdf_datacube_extract.py` | **Foundation** — clip source tiles to polygon regions; produce per-pixel CF-1.8 datacubes for downstream use |
| `phenology` | `src/vi_phenology.py` | ROI-mean time series, smoothing, phenological metrics, and plots — reads datacubes or raw tiles |
| `pixel_phenology` | `src/pixel_phenology_extract.py` | 19 per-pixel phenological metric maps — reads datacubes produced by `netcdf_datacube` |
| `datacube_to_geotiff` | `src/datacube_to_geotiff.py` | Per-year / per-month / per-DOY summary statistics as multi-band GeoTiffs — reads datacubes produced by `netcdf_datacube` |

Start with `netcdf_datacube`. It clips source tiles to your polygon boundaries once and
produces the per-pixel datacubes that power all downstream analysis. From those datacubes,
run `phenology` for ROI-mean time series and plots, `pixel_phenology` for spatially
explicit metric maps, `datacube_to_geotiff` for model-ready raster statistics, or any
combination. Select the pipeline by setting `PIPELINE` in `config.local.env`
and running `./run_phenology.sh`, or invoke each script directly from the CLI.

---

## Quickstart

```bash
cp config.env config.local.env   # then edit config.local.env with your paths and PIPELINE
./run_phenology.sh
```

`config.env` is the documented base template (committed to git). `config.local.env` holds your
project-specific overrides and is gitignored. Full setup details are in the
[documentation](https://vi-phenology.readthedocs.io/en/latest/overview.html#setup).

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

### Workflow D — Model-ready raster statistics

```
Step 1 (required): PIPELINE="netcdf_datacube"
  Clips source tiles → *_datacube.nc files

Step 2: PIPELINE="datacube_to_geotiff"
  Reads those datacubes → per-year / per-month / per-DOY summary GeoTiffs
  Three multi-band GeoTiffs per region: *_per_year.tif, *_per_month.tif, *_per_doy.tif
```

---

### netCDF datacube pipeline (`netcdf_datacube_extract.py`)

Clips source tiles to polygon boundaries and delivers per-pixel CF-1.8 datacubes (time × y × x):
- Retains the full time axis across all tile acquisition dates — each pixel carries a complete VI time series
- Preserves full spatial resolution for downstream per-pixel analysis (trend, anomaly, classification, etc.)
- Same-CRS tiles mosaiced via a memory-bounded write loop (no resampling)
- Cross-CRS tiles bilinearly reprojected to the dominant CRS before merging
- CF-1.8 global attributes written to all output files
- Output feeds both `phenology` (datacube input mode) and `pixel_phenology` directly

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

### datacube_to_geotiff pipeline (`datacube_to_geotiff.py`)

Reads per-pixel datacubes (output of the `netcdf_datacube` pipeline) and writes three
multi-band GeoTiffs per (VI, region) for delivery to downstream models:
- **`*_per_year.tif`** — N_years × 3 bands: median, p05, p95 per calendar year
- **`*_per_month.tif`** — 36 bands (12 months × 3 statistics); per-year percentiles computed first, then averaged across years
- **`*_per_doy.tif`** — 1095 bands (365 DOYs × 3 statistics); raw observations pooled across all years at each DOY
- LZW-compressed, 256×256 tiled GeoTiff; BigTIFF when > 4 GB; NoData = CF float32 fill value
- Band descriptions accessible via `gdalinfo -mdd all` or `rasterio.open().descriptions`
- Streaming band-by-band write — peak memory is one spatial band regardless of output size
- Use `--skip-per-doy` for large regions where the 1095-band product is impractical

---

## Documentation

Full documentation — setup, CLI reference, spatial input, smoothing methods, phenological
metrics, and output files — is available at:

**[https://vi-phenology.readthedocs.io/en/latest/overview.html](https://vi-phenology.readthedocs.io/en/latest/overview.html)**

---

## Authors

**Stephen Conklin**, Geospatial Analyst — Pipeline architecture, orchestration, and all original code.
[https://github.com/stephenconklin](https://github.com/stephenconklin)

**G. Burch Fisher, PhD**, Research Scientist — Conceptual guidance and original code adapted for:
- `src/pixel_phenology_extract.py` (Per-pixel phenological metric extraction from CF-1.8 datacubes)

**AI Assistance:** This tool was developed with the assistance of Anthropic Claude / Claude Code. These tools assisted
with code generation and refinement under the direction and review of the author.

---

## License

[MIT License](LICENSE) · Copyright (c) 2026 Stephen Conklin
