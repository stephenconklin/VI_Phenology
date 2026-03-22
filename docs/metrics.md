# Phenological Metrics — Phenology Pipeline

This page covers metrics computed by the **phenology pipeline** (`src/vi_phenology.py`):
one row per year per region in a CSV, enabled with the `--metrics` flag.

The **pixel phenology pipeline** computes a related but distinct set of 19 per-pixel
metrics stored as spatial map layers (one band per metric in a NetCDF). See
[Pixel Phenology Pipeline — Metrics](pixel_phenology.md#metrics-19).

---

Computed per year, per region, per VI from the smoothed daily series. Enable with `--metrics`.
Requires a smoothing method (`--smooth-method` must not be `none`).

---

## Metrics

| Metric | Column | Description |
|--------|--------|-------------|
| SOS | `sos_date`, `sos_doy` | Start of Season: first date VI crosses the amplitude threshold going up |
| POS | `pos_date`, `pos_doy`, `pos_value` | Peak of Season: date and value of annual maximum |
| EOS | `eos_date`, `eos_doy` | End of Season: last date VI is still above the amplitude threshold |
| LOS | `los_days` | Length of Season: EOS − SOS in days |
| IVI | `ivi` | Integrated VI: trapezoidal area under the smooth curve between SOS and EOS |
| Greening rate | `greening_rate` | Mean slope (VI/day) from SOS to POS |
| Senescence rate | `senescence_rate` | Mean slope (VI/day) from POS to EOS (negative for a declining curve) |
| Floor NDVI | `floor_ndvi` | Annual minimum of the smooth curve (dry-season trough). Derived directly from the curve — no biome-specific DOY window needed. |
| Ceiling NDVI | `ceiling_ndvi` | Annual maximum of the smooth curve (= POS value) |
| Season length | `season_length_days` | Days above `floor + sos_threshold × amplitude`; computed from actual dates (cross-year safe) |
| Green-up rate | `greenup_rate` | `(ceiling − floor) / (peak_date − floor_date).days` |
| Peak count | `n_peaks` | Number of peaks detected by `scipy.signal.find_peaks` (prominence + distance thresholds) |
| Peak separation | `peak_separation_days` | Calendar days between the two tallest peaks (NaN if n_peaks < 2) |
| Relative peak amplitude | `relative_peak_amplitude` | `min(h1, h2) / max(h1, h2)` ratio between the two tallest peaks (NaN if n_peaks < 2) |
| Valley depth | `valley_depth` | Normalised trough depth between peaks: `((h1+h2)/2 − valley) / ((h1+h2)/2)` (NaN if n_peaks < 2) |
| CV | `cv` | Coefficient of variation of raw (unsmoothed) observations across the full time series. Same value on every year row for a given (vi, region). |

---

## SOS/EOS Threshold

SOS and EOS are determined relative to the annual amplitude:

```
baseline  = annual minimum VI
amplitude = annual peak − baseline
threshold = baseline + sos_threshold × amplitude
```

`--sos-threshold` controls the fraction of amplitude (default `0.20` = 20%).

---

## Annual Windows — `--year-start-doy`

`--year-start-doy` sets the day of year at which each annual phenology window begins
(default `1` = January 1). The correct value depends on **biome and rainfall regime**, not
simply on hemisphere.

**The rule:** place the window boundary at the VI minimum for the target region. Placing
it at or near the seasonal peak will cause SOS/EOS detection to fail or produce nonsensical
results.

| Biome / regime | Peak VI | VI minimum | Recommended `--year-start-doy` |
|---|---|---|---|
| Northern Hemisphere temperate | Jun–Aug | Dec–Jan | `1` (January 1) |
| Southern Hemisphere summer-rainfall (Savanna, Highveld) | Dec–Jan | Jun–Jul | `182` (July 1) |
| Southern Hemisphere winter-rainfall (Cape fynbos, Mediterranean) | Jun–Aug | Dec–Jan | `1` (January 1) |

`--year-start-doy` only affects how annual windows are split for metric computation. It does
**not** affect the annual phenology plot, which always groups by calendar year and displays
January through December.

---

## CLI Parameters

| Argument | Default | Description |
|----------|---------|-------------|
| `--metrics` | off | Compute and export phenological metrics |
| `--sos-threshold FRACTION` | `0.20` | Amplitude fraction for SOS/EOS detection |
| `--year-start-doy DOY` | `1` | Day of year to begin each annual phenology window (1–365) |
| `--peak-prominence NDVI` | `0.05` | Minimum NDVI prominence for a peak to be counted by bimodality detection (only active when `--metrics` is set) |
| `--peak-min-distance DAYS` | `45` | Minimum separation in days between detected peaks (only active when `--metrics` is set) |

---

## Output Files

Per-region metrics are written to `{VI}_{region}_metrics.csv` in the per-region subdirectory.
When `--shapefile-field` yields multiple regions for a shapefile, a combined
`{VI}_{shapefile_stem}_metrics.csv` is also written to the shapefile root folder.

For full output file details, see [Output](output.md).
