# Phenological Metrics

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

---

## Output Files

Per-region metrics are written to `{VI}_{region}_metrics.csv` in the per-region subdirectory.
When `--shapefile-field` yields multiple regions for a shapefile, a combined
`{VI}_{shapefile_stem}_metrics.csv` is also written to the shapefile root folder.

For full output file details, see [Output](output.md).
