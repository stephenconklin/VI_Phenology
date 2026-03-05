# Smoothing

Smoothing is applied using an **obs-first** strategy: the filter is applied to raw observation
dates only (not to the gap-filled daily series), then the result is interpolated to a complete
daily axis. This avoids artifacts at gap boundaries that arise when a smoothing filter is run
over a NaN-filled series.

---

## Methods

| Method | Flag | Notes |
|--------|------|-------|
| Savitzky-Golay | `savgol` | Default. Uses `--smooth-window` + `--smooth-polyorder`. Bins observations to median inter-observation spacing before filtering. |
| LOESS/LOWESS | `loess` | Adaptive to irregular observation spacing. Uses `--smooth-window`. |
| Linear | `linear` | Gap-fill only — connects observations with straight lines; no smoothing applied. |
| Harmonic | `harmonic` | Fourier fit: `VI(t) = a₀ + Σ [aₖ cos(2πkt/T) + bₖ sin(2πkt/T)]` with T=365.25 days. Best for multi-year trend decomposition. |
| None | `none` | Skip Layer 2 entirely. Only Layers 0+1 (raw + daily reindex) are produced. `--metrics` requires a smoothing method. |

---

## Provenance Flags

Each smoothed value carries a provenance flag in the `vi_smooth_flag` column:

| Flag | Meaning |
|------|---------|
| `observed` | Value falls on an actual observation date |
| `interpolated` | Value is between the first and last observation (gap-filled by interpolation) |
| `extrapolated` | Value is before the first or after the last observation |

---

## Savitzky-Golay Binning

S-G requires uniformly spaced input. Observations are binned to the median inter-observation
spacing before filtering, then the filtered result is interpolated back to the full daily axis.
A fixed 1-day grid is not used — observation density varies across seasons and sensors.

---

## CLI Parameters

| Argument | Default | Description |
|----------|---------|-------------|
| `--smooth-method METHOD` | `savgol` | Method: `savgol` `loess` `linear` `harmonic` `none` |
| `--smooth-window DAYS` | `15` | Window size in days (savgol and loess) |
| `--smooth-polyorder N` | `3` | Polynomial order for Savitzky-Golay |
