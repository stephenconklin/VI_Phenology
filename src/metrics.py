#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# metrics.py
# Layer 3: Compute phenological metrics from the smooth daily VI series (Layer 2).
#
# Metrics computed per year per region per VI:
#   SOS  — Start of Season: date VI first crosses the amplitude threshold going up
#   POS  — Peak of Season: date and value of annual maximum
#   EOS  — End of Season: date VI last crosses the amplitude threshold going down
#   LOS  — Length of Season: EOS - SOS in days
#   IVI  — Integrated VI: trapezoidal area under curve between SOS and EOS
#   Greening rate    — mean slope (VI/day) from SOS to POS
#   Senescence rate  — mean slope (VI/day) from POS to EOS (negative = declining)
#
# SOS/EOS threshold (config.sos_threshold, default 0.20):
#   baseline  = minimum VI value in the annual window
#   amplitude = peak - baseline
#   threshold = baseline + sos_threshold * amplitude
#
# Annual windows are split by config.year_start_doy (default 1 = Jan 1).
# Set year_start_doy > 1 for Southern Hemisphere or Mediterranean phenology.
#
# Output: pd.DataFrame saved as CSV to config.output_dir.
#
# Author:  Stephen Conklin <stephenconklin@gmail.com>
# License: MIT

import logging

import numpy as np
import pandas as pd
from pathlib import Path
from scipy.signal import find_peaks as _find_peaks

from phenology_config import PhenologyConfig

logger = logging.getLogger(__name__)

# Minimum VI amplitude (peak − baseline) required to attempt SOS/EOS detection.
# Below this threshold the signal is considered flat and no season is reported.
_MIN_AMPLITUDE = 1e-6


# ---------------------------------------------------------------------------
# Annual window splitting
# ---------------------------------------------------------------------------

def split_by_year(daily_df: pd.DataFrame, year_start_doy: int = 1) -> dict:
    """Split a daily DataFrame into annual windows.

    If year_start_doy == 1, windows align to calendar years (Jan 1 → Dec 31).
    If year_start_doy > 1, each window runs from that DOY to DOY-1 the next year.
    Example: year_start_doy=274 splits Oct 1 → Sep 30 (Mediterranean seasons).

    Args:
        daily_df:        pd.DataFrame with a 'date' column (datetime64).
        year_start_doy:  Day of year (1–365) to begin each annual window.

    Returns:
        dict keyed by year_label (str, e.g. '2022' or '2022-2023') →
        pd.DataFrame subset for that window.
    """
    df = daily_df.copy()
    doy = df['date'].dt.dayofyear
    year = df['date'].dt.year

    if year_start_doy == 1:
        df['_season_year'] = year
        label_fn = lambda y: str(int(y))
    else:
        # Dates on or after year_start_doy belong to the season starting that year;
        # dates before year_start_doy belong to the season starting the prior year.
        df['_season_year'] = np.where(doy >= year_start_doy, year, year - 1)
        label_fn = lambda y: f"{int(y)}-{int(y) + 1}"

    result = {}
    for season_year, group in df.groupby('_season_year'):
        label = label_fn(season_year)
        result[label] = group.drop(columns=['_season_year']).reset_index(drop=True)
    return result


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------

def find_pos(vi_series: pd.Series) -> tuple:
    """Find the Peak of Season (POS).

    Args:
        vi_series: pd.Series with datetime index, continuous daily VI values.

    Returns:
        (pos_date, pos_value) — (pd.Timestamp, float)
        Returns (None, NaN) if the series is all NaN.
    """
    valid = vi_series.dropna()
    if valid.empty:
        return None, np.nan
    pos_date = valid.idxmax()
    pos_value = float(valid[pos_date])
    return pos_date, pos_value


def find_sos_eos(vi_series: pd.Series, threshold_pct: float = 0.20) -> tuple:
    """Find Start (SOS) and End (EOS) of Season using an amplitude threshold.

    Algorithm:
      1. baseline  = minimum VI in the series
      2. amplitude = peak_value - baseline
      3. threshold = baseline + threshold_pct * amplitude
      4. SOS = first date VI crosses threshold going up (before POS)
      5. EOS = last date VI crosses threshold going down (after POS)

    Args:
        vi_series:     pd.Series with datetime index, smooth daily VI values.
        threshold_pct: Fraction of amplitude for SOS/EOS detection (default 0.20).

    Returns:
        (sos_date, eos_date) — (pd.Timestamp | None, pd.Timestamp | None)
        Returns (None, None) if season cannot be detected (e.g., too few obs,
        flat signal, or amplitude too small).
    """
    valid = vi_series.dropna()
    if valid.empty:
        return None, None

    pos_date, pos_value = find_pos(valid)
    if pos_date is None:
        return None, None

    baseline = float(valid.min())
    amplitude = pos_value - baseline

    if amplitude < _MIN_AMPLITUDE:
        return None, None

    threshold = baseline + threshold_pct * amplitude

    # SOS: first date at or before POS where VI first reaches the threshold.
    pre_pos = valid.loc[:pos_date]
    above_pre = pre_pos[pre_pos >= threshold]
    sos_date = above_pre.index[0] if not above_pre.empty else None

    # EOS: last date at or after POS where VI is still above the threshold.
    post_pos = valid.loc[pos_date:]
    above_post = post_pos[post_pos >= threshold]
    eos_date = above_post.index[-1] if not above_post.empty else None

    return sos_date, eos_date


def compute_ivi(vi_series: pd.Series, sos_date: pd.Timestamp, eos_date: pd.Timestamp) -> float:
    """Compute Integrated VI (IVI) — area under curve between SOS and EOS.

    Uses the trapezoidal rule (np.trapezoid) on daily values.
    Returns NaN if sos_date or eos_date is None.
    """
    if sos_date is None or eos_date is None:
        return np.nan
    season = vi_series.loc[sos_date:eos_date].dropna()
    if season.empty:
        return np.nan
    return float(np.trapezoid(season.values))


def compute_greening_rate(vi_series: pd.Series, sos_date: pd.Timestamp, pos_date: pd.Timestamp) -> float:
    """Compute mean greening rate (VI per day) from SOS to POS.

    Simple linear slope: (VI_pos - VI_sos) / (pos_date - sos_date).days
    Returns NaN if either date is None or the span is zero.
    """
    if sos_date is None or pos_date is None:
        return np.nan
    span = (pos_date - sos_date).days
    if span == 0:
        return np.nan
    if sos_date not in vi_series.index or pos_date not in vi_series.index:
        logger.warning(
            "compute_greening_rate: sos_date (%s) or pos_date (%s) not in series index",
            sos_date.date(), pos_date.date(),
        )
        return np.nan
    vi_sos = float(vi_series.loc[sos_date])
    vi_pos = float(vi_series.loc[pos_date])
    return (vi_pos - vi_sos) / span


def compute_senescence_rate(vi_series: pd.Series, pos_date: pd.Timestamp, eos_date: pd.Timestamp) -> float:
    """Compute mean senescence rate (VI per day) from POS to EOS.

    Simple linear slope: (VI_eos - VI_pos) / (eos_date - pos_date).days
    Will be negative for a typical declining curve.
    Returns NaN if either date is None or the span is zero.
    """
    if pos_date is None or eos_date is None:
        return np.nan
    span = (eos_date - pos_date).days
    if span == 0:
        return np.nan
    if pos_date not in vi_series.index or eos_date not in vi_series.index:
        logger.warning(
            "compute_senescence_rate: pos_date (%s) or eos_date (%s) not in series index",
            pos_date.date(), eos_date.date(),
        )
        return np.nan
    vi_pos = float(vi_series.loc[pos_date])
    vi_eos = float(vi_series.loc[eos_date])
    return (vi_eos - vi_pos) / span


# ---------------------------------------------------------------------------
# Extended metric helpers (floor/ceiling/season_length/bimodality/cv)
# ---------------------------------------------------------------------------

def compute_floor_ceiling(vi_series: pd.Series) -> tuple:
    """Derive floor and ceiling NDVI directly from the annual smooth curve.

    Floor   = minimum of the smoothed annual series (dry-season trough).
    Ceiling = maximum of the smoothed annual series (wet-season peak).

    Both are derived entirely from the curve shape — no DOY windows or
    biome-specific configuration required.

    Args:
        vi_series: pd.Series with DatetimeIndex, smooth daily VI values for one year.

    Returns:
        (floor_ndvi, ceiling_ndvi) as floats; both NaN if series is empty.
    """
    valid = vi_series.dropna()
    if valid.empty:
        return np.nan, np.nan
    return float(valid.min()), float(valid.max())


def compute_season_length(
    vi_series: pd.Series, floor_ndvi: float, threshold_pct: float
) -> float:
    """Days the VI remains above floor + threshold_pct * amplitude.

    Uses actual dates (not DOY arithmetic) so it handles cross-year windows
    (e.g. Southern Hemisphere seasons starting mid-year) correctly.

    Args:
        vi_series:     pd.Series with DatetimeIndex, smooth daily VI values.
        floor_ndvi:    Dry-season floor derived from compute_floor_ceiling().
        threshold_pct: Amplitude fraction defining the season edges (same as sos_threshold).

    Returns:
        Season length in days (float); NaN if undetermined.
    """
    if np.isnan(floor_ndvi) or vi_series.empty:
        return np.nan
    valid = vi_series.dropna()
    if valid.empty:
        return np.nan
    ceiling = float(valid.max())
    amplitude = ceiling - floor_ndvi
    if amplitude < _MIN_AMPLITUDE:
        return np.nan
    threshold = floor_ndvi + threshold_pct * amplitude
    above_dates = valid.index[valid >= threshold]
    if len(above_dates) < 2:
        return np.nan
    return float((above_dates[-1] - above_dates[0]).days)


def compute_greenup_rate(vi_series: pd.Series, floor_ndvi: float) -> float:
    """Green-up rate (VI/day) from the curve-derived floor to the seasonal peak.

    Slope = (peak_value - floor_value) / (peak_date - floor_date).days.
    Floor location is the date of the annual minimum (from the smooth curve).

    Args:
        vi_series:  pd.Series with DatetimeIndex, smooth daily VI values.
        floor_ndvi: Annual minimum value from compute_floor_ceiling().

    Returns:
        Green-up rate (float); NaN if undetermined.
    """
    if np.isnan(floor_ndvi) or vi_series.empty:
        return np.nan
    valid = vi_series.dropna()
    if valid.empty:
        return np.nan
    floor_date = valid.idxmin()
    peak_date = valid.idxmax()
    if floor_date >= peak_date:
        return np.nan
    span = (peak_date - floor_date).days
    if span == 0:
        return np.nan
    return float((float(valid[peak_date]) - floor_ndvi) / span)


def compute_bimodality(
    vi_series: pd.Series,
    peak_prominence: float,
    peak_min_distance_days: int,
) -> tuple:
    """Detect peaks in the smooth annual series and quantify bimodality.

    Uses scipy.signal.find_peaks with prominence and minimum-distance constraints.
    When fewer than two peaks are found, bimodality metrics are NaN.

    Peak separation is measured in calendar days (not DOY), so it is correct for
    cross-year annual windows.

    Args:
        vi_series:              pd.Series with DatetimeIndex, smooth daily VI values.
        peak_prominence:        Min NDVI prominence for a peak to count (e.g. 0.05).
        peak_min_distance_days: Min separation (integer array positions ≈ days) between peaks.

    Returns:
        (n_peaks, peak_separation_days, relative_peak_amplitude, valley_depth)
        All bimodality scalars are NaN when n_peaks < 2.
    """
    valid = vi_series.dropna()
    if valid.empty:
        return 0, np.nan, np.nan, np.nan

    y = valid.values
    peaks, _ = _find_peaks(
        y,
        prominence=peak_prominence,
        distance=peak_min_distance_days,
    )
    n_p = int(len(peaks))

    if n_p < 2:
        return n_p, np.nan, np.nan, np.nan

    # Two tallest peaks (by height).
    sorted_peaks = peaks[np.argsort(y[peaks])[::-1]]
    p1, p2 = sorted_peaks[0], sorted_peaks[1]

    # Separation in calendar days.
    dates = valid.index
    sep = float(abs((dates[p1] - dates[p2]).days))

    h1, h2 = float(y[p1]), float(y[p2])
    if max(h1, h2) > 0:
        rel_amp = float(min(h1, h2) / max(h1, h2))
    else:
        rel_amp = np.nan

    # Normalised valley depth between the two peaks.
    lo, hi = min(p1, p2), max(p1, p2)
    valley = float(np.nanmin(y[lo : hi + 1]))
    mean_peak = (h1 + h2) / 2
    valley_d = float((mean_peak - valley) / mean_peak) if mean_peak > 0 else np.nan

    return n_p, sep, rel_amp, valley_d


def compute_cv(raw_obs: pd.Series) -> float:
    """Coefficient of variation of raw (unsmoothed) VI observations.

    CV = std / mean over all valid observation values for the full time series.
    Whole-series metric — the same value is attached to every annual row for a
    given (vi, region) pair.

    Args:
        raw_obs: pd.Series of raw VI values; may contain NaN (non-obs days).

    Returns:
        CV (float); NaN if mean ≤ 0 or no valid observations.
    """
    valid = raw_obs.dropna()
    if valid.empty:
        return np.nan
    mean_val = float(valid.mean())
    if mean_val <= 0:
        return np.nan
    return float(valid.std() / mean_val)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def compute_metrics(smoothed: dict, config: PhenologyConfig) -> pd.DataFrame:
    """Main entry point for Layer 3 phenological metrics.

    For each (vi, region_label) key in smoothed:
      - Splits the smooth daily series into annual windows
      - Computes SOS, POS, EOS, LOS, IVI, greening rate, senescence rate per year
      - Collects results into a single DataFrame

    Saves to: config.output_dir / '{vi}_{region_label}_metrics.csv'
    Also returns the full metrics DataFrame.

    Output columns:
        vi, region, year_label,
        sos_date, sos_doy,
        pos_date, pos_doy, pos_value,
        eos_date, eos_doy,
        los_days, ivi,
        greening_rate, senescence_rate,
        floor_ndvi, ceiling_ndvi,
        season_length_days,
        greenup_rate,
        n_peaks, peak_separation_days,
        relative_peak_amplitude, valley_depth,
        cv

    Args:
        smoothed: dict keyed by (vi, region_label) → pd.DataFrame (Layer 2 output)
        config:   PhenologyConfig with sos_threshold, year_start_doy, output_dir,
                  min_valid_obs_per_year

    Returns:
        pd.DataFrame with one row per (vi, region, year).
    """
    all_rows = []

    for (vi, region_label), df in smoothed.items():
        annual_windows = split_by_year(df, config.year_start_doy)
        logger.info(
            "%s / %s: computing metrics for %d annual window(s) "
            "(sos_threshold=%.2f, year_start_doy=%d)",
            vi, region_label, len(annual_windows),
            config.sos_threshold, config.year_start_doy,
        )

        # CV is a whole-series metric — computed once per (vi, region).
        cv = compute_cv(df['vi_daily'])

        pair_rows = []
        for year_label, year_df in annual_windows.items():
            vi_series = year_df.set_index('date')['vi_smooth'].dropna()

            if vi_series.empty:
                logger.warning(
                    "%s / %s / %s: no valid smooth data in annual window — skipping",
                    vi, region_label, year_label,
                )
                continue

            n_obs_yr = int((year_df['vi_count'] > 0).sum())
            if n_obs_yr < config.min_valid_obs_per_year:
                logger.warning(
                    "%s / %s / %s: only %d observation(s) in annual window "
                    "(min_valid_obs_per_year=%d) — skipping year",
                    vi, region_label, year_label,
                    n_obs_yr, config.min_valid_obs_per_year,
                )
                continue

            pos_date, pos_value = find_pos(vi_series)
            sos_date, eos_date = find_sos_eos(vi_series, config.sos_threshold)

            # los_days stored as float so it can hold NaN cleanly in the DataFrame.
            los_days = float((eos_date - sos_date).days) if sos_date and eos_date else np.nan
            ivi = compute_ivi(vi_series, sos_date, eos_date)
            green_rate = compute_greening_rate(vi_series, sos_date, pos_date)
            sen_rate = compute_senescence_rate(vi_series, pos_date, eos_date)

            # Extended metrics — derived directly from the annual smooth curve.
            floor_ndvi, ceiling_ndvi = compute_floor_ceiling(vi_series)
            season_len = compute_season_length(vi_series, floor_ndvi, config.sos_threshold)
            greenup_rate = compute_greenup_rate(vi_series, floor_ndvi)
            n_peaks, peak_sep, rel_amp, valley_d = compute_bimodality(
                vi_series, config.peak_prominence, config.peak_min_distance_days
            )

            logger.info(
                "%s / %s / %s: "
                "SOS=%s (DOY %s), POS=%s (DOY %s, val=%.4f), EOS=%s (DOY %s), "
                "LOS=%s d, IVI=%.3f, green_rate=%.5f, sen_rate=%.5f | "
                "floor=%.4f, ceil=%.4f, season_len=%s d, n_peaks=%d",
                vi, region_label, year_label,
                sos_date.date() if sos_date else "N/A",
                int(sos_date.dayofyear) if sos_date else "N/A",
                pos_date.date() if pos_date else "N/A",
                int(pos_date.dayofyear) if pos_date else "N/A",
                pos_value,
                eos_date.date() if eos_date else "N/A",
                int(eos_date.dayofyear) if eos_date else "N/A",
                int(los_days) if not np.isnan(los_days) else "N/A",
                ivi if not np.isnan(ivi) else float('nan'),
                green_rate if not np.isnan(green_rate) else float('nan'),
                sen_rate if not np.isnan(sen_rate) else float('nan'),
                floor_ndvi if not np.isnan(floor_ndvi) else float('nan'),
                ceiling_ndvi if not np.isnan(ceiling_ndvi) else float('nan'),
                int(season_len) if not np.isnan(season_len) else "N/A",
                n_peaks,
            )

            pair_rows.append({
                'vi': vi,
                'region': region_label,
                'year_label': year_label,
                'sos_date': sos_date,
                'sos_doy': int(sos_date.dayofyear) if sos_date else np.nan,
                'pos_date': pos_date,
                'pos_doy': int(pos_date.dayofyear) if pos_date else np.nan,
                'pos_value': pos_value,
                'eos_date': eos_date,
                'eos_doy': int(eos_date.dayofyear) if eos_date else np.nan,
                'los_days': los_days,
                'ivi': ivi,
                'greening_rate': green_rate,
                'senescence_rate': sen_rate,
                # Extended metrics
                'floor_ndvi': floor_ndvi,
                'ceiling_ndvi': ceiling_ndvi,
                'season_length_days': season_len,
                'greenup_rate': greenup_rate,
                'n_peaks': n_peaks,
                'peak_separation_days': peak_sep,
                'relative_peak_amplitude': rel_amp,
                'valley_depth': valley_d,
                'cv': cv,
            })

        if pair_rows:
            pair_df = pd.DataFrame(pair_rows)
            filename = f"{vi}_{region_label}_metrics.csv"
            out_dir = config.output_dir_for(region_label)
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / filename
            pair_df.to_csv(out_path, index=False)
            logger.info("Saved metrics CSV: %s", out_path)
            all_rows.extend(pair_rows)
        else:
            logger.warning(
                "%s / %s: no annual windows produced valid metrics — no CSV written",
                vi, region_label,
            )

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Combined shapefile metrics (--shapefile-field mode only)
# ---------------------------------------------------------------------------

def write_combined_metrics(metrics_df: pd.DataFrame, config: PhenologyConfig) -> None:
    """Write a per-shapefile combined metrics CSV when --shapefile-field is active.

    For each shapefile × VI combination, stacks all per-region metrics rows into a
    single CSV. The existing 'region' column identifies the source field value for
    each row. Rows are sorted by (region, year_label) for readability.

    This is a complement to the per-region CSVs written by compute_metrics() — both
    are always written when --shapefile-field is used.

    Output: config.output_dir / '{VI}_{shapefile_stem}_metrics.csv'

    Only runs when:
      - config.shapefile_field is set
      - config.shapefiles is not None
      - metrics_df is non-empty
    """
    if not config.shapefile_field or not config.shapefiles or metrics_df.empty:
        return

    import geopandas as gpd
    from io_utils import sanitize_label

    for sf_index, shapefile in enumerate(config.shapefiles):
        field = config.field_for_shapefile(sf_index)
        if field is None:
            # This shapefile was dissolved (field value was 'none') — nothing to combine.
            logger.debug(
                "write_combined_metrics: shapefile '%s' was dissolved — skipping combined CSV",
                shapefile.name,
            )
            continue

        stem = shapefile.stem

        # Read the attribute table to recover which region labels belong to this shapefile.
        try:
            gdf = gpd.read_file(shapefile)
        except Exception as e:
            logger.warning(
                "write_combined_metrics: could not read '%s': %s — skipping combined CSV",
                shapefile.name, e,
            )
            continue

        if field not in gdf.columns:
            logger.warning(
                "write_combined_metrics: field '%s' not found in '%s' — skipping combined CSV",
                field, shapefile.name,
            )
            continue

        region_labels = {
            sanitize_label(str(v))
            for v in gdf[field].dropna().unique()
        }

        subset = metrics_df[metrics_df["region"].isin(region_labels)]
        if subset.empty:
            logger.warning(
                "write_combined_metrics: no metrics found for shapefile '%s' — skipping",
                shapefile.name,
            )
            continue

        for vi in sorted(subset["vi"].unique()):
            vi_subset = (
                subset[subset["vi"] == vi]
                .sort_values(["region", "year_label"])
                .reset_index(drop=True)
            )
            out_path = config.output_dir / f"{vi}_{stem}_metrics.csv"
            vi_subset.to_csv(out_path, index=False)
            logger.info(
                "Saved combined metrics CSV (%d region(s), %d row(s)): %s",
                vi_subset["region"].nunique(), len(vi_subset), out_path,
            )
