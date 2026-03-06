#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# smooth.py
# Layer 2: Gap-fill and smooth a daily VI time series.
#
# Strategy (avoids gap-boundary artifacts):
#   1. Apply smoothing to raw observation dates only (irregular spacing OK for LOESS;
#      bin to observation-density grid for Savitzky-Golay)
#   2. Interpolate the smoothed-but-sparse result to a complete daily axis
#   3. Tag each daily value with a provenance flag:
#        'observed'      — smoothed value on an actual observation date
#        'interpolated'  — estimated between first and last observation
#        'extrapolated'  — estimated before first or after last observation
#
# Adds columns to the Layer 1 DataFrame:
#   vi_smooth      (float32) — continuous daily smoothed values
#   vi_smooth_flag (str)     — provenance: observed | interpolated | extrapolated
#
# Author:  Stephen Conklin <stephenconklin@gmail.com>
# License: MIT

import logging

import numpy as np
import pandas as pd
from scipy.signal import savgol_filter

from phenology_config import PhenologyConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Smoothing methods
# ---------------------------------------------------------------------------

def smooth_savgol(obs_series: pd.Series, window_days: int, polyorder: int) -> pd.Series:
    """Apply Savitzky-Golay filter to observation dates, then interpolate to daily.

    S-G requires a uniformly spaced series. Strategy:
      - Bin observations to the median inter-observation spacing
      - Apply S-G filter on the binned series
      - Interpolate binned result back to the full daily axis

    Falls back to smooth_linear() if there are too few observations to fit the
    filter (len(obs_series) <= polyorder or feasible window cannot be constructed).

    Args:
        obs_series:   pd.Series with datetime index (observation dates only, no NaN).
        window_days:  Filter window in days. Rounded up to nearest odd number.
        polyorder:    Polynomial order for the S-G filter (must be < window length).

    Returns:
        pd.Series with datetime index covering all days (first obs → last obs),
        smoothed and gap-filled.
    """
    if len(obs_series) <= polyorder:
        logger.warning(
            "smooth_savgol: only %d observations (polyorder=%d) — "
            "falling back to linear interpolation",
            len(obs_series), polyorder,
        )
        return smooth_linear(obs_series)

    # Compute median inter-observation spacing to set bin size.
    diffs = pd.Series(obs_series.index).diff().dt.days.dropna()
    bin_size = max(int(np.round(diffs.median())), 1)
    logger.debug(
        "smooth_savgol: %d obs, median spacing=%dd, bin_size=%dd",
        len(obs_series), int(diffs.median()), bin_size,
    )

    first, last = obs_series.index[0], obs_series.index[-1]
    bin_dates = pd.date_range(first, last, freq=f'{bin_size}D')

    # Interpolate observations onto the regular bin grid.
    combined_index = obs_series.index.union(bin_dates)
    binned = (
        obs_series
        .reindex(combined_index)
        .interpolate(method='time')
        .reindex(bin_dates)
    )
    n_bins = len(binned)

    # Determine window in bins; must be odd and > polyorder.
    win_bins = max(window_days // bin_size, polyorder + 2)
    if win_bins % 2 == 0:
        win_bins += 1

    # Cap at series length (keeping odd).
    if win_bins > n_bins:
        win_bins = n_bins if n_bins % 2 == 1 else n_bins - 1

    # Re-enforce polyorder constraint after capping.
    min_win = polyorder + 1 if (polyorder + 1) % 2 == 1 else polyorder + 2
    win_bins = max(win_bins, min_win)

    # Final safety check: if window still exceeds n_bins, S-G is infeasible.
    if win_bins > n_bins:
        logger.warning(
            "smooth_savgol: S-G window (%d bins) > n_bins (%d) after all adjustments "
            "(polyorder=%d, bin_size=%dd) — falling back to linear interpolation",
            win_bins, n_bins, polyorder, bin_size,
        )
        return smooth_linear(obs_series)

    logger.debug(
        "smooth_savgol: applying filter with window=%d bins (%dd), polyorder=%d",
        win_bins, win_bins * bin_size, polyorder,
    )
    smoothed_vals = savgol_filter(binned.values, window_length=win_bins, polyorder=polyorder)
    smoothed_binned = pd.Series(smoothed_vals, index=bin_dates)

    # Interpolate filtered bins back to full daily axis.
    daily_index = pd.date_range(first, last, freq='D')
    combined = smoothed_binned.reindex(smoothed_binned.index.union(daily_index))
    result = combined.interpolate(method='time').reindex(daily_index)
    return result.astype(np.float32)


def smooth_loess(obs_series: pd.Series, window_days: int) -> pd.Series:
    """Apply LOESS/LOWESS smoothing to observation dates, then interpolate to daily.

    Uses statsmodels.nonparametric.smoothers_lowess.lowess().
    The frac parameter is derived from window_days / total_span_days.

    Falls back to smooth_linear() when fewer than 3 observations are available
    (LOWESS requires at least 3 points to be meaningful).

    Args:
        obs_series:   pd.Series with datetime index (observation dates only, no NaN).
        window_days:  Approximate smoothing window in days (converted to frac).

    Returns:
        pd.Series with datetime index covering all days (first obs → last obs).
    """
    if len(obs_series) < 3:
        logger.warning(
            "smooth_loess: only %d observation(s) — falling back to linear interpolation",
            len(obs_series),
        )
        return smooth_linear(obs_series)

    from statsmodels.nonparametric.smoothers_lowess import lowess

    first = obs_series.index[0]
    x = np.array((obs_series.index - first).days, dtype=float)
    y = obs_series.values.astype(float)

    total_span = x[-1] if x[-1] > 0 else 1.0
    frac = min(window_days / total_span, 1.0)
    logger.debug(
        "smooth_loess: %d obs, span=%.0fd, frac=%.3f", len(obs_series), total_span, frac
    )

    smoothed = lowess(y, x, frac=frac, return_sorted=True)

    daily_index = pd.date_range(first, obs_series.index[-1], freq='D')
    daily_x = np.array((daily_index - first).days, dtype=float)
    daily_y = np.interp(daily_x, smoothed[:, 0], smoothed[:, 1])
    return pd.Series(daily_y.astype(np.float32), index=daily_index)


def smooth_linear(obs_series: pd.Series) -> pd.Series:
    """Linear interpolation between observations — gap-fill only, no smoothing.

    Connects raw observations with straight lines. No smoothing is applied;
    the result passes exactly through each observation value.

    Args:
        obs_series:   pd.Series with datetime index (observation dates only, no NaN).

    Returns:
        pd.Series with datetime index covering all days (first obs → last obs).
    """
    first, last = obs_series.index[0], obs_series.index[-1]
    daily_index = pd.date_range(first, last, freq='D')
    combined = obs_series.reindex(obs_series.index.union(daily_index))
    result = combined.interpolate(method='time').reindex(daily_index)
    return result.astype(np.float32)


def smooth_harmonic(obs_series: pd.Series, n_harmonics: int = 3) -> pd.Series:
    """Fit a harmonic (Fourier) model to the observation series, evaluate on daily axis.

    Fits: VI(t) = a0 + Σ_k [ a_k * cos(2π k t / T) + b_k * sin(2π k t / T) ]
    where T = 365.25 days and t = day of year.

    Falls back to smooth_linear() when there are fewer observations than model
    parameters (2 * n_harmonics + 1), ensuring the system is not underdetermined.

    Best for:
      - Multi-year smooth trend decomposition
      - Interpolating across very long cloud gaps

    Args:
        obs_series:   pd.Series with datetime index (observation dates only, no NaN).
        n_harmonics:  Number of harmonic terms. 1 = annual cycle only; 3 = default.

    Returns:
        pd.Series with datetime index covering all days (first obs → last obs).
    """
    n_params = 2 * n_harmonics + 1
    if len(obs_series) < n_params:
        logger.warning(
            "smooth_harmonic: only %d observations but model requires >= %d "
            "(n_harmonics=%d) — falling back to linear interpolation",
            len(obs_series), n_params, n_harmonics,
        )
        return smooth_linear(obs_series)

    T = 365.25
    first = obs_series.index[0]
    t = np.array((obs_series.index - first).days, dtype=float)
    y = obs_series.values.astype(float)

    # Build design matrix: [1, cos(2πkt/T), sin(2πkt/T)] for k = 1..n_harmonics
    cols = [np.ones(len(t))]
    for k in range(1, n_harmonics + 1):
        cols.append(np.cos(2 * np.pi * k * t / T))
        cols.append(np.sin(2 * np.pi * k * t / T))
    X = np.column_stack(cols)

    coeffs, residuals, rank, _ = np.linalg.lstsq(X, y, rcond=None)
    logger.debug(
        "smooth_harmonic: %d obs, %d harmonics, matrix rank=%d", len(obs_series), n_harmonics, rank
    )

    # Evaluate on full daily axis.
    daily_index = pd.date_range(first, obs_series.index[-1], freq='D')
    t_daily = np.array((daily_index - first).days, dtype=float)
    cols_daily = [np.ones(len(t_daily))]
    for k in range(1, n_harmonics + 1):
        cols_daily.append(np.cos(2 * np.pi * k * t_daily / T))
        cols_daily.append(np.sin(2 * np.pi * k * t_daily / T))
    X_daily = np.column_stack(cols_daily)

    y_hat = X_daily @ coeffs
    return pd.Series(y_hat.astype(np.float32), index=daily_index)


# ---------------------------------------------------------------------------
# Provenance flagging
# ---------------------------------------------------------------------------

def assign_provenance_flags(
    daily_series: pd.Series, obs_dates: pd.DatetimeIndex
) -> pd.Series:
    """Assign provenance flags to each daily date.

    Returns a pd.Series of str with same index as daily_series:
      'observed'     — date is in obs_dates
      'interpolated' — date is between first and last obs_date (not in obs_dates)
      'extrapolated' — date is before first or after last obs_date
    """
    obs_set = set(obs_dates.normalize())
    first_obs = obs_dates.min()
    last_obs = obs_dates.max()

    flags = []
    for date in daily_series.index:
        d = pd.Timestamp(date).normalize()
        if d in obs_set:
            flags.append('observed')
        elif first_obs <= d <= last_obs:
            flags.append('interpolated')
        else:
            flags.append('extrapolated')
    return pd.Series(flags, index=daily_series.index, dtype=str)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def smooth_timeseries(raw: dict, config: PhenologyConfig) -> dict:
    """Main entry point for Layer 2 smoothing.

    Accepts the raw dict produced by extract_timeseries() (Layer 0+1 output).
    For each (vi, region_label) key:
      - Extracts non-NaN observation rows
      - Applies the configured smoothing method
      - Assigns provenance flags
      - Adds vi_smooth and vi_smooth_flag columns to the DataFrame

    Args:
        raw:    dict keyed by (vi, region_label) → pd.DataFrame (Layer 0+1 output)
        config: PhenologyConfig with smooth_method, smooth_window, smooth_polyorder

    Returns:
        A new dict with the same keys and DataFrames extended with:
            vi_smooth      (float32)
            vi_smooth_flag (str: observed | interpolated | extrapolated)
    """
    result = {}

    for (vi, region_label), df in raw.items():
        out_df = df.copy()

        # Extract observation-only rows (non-NaN vi_daily).
        obs_df = df[df['vi_daily'].notna()].set_index('date')
        obs_series = obs_df['vi_daily'].astype(np.float32)

        if obs_series.empty:
            logger.warning(
                "%s / %s: no valid observations — assigning 'extrapolated' to all dates",
                vi, region_label,
            )
            out_df['vi_smooth'] = np.float32(np.nan)
            out_df['vi_smooth_flag'] = 'extrapolated'
            result[(vi, region_label)] = out_df
            continue

        n_obs = len(obs_series)
        date_range_str = (
            f"{obs_series.index.min().date()} → {obs_series.index.max().date()}"
        )
        logger.info(
            "%s / %s: smoothing %d observations (%s) with method='%s'",
            vi, region_label, n_obs, date_range_str, config.smooth_method,
        )

        # Apply the configured smoothing method.
        method = config.smooth_method
        if method == 'savgol':
            smoothed = smooth_savgol(obs_series, config.smooth_window, config.smooth_polyorder)
        elif method == 'loess':
            smoothed = smooth_loess(obs_series, config.smooth_window)
        elif method == 'linear':
            smoothed = smooth_linear(obs_series)
        elif method == 'harmonic':
            smoothed = smooth_harmonic(obs_series)
        else:
            raise ValueError(f"Unknown smooth_method: {method!r}")

        # Assign provenance flags.
        obs_dates = pd.DatetimeIndex(obs_series.index)
        flags = assign_provenance_flags(smoothed, obs_dates)

        n_observed = int((flags == 'observed').sum())
        n_interpolated = int((flags == 'interpolated').sum())
        n_extrapolated = int((flags == 'extrapolated').sum())
        logger.info(
            "%s / %s: %d daily values — %d observed, %d interpolated, %d extrapolated",
            vi, region_label, len(smoothed),
            n_observed, n_interpolated, n_extrapolated,
        )

        # Merge smoothed values and flags back onto the full daily DataFrame.
        out_df = out_df.set_index('date')
        out_df['vi_smooth'] = smoothed.reindex(out_df.index).astype(np.float32)
        out_df['vi_smooth_flag'] = flags.reindex(out_df.index)
        # Rows outside the smoothed range (before first/after last obs) are extrapolated.
        out_df.loc[out_df['vi_smooth_flag'].isna(), 'vi_smooth_flag'] = 'extrapolated'

        result[(vi, region_label)] = out_df.reset_index()

    return result
