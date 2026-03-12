#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# plot.py
# Generate phenology plots from raw (Layer 1) and smoothed (Layer 2) VI data.
#
# Plot types:
#   annual      — VI vs DOY, multi-year overlay, raw scatter + smooth curves
#   timeseries  — Full temporal range, smooth curve + raw scatter + optional markers
#   anomaly     — Annual deviation from multi-year mean VI
#   multi_vi    — Side-by-side NDVI / EVI2 / NIRv for the same region
#
# Output formats: PNG (matplotlib) and/or HTML (plotly interactive).
# Controlled by config.plot_style ('raw' | 'smooth' | 'combined') and
# config.plot_formats (['png'] | ['html'] | ['png', 'html']).
#
# Author:  Stephen Conklin <stephenconklin@gmail.com>
# License: MIT

import datetime
import logging

import matplotlib
matplotlib.use("Agg")  # non-interactive backend for PNG output
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from pathlib import Path

from phenology_config import PhenologyConfig

logger = logging.getLogger(__name__)

_TAB10 = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
]

# DOY positions and labels for each month's first day (non-leap year reference).
_MONTH_DOYS = [datetime.date(2001, m, 1).timetuple().tm_yday for m in range(1, 13)]
_MONTH_LABELS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------

def _output_path(config: PhenologyConfig, stem: str, fmt: str, region_label: str = "") -> Path:
    """Build an output file path inside the region-specific output directory.

    Uses config.output_dir_for(region_label) to resolve the directory, which
    places outputs in per-region subdirectories when shapefiles are provided.
    Creates the directory if it does not exist.
    """
    out_dir = config.output_dir_for(region_label)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir / f"{stem}.{fmt}"


def _save_or_show(fig, path: Path):
    """Save a matplotlib figure to path and close it."""
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info("Saved: %s", path)


# ---------------------------------------------------------------------------
# Annual phenology plot
# ---------------------------------------------------------------------------

def plot_annual_phenology(
    raw: dict, smoothed: dict, config: PhenologyConfig, vi: str, region_label: str
):
    """Annual phenology curve: VI vs day-of-year, one line per year.

    Layout:
      - X axis: day of year (1–365)
      - Y axis: VI value
      - One line per year (color-coded)
      - If plot_style 'combined' or 'raw': semi-transparent raw observation scatter
      - If plot_style 'combined' or 'smooth': solid smooth curve per year

    Saves to: {VI}_{region_label}_annual.{ext}
    """
    df_raw = raw.get((vi, region_label))
    df_smooth = smoothed.get((vi, region_label)) if smoothed else None

    if df_raw is None and df_smooth is None:
        logger.warning(
            "plot_annual_phenology: no data found for %s / %s — skipping", vi, region_label
        )
        return

    # Determine source and VI column for the smooth/filled curve.
    df_curve = df_smooth if df_smooth is not None else df_raw
    vi_col = 'vi_smooth' if (df_smooth is not None and 'vi_smooth' in df_curve.columns) else 'vi_daily'

    df_curve = df_curve.copy()
    df_curve['_year'] = df_curve['date'].dt.year
    df_curve['_doy'] = df_curve['date'].dt.dayofyear
    years = sorted(df_curve['_year'].unique())

    # Multi-year mean curve (only meaningful with >= 2 years).
    mean_by_doy = df_curve.groupby('_doy')[vi_col].mean() if len(years) >= 2 else None

    logger.debug(
        "plot_annual_phenology: %s / %s — %d year(s): %s",
        vi, region_label, len(years), years,
    )

    stem = f"{vi}_{region_label}_annual"

    for fmt in config.plot_formats:
        if fmt == 'png':
            fig, ax = plt.subplots(figsize=(10, 5))

            for i, year in enumerate(years):
                color = _TAB10[i % len(_TAB10)]
                year_curve = df_curve[df_curve['_year'] == year]

                if config.plot_style in ('smooth', 'combined') and vi_col in year_curve.columns:
                    ax.plot(year_curve['_doy'], year_curve[vi_col],
                            color=color, linewidth=1.5, label=str(year))

                if config.plot_style in ('raw', 'combined') and df_raw is not None:
                    raw_year = df_raw[
                        (df_raw['date'].dt.year == year) & df_raw['vi_daily'].notna()
                    ].copy()
                    raw_year['_doy'] = raw_year['date'].dt.dayofyear
                    ax.scatter(raw_year['_doy'], raw_year['vi_daily'],
                               s=20, alpha=0.4, color=color, zorder=3,
                               label=None if config.plot_style == 'combined' else f'{year} obs')

            if mean_by_doy is not None:
                ax.plot(mean_by_doy.index, mean_by_doy.values,
                        color='black', linewidth=2.5, linestyle='--',
                        label='Mean', zorder=5)

            ax.set_xlabel('Month')
            ax.set_ylabel(vi)
            ax.set_title(f'{vi} — {region_label} — Annual Phenology')
            ax.set_xlim(1, 365)
            ax.set_xticks(_MONTH_DOYS)
            ax.set_xticklabels(_MONTH_LABELS)
            ax.legend(title='Year')
            ax.grid(True, alpha=0.3)
            _save_or_show(fig, _output_path(config, stem, 'png', region_label))

        elif fmt == 'html':
            import plotly.graph_objects as go
            fig = go.Figure()

            for i, year in enumerate(years):
                color = _TAB10[i % len(_TAB10)]
                year_curve = df_curve[df_curve['_year'] == year]

                if config.plot_style in ('smooth', 'combined') and vi_col in year_curve.columns:
                    fig.add_trace(go.Scatter(
                        x=year_curve['_doy'], y=year_curve[vi_col],
                        mode='lines', name=str(year),
                        line=dict(color=color, width=2),
                    ))

                if config.plot_style in ('raw', 'combined') and df_raw is not None:
                    raw_year = df_raw[
                        (df_raw['date'].dt.year == year) & df_raw['vi_daily'].notna()
                    ].copy()
                    raw_year['_doy'] = raw_year['date'].dt.dayofyear
                    fig.add_trace(go.Scatter(
                        x=raw_year['_doy'], y=raw_year['vi_daily'],
                        mode='markers', name=f'{year} obs',
                        marker=dict(size=5, opacity=0.4, color=color),
                        showlegend=(config.plot_style != 'combined'),
                    ))

            if mean_by_doy is not None:
                fig.add_trace(go.Scatter(
                    x=mean_by_doy.index.tolist(), y=mean_by_doy.values.tolist(),
                    mode='lines', name='Mean',
                    line=dict(color='black', width=2.5, dash='dash'),
                ))

            fig.update_layout(
                title=f'{vi} — {region_label} — Annual Phenology',
                xaxis_title='Month', yaxis_title=vi,
                xaxis=dict(
                    range=[1, 365],
                    tickvals=_MONTH_DOYS,
                    ticktext=_MONTH_LABELS,
                ),
                template='simple_white',
            )
            out = _output_path(config, stem, 'html', region_label)
            fig.write_html(str(out))
            logger.info("Saved: %s", out)


# ---------------------------------------------------------------------------
# Full time-series plot
# ---------------------------------------------------------------------------

def plot_timeseries(
    raw: dict, smoothed: dict, config: PhenologyConfig, vi: str, region_label: str
):
    """Full temporal range time-series plot.

    Layout:
      - X axis: calendar date (full range)
      - Y axis: VI value
      - Smooth curve (if available) as a solid line
      - Raw observation scatter behind it (semi-transparent, if 'combined' or 'raw')
      - Shaded ±1 std band around observations (from vi_std column)
      - Optional: SOS/POS/EOS vertical markers if metrics CSV exists alongside

    Saves to: {VI}_{region_label}_timeseries.{ext}
    """
    df_raw = raw.get((vi, region_label))
    df_smooth = smoothed.get((vi, region_label)) if smoothed else None

    if df_raw is None:
        logger.warning(
            "plot_timeseries: no raw data for %s / %s — skipping", vi, region_label
        )
        return

    obs = df_raw[df_raw['vi_daily'].notna()].copy()
    stem = f"{vi}_{region_label}_timeseries"

    for fmt in config.plot_formats:
        if fmt == 'png':
            fig, ax = plt.subplots(figsize=(12, 5))

            if config.plot_style in ('raw', 'combined'):
                # ±1 std shading
                std_obs = obs[obs['vi_std'].notna()]
                if not std_obs.empty:
                    ax.fill_between(
                        std_obs['date'],
                        std_obs['vi_daily'] - std_obs['vi_std'],
                        std_obs['vi_daily'] + std_obs['vi_std'],
                        alpha=0.15, color='steelblue', label='±1 std',
                    )
                ax.scatter(obs['date'], obs['vi_daily'],
                           s=18, alpha=0.55, color='steelblue', label='Observations', zorder=3)

            if df_smooth is not None and config.plot_style in ('smooth', 'combined'):
                ax.plot(df_smooth['date'], df_smooth['vi_smooth'],
                        color='#2ca02c', linewidth=1.8, label='Smooth', zorder=4)

            ax.set_xlabel('Date')
            ax.set_ylabel(vi)
            ax.set_title(f'{vi} — {region_label}')
            locator = mdates.AutoDateLocator()
            ax.xaxis.set_major_locator(locator)
            ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
            ax.legend()
            ax.grid(True, alpha=0.3)
            _save_or_show(fig, _output_path(config, stem, 'png', region_label))

        elif fmt == 'html':
            import plotly.graph_objects as go
            fig = go.Figure()

            if config.plot_style in ('raw', 'combined'):
                # ±1 std as a filled band
                std_obs = obs[obs['vi_std'].notna()]
                if not std_obs.empty:
                    fig.add_trace(go.Scatter(
                        x=pd.concat([std_obs['date'], std_obs['date'][::-1]]),
                        y=pd.concat([
                            std_obs['vi_daily'] + std_obs['vi_std'],
                            (std_obs['vi_daily'] - std_obs['vi_std'])[::-1],
                        ]),
                        fill='toself', fillcolor='rgba(70,130,180,0.15)',
                        line=dict(color='rgba(0,0,0,0)'),
                        name='±1 std', showlegend=True,
                    ))
                fig.add_trace(go.Scatter(
                    x=obs['date'], y=obs['vi_daily'],
                    mode='markers', name='Observations',
                    marker=dict(size=5, opacity=0.55, color='steelblue'),
                ))

            if df_smooth is not None and config.plot_style in ('smooth', 'combined'):
                fig.add_trace(go.Scatter(
                    x=df_smooth['date'], y=df_smooth['vi_smooth'],
                    mode='lines', name='Smooth',
                    line=dict(color='#2ca02c', width=2),
                ))

            fig.update_layout(
                title=f'{vi} — {region_label}',
                xaxis_title='Date', yaxis_title=vi,
                template='simple_white',
            )
            out = _output_path(config, stem, 'html', region_label)
            fig.write_html(str(out))
            logger.info("Saved: %s", out)


# ---------------------------------------------------------------------------
# Anomaly plot
# ---------------------------------------------------------------------------

def plot_anomaly(
    smoothed: dict, config: PhenologyConfig, vi: str, region_label: str
):
    """Annual anomaly: per-year deviation from multi-year mean VI by DOY.

    Multi-year mean smooth curve is computed from all years in the smoothed series.
    Each year's deviation from that mean is plotted as a separate colored line.
    Positive deviation = greener than average; negative = less green.

    Skipped (with an INFO log) when fewer than 2 calendar years of data are present.

    Saves to: {VI}_{region_label}_anomaly.{ext}
    """
    df = smoothed.get((vi, region_label))
    if df is None:
        return

    df = df.copy()
    df['_year'] = df['date'].dt.year
    df['_doy'] = df['date'].dt.dayofyear

    years = sorted(df['_year'].unique())
    if len(years) < 2:
        logger.info(
            "plot_anomaly: %s / %s — only %d calendar year(s) of data; "
            "anomaly plot requires >= 2 years — skipping",
            vi, region_label, len(years),
        )
        return

    mean_by_doy = df.groupby('_doy')['vi_smooth'].mean()
    stem = f"{vi}_{region_label}_anomaly"

    logger.debug(
        "plot_anomaly: %s / %s — %d years, DOY range %d–%d",
        vi, region_label, len(years), int(df['_doy'].min()), int(df['_doy'].max()),
    )

    for fmt in config.plot_formats:
        if fmt == 'png':
            fig, ax = plt.subplots(figsize=(10, 5))

            for i, year in enumerate(years):
                color = _TAB10[i % len(_TAB10)]
                year_data = df[df['_year'] == year].copy()
                year_data['_anomaly'] = year_data['vi_smooth'] - year_data['_doy'].map(mean_by_doy)
                ax.plot(year_data['_doy'], year_data['_anomaly'],
                        color=color, linewidth=1.5, label=str(year))

            ax.axhline(0, color='black', linewidth=0.8, linestyle='--')
            ax.set_xlabel('Month')
            ax.set_ylabel(f'{vi} Anomaly')
            ax.set_title(f'{vi} — {region_label} — Annual Anomaly')
            ax.set_xlim(1, 365)
            ax.set_xticks(_MONTH_DOYS)
            ax.set_xticklabels(_MONTH_LABELS)
            ax.legend(title='Year')
            ax.grid(True, alpha=0.3)
            _save_or_show(fig, _output_path(config, stem, 'png', region_label))

        elif fmt == 'html':
            import plotly.graph_objects as go
            fig = go.Figure()

            for i, year in enumerate(years):
                color = _TAB10[i % len(_TAB10)]
                year_data = df[df['_year'] == year].copy()
                year_data['_anomaly'] = year_data['vi_smooth'] - year_data['_doy'].map(mean_by_doy)
                fig.add_trace(go.Scatter(
                    x=year_data['_doy'], y=year_data['_anomaly'],
                    mode='lines', name=str(year),
                    line=dict(color=color, width=2),
                ))

            fig.add_hline(y=0, line_dash='dash', line_color='black', line_width=1)
            fig.update_layout(
                title=f'{vi} — {region_label} — Annual Anomaly',
                xaxis_title='Month', yaxis_title=f'{vi} Anomaly',
                xaxis=dict(
                    range=[1, 365],
                    tickvals=_MONTH_DOYS,
                    ticktext=_MONTH_LABELS,
                ),
                template='simple_white',
            )
            out = _output_path(config, stem, 'html', region_label)
            fig.write_html(str(out))
            logger.info("Saved: %s", out)


# ---------------------------------------------------------------------------
# Multi-VI comparison plot
# ---------------------------------------------------------------------------

def plot_multi_vi(
    raw: dict, smoothed: dict, config: PhenologyConfig, region_label: str
):
    """Side-by-side comparison of all configured VIs for the same region.

    One subplot column per VI. Rows share the same X axis (date or DOY).
    Useful for comparing how NDVI, EVI2, and NIRv track the same phenological signal.

    Only generated when config.vi_list contains more than one VI.

    Saves to: {region_label}_multi_vi.{ext}
    """
    if len(config.vi_list) <= 1:
        return

    stem = f"{region_label}_multi_vi"
    n_vi = len(config.vi_list)

    for fmt in config.plot_formats:
        if fmt == 'png':
            fig, axes = plt.subplots(n_vi, 1, figsize=(12, 4 * n_vi), sharex=True)
            if n_vi == 1:
                axes = [axes]

            for ax, vi in zip(axes, config.vi_list):
                df_raw = raw.get((vi, region_label))
                df_smooth = smoothed.get((vi, region_label)) if smoothed else None

                if df_raw is None:
                    logger.warning(
                        "plot_multi_vi: no data for %s / %s", vi, region_label
                    )
                    ax.set_ylabel(vi)
                    continue

                obs = df_raw[df_raw['vi_daily'].notna()]

                if config.plot_style in ('raw', 'combined'):
                    ax.scatter(obs['date'], obs['vi_daily'],
                               s=15, alpha=0.45, color='steelblue', label='Observations', zorder=3)

                if df_smooth is not None and config.plot_style in ('smooth', 'combined'):
                    ax.plot(df_smooth['date'], df_smooth['vi_smooth'],
                            color='#2ca02c', linewidth=1.5, label='Smooth', zorder=4)

                ax.set_ylabel(vi)
                ax.legend(loc='upper right')
                ax.grid(True, alpha=0.3)

            axes[-1].set_xlabel('Date')
            locator = mdates.AutoDateLocator()
            axes[-1].xaxis.set_major_locator(locator)
            axes[-1].xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
            fig.suptitle(f'{region_label} — Multi-VI Comparison')
            fig.tight_layout()
            _save_or_show(fig, _output_path(config, stem, 'png', region_label))

        elif fmt == 'html':
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots

            fig = make_subplots(rows=n_vi, cols=1, shared_xaxes=True,
                                subplot_titles=config.vi_list,
                                vertical_spacing=0.06)

            for row_i, vi in enumerate(config.vi_list, start=1):
                df_raw = raw.get((vi, region_label))
                df_smooth = smoothed.get((vi, region_label)) if smoothed else None

                if df_raw is None:
                    continue

                obs = df_raw[df_raw['vi_daily'].notna()]

                if config.plot_style in ('raw', 'combined'):
                    fig.add_trace(go.Scatter(
                        x=obs['date'], y=obs['vi_daily'],
                        mode='markers', name=f'{vi} obs',
                        marker=dict(size=4, opacity=0.45, color='steelblue'),
                    ), row=row_i, col=1)

                if df_smooth is not None and config.plot_style in ('smooth', 'combined'):
                    fig.add_trace(go.Scatter(
                        x=df_smooth['date'], y=df_smooth['vi_smooth'],
                        mode='lines', name=f'{vi} smooth',
                        line=dict(color='#2ca02c', width=2),
                    ), row=row_i, col=1)

                fig.update_yaxes(title_text=vi, row=row_i, col=1)

            fig.update_layout(
                title=f'{region_label} — Multi-VI Comparison',
                template='simple_white',
                height=300 * n_vi,
            )
            out = _output_path(config, stem, 'html', region_label)
            fig.write_html(str(out))
            logger.info("Saved: %s", out)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_plots(raw: dict, smoothed: dict, config: PhenologyConfig):
    """Main entry point: generate all plot types for all VI + region combinations.

    Generates:
      - annual phenology plot    (per VI, per region)
      - full time-series plot    (per VI, per region)
      - anomaly plot             (per VI, per region; skipped if < 2 years of data)
      - multi-VI comparison plot (per region; skipped if only one VI configured)

    Respects config.plot_style and config.plot_formats.
    All outputs are written to config.output_dir.

    Args:
        raw:      dict from extract_timeseries() — Layers 0+1
        smoothed: dict from smooth_timeseries() — Layer 2, or None if smooth_method='none'
        config:   PhenologyConfig
    """
    all_keys = set(raw.keys())
    regions = set(rl for (_, rl) in all_keys)

    for (vi, region_label) in sorted(all_keys):
        logger.info("Plotting %s / %s (formats: %s)", vi, region_label, config.plot_formats)

        if config.plot_timeseries:
            plot_timeseries(raw, smoothed, config, vi, region_label)

        if config.plot_annual:
            plot_annual_phenology(raw, smoothed, config, vi, region_label)

        if config.plot_anomaly and smoothed is not None:
            df_smooth = smoothed.get((vi, region_label))
            if df_smooth is not None:
                n_years = df_smooth['date'].dt.year.nunique()
                if n_years >= 2:
                    plot_anomaly(smoothed, config, vi, region_label)

    if config.plot_multi_vi and len(config.vi_list) > 1:
        for region_label in sorted(regions):
            logger.info("Plotting multi-VI comparison for region '%s'", region_label)
            plot_multi_vi(raw, smoothed, config, region_label)
