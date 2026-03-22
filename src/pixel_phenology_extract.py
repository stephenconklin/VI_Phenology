#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# pixel_phenology_extract.py
# Per-pixel phenological metric extraction from CF-1.8 datacubes.
#
# Reads one or more per-pixel datacubes produced by the netcdf_datacube pipeline
# and outputs, per (VI, region), a CF-1.8 NetCDF containing 19 phenological
# metric bands on the same x/y grid as the input, plus a summary CSV with
# spatial statistics (mean, std, p05, p50, p95, n_valid_pixels) per metric.
#
# Processing model
# ────────────────
# 1. For each input datacube (one VI × one region):
#    a. Open with xarray (lazy); apply optional date-range filter on time axis.
#    b. Warn if uncompressed array would exceed MEM_WARN_GB.
#    c. Load the full (time, y, x) array into a numpy float32 array.
#    d. Build the Whittaker D-matrix once for the full time axis (all pixels share
#       the same daily grid after time-axis standardisation).
#    e. Dispatch y-row chunks to a ThreadPoolExecutor (threads share the in-memory
#       array; scipy sparse solver releases the GIL for true parallelism).
#    f. Each thread processes its chunk pixel-by-pixel:
#         - Map observations onto the daily grid (NaN → weight 0).
#         - Solve (W + λ D^T D) z = W y for the smooth daily series.
#         - Compute 19 per-year metrics, aggregate mean/std across years.
#    g. Assemble 19-band output array; write CF-1.8 NetCDF with compression.
#    h. Write summary CSV.
#
# Metric bands (19)
# ─────────────────
#   peak_ndvi_mean, peak_ndvi_std
#   peak_doy_mean,  peak_doy_std
#   integrated_ndvi_mean, integrated_ndvi_std
#   greenup_rate_mean, greenup_rate_std
#   floor_ndvi_mean, ceiling_ndvi_mean          ← derived from curve min/max
#   season_length_mean, season_length_std
#   cv                                          ← std/mean of raw obs (whole-series)
#   interannual_peak_range, interannual_peak_std
#   n_peaks_mean
#   peak_separation_mean
#   relative_peak_amplitude_mean
#   valley_depth_mean
#
# Authors: Stephen Conklin <stephenconklin@gmail.com>
#          G. Burch Fisher, PhD — conceptual guidance and original code
#                                 adapted for per-pixel metric extraction
# License: MIT

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

import netCDF4 as nc4
import numpy as np
import pandas as pd
import xarray as xr
from scipy.signal import find_peaks as _find_peaks
from scipy.sparse import diags as sp_diags
from scipy.sparse.linalg import spsolve

from io_utils import setup_log_file

logger = logging.getLogger(__name__)

# Warn when the uncompressed input array would exceed this threshold.
_MEM_WARN_GB = 8.0

# Number of y-rows per thread chunk.
_Y_CHUNK_ROWS = 50

# Standard NetCDF4 fill value for float32. Using np.nan as _FillValue causes
# NaN != NaN comparison failures in tools like Panoply, making all pixels appear
# missing. This value is the CF/NetCDF4 convention for float32 missing data.
_FILL_F4 = np.float32(9.96920996838687e+36)

# Ordered list of the 18 output metric names.
METRIC_NAMES = [
    "peak_ndvi_mean",
    "peak_ndvi_std",
    "peak_doy_mean",
    "peak_doy_std",
    "integrated_ndvi_mean",
    "integrated_ndvi_std",
    "greenup_rate_mean",
    "greenup_rate_std",
    "floor_ndvi_mean",
    "ceiling_ndvi_mean",
    "season_length_mean",
    "season_length_std",
    "cv",
    "interannual_peak_range",
    "interannual_peak_std",
    "n_peaks_mean",
    "peak_separation_mean",
    "relative_peak_amplitude_mean",
    "valley_depth_mean",
]

_MIN_AMPLITUDE = 1e-6

# ---------------------------------------------------------------------------
# Metric metadata — human-readable labels, units, colormaps, group membership
# ---------------------------------------------------------------------------
# Each entry: (human_title, unit_label, mpl_colormap, group_name, plotly_colorscale)
METRIC_META = {
    # ── Peak ──────────────────────────────────────────────────────────────
    "peak_ndvi_mean":              ("Mean Annual Peak NDVI",           "NDVI",           "YlGn",            "Peak",         "YlGn"),
    "peak_ndvi_std":               ("Peak NDVI — Interannual Std",     "NDVI",           "YlOrRd",          "Peak",         "YlOrRd"),
    "peak_doy_mean":               ("Mean Peak Day of Year",           "DOY",            "twilight_shifted", "Peak",         "Phase"),
    "peak_doy_std":                ("Peak DOY — Interannual Std",      "Days",           "YlOrRd",          "Peak",         "YlOrRd"),
    # ── Productivity ──────────────────────────────────────────────────────
    "integrated_ndvi_mean":        ("Mean Integrated NDVI",            "NDVI·days yr⁻¹", "YlGn",            "Productivity", "YlGn"),
    "integrated_ndvi_std":         ("Integrated NDVI — Interannual Std","NDVI·days",     "YlOrRd",          "Productivity", "YlOrRd"),
    "greenup_rate_mean":           ("Mean Green-Up Rate",              "NDVI day⁻¹",    "PuBuGn",          "Productivity", "PuBuGn"),
    "greenup_rate_std":            ("Green-Up Rate — Interannual Std", "NDVI day⁻¹",    "YlOrRd",          "Productivity", "YlOrRd"),
    # ── Seasonality ───────────────────────────────────────────────────────
    "floor_ndvi_mean":             ("Mean Dry-Season Floor NDVI",      "NDVI",           "YlOrBr",          "Seasonality",  "YlOrBr"),
    "ceiling_ndvi_mean":           ("Mean Wet-Season Ceiling NDVI",    "NDVI",           "YlGn",            "Seasonality",  "YlGn"),
    "season_length_mean":          ("Mean Season Length",              "Days",           "RdYlGn",          "Seasonality",  "RdYlGn"),
    "season_length_std":           ("Season Length — Interannual Std", "Days",           "YlOrRd",          "Seasonality",  "YlOrRd"),
    # ── Variability ───────────────────────────────────────────────────────
    "cv":                          ("Coefficient of Variation",        "CV",             "OrRd",            "Variability",  "Oranges"),
    "interannual_peak_range":      ("Interannual Peak NDVI Range",     "NDVI",           "PuRd",            "Variability",  "PuRd"),
    "interannual_peak_std":        ("Interannual Peak NDVI Std",       "NDVI",           "YlOrRd",          "Variability",  "YlOrRd"),
    # ── Bimodality ────────────────────────────────────────────────────────
    "n_peaks_mean":                ("Mean Number of Peaks per Year",   "Peaks yr⁻¹",    "Blues",           "Bimodality",   "Blues"),
    "peak_separation_mean":        ("Mean Peak Separation",            "Days",           "PuBu",            "Bimodality",   "PuBu"),
    "relative_peak_amplitude_mean":("Mean Relative Peak Amplitude",    "Ratio",          "RdPu",            "Bimodality",   "RdPu"),
    "valley_depth_mean":           ("Mean Valley Depth",               "Normalised",     "BuPu",            "Bimodality",   "BuPu"),
}

# Group display order and sidebar colors
_GROUP_ORDER  = ["Peak", "Productivity", "Seasonality", "Variability", "Bimodality"]
_GROUP_COLORS = {
    "Peak":         "#d4edda",
    "Productivity": "#cce5ff",
    "Seasonality":  "#fff3cd",
    "Variability":  "#f8d7da",
    "Bimodality":   "#e2d9f3",
}


# ---------------------------------------------------------------------------
# Overview figure
# ---------------------------------------------------------------------------

def _write_overview_figure(
    out_nc_path: Path,
    summary_csv_path: Path,
    vi_name: str,
    region_label: str,
    config: dict,
    datacube_path: Path,
) -> Path:
    """Render a print-quality 4×5 overview sheet of all 19 metric bands.

    Reads the already-written output NetCDF and summary CSV to avoid keeping
    the large out_array in memory. Returns the path to the saved PNG.
    """
    FILL = _FILL_F4 * 0.9  # threshold for masking fill pixels

    # ── Load data ─────────────────────────────────────────────────────────
    bands = {}
    with nc4.Dataset(str(out_nc_path), "r") as ds:
        y = np.array(ds.variables["y"][:])
        x = np.array(ds.variables["x"][:])
        for name in METRIC_NAMES:
            raw = np.array(ds.variables[name][:], dtype=np.float64)
            raw[raw >= FILL] = np.nan
            bands[name] = raw

    stats = pd.read_csv(summary_csv_path).set_index("metric")
    extent = [x.min(), x.max(), y.min(), y.max()]

    # ── Figure geometry ───────────────────────────────────────────────────
    N_COLS, N_ROWS = 4, 5          # 20 slots: 19 metrics + 1 metadata panel
    FIG_W, FIG_H  = 22, 28        # inches — tabloid/A1 portrait
    fig = plt.figure(figsize=(FIG_W, FIG_H), dpi=300)
    fig.patch.set_facecolor("#f7f7f7")

    # Outer gridspec: left sidebar (group labels) + main panel area.
    # left=0.07 gives the sidebar and y-axis tick labels enough room so they
    # don't bleed into each other.
    outer = gridspec.GridSpec(
        1, 2, figure=fig,
        width_ratios=[0.030, 0.970],
        left=0.07, right=0.97,
        top=0.93, bottom=0.03,
        wspace=0.02,
    )
    # Main 4×5 grid
    gs = gridspec.GridSpecFromSubplotSpec(
        N_ROWS, N_COLS,
        subplot_spec=outer[1],
        hspace=0.55, wspace=0.30,
    )

    # ── Page header ───────────────────────────────────────────────────────
    header_txt = (
        f"{vi_name} Pixel Phenology — Region: {region_label}"
    )
    sub_txt = (
        f"Whittaker λ={config['smooth_lambda']:.0f}  |  "
        f"min obs={config['min_valid_obs']}  |  "
        f"season threshold={config['season_threshold']:.2f}  |  "
        f"peak prominence={config['peak_prominence']:.2f}  |  "
        f"Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
    )
    fig.text(0.50, 0.965, header_txt, ha="center", va="bottom",
             fontsize=20, fontweight="bold", color="#1a1a1a")
    fig.text(0.50, 0.955, sub_txt, ha="center", va="bottom",
             fontsize=7.5, color="#555555")

    # ── Group sidebar ─────────────────────────────────────────────────────
    # Map each metric to its row slot to draw sidebar rectangles
    sidebar_ax = fig.add_subplot(outer[0])
    sidebar_ax.set_xlim(0, 1)
    sidebar_ax.set_ylim(0, N_ROWS)
    sidebar_ax.axis("off")

    # Count rows per group to size the sidebar bands
    group_rows = {}
    row = 0
    for name in METRIC_NAMES:
        g = METRIC_META[name][3]
        group_rows.setdefault(g, []).append(row // N_COLS)
        row += 1

    # Draw one rectangle per group spanning its rows
    for g in _GROUP_ORDER:
        if g not in group_rows:
            continue
        rows_in_group = sorted(set(group_rows[g]))
        y_top    = N_ROWS - rows_in_group[0]
        y_bottom = N_ROWS - rows_in_group[-1] - 1
        rect = mpatches.FancyBboxPatch(
            (0.05, y_bottom + 0.05), 0.90, (y_top - y_bottom) - 0.10,
            boxstyle="round,pad=0.02",
            facecolor=_GROUP_COLORS[g], edgecolor="#aaaaaa", linewidth=0.5,
        )
        sidebar_ax.add_patch(rect)
        sidebar_ax.text(
            0.50, (y_top + y_bottom) / 2,
            g, ha="center", va="center",
            fontsize=7, fontweight="bold", color="#333333",
            rotation=90,
        )

    # ── Metric panels ─────────────────────────────────────────────────────
    km_fmt = FuncFormatter(lambda v, _: f"{v/1000:.0f}k")

    for idx, name in enumerate(METRIC_NAMES):
        row_i, col_i = divmod(idx, N_COLS)
        ax = fig.add_subplot(gs[row_i, col_i])
        ax.set_facecolor("#dddddd")    # shows as grey for fully-NaN edges

        title, unit, cmap, group, _plotly_cs = METRIC_META[name]
        data = bands[name]

        valid = data[~np.isnan(data)]
        if len(valid) == 0:
            ax.set_title(title, fontsize=6.5, fontweight="bold", pad=3)
            ax.text(0.5, 0.5, "No valid data", transform=ax.transAxes,
                    ha="center", va="center", fontsize=7, color="#888888")
            ax.axis("off")
            continue

        vmin = np.nanpercentile(data, 2)
        vmax = np.nanpercentile(data, 98)
        if vmin == vmax:
            vmax = vmin + 1e-6

        im = ax.imshow(
            data, origin="upper", extent=extent,
            cmap=cmap, vmin=vmin, vmax=vmax,
            interpolation="nearest", aspect="auto",
        )

        # Colorbar
        cb = fig.colorbar(im, ax=ax, fraction=0.038, pad=0.02, shrink=0.88)
        cb.set_label(unit, fontsize=6, labelpad=2)
        cb.ax.tick_params(labelsize=5.5)
        cb.ax.yaxis.set_major_locator(plt.MaxNLocator(nbins=5))

        # Title
        ax.set_title(title, fontsize=6.8, fontweight="bold", pad=3,
                     color="#1a1a1a")

        # Subtitle: spatial mean ± std from summary CSV
        if name in stats.index:
            row_s = stats.loc[name]
            sub = (f"μ={row_s['mean']:.3g}  σ={row_s['std']:.3g}  "
                   f"n={int(row_s['n_valid_pixels']):,}")
        else:
            sub = ""
        ax.set_xlabel(sub, fontsize=5.5, color="#444444", labelpad=2)

        # Axis formatting
        ax.xaxis.set_major_formatter(km_fmt)
        ax.yaxis.set_major_formatter(km_fmt)
        ax.tick_params(labelsize=5, length=2, pad=1)
        # Only label y-axis on left-column panels to avoid crowding the sidebar
        if col_i == 0:
            ax.set_ylabel("Northing (m)", fontsize=5, labelpad=1)
        else:
            ax.set_ylabel("")

        # Group background tint on panel face
        ax.set_facecolor(_GROUP_COLORS[group])

    # ── Metadata panel (slot 20) ──────────────────────────────────────────
    meta_ax = fig.add_subplot(gs[4, 3])
    meta_ax.axis("off")
    meta_ax.set_facecolor("#eeeeee")
    meta_lines = [
        ("Source datacube", datacube_path.name),
        ("Region", region_label),
        ("VI", vi_name),
        ("Whittaker λ", f"{config['smooth_lambda']:.0f}"),
        ("Min valid obs", str(config["min_valid_obs"])),
        ("Min obs / year", str(config["min_valid_obs_per_year"])),
        ("Season threshold", f"{config['season_threshold']:.2f}"),
        ("Peak prominence", f"{config['peak_prominence']:.2f}"),
        ("Peak min distance", f"{config['peak_min_distance_days']} days"),
        ("Total metrics", "19"),
        ("Generated (UTC)", datetime.utcnow().strftime("%Y-%m-%d %H:%M")),
    ]
    y_pos = 0.96
    meta_ax.text(0.5, y_pos + 0.03, "Processing Parameters",
                 transform=meta_ax.transAxes,
                 ha="center", va="top", fontsize=7.5, fontweight="bold",
                 color="#1a1a1a")
    for label, value in meta_lines:
        y_pos -= 0.075
        meta_ax.text(0.04, y_pos, f"{label}:", transform=meta_ax.transAxes,
                     ha="left", va="top", fontsize=6, color="#555555",
                     fontweight="bold")
        meta_ax.text(0.96, y_pos, value, transform=meta_ax.transAxes,
                     ha="right", va="top", fontsize=6, color="#1a1a1a")
    # Thin border
    for spine in meta_ax.spines.values():
        spine.set_visible(True)
        spine.set_edgecolor("#bbbbbb")
        spine.set_linewidth(0.5)

    # ── Save ──────────────────────────────────────────────────────────────
    out_fig_path = out_nc_path.parent / out_nc_path.name.replace(
        "_pixel_metrics.nc", "_pixel_metrics_overview.png"
    )
    fig.savefig(str(out_fig_path), dpi=300, bbox_inches="tight",
                facecolor=fig.get_facecolor())
    plt.close(fig)
    return out_fig_path


# ---------------------------------------------------------------------------
# Interactive HTML overview (Plotly)
# ---------------------------------------------------------------------------

# Target max pixels per panel for the HTML version.  Downsampling keeps the
# HTML file at a practical size (~10–20 MB) while preserving spatial pattern.
_HTML_MAX_PX = 40_000   # ~200×200 equivalent


def _write_overview_html(
    out_nc_path: Path,
    summary_csv_path: Path,
    vi_name: str,
    region_label: str,
    config: dict,
    datacube_path: Path,
) -> Path:
    """Render an interactive Plotly HTML overview of all 19 metric bands.

    Each panel is a heatmap; hovering shows easting, northing, and the
    metric value with its unit.  Data are spatially downsampled to keep
    the HTML file at a manageable size.
    """
    FILL = _FILL_F4 * 0.9

    # ── Load and downsample ───────────────────────────────────────────────
    bands = {}
    with nc4.Dataset(str(out_nc_path), "r") as ds:
        y_full = np.array(ds.variables["y"][:])
        x_full = np.array(ds.variables["x"][:])
        n_y, n_x = len(y_full), len(x_full)

        # Compute a single downsample factor that keeps total pixels ≤ _HTML_MAX_PX
        factor = max(1, int(np.ceil(np.sqrt((n_y * n_x) / _HTML_MAX_PX))))
        y_ds = y_full[::factor]
        x_ds = x_full[::factor]

        for name in METRIC_NAMES:
            raw = np.array(ds.variables[name][:], dtype=np.float32)
            raw[raw >= FILL] = np.nan
            bands[name] = raw[::factor, ::factor]

    stats = pd.read_csv(summary_csv_path).set_index("metric")

    # ── Build subplot grid ────────────────────────────────────────────────
    N_COLS, N_ROWS = 4, 5
    titles = []
    for name in METRIC_NAMES:
        t, unit, *_ = METRIC_META[name]
        row_s = stats.loc[name] if name in stats.index else None
        sub = (f"μ={row_s['mean']:.3g}  σ={row_s['std']:.3g}"
               if row_s is not None else "")
        titles.append(f"<b>{t}</b><br><sup>{sub}</sup>")
    # 20th slot = metadata
    titles.append("<b>Processing Parameters</b>")

    fig = make_subplots(
        rows=N_ROWS, cols=N_COLS,
        subplot_titles=titles,
        horizontal_spacing=0.06,
        vertical_spacing=0.08,
    )

    # ── Add heatmap traces ────────────────────────────────────────────────
    for idx, name in enumerate(METRIC_NAMES):
        row_i = idx // N_COLS + 1
        col_i = idx % N_COLS + 1
        title, unit, _mpl, group, colorscale = METRIC_META[name]
        data = bands[name]

        valid = data[~np.isnan(data)]
        zmin = float(np.nanpercentile(valid, 2))  if len(valid) else 0
        zmax = float(np.nanpercentile(valid, 98)) if len(valid) else 1
        if zmin == zmax:
            zmax = zmin + 1e-6

        heatmap = go.Heatmap(
            z=data.tolist(),
            x=x_ds.tolist(),
            y=y_ds.tolist(),
            colorscale=colorscale,
            zmin=zmin,
            zmax=zmax,
            colorbar=dict(
                title=dict(text=unit, side="right", font=dict(size=8)),
                thickness=10,
                len=0.18,
                x=col_i / N_COLS - 0.01,
                y=1 - (row_i - 0.5) / N_ROWS,
                tickfont=dict(size=8),
            ),
            hovertemplate=(
                f"<b>{title}</b><br>"
                "Easting: %{x:.0f} m<br>"
                "Northing: %{y:.0f} m<br>"
                f"Value: %{{z:.4g}} {unit}"
                "<extra></extra>"
            ),
            name=title,
        )
        fig.add_trace(heatmap, row=row_i, col=col_i)

        fig.update_xaxes(title_text="Easting (m)", title_font_size=7,
                         tickfont_size=6, row=row_i, col=col_i)
        fig.update_yaxes(title_text="Northing (m)", title_font_size=7,
                         tickfont_size=6, row=row_i, col=col_i)

    # ── Metadata annotation (20th slot) ──────────────────────────────────
    meta_lines = [
        f"<b>Region:</b> {region_label}",
        f"<b>VI:</b> {vi_name}",
        f"<b>Source:</b> {datacube_path.name}",
        f"<b>Whittaker λ:</b> {config['smooth_lambda']:.0f}",
        f"<b>Min valid obs:</b> {config['min_valid_obs']}",
        f"<b>Min obs / year:</b> {config['min_valid_obs_per_year']}",
        f"<b>Season threshold:</b> {config['season_threshold']:.2f}",
        f"<b>Peak prominence:</b> {config['peak_prominence']:.2f}",
        f"<b>Peak min dist:</b> {config['peak_min_distance_days']} days",
        f"<b>Generated (UTC):</b> {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}",
    ]
    fig.add_annotation(
        text="<br>".join(meta_lines),
        xref="paper", yref="paper",
        x=0.98, y=0.04,
        xanchor="right", yanchor="bottom",
        showarrow=False,
        font=dict(size=9, color="#333333"),
        align="left",
        bgcolor="#f0f0f0",
        bordercolor="#aaaaaa",
        borderwidth=1,
        borderpad=8,
    )

    # ── Layout ────────────────────────────────────────────────────────────
    fig.update_layout(
        title=dict(
            text=(
                f"<b>{vi_name} Pixel Phenology — Region: {region_label}</b><br>"
                f"<sup>Whittaker λ={config['smooth_lambda']:.0f}  |  "
                f"min obs={config['min_valid_obs']}  |  "
                f"Hover any panel for pixel values</sup>"
            ),
            x=0.5, xanchor="center",
            font=dict(size=16),
        ),
        height=1800,
        width=1600,
        paper_bgcolor="#f7f7f7",
        plot_bgcolor="#eeeeee",
        showlegend=False,
        font=dict(family="Arial, sans-serif"),
    )

    out_html_path = out_nc_path.parent / out_nc_path.name.replace(
        "_pixel_metrics.nc", "_pixel_metrics_overview.html"
    )
    fig.write_html(str(out_html_path), include_plotlyjs="cdn")
    return out_html_path


# ---------------------------------------------------------------------------
# Whittaker smoother
# ---------------------------------------------------------------------------

def _build_whittaker_system(n: int, lam: float):
    """Pre-build the λ D^T D penalty term for a daily grid of length n.

    Returns the sparse (n × n) matrix lam_DTD so each pixel only needs to
    add the diagonal weight matrix W and solve (W + lam_DTD) z = W y.
    """
    e = np.ones(n)
    D = sp_diags(
        [e[:-2], -2 * e[:-1], e],
        offsets=[0, 1, 2],
        shape=(n - 2, n),
        format='csc',
    )
    return lam * D.T @ D


def _whittaker_smooth_pixel(
    daily_y: np.ndarray,
    daily_w: np.ndarray,
    lam_DTD,
) -> np.ndarray:
    """Solve the Whittaker system for one pixel.

    Args:
        daily_y:  float64 array of length n (0 where no observation).
        daily_w:  float64 weight array of length n (1 = observed, 0 = gap).
        lam_DTD:  precomputed λ D^T D sparse matrix.

    Returns:
        Smoothed float64 array of length n, or daily_y unchanged on failure.
    """
    W = sp_diags(daily_w, format='csc')
    A = W + lam_DTD
    b = daily_w * daily_y
    try:
        return spsolve(A, b)
    except Exception:
        return daily_y.copy()


# ---------------------------------------------------------------------------
# Per-pixel metric extraction
# ---------------------------------------------------------------------------

def _extract_pixel_metrics(
    pixel_ts: np.ndarray,
    times: pd.DatetimeIndex,
    lam_DTD,
    config: dict,
) -> dict:
    """Compute all 18 phenological metrics for one pixel time series.

    Args:
        pixel_ts: float32/float64 array of shape (n_time,); NaN = masked.
        times:    DatetimeIndex aligned with pixel_ts.
        lam_DTD:  precomputed Whittaker penalty matrix (shared across pixels).
        config:   dict with keys: min_valid_obs, min_valid_obs_per_year, vi_min, vi_max,
                  peak_prominence, peak_min_distance_days, season_threshold.

    Returns:
        dict of metric_name → float scalar (np.nan if not computable).
    """
    nan_result = {k: np.nan for k in METRIC_NAMES}

    # Validity check.
    vi_min = config["vi_min"]
    vi_max = config["vi_max"]
    valid_mask = (
        ~np.isnan(pixel_ts)
        & (pixel_ts >= vi_min)
        & (pixel_ts <= vi_max)
    )
    if valid_mask.sum() < config["min_valid_obs"]:
        return nan_result

    # Coefficient of variation from raw observations (whole-series).
    raw_vals = pixel_ts[valid_mask].astype(np.float64)
    mean_raw = float(np.mean(raw_vals))
    cv = float(np.std(raw_vals) / mean_raw) if mean_raw > 0 else np.nan

    # Map observations onto the daily grid spanning the full time axis.
    start_date = times[0]
    end_date = times[-1]
    n_days = (end_date - start_date).days + 1
    all_dates = pd.date_range(start=start_date, end=end_date, freq='D')

    daily_y = np.zeros(n_days, dtype=np.float64)
    daily_w = np.zeros(n_days, dtype=np.float64)

    for t_idx, ts in enumerate(times):
        if valid_mask[t_idx]:
            d = (ts - start_date).days
            if daily_w[d] > 0:
                # Same-day duplicate — take mean.
                daily_y[d] = (daily_y[d] + float(pixel_ts[t_idx])) / 2.0
            else:
                daily_y[d] = float(pixel_ts[t_idx])
                daily_w[d] = 1.0

    # Whittaker smooth.
    if n_days < 3 or lam_DTD is None:
        # Fallback: linear interpolation between observations.
        smoothed = daily_y.copy()
    else:
        smoothed = _whittaker_smooth_pixel(daily_y, daily_w, lam_DTD)
        smoothed = np.clip(smoothed, vi_min, vi_max)

    # Build a date-indexed DataFrame for the smoothed series.
    smooth_df = pd.DataFrame({
        "date": all_dates,
        "ndvi": smoothed.astype(np.float32),
    }).set_index("date")

    years = sorted(smooth_df.index.year.unique())

    # Per-year accumulators.
    annual = {
        "peak_ndvi":   [],
        "peak_doy":    [],
        "integrated":  [],
        "greenup":     [],
        "floor":       [],
        "ceiling":     [],
        "season_len":  [],
        "n_peaks":     [],
        "peak_sep":    [],
        "rel_amp":     [],
        "valley":      [],
    }

    peak_prominence = config["peak_prominence"]
    peak_min_dist = config["peak_min_distance_days"]
    season_thr = config["season_threshold"]
    min_obs_per_year = config["min_valid_obs_per_year"]

    for yr in years:
        yr_s = smooth_df[smooth_df.index.year == yr]["ndvi"]
        if len(yr_s) < 30:
            continue

        # Count valid observations in this annual window; skip if too sparse.
        yr_obs = int(daily_w[all_dates.year == yr].sum())
        if yr_obs < min_obs_per_year:
            continue

        y = yr_s.values.astype(np.float64)
        doys = yr_s.index.dayofyear.values

        # Peak.
        peak_idx = int(np.argmax(y))
        annual["peak_ndvi"].append(float(y[peak_idx]))
        annual["peak_doy"].append(int(doys[peak_idx]))

        # Integrated NDVI (trapezoidal, using integer day indices as x-axis).
        annual["integrated"].append(float(np.trapezoid(y)))

        # Floor and ceiling from the curve (no DOY windows).
        floor_val = float(np.nanmin(y))
        ceil_val = float(np.nanmax(y))
        annual["floor"].append(floor_val)
        annual["ceiling"].append(ceil_val)

        # Green-up rate: slope from curve minimum to curve maximum.
        floor_idx = int(np.argmin(y))
        if floor_idx < peak_idx:
            delta_ndvi = float(y[peak_idx] - y[floor_idx])
            delta_days = int(doys[peak_idx] - doys[floor_idx])
            rate = delta_ndvi / delta_days if delta_days > 0 else np.nan
            annual["greenup"].append(rate)

        # Season length: days above floor + season_thr * amplitude.
        amplitude = ceil_val - floor_val
        if amplitude >= _MIN_AMPLITUDE:
            threshold = floor_val + season_thr * amplitude
            above_dates = yr_s.index[y >= threshold]
            if len(above_dates) >= 2:
                annual["season_len"].append(
                    float((above_dates[-1] - above_dates[0]).days)
                )

        # Bimodality.
        peaks, _ = _find_peaks(y, prominence=peak_prominence, distance=peak_min_dist)
        n_p = int(len(peaks))
        annual["n_peaks"].append(n_p)

        if n_p >= 2:
            sorted_peaks = peaks[np.argsort(y[peaks])[::-1]]
            p1, p2 = sorted_peaks[0], sorted_peaks[1]
            sep = float(abs(doys[p1] - doys[p2]))
            annual["peak_sep"].append(sep)
            h1, h2 = float(y[p1]), float(y[p2])
            if max(h1, h2) > 0:
                annual["rel_amp"].append(float(min(h1, h2) / max(h1, h2)))
            lo, hi = min(p1, p2), max(p1, p2)
            valley = float(np.nanmin(y[lo : hi + 1]))
            mean_pk = (h1 + h2) / 2.0
            if mean_pk > 0:
                annual["valley"].append(float((mean_pk - valley) / mean_pk))
        else:
            annual["peak_sep"].append(np.nan)
            annual["rel_amp"].append(np.nan)
            annual["valley"].append(np.nan)

    def _safe_mean(lst):
        a = [v for v in lst if not np.isnan(v)]
        return float(np.mean(a)) if a else np.nan

    def _safe_std(lst):
        a = [v for v in lst if not np.isnan(v)]
        return float(np.std(a)) if a else np.nan

    peak_list = annual["peak_ndvi"]
    interannual_range = (
        float(np.nanmax(peak_list) - np.nanmin(peak_list)) if peak_list else np.nan
    )

    return {
        "peak_ndvi_mean":               _safe_mean(annual["peak_ndvi"]),
        "peak_ndvi_std":                _safe_std(annual["peak_ndvi"]),
        "peak_doy_mean":                _safe_mean(annual["peak_doy"]),
        "peak_doy_std":                 _safe_std(annual["peak_doy"]),
        "integrated_ndvi_mean":         _safe_mean(annual["integrated"]),
        "integrated_ndvi_std":          _safe_std(annual["integrated"]),
        "greenup_rate_mean":            _safe_mean(annual["greenup"]),
        "greenup_rate_std":             _safe_std(annual["greenup"]),
        "floor_ndvi_mean":              _safe_mean(annual["floor"]),
        "ceiling_ndvi_mean":            _safe_mean(annual["ceiling"]),
        "season_length_mean":           _safe_mean(annual["season_len"]),
        "season_length_std":            _safe_std(annual["season_len"]),
        "cv":                           cv,
        "interannual_peak_range":       interannual_range,
        "interannual_peak_std":         _safe_std(annual["peak_ndvi"]),
        "n_peaks_mean":                 _safe_mean(annual["n_peaks"]),
        "peak_separation_mean":         _safe_mean(annual["peak_sep"]),
        "relative_peak_amplitude_mean": _safe_mean(annual["rel_amp"]),
        "valley_depth_mean":            _safe_mean(annual["valley"]),
    }


# ---------------------------------------------------------------------------
# Thread worker
# ---------------------------------------------------------------------------

def _process_y_chunk(
    ndvi_chunk: np.ndarray,
    times: pd.DatetimeIndex,
    lam_DTD,
    config: dict,
) -> np.ndarray:
    """Process all pixels in a y-row chunk.

    Args:
        ndvi_chunk: float32 array of shape (n_time, n_y_chunk, n_x).
        times:      DatetimeIndex of length n_time.
        lam_DTD:    precomputed Whittaker penalty matrix.
        config:     per-pixel config dict.

    Returns:
        float32 array of shape (n_metrics, n_y_chunk, n_x).
    """
    n_metrics = len(METRIC_NAMES)
    n_time, n_y, n_x = ndvi_chunk.shape
    out = np.full((n_metrics, n_y, n_x), np.nan, dtype=np.float32)

    for iy in range(n_y):
        for ix in range(n_x):
            pixel_ts = ndvi_chunk[:, iy, ix].astype(np.float64)
            if np.all(np.isnan(pixel_ts)):
                continue
            metrics = _extract_pixel_metrics(pixel_ts, times, lam_DTD, config)
            for im, name in enumerate(METRIC_NAMES):
                out[im, iy, ix] = metrics.get(name, np.nan)

    return out


# ---------------------------------------------------------------------------
# Per-datacube pipeline
# ---------------------------------------------------------------------------

def process_datacube(
    datacube_path: Path,
    output_dir: Path,
    config: dict,
    n_workers: int,
    start_date: str | None,
    end_date: str | None,
    overview_figure: bool = True,
    overview_html: bool = True,
) -> None:
    """Extract 19 per-pixel metrics from one datacube and write outputs.

    Args:
        datacube_path: Path to a *_datacube.nc file.
        output_dir:    Directory to write outputs (created if needed).
        config:        Processing config dict (vi_min, vi_max, min_valid_obs,
                       smooth_lambda, peak_prominence, peak_min_distance_days,
                       season_threshold).
        n_workers:     Number of threads.
        start_date:    Optional YYYY-MM-DD lower bound (inclusive).
        end_date:      Optional YYYY-MM-DD upper bound (inclusive).
    """
    # ── Parse VI and region label from filename ───────────────────────────────
    stem = datacube_path.stem  # e.g. "NDVI_MyRegion_datacube" after stripping .nc
    if not stem.endswith("_datacube"):
        logger.warning(
            "Unexpected datacube filename '%s' — expected *_datacube.nc; "
            "proceeding anyway.", datacube_path.name
        )
    # Strip trailing _datacube if present.
    base = stem[: -len("_datacube")] if stem.endswith("_datacube") else stem

    # VI is the first underscore-separated token; region_label is the rest.
    parts = base.split("_", 1)
    vi_name = parts[0].upper()
    region_label = parts[1] if len(parts) > 1 else "unknown_region"

    logger.info("Processing datacube: VI=%s  region=%s  path=%s",
                vi_name, region_label, datacube_path)

    # ── Open and date-filter ──────────────────────────────────────────────────
    ds = xr.open_dataset(datacube_path, chunks={})

    # Detect VI variable (first non-coordinate, non-spatial_ref data variable).
    vi_var = None
    for vname in ds.data_vars:
        if vname not in ("spatial_ref",):
            vi_var = vname
            break
    if vi_var is None:
        logger.error("No VI variable found in %s — skipping.", datacube_path)
        return

    da = ds[vi_var]

    # Apply date range filter before loading into memory.
    if start_date or end_date:
        time_sel = {}
        if start_date:
            time_sel["time"] = slice(start_date, None)
        if end_date:
            existing = time_sel.get("time", slice(None, None))
            time_sel["time"] = slice(existing.start, end_date)
        da = da.sel(**time_sel)

    times_raw = da.time.values
    times = pd.DatetimeIndex(pd.to_datetime(times_raw))

    n_time = len(times)
    n_y = da.sizes.get("y", da.sizes.get("lat", 0))
    n_x = da.sizes.get("x", da.sizes.get("lon", 0))
    y_coords = da.coords["y"].values if "y" in da.coords else da.coords["lat"].values
    x_coords = da.coords["x"].values if "x" in da.coords else da.coords["lon"].values

    # Clamp valid range from config.
    vi_key = vi_name.lower()
    vi_min = config.get(f"vi_min_{vi_key}", config.get("vi_min", -1.0))
    vi_max = config.get(f"vi_max_{vi_key}", config.get("vi_max", 2.0))

    # Memory check.
    n_bytes = n_time * n_y * n_x * 4
    if n_bytes > _MEM_WARN_GB * 1e9:
        logger.warning(
            "Datacube is large (%.1f GB uncompressed, %d×%d spatial, %d time steps). "
            "Consider using --start-date/--end-date to reduce temporal extent.",
            n_bytes / 1e9, n_y, n_x, n_time,
        )

    logger.info(
        "Loading array: time=%d  y=%d  x=%d  (%.2f GB uncompressed)",
        n_time, n_y, n_x, n_bytes / 1e9,
    )
    ndvi_np = da.values.astype(np.float32)  # shape: (time, y, x)

    # Build Whittaker penalty matrix once for the daily grid (not n_time).
    # n_days = calendar days from first to last acquisition; always >= n_time since
    # HLS does not observe every day. W in _whittaker_smooth_pixel has size n_days,
    # so lam_DTD must match that dimension.
    n_days = (times[-1] - times[0]).days + 1
    lam = config["smooth_lambda"]
    if n_days >= 3:
        try:
            lam_DTD = _build_whittaker_system(n_days, lam)
            logger.debug(
                "Built Whittaker D^T D matrix: n_days=%d, n_time=%d, λ=%.1f",
                n_days, n_time, lam,
            )
        except Exception as exc:
            logger.warning("Could not build Whittaker matrix (%s); using linear fill.", exc)
            lam_DTD = None
    else:
        lam_DTD = None

    per_pixel_cfg = {
        "vi_min":                   vi_min,
        "vi_max":                   vi_max,
        "min_valid_obs":            config["min_valid_obs"],
        "min_valid_obs_per_year":   config["min_valid_obs_per_year"],
        "peak_prominence":          config["peak_prominence"],
        "peak_min_distance_days":   config["peak_min_distance_days"],
        "season_threshold":         config["season_threshold"],
    }

    # ── Parallel pixel processing ─────────────────────────────────────────────
    n_metrics = len(METRIC_NAMES)
    out_array = np.full((n_metrics, n_y, n_x), np.nan, dtype=np.float32)

    y_chunks = list(range(0, n_y, _Y_CHUNK_ROWS))
    n_chunks = len(y_chunks)
    logger.info("Dispatching %d y-row chunks to %d threads ...", n_chunks, n_workers)

    futures = {}
    with logging_redirect_tqdm():
        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            for chunk_start in y_chunks:
                chunk_end = min(chunk_start + _Y_CHUNK_ROWS, n_y)
                ndvi_chunk = ndvi_np[:, chunk_start:chunk_end, :]
                future = executor.submit(
                    _process_y_chunk, ndvi_chunk, times, lam_DTD, per_pixel_cfg
                )
                futures[future] = (chunk_start, chunk_end)

            with tqdm(total=n_chunks, desc=f"{vi_name}/{region_label}",
                      unit="chunk", dynamic_ncols=True) as pbar:
                for future in as_completed(futures):
                    chunk_start, chunk_end = futures[future]
                    result = future.result()
                    out_array[:, chunk_start:chunk_end, :] = result
                    pbar.update(1)

    # ── Write output NetCDF ───────────────────────────────────────────────────
    region_out_dir = output_dir / region_label
    region_out_dir.mkdir(parents=True, exist_ok=True)
    out_nc_path = region_out_dir / f"{vi_name}_{region_label}_pixel_metrics.nc"

    logger.info("Writing pixel metrics NetCDF: %s", out_nc_path)
    with nc4.Dataset(str(out_nc_path), "w", format="NETCDF4") as ncout:
        # Dimensions.
        y_dim = "y" if "y" in da.coords else "lat"
        x_dim = "x" if "x" in da.coords else "lon"
        ncout.createDimension(y_dim, n_y)
        ncout.createDimension(x_dim, n_x)

        # Coordinate variables.
        y_var = ncout.createVariable(y_dim, "f8", (y_dim,))
        y_var[:] = y_coords
        x_var = ncout.createVariable(x_dim, "f8", (x_dim,))
        x_var[:] = x_coords

        # Copy CRS if present.
        if "spatial_ref" in ds.data_vars:
            sr = ncout.createVariable("spatial_ref", "i4")
            sr.setncatts(
                {k: ds["spatial_ref"].attrs[k] for k in ds["spatial_ref"].attrs}
            )

        # Metric bands.
        for im, name in enumerate(METRIC_NAMES):
            v = ncout.createVariable(
                name, "f4", (y_dim, x_dim),
                zlib=True, complevel=4, fill_value=_FILL_F4,
            )
            band = out_array[im].copy()
            band[np.isnan(band)] = _FILL_F4
            v[:] = band
            v.long_name = name
            v.grid_mapping = "spatial_ref"

        # Global attributes.
        ncout.Conventions = "CF-1.8"
        ncout.history = (
            f"Created {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')} "
            "by pixel_phenology_extract.py"
        )
        ncout.region = region_label
        ncout.vi = vi_name
        ncout.source_datacube = str(datacube_path)
        ncout.whittaker_lambda = float(lam)
        ncout.peak_prominence = float(config["peak_prominence"])
        ncout.peak_min_distance_days = int(config["peak_min_distance_days"])
        ncout.season_threshold = float(config["season_threshold"])
        ncout.min_valid_obs = int(config["min_valid_obs"])
        if start_date:
            ncout.start_date = start_date
        if end_date:
            ncout.end_date = end_date

    logger.info("Saved → %s", out_nc_path)

    # ── Write summary CSV ─────────────────────────────────────────────────────
    rows = []
    for im, name in enumerate(METRIC_NAMES):
        vals = out_array[im].ravel()
        vals = vals[~np.isnan(vals)]
        rows.append({
            "metric":         name,
            "mean":           float(np.mean(vals))         if len(vals) > 0 else np.nan,
            "std":            float(np.std(vals))          if len(vals) > 0 else np.nan,
            "p05":            float(np.percentile(vals, 5))  if len(vals) > 0 else np.nan,
            "p50":            float(np.percentile(vals, 50)) if len(vals) > 0 else np.nan,
            "p95":            float(np.percentile(vals, 95)) if len(vals) > 0 else np.nan,
            "n_valid_pixels": int(len(vals)),
        })
    csv_path = region_out_dir / f"{vi_name}_{region_label}_pixel_metrics_summary.csv"
    pd.DataFrame(rows).to_csv(csv_path, index=False)
    logger.info("Saved summary CSV → %s", csv_path)

    # ── Overview figure (PNG) ─────────────────────────────────────────────
    if overview_figure:
        logger.info("Rendering overview figure ...")
        fig_path = _write_overview_figure(
            out_nc_path=out_nc_path,
            summary_csv_path=csv_path,
            vi_name=vi_name,
            region_label=region_label,
            config=config,
            datacube_path=datacube_path,
        )
        logger.info("Saved overview figure → %s", fig_path)

    # ── Overview figure (HTML) ────────────────────────────────────────────
    if overview_html:
        logger.info("Rendering interactive HTML overview ...")
        html_path = _write_overview_html(
            out_nc_path=out_nc_path,
            summary_csv_path=csv_path,
            vi_name=vi_name,
            region_label=region_label,
            config=config,
            datacube_path=datacube_path,
        )
        logger.info("Saved interactive HTML → %s", html_path)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_valid_range(s: str, vi: str) -> tuple:
    parts = s.split(",")
    if len(parts) != 2:
        raise argparse.ArgumentTypeError(
            f"--valid-range-{vi.lower()} must be MIN,MAX, got: {s!r}"
        )
    return float(parts[0]), float(parts[1])


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Pixel Phenology: extract 18 per-pixel phenological metrics from "
            "CF-1.8 datacubes produced by the netcdf_datacube pipeline."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # --- Input ---
    parser.add_argument(
        "--input-datacubes", nargs="+", required=True, metavar="PATH",
        help=(
            "Path(s) to *_datacube.nc files produced by the netcdf_datacube pipeline. "
            "VI and region label are inferred from each filename "
            "({VI}_{region_label}_datacube.nc)."
        ),
    )

    # --- Output ---
    parser.add_argument(
        "--output-dir", required=True,
        help="Root output directory. Per-region subdirectories are created automatically.",
    )

    # --- Valid ranges ---
    parser.add_argument("--valid-range-ndvi", default="-0.1,1.0", metavar="MIN,MAX",
                        help="Valid range for NDVI pixels")
    parser.add_argument("--valid-range-evi2", default="-1,2", metavar="MIN,MAX",
                        help="Valid range for EVI2 pixels")
    parser.add_argument("--valid-range-nirv", default="-0.5,1", metavar="MIN,MAX",
                        help="Valid range for NIRv pixels")

    # --- Whittaker smoother ---
    parser.add_argument(
        "--smooth-lambda", type=float, default=100.0, metavar="LAMBDA",
        help=(
            "Whittaker smoothing strength λ (default: 100). "
            "Higher = smoother curve. Typical range: 10 (tight) – 1000 (very smooth)."
        ),
    )

    # --- Pixel validity ---
    parser.add_argument(
        "--min-valid-obs", type=int, default=20, metavar="N",
        help=(
            "Minimum valid observations over the full record required to compute metrics "
            "for a pixel (default: 20). Pixels with fewer observations are set to NaN."
        ),
    )
    parser.add_argument(
        "--min-valid-obs-per-year", type=int, default=5, metavar="N",
        help=(
            "Minimum valid observations within an annual window for that year's metrics "
            "to be included in the pixel aggregate (default: 5). Years with fewer "
            "observations are skipped rather than contributing unreliable values."
        ),
    )

    # --- Bimodality ---
    parser.add_argument(
        "--peak-prominence", type=float, default=0.05, metavar="NDVI",
        help=(
            "Minimum NDVI prominence for a peak to count as bimodal (default: 0.05). "
            "Floor and ceiling NDVI are derived directly from the curve (no DOY windows)."
        ),
    )
    parser.add_argument(
        "--peak-min-distance", type=int, default=45, metavar="DAYS",
        help="Minimum separation (days) between detected peaks (default: 45).",
    )

    # --- Season length ---
    parser.add_argument(
        "--season-threshold", type=float, default=0.20, metavar="FRACTION",
        help=(
            "Amplitude fraction above floor for season-length calculation (default: 0.20). "
            "Season length = days above floor + threshold × (ceiling − floor)."
        ),
    )

    # --- Date range ---
    parser.add_argument(
        "--start-date", default=None, metavar="YYYY-MM-DD",
        help="Only use time steps on or after this date (inclusive).",
    )
    parser.add_argument(
        "--end-date", default=None, metavar="YYYY-MM-DD",
        help="Only use time steps on or before this date (inclusive).",
    )

    # --- Parallelization ---
    parser.add_argument(
        "--workers", type=int, default=8, metavar="N",
        help=(
            "Number of parallel threads for pixel processing (default: 8). "
            "Threads share the in-memory array; scipy sparse solver releases the GIL."
        ),
    )

    # --- Output options ---
    parser.add_argument(
        "--no-overview-figure", action="store_true", default=False,
        help=(
            "Skip the print-quality 19-panel overview PNG. "
            "By default an overview figure is generated for every datacube."
        ),
    )
    parser.add_argument(
        "--no-overview-html", action="store_true", default=False,
        help=(
            "Skip the interactive Plotly HTML overview. "
            "By default an HTML file with hover-enabled maps is generated for every datacube."
        ),
    )

    # --- Logging ---
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    setup_log_file(output_dir, "pixel_phenology", args.log_level)

    # Parse valid ranges.
    ndvi_min, ndvi_max = _parse_valid_range(args.valid_range_ndvi, "NDVI")
    evi2_min, evi2_max = _parse_valid_range(args.valid_range_evi2, "EVI2")
    nirv_min, nirv_max = _parse_valid_range(args.valid_range_nirv, "NIRv")

    config = {
        "smooth_lambda":          args.smooth_lambda,
        "min_valid_obs":          args.min_valid_obs,
        "min_valid_obs_per_year": args.min_valid_obs_per_year,
        "peak_prominence":        args.peak_prominence,
        "peak_min_distance_days": args.peak_min_distance,
        "season_threshold":       args.season_threshold,
        # Default fallback range (overridden per-datacube by VI name).
        "vi_min": -1.0,
        "vi_max":  2.0,
        # Per-VI ranges.
        "vi_min_ndvi": ndvi_min, "vi_max_ndvi": ndvi_max,
        "vi_min_evi2": evi2_min, "vi_max_evi2": evi2_max,
        "vi_min_nirv": nirv_min, "vi_max_nirv": nirv_max,
    }

    datacube_paths = [Path(p) for p in args.input_datacubes]
    logger.info("Pixel Phenology pipeline starting")
    logger.info("  Datacubes     : %d file(s)", len(datacube_paths))
    logger.info("  Output dir    : %s", output_dir)
    logger.info("  Workers       : %d threads", args.workers)
    logger.info("  Smooth λ      : %.1f", args.smooth_lambda)
    if args.start_date or args.end_date:
        logger.info(
            "  Date range    : %s → %s",
            args.start_date or "start of record",
            args.end_date or "end of record",
        )

    for dc_path in datacube_paths:
        if not dc_path.exists():
            logger.error("Datacube not found: %s — skipping.", dc_path)
            continue
        process_datacube(
            datacube_path=dc_path,
            output_dir=output_dir,
            config=config,
            n_workers=args.workers,
            start_date=args.start_date,
            end_date=args.end_date,
            overview_figure=not args.no_overview_figure,
            overview_html=not args.no_overview_html,
        )

    logger.info("Done. All outputs written to: %s", output_dir)


if __name__ == "__main__":
    main()
