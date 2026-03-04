#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# vi_phenology.py
# CLI entrypoint for VI Phenology analysis tool.
#
# Orchestrates the four-layer processing pipeline one region at a time:
#   Layer 0+1  extract.py  — raw observations + daily reindex
#   Layer 2    smooth.py   — gap-fill + smooth (skipped if --smooth-method none)
#   Layer 3    metrics.py  — phenological metrics (requires --metrics flag)
#
# Processing order: for each region, all layers run to completion and outputs
# are written to disk before the next region begins. This bounds peak memory
# to one region's data at a time regardless of how many regions are configured.
# The combined shapefile metrics CSV is written at the end after all regions.
#
# Author:  Stephen Conklin <stephenconklin@gmail.com>
#          https://github.com/stephenconklin
# License: MIT

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from phenology_config import PhenologyConfig
from extract import (
    enumerate_regions,
    discover_netcdfs,
    aggregate_across_tiles,
    reindex_to_daily,
)
from smooth import smooth_timeseries
from metrics import compute_metrics, write_combined_metrics
from plot import generate_plots
from io_utils import save_parquet

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "VI Phenology: Extract, smooth, and visualize vegetation index phenology "
            "from CF-1.8 NetCDF time-series files."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # --- Input ---
    parser.add_argument(
        "--netcdf-dir", required=True,
        help="Directory containing VI NetCDF files (T{TILE}_{VI}.nc)",
    )
    parser.add_argument(
        "--vi", nargs="+", default=["NDVI"],
        choices=["NDVI", "EVI2", "NIRv"],
        help="Vegetation indices to process",
    )
    parser.add_argument(
        "--shapefile", nargs="+", default=None,
        help="Optional shapefile(s) for spatial subsetting. One time series per shapefile.",
    )
    parser.add_argument(
        "--shapefile-field", nargs="+", default=None, metavar="FIELDNAME",
        help=(
            "Attribute field(s) to split shapefile(s) by. Provide one value per shapefile "
            "in the same positional order as --shapefile. Use 'none' to dissolve a specific "
            "shapefile instead of splitting it. Count must match --shapefile exactly. "
            "When omitted, all features in all shapefiles are dissolved (default behavior)."
        ),
    )

    # --- Valid ranges ---
    parser.add_argument("--valid-range-ndvi", default="-1,1",  metavar="MIN,MAX",
                        help="Valid range for NDVI")
    parser.add_argument("--valid-range-evi2", default="-1,2",  metavar="MIN,MAX",
                        help="Valid range for EVI2")
    parser.add_argument("--valid-range-nirv", default="-0.5,1", metavar="MIN,MAX",
                        help="Valid range for NIRv")

    # --- Output ---
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory for all output files (created if it does not exist)",
    )

    # --- Smoothing ---
    parser.add_argument(
        "--smooth-method", default="savgol",
        choices=["savgol", "loess", "linear", "harmonic", "none"],
        help=(
            "Smoothing / gap-fill method for Layer 2 daily series. "
            "'none' skips Layer 2 and produces only the sparse daily output."
        ),
    )
    parser.add_argument(
        "--smooth-window", type=int, default=15, metavar="DAYS",
        help="Smoothing window in days (savgol and loess)",
    )
    parser.add_argument(
        "--smooth-polyorder", type=int, default=3,
        help="Polynomial order for Savitzky-Golay smoothing",
    )

    # --- Phenological metrics ---
    parser.add_argument(
        "--metrics", action="store_true",
        help="Compute and export phenological metrics table (SOS, POS, EOS, LOS, IVI)",
    )
    parser.add_argument(
        "--sos-threshold", type=float, default=0.20, metavar="FRACTION",
        help="Amplitude fraction used to define SOS and EOS (default: 0.20 = 20%%)",
    )
    parser.add_argument(
        "--year-start-doy", type=int, default=1, metavar="DOY",
        help=(
            "Day of year to begin each annual phenology window (1–365). "
            "Use values > 1 for Southern Hemisphere or Mediterranean seasonality."
        ),
    )

    # --- Spatial mode ---
    parser.add_argument(
        "--mode", default="roi_mean",
        choices=["roi_mean", "per_pixel"],
        help=(
            "Spatial aggregation mode. "
            "'roi_mean': single time series per shapefile region. "
            "'per_pixel': preserve spatial dimensions within the ROI."
        ),
    )

    # --- Plotting ---
    parser.add_argument(
        "--plot-style", default="combined",
        choices=["raw", "smooth", "combined"],
        help=(
            "'raw': observation scatter only. "
            "'smooth': smooth curve only. "
            "'combined': smooth curve + raw scatter behind it."
        ),
    )
    parser.add_argument(
        "--plot-format", nargs="+", default=["png"],
        choices=["png", "html"],
        help="Output format(s) for plots",
    )

    # --- Parallelization ---
    parser.add_argument(
        "--workers", type=int, default=8, metavar="N",
        help=(
            "Number of parallel worker processes for tile-level extraction (default: 8). "
            "Set to 1 to disable parallelism and process tiles sequentially."
        ),
    )

    # --- Date range filtering ---
    parser.add_argument(
        "--start-date", default=None, metavar="YYYY-MM-DD",
        help=(
            "Only include observations on or after this date (inclusive). "
            "Useful for limiting processing to a specific time window."
        ),
    )
    parser.add_argument(
        "--end-date", default=None, metavar="YYYY-MM-DD",
        help=(
            "Only include observations on or before this date (inclusive). "
            "Useful for limiting processing to a specific time window."
        ),
    )

    # --- Logging ---
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level",
    )
    parser.add_argument(
        "--no-logfile", action="store_true",
        help=(
            "Disable automatic log file creation. "
            "By default, a timestamped log file is written to --output-dir."
        ),
    )

    return parser.parse_args()


def _parse_valid_range(raw: str, vi: str) -> tuple:
    """Parse 'min,max' string to (float, float), with a helpful error message."""
    try:
        parts = raw.split(",")
        return float(parts[0]), float(parts[1])
    except (ValueError, IndexError):
        logger.error(
            "Could not parse --valid-range-%s='%s'. Expected format: 'min,max' e.g. '-1,1'",
            vi.lower(), raw,
        )
        sys.exit(1)


def main():
    args = parse_args()

    # Configure logging before anything else.
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    # Validate smooth_method / metrics combination.
    if args.metrics and args.smooth_method == "none":
        logger.error(
            "--metrics requires a smoothing method. "
            "Use --smooth-method savgol (or any method other than 'none')."
        )
        sys.exit(1)

    # per_pixel mode is not yet implemented.
    if args.mode == "per_pixel":
        logger.error("--mode per_pixel is not yet implemented. Use --mode roi_mean instead.")
        sys.exit(1)

    valid_ranges = {
        "NDVI": _parse_valid_range(args.valid_range_ndvi, "NDVI"),
        "EVI2": _parse_valid_range(args.valid_range_evi2, "EVI2"),
        "NIRv": _parse_valid_range(args.valid_range_nirv, "NIRv"),
    }

    config = PhenologyConfig(
        netcdf_dir=Path(args.netcdf_dir),
        vi_list=args.vi,
        shapefiles=[Path(s) for s in args.shapefile] if args.shapefile else None,
        shapefile_field=args.shapefile_field,
        valid_ranges=valid_ranges,
        output_dir=Path(args.output_dir),
        smooth_method=args.smooth_method,
        smooth_window=args.smooth_window,
        smooth_polyorder=args.smooth_polyorder,
        sos_threshold=args.sos_threshold,
        year_start_doy=args.year_start_doy,
        mode=args.mode,
        plot_style=args.plot_style,
        plot_formats=args.plot_format,
        compute_metrics=args.metrics,
        start_date=args.start_date,
        end_date=args.end_date,
        n_workers=args.workers,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)

    # Attach a timestamped log file to the root logger (unless --no-logfile).
    if not args.no_logfile:
        run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = config.output_dir / f"vi_phenology_{run_ts}.log"
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(getattr(logging, args.log_level))
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logging.getLogger("").addHandler(file_handler)
        logger.info("Log file: %s", log_path)

    logger.info("VI Phenology pipeline starting")
    logger.info("  NetCDF dir    : %s", config.netcdf_dir)
    logger.info("  VIs           : %s", ", ".join(config.vi_list))
    logger.info(
        "  Shapefiles    : %s",
        [str(s) for s in config.shapefiles] if config.shapefiles else "None (full extent)",
    )
    if config.shapefile_field:
        logger.info("  Shapefile fields: %s", config.shapefile_field)
    logger.info("  Smooth method : %s", config.smooth_method)
    logger.info("  Compute metrics: %s", config.compute_metrics)
    logger.info("  Workers       : %d", config.n_workers)
    logger.info("  Output dir    : %s", config.output_dir)
    if config.start_date or config.end_date:
        logger.info(
            "  Date range    : %s → %s",
            config.start_date or "beginning of record",
            config.end_date or "end of record",
        )
    logger.info("  Log level     : %s", args.log_level)

    # Enumerate all regions upfront (validates shapefiles exist).
    regions = enumerate_regions(config)
    logger.info("[Setup] %d region(s) to process.", len(regions))

    # Accumulate metrics rows across all regions for the combined CSV.
    all_metrics: list[pd.DataFrame] = []
    any_extracted = False

    # ── Per-region streaming pipeline ────────────────────────────────────────
    for region_idx, (region_label, roi_gdf) in enumerate(regions, start=1):
        logger.info(
            "══ Region %d/%d: %s ══", region_idx, len(regions), region_label
        )

        # Layer 0+1: extract all configured VIs for this region.
        region_raw: dict = {}
        for vi in config.vi_list:
            nc_paths = discover_netcdfs(config.netcdf_dir, vi)
            if not nc_paths:
                logger.warning(
                    "Skipping %s / %s — no matching NetCDF files in %s",
                    vi, region_label, config.netcdf_dir,
                )
                continue

            vmin, vmax = config.valid_range_for(vi)
            logger.info(
                "[Layer 0+1] %s / %s — %d tile(s), valid range [%.4f, %.4f]",
                vi, region_label, len(nc_paths), vmin, vmax,
            )

            obs_df = aggregate_across_tiles(
                nc_paths, roi_gdf, vmin, vmax,
                start_date=config.start_date,
                end_date=config.end_date,
                n_workers=config.n_workers,
            )
            if obs_df.empty:
                logger.warning(
                    "%s / %s: no valid observations extracted — skipping this VI.",
                    vi, region_label,
                )
                continue

            daily_df = reindex_to_daily(obs_df)
            region_raw[(vi, region_label)] = daily_df

        if not region_raw:
            logger.warning(
                "Region '%s': no data extracted for any VI — skipping.", region_label
            )
            continue

        any_extracted = True

        # Layer 2: smooth.
        region_smoothed = None
        if config.smooth_method != "none":
            logger.info(
                "[Layer 2] Smoothing with method='%s' for region '%s' ...",
                config.smooth_method, region_label,
            )
            region_smoothed = smooth_timeseries(region_raw, config)

        # Save Parquet for this region (Layers 1 + 2 combined).
        logger.info("[Output] Saving Parquet for region '%s' ...", region_label)
        save_parquet(region_raw, region_smoothed, config)

        # Layer 3: phenological metrics for this region.
        if config.compute_metrics:
            logger.info("[Layer 3] Computing metrics for region '%s' ...", region_label)
            region_metrics_df = compute_metrics(region_smoothed, config)
            if not region_metrics_df.empty:
                all_metrics.append(region_metrics_df)

        # Plots for this region.
        logger.info(
            "[Plots] Generating %s plots for region '%s' ...",
            ", ".join(config.plot_formats), region_label,
        )
        generate_plots(region_raw, region_smoothed, config)

        logger.info("══ Region '%s' complete ══", region_label)
        # region_raw and region_smoothed go out of scope here and are eligible for GC.

    # ── Post-loop ─────────────────────────────────────────────────────────────
    if not any_extracted:
        logger.error(
            "No time series extracted for any region. "
            "Check --netcdf-dir contains T*_{VI}.nc files "
            "and that any shapefile intersects the data extent."
        )
        sys.exit(1)

    # Combined shapefile metrics CSV (written after all regions are complete).
    if config.compute_metrics and all_metrics:
        all_metrics_df = pd.concat(all_metrics, ignore_index=True)
        write_combined_metrics(all_metrics_df, config)

    logger.info("Done. All outputs written to: %s", config.output_dir)


if __name__ == "__main__":
    main()
