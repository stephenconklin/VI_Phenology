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
from pathlib import Path

import pandas as pd

from phenology_config import PhenologyConfig
from extract import (
    enumerate_regions,
    discover_netcdfs,
    aggregate_across_tiles,
    aggregate_from_datacube,
    reindex_to_daily,
)
from smooth import smooth_timeseries
from metrics import compute_metrics, write_combined_metrics
from plot import generate_plots
from io_utils import (
    save_observations_csv,
    write_combined_observations_csv,
    parse_valid_range, setup_log_file,
)

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
        "--netcdf-dir", default=None,
        help=(
            "Directory containing VI NetCDF files (T{TILE}_{VI}.nc). "
            "Mutually exclusive with --input-datacubes."
        ),
    )
    parser.add_argument(
        "--input-datacubes", nargs="+", default=None, metavar="PATH",
        help=(
            "Pre-clipped per-pixel datacube file(s) from the netcdf_datacube pipeline "
            "({VI}_{region_label}_datacube.nc). VI and region_label are inferred from "
            "the filename. Mutually exclusive with --netcdf-dir and --shapefile."
        ),
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
        choices=["savgol", "loess", "linear", "harmonic", "whittaker", "none"],
        help=(
            "Smoothing / gap-fill method for Layer 2 daily series. "
            "'none' skips Layer 2 and produces only the sparse daily output. "
            "'whittaker' uses penalised least-squares (λ set by --smooth-lambda)."
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
    parser.add_argument(
        "--smooth-lambda", type=float, default=100.0, metavar="LAMBDA",
        help=(
            "Smoothing strength for Whittaker method (default: 100). "
            "Larger values produce smoother curves. Typical range: 10–1000. "
            "Only used when --smooth-method whittaker."
        ),
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
    parser.add_argument(
        "--peak-prominence", type=float, default=0.05, metavar="NDVI",
        help=(
            "Minimum NDVI prominence for bimodality peak detection (default: 0.05). "
            "Increase to 0.08–0.10 for noisier or semi-arid time series. "
            "Floor and ceiling NDVI are derived directly from the annual smooth "
            "curve (no seasonal DOY windows required). Only used with --metrics."
        ),
    )
    parser.add_argument(
        "--peak-min-distance", type=int, default=45, metavar="DAYS",
        help=(
            "Minimum separation (days) between detected peaks for bimodality "
            "(default: 45). Only used with --metrics."
        ),
    )

    # --- Observation count thresholds ---
    parser.add_argument(
        "--min-valid-obs", type=int, default=20, metavar="N",
        help=(
            "Minimum valid observations over the full record for a region to be processed "
            "(default: 20). Regions with fewer observations are skipped with a warning."
        ),
    )
    parser.add_argument(
        "--min-valid-obs-per-year", type=int, default=5, metavar="N",
        help=(
            "Minimum valid observations within an annual window for that year's metrics "
            "to be computed (default: 5). Years with fewer observations are skipped "
            "(NaN row omitted), rather than producing unreliable phenological metrics."
        ),
    )

    # --- Pixel sampling ---
    parser.add_argument(
        "--sample-pixels", type=int, default=None, metavar="N",
        help=(
            "Randomly sample N pixels per region and use only those pixels consistently "
            "across the full time series. Eliminates date-to-date variation in the spatial "
            "sample caused by cloud masking. None (default) = use all valid pixels."
        ),
    )
    parser.add_argument(
        "--random-seed", type=int, default=None, metavar="SEED",
        help=(
            "Integer seed for the pixel sampling RNG (default: None = random). "
            "Set to a fixed value for reproducible pixel samples across runs."
        ),
    )
    parser.add_argument(
        "--min-ndvi-mean", type=float, default=None, metavar="NDVI",
        help=(
            "Exclude pixels whose temporal mean NDVI is below this threshold before "
            "sampling. Removes bare soil, water, and low-vegetation pixels from the "
            "spatial pool. None (default) = no exclusion."
        ),
    )
    parser.add_argument(
        "--min-quality-frac", type=float, default=0.0, metavar="FRAC",
        help=(
            "Minimum fraction of time steps a pixel must have valid (non-NaN) data to "
            "be eligible for sampling (default: 0.0 = no filter). Use 0.2–0.3 to exclude "
            "persistently cloud-covered pixels."
        ),
    )

    # --- Plotting style/format ---
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

    # --- Output toggles ---
    parser.add_argument(
        "--no-observations-csv", action="store_true",
        help="Skip writing per-region observations-only CSV files",
    )
    parser.add_argument(
        "--no-combined-outputs", action="store_true",
        help="Skip writing the combined shapefile observations CSV",
    )
    parser.add_argument(
        "--no-plot-annual", action="store_true",
        help="Skip the annual DOY overlay plot",
    )
    parser.add_argument(
        "--no-plot-timeseries", action="store_true",
        help="Skip the full calendar time-series plot",
    )
    parser.add_argument(
        "--no-plot-anomaly", action="store_true",
        help="Skip the anomaly (departure from multi-year mean) plot",
    )
    parser.add_argument(
        "--no-plot-multi-vi", action="store_true",
        help="Skip the multi-VI comparison plot",
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
    return parser.parse_args()


_VALID_VIS = {"NDVI", "EVI2", "NIRv"}


def _enumerate_datacube_regions(config) -> list:
    """Parse datacube filenames → list of (vi, region_label, dc_path) triples.

    Expected filename pattern: {VI}_{region_label}_datacube.nc
    VI is the first underscore-separated token; region_label is everything
    between the first underscore and the trailing '_datacube' suffix.
    Files that do not match the pattern or whose VI is not in config.vi_list
    are skipped with a warning/info log.
    """
    triples = []
    for dc_path in config.input_datacubes:
        stem = dc_path.stem   # e.g. "NDVI_G5_12_datacube"
        if not stem.endswith("_datacube"):
            logger.warning(
                "Datacube filename '%s' does not match expected pattern "
                "{VI}_{region_label}_datacube.nc — skipping.", dc_path.name,
            )
            continue
        stem_no_suffix = stem[:-len("_datacube")]   # "NDVI_G5_12"
        parts = stem_no_suffix.split("_", 1)
        if len(parts) != 2:
            logger.warning(
                "Cannot parse VI and region_label from '%s' — skipping.", dc_path.name,
            )
            continue
        vi_from_name, region_label = parts
        if vi_from_name not in _VALID_VIS:
            logger.warning(
                "Unrecognised VI '%s' in '%s' — skipping.", vi_from_name, dc_path.name,
            )
            continue
        if vi_from_name not in config.vi_list:
            logger.info(
                "VI '%s' from '%s' not in --vi list %s — skipping.",
                vi_from_name, dc_path.name, config.vi_list,
            )
            continue
        triples.append((vi_from_name, region_label, dc_path))
    return triples


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

    valid_ranges = {
        "NDVI": parse_valid_range(args.valid_range_ndvi, "NDVI"),
        "EVI2": parse_valid_range(args.valid_range_evi2, "EVI2"),
        "NIRv": parse_valid_range(args.valid_range_nirv, "NIRv"),
    }

    config = PhenologyConfig(
        netcdf_dir=Path(args.netcdf_dir) if args.netcdf_dir else None,
        input_datacubes=[Path(p) for p in args.input_datacubes] if args.input_datacubes else None,
        vi_list=args.vi,
        shapefiles=[Path(s) for s in args.shapefile] if args.shapefile else None,
        shapefile_field=args.shapefile_field,
        valid_ranges=valid_ranges,
        output_dir=Path(args.output_dir),
        smooth_method=args.smooth_method,
        smooth_window=args.smooth_window,
        smooth_polyorder=args.smooth_polyorder,
        smooth_lambda=args.smooth_lambda,
        sos_threshold=args.sos_threshold,
        year_start_doy=args.year_start_doy,
        peak_prominence=args.peak_prominence,
        peak_min_distance_days=args.peak_min_distance,
        min_valid_obs=args.min_valid_obs,
        min_valid_obs_per_year=args.min_valid_obs_per_year,
        sample_pixels=args.sample_pixels,
        random_seed=args.random_seed,
        min_ndvi_mean=args.min_ndvi_mean,
        min_quality_frac=args.min_quality_frac,
        plot_style=args.plot_style,
        plot_formats=args.plot_format,
        compute_metrics=args.metrics,
        start_date=args.start_date,
        end_date=args.end_date,
        n_workers=args.workers,
        save_observations_csv=not args.no_observations_csv,
        save_combined_outputs=not args.no_combined_outputs,
        plot_annual=not args.no_plot_annual,
        plot_timeseries=not args.no_plot_timeseries,
        plot_anomaly=not args.no_plot_anomaly,
        plot_multi_vi=not args.no_plot_multi_vi,
    )

    config.output_dir.mkdir(parents=True, exist_ok=True)

    setup_log_file(config.output_dir, "vi_phenology", args.log_level)

    logger.info("VI Phenology pipeline starting")
    if config.input_datacubes:
        logger.info("  Input mode      : input-datacubes")
        logger.info("  Datacubes       : %s", [str(p) for p in config.input_datacubes])
    else:
        logger.info("  Input mode      : netcdf-dir")
        logger.info("  NetCDF dir      : %s", config.netcdf_dir)
    logger.info("  VIs             : %s", ", ".join(config.vi_list))

    # Valid ranges — one entry per configured VI.
    ranges_str = "  ".join(
        f"{vi} [{config.valid_range_for(vi)[0]:.4f}, {config.valid_range_for(vi)[1]:.4f}]"
        for vi in config.vi_list
    )
    logger.info("  Valid ranges    : %s", ranges_str)

    if not config.input_datacubes:
        logger.info(
            "  Shapefiles      : %s",
            [str(s) for s in config.shapefiles] if config.shapefiles else "None (full extent)",
        )
        if config.shapefile_field:
            logger.info("  Shapefile fields: %s", config.shapefile_field)

    if config.start_date or config.end_date:
        logger.info(
            "  Date range      : %s → %s",
            config.start_date or "beginning of record",
            config.end_date or "end of record",
        )

    # Smoothing — method name plus its active parameters.
    _m = config.smooth_method
    if _m == "whittaker":
        smooth_desc = f"whittaker  λ={config.smooth_lambda:.0f}"
    elif _m == "savgol":
        smooth_desc = f"savgol  window={config.smooth_window}d  polyorder={config.smooth_polyorder}"
    elif _m == "loess":
        smooth_desc = f"loess  window={config.smooth_window}d"
    else:
        smooth_desc = _m   # linear | harmonic | none — no additional parameters
    logger.info("  Smooth          : %s", smooth_desc)

    # Metrics — log all parameters that affect metric computation.
    if config.compute_metrics:
        logger.info(
            "  Metrics         : sos_threshold=%.2f  year_start_doy=%d  "
            "peak_prominence=%.3f  peak_min_distance=%dd",
            config.sos_threshold, config.year_start_doy,
            config.peak_prominence, config.peak_min_distance_days,
        )
    else:
        logger.info("  Metrics         : disabled")

    # Observation thresholds — always logged as they affect which data is used.
    logger.info(
        "  Obs thresholds  : min_valid_obs=%d  min_valid_obs_per_year=%d",
        config.min_valid_obs, config.min_valid_obs_per_year,
    )

    # Pixel sampling — only logged when active.
    if config.sample_pixels is not None or config.min_ndvi_mean is not None or config.min_quality_frac > 0:
        logger.info(
            "  Pixel sampling  : n=%s  seed=%s  min_ndvi_mean=%s  min_quality_frac=%.2f",
            config.sample_pixels if config.sample_pixels is not None else "all",
            config.random_seed if config.random_seed is not None else "random",
            f"{config.min_ndvi_mean:.4f}" if config.min_ndvi_mean is not None else "none",
            config.min_quality_frac,
        )

    logger.info("  Workers         : %d", config.n_workers)
    logger.info("  Output dir      : %s", config.output_dir)
    logger.info("  Log level       : %s", args.log_level)

    # Enumerate all regions upfront.
    if config.input_datacubes:
        # Datacube mode: regions are determined by filename parsing.
        from collections import defaultdict
        dc_triples = _enumerate_datacube_regions(config)
        if not dc_triples:
            logger.error(
                "No valid datacubes found. Check filenames follow "
                "{VI}_{region_label}_datacube.nc and --vi includes the VI in the filename."
            )
            sys.exit(1)
        # Group by region_label preserving insertion order.
        by_region: dict = defaultdict(list)
        for vi, region_label, dc_path in dc_triples:
            by_region[region_label].append((vi, dc_path))
        # Register each region so output_dir_for() builds correct paths.
        for region_label in by_region:
            config.register_region(region_label, region_label)
        regions_iter = list(by_region.items())   # [(region_label, [(vi, dc_path), ...])]
        n_regions = len(regions_iter)
        logger.info("[Setup] %d region(s) to process (datacube mode).", n_regions)
    else:
        # Standard mode: regions come from shapefiles (or full_extent).
        std_regions = enumerate_regions(config)
        regions_iter = [(rl, roi_gdf) for rl, roi_gdf in std_regions]
        n_regions = len(regions_iter)
        logger.info("[Setup] %d region(s) to process.", n_regions)

    # Accumulate metrics rows and observation data across all regions for combined CSVs.
    all_metrics: list[pd.DataFrame] = []
    all_obs: dict = {}
    any_extracted = False

    # ── Per-region streaming pipeline ────────────────────────────────────────
    for region_idx, region_item in enumerate(regions_iter, start=1):

        if config.input_datacubes:
            region_label, vi_dc_pairs = region_item
            roi_gdf = None
        else:
            region_label, roi_gdf = region_item
            vi_dc_pairs = None

        logger.info(
            "══ Region %d/%d: %s ══", region_idx, n_regions, region_label
        )

        # Layer 0+1: extract all configured VIs for this region.
        region_raw: dict = {}

        if config.input_datacubes:
            # Datacube mode: read each VI from its pre-clipped file.
            for vi, dc_path in vi_dc_pairs:
                vmin, vmax = config.valid_range_for(vi)
                logger.info(
                    "[Layer 0+1] %s / %s — datacube mode, valid range [%.4f, %.4f]",
                    vi, region_label, vmin, vmax,
                )
                obs_df = aggregate_from_datacube(
                    dc_path, vi, vmin, vmax,
                    start_date=config.start_date,
                    end_date=config.end_date,
                    n_sample=config.sample_pixels,
                    random_seed=config.random_seed,
                    min_ndvi_mean=config.min_ndvi_mean,
                    min_quality_frac=config.min_quality_frac,
                )
                if obs_df.empty:
                    logger.warning(
                        "%s / %s: no valid observations extracted — skipping this VI.",
                        vi, region_label,
                    )
                    continue
                n_obs = int(len(obs_df))
                if n_obs < config.min_valid_obs:
                    logger.warning(
                        "%s / %s: only %d valid observation(s) (min_valid_obs=%d) — skipping.",
                        vi, region_label, n_obs, config.min_valid_obs,
                    )
                    continue
                daily_df = reindex_to_daily(obs_df)
                region_raw[(vi, region_label)] = daily_df

        else:
            # Standard mode: discover tiles and aggregate across them.
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
                    n_sample=config.sample_pixels,
                    random_seed=config.random_seed,
                    min_ndvi_mean=config.min_ndvi_mean,
                    min_quality_frac=config.min_quality_frac,
                )
                if obs_df.empty:
                    logger.warning(
                        "%s / %s: no valid observations extracted — skipping this VI.",
                        vi, region_label,
                    )
                    continue

                n_obs = int(len(obs_df))
                if n_obs < config.min_valid_obs:
                    logger.warning(
                        "%s / %s: only %d valid observation(s) (min_valid_obs=%d) — skipping.",
                        vi, region_label, n_obs, config.min_valid_obs,
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

        if config.save_observations_csv:
            logger.info("[Output] Saving observations CSV for region '%s' ...", region_label)
            region_obs = save_observations_csv(region_raw, region_smoothed, config)
            for key, dfs in region_obs.items():
                all_obs.setdefault(key, []).extend(dfs)

        # Layer 3: phenological metrics for this region.
        if config.compute_metrics:
            logger.info("[Layer 3] Computing metrics for region '%s' ...", region_label)
            region_metrics_df = compute_metrics(region_smoothed, config)
            if not region_metrics_df.empty:
                all_metrics.append(region_metrics_df)

        # Plots for this region — skipped entirely when all plot types are disabled.
        _any_plots = any([
            config.plot_annual, config.plot_timeseries,
            config.plot_anomaly, config.plot_multi_vi,
        ])
        if _any_plots:
            logger.info(
                "[Plots] Generating %s plots for region '%s' ...",
                ", ".join(config.plot_formats), region_label,
            )
            generate_plots(region_raw, region_smoothed, config)

        logger.info("══ Region '%s' complete ══", region_label)
        # region_raw and region_smoothed go out of scope here and are eligible for GC.

    # ── Post-loop ─────────────────────────────────────────────────────────────
    if not any_extracted:
        if config.input_datacubes:
            logger.error(
                "No time series extracted from any datacube. "
                "Check that files exist and the VI variable is present in each file."
            )
        else:
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

    if config.save_combined_outputs:
        write_combined_observations_csv(all_obs, config)

    logger.info("Done. All outputs written to: %s", config.output_dir)


if __name__ == "__main__":
    main()
