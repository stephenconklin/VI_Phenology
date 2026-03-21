#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# phenology_config.py
# Configuration dataclass for VI Phenology.
# Built from parsed CLI arguments in vi_phenology.py.
#
# Author:  Stephen Conklin <stephenconklin@gmail.com>
# License: MIT

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass
class PhenologyConfig:
    # Input — one of netcdf_dir or input_datacubes must be set (see __post_init__)
    vi_list: list                        # e.g. ["NDVI", "EVI2"]
    shapefiles: Optional[list]           # list of Path, or None for full extent

    # Valid ranges per VI: {VI: (vmin, vmax)}
    valid_ranges: dict

    # Output
    output_dir: Path

    # Layer 2 smoothing
    smooth_method: str                   # savgol | loess | linear | harmonic | whittaker | none
    smooth_window: int                   # days (savgol, loess)
    smooth_polyorder: int                # savgol only

    # Layer 3 metrics
    compute_metrics: bool
    sos_threshold: float                 # fraction of amplitude (default 0.20)
    year_start_doy: int                  # 1 = Jan 1; >1 shifts annual window

    # Plotting style/format
    plot_style: str                      # raw | smooth | combined
    plot_formats: list                   # ["png"] | ["html"] | ["png", "html"]

    # ── Fields with defaults follow (Python dataclass requirement) ─────────────

    # Input mode — exactly one of these must be set
    netcdf_dir: Optional[Path] = None        # directory of source VI NetCDF tiles
    input_datacubes: Optional[list] = None   # list of Path to pre-clipped datacubes

    # Whittaker smoothing strength (Layer 2, only used when smooth_method='whittaker')
    smooth_lambda: float = 100.0         # penalised least-squares λ (typical 10–1000)

    # Layer 3 extended metrics — bimodality peak detection
    # Floor/ceiling NDVI are derived directly from the annual smooth curve (no DOY windows).
    peak_prominence: float = 0.05        # min NDVI prominence for a peak to count as bimodal
    peak_min_distance_days: int = 45     # min separation (days) between detected peaks

    # Observation count thresholds
    min_valid_obs: int = 20              # min valid obs over the full record; fewer → skip region
    min_valid_obs_per_year: int = 5      # min valid obs in an annual window; fewer → skip that year

    # Pixel sampling (optional — all None/0.0 = use all pixels)
    sample_pixels: Optional[int] = None         # N pixels to sample per region; None = all pixels
    random_seed: Optional[int] = None           # RNG seed for reproducibility; None = random
    min_ndvi_mean: Optional[float] = None       # exclude pixels below this temporal mean NDVI
    min_quality_frac: float = 0.0               # min fraction of timesteps a pixel must be valid

    # Shapefile attribute field for per-feature splitting (optional)
    # One entry per shapefile, positional. Use 'none' to dissolve a specific shapefile.
    shapefile_field: Optional[list] = None  # list[str] | None; len must == len(shapefiles)

    # Date range filtering (optional)
    start_date: Optional[str] = None    # YYYY-MM-DD, inclusive; None = no lower bound
    end_date: Optional[str] = None      # YYYY-MM-DD, inclusive; None = no upper bound

    # Parallelization
    n_workers: int = 4                  # parallel worker processes for tile extraction

    # Output toggles — all True by default for backwards compatibility
    save_observations_csv: bool = True   # write per-region observations-only CSV
    save_combined_outputs: bool = True   # write combined shapefile observations CSV
    plot_annual: bool = True             # annual DOY overlay plot
    plot_timeseries: bool = True         # full calendar time-series plot
    plot_anomaly: bool = True            # anomaly (departure from multi-year mean) plot
    plot_multi_vi: bool = True           # multi-VI comparison plot

    def __post_init__(self):
        """Validate configuration values at construction time."""
        # Internal registry: region_label → shapefile stem. Populated by enumerate_regions
        # so that output_dir_for can build the correct nested output path.
        self._region_shapefile_map: dict = {}

        errors = []

        if not (0.0 < self.sos_threshold < 1.0):
            errors.append(
                f"sos_threshold must be in (0, 1), got {self.sos_threshold}"
            )
        if not (1 <= self.year_start_doy <= 365):
            errors.append(
                f"year_start_doy must be in [1, 365], got {self.year_start_doy}"
            )
        if self.smooth_window < 1:
            errors.append(
                f"smooth_window must be >= 1, got {self.smooth_window}"
            )
        if self.smooth_polyorder < 0:
            errors.append(
                f"smooth_polyorder must be >= 0, got {self.smooth_polyorder}"
            )
        if self.smooth_method not in {
            "savgol", "loess", "linear", "harmonic", "whittaker", "none"
        }:
            errors.append(
                f"smooth_method must be one of savgol|loess|linear|harmonic|whittaker|none, "
                f"got {self.smooth_method!r}"
            )
        if (
            self.smooth_method == "savgol"
            and self.smooth_polyorder >= self.smooth_window
        ):
            errors.append(
                f"For savgol, smooth_polyorder ({self.smooth_polyorder}) must be "
                f"< smooth_window ({self.smooth_window})"
            )
        if self.shapefile_field is not None:
            if not self.shapefiles:
                errors.append(
                    "--shapefile-field requires --shapefile to be set"
                )
            elif len(self.shapefile_field) != len(self.shapefiles):
                errors.append(
                    f"--shapefile-field has {len(self.shapefile_field)} value(s) but "
                    f"--shapefile has {len(self.shapefiles)} path(s) — counts must match exactly "
                    "(use 'none' to dissolve a specific shapefile rather than splitting by field)"
                )
        if not self.vi_list:
            errors.append("vi_list must contain at least one VI name")

        # Input mode: exactly one of netcdf_dir or input_datacubes must be set.
        if self.netcdf_dir is not None and self.input_datacubes is not None:
            errors.append(
                "Specify exactly one input mode: --netcdf-dir or --input-datacubes, not both."
            )
        elif self.netcdf_dir is None and not self.input_datacubes:
            errors.append(
                "One of --netcdf-dir or --input-datacubes must be provided."
            )
        elif self.netcdf_dir is not None and not self.netcdf_dir.exists():
            errors.append(f"netcdf_dir does not exist: {self.netcdf_dir}")
        elif self.input_datacubes:
            for dc_path in self.input_datacubes:
                if not Path(dc_path).exists():
                    errors.append(f"input datacube does not exist: {dc_path}")

        if self.n_workers < 1:
            errors.append(f"n_workers must be >= 1, got {self.n_workers}")
        if self.smooth_lambda <= 0:
            errors.append(f"smooth_lambda must be > 0, got {self.smooth_lambda}")
        if not (0 < self.peak_prominence < 1):
            errors.append(
                f"peak_prominence must be in (0, 1), got {self.peak_prominence}"
            )
        if self.peak_min_distance_days < 1:
            errors.append(
                f"peak_min_distance_days must be >= 1, got {self.peak_min_distance_days}"
            )
        if self.min_valid_obs < 1:
            errors.append(f"min_valid_obs must be >= 1, got {self.min_valid_obs}")
        if self.min_valid_obs_per_year < 1:
            errors.append(
                f"min_valid_obs_per_year must be >= 1, got {self.min_valid_obs_per_year}"
            )
        if self.sample_pixels is not None and self.sample_pixels < 1:
            errors.append(f"sample_pixels must be >= 1, got {self.sample_pixels}")
        if not (0.0 <= self.min_quality_frac <= 1.0):
            errors.append(
                f"min_quality_frac must be in [0, 1], got {self.min_quality_frac}"
            )

        _fmt = "%Y-%m-%d"
        if self.start_date is not None:
            try:
                datetime.strptime(self.start_date, _fmt)
            except ValueError:
                errors.append(
                    f"start_date must be YYYY-MM-DD, got '{self.start_date}'"
                )
        if self.end_date is not None:
            try:
                datetime.strptime(self.end_date, _fmt)
            except ValueError:
                errors.append(
                    f"end_date must be YYYY-MM-DD, got '{self.end_date}'"
                )
        if (
            self.start_date and self.end_date
            and self.start_date > self.end_date
        ):
            errors.append(
                f"start_date ({self.start_date}) must be <= end_date ({self.end_date})"
            )

        if errors:
            raise ValueError(
                "PhenologyConfig validation failed:\n"
                + "\n".join(f"  - {e}" for e in errors)
            )

    def register_region(self, region_label: str, shapefile_stem: str) -> None:
        """Register the source shapefile stem for a region label.

        Called by enumerate_regions() in extract.py for each (region_label, roi_gdf)
        pair so that output_dir_for() can build the correct nested output path.
        """
        self._region_shapefile_map[region_label] = shapefile_stem

    def field_for_shapefile(self, index: int) -> Optional[str]:
        """Return the split field name for the shapefile at position index.

        Returns None when:
          - shapefile_field is not set (all shapefiles dissolved), or
          - the field value at this position is 'none' (case-insensitive).
        """
        if not self.shapefile_field:
            return None
        value = self.shapefile_field[index]
        return None if value.lower() == "none" else value

    def valid_range_for(self, vi: str) -> tuple:
        """Return (vmin, vmax) for the given VI. Raises KeyError if VI not configured."""
        return self.valid_ranges[vi]

    def region_label_for(self, shapefile: Optional[Path]) -> str:
        """Return a filesystem-safe region label from a shapefile path, or 'full_extent'."""
        if shapefile is None:
            return "full_extent"
        return shapefile.stem

    def output_dir_for(self, region_label: str) -> Path:
        """Return the output directory for a given region.

        Directory structure:
          Datacube mode                 → output_dir/{region_label}/
          No shapefile                  → output_dir/
          Shapefile, dissolved          → output_dir/{shapefile_stem}/
          Shapefile + field             → output_dir/{shapefile_stem}/{field_value}/
        """
        if self.input_datacubes:
            return self.output_dir / region_label
        if not self.shapefiles:
            return self.output_dir
        shapefile_stem = self._region_shapefile_map.get(region_label, region_label)
        if shapefile_stem != region_label:
            # Field-split: nest the field value under its source shapefile folder.
            return self.output_dir / shapefile_stem / region_label
        # Dissolved: the region_label IS the shapefile stem.
        return self.output_dir / region_label
