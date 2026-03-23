#!/usr/bin/env bash
# run_phenology.sh — VI Phenology pipeline execution engine.
# Edit config.local.env (not this file) to configure your run.
# Usage: ./run_phenology.sh

# ── Load configuration ─────────────────────────────────────────────────────────
if [ ! -f config.env ]; then
    echo "ERROR: config.env not found. Run from the repository root." >&2
    exit 1
fi
set -a
source config.env
[ -f config.local.env ] && source config.local.env
set +a

# ── Per-pipeline output directory defaults ─────────────────────────────────────
# Each pipeline has its own output dir variable, all defaulting to OUTPUT_DIR.
# Override any of these in config.local.env if you want separate directories.
NETCDF_DATACUBE_OUTPUT_DIR="${NETCDF_DATACUBE_OUTPUT_DIR:-${OUTPUT_DIR}}"
PHENOLOGY_OUTPUT_DIR="${PHENOLOGY_OUTPUT_DIR:-${OUTPUT_DIR}}"
PIXEL_OUTPUT_DIR="${PIXEL_OUTPUT_DIR:-${OUTPUT_DIR}/pixel_phenology_metrics}"
GEOTIFF_OUTPUT_DIR="${GEOTIFF_OUTPUT_DIR:-${OUTPUT_DIR}/geotiff_stats}"

set -euo pipefail

# ── Shared flag assembly ───────────────────────────────────────────────────────
SHAPEFILE_FLAG=""
if [ -n "${SHAPEFILE:-}" ]; then
    SHAPEFILE_FLAG="--shapefile $SHAPEFILE"
fi

SHAPEFILE_FIELD_FLAG=""
if [ -n "${SHAPEFILE_FIELD:-}" ]; then
    SHAPEFILE_FIELD_FLAG="--shapefile-field $SHAPEFILE_FIELD"
fi

DATE_FLAG=""
if [ -n "${START_DATE:-}" ]; then
    DATE_FLAG="$DATE_FLAG --start-date $START_DATE"
fi
if [ -n "${END_DATE:-}" ]; then
    DATE_FLAG="$DATE_FLAG --end-date $END_DATE"
fi

# ── Route to selected pipeline ─────────────────────────────────────────────────
if [ "$PIPELINE" = "netcdf_datacube" ]; then

    NO_MERGE_SAME_CRS_FLAG=""
    if [ "${MERGE_SAME_CRS:-true}" = false ]; then
        NO_MERGE_SAME_CRS_FLAG="--no-merge-same-crs"
    fi

    NO_MERGE_CROSS_CRS_FLAG=""
    if [ "${MERGE_CROSS_CRS:-true}" = false ]; then
        NO_MERGE_CROSS_CRS_FLAG="--no-merge-cross-crs"
    fi

    python src/netcdf_datacube_extract.py \
        --netcdf-dir   "$NETCDF_DIR" \
        --vi           $VI \
        $SHAPEFILE_FLAG \
        $SHAPEFILE_FIELD_FLAG \
        --valid-range-ndvi="$VALID_RANGE_NDVI" \
        --valid-range-evi2="$VALID_RANGE_EVI2" \
        --valid-range-nirv="$VALID_RANGE_NIRV" \
        --output-dir   "$NETCDF_DATACUBE_OUTPUT_DIR" \
        --workers      "$WORKERS" \
        $DATE_FLAG \
        $NO_MERGE_SAME_CRS_FLAG \
        $NO_MERGE_CROSS_CRS_FLAG

elif [ "$PIPELINE" = "phenology" ]; then

    # ── Phenology-specific flag assembly ──────────────────────────────────────
    METRICS_FLAG=""
    if [ "$COMPUTE_METRICS" = true ]; then
        METRICS_FLAG="--metrics"
    fi

    NO_OBS_CSV_FLAG=""
    if [ "${SAVE_OBSERVATIONS_CSV:-true}" = false ]; then
        NO_OBS_CSV_FLAG="--no-observations-csv"
    fi

    NO_COMBINED_FLAG=""
    if [ "${SAVE_COMBINED_OUTPUTS:-true}" = false ]; then
        NO_COMBINED_FLAG="--no-combined-outputs"
    fi

    NO_PLOT_ANNUAL_FLAG=""
    if [ "${PLOT_ANNUAL:-true}" = false ]; then
        NO_PLOT_ANNUAL_FLAG="--no-plot-annual"
    fi

    NO_PLOT_TIMESERIES_FLAG=""
    if [ "${PLOT_TIMESERIES:-true}" = false ]; then
        NO_PLOT_TIMESERIES_FLAG="--no-plot-timeseries"
    fi

    NO_PLOT_ANOMALY_FLAG=""
    if [ "${PLOT_ANOMALY:-true}" = false ]; then
        NO_PLOT_ANOMALY_FLAG="--no-plot-anomaly"
    fi

    NO_PLOT_MULTI_VI_FLAG=""
    if [ "${PLOT_MULTI_VI:-true}" = false ]; then
        NO_PLOT_MULTI_VI_FLAG="--no-plot-multi-vi"
    fi

    SAMPLE_PIXELS_FLAG=""
    if [ -n "${SAMPLE_PIXELS:-}" ]; then
        SAMPLE_PIXELS_FLAG="--sample-pixels $SAMPLE_PIXELS"
    fi

    RANDOM_SEED_FLAG=""
    if [ -n "${RANDOM_SEED:-}" ]; then
        RANDOM_SEED_FLAG="--random-seed $RANDOM_SEED"
    fi

    MIN_NDVI_MEAN_FLAG=""
    if [ -n "${MIN_NDVI_MEAN:-}" ]; then
        MIN_NDVI_MEAN_FLAG="--min-ndvi-mean $MIN_NDVI_MEAN"
    fi

    MIN_QUALITY_FRAC_FLAG=""
    if [ -n "${MIN_QUALITY_FRAC:-}" ]; then
        MIN_QUALITY_FRAC_FLAG="--min-quality-frac $MIN_QUALITY_FRAC"
    fi

    USE_MEDIAN_FLAG=""
    if [ "${USE_MEDIAN:-false}" = true ]; then
        USE_MEDIAN_FLAG="--use-median"
    fi

    # Common flags passed to vi_phenology.py regardless of input mode.
    _PHENO_COMMON_FLAGS=(
        --vi               $VI
        --valid-range-ndvi="$VALID_RANGE_NDVI"
        --valid-range-evi2="$VALID_RANGE_EVI2"
        --valid-range-nirv="$VALID_RANGE_NIRV"
        --output-dir       "$PHENOLOGY_OUTPUT_DIR"
        --smooth-method    "$SMOOTH_METHOD"
        --smooth-window    "$SMOOTH_WINDOW"
        --smooth-polyorder "$SMOOTH_POLYORDER"
        --smooth-lambda    "$SMOOTH_LAMBDA"
        $METRICS_FLAG
        --sos-threshold    "$SOS_THRESHOLD"
        --year-start-doy   "$YEAR_START_DOY"
        --peak-prominence  "$PEAK_PROMINENCE"
        --peak-min-distance "$PEAK_MIN_DISTANCE"
        --min-valid-obs    "$MIN_VALID_OBS"
        --min-valid-obs-per-year "$MIN_VALID_OBS_PER_YEAR"
        $SAMPLE_PIXELS_FLAG
        $RANDOM_SEED_FLAG
        $MIN_NDVI_MEAN_FLAG
        $MIN_QUALITY_FRAC_FLAG
        $USE_MEDIAN_FLAG
        --plot-style       "$PLOT_STYLE"
        --plot-format      $PLOT_FORMAT
        --workers          "$WORKERS"
        $DATE_FLAG
        $NO_OBS_CSV_FLAG
        $NO_COMBINED_FLAG
        $NO_PLOT_ANNUAL_FLAG
        $NO_PLOT_TIMESERIES_FLAG
        $NO_PLOT_ANOMALY_FLAG
        $NO_PLOT_MULTI_VI_FLAG
    )

    if [ -n "${PHENOLOGY_INPUT_DATACUBES:-}" ]; then
        # Datacube input mode: bypass tile discovery and shapefile handling.
        # If PHENOLOGY_INPUT_DATACUBES is a directory, find all *_datacube.nc files within it.
        _DC_FILES=()
        if [ -d "${PHENOLOGY_INPUT_DATACUBES}" ]; then
            while IFS= read -r _f; do
                _DC_FILES+=("$_f")
            done < <(find "${PHENOLOGY_INPUT_DATACUBES}" -name "*_datacube.nc" | sort)
            if [ ${#_DC_FILES[@]} -eq 0 ]; then
                echo "ERROR: No *_datacube.nc files found in: ${PHENOLOGY_INPUT_DATACUBES}" >&2
                exit 1
            fi
            echo "Found ${#_DC_FILES[@]} datacube(s) in ${PHENOLOGY_INPUT_DATACUBES}"
        else
            # Space-separated file paths.
            read -ra _DC_FILES <<< "${PHENOLOGY_INPUT_DATACUBES}"
        fi

        python src/vi_phenology.py \
            --input-datacubes "${_DC_FILES[@]}" \
            "${_PHENO_COMMON_FLAGS[@]}"
    else
        # Standard mode: discover and clip source VI NetCDF tiles.
        python src/vi_phenology.py \
            --netcdf-dir    "$NETCDF_DIR" \
            $SHAPEFILE_FLAG \
            $SHAPEFILE_FIELD_FLAG \
            "${_PHENO_COMMON_FLAGS[@]}"
    fi

elif [ "$PIPELINE" = "pixel_phenology" ]; then

    if [ -z "${PIXEL_INPUT_DATACUBES:-}" ]; then
        echo "ERROR: PIXEL_INPUT_DATACUBES must be set for the pixel_phenology pipeline." >&2
        exit 1
    fi

    # Resolve PIXEL_INPUT_DATACUBES: directory → find all *_datacube.nc recursively;
    # otherwise treat as space-separated file paths.
    _PIXEL_DC_FILES=()
    if [ -d "${PIXEL_INPUT_DATACUBES}" ]; then
        while IFS= read -r _f; do
            _PIXEL_DC_FILES+=("$_f")
        done < <(find "${PIXEL_INPUT_DATACUBES}" -name "*_datacube.nc" | sort)
        if [ ${#_PIXEL_DC_FILES[@]} -eq 0 ]; then
            echo "ERROR: No *_datacube.nc files found in: ${PIXEL_INPUT_DATACUBES}" >&2
            exit 1
        fi
        echo "Found ${#_PIXEL_DC_FILES[@]} datacube(s) in ${PIXEL_INPUT_DATACUBES}"
    else
        read -ra _PIXEL_DC_FILES <<< "${PIXEL_INPUT_DATACUBES}"
    fi

    _NO_OVERVIEW_FLAG=""
    if [ "${PIXEL_OVERVIEW_FIGURE:-true}" = false ]; then
        _NO_OVERVIEW_FLAG="--no-overview-figure"
    fi

    _NO_OVERVIEW_HTML_FLAG=""
    if [ "${PIXEL_OVERVIEW_HTML:-true}" = false ]; then
        _NO_OVERVIEW_HTML_FLAG="--no-overview-html"
    fi

    python src/pixel_phenology_extract.py \
        --input-datacubes  "${_PIXEL_DC_FILES[@]}" \
        --output-dir       "$PIXEL_OUTPUT_DIR" \
        --smooth-lambda    "$PIXEL_SMOOTH_LAMBDA" \
        --min-valid-obs    "$PIXEL_MIN_VALID_OBS" \
        --min-valid-obs-per-year "$PIXEL_MIN_VALID_OBS_PER_YEAR" \
        --peak-prominence  "$PIXEL_PEAK_PROMINENCE" \
        --peak-min-distance "$PIXEL_PEAK_MIN_DISTANCE" \
        --season-threshold "$PIXEL_SEASON_THRESHOLD" \
        --valid-range-ndvi="$VALID_RANGE_NDVI" \
        --valid-range-evi2="$VALID_RANGE_EVI2" \
        --valid-range-nirv="$VALID_RANGE_NIRV" \
        --workers          "$WORKERS" \
        $_NO_OVERVIEW_FLAG \
        $_NO_OVERVIEW_HTML_FLAG \
        $DATE_FLAG

elif [ "$PIPELINE" = "datacube_to_geotiff" ]; then

    if [ -z "${GEOTIFF_INPUT_DATACUBES:-}" ]; then
        echo "ERROR: GEOTIFF_INPUT_DATACUBES must be set for the datacube_to_geotiff pipeline." >&2
        exit 1
    fi

    # Resolve GEOTIFF_INPUT_DATACUBES: directory → find all *_datacube.nc recursively;
    # otherwise treat as space-separated file paths.
    _GTIFF_DC_FILES=()
    if [ -d "${GEOTIFF_INPUT_DATACUBES}" ]; then
        while IFS= read -r _f; do
            _GTIFF_DC_FILES+=("$_f")
        done < <(find "${GEOTIFF_INPUT_DATACUBES}" -name "*_datacube.nc" | sort)
        if [ ${#_GTIFF_DC_FILES[@]} -eq 0 ]; then
            echo "ERROR: No *_datacube.nc files found in: ${GEOTIFF_INPUT_DATACUBES}" >&2
            exit 1
        fi
        echo "Found ${#_GTIFF_DC_FILES[@]} datacube(s) in ${GEOTIFF_INPUT_DATACUBES}"
    else
        read -ra _GTIFF_DC_FILES <<< "${GEOTIFF_INPUT_DATACUBES}"
    fi

    _SKIP_YEAR_FLAG=""
    if [ "${GEOTIFF_PER_YEAR:-true}" = false ]; then
        _SKIP_YEAR_FLAG="--skip-per-year"
    fi

    _SKIP_MONTH_FLAG=""
    if [ "${GEOTIFF_PER_MONTH:-true}" = false ]; then
        _SKIP_MONTH_FLAG="--skip-per-month"
    fi

    _SKIP_DOY_FLAG=""
    if [ "${GEOTIFF_PER_DOY:-true}" = false ]; then
        _SKIP_DOY_FLAG="--skip-per-doy"
    fi

    python src/datacube_to_geotiff.py \
        --input-datacubes  "${_GTIFF_DC_FILES[@]}" \
        --output-dir       "$GEOTIFF_OUTPUT_DIR" \
        --valid-range-ndvi="$VALID_RANGE_NDVI" \
        --valid-range-evi2="$VALID_RANGE_EVI2" \
        --valid-range-nirv="$VALID_RANGE_NIRV" \
        --workers          "$WORKERS" \
        $_SKIP_YEAR_FLAG \
        $_SKIP_MONTH_FLAG \
        $_SKIP_DOY_FLAG \
        $DATE_FLAG

else
    echo "ERROR: Unknown PIPELINE value '${PIPELINE}'. Choose: phenology | netcdf_datacube | pixel_phenology | datacube_to_geotiff" >&2
    exit 1

fi
