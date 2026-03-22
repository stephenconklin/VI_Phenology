#!/usr/bin/env bash
# run_phenology.sh
# Edit the variables below to configure your VI Phenology run.
# Usage: ./run_phenology.sh

# ── Pipeline ───────────────────────────────────────────────────────────────────
# phenology        → ROI-mean time series, smoothing, metrics, plots
# netcdf_datacube  → CF-1.8 per-pixel datacubes clipped to polygon regions
# pixel_phenology  → per-pixel metric maps from existing datacubes (19 metrics)
# PIPELINE="phenology"
# PIPELINE="netcdf_datacube"
PIPELINE="pixel_phenology"

# ==============================================================================
# Shared inputs  (all three pipelines)
# ==============================================================================

# ── Output ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR="/Volumes/ConklinGeospatialData/Data/BioSCape_SA_LVIS/VI_Phenology"

# ── Valid ranges (min,max) ─────────────────────────────────────────────────────
VALID_RANGE_NDVI="-1,1"
VALID_RANGE_EVI2="-1,2"
VALID_RANGE_NIRV="-0.5,1"

# ── Parallelization ────────────────────────────────────────────────────────────
WORKERS=8    # Parallel worker processes for tile extraction; set to 1 to disable

# ── Date range filtering (optional) ────────────────────────────────────────────
# Limit processing to a specific time window. Leave commented out (or empty) to
# process all available dates. Format: YYYY-MM-DD
# START_DATE="2025-01-01"
# END_DATE="2025-12-31"

# ==============================================================================
# phenology + netcdf_datacube pipeline inputs  (not used by pixel_phenology)
# ============================================================================== 
#      NOTE: "Datacube input mode" under the phenology pipeline settings is an
#             optional alternative to NETCDF_DIR + SHAPEFILE when running only
#             the phenology pipeline. "Datacube input mode" is also preferable
#             when no shapefile input is possible, and when the entire netCDF
#             input file should be processed.
# ==============================================================================

# ── Source NetCDF directory ────────────────────────────────────────────────────
NETCDF_DIR="/Volumes/ConklinGeospatialData/Data/BioSCape_SA_LVIS/2_Interim/2_NetCDF"

# ── Vegetation index ───────────────────────────────────────────────────────────
# VI="NDVI EVI2 NIRv"                        # space-separated; choices: NDVI EVI2 NIRv
VI="NDVI"                        # space-separated; choices: NDVI EVI2 NIRv

# ── Shapefile(s) ──────────────────────────────────────────────────────────────
# Accepts any GeoPandas-readable vector format: .shp, .gpkg, .geojson, .kml, .fgb, .gdb, etc.
# For .shp, all sidecar files (.dbf, .prj, .shx) must be present alongside the .shp.
# To process multiple regions independently, list paths space-separated (one output set per region).
# Omit this variable (and --shapefile below) entirely to process the full NetCDF extent.
#
# Single region:
# SHAPEFILE="/path/to/roi.gpkg"
# Multiple regions (uncomment and adjust; comment out the single-region line above):
SHAPEFILE="/Users/stephenconklin/Documents/ConklinGeospatial/Projects/UMCES/BioSCape_SouthAfrica/BioSCape_QGIS/shapefiles/LVIS_flightboxes_final.shp"

# ── Shapefile attribute field (optional) ──────────────────────────────────────
# Provide one field name per shapefile (space-separated, same positional order as SHAPEFILE).
# Each unique value in the field produces an independent output named after that value.
# Use 'none' to dissolve a specific shapefile instead of splitting it by field.
# Count MUST match the number of shapefile paths above — any mismatch is a hard error.
# Leave commented out entirely to dissolve all shapefiles into one region each (default).
#
# Single shapefile split by field:
SHAPEFILE_FIELD="box_nr"
#
# Two shapefiles: split first by field, dissolve second:
# SHAPEFILE_FIELD="box_nr none"
#
# Two shapefiles: split first by field, split second by field:
# SHAPEFILE_FIELD="box_nr tile_id"

# ==============================================================================
# netcdf_datacube pipeline options  (only used when PIPELINE="netcdf_datacube")
# ==============================================================================

# ── Tile merge options ─────────────────────────────────────────────────────────
# Same-CRS tiles (all in the same UTM zone) share an identical 30-m pixel grid
# and can be mosaiced without resampling. First-wins strategy for the MGRS
# overlap zone (~163 px). Time union covers all tiles' acquisition dates.
MERGE_SAME_CRS=true    # true = merge into one datacube per region (default)
                       # false = one file per tile, native CRS

# Cross-CRS tiles (polygon spans a UTM zone boundary) require reprojection to a
# common CRS before merging. Minority tiles are bilinearly reprojected to the
# dominant CRS (the CRS covering the most pixels in the polygon). The output
# file's global attributes document the target CRS and resampling method.
MERGE_CROSS_CRS=true   # true = reproject + merge into one datacube (default)
                       # false = one file per tile, native CRS, no reprojection

# ==============================================================================
# Phenology pipeline options  (only used when PIPELINE="phenology")
# ==============================================================================

# ── Smoothing ──────────────────────────────────────────────────────────────────
SMOOTH_METHOD="whittaker"             # savgol | loess | linear | harmonic | whittaker | none
SMOOTH_LAMBDA=100                     # Whittaker only: smoothing strength (10–1000; higher = smoother)
SMOOTH_WINDOW=15                      # days (savgol, loess)
SMOOTH_POLYORDER=3                    # savgol only

# ── Metrics ────────────────────────────────────────────────────────────────────
COMPUTE_METRICS=false                  # true | false
SOS_THRESHOLD=0.20                    # fraction of amplitude for SOS/EOS
YEAR_START_DOY=1                      # 1 = Jan 1 — correct for Cape fynbos (winter-rainfall;
                                      # peak green ~Jul, minimum ~Jan)
                                      # Use 182 (Jul 1) for summer-rainfall biomes (Highveld,
                                      # Savanna) where peak is Dec-Jan and minimum is Jul

# Bimodality detection (extended metrics, only used with COMPUTE_METRICS=true)
# Floor and ceiling NDVI are derived directly from the annual smooth curve —
# no seasonal DOY windows required.
PEAK_PROMINENCE=0.05                  # min NDVI prominence for a peak to count as bimodal
                                      # (increase to 0.08–0.10 for noisier/semi-arid regions)
PEAK_MIN_DISTANCE=45                  # min separation (days) between detected peaks

# ── Observation count thresholds ───────────────────────────────────────────────
MIN_VALID_OBS=20                      # min valid obs over full record; fewer → skip region entirely
MIN_VALID_OBS_PER_YEAR=5             # min valid obs per annual window; fewer → skip that year's metrics
                                      # (5 is permissive; increase to 8–10 for stricter quality control)

# ── Datacube input mode (optional alternative to NETCDF_DIR + SHAPEFILE) ───────
# When set, reads pre-clipped datacubes from the netcdf_datacube pipeline instead
# of re-clipping source tiles.  Faster for repeated runs with different smoothing
# settings, thresholds, or plot styles — no tile discovery or parallel clip needed.
# VI and region_label are inferred from each filename ({VI}_{region_label}_datacube.nc).
# NETCDF_DIR, SHAPEFILE, and SHAPEFILE_FIELD are ignored when INPUT_DATACUBES is set.
#
# Accepts either:
#   A directory — all *_datacube.nc files found recursively within it are used.
#                 Set to the shapefile subfolder produced by netcdf_datacube, e.g.:
#                 INPUT_DATACUBES="${OUTPUT_DIR}/LVIS_flightboxes_final"
#   File path(s) — space-separated list of individual datacube files.
#                 INPUT_DATACUBES="/path/to/NDVI_G5_1_datacube.nc /path/to/NDVI_G5_12_datacube.nc"
#
INPUT_DATACUBES="/Volumes/ConklinGeospatialData/Data/Durango_HLS_VI/2_Interim/2_NetCDF/NDVI_T13SBB_datacube.nc"

# ── Pixel sampling (optional) ──────────────────────────────────────────────────
# Randomly sample N pixels per region and use them consistently across the full
# time series, eliminating date-to-date variation caused by cloud masking.
# Pixels are selected once (Phase A) then applied to every time step (Phase B).
#
# Leave any of these commented out to disable that filter and use all valid pixels.
#
# SAMPLE_PIXELS=200          # N random pixels per region; comment out = use all pixels
# RANDOM_SEED=42             # reproducibility seed; comment out = random each run
# MIN_NDVI_MEAN=0.40         # exclude pixels below this temporal mean NDVI; comment out = no filter
# MIN_QUALITY_FRAC=0.20      # min fraction of valid timesteps to be eligible; comment out = no filter
                               # e.g. 0.20 excludes persistently cloud-covered pixels

# ── Data outputs ───────────────────────────────────────────────────────────────
SAVE_OBSERVATIONS_CSV=false   # Per-region observations-only CSV (vi_count > 0 rows only)
SAVE_COMBINED_OUTPUTS=false   # Combined shapefile observations CSV (all regions in one file)

# ── Plot outputs ───────────────────────────────────────────────────────────────
PLOT_ANNUAL=true              # Annual DOY overlay (one curve per year + multi-year mean)
PLOT_TIMESERIES=true          # Full calendar time-series
PLOT_ANOMALY=false             # Anomaly (departure from multi-year mean)
PLOT_MULTI_VI=true            # Multi-VI comparison panel (requires >1 VI)
PLOT_STYLE="combined"         # raw | smooth | combined
PLOT_FORMAT="png html"        # space-separated; png and/or html

# ==============================================================================
# pixel_phenology pipeline options  (only used when PIPELINE="pixel_phenology")
# ==============================================================================
# Reads per-pixel datacubes produced by the netcdf_datacube pipeline and writes
# one CF-1.8 NetCDF per (VI, region) containing 19 phenological metric bands,
# plus a summary CSV with spatial statistics per metric.

# Input: datacube(s) produced by PIPELINE="netcdf_datacube".
# Accepts either:
#   A directory — all *_datacube.nc files found recursively within it are used.
#                 Set to the shapefile subfolder produced by netcdf_datacube, e.g.:
#                 PIXEL_INPUT_DATACUBES="${OUTPUT_DIR}/LVIS_flightboxes_final"
#   File path(s) — space-separated list of individual datacube files.
#                 PIXEL_INPUT_DATACUBES="/path/to/NDVI_G5_1_datacube.nc /path/to/NDVI_G5_12_datacube.nc"
#
# PIXEL_INPUT_DATACUBES="/path/to/NDVI_MyRegion_datacube.nc"
PIXEL_INPUT_DATACUBES="/Volumes/ConklinGeospatialData/Data/BioSCape_SA_LVIS/VI_Phenology/netcdf_datacube"              # required — set before running pixel_phenology

# Output directory for pixel metric files (created if it does not exist).
# PIXEL_OUTPUT_DIR="${OUTPUT_DIR}/pixel_metrics"
PIXEL_OUTPUT_DIR="${OUTPUT_DIR}/pixel_metrics"

PIXEL_SMOOTH_LAMBDA=100               # Whittaker smoothing strength (10–1000)
PIXEL_MIN_VALID_OBS=20                # min valid obs over full record; fewer → pixel set to NaN
PIXEL_MIN_VALID_OBS_PER_YEAR=5       # min valid obs per annual window; fewer → skip that year
                                      # (5 is permissive; increase to 8–10 for stricter quality control)
PIXEL_PEAK_PROMINENCE=0.05            # min NDVI prominence for bimodality peak detection
PIXEL_PEAK_MIN_DISTANCE=45            # min separation (days) between detected peaks
PIXEL_SEASON_THRESHOLD=0.20           # amplitude fraction for season-length calculation
# Set to true to generate the 19-panel print-quality overview PNG (default: true = generate it).
PIXEL_OVERVIEW_FIGURE=true
# Set to true to generate the interactive Plotly HTML overview (default: true = generate it).
PIXEL_OVERVIEW_HTML=true

# ==============================================================================
# Build and run
# ==============================================================================
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
        --output-dir   "$OUTPUT_DIR" \
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

    # Common flags passed to vi_phenology.py regardless of input mode.
    _PHENO_COMMON_FLAGS=(
        --vi               $VI
        --valid-range-ndvi="$VALID_RANGE_NDVI"
        --valid-range-evi2="$VALID_RANGE_EVI2"
        --valid-range-nirv="$VALID_RANGE_NIRV"
        --output-dir       "$OUTPUT_DIR"
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

    if [ -n "${INPUT_DATACUBES:-}" ]; then
        # Datacube input mode: bypass tile discovery and shapefile handling.
        # If INPUT_DATACUBES is a directory, find all *_datacube.nc files within it.
        _DC_FILES=()
        if [ -d "${INPUT_DATACUBES}" ]; then
            while IFS= read -r _f; do
                _DC_FILES+=("$_f")
            done < <(find "${INPUT_DATACUBES}" -name "*_datacube.nc" | sort)
            if [ ${#_DC_FILES[@]} -eq 0 ]; then
                echo "ERROR: No *_datacube.nc files found in: ${INPUT_DATACUBES}" >&2
                exit 1
            fi
            echo "Found ${#_DC_FILES[@]} datacube(s) in ${INPUT_DATACUBES}"
        else
            # Space-separated file paths.
            read -ra _DC_FILES <<< "${INPUT_DATACUBES}"
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
    # otherwise treat as space-separated file paths (same logic as INPUT_DATACUBES).
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
        # Space-separated file paths.
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

else
    echo "ERROR: Unknown PIPELINE value '${PIPELINE}'. Choose: phenology | netcdf_datacube | pixel_phenology" >&2
    exit 1

fi
