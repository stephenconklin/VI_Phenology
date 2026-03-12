#!/usr/bin/env bash
# run_phenology.sh
# Edit the variables below to configure your VI Phenology run.
# Usage: ./run_phenology.sh

# ── Pipeline ───────────────────────────────────────────────────────────────────
# phenology     → VI phenology analysis (Parquet, metrics, plots)
# netcdf_datacube → CF-1.8 netCDF datacube per polygon per tile (no aggregation)
# PIPELINE="phenology"
PIPELINE="netcdf_datacube"

# ── Inputs ────────────────────────────────────────────────────────────────────
NETCDF_DIR="/Volumes/ConklinGeospatialData/Data/BioSCape_SA/2_Interim/2_NetCDF"
# VI="NDVI EVI2 NIRv"                        # space-separated; choices: NDVI EVI2 NIRv
VI="NDVI"                        # space-separated; choices: NDVI EVI2 NIRv
# ── Output ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR="/Volumes/ConklinGeospatialData/Data/BioSCape_SA/VI_Phenology"

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

# ── Logging ────────────────────────────────────────────────────────────────────
NO_LOGFILE=false              # true = disable log file in OUTPUT_DIR; false = write log file

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
SMOOTH_METHOD="savgol"                # savgol | loess | linear | harmonic | none
SMOOTH_WINDOW=15                      # days
SMOOTH_POLYORDER=3                    # savgol only

# ── Metrics ────────────────────────────────────────────────────────────────────
COMPUTE_METRICS=true                  # true | false
SOS_THRESHOLD=0.20                    # fraction of amplitude for SOS/EOS
YEAR_START_DOY=1                      # 1 = Jan 1 — correct for Cape fynbos (winter-rainfall;
                                      # peak green ~Jul, minimum ~Jan)
                                      # Use 182 (Jul 1) for summer-rainfall biomes (Highveld,
                                      # Savanna) where peak is Dec-Jan and minimum is Jul

# ── Data outputs ───────────────────────────────────────────────────────────────
SAVE_PARQUET=true             # Per-region Parquet time series (vi_raw, vi_smooth, etc.)
SAVE_OBSERVATIONS_CSV=true    # Per-region observations-only CSV (vi_count > 0 rows only)
SAVE_COMBINED_OUTPUTS=true    # Combined shapefile Parquet + observations CSV

# ── Plot outputs ───────────────────────────────────────────────────────────────
PLOT_ANNUAL=true              # Annual DOY overlay (one curve per year + multi-year mean)
PLOT_TIMESERIES=true          # Full calendar time-series
PLOT_ANOMALY=true             # Anomaly (departure from multi-year mean)
PLOT_MULTI_VI=true            # Multi-VI comparison panel (requires >1 VI)
PLOT_STYLE="combined"         # raw | smooth | combined
PLOT_FORMAT="png html"        # space-separated; png and/or html

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

NO_LOGFILE_FLAG=""
if [ "${NO_LOGFILE:-false}" = true ]; then
    NO_LOGFILE_FLAG="--no-logfile"
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
        $NO_LOGFILE_FLAG \
        $NO_MERGE_SAME_CRS_FLAG \
        $NO_MERGE_CROSS_CRS_FLAG

else

    # ── Phenology-specific flag assembly ──────────────────────────────────────
    METRICS_FLAG=""
    if [ "$COMPUTE_METRICS" = true ]; then
        METRICS_FLAG="--metrics"
    fi

    NO_PARQUET_FLAG=""
    if [ "${SAVE_PARQUET:-true}" = false ]; then
        NO_PARQUET_FLAG="--no-parquet"
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

    python src/vi_phenology.py \
        --netcdf-dir       "$NETCDF_DIR" \
        --vi               $VI \
        $SHAPEFILE_FLAG \
        $SHAPEFILE_FIELD_FLAG \
        --valid-range-ndvi="$VALID_RANGE_NDVI" \
        --valid-range-evi2="$VALID_RANGE_EVI2" \
        --valid-range-nirv="$VALID_RANGE_NIRV" \
        --output-dir       "$OUTPUT_DIR" \
        --smooth-method    "$SMOOTH_METHOD" \
        --smooth-window    "$SMOOTH_WINDOW" \
        --smooth-polyorder "$SMOOTH_POLYORDER" \
        $METRICS_FLAG \
        --sos-threshold    "$SOS_THRESHOLD" \
        --year-start-doy   "$YEAR_START_DOY" \
        --plot-style       "$PLOT_STYLE" \
        --plot-format      $PLOT_FORMAT \
        --workers          "$WORKERS" \
        $DATE_FLAG \
        $NO_LOGFILE_FLAG \
        $NO_PARQUET_FLAG \
        $NO_OBS_CSV_FLAG \
        $NO_COMBINED_FLAG \
        $NO_PLOT_ANNUAL_FLAG \
        $NO_PLOT_TIMESERIES_FLAG \
        $NO_PLOT_ANOMALY_FLAG \
        $NO_PLOT_MULTI_VI_FLAG

fi
