#!/usr/bin/env bash
# run_phenology.sh
# Edit the variables below to configure your VI Phenology run.
# Usage: ./run_phenology.sh

# ── Inputs ────────────────────────────────────────────────────────────────────
NETCDF_DIR="/Volumes/ConklinGeospatialData/Data/BioSCape_SA/2_Interim/2_NetCDF"
VI="NDVI"                        # space-separated; choices: NDVI EVI2 NIRv
# 
# ── Output ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR="/Volumes/ConklinGeospatialData/Data/BioSCape_SA_Phenology_2"
# 
# ── Shapefile(s) ──────────────────────────────────────────────────────────────
# Accepts any GeoPandas-readable vector format: .shp, .gpkg, .geojson, .kml, .fgb, .gdb, etc.
# For .shp, all sidecar files (.dbf, .prj, .shx) must be present alongside the .shp.
# To process multiple regions independently, list paths space-separated (one output set per region).
# Omit this variable (and --shapefile below) entirely to process the full NetCDF extent.
#
# Single region:
# SHAPEFILE="/path/to/roi.gpkg"
SHAPEFILE="/Users/stephenconklin/Documents/ConklinGeospatial/Projects/UMCES/BioSCape_SouthAfrica/BioSCape_QGIS/shapefiles/LVIS_flightboxes_final.shp"
# Multiple regions (uncomment and adjust; comment out the single-region line above):
# SHAPEFILE="/path/to/region1.gpkg /path/to/region2.geojson"
#
# ── Shapefile attribute field (optional) ──────────────────────────────────────
# Provide one field name per shapefile (space-separated, same positional order as SHAPEFILE).
# Each unique value in the field produces an independent time series named after that value.
# Use 'none' to dissolve a specific shapefile instead of splitting it by field.
# Count MUST match the number of shapefile paths above — any mismatch is a hard error.
# Leave commented out entirely to dissolve all shapefiles into one region each (default).
#
# Single shapefile split by field:
# SHAPEFILE_FIELD="box_nr"
SHAPEFILE_FIELD="box_nr"
#
# Two shapefiles: split first by field, dissolve second:
# SHAPEFILE_FIELD="box_nr none"
# 
# Two shapefiles: split first by field, split second by field:
# SHAPEFILE_FIELD="box_nr tile_id"
# 
# ── Valid ranges (min,max) ─────────────────────────────────────────────────────
VALID_RANGE_NDVI="-1,1"
VALID_RANGE_EVI2="-1,2"
VALID_RANGE_NIRV="-0.5,1"

# ── Smoothing ──────────────────────────────────────────────────────────────────
SMOOTH_METHOD="savgol"                # savgol | loess | linear | harmonic | none
SMOOTH_WINDOW=15                      # days
SMOOTH_POLYORDER=3                    # savgol only

# ── Metrics ────────────────────────────────────────────────────────────────────
COMPUTE_METRICS=true                  # true | false
SOS_THRESHOLD=0.20                    # fraction of amplitude for SOS/EOS
YEAR_START_DOY=1                        # 1 = Jan 1 — correct for Cape fynbos (winter-rainfall;
                                      # peak green ~Jul, minimum ~Jan)
                                      # Use 182 (Jul 1) for summer-rainfall biomes (Highveld,
                                      # Savanna) where peak is Dec-Jan and minimum is Jul

# ── Spatial mode ───────────────────────────────────────────────────────────────
MODE="roi_mean"                       # roi_mean | per_pixel

# ── Plotting ───────────────────────────────────────────────────────────────────
PLOT_STYLE="combined"                 # raw | smooth | combined
PLOT_FORMAT="png html"                # space-separated; png and/or html

# ── Parallelization ────────────────────────────────────────────────────────────
WORKERS=8    # Parallel worker processes for tile extraction; set to 1 to disable

# ── Logging ────────────────────────────────────────────────────────────────────
NO_LOGFILE=false              # true = disable log file in OUTPUT_DIR; false = write log file

# ── Date range filtering (optional) ────────────────────────────────────────────
# Limit processing to a specific time window. Leave commented out (or empty) to
# process all available dates. Format: YYYY-MM-DD
# START_DATE="2025-01-01"
# END_DATE="2025-12-31"

# ── Build and run command ──────────────────────────────────────────────────────
set -euo pipefail

METRICS_FLAG=""
if [ "$COMPUTE_METRICS" = true ]; then
    METRICS_FLAG="--metrics"
fi

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

python vi_phenology.py \
    --netcdf-dir   "$NETCDF_DIR" \
    --vi           $VI \
    $SHAPEFILE_FLAG \
    $SHAPEFILE_FIELD_FLAG \
    --valid-range-ndvi="$VALID_RANGE_NDVI" \
    --valid-range-evi2="$VALID_RANGE_EVI2" \
    --valid-range-nirv="$VALID_RANGE_NIRV" \
    --output-dir   "$OUTPUT_DIR" \
    --smooth-method    "$SMOOTH_METHOD" \
    --smooth-window    "$SMOOTH_WINDOW" \
    --smooth-polyorder "$SMOOTH_POLYORDER" \
    $METRICS_FLAG \
    --sos-threshold    "$SOS_THRESHOLD" \
    --year-start-doy   "$YEAR_START_DOY" \
    --mode         "$MODE" \
    --plot-style   "$PLOT_STYLE" \
    --plot-format  $PLOT_FORMAT \
    --workers      "$WORKERS" \
    $DATE_FLAG \
    $NO_LOGFILE_FLAG
