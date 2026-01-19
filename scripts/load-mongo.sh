#!/usr/bin/env bash
set -euo pipefail

# scripts/load-mongo.sh
#
# Load generated CarrierOps NDJSON datasets into MongoDB using mongoimport,
# then create indexes using mongosh.
#
# Default DB names:
#   carrierops_normalized  (seed/mongo_normalized/*.ndjson)
#   carrierops_optimized   (seed/mongo_optimized/*.ndjson)

SHAPE="both"                  # normalized|optimized|both
DROP="false"
URI=""
SEED_DIR="seed"
DB_NORMALIZED="carrierops_normalized"
DB_OPTIMIZED="carrierops_optimized"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --uri) URI="${2:?missing value for --uri}"; shift 2 ;;
    --shape) SHAPE="${2:?missing value for --shape}"; shift 2 ;;
    --drop) DROP="true"; shift 1 ;;
    --seed-dir) SEED_DIR="${2:?missing value for --seed-dir}"; shift 2 ;;
    --db-normalized) DB_NORMALIZED="${2:?missing value for --db-normalized}"; shift 2 ;;
    --db-optimized) DB_OPTIMIZED="${2:?missing value for --db-optimized}"; shift 2 ;;
    -h|--help)
      sed -n '1,140p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      echo "Run: $0 --help"
      exit 1
      ;;
  esac
done

if [[ -z "${URI}" ]]; then
  echo "ERROR: --uri is required"
  echo "Example: ./scripts/load-mongo.sh --uri \"mongodb://localhost:27017\" --shape both --drop"
  exit 1
fi

case "${SHAPE}" in
  normalized|optimized|both) ;;
  *) echo "ERROR: --shape must be normalized|optimized|both"; exit 1 ;;
esac

if ! command -v mongoimport >/dev/null 2>&1; then
  echo "ERROR: mongoimport not found on PATH (MongoDB Database Tools required)"
  exit 1
fi

if ! command -v mongosh >/dev/null 2>&1; then
  echo "ERROR: mongosh not found on PATH"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIR_NORMALIZED="${ROOT_DIR}/${SEED_DIR}/mongo_normalized"
DIR_OPTIMIZED="${ROOT_DIR}/${SEED_DIR}/mongo_optimized"
INDEX_NORM="${ROOT_DIR}/mongo/indexes.normalized.js"
INDEX_OPT="${ROOT_DIR}/mongo/indexes.optimized.js"

# Build a proper per-db URI by inserting "/<db>" before any "?query"
# Examples:
#   mongodb://u:p@h:27017/?authSource=admin  + db -> mongodb://u:p@h:27017/<db>?authSource=admin
#   mongodb://u:p@h:27017                   + db -> mongodb://u:p@h:27017/<db>
uri_with_db() {
  local base="$1"
  local db="$2"

  local noQuery="$base"
  local query=""

  if [[ "$base" == *"?"* ]]; then
    noQuery="${base%%\?*}"
    query="${base#*\?}"
  fi

  # Strip trailing slash (so we can safely append /db)
  noQuery="${noQuery%/}"

  if [[ -n "$query" ]]; then
    echo "${noQuery}/${db}?${query}"
  else
    echo "${noQuery}/${db}"
  fi
}

import_dir_into_db() {
  local dir="$1"
  local dbname="$2"

  if [[ ! -d "${dir}" ]]; then
    echo "ERROR: seed directory not found: ${dir}"
    echo "Did you run: ./scripts/gen-dataset.sh ... ?"
    exit 1
  fi

  echo ""
  echo "==> Importing NDJSON from:"
  echo "    ${dir}"
  echo "    into DB: ${dbname}"
  echo ""

  local collections=(
    accounts
    subscribers
    subscriber_profiles
    devices
    device_events
    orders
    order_items
    features
    subscriber_features
    subscriber_feature_state
    ticket_status_codes
    tickets
    notes
    org_units
    plans
    regions
    device_classes
    rates
    usage_records
  )

  for c in "${collections[@]}"; do
    local f="${dir}/${c}.ndjson"
    if [[ -f "${f}" ]]; then
      echo "  -> mongoimport ${dbname}.${c}  (${f})"
      mongoimport \
        --uri "${URI}" \
        --db "${dbname}" \
        --collection "${c}" \
        --file "${f}" \
        --type json \
        $( [[ "${DROP}" == "true" ]] && echo "--drop" ) \
        --quiet
    fi
  done
}

create_indexes() {
  local dbname="$1"
  local indexFile="$2"

  if [[ ! -f "${indexFile}" ]]; then
    echo "ERROR: index file not found: ${indexFile}"
    exit 1
  fi

  local dbUri
  dbUri="$(uri_with_db "${URI}" "${dbname}")"

  echo ""
  echo "==> Creating indexes for DB: ${dbname}"
  mongosh "${dbUri}" --file "${indexFile}" --quiet
}

# ----------------- run -----------------
if [[ "${SHAPE}" == "normalized" || "${SHAPE}" == "both" ]]; then
  import_dir_into_db "${DIR_NORMALIZED}" "${DB_NORMALIZED}"
  create_indexes "${DB_NORMALIZED}" "${INDEX_NORM}"
fi

if [[ "${SHAPE}" == "optimized" || "${SHAPE}" == "both" ]]; then
  import_dir_into_db "${DIR_OPTIMIZED}" "${DB_OPTIMIZED}"
  create_indexes "${DB_OPTIMIZED}" "${INDEX_OPT}"
fi

echo ""
echo "✅ MongoDB load complete."
if [[ "${SHAPE}" == "normalized" || "${SHAPE}" == "both" ]]; then
  echo "   - Normalized DB: ${DB_NORMALIZED}"
fi
if [[ "${SHAPE}" == "optimized" || "${SHAPE}" == "both" ]]; then
  echo "   - Optimized DB:  ${DB_OPTIMIZED}"
fi
