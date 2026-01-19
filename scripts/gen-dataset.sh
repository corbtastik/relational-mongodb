#!/usr/bin/env bash
set -euo pipefail

# Generate deterministic CarrierOps datasets (canonical + mongo projections + postgres CSV).
#
# Usage:
#   ./scripts/gen-dataset.sh --size S --seed 42
#   ./scripts/gen-dataset.sh --size M --seed 42 --overwrite
#   ./scripts/gen-dataset.sh --size S --seed 42 --only canonical
#   ./scripts/gen-dataset.sh --size S --seed 42 --out seed --overwrite
#
# Notes:
# - Determinism comes from the Node generator, not bash.
# - Run from repo root.

SIZE="S"
SEED="42"
OUT_DIR="seed"
ONLY="all"        # canonical|mongo_normalized|mongo_optimized|postgres|all
OVERWRITE="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --size) SIZE="${2:?missing value for --size}"; shift 2 ;;
    --seed) SEED="${2:?missing value for --seed}"; shift 2 ;;
    --out) OUT_DIR="${2:?missing value for --out}"; shift 2 ;;
    --only) ONLY="${2:?missing value for --only}"; shift 2 ;;
    --overwrite) OVERWRITE="true"; shift 1 ;;
    -h|--help)
      sed -n '1,80p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      echo "Run: $0 --help"
      exit 1
      ;;
  esac
done

case "${SIZE}" in
  S|M|L) ;;
  *) echo "Invalid --size '${SIZE}'. Use S|M|L."; exit 1 ;;
esac

case "${ONLY}" in
  canonical|mongo_normalized|mongo_optimized|postgres|all) ;;
  *) echo "Invalid --only '${ONLY}'. Use canonical|mongo_normalized|mongo_optimized|postgres|all."; exit 1 ;;
esac

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GEN="${ROOT_DIR}/tools/generate-dataset.js"

if [[ ! -f "${GEN}" ]]; then
  echo "Missing generator: ${GEN}"
  exit 1
fi

if ! command -v node >/dev/null 2>&1; then
  echo "node is required but not found on PATH"
  exit 1
fi

# Safety: refuse to overwrite unless requested
MANIFEST="${ROOT_DIR}/${OUT_DIR}/manifest.json"
if [[ -f "${MANIFEST}" && "${OVERWRITE}" != "true" ]]; then
  echo "Refusing to overwrite existing dataset at '${OUT_DIR}/' (manifest.json exists)."
  echo "Re-run with --overwrite to regenerate."
  exit 1
fi

node "${GEN}" \
  --size "${SIZE}" \
  --seed "${SEED}" \
  --out "${ROOT_DIR}/${OUT_DIR}" \
  --only "${ONLY}" \
  $( [[ "${OVERWRITE}" == "true" ]] && echo "--overwrite" )

echo "✅ Dataset generated in: ${OUT_DIR}/"
case "${ONLY}" in
  canonical)
    echo "   - ${OUT_DIR}/canonical/*.ndjson"
    ;;
  mongo_normalized)
    echo "   - ${OUT_DIR}/mongo_normalized/*.ndjson"
    ;;
  mongo_optimized)
    echo "   - ${OUT_DIR}/mongo_optimized/*.ndjson"
    ;;
  postgres)
    echo "   - ${OUT_DIR}/postgres/data/*.csv"
    ;;
  all)
    echo "   - ${OUT_DIR}/canonical/*.ndjson"
    echo "   - ${OUT_DIR}/mongo_normalized/*.ndjson"
    echo "   - ${OUT_DIR}/mongo_optimized/*.ndjson"
    echo "   - ${OUT_DIR}/postgres/data/*.csv"
    ;;
esac
echo "   - ${OUT_DIR}/manifest.json"
