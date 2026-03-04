#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RAW_DIR="$ROOT_DIR/data/raw"

DATASETS=(
  "ieee_cis"
  "creditcard_fraud"
  "paysim"
  "ibm_aml_data"
  "banksim"
  "ibm_amlsim"
  "elliptic"
)

echo "Dataset layout check"
echo "Root: $RAW_DIR"
echo

missing=0

for ds in "${DATASETS[@]}"; do
  original="$RAW_DIR/$ds/original"
  extracted="$RAW_DIR/$ds/extracted"

  if [[ ! -d "$original" || ! -d "$extracted" ]]; then
    echo "[MISSING DIR] $ds"
    missing=$((missing + 1))
    continue
  fi

  orig_count="$(find "$original" -type f | wc -l | tr -d ' ')"
  ext_count="$(find "$extracted" -type f | wc -l | tr -d ' ')"

  if [[ "$orig_count" -eq 0 && "$ext_count" -eq 0 ]]; then
    echo "[EMPTY] $ds (original:0, extracted:0)"
  else
    echo "[OK] $ds (original:$orig_count, extracted:$ext_count)"
  fi
done

echo
if [[ "$missing" -gt 0 ]]; then
  echo "Missing dataset directories: $missing"
  exit 1
fi

echo "Layout directories are ready."
