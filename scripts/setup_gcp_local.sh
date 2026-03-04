#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SECRETS_DIR="$ROOT_DIR/.secrets"
TARGET_KEY="$SECRETS_DIR/gcp-service-account.json"
ENV_FILE="$ROOT_DIR/.env.local"

SOURCE_KEY="${1:-$HOME/Downloads/fraud-aml-graph.json}"
PROJECT_ID="${2:-fraud-aml-graph}"
BQ_DATASET="${3:-fraud_aml_graph_dev}"
BQ_LOCATION="${4:-EU}"

if [[ ! -f "$SOURCE_KEY" ]]; then
  echo "[ERROR] Source key not found: $SOURCE_KEY" >&2
  exit 1
fi

mkdir -p "$SECRETS_DIR"
cp "$SOURCE_KEY" "$TARGET_KEY"
chmod 600 "$TARGET_KEY"

cat > "$ENV_FILE" <<EOF
export GCP_PROJECT_ID="$PROJECT_ID"
export GOOGLE_APPLICATION_CREDENTIALS="$TARGET_KEY"
export BQ_DATASET="$BQ_DATASET"
export BQ_LOCATION="$BQ_LOCATION"
EOF
chmod 600 "$ENV_FILE"

echo "[DONE] Wrote key: $TARGET_KEY"
echo "[DONE] Wrote env: $ENV_FILE"
echo
echo "Run this before BigQuery commands:"
echo "  set -a && source .env.local && set +a"
