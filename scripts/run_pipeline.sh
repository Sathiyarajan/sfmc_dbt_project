#!/usr/bin/env bash
# =====================================================================
# run_pipeline.sh - run inside WSL Ubuntu from the project root
# Usage: ./scripts/run_pipeline.sh <dev|uat|prod> <full|models_only|snapshot_only|test_only>
# =====================================================================
set -euo pipefail

TARGET="${1:-dev}"
MODE="${2:-full}"

if [[ ! "$TARGET" =~ ^(dev|uat|prod)$ ]]; then
  echo "ERROR: target must be dev, uat, or prod" >&2
  exit 1
fi

echo ">>> Target environment: $TARGET"
echo ">>> Mode: $MODE"

cd "$(dirname "$0")/.."

# Activate venv if present
if [ -d ".venv" ]; then
  source .venv/bin/activate
fi

dbt deps

case "$MODE" in
  full)
    dbt debug --target "$TARGET"
    dbt seed --target "$TARGET"
    dbt snapshot --target "$TARGET"
    dbt run --target "$TARGET" --select tag:bronze
    dbt test --target "$TARGET" --select tag:bronze
    dbt run --target "$TARGET" --select tag:silver
    dbt test --target "$TARGET" --select tag:silver
    dbt run --target "$TARGET" --select tag:gold
    dbt test --target "$TARGET" --select tag:gold
    dbt docs generate --target "$TARGET"
    ;;
  models_only)
    dbt run --target "$TARGET"
    ;;
  snapshot_only)
    dbt snapshot --target "$TARGET"
    ;;
  test_only)
    dbt test --target "$TARGET"
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    exit 1
    ;;
esac

echo ">>> Pipeline run complete for $TARGET ($MODE)"
