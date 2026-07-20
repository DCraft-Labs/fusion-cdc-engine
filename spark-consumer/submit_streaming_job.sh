#!/usr/bin/env bash
# Submit streaming job to Spark cluster or run locally
set -euo pipefail

MODE="${1:-realtime}"
CONNECTION_ID="${2:-}"
MASTER="${SPARK_MASTER:-local[*]}"
CHECKPOINT_DIR="${SPARK_CHECKPOINT_DIR:-/tmp/spark-checkpoints}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ "$MODE" == "realtime" ]]; then
    spark-submit \
        --master "$MASTER" \
        --py-files "$SCRIPT_DIR" \
        "$SCRIPT_DIR/consumer/streaming_consumer.py" \
        --connection-id "$CONNECTION_ID" \
        --checkpoint-dir "$CHECKPOINT_DIR"
elif [[ "$MODE" == "batch" ]]; then
    spark-submit \
        --master "$MASTER" \
        --py-files "$SCRIPT_DIR" \
        "$SCRIPT_DIR/consumer/batch_consumer.py" \
        --connection-id "$CONNECTION_ID"
else
    echo "Usage: $0 [realtime|batch] <connection_id>"
    exit 1
fi
