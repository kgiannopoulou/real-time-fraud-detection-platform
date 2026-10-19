#!/usr/bin/env bash
# Create the platform topics. Safe to run repeatedly (--if-not-exists).
#
#   transactions        raw card events, keyed by user_id so a user's events
#                       stay ordered within one partition
#   fraud_predictions   one scored record per transaction
#   fraud_alerts        predictions above the decision threshold
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
KT=/opt/kafka/bin/kafka-topics.sh

create() {
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1 \
    --config retention.ms="$3"
}

create transactions 6 86400000
create fraud_predictions 3 86400000
create fraud_alerts 1 604800000

"$KT" --bootstrap-server "$BOOTSTRAP" --list
