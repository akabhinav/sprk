#!/usr/bin/env bash
# Submit an example to a running MiniYarn cluster.
# Requires: RM started on RMHOST:RMPORT, at least one NodeManager.
#   scripts/submit-yarn.sh /path/to/book.txt
# Env: RMHOST (127.0.0.1), RMPORT (8032), EXECUTORS (2), CORES (2), MEM (256)
set -euo pipefail
INPUT="${1:-README.txt}"
RMHOST="${RMHOST:-127.0.0.1}"
RMPORT="${RMPORT:-8032}"
EXECUTORS="${EXECUTORS:-2}"
CORES="${CORES:-2}"
MEM="${MEM:-256}"
mvn -q -pl minispark-examples -am install -DskipTests
mvn -q -pl minispark-examples exec:java \
  -Dexec.mainClass=com.minispark.examples.WordCount \
  -Dexec.args="$INPUT" \
  -Dminispark.master="miniyarn://$RMHOST:$RMPORT" \
  -Dminispark.executor.instances="$EXECUTORS" \
  -Dminispark.executor.cores="$CORES" \
  -Dminispark.executor.memoryMB="$MEM"
