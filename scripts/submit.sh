#!/usr/bin/env bash
# Run an example in DISTRIBUTED mode: the driver launches real executor JVMs
# that connect back over TCP (rpc.mode=netty).
#
#   scripts/submit.sh /path/to/book.txt
#
# Optional env vars: EXECUTORS (default 2), CORES (default 2)
set -euo pipefail

INPUT="${1:-README.txt}"
EXECUTORS="${EXECUTORS:-2}"
CORES="${CORES:-2}"

mvn -q -pl minispark-examples -am install -DskipTests
mvn -q -pl minispark-examples exec:java \
  -Dexec.mainClass=com.minispark.examples.WordCount \
  -Dexec.args="$INPUT" \
  -Dminispark.rpc.mode=netty \
  -Dminispark.executor.instances="$EXECUTORS" \
  -Dminispark.executor.cores="$CORES"
