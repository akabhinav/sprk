#!/usr/bin/env bash
# Start the MiniYarn ResourceManager. Defaults: 127.0.0.1:8032.
#   scripts/start-rm.sh [host] [port]
set -euo pipefail
HOST="${1:-127.0.0.1}"
PORT="${2:-8032}"
mvn -q -pl miniyarn -am install -DskipTests
mvn -q -pl miniyarn exec:java \
  -Dexec.mainClass=com.miniyarn.rm.ResourceManager \
  -Dexec.args="$HOST $PORT"
