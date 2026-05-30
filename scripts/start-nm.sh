#!/usr/bin/env bash
# Start a MiniYarn NodeManager. Required: name.
#   scripts/start-nm.sh <nodeName> [rmHost] [rmPort] [host] [port] [cores] [memMB]
set -euo pipefail
NAME="${1:?Usage: $0 <nodeName> [rmHost] [rmPort] [host] [port] [cores] [memMB]}"
RMHOST="${2:-127.0.0.1}"
RMPORT="${3:-8032}"
HOST="${4:-127.0.0.1}"
PORT="${5:-0}"
CORES="${6:-4}"
MEM="${7:-4096}"
mvn -q -pl miniyarn -am install -DskipTests
mvn -q -pl miniyarn exec:java \
  -Dexec.mainClass=com.miniyarn.nm.NodeManager \
  -Dexec.args="$NAME $RMHOST $RMPORT $HOST $PORT $CORES $MEM"
