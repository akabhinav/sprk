#!/usr/bin/env bash
# Run a PySpark-style script against MiniSpark.
#
#   scripts/pyminispark.sh path/to/script.py [args...]
#
# Builds the project (unless SKIP_BUILD=1), assembles the JVM classpath, points
# PYTHONPATH at the `python/` package, and runs your script. Requires python3
# with `cloudpickle` installed (the executor-side worker needs it too).
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "[pyminispark] building (set SKIP_BUILD=1 to skip)…" >&2
  mvn -q -pl minispark-python -am install -DskipTests
fi

CP_FILE="$(mktemp)"
mvn -q -pl minispark-python dependency:build-classpath -Dmdep.outputFile="$CP_FILE" >/dev/null
DEP_CP="$(cat "$CP_FILE")"; rm -f "$CP_FILE"
MODULE_CP="minispark-rpc/target/classes:miniyarn/target/classes:minispark-core/target/classes:minispark-sql/target/classes:minispark-python/target/classes"

export MINISPARK_CLASSPATH="$MODULE_CP:$DEP_CP"
export PYTHONPATH="$(pwd)/python:${PYTHONPATH:-}"
export MINISPARK_PYTHON="${MINISPARK_PYTHON:-python3}"

exec "$MINISPARK_PYTHON" "$@"
