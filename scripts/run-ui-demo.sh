#!/usr/bin/env bash
# Run WordCount with the web UI enabled and keep the driver alive for browsing.
#   scripts/run-ui-demo.sh [/path/to/book.txt] [keepAliveSeconds]
# Then open the printed http://127.0.0.1:4040/ in a browser (auto-refreshes).
set -euo pipefail
INPUT="${1:-/tmp/book.txt}"
SECONDS_ALIVE="${2:-60}"
if [ ! -f "$INPUT" ]; then
  printf 'the quick brown fox\nthe lazy dog and the quick fox\nfox fox dog\n' > "$INPUT"
fi
mvn -q -pl minispark-examples -am install -DskipTests
mvn -q -pl minispark-examples exec:java \
  -Dexec.mainClass=com.minispark.examples.WordCountWithUI \
  -Dexec.args="$INPUT $SECONDS_ALIVE"
