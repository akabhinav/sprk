#!/usr/bin/env bash
#
# minispark-submit — the MiniSpark analogue of `spark-submit`.
#
# Submits an application class (with its app args) to MiniSpark, launching the
# driver in this process and — for a non-local master — real executor JVMs that
# connect back over TCP. Options mirror spark-submit.
#
#   scripts/minispark-submit.sh \
#       --class com.minispark.examples.WordCount \
#       --master netty \
#       --num-executors 2 \
#       --executor-cores 2 \
#       /path/to/book.txt
#
# Options:
#   --class <FQCN>            application main class (required)
#   --master <url>            local[N] | netty | miniyarn://host:port  (default local[*])
#   --name <str>              application name
#   --num-executors <n>       executor count (default 2 for non-local masters)
#   --executor-cores <n>      cores per executor
#   --executor-memory <mb>    executor memory (MB)
#   --conf key=value          extra minispark.* config (repeatable)
#   [app.jar] [app args...]   application jar (optional) and its arguments
#
# Env:
#   SKIP_BUILD=1   skip the mvn build (assume target/classes is current)
set -euo pipefail

cd "$(dirname "$0")/.."

# Build the project so every module's classes + deps are on the classpath.
if [ "${SKIP_BUILD:-0}" != "1" ]; then
  mvn -q -pl minispark-examples -am install -DskipTests
fi

# Assemble the full runtime classpath: all module target/classes + dependency
# jars (resolved once via the dependency plugin into a cached file).
CP_FILE="target/minispark-classpath.txt"
mkdir -p target
if [ ! -s "$CP_FILE" ] || [ "${SKIP_BUILD:-0}" != "1" ]; then
  mvn -q -pl minispark-examples dependency:build-classpath \
      -Dmdep.outputFile="$PWD/$CP_FILE" >/dev/null
fi
DEP_CP="$(cat "$CP_FILE")"
MODULE_CP="minispark-rpc/target/classes:miniyarn/target/classes:minispark-core/target/classes:minispark-sql/target/classes:minispark-examples/target/classes"
CLASSPATH="$MODULE_CP:$DEP_CP"

# Hand everything to the Java launcher (it parses the spark-submit-style flags,
# sets minispark.* system properties, and reflectively calls the app's main).
exec java -cp "$CLASSPATH" com.minispark.deploy.MiniSparkSubmit "$@"
