#!/usr/bin/env bash
# Dispatch to a cluster role. Classpath = every module's target/classes (absolute)
# + the resolved dependency jars. Run from /src so nothing depends on cwd.
set -euo pipefail
CP="$(echo /src/*/target/classes | tr ' ' ':'):$(cat /src/deps-cp.txt)"
role="${1:-}"; shift || true
case "$role" in
  rm)     exec java -cp "$CP" com.miniyarn.rm.ResourceManager "${1:-rm}" "${2:-8032}" ;;
  nm)     exec java -cp "$CP" com.miniyarn.nm.NodeManager "$@" ;;          # <name> <rmHost> <rmPort> <host> <port> <cores> <memMB>
  submit) exec java -cp "$CP" com.minispark.deploy.MiniSparkSubmit "$@" ;; # --class ... --master miniyarn://rm:8032 ...
  all)    exec java -cp "$CP" com.minispark.examples.AllExamplesDriver "$@" ;; # <rmHost> <rmPort> <driverHost> [sharedDir]
  *)      echo "usage: entrypoint.sh {rm|nm <args>|submit <args>|all <args>}" >&2; exit 1 ;;
esac
