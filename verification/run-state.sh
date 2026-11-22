#!/usr/bin/env bash
# Checks the state and timers the simulator shows against the real Flink operators.
#
#   verification/run-state.sh [--dump] [pattern/variant ...]
#
# --dump prints every state value Flink holds after every step, not just after a failing one.
#
# tools/export-replay.mjs writes a spec per pattern to verification/runtime/, with a trace for each
# operator whose state can be checked: every item it handled, what it emitted, and its state and
# timers afterwards. harness/ (StateCheck.java) plans the pattern's SQL, runs the operators the
# planner built in Flink's operator test harness, feeds them the same items, and compares after
# every step. Specs without a trace are skipped.
set -euo pipefail

FLINK_VERSION="${FLINK_VERSION:-2.3.0}"
flags=()
if [[ "${1:-}" == "--dump" ]]; then flags+=(--dump); shift; fi
here="$(cd "$(dirname "$0")" && pwd)"
harness="$here/harness"
cp_file="$harness/target/classpath-$FLINK_VERSION.txt"

node "$here/../tools/export-replay.mjs" "$@" >/dev/null

(cd "$harness" && mvn -q -Dflink.version="$FLINK_VERSION" compile dependency:build-classpath \
  -Dmdep.outputFile="$cp_file")

# Tabs checked on a snapshot build only run on that build.
specs=()
if [[ $# -eq 0 ]]; then
  for f in "$here"/runtime/*.json; do
    v="$(sed -n 's/.*"flinkVersion": "\([^"]*\)".*/\1/p' "$f")"
    if [[ -z "$v" || "$v" == "$FLINK_VERSION" ]]; then
      specs+=("$f")
    fi
  done
else
  for id in "$@"; do specs+=("$here/runtime/${id/\//-}.json"); done
fi

java -cp "$harness/target/classes:$(cat "$cp_file")" StateCheck ${flags[@]+"${flags[@]}"} "${specs[@]}"
