#!/usr/bin/env bash
# Replays patterns through real Flink and compares their sink output with the simulator's.
#
#   verification/run-replay.sh [pattern/variant ...]
#
# tools/export-replay.mjs writes a spec per pattern to verification/runtime/ (inputs, watermarks
# and the simulator's sink rows), then harness/ runs each one on a local Flink mini-cluster
# (Replay.java) for FLINK_VERSION (default 2.3.0), and prints PASS or FAIL with a row-by-row
# comparison. No Kafka is needed.
set -euo pipefail

FLINK_VERSION="${FLINK_VERSION:-2.3.0}"
here="$(cd "$(dirname "$0")" && pwd)"
harness="$here/harness"
cp_file="$harness/target/classpath-$FLINK_VERSION.txt"

node "$here/../tools/export-replay.mjs" "$@"

(cd "$harness" && mvn -q -Dflink.version="$FLINK_VERSION" compile dependency:build-classpath \
  -Dmdep.outputFile="$cp_file" >/dev/null)

# Tabs checked on a snapshot build only run on that build.
specs=()
if [[ $# -eq 0 ]]; then
  for f in "$here"/runtime/*.json; do
    v="$(sed -n 's/.*"flinkVersion": "\([^"]*\)".*/\1/p' "$f")"
    if [[ -z "$v" || "$v" == "$FLINK_VERSION" ]]; then
      specs+=("$f")
    else
      name="$(basename "$f" .json)"
      echo "skipping $name: run FLINK_VERSION=$v verification/run-replay.sh ${name/-//}"
    fi
  done
else
  for id in "$@"; do specs+=("$here/runtime/${id/\//-}.json"); done
fi

java -cp "$harness/target/classes:$(cat "$cp_file")" Replay "${specs[@]}"
