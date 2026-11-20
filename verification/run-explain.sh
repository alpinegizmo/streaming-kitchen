#!/usr/bin/env bash
# Runs EXPLAIN CHANGELOG_MODE for every verification/*.sql file (or just the names given) and
# writes the optimized physical plans to verification/<name>.plan.txt.
#
# The statements run in harness/, a small Maven project, against the Flink jars for
# FLINK_VERSION (default 2.3.0). Unreleased versions work too: FLINK_VERSION=2.4-SNAPSHOT pulls
# the nightly snapshot jars from the Apache snapshot repository. No cluster or Kafka is needed.
set -euo pipefail

FLINK_VERSION="${FLINK_VERSION:-2.3.0}"
here="$(cd "$(dirname "$0")" && pwd)"
harness="$here/harness"
cp_file="$harness/target/classpath-$FLINK_VERSION.txt"

(cd "$harness" && mvn -q -Dflink.version="$FLINK_VERSION" compile dependency:build-classpath \
  -Dmdep.outputFile="$cp_file" >/dev/null)

names=("$@")
if [[ ${#names[@]} -eq 0 ]]; then
  for f in "$here"/*.sql; do names+=("$(basename "$f" .sql)"); done
fi

for name in "${names[@]}"; do
  out="$here/$name.plan.txt"
  java -cp "$harness/target/classes:$(cat "$cp_file")" Explain "$here/$name.sql" 2>&1 | python3 -I -c '
import re, sys
text = sys.stdin.read()
errors = re.findall(r"^\[ERROR\].*$", text, re.M)
blocks = re.findall(r"== Optimized Physical Plan ==\n(.*?)\n\n== Optimized Execution Plan ==", text, re.S)
if errors:
    sys.exit("\n".join(errors))
if not blocks:
    sys.exit("no plans found:\n" + text[-2000:])
print("# Flink '"$FLINK_VERSION"', EXPLAIN CHANGELOG_MODE, optimized physical plans\n")
for i, b in enumerate(blocks, 1):
    print(f"## Statement {i}\n\n{b.strip()}\n")
' > "$out.tmp" && mv "$out.tmp" "$out" || { rm -f "$out.tmp"; echo "FAILED: $name" >&2; exit 1; }
  echo "wrote ${out#$here/}"
done
