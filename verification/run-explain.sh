#!/usr/bin/env bash
# Runs EXPLAIN CHANGELOG_MODE for every verification/*.sql file on a local Flink
# distribution, and writes the optimized physical plans to verification/<name>.plan.txt.
# Downloads Flink and the Kafka SQL connector into verification/.cache on first use.
set -euo pipefail

FLINK_VERSION="${FLINK_VERSION:-2.3.0}"
KAFKA_CONNECTOR="${KAFKA_CONNECTOR:-5.0.0-2.2}"

here="$(cd "$(dirname "$0")" && pwd)"
cache="$here/.cache"
flink="$cache/flink-$FLINK_VERSION"
jar="$cache/flink-sql-connector-kafka-$KAFKA_CONNECTOR.jar"
mkdir -p "$cache"

if [[ ! -d "$flink" ]]; then
  curl -sfL "https://dlcdn.apache.org/flink/flink-$FLINK_VERSION/flink-$FLINK_VERSION-bin-scala_2.12.tgz" \
    | tar xz -C "$cache"
fi
if [[ ! -f "$jar" ]]; then
  curl -sfL -o "$jar" \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/$KAFKA_CONNECTOR/flink-sql-connector-kafka-$KAFKA_CONNECTOR.jar"
fi

for sql in "$here"/${1:-*}.sql; do
  name="$(basename "$sql" .sql)"
  out="$here/$name.plan.txt"
  "$flink/bin/sql-client.sh" -j "$jar" -f "$sql" 2>&1 | python3 -I -c '
import re, sys
text = sys.stdin.read()
blocks = re.findall(r"== Optimized Physical Plan ==\n(.*?)\n\n== Optimized Execution Plan ==", text, re.S)
if not blocks:
    sys.exit("no plans found:\n" + text[-2000:])
print("# Flink '"$FLINK_VERSION"', EXPLAIN CHANGELOG_MODE, optimized physical plans\n")
for i, b in enumerate(blocks, 1):
    print(f"## Statement {i}\n\n{b.strip()}\n")
' > "$out"
  echo "wrote ${out#$here/}"
done
