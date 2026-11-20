# Stream processing patterns in Apache Flink® SQL

A single-page site with animated examples of the basic Apache Flink® SQL patterns, with Apache
Kafka® sources and sinks. For each one it shows
the use case, the SQL, the job graph with the stream type on every edge, the state each operator
keeps, and what watermarks do. The examples use the pizza-chain story from the flink-intro talk.
See [PLAN.md](PLAN.md) for the full plan.

## View it

Open `index.html` in a browser. There's no build step and nothing to install. Pushes to `main`
are published to GitHub Pages by `.github/workflows/pages.yml`.

## How it fits together

| Path | What it is |
| --- | --- |
| `js/patterns/*.js` | One file per pattern: SQL, job graph, input records, captions |
| `js/operators.js` | Simplified simulators for Flink's operators (Calc, ChangelogNormalize, sinks, ...) |
| `js/sim.js` | Runs a pattern's input through its graph, and records every step with a state snapshot |
| `js/graph.js` | Draws the job graph in SVG, and animates records along the edges |
| `js/player.js` | Builds a pattern section, and the playback controls |
| `verification/` | The SQL behind every job graph, and the plans Flink produced for it |
| `tools/shoot.mjs` | Screenshots the page in headless Chrome, and reports console errors |

## Checking the job graphs against Flink

Each job graph, and the stream type on each edge, is copied from what Flink's planner produces:

```sh
verification/run-explain.sh                     # every verification/*.sql file, on Flink 2.3.0
verification/run-explain.sh filter              # just one
FLINK_VERSION=2.4-SNAPSHOT verification/run-explain.sh lateral-snapshot
```

The script runs each statement through `TableEnvironment` in `verification/harness/`, a small Maven
project, against the Flink jars for `FLINK_VERSION` (2.3.0 by default). Snapshot versions come from
the Apache snapshot repository, which is how features that aren't released yet are checked. It writes
the optimized plans from `EXPLAIN CHANGELOG_MODE` to `verification/<name>.plan.txt`, and fails if a
statement doesn't plan. You need Java 17 or later and Maven; no cluster or Kafka broker is needed.
Keep the SQL in `verification/` identical to the SQL shown on the page.

## Screenshots

```sh
node tools/shoot.mjs out.png --full                        # the whole page
node tools/shoot.mjs out.png --selector '#filter .stage'   # one element
node tools/shoot.mjs out.png --width 390 --dark            # phone width, dark mode
```

`--eval JS` runs in the page before the screenshot, for example to step an animation forward.
The script exits non-zero if the page logged an error.

## Trademarks

Apache®, Apache Flink®, Flink®, Apache Kafka® and Kafka® are either registered trademarks or
trademarks of The Apache Software Foundation in the United States and/or other countries. This
project is not affiliated with The Apache Software Foundation, and no endorsement by The Apache
Software Foundation is implied by the use of these marks. Pages follow the
[ASF trademark policy](https://www.apache.org/foundation/marks/): the first and most prominent
mentions, the page title and the description use the full names with ®, and the footer carries this
notice. The site uses no Apache project logos; don't host it on a domain name that contains "flink"
or "kafka".
