# A streaming cookbook

Apache Flink® SQL recipes for a chain of pizza shops.

A single-page site with animated examples of the basic Apache Flink® SQL patterns, with Apache
Kafka® sources and sinks. For each one it shows
the use case, the SQL, the job graph with the stream type on every edge, the state each operator
keeps, and what watermarks do. The examples use the pizza-chain story from the flink-intro talk.
See [PLAN.md](PLAN.md) for the full plan.

## View it

Open `index.html` in a browser. There's no build step and nothing to install. Pushes to `main`
are published to GitHub Pages by `.github/workflows/pages.yml`.

## Share it as one file

```sh
node tools/bundle.mjs              # writes dist/flink-sql-patterns.html
```

The bundle inlines the stylesheet and every script, and embeds the fonts, so it's a single HTML
file that works offline. Building it fetches the fonts from Google Fonts once; add `--no-fonts` to
keep the font link instead (the page then falls back to system fonts when offline).

## How it fits together

| Path | What it is |
| --- | --- |
| `js/patterns/*.js` | One file per pattern: SQL, job graph, input records, captions |
| `js/operators.js` | Simplified simulators for Flink's operators (Calc, ChangelogNormalize, sinks, ...) |
| `js/sim.js` | Runs a pattern's input through its graph, and records every step with a state snapshot |
| `js/graph.js` | Draws the job graph in SVG, and animates records along the edges |
| `js/player.js` | Builds a pattern section, and the playback controls |
| `js/hero.js` | Moves abstract records through the hero's miniature job graph (the SVG is in index.html): append-only in, a state update, upsert out, and the odd watermark |
| `verification/` | The SQL behind every job graph, and the plans Flink produced for it |
| `tools/shoot.mjs` | Screenshots the page in headless Chrome, and reports console errors |
| `tools/bundle.mjs` | Bundles the site into one standalone HTML file |
| `tools/export-replay.mjs` | Exports patterns, with the simulator's output, for `verification/run-replay.sh` |

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

### Replaying patterns through real Flink

```sh
verification/run-replay.sh                       # every tab, on Flink 2.3.0
verification/run-replay.sh windows/tumble        # just one (pattern id / tab id)
FLINK_VERSION=2.4-SNAPSHOT verification/run-replay.sh enrichment/lateral
FLINK_VERSION=2.3-SNAPSHOT verification/run-replay.sh dedup/byid
```

This checks what the job graphs can't: that the simulator behind each animation produces the rows
real Flink does. `tools/export-replay.mjs` runs the page's own simulator, and writes a spec per
pattern to `verification/runtime/`: the SQL and tables, the inputs with the exact watermarks the
simulator emitted, and the rows each sink received. `Replay.java` then runs the pattern's SQL on a
local Flink mini-cluster, replaying those inputs and watermarks, into `capture` sinks that declare
the real sinks' changelog modes, and compares the output row by row, row kinds included. Its
source never finishes, so Flink never fires the windows the simulator leaves open.

With more than one source, the order in which inputs reach an operator isn't fixed in a real job,
so those outputs are compared as unordered sets of rows. Tabs checked on a snapshot build are
skipped by a plain run, which says how to run them. The lookup join isn't replayed: it needs an
HTTP service. For LATERAL SNAPSHOT, the replay passes `load_completed_time` with the compile time
the tab assumes (10:00:00), since the replay's own compile time is now. Only sink output is
compared here; `run-state.sh` checks the state panels.

### Checking state and timers

```sh
verification/run-state.sh                        # every tab with a traced operator
verification/run-state.sh windows/tumble         # just one
FLINK_VERSION=2.4-SNAPSHOT verification/run-state.sh enrichment/lateral
```

This checks what the state panels show, operator by operator. For each operator with a `facts()`
function in `js/operators.js` (every stateful one except the lookup join), the exporter also writes
every item the simulated operator received, what it emitted, and its state and timers afterwards.
`StateCheck.java` plans the pattern's SQL, takes the operators the planner built for it
(LocalWindowAggregate and GlobalWindowAggregate for one simulated WindowAggregate), and runs them in
Flink's operator test harness, with the heap state backend; a two-input operator gets each item on
the input that reads from the same source table. After each item it compares the emitted rows and
watermarks, the keys and namespaces in state, and every timer. Window aggregations and Rank's
FastTop1Function hold records in memory until a checkpoint, so the harness takes one before each
comparison. Output fields the simulator doesn't model (a join's copy of the other side's key, say)
and input fields it leaves out are listed, not compared. The same snapshot gating as
`run-replay.sh` applies. What the checks found, and what's left, is in
`verification/STATE-PLAN.md`.

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
