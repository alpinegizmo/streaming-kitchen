# Plan: Flink SQL design patterns, animated

A single-page website. Each basic stream-processing pattern gets its own section with an animated
job graph. The examples are about the pizza chain from `flink-intro-talk/flink-intro.html`, and
each section answers one question the chain asks of its data.

## 1. What every pattern section shows

| Element | How it's presented |
| --- | --- |
| Use case | A question in plain English ("Sales per store, every 5 minutes"), then a 2–3 sentence summary |
| SQL | A highlighted query. While the animation runs, the lines for the active operator light up |
| Job graph | SVG: Kafka source → operators → Kafka sink, with **ChangelogNormalize** and **SinkUpsertMaterializer** only where the planner actually adds them |
| Stream type | A badge on every edge: `APPEND`, `UPSERT` or `RETRACT`. Records carry row kinds `+I` `-U` `+U` `-D` |
| State | A live panel under each stateful operator showing its keyed state as it changes (and as it's freed), plus a static note on state shape, growth and TTL |
| Watermarks | Yellow watermark markers that flow along the edges with the data. A "Role of watermarks" note says one of: *not used*, *passed through*, *triggers results and frees state*, *orders events*, *holds results until both inputs catch up* |
| Narration | A one-line caption for each animation step that says what just happened and why, plus an expandable "Details" block per pattern with the longer explanation |

Each sink shows its topic as a log of records. Upsert sinks also show the table you get when you
compact that log, which is what makes upsert vs. retract click for most people.

## 2. Shared data model (the pizza chain)

| Table | Kind | Key / time | Used by |
| --- | --- | --- | --- |
| `orders` | Kafka, append-only | `event_time`, watermark −1 s, plus `proc_time AS PROCTIME()` | most patterns |
| `order_status` | upsert-kafka, PK `order_id` | `event_time` | ChangelogNormalize variants |
| `deliveries` | Kafka, append-only | `order_id`, `event_time` | interval join |
| `pizza_costs` | upsert-kafka / CDC, PK `pizza_id`, versioned | `valid_from` + watermark | temporal and lateral snapshot joins |
| `routes` | HTTP lookup connector (not Kafka) | `(store_id, address)` | lookup join |
| `kitchen_tickets` | Kafka, append-only | `store_id`, `prep_minutes`, `event_time` | MATCH_RECOGNIZE |

The `orders` DDL comes straight from the deck (`order_id, store_id, pizza_id, channel, total,
delivery_address, event_time`).

## 3. Pattern catalogue

What the planner adds and which changelog mode appears on each edge are marked **(verify)** when they
must be confirmed with `EXPLAIN CHANGELOG_MODE` before they're drawn (see §6).

### 3.1 Filtering and projection
- **Question:** Delivery orders only, with tax added.
- **SQL:** `SELECT order_id, store_id, total * 1.08 AS total_with_tax, event_time FROM orders WHERE channel = 'delivery'`
- **Graph:** Source `APPEND` → Calc `APPEND` → Sink (kafka)
- **State:** none. **Watermarks:** passed through and not used.
- **Variant ("filtering an upsert stream"):** `order_status WHERE status <> 'cancelled'`. Source
  `UPSERT` (`+U −D`) → hash(order_id) → **ChangelogNormalize with the filter folded in** (last row per
  `order_id` that passes the filter) `UPSERT` (`+I +U −D`) → upsert-kafka sink. There's no separate
  Calc. When a row stops matching, ChangelogNormalize emits `−D` for the old row and clears the key.
  *Verified on 2.3 (`verification/filter.plan.txt`, and `DeduplicateFunctionHelper.processLastRowOnChangelogWithFilter`).*

### 3.2 Deduplication
**a) Processing time.** Count each order once, despite producer retries.
- `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proc_time ASC) = 1`
- Graph: Source `APPEND` → Exchange(hash order_id) → Deduplicate (keep first, proctime) `APPEND` → Sink (kafka)
- State: one "seen" flag per `order_id`. It grows with cardinality, so the takeaway is **state TTL**.
- Watermarks: not used.

**b) Event time.** Keep the earliest version of each order by event time.
- `... ORDER BY event_time ASC) = 1`
- Graph: Source → Exchange → RowTime Deduplicate `RETRACT`/`UPSERT` **(verify)** → upsert-kafka sink
- State: the full stored row per key, because a row with an earlier event time can arrive later and replace it.
- Watermarks: the operator doesn't wait for them. Event time decides *which* row wins, not *when* to
  emit. That's why the output is updating.

### 3.3 Non-windowed aggregation
**a) Running total.** Running sales per store.
- `SELECT store_id, SUM(total) FROM orders GROUP BY store_id`
- Graph: Source `APPEND` → Exchange(hash store_id) → GroupAggregate `UPSERT` → upsert-kafka sink (PK store_id).
  No materializer, because the upsert key matches the sink's PK. **(verify)** Toggle: with a retract-capable
  sink, the same edge becomes `RETRACT` (`-U`/`+U` pairs, as in the deck's "Append-only vs updating" slide).
- State: accumulator per store (sum + count, so it can retract). It's unbounded, so TTL matters.
- Watermarks: not used.

**b) Aggregating an updating stream.** How many orders are in each status right now?
- `SELECT status, COUNT(*) FROM order_status GROUP BY status`
- Graph: Source `UPSERT` → **ChangelogNormalize** `RETRACT` → Exchange(hash status) → GroupAggregate → upsert sink
- Shows that an order moving from "baking" to "out for delivery" has to *decrement* one count and
  increment another, so the aggregate needs `-U`. That's why ChangelogNormalize is there.

### 3.4 Windows (all: Window TVF + GROUP BY, append-only output, watermark triggers emission and frees state)
| Window | Question | Teaching point |
| --- | --- | --- |
| **Tumble** 5 min | Sales per store, every 5 minutes | The deck's W1/W2/W3 example. A window emits once, when the watermark passes its end |
| **Hop** 15 min / 5 min | Sales in the last 15 minutes, updated every 5 | State is kept per **slice**, so each event lands in one slice and each window fire merges 3 slices |
| **Cumulate** 1 h / 15 min | Sales so far this hour, every 15 minutes | Early, growing results that are still append-only. Slices are shared |
| **Session** 10 min gap, `PARTITION BY store_id` | How long was each store's lunch rush? | Windows **merge** when a new event bridges a gap. The end of a window isn't known until the watermark passes last event + gap |

- Graph: Source → WindowAggregate → Calc → Sink (kafka) **(verify)**. If the plan splits it into
  LocalWindowAggregate → Exchange → GlobalWindowAggregate, draw that as a single WindowAggregate node
  with a "two-phase" note.
- State panel: one accumulator per `(store, slice/window)` plus a timer at the window's end.

### 3.5 Event correlation (interval join)
- **Question:** Which orders are late right now? (no delivery within 45 minutes)
- SQL from the deck: `orders LEFT JOIN deliveries ON … AND d.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '45' MINUTE WHERE d.order_id IS NULL`
- Graph: two Sources → Exchange(hash order_id) ×2 → IntervalJoin `APPEND` → Calc → Sink (kafka)
- State: a buffer for each side, keyed by `order_id` and indexed by time.
- Watermarks: when the watermark passes `order time + 45 min`, the order's buffer is cleared, and an
  unmatched order is emitted null-padded. Because of that, outer-join output is still append-only.
  The join's output watermark is held back by the interval.

### 3.6 Enrichment
**a) Lookup join.** How long is the drive to this address?
- `JOIN routes FOR SYSTEM_TIME AS OF o.proc_time`
- Graph: Source → LookupJoin (external maps API drawn beside it, with an LRU+TTL cache) `APPEND` → Sink
- State: none in Flink. The cache isn't checkpointed. Watermarks: not used. Mention async lookup.

**b) Temporal join (event time).** What did that pizza cost to make when it was ordered?
- `JOIN pizza_costs FOR SYSTEM_TIME AS OF o.event_time`
- Graph: Source orders `APPEND` + Source pizza_costs `UPSERT` (→ ChangelogNormalize? **verify**) → Exchange(hash pizza_id) → TemporalJoin `APPEND` → Sink
- State: orders buffered until the watermark reaches them, and versions of each pizza's cost, pruned
  as the watermark advances.
- Watermarks: results are held until the **minimum** watermark of both inputs passes the order's time.
  Show the stall when the costs topic goes quiet (the idle-source trap from the deck).

**c) LATERAL SNAPSHOT join (FLIP-579).** Join with the *current* cost, right away.
- `LEFT JOIN LATERAL SNAPSHOT(TABLE pizza_costs) AS c ON …`
- Graph: two Sources → LateralSnapshotJoin `APPEND` → Sink
- State: the latest cost per pizza, plus orders held during the **LOAD** phase.
- Watermarks: not used for triggering. Animate the two phases: LOAD (orders wait until costs have
  caught up), then JOIN (immediate). Caption: not deterministic.
- **(verify against FLIP-579 and the released 2.4 docs: operator name, syntax, how updating right sides are handled)**

### 3.7 Complex event processing
- **Question:** Which kitchens are falling behind? (three tickets in a row, each slower than the last)
- `MATCH_RECOGNIZE` from the deck (`PARTITION BY store_id ORDER BY event_time … PATTERN (A B C)`)
- Graph: Source `APPEND` → Exchange(hash store_id) → Match `APPEND` → Sink
- State: events buffered until the watermark (so they can be **sorted by event time**), plus partial
  matches (NFA + shared buffer).
- Watermarks: events are processed in order only after the watermark passes them. Late events are
  dropped. Mention `WITHIN` timeouts.

## 4. Architecture

**Static site with no build step and no runtime dependencies.** Plain HTML, CSS and JS, loaded with
classic `<script>` tags so it also opens from `file://`. Published to GitHub Pages from this repo
by a new `.github/workflows/pages.yml` that deploys `main`. (flink-intro-talk has no Pages workflow
to copy.)

```
index.html            page shell, intro, nav, one <section> per pattern
css/style.css         design tokens (Confluent palette from the deck), light and dark
js/engine.js          animation engine: timeline, playback controls, renderer
js/graph.js           SVG job-graph layout and drawing (nodes, edges, badges, state panels)
js/operators.js       simplified operator simulators (filter, dedup, agg, window, joins, match)
js/sql.js             tiny SQL highlighter with line→operator mapping
js/patterns/*.js      one spec per pattern: question, summary, SQL, graph, input events, captions
verification/         EXPLAIN CHANGELOG_MODE output per query, plus the Flink setup used
```

**Data-driven patterns.** Each pattern is a spec:
`{ id, question, summary, sql, sqlMap, graph: {nodes, edges(mode)}, stateNotes, watermarkRole, inputs, captions }`.

**Traces generated by simulators, not written by hand.** Each operator kind has a small, deterministic
simulator with simplified Flink semantics. The engine feeds the pattern's input events and watermarks
through the graph and records a **trace** of steps: record moves, state updates, emits with row kind,
watermark advances, timer fires. Captions attach to step indices. This keeps animations consistent
with the stated semantics, and changing the inputs never means re-choreographing by hand.

**Playback.** The state at step *n* is computed by replaying steps 0…n, so **step back** and
scrubbing are trivial and always correct. Only the latest step is tweened (Web Animations API).
Controls: play/pause, step ±, restart, speed, keyboard shortcuts. Animations auto-pause when
off-screen (IntersectionObserver). `prefers-reduced-motion` switches to discrete steps.

**Visual language** (taken from the deck): blue `#0056F9` for data flow, green `#00B074` for good
choices and takeaways, orange `#FF7C52` for warnings and late data, yellow `#FFDC00` for
watermarks. Poppins with a system fallback. Record pills show key, value and event time, and
row-kind chips are colour-coded.

**Page structure:** a hero, then a short **"How to read these diagrams"** primer (stream types, row
kinds, watermarks, state, the job-graph legend), then the patterns in the order listed in CLAUDE.md.
A sticky nav with sub-items, and a summary matrix at the end (pattern × state × watermarks × output
mode) that links back to each section.

## 5. Milestones

1. **Skeleton + engine + Filter** (both variants). Shows the pipeline end to end: spec → simulator →
   trace → render → playback, including ChangelogNormalize and an upsert sink's log and table.
2. **Tumble window.** Exercises the hard engine features: watermarks, timers, state release.
3. **Non-windowed aggregation** (a + b). Updating streams, `RETRACT` vs `UPSERT` edges.
4. **Deduplication** (a + b) and **Hop / Cumulate / Session**.
5. **Joins:** interval, lookup, temporal, lateral snapshot (two-input layouts, the external system node).
6. **MATCH_RECOGNIZE.**
7. **Primer, summary matrix, polish:** responsive layout, dark mode, accessibility (captions as a
   live region, keyboard), and a Flink-accuracy review pass on every caption.

Each milestone ends with a browser check (screenshots, light and dark, narrow width).

## 6. Verification of Flink semantics

- Run every query on the target Flink version with `verification/run-explain.sh` (a local Flink
  distribution and the Kafka SQL connector; no cluster or Kafka broker needed) with
  `EXPLAIN CHANGELOG_MODE`. Check operator names, edge changelog modes, and whether
  ChangelogNormalize and SinkUpsertMaterializer are present. Save the output in `verification/`, and
  draw graphs only from it. Every **(verify)** item above is resolved this way.
- For a few patterns (tumble, temporal join, event-time dedup), feed the same input events to real
  Flink and compare its sink output with the simulator's trace.
- For SinkUpsertMaterializer, find a query in the set that naturally triggers it (a sink PK that
  differs from the upsert key). If none does, add a short "When does Flink add a
  SinkUpsertMaterializer?" aside to §3.3 instead of contriving a pattern.

## 7. Decisions

1. **Target platform and version: open-source Flink 2.4.** DDL uses OSS `'connector'` options, and
   graphs use OSS operator names. 2.4 isn't released yet (as of 2026-11-19 the newest release is
   2.3.0), so plans are verified on 2.3.0 for now and re-checked once 2.4 ships (§6). Any feature that
   hasn't been released yet is labelled as such on the page.
2. **Narration: per-step captions plus a "Details" block.** Each animation step gets a one-line
   caption, and each pattern gets an expandable block with the longer explanation (§1).
3. **Hosting: GitHub Pages from this repo**, via a new Pages workflow (§4).
4. **Two-phase aggregation: collapsed.** Local/Global window aggregation is drawn as one
   WindowAggregate node with a "two-phase" note (§3.4).
