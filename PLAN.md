# Plan: Flink SQL design patterns, animated

A single-page website. Each basic stream-processing pattern gets its own section with an animated
job graph. The examples are about the pizza chain from `flink-intro-talk/flink-intro.html`, and
each section answers one question the chain asks of its data.

## 1. What every pattern section shows

| Element | How it's presented |
| --- | --- |
| Use case | A question in plain English ("Sales per store, every 5 minutes"), then a 2–3 sentence summary |
| SQL | A highlighted query. While the animation runs, the lines for the active operator light up |
| Job graph | SVG: Kafka source → operators → Kafka sink, with **ChangelogNormalize** and **SinkUpsertMaterializer** only where the planner actually adds them. Calcs that only drop columns are left out, and two-phase aggregations are drawn as one operator; the full plan is in the Details block |
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
| `kitchen_tickets` | Kafka, append-only | `store_id`, `prep_minutes`, `event_time` | MATCH_RECOGNIZE, window join |
| `stores` | upsert-kafka, PK `store_id` | `city`, `manager`; no watermark | regular join |
| `late_orders` | Kafka, append-only | same columns as `orders` | dead letter queue |

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
Verified on 2.3 (`verification/dedup.sql`). Both are planned as `Rank(AppendFastStrategy)`, which the
execution plan shows as `Deduplicate(keep=[FirstRow], ..., outputInsertOnly=[true])`. Both variants use
the same input, so the results can be compared.

**a) Processing time.** Count each order once, even when the app sends it twice.
- `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proc_time ASC) = 1`
- Graph: Source `APPEND` → hash(order_id) → Deduplicate (keep first, by arrival) `APPEND` → Sink (kafka)
- State: one "seen" flag per `order_id` (`ProcTimeDeduplicateKeepFirstRowFunction`). Takeaway: **state TTL**.
- Watermarks: not used.

**b) Event time.** Keep the earliest attempt of each order, by event time.
- `... ORDER BY event_time ASC) = 1`
- Graph: Source → hash(order_id) → Deduplicate (keep first, by event time) `APPEND` → Sink (kafka).
  *Not* updating, as the plan first assumed.
- Semantics (`RowTimeDeduplicateKeepFirstRowFunction`): a row older than the watermark is late, and
  dropped; the earliest row so far waits with a timer at its event time (ties keep the first to arrive);
  the watermark emits it and marks the key as emitted; later rows for the key are ignored.
- Keeping the last row (`DESC`) gives an updating stream (checked).

**c) Ordered by order_id** (added later). The same orders, `ORDER BY order_id ASC`.
- Verified on a 2.3 snapshot (`verification/dedup-by-id.sql`). Not a time attribute, so no Deduplicate:
  `Rank(AppendFastStrategy)`, run by `FastTop1Function`, which keeps the whole first row per key and
  replaces it only with a row that sorts strictly ahead (as `+U`). Ties everywhere, so the first copy
  wins and only `+I` is ever sent, but the plan declares `[I,UA,D]`, so the sink is upsert-kafka.
- Flink 2.3.0 declares the Rank insert-only and accepts an append-only sink; that's the planner bug
  fixed in 2.3.1 by FLINK-40368 (checked: 2.3 and 2.4 snapshots reject the append-only sink).

**d) Updating input** (added later). Each store's first order that wasn't cancelled.
- `order_status` (upsert-kafka) is already unique by `order_id`, so this partitions by `store_id`:
  `ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY event_time ASC) = 1`, `WHERE status <> 'cancelled'`.
- Graph: upsert Source → hash(order_id) → ChangelogNormalize (filter pushed in) `RETRACT` →
  hash(store_id) → `Rank(RetractStrategy)` `[I,UA,D]` → Sink (upsert-kafka). Over an updating input
  the planner keeps the Rank instead of a Deduplicate, for every ordering (checked).
- Semantics (`RetractableTopNFunction`, top 1 without the row number): every row per partition is kept,
  sorted. A new first row emits `−D` for the displaced row, then `+I`; retracting the first row emits
  `−D`, then `+I` for the next; other rows are stored or removed silently. It never emits `+U`, though
  the plan lists `UA`. Event time is only a sort key: no watermark, nothing late.
- Inputs: 203 and 204 are stored without output; cancelling 201 promotes 203; 205 (earlier event time,
  later arrival) displaces 202; cancelling 204 emits nothing.

### 3.3 Non-windowed aggregation
Three variants, all verified on 2.3 (`verification/aggregation.sql`, `upsert-materializer.sql`), with
emit rules from `GroupAggHelper.processElement` and the debezium-json format from
`DebeziumJsonSerializationSchema`.

**a) Upsert sink.** How much has each store sold so far?
- `SELECT store_id, COUNT(*), SUM(total) FROM orders GROUP BY store_id` into `upsert-kafka` (PK store_id)
- Graph: Source `APPEND` → hash(store_id) → GroupAggregate `UPSERT` (`+I +U`) → Sink. No `−U`, no `−D`,
  no materializer. (A projection-only Calc after the source is left out.)
- State: accumulator per store, kept forever without a TTL. Watermarks: not used.

**b) Retract sink.** Same query into a `kafka` sink with `debezium-json`. GroupAggregate now emits
`RETRACT` (`+I −U +U`); the format writes `−U` as `op: d` with a before image, `+U` as `op: c`. A plain
`json` sink is rejected ("doesn't support consuming update changes").

**c) Updating input.** How many orders are in each status right now?
- `SELECT status, COUNT(*) FROM order_status GROUP BY status`
- Graph: Source `UPSERT` (`+U −D`) → hash(order_id) → **ChangelogNormalize** `RETRACT` (`+I −U +U −D`) →
  hash(status) → GroupAggregate (`COUNT_RETRACT`) `UPSERT` (`+I +U −D`) → upsert sink.
- A debezium-json source would make ChangelogNormalize unnecessary (checked).

**SinkUpsertMaterializer** doesn't arise naturally here, so (a) has an aside: in 2.3 a query whose upsert
key differs from the sink's PK is rejected unless it has `ON CONFLICT DO DEDUPLICATE | DO ERROR | DO
NOTHING`, each of which plans the sink with `upsertMaterialize=[true]` and makes the aggregate emit `−U`.

### 3.4 Windows (all: Window TVF + GROUP BY, append-only output, watermark triggers emission and frees state)
| Window | Question | Teaching point |
| --- | --- | --- |
| **Tumble** 5 min | Sales per store, every 5 minutes | The deck's W1/W2/W3 example. A window emits once, when the watermark passes its end |
| **Hop** 15 min / 5 min | Sales in the last 15 minutes, updated every 5 | State is kept per **slice**, so each event lands in one slice and each window fire merges 3 slices |
| **Cumulate** 1 h / 15 min | Sales so far this hour, every 15 minutes | Early, growing results that are still append-only. Slices are shared |
| **Session** 10 min gap, `PARTITION BY store_id` | How long was each store's lunch rush? | Windows **merge** when a new event bridges a gap. The end of a window isn't known until the watermark passes last event + gap |
| **Updating input** (added later) Tumble 5 min over `order_status` (upsert-kafka), `WHERE status <> 'cancelled'` | Sales every 5 minutes, not counting cancelled orders | Window aggregation accepts updating input, and its output is still append-only. A cancellation retracts the order from its window, but only until the window fires; after that it's late, and dropped |

- Graph: Source → WindowAggregate → Sink (kafka), all `[I]` (`verification/windows.sql`). Tumble, hop and
  cumulate are planned as Calc → LocalWindowAggregate → hash(store_id) → GlobalWindowAggregate → Calc,
  drawn as one operator; session as a single WindowAggregate (no two-phase).
- Semantics from Flink 2.3's slice processors and assigners: a window fires when the watermark reaches
  `window_end − 1 ms`; a record goes into one slice and sets a timer at the first unfired window end; hop
  merges the slices a window spans, schedules the next window only if this one wasn't empty, and frees
  its first slice; cumulate merges each step's slice into the shared first slice and frees it at the
  hour; sessions merge overlapping windows and fire at last event + gap.
- State panels show slices (or sessions) and the pending timers. Hop and cumulate use store 7 only, to
  keep the output readable.
- Updating input: upsert source → hash(order_id) → ChangelogNormalize (the filter is pushed into it as
  `condition=`) → `[I,UB,UA,D]` → the two-phase window aggregate with `COUNT_RETRACT`/`SUM_RETRACT` →
  `[I]`. `event_time` is when the order was placed, so ChangelogNormalize's `−D` carries the original
  time and lands in the window that counted the order. From Flink's source: a late record is dropped
  whether it accumulates or retracts (`AbstractSliceSyncStateWindowAggProcessor.processElement`), and a
  window whose `COUNT(*)` is 0 isn't emitted (`WindowIsEmptySupplier`). Order 101 is cancelled in time,
  103 too late, and 104 leaves store 7's second window empty. Window Top-N, window dedup, window join
  and OVER reject updating input; a regular join or non-windowed GROUP BY loses the time attribute.

### 3.5 Event correlation (interval join)
- **Question:** Which orders haven't been delivered within 45 minutes?
- SQL from the deck: `orders LEFT JOIN deliveries ON … AND d.event_time BETWEEN o.event_time AND o.event_time + INTERVAL '45' MINUTE WHERE d.order_id IS NULL`
- Graph (verified on 2.3, `verification/interval-join.sql`): two Sources → hash(order_id) → IntervalJoin
  (LeftOuterJoin) `APPEND` → Calc (`d.order_id IS NULL`) → Sink (kafka). All `[I]`.
- Semantics from `TimeIntervalJoin`/`RowTimeIntervalJoin`: combined watermark W = min of both inputs; an
  order is buffered while W < time + 45 min, a delivery while W < its time; with the 2.3 default
  `min-cleanup-interval` of 0, an unmatched order is emitted null-padded as soon as W passes time + 45 min;
  the output watermark is W − 45 min.

### 3.6 Enrichment
**a) Lookup join.** How long is the drive to each delivery address?
- Graph (verified with the JDBC connector, which implements lookup): Source → LookupJoin `APPEND` → Sink,
  with the maps service drawn beside it. The page leaves the connector name out of the DDL.
- State: none in Flink; the connector's cache (shown, labelled as not Flink state). Watermarks: not used.

**b) Temporal join (event time).** What did each pizza cost to make, when it was ordered?
- Graph (verified on 2.3): orders `APPEND` + pizza_costs `UPSERT` → hash(pizza_id) → TemporalJoin `APPEND`
  → Sink. **No ChangelogNormalize.**
- Semantics from `TemporalRowTimeJoinOperator`: orders wait until W passes their time, then join the
  latest version at or before it; older versions are removed. Shows the stall when one side lags.

**c) LATERAL SNAPSHOT join (FLIP-579, Flink 2.4).** Join with the current cost, right away.
- Not released yet (2.4 is in progress, on the `release-2.4` branch). Verified against the
  **2.4-SNAPSHOT** jars (`verification/lateral-snapshot.sql`, `FLINK_VERSION=2.4-SNAPSHOT`).
- Syntax accepted by 2.4: `JOIN LATERAL SNAPSHOT(input => TABLE pizza_costs, on_time => DESCRIPTOR(valid_from))`;
  `on_time` is required in streaming mode (the FLIP and the deck's backup slide omit it).
- Graph: orders `APPEND` + pizza_costs `UPSERT` → **ChangelogNormalize** `RETRACT` → LateralSnapshotJoin
  (`loadCompletedCondition=[compile_time]`) `APPEND` → Sink.
- Semantics from the 2.4 `LateralSnapshotJoinOperator` and docs: LOAD buffers orders and holds their
  watermarks until the costs watermark reaches the compile time; JOIN joins on arrival; build changes are
  buffered by `on_time` and applied once the costs watermark reaches them, in both phases.
- **Open question:** with an upsert build side, ChangelogNormalize's `−U` carries the old row's
  `valid_from`, so it can become due before the matching `+U`, leaving the key briefly empty. The
  operator's tests only cover pairs that share a row time. The example data avoids the case; worth
  checking with the FLIP author.

### 3.7 Complex event processing
- **Question:** Which kitchens are falling behind? (three tickets in a row, each slower than the last)
- `MATCH_RECOGNIZE` from the deck (`PARTITION BY store_id ORDER BY event_time … PATTERN (A B C)`,
  `AFTER MATCH SKIP PAST LAST ROW`), written to an append-only `slow_kitchens` topic.
- Graph (verified on 2.3, `verification/cep.plan.txt`): Source `APPEND` → Exchange(hash store_id) →
  Match `APPEND` → Sink. No ChangelogNormalize or materializer.
- State (CepOperator): rows buffered per key and timestamp until the watermark passes them, plus the
  NFA's partial matches. With strict contiguity a key has at most two partial matches.
- Watermarks: a row with a timestamp at or before the operator's watermark is late, and dropped (SQL
  has no side output). Buffered rows are fed to the NFA in timestamp order once the watermark passes
  them, so a match is emitted only when the watermark passes its last row. `WITHIN` is mentioned in
  the Details block as the bound for patterns with gaps or quantifiers.
- Data: store 7 from the deck (14, 9, 13, 18, 10) with 13 arriving after 18, interleaved with store
  9; one match (9 → 13 → 18 at 12:35), and a late store 9 ticket that would have completed
  11 → 12 → 15.

### 3.8 Regular join (added after the first plan; shown right after Non-windowed aggregation)
- **Question:** How much has each store sold, and who manages it right now? (a live leaderboard)
- Per-store totals (GroupAggregate) joined with `stores` (upsert-kafka) on `store_id`, written to an
  upsert `store_leaderboard` topic.
- Graph (verified on 2.3, `verification/regular-join.plan.txt`): orders `APPEND` → GroupAggregate
  `UPSERT [I,UA]` → Join ← stores `UPSERT [UA,D]`; Join `UPSERT [I,UA,D]` → upsert sink. Both join
  inputs are `JoinKeyContainsUniqueKey`, so there's **no ChangelogNormalize** on `stores`, no −U
  anywhere, and no SinkUpsertMaterializer.
- State: one row per `store_id` on each side of the join, bounded by the number of stores.
- Watermarks: not used. `stores` has none, so the join never emits one.
- A second tab, **Join, then aggregate (anti-pattern)**, animates the contrasting query on the same inputs
  (verified as statement 3 of `regular-join.plan.txt`): the join keeps every order (`NoUniqueKey`),
  the planner adds ChangelogNormalize (−U/+U for the join) and, because the upsert key
  (store, city, manager) differs from the sink's key, Flink 2.3 requires `ON CONFLICT`;
  `DO DEDUPLICATE` adds a SinkUpsertMaterializer (the legacy one). One manager change retracts and
  re-joins every order of the store, and the materializer turns each −U that empties its list into
  a delete, so the topic gets six records: three tombstones and two never-true totals.
- Layout changes this needed: graphs of six or more columns size each column to its labels; a
  source sits just before (and on the row of) the operator it feeds; labels of a join input go
  above the edge when the other input curves in from below; the table of contents appears only
  from 1680px, so the stage stays ~1350px wide.

### 3.9 Late events to a dead letter queue (added later; shown right after Windows)
- **Question:** Which orders arrived too late to be counted?
- The approach from the flink-intro talk: one `EXECUTE STATEMENT SET` with one definition of late,
  `event_time <= CURRENT_WATERMARK(event_time)`. That INSERT writes `late_orders`; the main INSERT
  runs a 1-minute TUMBLE over `(SELECT * FROM orders WHERE CURRENT_WATERMARK(event_time) IS NULL
  OR event_time > CURRENT_WATERMARK(event_time))` into `store_sales`. (The talk's slide writes
  `STATEMENT SET BEGIN` without `EXECUTE`, which open-source Flink 2.3 doesn't parse.)
- Graph (verified on 2.3, `verification/late-events.plan.txt`): the execution plan has one
  TableSourceScan (`reuse_id=[1]`) feeding both branches, and both filters are Calcs on that scan,
  before any exchange, so they're chained to the source. The main filter is merged with the window
  branch's projection Calc. All edges `APPEND`. The harness now joins a `BEGIN … END` block back
  into one statement, and the plan file records the execution plan whenever it shows reuse.
- Every order lands in exactly one topic, at any parallelism: both chained filters see the same
  watermark for each row, the conditions are complements (NULL included), and the window never
  drops a row that passed (its watermark can't be ahead of the filter's; LocalWindowAggregate
  flushes before forwarding a watermark that fires a buffered slice).
- Trade-off, shown by order 106: it's behind the watermark but its window is open, so a window on
  its own would count it; here it goes to the DLQ. In return the definition doesn't depend on the
  window type.
- The Calc simulator tracks its watermark and passes it to `where`.
- Layout: a fan-out (one operator, two stream outputs) gets the phone rail too, with the second
  branch's labels in a gap above its target; on wide screens the second branch's badge sits left of
  its curve and the straight branch's labels go above its edge.

### 3.10 OVER aggregation (added later; shown right after Late events)
Each order with its store's sales in the 5 minutes up to it:
`COUNT(*)` and `SUM(total) OVER w`, `WINDOW w AS (PARTITION BY store_id ORDER BY event_time RANGE BETWEEN
INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW)`.
- Graph: Source → hash(store_id) → OverAggregate `APPEND` → Sink (kafka), verified on 2.3
  (`verification/over.sql`). The graph leaves out a Calc that drops columns and one that turns `$SUM0`
  into `SUM` (null for no rows).
- Semantics (`RowTimeRangeBoundedPrecedingFunction`, the planner's choice for an event-time RANGE): a row
  newer than the key's last fired timestamp is buffered under its event time with a timer; others are
  late, and dropped, whatever the watermark. When the watermark reaches a timer, rows more than the range
  older are retracted (an offset of exactly the range stays in), the rows at that time are added and
  emitted with the result. A clean-up timer at newest + 1.5 × range (+1 ms) clears an idle key.
- Inputs: 105 arrives after 104 but is computed first; 106 is late for store 7; 107 is behind the
  watermark but accepted, because store 9 has only fired up to 12:02:30; 101 leaves 104's range; 102 is
  exactly 5 minutes before 108 and stays in. No clean-up timer fires.

### 3.11 Window join (added later; shown right after Event correlation)
Orders placed against pizzas cooked, per store, every 15 minutes: two tumbling window aggregates
(`COUNT(*) AS placed` over `orders`, `COUNT(*) AS cooked` over `kitchen_tickets`), joined with
`FULL OUTER JOIN ... ON store_id, window_start, window_end`, with `COALESCE` for the keys.
- Graph: two sources → hash(store_id) → WindowAggregate (each drawn as one node for Local + Global) →
  hash(store_id) → WindowJoin (`FullOuterJoin`) `APPEND` → Sink (kafka), verified on 2.3
  (`verification/window-join.sql`).
- Semantics (`WindowJoinOperator`, `WindowJoinHelper`): rows are kept per key and window on each side,
  with a timer at the window's end; a row whose window has fired is late, and dropped. The watermark is
  the minimum of both inputs. When it passes a window's end: matching pairs, then null padding for
  unmatched rows (only padding if one side is empty), then the window's state is cleared.
- Inputs: the kitchen watermark lags the orders', so each window waits for it. Store 9 has orders but
  no tickets in the first window (cooked null), and a ticket but no orders in the second (placed null).
  Order 103 is placed at 12:09:40 and cooked at 12:16:50, which the details use to show why raw events
  are better paired with an interval join.
- `kitchen_tickets` is used instead of `deliveries`, which has no `store_id`.

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
   - Phones get their own graph layout: a column of full-width operator cards, each state panel
     right under its operator, and a join's other input routed down a rail on the right. Long panel
     titles and footers wrap.
   - The summary table is generated from a short `matrix` entry in each variant, plus the sink
     edge's stream type and the planner-added nodes from the graph.
   - Deep links (`#windows-hop`) open a variant's tab; the sticky nav lists the variants.
   - The caption is a live region only while paused, so playback doesn't flood screen readers.
     Variant tabs use a roving tabindex.
   - The accuracy review (an independent pass against the 2.3 / 2.4 sources) corrected: window and
     session lateness rules, the temporal join's version clean-up (per key, when that key's timer
     fires; the simulator now keeps one timer per key), upsert-kafka's runtime row kind, why the
     debezium sink gets −U, event-time dedup with mini-batch, `DECIMAL(14, 4)`, the JDBC
     connector's packaging, the primer's watermark definition (at or before), and the footer.

Each milestone ends with a browser check (screenshots, light and dark, narrow width).

## 6. Verification of Flink semantics

- Run every query on the target Flink version with `verification/run-explain.sh` (a small Maven
  harness in `verification/harness/`; any released version, or a snapshot such as `2.4-SNAPSHOT`; no
  cluster or Kafka broker needed) with
  `EXPLAIN CHANGELOG_MODE`. Check operator names, edge changelog modes, and whether
  ChangelogNormalize and SinkUpsertMaterializer are present. Save the output in `verification/`, and
  draw graphs only from it. Every **(verify)** item above is resolved this way.
- Feed the same input events to real Flink and compare its sink output with the simulator's trace:
  `verification/run-replay.sh` (see the README). Done for every tab except the lookup join and LATERAL
  SNAPSHOT: 22 of 22 match on Flink 2.3.0, row kinds included (2026-11-21). A negative control (the
  tumble spec with one watermark removed) fails, as it should. Patterns with two sources are compared
  unordered; only sink output is compared, not state or intermediate operators.
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
