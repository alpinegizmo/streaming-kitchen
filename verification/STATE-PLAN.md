# Plan: checking state and timers against Flink

## What needs checking

Every step in the simulator stores a snapshot of each operator's state. That snapshot is what the
state panels show: keys, values, and a "timers:" footer.

Some of the details the animations show:

- window slices and their timers;
- the OVER cleanup timer at ts + 1.5×preceding + 1 ms;
- rows kept by dedup and Top-1;
- rows buffered by the interval, window, and temporal joins;
- "clears its state" captions;
- each operator's watermark, including the combined watermark of two-input operators.

Today's replay (`run-replay.sh`) only compares what reaches the sinks. A wrong timer, or state left
behind that never changes the output, would go unnoticed.

## Approach: run each stateful operator in Flink's test harness

Flink's own operator tests use `KeyedOneInputStreamOperatorTestHarness` and
`KeyedTwoInputStreamOperatorTestHarness`. These live in the `flink-streaming-java` and
`flink-runtime` test-jars, which are in `~/.m2` for 2.3.0. With them, the harness can feed one
operator one record or watermark at a time and inspect its state and timers between steps. That
matches the simulator's step model exactly.

1. **Get the real operators from the planner.** Plan the recipe's SQL as Replay.java does now, then
   attach it to a `StreamExecutionEnvironment` with `StreamStatementSet.attachAsDataStream()` and
   call `env.getStreamGraph()`.
   - Each `StreamNode` carries the operator factory the planner generated (GroupAggregate,
     Deduplicate, Rank, and so on), its key selector, key serializer, and input types.
   - So the harness runs the same code-generated operator that a job would run, not a hand-built
     copy.

2. **Feed each operator what the simulator fed it.** The exporter already has every step, with
   `node`, `via`, and `item`. For each stateful node it would write:
   - the items that node received, in order, with which input each came on;
   - what the simulator emitted after each item;
   - the simulator's state after each item, in a normalized form.

   Each node is tested on its own inputs, so a difference shows up at the operator that causes it.
   This also checks every operator's intermediate output, not just the sinks.

3. **Dump the Flink state after each step, the same way for every operator.**
   - **Keyed state:** use the heap (HashMap) backend and iterate its state tables by reflection.
     That gives every entry as (state name, key, namespace, value), so window slices show up as
     namespaces.
   - **Timers:** iterate the operator's timer services with `forEachEventTimeTimer` and
     `forEachProcessingTimeTimer`, which gives (key, namespace, timestamp).
   - **Watermark:** read the watermark the harness emits.

4. **Compare in two tiers.**
   - **Tier 1, for every operator:** which keys and namespaces are in state, every timer as
     (key, timestamp), and the operator's watermark. This needs no per-operator code. It covers
     state being cleared, cleanup timers, window timers, and the 7½-minute claim.
   - **Tier 2, operator by operator:** the values. That means accumulators (count, sum), buffered
     rows, sorted Top-N lists, the row dedup keeps, LOAD versus JOIN phase, and so on. Each needs a
     small Java projector that turns Flink's RowData state into the same shape as the simulator's.
     Each operator in `js/operators.js` gets a `facts(st, cfg)` function next to its `view()`. Both
     functions read the same state structure, so they change together.

   When a check fails, it prints the step, the simulator's caption, and a diff of the two states,
   the same way the replay prints row diffs now.

## Expected pitfalls

- **The window aggregate buffers records in memory.** `RecordsWindowBuffer` keeps records outside
  state until a watermark arrives or a checkpoint flushes them. Right after a record, Flink's state
  won't show the accumulator yet, even though the simulator does.
  - Fix: trigger the pre-checkpoint flush (`prepareSnapshotPreBarrier`) before each inspection. A
    checkpoint would do the same thing.
  - Consider a sentence about this in the window recipe.
- **Some differences are deliberate simplifications.** For example, proc-time dedup shows
  "seen: yes" while Flink stores a value, the simulator says "slice" where Flink uses namespaces,
  and MATCH_RECOGNIZE's shared buffer is simplified. Each spec lists these exceptions explicitly, so
  nothing gets ignored silently.
  - MATCH_RECOGNIZE stops at tier 1 (keys and timers). Its shared buffer is too detailed to project
    usefully.
- **The lookup join is skipped.** Its cache belongs to the connector, not to Flink state, and the
  page already says so.
- **Both the reflection and the planner calls use internal APIs.** That's acceptable for a harness
  tied to specific versions. LATERAL SNAPSHOT runs on the 2.4 snapshot, and dedup by order_id on
  the 2.3 snapshot, using the same `flinkVersion` gate as `run-replay.sh`.
- **Two-input operators:** the simulator's edge has to be mapped to Flink's input 1 or 2 using the
  plan's input order.

## Alternatives ruled out

- **Read savepoints with the State Processor API.** It needs a stop-with-savepoint after every
  step, so it's slow. SQL operators have generated uids, and reading their state means rebuilding
  their internal serializers.
- **Queryable state.** It was removed in Flink 2.0.
- **Infer state from the outputs.** That's what the replay already does, and it's exactly what
  misses this kind of detail.

## Phases

1. **Prototype (the go/no-go): done.** `run-state.sh` and `StateCheck.java` check tier 1 (keys,
   namespaces, timers) and each step's output, for GroupAggregate and the window aggregations: 11
   tabs, all passing on 2.3.0. What it showed:
   - The transformations come from `StreamStatementSet.attachAsDataStream()` and
     `env.getTransformations()`, not the StreamGraph; each has its operator factory, key selector,
     and key type.
   - Rows must cross from one operator to the next as BinaryRowData, as after an exchange: the
     global window aggregation reads the slice end (a BIGINT) as a compact timestamp, which only
     works on the binary layout.
   - The buffering pitfall is real: without the flush, a window's state and timers don't exist
     until a watermark arrives.
   - A simulated operator can stand for several Flink operators; the exporter lists them, and
     they run as a chain of harnesses. (The window aggregations were drawn as one operator then;
     they're now two, LocalWindowAggregate and GlobalWindowAggregate, each checked on its own,
     with their in-memory buffers simulated and no checkpoint before each comparison.) When a job
     has two of a kind (the window join), the one fed by the same source tables is picked.
   - Statement sets are split into their INSERTs.
   - A negative control (one timer moved by 1 ms) fails, with a readable diff.
2. **Exporter: done.** The traces live in the specs themselves (`operators` in
   `verification/runtime/*.json`), not in a separate directory. Every stateful operator except the
   lookup join has `facts()`.
3. **Tier 1 for every recipe: done**, in `run-state.sh`. 30 operators in 20 tabs pass on 2.3.0,
   `dedup/byid` on the 2.3 snapshot, and LATERAL SNAPSHOT on the 2.4 snapshot. Negative controls
   on a one-input operator (a window timer) and on two two-input operators (a temporal-join timer,
   an interval-join key) fail. What it showed:
   - Two-input operators run in `KeyedTwoInputStreamOperatorTestHarness`, which takes an operator
     instance, taken from the planner's SimpleOperatorFactory (or made from a
     CodeGenOperatorFactory's generated class). Each item goes to the input that reads from the
     same source tables as the simulator's input edge.
   - FastTop1Function (Rank, `dedup/byid`) also holds rows in memory: in a cache, written to state
     only by snapshotState. So each inspection takes a full checkpoint (prepareSnapshotPreBarrier,
     then snapshot), not just the flush.
   - Session windows: Flink keeps a session's accumulator under the window it started as, with a
     per-key map (MergingWindowSet) from the merged session to it. The panel shows merged sessions,
     so the check compares the map's keys. Timers are at the merged session's end, as shown.
   - MATCH_RECOGNIZE sets a timer at each buffered row's timestamp, as the operator comment says.
     The panel doesn't show them, but they're compared. The interval join's cleanup timers and the
     LATERAL SNAPSHOT join's timers aren't shown or compared; their keys are.
   - Output rows: some Flink operators name fields themselves (OVER's `w0$o0`...), a join's output
     carries both sides' columns (`store_id0`, ...), and the window join's simulated rows are the
     ones after its `COALESCE`. The exporter maps the first and the last; fields the simulator never
     models are listed, and not compared (Replay checks the sinks' full rows).
   - Inputs: an upsert tombstone carries only its key, and some columns aren't in the simulator's
     data (CEP's `order_id`) or aren't materialized (`proc_time`). They're given to Flink as null,
     and listed.
   - No differences in behaviour turned up: every difference so far was in how state is laid out,
     or in how rows are named.
4. **Tier 2 (values): done.** Rather than a Java projector per
   operator, each `facts()` lists, per Flink state name, the entries the simulator's state implies,
   in a small form (`F` in `js/operators.js`): scalars, tuples, maps, lists, and rows. A row is
   given as the simulator's row, and StateCheck renders it in the Flink operator's input layout, so
   column order and pruning come from the planner. StateCheck decodes Flink's state with its
   serializers (`--dump` prints all of it) and compares every entry of the named states. 29 of the
   30 operators on 2.3.0 compare values; two negative controls (an OVER sum, a join row's count)
   fail. What it showed:
   - Accumulators are laid out per aggregate: COUNT(*) is a count; SUM is a sum, plus a count of
     rows when its input can retract (SUM_RETRACT). The exporter tells facts() whether an input
     retracts, from the edge's changelog mode.
   - Stored rows keep their row kind: the temporal join's versions are stored as +U, so the
     simulator now keeps each version's kind.
   - Not compared, and why: session windows (each accumulator lives under the window the session
     started as, which the simulator doesn't track); the temporal join's waiting orders and the
     LATERAL SNAPSHOT join's buffered probe rows (kept under internal per-key counters); the
     interval join's right rows (Flink marks the ones that matched, which the simulator doesn't
     track); MATCH_RECOGNIZE's NFA and shared buffer; timer bookkeeping states.
   - **LATERAL SNAPSHOT applies build changes lazily (2.4 snapshot); now modelled.** The simulator
     used to apply a pending build change as soon as the build watermark reached its row time.
     Flink leaves it in `build-change-buffer` until that key is next touched: a probe row, a build
     row, or the timer of a buffered probe row (`applyBufferedChangesUpToCurrentWm()`, called from
     `processElement1`, `processElement2`, and `onEventTime`; `processWatermark2` applies nothing).
     And it applies them only if the build watermark has moved past `buffered-at-wm`, the
     watermark at which the key's buffer was last written. So a change that arrives already behind
     the watermark, like a -U carrying the old row's time, waits for the next watermark, which
     matters for results: applying the -U early would leave the pizza with no cost. The simulator
     now does both (`applyDue` and `applyLimit` in `js/operators.js`), the panel marks changes
     that are due but not yet applied, and the captions say when changes are applied. All values
     match on the 2.4 snapshot, and the replay still passes.
5. **Docs, and the recipes' state prose: done.** The README describes `run-state.sh`. Each
   recipe's `state:` fact, its summary-table entry, and the state sentences in its details were
   checked against the harness:
   - Confirmed by the checks that pass: every claim about what's kept per key, slice, window, or
     session, and when it's freed; the timers (window ends, hop and cumulate steps, OVER's clean-up
     timer between 5 and 7½ minutes, the temporal join's one timer per pizza, event-time dedup's
     timer moving to an earlier copy); state that's never cleared (no timers, keys only grow); the
     interval join's bounds (each delivery leaves once the combined watermark passes its time);
     and LATERAL SNAPSHOT's lazy changes.
   - Checked separately: MATCH_RECOGNIZE's "never more than two partial matches" (Flink's NFA
     holds the simulator's partial matches plus its start state); event-time dedup's "That flag
     can expire with a state TTL. The waiting row and its timer can't" (in
     RowTimeDeduplicateKeepFirstRowFunction only `already-emitted-state-boolean` gets a TTL);
     ChangelogNormalize's "A row identical to the stored one is swallowed, but only when state TTL
     is off" (with `table.exec.state.ttl` set through a spec's new `config` field, step 6 of
     `filter/upsert` forwards the identical row, and nothing else changes).
   - Not checked: the lookup join's "None in Flink" (it isn't replayed), and the general advice
     about what a state TTL does to joins.
