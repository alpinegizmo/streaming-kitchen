# The simulator

The page's animations, captions, and state panels all come from one place: a small, deterministic
model of a Flink SQL job, written in plain JavaScript. It runs a recipe's input through that
recipe's job graph, and records every step. The verification tools check the same record against
real Flink.

## How a run works (`js/sim.js`)

- **Input.** Each tab supplies a job graph (operators and edges), a list of inputs (Kafka records,
  and points where a source emits a watermark), and per-operator settings.
- **Running it.** `Sim.run(variant)` reads the inputs in order. Each item moves through the graph
  depth first: an operator's output is handed straight downstream before the next input is read.
  That's roughly how chained Flink operators pass a record down the chain.
- **What a step records.** A step is one operator handling one item: a source reading a record, or
  an operator receiving a row or a watermark on one of its input edges. Each step stores:
  - the item, and the edge it came in on;
  - what the operator emitted;
  - a small event describing what happened ("late", "merge", "fire", and so on), and the caption
    made from it;
  - a snapshot of the entire job's state.
- **Why snapshots.** They let the player jump to any step, backwards included, just by drawing
  that step's snapshot.
- **Edges and watermarks.** Items wait on each edge in order. An operator with two inputs knows
  which one an item came from, and reports its own combined watermark: the minimum of both inputs,
  and none until both have sent one.

## Operators (`js/operators.js`)

Each operator models one Flink 2.3 runtime class (the 2.4 one for LATERAL SNAPSHOT), and its
comment names the class. Each provides:

| Function | What it does |
| --- | --- |
| `init` | Its starting state, as plain data |
| `record`, `watermark` | What it does with a row or a watermark: what it emits, and an event |
| `view` | The state panel drawn under it |
| `say` | Its default caption, which a tab can replace with its own narration |
| `facts` | Its state laid out the way Flink holds it, used only for verification |
| `stateKind`, `stateParts` | How its state is bounded, for the panel's tags: freed by watermark, a small set of keys, grows with keys, or grows with rows. A recipe's node adds what depends on the data, such as whether its set of keys is small |

There are about 25 operators:

- sources and sinks (Kafka, upsert-kafka, changelog formats), and Calc;
- ChangelogNormalize and SinkUpsertMaterializer;
- both phases of window aggregation (LocalWindowAggregate and GlobalWindowAggregate), session
  windows, OVER, and GroupAggregate;
- the deduplication functions, and two Rank strategies;
- the regular, interval, window, temporal, lookup, and LATERAL SNAPSHOT joins;
- MATCH_RECOGNIZE.

They model the details that change what you see:

- row kinds (+I, −U, +U, −D), and when each is emitted;
- lateness rules, timers, and when state is cleared;
- per-slice window state;
- in-memory buffers: the local window phase's partial results, and the global phase's arrivals;
- the lazy application of build-side changes in LATERAL SNAPSHOT.

## What it leaves out

- **One instance of each operator.** There are no parallel instances, network buffers,
  backpressure, or real time. Processing time is abstract.
- **No checkpoints, except where they matter.** Buffers that only a checkpoint would flush are
  modelled only where the page shows them. Rank's FastTop1 cache isn't modelled; its state appears
  immediately.
- **Approximate event order across operators.** The depth-first order is a reasonable stand-in for
  how Flink interleaves work between operators. The page's footer says so: the animations simplify
  how Flink schedules work, but not what each operator does.

## How it's checked against Flink

1. **Exporting.** `tools/export-replay.mjs` runs the same code in Node. For each tab it writes the
   inputs, every sink's output, and a step-by-step trace for each stateful operator, to
   `verification/runtime/`.
2. **Sink-level replay.** `verification/run-replay.sh` (`Replay.java`) runs each recipe's real SQL
   on a local Flink mini-cluster, and compares every sink's rows, with their row kinds.
3. **Operator-level check.** `verification/run-state.sh` (`StateCheck.java`) runs the operators
   Flink's planner builds, in Flink's own test harness, feeding them exactly what each simulated
   operator received. After every step it compares what the operator emitted, the keys in state,
   every timer, and the stored values.

Every tab passes: on Flink 2.3.0, on a 2.3 snapshot build for deduplication ordered by order_id,
and on a 2.4 snapshot build for LATERAL SNAPSHOT. The exception is the lookup join, which needs an
external service, and isn't replayed.

The checks have caught real differences, which were then fixed in the simulator: LATERAL
SNAPSHOT's lazy application of build changes, and when each window phase flushes its buffers.
How to run the checks is in the [README](README.md); what they found is in
[verification/STATE-PLAN.md](verification/STATE-PLAN.md).
