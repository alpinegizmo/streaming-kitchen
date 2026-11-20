// Pattern: non-windowed aggregation.
// Verified against verification/aggregation.sql (see aggregation.plan.txt) and
// verification/upsert-materializer.sql.
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const money = { num: true, fmt: U.money };
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;

  // ---------- Shared by the two running-total variants ----------

  const orders = [
    { order_id: 101, store_id: 7, total: 18.00 },
    { order_id: 102, store_id: 9, total: 24.50 },
    { order_id: 103, store_id: 7, total: 31.00 },
    { order_id: 104, store_id: 7, total: 13.00 },
    { order_id: 105, store_id: 9, total: 19.00 },
  ];

  const ordersDdl = `
CREATE TABLE orders (
  order_id          BIGINT,
  store_id          INT,
  pizza_id          INT,
  channel           STRING,
  total             DECIMAL(10, 2),
  delivery_address  STRING,
  event_time        TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ('connector' = 'kafka', 'topic' = 'orders');`;

  const totalSql = (sink) => `
INSERT INTO ${sink}
SELECT store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM orders
GROUP BY store_id;`;

  const result = (r) => `store ${r.store_id}: ${plural(r.orders, 'order')}, ${U.money(r.sales)}`;

  function runningTotal({ id, label, sink, sinkOp, generateUpdateBefore, ddl, question, blurb, facts, matrix, explain, details, sinkCfg, narrateSink, mode, kinds }) {
    return {
      id, label, question, blurb,
      intro: 'Stores 7 and 9 send orders. Every order changes its store\'s running total. Press Play, or step through with the arrow keys.',
      sql: totalSql(sink),
      ddl: `${ordersDdl}\n\n${ddl}\n\n-- Connection and format options are left out.`,
      facts,
      matrix,
      graphLabel: `Job graph: Kafka source orders, hash partitioned by store_id into GroupAggregate, then Kafka sink ${sink}. The first edge is append-only, the second is ${mode}.`,
      graph: {
        nodes: [
          {
            id: 'src', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [5],
            cfg: { topic: 'orders', columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total', ...money }] },
          },
          {
            id: 'agg', kind: 'op', op: 'groupAggregate', title: 'GroupAggregate', lines: ['GROUP BY store_id'],
            stateful: true, sqlLines: [2, 3, 4, 6],
            cfg: {
              key: 'store_id', keyLabel: 'store', generateUpdateBefore,
              aggs: [{ name: 'orders', fn: 'count' }, { name: 'sales', fn: 'sum', field: 'total' }],
            },
          },
          { id: 'sink', kind: 'sink', op: sinkOp, title: 'Kafka sink', lines: sinkCfg.lines, sqlLines: [1], cfg: sinkCfg.cfg },
        ],
        edges: [
          { from: 'src', to: 'agg', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' },
          { from: 'agg', to: 'sink', mode, kinds },
        ],
      },
      inputs: orders.map((o) => ({ value: o })),
      pill: (r) => (r.order_id ? `${r.order_id} s${r.store_id} ${U.money(r.total)}` : `s${r.store_id} ${r.orders} ${U.money(r.sales)}`),
      narrate(node, ev) {
        if (node === 'src') {
          const r = ev.row;
          return `The source reads order ${r.order_id}: store ${r.store_id}, ${U.money(r.total)}.`;
        }
        if (node === 'agg') {
          const r = ev.in;
          if (ev.type === 'first') {
            return `Store ${r.store_id} has no accumulator yet. GroupAggregate creates one (1 order, ${U.money(r.total)}) and emits it as an insert (+I).`;
          }
          if (ev.type === 'update') {
            return generateUpdateBefore
              ? `Order ${r.order_id} makes ${result(ev.next)}. The debezium-json sink accepts retractions, so GroupAggregate first retracts the old result (−U, ${plural(ev.prev.orders, 'order')}, ${U.money(ev.prev.sales)}), then sends the new one (+U).`
              : `Order ${r.order_id} makes ${result(ev.next)}. The sink is keyed by store_id, so GroupAggregate only sends the new result (+U). The old one doesn't need retracting.`;
          }
        }
        if (node === 'sink') return narrateSink(ev);
        return null;
      },
      explain, details,
    };
  }

  // ---------- Variant 1: upsert sink ----------

  const upsertSink = runningTotal({
    id: 'upsert',
    label: 'Upsert sink',
    question: 'How much has each store sold so far?',
    sink: 'store_totals',
    sinkOp: 'upsertKafkaSink',
    generateUpdateBefore: false,
    mode: 'upsert',
    kinds: '+I +U',
    ddl: `
CREATE TABLE store_totals (
  store_id  INT,
  orders    BIGINT,
  sales     DECIMAL(38, 2),
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'store_totals');`.trim(),
    sinkCfg: {
      lines: ['store_totals', 'upsert-kafka'],
      cfg: { topic: 'store_totals', key: 'store_id', showKey: true, columns: [{ f: 'orders', num: true }, { f: 'sales', ...money }] },
    },
    narrateSink: (ev) => `The sink writes ${result(ev.row)}, keyed by store ${ev.key}. After compaction it replaces any earlier value for store ${ev.key}.`,
    matrix: { state: 'An accumulator per key, kept until state TTL', watermarks: 'Not used', watermarksOut: 'Passed through, but the results have no time attribute to use it with' },
    facts: {
      state: 'One accumulator per store: an order count and a running sum. Nothing ever removes it, so set a state TTL if the set of keys keeps growing.',
      watermarks: 'Not used. A non-windowed aggregate never decides that a group is complete, so nothing waits for time.',
      output: 'Upsert, keyed by <code>store_id</code>. Each store\'s row is replaced every time its total changes.',
    },
    explain: `
Sink(table=[store_totals], fields=[store_id, orders, sales], changelogMode=[NONE])
+- GroupAggregate(groupBy=[store_id], select=[store_id, COUNT(*) AS orders, SUM(total) AS sales], changelogMode=[I,UA])
   +- Exchange(distribution=[hash[store_id]], changelogMode=[I])
      +- Calc(select=[store_id, total], changelogMode=[I])
         +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])`,
    details: `
<p>A <code>GROUP BY</code> without a window never finishes. On a stream it keeps a result for each
store, and emits a new version of that result for every order. That's an updating stream, so the
sink has to be able to take updates.</p>
<p>The planner works out which row kinds each operator has to produce, starting from the sink.
<code>upsert-kafka</code> is keyed by <code>store_id</code>, and so is the aggregate's result, so
every new result simply replaces the old one. GroupAggregate never needs to send <code>−U</code>,
and its output is <code>[I, UA]</code>: there's no <code>−D</code> either, because a count over an
append-only stream never drops to zero.</p>
<p>The state is one accumulator per store. That's fine for a few
hundred stores, but if you GROUP BY something unbounded, like customers or sessions, it grows
forever. Set <code>table.exec.state.ttl</code>, or a <code>STATE_TTL</code> hint, to expire state for idle
keys.</p>
<p>Without mini-batch, the aggregation runs in one phase. Turn on
<code>table.exec.mini-batch.enabled</code>, and the planner splits it into a local and a global
aggregate, like the window aggregation.</p>
<h4>When does Flink add a SinkUpsertMaterializer?</h4>
<p>When the stream's upsert key doesn't match the sink's primary key. Say the query grouped by
<code>store_id</code> and <code>channel</code>, but still wrote into <code>store_totals</code>,
keyed by <code>store_id</code> alone. Several upstream rows would then map to one sink row, in no
particular order. Flink 2.3 rejects that query unless you say how to resolve the conflicts:
<code>ON CONFLICT DO DEDUPLICATE</code>, <code>DO ERROR</code> or <code>DO NOTHING</code>. Each one
plans a SinkUpsertMaterializer in front of the sink (<code>upsertMaterialize=[true]</code>), and
makes the aggregate send <code>−U</code> again, so the materializer can track every row. That
costs state, and it's usually better to make the keys match.</p>`,
  });

  // ---------- Variant 2: retract sink ----------

  const retractSink = runningTotal({
    id: 'retract',
    label: 'Retract sink',
    question: 'How much has each store sold so far?',
    blurb: 'Same query, but the sink is a plain Kafka topic in <code>debezium-json</code> format. It has no key, so every change has to say what it replaces.',
    sink: 'store_totals_changelog',
    sinkOp: 'changelogKafkaSink',
    generateUpdateBefore: true,
    mode: 'retract',
    kinds: '+I -U +U',
    ddl: `
CREATE TABLE store_totals_changelog (
  store_id  INT,
  orders    BIGINT,
  sales     DECIMAL(38, 2)
) WITH ('connector' = 'kafka', 'topic' = 'store_totals_changelog',
        'format' = 'debezium-json');`.trim(),
    sinkCfg: {
      lines: ['store_totals_changelog', 'debezium-json'],
      cfg: {
        topic: 'store_totals_changelog', key: 'store_id',
        columns: [{ f: 'op' }, { f: 'image' }, { f: 'store_id', num: true }, { f: 'orders', num: true }, { f: 'sales', ...money }],
        tableColumns: [{ f: 'store_id', num: true }, { f: 'orders', num: true }, { f: 'sales', ...money }],
        tableNote: 'as a consumer rebuilds it from the changes',
      },
    },
    narrateSink: (ev) => (ev.type === 'delete'
      ? `The sink writes the retraction as a Debezium delete (op d), with the old result as its before image.`
      : `The sink writes ${result(ev.row)} as a Debezium create (op c), with the new result as its after image.`),
    matrix: { state: 'An accumulator per key, kept until state TTL', watermarks: 'Not used', watermarksOut: 'Passed through, but the results have no time attribute to use it with' },
    facts: {
      state: 'One accumulator per store: an order count and a running sum, kept until a state TTL expires it.',
      watermarks: 'Not used. A non-windowed aggregate never decides that a group is complete.',
      output: 'Retract. Every change is sent as <code>−U</code> for the old result, then <code>+U</code> for the new one, and both are written to Kafka.',
    },
    explain: `
Sink(table=[store_totals_changelog], fields=[store_id, orders, sales], changelogMode=[NONE])
+- GroupAggregate(groupBy=[store_id], select=[store_id, COUNT(*) AS orders, SUM(total) AS sales], changelogMode=[I,UB,UA])
   +- Exchange(distribution=[hash[store_id]], changelogMode=[I])
      +- Calc(select=[store_id, total], changelogMode=[I])
         +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])`,
    details: `
<p>The plan is the same as with the upsert sink, except for one thing: GroupAggregate now produces
<code>[I, UB, UA]</code>. The <code>debezium-json</code> format declares that it accepts
retractions, so the planner asks for <code>−U</code> as well as <code>+U</code>. That comes from the
format alone; a primary key on the sink table wouldn't change it.</p>
<p>The format writes a <code>−U</code> as a Debezium delete (<code>op: d</code>, with a
<code>before</code> image), and a <code>+I</code> or <code>+U</code> as a create (<code>op: c</code>,
with an <code>after</code> image). A consumer that applies the changes in order ends up with the
same table as the upsert sink, but the topic carries nearly twice as many records, and the
consumer has to process every one of them.</p>
<p>A plain <code>json</code> Kafka sink can't take this query at all. The planner rejects an
updating result headed for an append-only sink.</p>`,
  });

  // ---------- Variant 3: updating input ----------

  const statusSql = `
INSERT INTO status_counts
SELECT status,
       COUNT(*) AS orders
FROM order_status
GROUP BY status;`;

  const updatingInput = {
    id: 'updating',
    label: 'Updating input',
    question: 'How many orders are in each status right now?',
    blurb: 'Now the input is <code>order_status</code>, the <code>upsert-kafka</code> table with the latest status of each order. When an order changes status, one count has to go down and another up.',
    intro: 'order_status is keyed by order_id, and each record is the latest status of its order. A null value (a tombstone) means the order was removed. Press Play, or step through with the arrow keys.',
    sql: statusSql,
    ddl: `
CREATE TABLE order_status (
  order_id  BIGINT,
  store_id  INT,
  status    STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'order_status');

CREATE TABLE status_counts (
  status  STRING,
  orders  BIGINT,
  PRIMARY KEY (status) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'status_counts');

-- Connection and format options are left out.`,
    matrix: { state: 'The last row per key, and an accumulator per key', watermarks: 'Not used', watermarksOut: 'Passed through, but the results have no time attribute to use it with' },
    facts: {
      state: 'ChangelogNormalize keeps the last row of every order, so it can retract it. GroupAggregate keeps a count per status, and drops a status when its count reaches zero.',
      watermarks: 'Not used. <code>order_status</code> declares no watermark, and nothing waits for time.',
      output: 'Upsert, keyed by <code>status</code>. A status whose count drops to zero is deleted.',
    },
    graphLabel: 'Job graph: upsert-kafka source order_status, hash partitioned by order_id into ChangelogNormalize, which emits a retract stream, hash partitioned by status into GroupAggregate, then upsert-kafka sink status_counts.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'upsertKafkaSource', title: 'Kafka source', lines: ['order_status', 'upsert-kafka'], sqlLines: [4],
          cfg: { topic: 'order_status', key: 'order_id', showKey: true, columns: [{ f: 'store_id', num: true }, { f: 'status' }] },
        },
        {
          id: 'cn', kind: 'op', op: 'changelogNormalize', title: 'ChangelogNormalize', lines: ['key: order_id'],
          stateful: true, planner: true, sqlLines: [4],
          cfg: { key: 'order_id', generateUpdateBefore: true, stateTitle: 'Last row per order_id', stateFields: ['order_id', 'status'] },
        },
        {
          id: 'agg', kind: 'op', op: 'groupAggregate', title: 'GroupAggregate', lines: ['GROUP BY status'],
          stateful: true, sqlLines: [2, 3, 5],
          cfg: { key: 'status', aggs: [{ name: 'orders', fn: 'count' }], generateUpdateBefore: false },
        },
        {
          id: 'sink', kind: 'sink', op: 'upsertKafkaSink', title: 'Kafka sink', lines: ['status_counts', 'upsert-kafka'], sqlLines: [1],
          cfg: { topic: 'status_counts', key: 'status', showKey: true, columns: [{ f: 'orders', num: true }] },
        },
      ],
      edges: [
        { from: 'src', to: 'cn', mode: 'upsert', kinds: '+U -D', exchange: 'hash(order_id)' },
        { from: 'cn', to: 'agg', mode: 'retract', kinds: '+I -U +U -D', exchange: 'hash(status)' },
        { from: 'agg', to: 'sink', mode: 'upsert', kinds: '+I +U -D' },
      ],
    },
    inputs: [
      { key: 201, value: { store_id: 7, status: 'placed' } },
      { key: 202, value: { store_id: 7, status: 'placed' } },
      { key: 201, value: { store_id: 7, status: 'baking' } },
      { key: 203, value: { store_id: 9, status: 'placed' } },
      { key: 202, value: { store_id: 7, status: 'baking' } },
      { key: 201, value: null },
      { key: 203, value: { store_id: 9, status: 'baking' } },
    ],
    pill: (r) => (r.order_id != null ? `${r.order_id} ${r.status ?? ''}`.trim() : `${r.status} ${r.orders}`),
    narrate(node, ev) {
      if (node === 'src' && ev.type === 'read') {
        return `The source reads ${ev.key} → ${ev.row.status}. upsert-kafka can't tell whether order ${ev.key} is new, so the planner treats every value as an upsert (+U).`;
      }
      if (node === 'src' && ev.type === 'tombstone') {
        return `A tombstone for ${ev.key}: the order was delivered, and removed from order_status. The source emits a delete (−D) that carries only the key.`;
      }
      if (node === 'cn') {
        switch (ev.type) {
          case 'insert': return `Order ${ev.key} isn't in state yet. ChangelogNormalize stores it and emits an insert (+I).`;
          case 'update': return `Order ${ev.key} moves from ${ev.prev.status} to ${ev.row.status}. ChangelogNormalize looks up the old row in state, and emits it as a retraction (−U), then the new row (+U). The aggregate needs both.`;
          case 'delete': return `ChangelogNormalize finds order ${ev.key} in state, and emits a delete with the full row (${ev.prev.status}), so the aggregate knows which count to decrement. Then it clears the key.`;
        }
      }
      if (node === 'agg') {
        const s = ev.key;
        switch (ev.type) {
          case 'first': return `There's no count for ${s} yet. GroupAggregate starts one at 1, and emits an insert (+I).`;
          case 'update': return ev.accumulate
            ? `The count for ${s} goes up to ${ev.next.orders}. GroupAggregate emits the new count (+U). The upsert sink doesn't need the old one.`
            : `The retraction takes the count for ${s} down to ${ev.next.orders}. GroupAggregate emits the new count (+U).`;
          case 'delete': return `The retraction takes ${s} down to 0 orders, so the group is gone. GroupAggregate emits a delete (−D) for it, and clears the key.`;
        }
      }
      if (node === 'sink' && ev.type === 'upsert') return `The sink writes ${ev.key} → ${plural(ev.row.orders, 'order')} to status_counts.`;
      if (node === 'sink' && ev.type === 'tombstone') return `The sink writes a tombstone for ${ev.key}, which removes it from status_counts.`;
      return null;
    },
    explain: `
Sink(table=[status_counts], fields=[status, orders], changelogMode=[NONE])
+- GroupAggregate(groupBy=[status], select=[status, COUNT_RETRACT(*) AS orders], changelogMode=[I,UA,D])
   +- Exchange(distribution=[hash[status]], changelogMode=[I,UB,UA,D])
      +- Calc(select=[status], changelogMode=[I,UB,UA,D])
         +- ChangelogNormalize(key=[order_id], changelogMode=[I,UB,UA,D])
            +- Exchange(distribution=[hash[order_id]], changelogMode=[UA,D])
               +- Calc(select=[order_id, status], changelogMode=[UA,D])
                  +- TableSourceScan(table=[[order_status]], fields=[order_id, store_id, status], changelogMode=[UA,D])`,
    details: `
<p>When order 201 moves from placed to baking, the count for placed has to go down. But the upsert
source only says what order 201 is now. To subtract from the right count, the aggregate needs the
old row as well.</p>
<p>So the planner puts a ChangelogNormalize in front of the aggregate. It keeps the last row of every
order, and turns each upsert into a proper retract stream: <code>−U</code> with the old row, then
<code>+U</code> with the new one. For a tombstone, which only carries the key, it emits a
<code>−D</code> with the full row from state.</p>
<p>Notice the two exchanges. ChangelogNormalize is partitioned by <code>order_id</code>, and the
aggregate by <code>status</code>. The <code>−U</code> and <code>+U</code> for one order usually go to
different parallel instances of the aggregate: the retraction to the instance for the old status,
and the update to the one for the new status.</p>
<p>Because its input can retract, the aggregate uses <code>COUNT_RETRACT</code>, and keeps a record
count per status. When the count reaches zero, the status is deleted, which is why the aggregate's
output includes <code>−D</code>.</p>
<p>This job holds state in two places, and the ChangelogNormalize state is one row per order, which
can be large. If the source were a CDC format such as <code>debezium-json</code>, which already
carries the old row in its updates, the planner wouldn't need ChangelogNormalize at all.</p>`,
  };

  window.PATTERNS.push({
    id: 'aggregation',
    nav: 'Non-windowed aggregation',
    title: 'Non-windowed aggregation',
    summary: '<p>A <code>GROUP BY</code> without a window keeps a running result for each key, and emits a new version every time it changes. The result is an updating stream, so the sink has to accept updates, and the state is kept until a TTL expires it.</p>',
    variants: [upsertSink, retractSink, updatingInput],
  });
})();
