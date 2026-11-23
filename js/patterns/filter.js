// Pattern: filtering and projection.
// Verified against verification/filter.sql (see filter.plan.txt).
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const money = { num: true, fmt: U.money };

  // ---------- Variant 1: append-only source ----------

  const orders = [
    { order_id: 101, store_id: 7, channel: 'delivery', total: 18.00, event_time: '12:00:01' },
    { order_id: 102, store_id: 7, channel: 'pickup', total: 24.50, event_time: '12:00:02' },
    { order_id: 103, store_id: 9, channel: 'delivery', total: 31.00, event_time: '12:00:04' },
    { order_id: 104, store_id: 9, channel: 'pickup', total: 22.00, event_time: '12:00:05' },
    { order_id: 105, store_id: 7, channel: 'delivery', total: 27.50, event_time: '12:00:07' },
  ];

  const appendOnly = {
    id: 'append',
    label: 'Append-only source',
    question: 'Which orders are for delivery, and what do they cost with tax?',
    intro: 'The orders topic holds five orders, some for delivery and some for pickup. Press Play, or step through with the arrow keys.',
    sql: `
INSERT INTO delivery_orders
SELECT order_id,
       store_id,
       total * 1.08 AS total_with_tax,
       event_time
FROM orders
WHERE channel = 'delivery';`,
    ddl: `
CREATE TABLE orders (
  order_id          BIGINT,
  store_id          INT,
  pizza_id          INT,
  channel           STRING,
  total             DECIMAL(10, 2),
  delivery_address  STRING,
  event_time        TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ('connector' = 'kafka', 'topic' = 'orders');

CREATE TABLE delivery_orders (
  order_id        BIGINT,
  store_id        INT,
  total_with_tax  DECIMAL(10, 2),
  event_time      TIMESTAMP(3)
) WITH ('connector' = 'kafka', 'topic' = 'delivery_orders');

-- Connection and format options are left out.`,
    matrix: { state: 'None', watermarks: 'Not used', watermarksOut: 'Passed through' },
    facts: {
      state: 'None. Each order is handled on its own, and nothing is remembered.',
      watermarks: 'Not used. The source generates them, because <code>orders</code> declares a watermark, and the Calc passes them along. Nothing waits for them.',
      output: 'Append-only, so any Kafka topic can take it.',
    },
    graphLabel: 'Job graph: Kafka source orders, then Calc, then Kafka sink delivery_orders. Both edges are append-only.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [6],
          cfg: {
            topic: 'orders', delay: 1, timeField: 'event_time',
            columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'channel' }, { f: 'total', ...money }, { f: 'event_time' }],
          },
        },
        {
          id: 'calc', kind: 'op', op: 'calc', title: 'Calc', lines: ['filter and project'], sqlLines: [2, 3, 4, 5, 7],
          cfg: {
            where: (r) => r.channel === 'delivery',
            project: (r) => ({ order_id: r.order_id, store_id: r.store_id, total_with_tax: Math.round(r.total * 108) / 100, event_time: r.event_time }),
          },
        },
        {
          id: 'sink', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: ['delivery_orders'], sqlLines: [1],
          cfg: {
            topic: 'delivery_orders',
            columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total_with_tax', ...money }, { f: 'event_time' }],
          },
        },
      ],
      edges: [
        { from: 'src', to: 'calc', mode: 'append', kinds: '+I' },
        { from: 'calc', to: 'sink', mode: 'append', kinds: '+I' },
      ],
    },
    inputs: [
      { value: orders[0] }, { value: orders[1] }, { value: orders[2] }, { wm: true },
      { value: orders[3] }, { value: orders[4] }, { wm: true },
    ],
    pill: (r) => (r.total_with_tax != null ? `${r.order_id} ${U.money(r.total_with_tax)}` : `${r.order_id} ${r.channel}`),
    narrate(node, ev) {
      if (node === 'src' && ev.type === 'read') return `The source reads order ${ev.row.order_id} from orders and emits it as an insert (+I).`;
      if (node === 'src' && ev.type === 'wm') {
        return `The source emits a watermark for ${ev.time}: the newest event time it has read, minus the 1 second the WATERMARK clause allows for out-of-order orders.`;
      }
      if (node === 'calc' && ev.type === 'pass') {
        return `Order ${ev.row.order_id} is a delivery order, so it passes the filter. The projection keeps the columns the query selects, and adds total_with_tax: ${U.money(ev.row.total_with_tax)}.`;
      }
      if (node === 'calc' && ev.type === 'drop') return `Order ${ev.row.order_id} is a ${ev.row.channel} order, so the filter drops it. Nothing about it is remembered.`;
      if (node === 'calc' && ev.type === 'wm') return `The watermark passes through the Calc unchanged. Filtering and projecting don't depend on time, so nothing here waits for it.`;
      if (node === 'sink' && ev.type === 'append') return `The sink appends order ${ev.row.order_id} to delivery_orders.`;
      if (node === 'sink' && ev.type === 'wm') return `Watermarks aren't written to Kafka. A job that only filters and projects never needs them.`;
      return null;
    },
    explain: `
Sink(table=[delivery_orders], fields=[order_id, store_id, total_with_tax, event_time], changelogMode=[NONE])
+- Calc(select=[order_id, store_id, CAST(*(total, 1.08) AS DECIMAL(10, 2)) AS total_with_tax,
          CAST(event_time AS TIMESTAMP(3)) AS event_time], where=[=(channel, 'delivery')], changelogMode=[I])
   +- TableSourceScan(table=[[orders, watermark=[-(event_time, 1000:INTERVAL SECOND)],
          watermarkEmitStrategy=[on-periodic]]], fields=[...], changelogMode=[I])`,
    details: `
<p>Filtering and projecting is the cheapest thing a Flink job can do. The Calc looks at one row at a
time, and keeps no state, so there's nothing to checkpoint and nothing to grow. Flink chains the
Calc onto the source, so the rows don't even cross the network between them.</p>
<p>The planner turns the whole <code>SELECT</code> list and the <code>WHERE</code> clause into this
one Calc. It also adds the cast from <code>total * 1.08</code>, a <code>DECIMAL(14, 4)</code>, to
the sink's <code>DECIMAL(10, 2)</code>.</p>
<p>Watermarks still flow, because <code>orders</code> declares one. Operators that don't care about
time just forward them. Here, nothing downstream uses them, and Kafka doesn't store them.</p>`,
  };

  // ---------- Variant 2: upsert source ----------

  const upsertSource = {
    id: 'upsert',
    label: 'Upsert source',
    question: 'Which orders are still open (not cancelled)?',
    blurb: 'Now the source is <code>order_status</code>, an <code>upsert-kafka</code> table that keeps the latest status of each order. Filtering it requires state.',
    intro: 'order_status is keyed by order_id. Each record is the latest status for its order, and a null value (a tombstone) means the order was removed.',
    sql: `
INSERT INTO open_orders
SELECT order_id, store_id, status
FROM order_status
WHERE status <> 'cancelled';`,
    ddl: `
CREATE TABLE order_status (
  order_id  BIGINT,
  store_id  INT,
  status    STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'order_status');

CREATE TABLE open_orders (
  order_id  BIGINT,
  store_id  INT,
  status    STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'open_orders');

-- Connection and format options are left out.`,
    matrix: { state: 'The last row per key, in ChangelogNormalize', watermarks: 'Not used', watermarksOut: 'Passed through' },
    facts: {
      state: 'ChangelogNormalize keeps the last row of every order that currently satisfies the filter. State is cleared when an order is cancelled or deleted.',
      watermarks: 'Not used. <code>order_status</code> declares no watermark, and nothing in this job depends on time.',
      output: 'Upsert, keyed by <code>order_id</code>. The sink needs a primary key, so it must be <code>upsert-kafka</code>.',
    },
    graphLabel: 'Job graph: upsert-kafka source order_status, hash partitioned by order_id into ChangelogNormalize with the filter, then upsert-kafka sink open_orders. Both edges are upsert streams.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'upsertKafkaSource', title: 'Kafka source', lines: ['order_status', 'upsert-kafka'], sqlLines: [3],
          cfg: { topic: 'order_status', key: 'order_id', showKey: true, columns: [{ f: 'store_id', num: true }, { f: 'status' }] },
        },
        {
          id: 'cn', kind: 'op', op: 'changelogNormalize', title: 'ChangelogNormalize', lines: ["status <> 'cancelled'"],
          stateful: true, stateKind: 'keys', planner: true, sqlLines: [2, 4],
          cfg: {
            key: 'order_id', condition: (r) => r.status !== 'cancelled',
            stateTitle: 'Last row per order_id',
            stateFields: ['order_id', 'store_id', 'status'],
          },
        },
        {
          id: 'sink', kind: 'sink', op: 'upsertKafkaSink', title: 'Kafka sink', lines: ['open_orders', 'upsert-kafka'], sqlLines: [1],
          cfg: { topic: 'open_orders', key: 'order_id', showKey: true, columns: [{ f: 'store_id', num: true }, { f: 'status' }] },
        },
      ],
      edges: [
        { from: 'src', to: 'cn', mode: 'upsert', kinds: '+U -D', exchange: 'hash(order_id)' },
        { from: 'cn', to: 'sink', mode: 'upsert', kinds: '+I +U -D' },
      ],
    },
    inputs: [
      { key: 201, value: { store_id: 7, status: 'placed' } },
      { key: 202, value: { store_id: 7, status: 'placed' } },
      { key: 201, value: { store_id: 7, status: 'baking' } },
      { key: 202, value: { store_id: 7, status: 'cancelled' } },
      { key: 203, value: { store_id: 9, status: 'cancelled' } },
      { key: 201, value: { store_id: 7, status: 'baking' } },
      { key: 201, value: null },
      { key: 204, value: { store_id: 9, status: 'placed' } },
    ],
    pill: (r) => (r.status ? `${r.order_id} ${r.status}` : `${r.order_id}`),
    narrate(node, ev) {
      if (node === 'src' && ev.type === 'read') {
        return `The source reads ${ev.key} → ${ev.row.status}. upsert-kafka can't tell whether order ${ev.key} is new, so the planner treats every value as an upsert (+U).`;
      }
      if (node === 'src' && ev.type === 'tombstone') {
        return `A tombstone for ${ev.key}: the order was delivered, and the status service removed it. The source emits a delete (−D) that carries only the key.`;
      }
      if (node === 'cn') {
        switch (ev.type) {
          case 'insert': return `Order ${ev.key} isn't in state, and ${ev.row.status} passes the filter. ChangelogNormalize stores the row and emits an insert (+I).`;
          case 'update': return `Order ${ev.key} is already in state, and ${ev.row.status} still passes the filter. The stored row is replaced, and an update (+U) goes downstream.`;
          case 'unmatched': return `Order ${ev.key} is now cancelled, so it fails the filter. Dropping the upsert wouldn't remove the order downstream. Because ChangelogNormalize remembers emitting it, it sends a delete (−D) for the old row, and clears the key.`;
          case 'skip-new': return `Order ${ev.key} was cancelled before it was ever emitted. It fails the filter and there's nothing to retract, so nothing is emitted or stored.`;
          case 'unchanged': return `A duplicate of order ${ev.key}'s last update, perhaps a producer retry. It's identical to the row in state, so nothing is emitted. (ChangelogNormalize only skips duplicates when state TTL is off.)`;
          case 'delete': return `ChangelogNormalize finds order ${ev.key} in state, emits a delete with the full row, which the tombstone didn't have, and clears the key.`;
          case 'delete-missing': return `A delete for order ${ev.key}, which isn't in state. There's nothing to retract.`;
        }
      }
      if (node === 'sink' && ev.type === 'upsert') return `The sink writes ${ev.key} → ${ev.row.status} to open_orders. After compaction, only the latest value for each key is left.`;
      if (node === 'sink' && ev.type === 'tombstone') return `The sink writes a tombstone for ${ev.key}, which removes order ${ev.key} from open_orders.`;
      return null;
    },
    explain: `
Sink(table=[open_orders], fields=[order_id, store_id, status], changelogMode=[NONE])
+- ChangelogNormalize(key=[order_id], condition=[<>(status, 'cancelled')], changelogMode=[I,UA,D])
   +- Exchange(distribution=[hash[order_id]], changelogMode=[UA,D])
      +- TableSourceScan(table=[[order_status]], fields=[order_id, store_id, status], changelogMode=[UA,D])`,
    details: `
<p>An upsert stream only says what each key's row is <em>now</em>. When order 202 changes from
placed to cancelled, the source sends the new row, and nothing about the old one. If a stateless
filter dropped that row, the old row for 202 would stay in <code>open_orders</code> forever.</p>
<p>So the planner puts the filter into a ChangelogNormalize, which remembers the last row it emitted
for each key. When a key's new row fails the filter, it can delete the old one. It also fills in
deletes: a Kafka tombstone only carries the key, so ChangelogNormalize emits the full row from
state.</p>
<p>The Exchange in front of it hashes by <code>order_id</code>, so every change to an order reaches
the same parallel instance, and the state for that order.</p>
<p>A few details from the Flink 2.3 code:</p>
<ul>
  <li><code>upsert-kafka</code> declares its output as updates and deletes (<code>UA</code>,
  <code>D</code>). At runtime the value format hands over each row as an insert, which
  ChangelogNormalize treats exactly like an update.</li>
  <li>Only rows that pass the filter are stored. A key that never matched costs nothing.</li>
  <li>A row identical to the stored one is swallowed, but only when state TTL is off. With a TTL,
  it's forwarded, to keep downstream state from expiring early.</li>
</ul>`,
  };

  window.PATTERNS.push({
    id: 'filter',
    nav: 'Filtering and projection',
    title: 'Filtering and projection',
    summary: '<p>Keep the rows you want and compute the columns you need. With an append-only source, this needs no state at all. With an upsert source, Flink must remember the last row for each key, so that it can delete rows that stop matching.</p>',
    variants: [appendOnly, upsertSource],
  });
})();
