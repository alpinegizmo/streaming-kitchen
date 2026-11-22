// Pattern: deduplication, by processing time and by event time.
// Verified against verification/dedup.sql (see dedup.plan.txt).
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const money = { num: true, fmt: U.money };
  const order = (order_id, store_id, total, event_time) => ({ value: { order_id, store_id, total, event_time } });
  const WM = { wm: true };

  // The ordering app retries when it times out, and each attempt gets its own event_time.
  // Both variants read exactly the same records, so their results can be compared.
  const inputs = [
    order(101, 7, 18.00, '12:00:01'),
    order(102, 9, 24.50, '12:00:03'),
    order(101, 7, 18.00, '12:00:04'),
    WM,
    order(103, 7, 31.00, '12:00:06'),
    order(103, 7, 31.00, '12:00:05'),
    order(102, 9, 24.50, '12:00:02'),
    WM,
    order(104, 9, 13.00, '12:00:08'),
    order(105, 7, 22.00, '12:00:10'),
    WM,
  ];

  const ddl = (sink) => `
CREATE TABLE orders (
  order_id          BIGINT,
  store_id          INT,
  pizza_id          INT,
  channel           STRING,
  total             DECIMAL(10, 2),
  delivery_address  STRING,
  event_time        TIMESTAMP(3),
  proc_time         AS PROCTIME(),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ('connector' = 'kafka', 'topic' = 'orders');

CREATE TABLE ${sink} (
  order_id    BIGINT,
  store_id    INT,
  total       DECIMAL(10, 2),
  event_time  TIMESTAMP(3)
) WITH ('connector' = 'kafka', 'topic' = '${sink}');

-- Connection and format options are left out.`;

  const sql = (sink, orderBy) => `
INSERT INTO ${sink}
SELECT order_id, store_id, total, event_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id
                       ORDER BY ${orderBy} ASC) AS row_num
  FROM orders)
WHERE row_num = 1;`;

  const columns = [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total', ...money }, { f: 'event_time' }];

  function graph(sink, op, by) {
    return {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [7],
          cfg: { topic: 'orders', delay: 1, timeField: 'event_time', columns },
        },
        {
          id: 'dedup', kind: 'op', op, title: 'Deduplicate', lines: ['keep first row', by],
          stateful: true, sqlLines: [4, 5, 6, 8],
          cfg: { key: 'order_id', timeField: 'event_time' },
        },
        { id: 'sink', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: [sink], sqlLines: [1], cfg: { topic: sink, columns } },
      ],
      edges: [
        { from: 'src', to: 'dedup', mode: 'append', kinds: '+I', exchange: 'hash(order_id)' },
        { from: 'dedup', to: 'sink', mode: 'append', kinds: '+I' },
      ],
    };
  }

  const pill = (r) => `${r.order_id} ${r.event_time}`;
  const srcNarration = (ev) => (ev.type === 'wm'
    ? `The source emits a watermark for ${ev.time}: the newest event time it has read, minus 1 second.`
    : `The source reads a copy of order ${ev.row.order_id}, with event time ${ev.row.event_time}.`);

  // ---------- Processing time ----------

  const procTime = {
    id: 'proctime',
    label: 'Processing time',
    question: 'Count each order once, even when the app sends it twice.',
    intro: 'The ordering app retries when it times out, so some orders arrive more than once, each copy with its own event time. Press Play, or step through with the arrow keys.',
    sql: sql('unique_orders', 'proc_time'),
    ddl: ddl('unique_orders'),
    matrix: { state: 'A flag per key that has been seen, kept until state TTL', watermarks: 'Not used', watermarksOut: 'Passed through' },
    facts: {
      state: 'One flag per order_id: seen or not. It grows with every new order and is never cleared, so set a state TTL a little longer than retries can take.',
      watermarks: 'Not used. The first copy to arrive wins, whatever its event time.',
      output: 'Append-only. Each order is emitted once, as soon as its first copy arrives.',
    },
    graphLabel: 'Job graph: Kafka source orders, hash partitioned by order_id into Deduplicate (keep first row by processing time), then Kafka sink unique_orders. Both edges are append-only.',
    graph: graph('unique_orders', 'procTimeDedup', 'by arrival'),
    inputs,
    pill,
    narrate(node, ev) {
      if (node === 'src') return srcNarration(ev);
      if (node === 'dedup') {
        if (ev.type === 'first') return `Order ${ev.key} hasn't been seen before. This copy is emitted at once, and order ${ev.key} is marked as seen.`;
        if (ev.type === 'duplicate') return `Order ${ev.key} has been seen already, so this copy (${ev.row.event_time}) is dropped, even if its event time is earlier.`;
        return 'The watermark passes through. Deduplicating by arrival order has no use for it.';
      }
      if (node === 'sink' && ev.type === 'append') return `The sink appends order ${ev.row.order_id}, the copy with event time ${ev.row.event_time}.`;
      if (node === 'sink' && ev.type === 'wm') return `Watermarks aren't written to Kafka.`;
      return null;
    },
    explain: `
Sink(table=[unique_orders], fields=[order_id, store_id, total, event_time], changelogMode=[NONE])
+- Calc(select=[order_id, store_id, total, event_time], changelogMode=[I])
   +- Rank(strategy=[AppendFastStrategy], rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=1],
        partitionBy=[order_id], orderBy=[proc_time ASC], select=[...], changelogMode=[I])
      +- Exchange(distribution=[hash[order_id]], changelogMode=[I])
         +- Calc(select=[order_id, store_id, total, Reinterpret(event_time) AS event_time,
                         PROCTIME() AS proc_time], changelogMode=[I])
            +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])

In the execution plan the Rank becomes:
   Deduplicate(keep=[FirstRow], key=[order_id], order=[PROCTIME], outputInsertOnly=[true])`,
    details: `
<p>Flink SQL has no <code>DEDUPLICATE</code> keyword. You write a <code>ROW_NUMBER()</code> over a
partition, ordered by a time attribute, and keep row 1. The planner recognizes the shape and replaces
the Rank with a Deduplicate operator, as the execution plan shows.</p>
<p>Ordered by <code>proc_time</code> ascending, "first" means the first copy to arrive. Deduplicate
emits it straight away, and remembers only that the order has been seen. Every later copy is
dropped, whatever its event time.</p>
<p>That flag is kept forever unless you set a state TTL, and there's one per order ever seen. Retries
arrive within seconds or minutes, so a TTL of an hour or a day keeps the state bounded without
letting duplicates through.</p>
<p>The graph leaves out a Calc that computes <code>proc_time</code> and one that drops columns.</p>`,
  };

  // ---------- Event time ----------

  const eventTime = {
    id: 'eventtime',
    label: 'Event time',
    question: 'Keep the earliest attempt of each order, by event time.',
    blurb: 'Same orders, but ordered by <code>event_time</code>: the copy that was placed first wins, even if it arrives second. That takes the watermark.',
    intro: 'The same copies arrive in the same order. This time, Flink keeps the copy with the earliest event time. Press Play, or step through with the arrow keys.',
    sql: sql('earliest_orders', 'event_time'),
    ddl: ddl('earliest_orders'),
    matrix: { state: 'Per key: the earliest row until the watermark passes it, then a flag', watermarks: 'Triggers results', watermarksOut: 'Passed through, after the rows it releases' },
    facts: {
      state: 'For each order_id, the earliest copy so far, with a timer at its event time, until the watermark reaches it. After that, a flag that says the order was emitted, which a state TTL can expire.',
      watermarks: 'They decide when a copy is final. An order is emitted when the watermark passes its earliest event time, and copies older than the watermark are late, and dropped.',
      output: 'Append-only. Each order is emitted once, when no earlier copy can still arrive.',
    },
    graphLabel: 'Job graph: Kafka source orders, hash partitioned by order_id into Deduplicate (keep first row by event time), then Kafka sink earliest_orders. Both edges are append-only.',
    graph: graph('earliest_orders', 'rowTimeDedup', 'by event time'),
    inputs,
    pill,
    narrate(node, ev) {
      if (node === 'src') return srcNarration(ev);
      if (node === 'dedup') {
        switch (ev.type) {
          case 'wait': return `First copy of order ${ev.key}. It can't be emitted yet, because an earlier copy might still arrive. It waits in state, with a timer at ${ev.row.event_time}.`;
          case 'replace': return `This copy of order ${ev.key} has an earlier event time (${ev.row.event_time}) than the one waiting (${ev.replaced.event_time}), so it replaces it, and the timer moves to ${ev.row.event_time}.`;
          case 'not-earlier': return `A copy of order ${ev.key} from ${ev.waiting.event_time} is already waiting, and this one (${ev.row.event_time}) isn't earlier, so it's ignored.`;
          case 'already-emitted': return `Order ${ev.key} has already been emitted, so this copy is ignored.`;
          case 'late': return `This copy of order ${ev.key} has event time ${ev.row.event_time}, but the watermark is already at ${ev.wm}. It's late, and dropped, even though it's the earliest attempt.`;
          case 'fire': return `The watermark reaches ${ev.time}. No copy older than that can still arrive, so the waiting ${ev.rows.length > 1 ? 'copies' : 'copy'} of order ${ev.rows.map((r) => r.order_id).join(' and ')} ${ev.rows.length > 1 ? 'are' : 'is'} final. Deduplicate emits ${ev.rows.length > 1 ? 'them' : 'it'}, and marks the order${ev.rows.length > 1 ? 's' : ''} as emitted.`;
          case 'wm': return `The watermark, ${ev.time}, hasn't reached any waiting copy, so nothing is emitted.`;
        }
      }
      if (node === 'sink' && ev.type === 'append') return `The sink appends order ${ev.row.order_id}, the copy with event time ${ev.row.event_time}.`;
      if (node === 'sink' && ev.type === 'wm') return `Watermarks aren't written to Kafka.`;
      return null;
    },
    explain: `
Sink(table=[earliest_orders], fields=[order_id, store_id, total, event_time], changelogMode=[NONE])
+- Rank(strategy=[AppendFastStrategy], rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=1],
     partitionBy=[order_id], orderBy=[ROWTIME event_time ASC], select=[...], changelogMode=[I])
   +- Exchange(distribution=[hash[order_id]], changelogMode=[I])
      +- Calc(select=[order_id, store_id, total, Reinterpret(event_time) AS event_time], changelogMode=[I])
         +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])

In the execution plan the Rank becomes:
   Deduplicate(keep=[FirstRow], key=[order_id], order=[ROWTIME], outputInsertOnly=[true])`,
    details: `
<p>Ordered by <code>event_time</code>, "first" means the copy placed first. The trouble is that an
earlier copy can always turn up later. So Deduplicate holds on to the earliest copy so far, with an
event-time timer, and only emits it when the watermark says nothing earlier can still come. In
Flink 2.3 that makes the output append-only, as long as mini-batch is off (the default). With
<code>table.exec.mini-batch.enabled</code>, Flink emits each new earliest copy right away, and
updates it, so the output is an updating stream.</p>
<p>Order 103 shows the difference from processing time: its copy from 12:00:06 arrives first, but
the copy from 12:00:05 replaces it, and that's the one emitted. The cost is latency: nothing comes
out until the watermark passes.</p>
<p>Order 102 shows the limit. Its first attempt, from 12:00:02, arrives after the watermark has
passed 12:00:03, so it's late, and dropped. The copy already emitted stands. Deduplicate never
retracts a result.</p>
<p>After a key is emitted, only a flag is kept, to ignore further copies. That flag can expire with
a state TTL. The waiting row and its timer can't: they're cleared by the watermark.</p>
<p>These semantics come from <code>RowTimeDeduplicateKeepFirstRowFunction</code> in Flink 2.3. Keep
the last row instead (<code>ORDER BY ... DESC</code>), and the output becomes an updating stream,
which an append-only sink can't take.</p>`,
  };

  // ---------- Ordered by order_id ----------
  //
  // The same orders, ordered by order_id instead of a time attribute. The planner can't use a
  // Deduplicate, so it plans a top-1 Rank, which keeps the whole first row and declares an
  // updating output, though ordering by the partition key means no copy can ever sort ahead.

  const byId = {
    id: 'byid',
    label: 'Ordered by order_id',
    question: 'Count each order once, ordering the copies by order_id.',
    blurb: 'Same orders, but <code>ORDER BY order_id</code>, which isn\'t a time attribute. The first copy to arrive still wins, but the planner can\'t use a Deduplicate, and the job costs more.',
    intro: 'The same copies arrive in the same order. Every copy of an order has the same order_id, so none can sort ahead of another. Press Play, or step through with the arrow keys.',
    sql: sql('deduped_orders', 'order_id'),
    ddl: `
CREATE TABLE orders (
  order_id          BIGINT,
  store_id          INT,
  pizza_id          INT,
  channel           STRING,
  total             DECIMAL(10, 2),
  delivery_address  STRING,
  event_time        TIMESTAMP(3),
  proc_time         AS PROCTIME(),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ('connector' = 'kafka', 'topic' = 'orders');

CREATE TABLE deduped_orders (
  order_id    BIGINT,
  store_id    INT,
  total       DECIMAL(10, 2),
  event_time  TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'deduped_orders');

-- Connection and format options are left out.`,
    flinkVersion: '2.3-SNAPSHOT',
    matrix: { state: 'The whole first row per key, kept until state TTL', watermarks: 'Not used', watermarksOut: 'Passed through' },
    facts: {
      state: 'The whole first row of every order_id, so later copies can be compared with it, not just a flag that says it was seen. It\'s never cleared, so set a state TTL.',
      watermarks: 'Not used. <code>order_id</code> isn\'t a time attribute.',
      output: 'Declared updating, so the sink needs a primary key. In practice only inserts arrive: every copy of an order has the same order_id, so none sorts ahead of the first.',
    },
    graphLabel: 'Job graph: Kafka source orders, hash partitioned by order_id into Rank (keep first row, ordered by order_id), then upsert-kafka sink deduped_orders. The first edge is append-only, the second is declared upsert.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [7],
          cfg: { topic: 'orders', delay: 1, timeField: 'event_time', columns },
        },
        {
          id: 'rank', kind: 'op', op: 'fastTop1', title: 'Rank', lines: ['keep first row', 'by order_id'],
          stateful: true, sqlLines: [4, 5, 6, 8],
          cfg: { key: 'order_id', sortField: 'order_id', stateFields: ['order_id', 'total', 'event_time'], stateFormat: { total: U.money } },
        },
        {
          id: 'sink', kind: 'sink', op: 'upsertKafkaSink', title: 'Kafka sink', lines: ['deduped_orders', 'upsert-kafka'], sqlLines: [1],
          cfg: {
            topic: 'deduped_orders', key: 'order_id', showKey: true, fields: ['store_id', 'total', 'event_time'],
            columns: [{ f: 'store_id', num: true }, { f: 'total', ...money }, { f: 'event_time' }],
          },
        },
      ],
      edges: [
        { from: 'src', to: 'rank', mode: 'append', kinds: '+I', exchange: 'hash(order_id)' },
        { from: 'rank', to: 'sink', mode: 'upsert', kinds: '+I +U -D' },
      ],
    },
    inputs,
    pill,
    narrate(node, ev) {
      if (node === 'src') return srcNarration(ev);
      if (node === 'rank') {
        if (ev.type === 'first') return `Order ${ev.key} has no row in state. Rank stores this copy, the whole row, and emits it (+I).`;
        if (ev.type === 'not-ahead') return `Order ${ev.key} already has a row in state. This copy has the same order_id, so it doesn't sort ahead of the stored one, and it's ignored, even if its event time is earlier.`;
        return 'The watermark passes through. Rank has no use for it.';
      }
      if (node === 'sink' && ev.type === 'upsert') return `The sink writes order ${ev.key}, the copy with event time ${ev.row.event_time}, to deduped_orders.`;
      if (node === 'sink' && ev.type === 'wm') return `Watermarks aren't written to Kafka.`;
      return null;
    },
    explain: `
Sink(table=[deduped_orders], fields=[order_id, store_id, total, event_time], changelogMode=[NONE])
+- Rank(strategy=[AppendFastStrategy], rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=1],
     partitionBy=[order_id], orderBy=[order_id ASC], select=[...], changelogMode=[I,UA,D])
   +- Exchange(distribution=[hash[order_id]], changelogMode=[I])
      +- Calc(select=[order_id, store_id, total, Reinterpret(event_time) AS event_time], changelogMode=[I])
         +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])

The execution plan keeps the Rank: there's no Deduplicate.`,
    details: `
<p>The planner only turns <code>ROW_NUMBER() ... = 1</code> into a Deduplicate when it's ordered by a
single time attribute. Ordered by <code>order_id</code>, it stays a top-1 Rank, run by
<code>FastTop1Function</code>.</p>
<p>That Rank keeps the whole first row of each order, because it compares every later copy with it.
A copy replaces it only if it sorts strictly ahead, and then it's sent as an update. Here every copy
of an order has the same <code>order_id</code>, so none ever sorts ahead, and the first copy to
arrive wins. That's the same result as the processing-time tab, but with a full row of state per
order, instead of a flag.</p>
<p>The planner doesn't reason about ties. It declares the Rank's output as an updating stream,
<code>[I, UA, D]</code>, so the sink needs a primary key, and a plain Kafka topic is rejected. Only
inserts ever arrive.</p>
<p>Flink 2.3.0 declares this Rank's output insert-only, and accepts an append-only sink. That's a
planner bug: ordered by another column, such as <code>total</code>, the same Rank does send updates.
<a href="https://issues.apache.org/jira/browse/FLINK-40368" target="_blank" rel="noopener">FLINK-40368</a>
fixes it in 2.3.1. The plan above is from a 2.3 snapshot build, which has the fix.</p>
<p>To deduplicate cheaply, order by a time attribute. If any copy will do, that's
<code>proc_time</code>.</p>`,
  };

  // ---------- Updating input ----------
  //
  // order_status already has one row per order, so this keeps each store's first order that
  // hasn't been cancelled. Over an updating input the planner keeps the Rank, with RetractStrategy,
  // which has to store every row.

  const statusRow = (order_id, store_id, status, total, event_time) => ({ key: order_id, value: { store_id, status, total, event_time } });
  const placed = (id, store, total, time) => statusRow(id, store, 'placed', total, time);
  const cancelled = (id, store, total, time) => statusRow(id, store, 'cancelled', total, time);

  const updating = {
    id: 'updating',
    label: 'Updating input',
    question: 'Which was each store\'s first order that wasn\'t cancelled?',
    blurb: 'Now the input is <code>order_status</code>, an <code>upsert-kafka</code> table. It already has one row per order, so this keeps each store\'s first order that hasn\'t been cancelled. When that order is cancelled, the next one has to take its place.',
    intro: 'order_status is keyed by order_id, and each record is the latest status of its order. Press Play, or step through with the arrow keys.',
    sql: `
INSERT INTO first_orders
SELECT store_id, order_id, total, event_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY store_id
                       ORDER BY event_time ASC) AS row_num
  FROM order_status
  WHERE status <> 'cancelled')
WHERE row_num = 1;`,
    ddl: `
CREATE TABLE order_status (
  order_id    BIGINT,
  store_id    INT,
  status      STRING,
  total       DECIMAL(10, 2),
  event_time  TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED,
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ('connector' = 'upsert-kafka', 'topic' = 'order_status');

CREATE TABLE first_orders (
  store_id    INT,
  order_id    BIGINT,
  total       DECIMAL(10, 2),
  event_time  TIMESTAMP(3),
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'first_orders');

-- Connection and format options are left out.`,
    matrix: { state: 'The last row per order, and every row per key', watermarks: 'Not used', watermarksOut: 'Passed through' },
    facts: {
      state: 'ChangelogNormalize keeps the last row of every order that isn\'t cancelled. Rank keeps all of those rows again, sorted per store, so it knows which order comes next if the first is cancelled. Both grow with every order.',
      watermarks: 'Not used. Here <code>event_time</code> is only a sort key: Rank emits at once, and nothing is ever late.',
      output: 'Upsert, keyed by <code>store_id</code>. A new first order arrives as a delete (−D) of the old one, then an insert (+I) of the new one.',
    },
    graphLabel: 'Job graph: upsert-kafka source order_status, hash partitioned by order_id into ChangelogNormalize with the filter, which emits a retract stream, hash partitioned by store_id into Rank (retract strategy), then upsert-kafka sink first_orders.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'upsertKafkaSource', title: 'Kafka source', lines: ['order_status', 'upsert-kafka'], sqlLines: [7],
          cfg: {
            topic: 'order_status', key: 'order_id', showKey: true, delay: 1, timeField: 'event_time',
            columns: [{ f: 'store_id', num: true }, { f: 'status' }, { f: 'total', ...money }, { f: 'event_time' }],
          },
        },
        {
          id: 'cn', kind: 'op', op: 'changelogNormalize', title: 'ChangelogNormalize', lines: ["status <> 'cancelled'"],
          stateful: true, planner: true, sqlLines: [7, 8],
          cfg: {
            key: 'order_id', condition: (r) => r.status !== 'cancelled', generateUpdateBefore: true,
            stateTitle: 'Last row per order_id',
            stateFields: ['order_id', 'store_id', 'event_time'],
          },
        },
        {
          id: 'rank', kind: 'op', op: 'retractRank', title: 'Rank', lines: ['keep first row', 'retract strategy'],
          stateful: true, sqlLines: [4, 5, 6, 9],
          cfg: { key: 'store_id', keyLabel: 'store', sortField: 'event_time', idField: 'order_id', stateFields: ['order_id', 'event_time'] },
        },
        {
          id: 'sink', kind: 'sink', op: 'upsertKafkaSink', title: 'Kafka sink', lines: ['first_orders', 'upsert-kafka'], sqlLines: [1],
          cfg: {
            topic: 'first_orders', key: 'store_id', showKey: true, fields: ['order_id', 'total', 'event_time'],
            columns: [{ f: 'order_id' }, { f: 'total', ...money }, { f: 'event_time' }],
          },
        },
      ],
      edges: [
        { from: 'src', to: 'cn', mode: 'upsert', kinds: '+U -D', exchange: 'hash(order_id)' },
        { from: 'cn', to: 'rank', mode: 'retract', kinds: '+I -U +U -D', exchange: 'hash(store_id)' },
        { from: 'rank', to: 'sink', mode: 'upsert', kinds: '+I +U -D' },
      ],
    },
    // 203 and 204 are stored though neither is first. Cancelling 201 promotes 203; 205 is placed
    // before 202 but arrives after it, and takes its place; cancelling 204 changes nothing.
    inputs: [
      placed(201, 7, 18.00, '12:00:05'), placed(202, 9, 24.50, '12:00:09'),
      placed(203, 7, 31.00, '12:00:12'), placed(204, 7, 13.00, '12:00:20'),
      cancelled(201, 7, 18.00, '12:00:05'),
      placed(205, 9, 22.00, '12:00:03'),
      cancelled(204, 7, 13.00, '12:00:20'),
    ],
    pill: (r) => (r.status === 'cancelled' ? `${r.order_id} cancelled` : `${r.order_id} s${r.store_id} ${r.event_time}`),
    narrate(node, ev) {
      if (node === 'src' && ev.type === 'read') {
        const r = ev.row;
        return r.status === 'cancelled'
          ? `The source reads ${ev.key} → cancelled. To the source it's just another upsert (+U) for order ${ev.key}.`
          : `The source reads ${ev.key} → placed: store ${r.store_id}, ${U.money(r.total)}, at ${r.event_time}. upsert-kafka can't tell whether order ${ev.key} is new, so the planner treats every value as an upsert (+U).`;
      }
      if (node === 'cn') {
        switch (ev.type) {
          case 'insert': return `Order ${ev.key} isn't in state, and it isn't cancelled. ChangelogNormalize stores the row and emits an insert (+I).`;
          case 'unmatched': return `Order ${ev.key} is now cancelled, so it fails the filter. ChangelogNormalize emits a delete (−D) with the row from state, and clears the key.`;
        }
      }
      if (node === 'rank') {
        const r = ev.row;
        switch (ev.type) {
          case 'first': return `Store ${ev.key} has no rows yet, so order ${r.order_id} is its first order. Rank stores it and emits an insert (+I).`;
          case 'store': return `Order ${r.order_id} (${r.event_time}) comes after store ${ev.key}'s first order, so nothing changes downstream. Rank stores it anyway, as row ${ev.rank} of ${ev.size}, in case the orders ahead of it are cancelled.`;
          case 'displace': return `Order ${r.order_id} was placed at ${r.event_time}, before order ${ev.prev.order_id} (${ev.prev.event_time}), though it arrived after it. It's store ${ev.key}'s first order now, so Rank deletes order ${ev.prev.order_id} (−D) and inserts order ${r.order_id} (+I).`;
          case 'promote': return `Order ${r.order_id} was store ${ev.key}'s first order, and it's been cancelled. Rank deletes it (−D), and inserts the next order it has kept, ${ev.next.order_id} (+I). Without every row in state, it couldn't.`;
          case 'empty': return `Order ${r.order_id} was store ${ev.key}'s only order, and it's been cancelled. Rank deletes it (−D), and the store is gone.`;
          case 'remove': return `Order ${r.order_id} is cancelled, but it was row ${ev.rank} for store ${ev.key}, not the first. Rank removes it from state, and emits nothing.`;
        }
      }
      if (node === 'sink' && ev.type === 'upsert') return `The sink writes store ${ev.key} → order ${ev.row.order_id} to first_orders.`;
      if (node === 'sink' && ev.type === 'tombstone') return `The sink writes a tombstone for store ${ev.key}. Until the next record, a consumer of first_orders sees no first order for store ${ev.key}.`;
      return null;
    },
    explain: `
Sink(table=[first_orders], fields=[store_id, order_id, total, event_time], changelogMode=[NONE])
+- Calc(select=[store_id, order_id, total, event_time], changelogMode=[I,UA,D])
   +- Rank(strategy=[RetractStrategy], rankType=[ROW_NUMBER], rankRange=[rankStart=1, rankEnd=1],
        partitionBy=[store_id], orderBy=[ROWTIME event_time ASC], select=[...], changelogMode=[I,UA,D])
      +- Exchange(distribution=[hash[store_id]], changelogMode=[I,UB,UA,D])
         +- Calc(select=[order_id, store_id, total, event_time], changelogMode=[I,UB,UA,D])
            +- ChangelogNormalize(key=[order_id], condition=[<>(status, 'cancelled')], changelogMode=[I,UB,UA,D])
               +- Exchange(distribution=[hash[order_id]], changelogMode=[UA,D])
                  +- TableSourceScan(table=[[order_status, watermark=[...]]], fields=[...], changelogMode=[UA,D])`,
    details: `
<p>An upsert table already has one row per key, so deduplicating <code>order_status</code> by
<code>order_id</code> would do nothing. This query uses the same <code>ROW_NUMBER()</code> shape
to keep each store's first order instead, leaving out cancelled ones.</p>
<p>The planner only turns that shape into a Deduplicate when its input is append-only. Here the
input can retract, so it keeps the Rank, with <code>RetractStrategy</code>. If the first order is
cancelled, Rank has to know which order comes next, so it keeps every row of every store, sorted
(<code>RetractableTopNFunction</code>). Orders 203 and 204 are stored, though neither is first, and
when order 201 is cancelled, order 203 takes its place. The append-only tabs keep one row, or just
a flag, per key.</p>
<p>So this job keeps every live order twice: once in ChangelogNormalize, and again in Rank. A state
TTL bounds both, at the risk that a cancellation arrives after the order it cancels has expired.</p>
<p>A change of first order goes out as a delete of the old row, then an insert of the new one. The
plan lists <code>UA</code> among the row kinds Rank may emit, but for a top 1 without the row number
in the output, it only emits <code>+I</code> and <code>−D</code>. The upsert-kafka sink writes a
tombstone and then the new value, so a consumer can briefly see no first order for the store.</p>
<p>Ordering by <code>event_time</code> doesn't make this wait for the watermark. It's only a sort
key: order 205, placed at 12:00:03, arrives after order 202, and becomes store 9's first order
straight away. Nothing is ever late, unlike the event-time tab.</p>`,
  };

  window.PATTERNS.push({
    id: 'dedup',
    nav: 'Deduplication',
    title: 'Deduplication',
    summary: '<p>Keep one row per key, and drop the duplicates. Which row is kept depends on the <code>ORDER BY</code>: processing time keeps the first to arrive, and event time waits for the watermark to keep the earliest. Only those two, on an append-only input, get Flink\'s lightweight Deduplicate operator. Anything else becomes a Top-1 Rank, which keeps more state.</p>',
    variants: [procTime, eventTime, byId, updating],
  });
})();
