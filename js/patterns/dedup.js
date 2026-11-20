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
    matrix: { state: 'A flag per key that has been seen, kept until state TTL', watermarks: 'Not used' },
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
    matrix: { state: 'Per key: the earliest row until the watermark passes it, then a flag', watermarks: 'Triggers results' },
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

  window.PATTERNS.push({
    id: 'dedup',
    nav: 'Deduplication',
    title: 'Deduplication',
    summary: '<p>Keep one row per key and drop the duplicates. Which row counts as the first depends on the time you order by: arrival order is immediate, event time has to wait for the watermark.</p>',
    variants: [procTime, eventTime],
  });
})();
