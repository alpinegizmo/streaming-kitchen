// Pattern: windows. Tumble for now; hop, cumulate and session join as more variants.
// Verified against verification/tumble.sql (see tumble.plan.txt).
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const money = { num: true, fmt: U.money };
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;

  const orders = {
    101: { order_id: 101, store_id: 7, total: 18.00, event_time: '12:01:10' },
    102: { order_id: 102, store_id: 9, total: 24.50, event_time: '12:02:30' },
    103: { order_id: 103, store_id: 7, total: 31.00, event_time: '12:03:45' },
    104: { order_id: 104, store_id: 7, total: 13.00, event_time: '12:04:20' },
    106: { order_id: 106, store_id: 7, total: 27.50, event_time: '12:05:01' },
    105: { order_id: 105, store_id: 7, total: 16.00, event_time: '12:04:58' },
    107: { order_id: 107, store_id: 9, total: 19.00, event_time: '12:07:30' },
    108: { order_id: 108, store_id: 7, total: 21.00, event_time: '12:09:10' },
    109: { order_id: 109, store_id: 9, total: 30.00, event_time: '12:10:20' },
  };
  const read = (id) => ({ value: orders[id] });
  const WM = { wm: true };

  const tumble = {
    id: 'tumble',
    label: 'Tumble',
    question: 'How much did each store sell, every 5 minutes?',
    intro: 'Stores 7 and 9 send orders. Windows are 5 minutes long, and the watermark trails the newest order by 1 second. Press Play, or step through with the arrow keys.',
    sql: `
INSERT INTO store_sales
SELECT window_start, window_end, store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM TUMBLE(
  TABLE orders,
  DESCRIPTOR(event_time),
  INTERVAL '5' MINUTES)
GROUP BY window_start, window_end, store_id;`,
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

CREATE TABLE store_sales (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  store_id      INT,
  orders        BIGINT,
  sales         DECIMAL(38, 2)
) WITH ('connector' = 'kafka', 'topic' = 'store_sales');

-- Connection and format options are left out.`,
    facts: {
      state: 'One accumulator per store and window (an order count and a running sum), and a timer at the end of each window. A window\'s state is freed as soon as it fires.',
      watermarks: 'They drive the job. A window fires when the watermark passes its end. An order that arrives after its window has fired is late, and dropped.',
      output: 'Append-only. Each window is emitted once, when it\'s final, so a plain Kafka topic can take it.',
    },
    graphLabel: 'Job graph: Kafka source orders, hash partitioned by store_id into WindowAggregate, then Kafka sink store_sales. Both edges are append-only.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [6],
          cfg: {
            topic: 'orders', delay: 1, timeField: 'event_time',
            columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total', ...money }, { f: 'event_time' }],
          },
        },
        {
          id: 'win', kind: 'op', op: 'windowAggregate', title: 'WindowAggregate', lines: ['TUMBLE 5 min', 'local + global'],
          stateful: true, sqlLines: [2, 3, 4, 5, 7, 8, 9],
          cfg: { size: 300, key: 'store_id', keyLabel: 'store', timeField: 'event_time', sumField: 'total' },
        },
        {
          id: 'sink', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: ['store_sales'], sqlLines: [1],
          cfg: {
            topic: 'store_sales',
            columns: [{ f: 'window_start' }, { f: 'window_end' }, { f: 'store_id', num: true }, { f: 'orders', num: true }, { f: 'sales', ...money }],
          },
        },
      ],
      edges: [
        { from: 'src', to: 'win', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' },
        { from: 'win', to: 'sink', mode: 'append', kinds: '+I' },
      ],
    },
    inputs: [
      read(101), read(102), read(103), WM,
      read(104), read(106), WM,
      read(105), read(107), read(108), WM,
      read(109), WM,
    ],
    pill: (r) => (r.window_start
      ? `s${r.store_id} ${r.window_start} ${U.money(r.sales)}`
      : `${r.order_id} s${r.store_id} ${r.event_time}`),
    narrate(node, ev) {
      if (node === 'src' && ev.type === 'read') {
        const r = ev.row;
        return `The source reads order ${r.order_id}: store ${r.store_id}, ${U.money(r.total)}, placed at ${r.event_time}.`;
      }
      if (node === 'src' && ev.type === 'wm') {
        return `The source emits a watermark for ${ev.time}: the newest event time it has read, minus 1 second.`;
      }
      if (node === 'win') {
        const r = ev.row;
        switch (ev.type) {
          case 'new-window':
            return `Order ${r.order_id} falls in the ${ev.win.start}–${ev.win.end} window. Store ${r.store_id} has no accumulator for it yet, so one is created, with a timer for ${ev.win.end}.`;
          case 'accumulate':
            return `Order ${r.order_id} also falls in the ${ev.win.start}–${ev.win.end} window. Store ${r.store_id}'s accumulator becomes ${plural(ev.acc.count, 'order')}, ${U.money(ev.acc.sum)}.`;
          case 'late':
            return `Order ${r.order_id} was placed at ${r.event_time}, but the watermark is already at ${ev.wm}. Its ${ev.win.start}–${ev.win.end} window has fired and its state is gone, so the order is late, and dropped.`;
          case 'wm':
            return `The watermark, ${ev.time}, hasn't reached the end of any window, so nothing fires. WindowAggregate forwards it.`;
          case 'fire': {
            const results = ev.rows.map((x) => `store ${x.store_id}, ${plural(x.orders, 'order')} and ${U.money(x.sales)}`).join('; ');
            return `The watermark reaches ${ev.time}, so the ${ev.windows.join(' and ')} window is complete. WindowAggregate emits a final row for each store (${results}), frees their state, and then forwards the watermark.`;
          }
        }
      }
      if (node === 'sink' && ev.type === 'append') {
        const r = ev.row;
        return `The sink appends store ${r.store_id}'s result for ${r.window_start}–${r.window_end}: ${plural(r.orders, 'order')}, ${U.money(r.sales)}. It's final, and will never be updated.`;
      }
      if (node === 'sink' && ev.type === 'wm') return `The watermark reaches the sink, which doesn't write it to Kafka.`;
      return null;
    },
    explain: `
Sink(table=[store_sales], fields=[window_start, window_end, store_id, orders, sales], changelogMode=[NONE])
+- Calc(select=[window_start, window_end, store_id, orders, sales], changelogMode=[I])
   +- GlobalWindowAggregate(groupBy=[store_id], window=[TUMBLE(slice_end=[$slice_end], size=[5 min])],
        select=[store_id, COUNT(count1$0) AS orders, SUM(sum$1) AS sales,
                start('w$) AS window_start, end('w$) AS window_end], changelogMode=[I])
      +- Exchange(distribution=[hash[store_id]], changelogMode=[I])
         +- LocalWindowAggregate(groupBy=[store_id], window=[TUMBLE(time_col=[event_time], size=[5 min])],
              select=[store_id, COUNT(*) AS count1$0, SUM(total) AS sum$1, slice_end('w$) AS $slice_end],
              changelogMode=[I])
            +- Calc(select=[store_id, total, event_time], changelogMode=[I])
               +- TableSourceScan(table=[[orders, watermark=[-(event_time, 1000:INTERVAL SECOND)],
                    watermarkEmitStrategy=[on-periodic]]], fields=[...], changelogMode=[I])`,
    details: `
<p>In Flink SQL a window is a table function. <code>TUMBLE</code> takes the orders table and gives it
back with <code>window_start</code> and <code>window_end</code> columns added. After that it's an
ordinary <code>GROUP BY</code>, with the window as part of the key.</p>
<p>It's the watermark that makes a window fire: not the wall clock, and not the next order. Strictly,
a window fires when the watermark reaches its end minus one millisecond, the last instant it covers.
That's also why results are never corrected. Once the watermark has passed, Flink treats the window
as complete, frees its state, and drops anything that turns up for it later.</p>
<p>The price is freshness. A window's result only comes out after the watermark passes its end: at
least the 1-second out-of-orderness delay later, and only once a later order has moved the
watermark along. A non-windowed <code>GROUP BY</code> is fresher, but its output is an updating
stream, and its state never goes away.</p>
<p>The planner splits the aggregation in two. LocalWindowAggregate pre-aggregates each window
before the shuffle, so only one partial result per store and window crosses the network.
GlobalWindowAggregate, after the hash exchange on <code>store_id</code>, merges them in keyed
state and fires the timers. The graph draws them as one operator. It also leaves out two Calcs that
only drop columns.</p>
<p>Under the hood, the state is kept per <em>slice</em>. For a tumbling window, a slice is the whole
window. Hopping and cumulating windows share slices between overlapping windows.</p>
<p>If late orders matter, don't let them vanish: route them to a separate table with
<code>WHERE event_time &lt;= CURRENT_WATERMARK(event_time)</code>, or give the watermark a longer
delay, at the cost of later results.</p>`,
  };

  window.PATTERNS.push({
    id: 'windows',
    nav: 'Windows',
    title: 'Windows',
    summary: '<p>Group events into windows of time, and emit one final result per window when the watermark says the window is complete. The output is append-only, and each window\'s state is freed when it fires.</p>',
    variants: [tumble],
  });
})();
