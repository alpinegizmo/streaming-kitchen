// Pattern: OVER aggregation, over an event-time RANGE.
// Verified against verification/over.sql (see over.plan.txt).
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const money = { num: true, fmt: U.money };
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;
  const result = (r) => `${plural(r.recent_orders, 'order')}, ${U.money(r.recent_sales)}`;
  const order = (order_id, store_id, total, event_time) => ({ value: { order_id, store_id, total, event_time } });
  const WM = { wm: true };

  const range = {
    id: 'range',
    label: 'Range',
    question: 'How much had each store sold in the 5 minutes before each order?',
    intro: 'Stores 7 and 9 send orders. The range is 5 minutes, and the watermark trails the newest order by 1 second.',
    sql: `
INSERT INTO orders_with_recent_sales
SELECT order_id, store_id, total, event_time,
       COUNT(*) OVER w AS recent_orders,
       SUM(total) OVER w AS recent_sales
FROM orders
WINDOW w AS (
  PARTITION BY store_id
  ORDER BY event_time
  RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW);`,
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

CREATE TABLE orders_with_recent_sales (
  order_id       BIGINT,
  store_id       INT,
  total          DECIMAL(10, 2),
  event_time     TIMESTAMP(3),
  recent_orders  BIGINT,
  recent_sales   DECIMAL(38, 2)
) WITH ('connector' = 'kafka', 'topic' = 'orders_with_recent_sales');

-- Connection and format options are left out.`,
    matrix: { state: 'The rows in the range per key, and a running aggregate', watermarks: 'Trigger results; lateness is per key', watermarksOut: 'Passed through, after the rows it releases' },
    facts: {
      state: 'For each store, every order in the last 5 minutes, the orders still waiting for the watermark, and a running count and sum. An order leaves state when it drops out of a newer order\'s range. A quiet store\'s state is cleared between 5 and 7½ minutes after its newest order, once the watermark gets there.',
      watermarks: 'They decide when an order\'s result can be computed: once the watermark reaches its event time, no earlier order for that store can still arrive. An order no newer than the last one its store has emitted is late, and dropped.',
      output: 'Append-only. Every order comes out once, with its result, and is never updated.',
    },
    graphLabel: 'Job graph: Kafka source orders, hash partitioned by store_id into OverAggregate (a 5-minute range per store), then Kafka sink orders_with_recent_sales. Both edges are append-only.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [5],
          cfg: {
            topic: 'orders', delay: 1, timeField: 'event_time',
            columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total', ...money }, { f: 'event_time' }],
          },
        },
        {
          id: 'over', kind: 'op', op: 'overAggregate', title: 'OverAggregate', lines: ['RANGE 5 min', 'per store_id'],
          stateful: true, sqlLines: [2, 3, 4, 6, 7, 8, 9],
          cfg: {
            key: 'store_id', keyLabel: 'store', timeField: 'event_time', preceding: 300, sumField: 'total', idField: 'order_id',
            countName: 'recent_orders', sumName: 'recent_sales',
          },
        },
        {
          id: 'sink', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: ['orders_with_recent_sales'], sqlLines: [1],
          cfg: {
            topic: 'orders_with_recent_sales',
            columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total', ...money }, { f: 'event_time' },
              { f: 'recent_orders', num: true }, { f: 'recent_sales', ...money }],
          },
        },
      ],
      edges: [
        { from: 'src', to: 'over', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' },
        { from: 'over', to: 'sink', mode: 'append', kinds: '+I' },
      ],
    },
    // 105 arrives after 104 but was placed earlier, so it's computed first. 106 is late for store 7,
    // which has already emitted 12:05:58. 107 is behind the watermark too, but newer than anything
    // store 9 has emitted, so it's accepted. 101 leaves 104's range; 102 is exactly 5 minutes before
    // 108, so it stays in.
    inputs: [
      order(101, 7, 18.00, '12:01:10'), order(102, 9, 24.50, '12:02:30'), order(103, 7, 31.00, '12:03:45'), WM,
      order(104, 7, 13.00, '12:06:20'), order(105, 7, 16.00, '12:05:58'), WM,
      order(106, 7, 27.50, '12:04:30'), order(107, 9, 14.00, '12:05:00'), order(108, 9, 19.00, '12:07:30'), WM,
      order(109, 9, 22.00, '12:08:30'), WM,
    ],
    pill: (r) => (r.recent_sales != null
      ? `${r.order_id} ${r.recent_orders} ${U.money(r.recent_sales)}`
      : `${r.order_id} s${r.store_id} ${r.event_time}`),
    narrate(node, ev) {
      if (node === 'src' && ev.type === 'read') {
        const r = ev.row;
        return `The source reads order ${r.order_id}: store ${r.store_id}, ${U.money(r.total)}, placed at ${r.event_time}.`;
      }
      if (node === 'src' && ev.type === 'wm') {
        return `The source emits a watermark for ${ev.time}: the newest event time it has read, minus 1 second.`;
      }
      if (node === 'over') {
        const r = ev.row;
        switch (ev.type) {
          case 'buffer': {
            const behind = ev.wm && r.event_time <= ev.wm;
            return behind
              ? `Order ${r.order_id} was placed at ${r.event_time}, behind the watermark (${ev.wm}). But store ${ev.key} has only emitted results up to ${ev.last}, so it isn't late. OverAggregate buffers it, and its timer, already due, fires with the next watermark.`
              : `Order ${r.order_id} is buffered under its event time, ${r.event_time}, with a timer. Its result has to wait until the watermark says no earlier order for store ${ev.key} can still arrive.`;
          }
          case 'late': return `Order ${r.order_id} was placed at ${r.event_time}, but store ${ev.key} has already emitted a result for ${ev.last}, and that result can't be revised. So the order is late, and dropped.`;
          case 'wm': return `The watermark, ${ev.time}, hasn't reached any buffered order, so nothing is emitted. OverAggregate forwards it.`;
          case 'fire': {
            const parts = ev.firings.filter((f) => f.type === 'emit').map((f) => {
              const gone = f.retracted.length
                ? ` Order ${f.retracted.map((x) => `${x.order_id} (${x.event_time})`).join(' and ')} is now more than 5 minutes older, so it's retracted from the sum, and leaves state.`
                : '';
              return `${gone} Order ${f.rows.map((x) => x.order_id).join(' and ')}, at ${f.t}: store ${f.k}'s last 5 minutes hold ${result(f.rows[0])}.`;
            });
            return `The watermark reaches ${ev.time}.${parts.join('')} OverAggregate emits ${ev.rows.length > 1 ? 'each result' : 'the result'}, then forwards the watermark.`;
          }
        }
      }
      if (node === 'sink' && ev.type === 'append') {
        const r = ev.row;
        return `The sink appends order ${r.order_id}, with store ${r.store_id}'s sales in the 5 minutes up to ${r.event_time}: ${result(r)}.`;
      }
      if (node === 'sink' && ev.type === 'wm') return `The watermark reaches the sink, which doesn't write it to Kafka.`;
      return null;
    },
    explain: `
Sink(table=[orders_with_recent_sales], fields=[order_id, store_id, total, event_time, recent_orders,
     recent_sales], changelogMode=[NONE])
+- Calc(select=[order_id, store_id, total, event_time, w0$o0 AS recent_orders,
        CASE(>(w0$o1, 0), w0$o2, null) AS recent_sales], changelogMode=[I])
   +- OverAggregate(partitionBy=[store_id], orderBy=[event_time ASC],
        window=[ RANGE BETWEEN 300000 PRECEDING AND CURRENT ROW],
        select=[order_id, store_id, total, event_time, COUNT(*) AS w0$o0, COUNT(total) AS w0$o1,
                $SUM0(total) AS w0$o2], changelogMode=[I])
      +- Exchange(distribution=[hash[store_id]], changelogMode=[I])
         +- Calc(select=[order_id, store_id, total, event_time], changelogMode=[I])
            +- TableSourceScan(table=[[orders, watermark=[-(event_time, 1000:INTERVAL SECOND)],
                 watermarkEmitStrategy=[on-periodic]]], fields=[...], changelogMode=[I])`,
    details: `
<p>An OVER aggregate computes a result for every row, over a range of rows around it: here, its
store's orders in the 5 minutes up to and including it. Unlike <code>GROUP BY</code>, it doesn't
collapse rows. Each order comes out once, with its own result attached.</p>
<p>On a stream, the range has to end at the current row, because Flink can't look ahead, and it
has to be ordered by a time attribute, ascending. Orders arrive out of order, so OverAggregate can't
compute an order's result until no earlier order for its store can still arrive. It buffers each
order under its event time, with a timer. When the watermark reaches that time, it retracts the
orders that have fallen out of the range, adds the new one, and emits it. Order 101 drops out at
12:06:20. Order 102 is still in at 12:07:30, exactly 5 minutes later, because the range includes its
start.</p>
<p>Lateness works per store, not against the watermark. Order 106 is dropped because store 7 has
already emitted a result for 12:05:58, which can't be revised. Order 107 is behind the watermark
as well, but store 9 has only emitted up to 12:02:30, so it's accepted. Its timer is already due,
and fires with the next watermark.</p>
<p>Retracting old orders keeps the state bounded by the range. When a store goes quiet, a clean-up
timer clears all of its state, between one and one and a half ranges (here 5 to 7½ minutes) after
its newest order, once the watermark gets there.</p>
<p>Every OVER aggregate in one streaming query has to use the same window, which is why it's
written once, as <code>WINDOW w</code>. <code>ROWS BETWEEN n PRECEDING</code> counts rows instead of
time. These semantics come from <code>RowTimeRangeBoundedPrecedingFunction</code> in Flink 2.3.</p>
<p>The graph leaves out two Calcs: one that drops columns before the exchange, and one after
OverAggregate that turns the running sum into <code>SUM</code>'s result, which is null for no
rows.</p>`,
  };

  window.PATTERNS.push({
    id: 'over',
    nav: 'OVER aggregation',
    title: 'OVER aggregation',
    summary: '<p>Compute an aggregate over a range of rows ending at each row, such as a store\'s sales in the 5 minutes before each order. Every row comes out once, with its result, so the output is append-only, but each row must wait for the watermark.</p>',
    variants: [range],
  });
})();
