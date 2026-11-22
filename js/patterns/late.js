// Pattern: late events to a dead letter queue, with a statement set.
// Verified against verification/late-events.sql (see late-events.plan.txt).
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const money = { num: true, fmt: U.money };
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;
  const result = (r) => `${plural(r.orders, 'order')}, ${U.money(r.sales)}`;
  const list = (xs) => (xs.length < 2 ? xs.join('') : `${xs.slice(0, -1).join(', ')} and ${xs[xs.length - 1]}`);
  const order = (order_id, store_id, total, event_time) => ({ value: { order_id, store_id, total, event_time } });
  const WM = { wm: true };

  // One definition of late, used by both branches: at or behind the watermark. Before the first
  // watermark, CURRENT_WATERMARK is NULL and nothing is late.
  const isLate = (r, wm) => wm != null && U.toSec(r.event_time) <= U.toSec(wm);
  const onTime = (r, wm) => wm == null || U.toSec(r.event_time) > U.toSec(wm);
  // The window an order belongs to, for the narration: 1-minute tumbling windows.
  const SIZE = 60;
  const windowEnd = (r) => Math.floor(U.toSec(r.event_time) / SIZE) * SIZE + SIZE;
  const hm = (sec) => U.fromSec(sec).slice(0, 5);

  const windowCfg = { kind: 'tumble', size: SIZE, key: 'store_id', keyLabel: 'store', timeField: 'event_time', sumField: 'total' };

  const late = {
    id: 'dlq',
    label: 'Statement set',
    question: 'Which orders arrived too late to be counted?',
    intro: 'Stores 7 and 9 send orders. Windows are 1 minute long, and the watermark trails the newest order by 1 second. Every order goes to both INSERTs, and each one keeps the orders the other leaves out: late orders go to the dead letter queue, on-time orders to the window. Press Play, or step through with the arrow keys.',
    sql: `
EXECUTE STATEMENT SET
BEGIN

-- Late orders go to a dead letter queue.
INSERT INTO late_orders
SELECT * FROM orders
WHERE event_time <= CURRENT_WATERMARK(event_time);

-- The main query counts the rest.
INSERT INTO store_sales
SELECT window_start, window_end, store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM TUMBLE(
  (SELECT * FROM orders
   WHERE CURRENT_WATERMARK(event_time) IS NULL
      OR event_time > CURRENT_WATERMARK(event_time)),
  DESCRIPTOR(event_time),
  INTERVAL '1' MINUTE)
GROUP BY window_start, window_end, store_id;

END;`,
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

-- The dead letter queue: the same columns as orders.
CREATE TABLE late_orders (
  order_id          BIGINT,
  store_id          INT,
  pizza_id          INT,
  channel           STRING,
  total             DECIMAL(10, 2),
  delivery_address  STRING,
  event_time        TIMESTAMP(3)
) WITH ('connector' = 'kafka', 'topic' = 'late_orders');

-- Connection and format options are left out.`,
    matrix: {
      state: 'An accumulator per key and window; the filters keep none',
      memory: 'LocalWindowAggregate keeps partial results in memory, not state, until a watermark or checkpoint sends them on',
      watermarks: 'Fire windows, and split orders into on-time and late',
      watermarksOut: 'Passed through, after the windows it fires',
    },
    facts: {
      state: 'GlobalWindowAggregate keeps one accumulator per store and window, freed when the window fires, and LocalWindowAggregate\'s partial results are in memory. The two filters keep nothing: <code>CURRENT_WATERMARK</code> reads the watermark each Calc already has.',
      watermarks: 'They fire the windows, and they define late: an order at or behind the watermark goes to the dead letter queue, and every other order goes to the window. Until the first watermark arrives, nothing is late.',
      output: 'Two append-only topics: one final row per store and window in store_sales, and every late order in late_orders. Each order lands in exactly one of them.',
    },
    graphLabel: 'Job graph: one Kafka source, orders, feeds two branches. One is a Calc that keeps on-time orders, hash partitioned by store_id into WindowAggregate (tumble, 1 minute), then Kafka sink store_sales. The other is a Calc that keeps late orders, then Kafka sink late_orders. All edges are append-only.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [6, 15],
          cfg: {
            topic: 'orders', delay: 1, timeField: 'event_time',
            columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total', ...money }, { f: 'event_time' }],
          },
        },
        {
          id: 'ontime', kind: 'op', op: 'calc', title: 'Calc', lines: ['keep on-time orders'], sqlLines: [16, 17],
          cfg: { where: onTime },
        },
        {
          id: 'local', kind: 'op', op: 'localWindowAggregate', title: 'LocalWindowAggregate', lines: ['TUMBLE 1 min', 'pre-aggregate'],
          panel: true, sqlLines: [12, 13, 14, 18, 19], cfg: windowCfg,
        },
        {
          id: 'win', kind: 'op', op: 'globalWindowAggregate', title: 'GlobalWindowAggregate', lines: ['TUMBLE 1 min', 'merge, fire'],
          stateful: true, sqlLines: [11, 12, 13, 20], cfg: windowCfg,
        },
        {
          id: 'sales', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: ['store_sales'], sqlLines: [10],
          cfg: {
            topic: 'store_sales',
            columns: [{ f: 'window_start' }, { f: 'window_end' }, { f: 'store_id', num: true }, { f: 'orders', num: true }, { f: 'sales', ...money }],
          },
        },
        {
          id: 'late', kind: 'op', op: 'calc', title: 'Calc', lines: ['keep late orders'], sqlLines: [7],
          cfg: { where: isLate },
        },
        {
          id: 'dlq', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: ['late_orders'], sqlLines: [5],
          cfg: {
            topic: 'late_orders',
            columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total', ...money }, { f: 'event_time' }],
          },
        },
      ],
      edges: [
        { from: 'src', to: 'ontime', mode: 'append', kinds: '+I' },
        { from: 'ontime', to: 'local', mode: 'append', kinds: '+I' },
        { from: 'local', to: 'win', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' },
        { from: 'win', to: 'sales', mode: 'append', kinds: '+I' },
        { from: 'src', to: 'late', mode: 'append', kinds: '+I' },
        { from: 'late', to: 'dlq', mode: 'append', kinds: '+I' },
      ],
    },
    inputs: [
      order(101, 7, 18.00, '12:00:10'), order(102, 9, 24.50, '12:00:35'), order(103, 7, 31.00, '12:00:52'),
      order(104, 7, 13.00, '12:01:05'), WM,
      order(105, 9, 16.00, '12:00:58'), order(106, 7, 27.50, '12:01:02'), order(107, 9, 19.00, '12:02:03'), WM,
      order(108, 7, 21.00, '12:01:50'),
    ],
    pill: (r) => {
      if (r.window_start) return `s${r.store_id} ${r.window_start} ${U.money(r.sales)}`;
      if (r.slice_end) return `s${r.store_id} ≤${r.slice_end.slice(0, 5)} ${r.sales < 0 ? `−${U.money(-r.sales)}` : U.money(r.sales)}`;
      return `${r.order_id} s${r.store_id} ${r.event_time}`;
    },
    narrate(node, ev) {
      const r = ev.row;
      if (node === 'src') {
        if (ev.type === 'wm') return `The source emits a watermark for ${ev.time}: the newest event time it has read, minus 1 second. It goes down both branches.`;
        const read = `The source reads order ${r.order_id}: store ${r.store_id}, ${U.money(r.total)}, placed at ${r.event_time}.`;
        return r.order_id === 101 ? `${read} Both INSERTs read this one source, so every order goes down both branches.` : read;
      }
      if (node === 'win' && ev.type === 'fire') {
        const wins = [...new Set(ev.firings.map((f) => `${f.start}–${f.end}`))];
        const rows = ev.rows.map((x) => `store ${x.store_id}, ${result(x)}`).join('; ');
        const text = Ops.globalWindowAggregate.mergeText(ev, windowCfg);
        return `The watermark, ${ev.time}, arrives. ${text ? `${text} ` : ''}Then the ${list(wins)} window is complete: GlobalWindowAggregate emits a final row for each store (${rows}), frees their state, and forwards the watermark.`;
      }
      if (node === 'ontime') {
        if (ev.type === 'wm') return `The watermark reaches the main query's filter. From now on, CURRENT_WATERMARK returns ${ev.time}, and orders at or before it are late.`;
        if (ev.wm == null) {
          return r.order_id === 101
            ? 'No watermark has reached the filter yet, so CURRENT_WATERMARK(event_time) is NULL, and nothing can be late. The IS NULL check lets order 101 through to the window. Without it, the main query would drop every order until the first watermark.'
            : `Still no watermark, so order ${r.order_id} goes through to the window.`;
        }
        if (ev.type === 'pass') return `Order ${r.order_id}, at ${r.event_time}, is ahead of the watermark (${ev.wm}). It's on time, so it goes to the window.`;
        const open = windowEnd(r) > U.toSec(ev.wm);
        return open
          ? `Order ${r.order_id}, at ${r.event_time}, is behind the watermark (${ev.wm}), so by this query's definition it's late, and the main query leaves it out. Its ${hm(windowEnd(r) - SIZE)}–${hm(windowEnd(r))} window is still open, so the window would have counted it, but then it would also be in late_orders.`
          : `Order ${r.order_id}, at ${r.event_time}, is at or behind the watermark (${ev.wm}). It's late, so the main query leaves it out.`;
      }
      if (node === 'late') {
        if (ev.type === 'wm') return `The same watermark reaches the dead letter queue's filter. Both filters hang off the one source, so they always agree on what's late.`;
        if (ev.wm == null) {
          return r.order_id === 101
            ? 'The other branch gets the same order. CURRENT_WATERMARK is NULL, so event_time <= NULL is NULL too, and the order is dropped here. It\'s counted in the main query instead.'
            : `Still no watermark, so order ${r.order_id} is dropped here.`;
        }
        if (ev.type === 'pass') return `Order ${r.order_id}, at ${r.event_time}, is at or behind the watermark (${ev.wm}). It's late, so it passes the filter, and goes to late_orders.`;
        return `Order ${r.order_id} is ahead of the watermark, so it isn't late. It's dropped here; the main query counts it.`;
      }
      if (node === 'sales') {
        if (ev.type === 'wm') return 'The watermark reaches the sink, which doesn\'t write it to Kafka.';
        return `The sink appends store ${r.store_id}'s result for ${r.window_start}–${r.window_end} to store_sales: ${result(r)}. It's final.`;
      }
      if (node === 'dlq') {
        if (ev.type === 'wm') return 'The watermark reaches the sink, which doesn\'t write it to Kafka.';
        return `The sink appends order ${r.order_id} to late_orders, the dead letter queue. It isn't counted in store_sales, but it isn't lost: it can be inspected, or corrected and sent again.`;
      }
      return null;
    },
    explain: `
Sink(table=[late_orders], fields=[order_id, store_id, pizza_id, channel, total, delivery_address, event_time],
     changelogMode=[NONE])
+- Calc(select=[order_id, store_id, pizza_id, channel, total, delivery_address, event_time],
     where=[<=(CAST(event_time AS TIMESTAMP(3)), CURRENT_WATERMARK(CAST(event_time AS TIMESTAMP(3))))],
     changelogMode=[I])
   +- TableSourceScan(table=[[orders, watermark=[-(event_time, 1000:INTERVAL SECOND)],
        watermarkEmitStrategy=[on-periodic]]], fields=[...], changelogMode=[I])

Sink(table=[store_sales], fields=[window_start, window_end, store_id, orders, sales], changelogMode=[NONE])
+- Calc(select=[window_start, window_end, store_id, orders, sales], changelogMode=[I])
   +- GlobalWindowAggregate(groupBy=[store_id], window=[TUMBLE(slice_end=[$slice_end], size=[1 min])],
        select=[store_id, COUNT(count1$0) AS orders, SUM(sum$1) AS sales,
                start('w$) AS window_start, end('w$) AS window_end], changelogMode=[I])
      +- Exchange(distribution=[hash[store_id]], changelogMode=[I])
         +- LocalWindowAggregate(groupBy=[store_id], window=[TUMBLE(time_col=[event_time], size=[1 min])],
              select=[store_id, COUNT(*) AS count1$0, SUM(total) AS sum$1, slice_end('w$) AS $slice_end],
              changelogMode=[I])
            +- Calc(select=[store_id, total, event_time],
                 where=[OR(IS NULL(CURRENT_WATERMARK(CAST(event_time AS TIMESTAMP(3)))),
                           >(CAST(event_time AS TIMESTAMP(3)), CURRENT_WATERMARK(CAST(event_time AS TIMESTAMP(3)))))],
                 changelogMode=[I])
               +- TableSourceScan(table=[[orders, watermark=[-(event_time, 1000:INTERVAL SECOND)],
                    watermarkEmitStrategy=[on-periodic]]], fields=[...], changelogMode=[I])

In the execution plan, the two scans are one: TableSourceScan(...)(reuse_id=[1]),
and each branch reads from Reused(reference_id=[1]).`,
    details: `
<p>A statement set runs several <code>INSERT</code>s as one job. The planner optimizes them
together, and shares what they have in common: here, one Kafka source feeds both branches. The
topic is read once, and both branches see the same records and the same watermarks. Run as two
separate jobs, each would read <code>orders</code> on its own, with its own watermark, and they
could disagree about what's late.</p>
<p>Flink SQL has no side output for late rows, so the query builds one. Both <code>INSERT</code>s
use one definition of late: an order at or behind the watermark.
<code>CURRENT_WATERMARK(event_time)</code> returns the watermark at the operator that evaluates it.
The dead letter queue keeps the late orders, and the main query keeps the rest, before its window.
The <code>IS NULL</code> check matters: <code>CURRENT_WATERMARK</code> is NULL until the first
watermark arrives, and without the check the main query would drop every order from the start of
the job.</p>
<p>Every order lands in exactly one topic. Both filters are Calcs chained to the source, so for
each order they see the same watermark, and their conditions are exact opposites, NULL included.
That holds at any parallelism. And the window never drops an order that got through: the order is
ahead of the filter's watermark, and the window's own watermark, which waits for every source
subtask, can't be ahead of that.</p>
<p>The trade-off is that this is stricter than the window's own rule. A window drops an order only
once its window has fired. Order 106 is behind the watermark, but its window is still open, so a
window on its own would have counted it; here it goes to <code>late_orders</code> instead. In
return, the definition doesn't depend on the window: the same two filters work for hopping,
cumulating and session windows, or for a query with no window at all.</p>`,
  };

  window.PATTERNS.push({
    id: 'late',
    nav: 'Late events',
    title: 'Late events to a dead letter queue',
    summary: '<p>A window silently drops orders that arrive after it has fired. A statement set can run a second <code>INSERT</code> in the same job, over the same source, that catches exactly those orders and writes them to a topic of their own.</p>',
    variants: [late],
  });
})();
