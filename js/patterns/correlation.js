// Pattern: event correlation with an interval join.
// Verified against verification/interval-join.sql (see interval-join.plan.txt).
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const order = (order_id, store_id, event_time) => ({ at: 'orders', value: { order_id, store_id, event_time } });
  const delivery = (order_id, driver_id, event_time) => ({ at: 'deliveries', value: { order_id, driver_id, event_time } });
  const wmFrom = (at) => ({ at, wm: true });
  const hm = (t) => t.slice(0, 5);

  const interval = {
    id: 'interval',
    label: 'Interval join',
    question: 'Which orders haven\'t been delivered within 45 minutes?',
    intro: 'Two topics: orders, and deliveries. An order is late if no delivery for it arrives within 45 minutes of the order. Press Play, or step through with the arrow keys.',
    sql: `
INSERT INTO late_orders
SELECT o.order_id, o.store_id, o.event_time AS ordered_at
FROM orders o
LEFT JOIN deliveries d
  ON o.order_id = d.order_id
 AND d.event_time BETWEEN o.event_time
                      AND o.event_time + INTERVAL '45' MINUTE
WHERE d.order_id IS NULL;`,
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

CREATE TABLE deliveries (
  order_id    BIGINT,
  driver_id   INT,
  event_time  TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ('connector' = 'kafka', 'topic' = 'deliveries');

CREATE TABLE late_orders (
  order_id    BIGINT,
  store_id    INT,
  ordered_at  TIMESTAMP(3)
) WITH ('connector' = 'kafka', 'topic' = 'late_orders');

-- Connection and format options are left out.`,
    facts: {
      state: 'Both sides are buffered by order_id: each order until the watermark passes its time + 45 minutes, each delivery until the watermark passes its own time. The interval bounds the state.',
      watermarks: 'The join takes the minimum of both inputs\' watermarks. That decides when an order can no longer be matched, so it\'s emitted with no delivery, and when buffered rows are freed. The output watermark is held back by 45 minutes.',
      output: 'Append-only, even for the outer join: an order with no delivery is only emitted once it can no longer match.',
    },
    graphLabel: 'Job graph: Kafka sources orders and deliveries, both hash partitioned by order_id into IntervalJoin, then a Calc that keeps unmatched orders, then Kafka sink late_orders. All edges are append-only.',
    graph: {
      nodes: [
        {
          id: 'orders', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [3],
          cfg: { topic: 'orders', delay: 1, timeField: 'event_time', columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'event_time' }] },
        },
        {
          id: 'deliveries', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['deliveries'], sqlLines: [4],
          cfg: { topic: 'deliveries', delay: 1, timeField: 'event_time', columns: [{ f: 'order_id' }, { f: 'driver_id', num: true }, { f: 'event_time' }] },
        },
        {
          id: 'join', kind: 'op', op: 'intervalJoin', title: 'IntervalJoin', lines: ['LEFT OUTER', 'within 45 min'],
          stateful: true, sqlLines: [4, 5, 6, 7],
          cfg: {
            left: 'orders', key: 'order_id', leftTime: 'event_time', rightTime: 'event_time', lower: 0, upper: 2700,
            leftName: 'order', rightName: 'delivery', leftTopic: 'orders', rightTopic: 'deliveries',
            joinRow: (o, d) => ({ order_id: o.order_id, store_id: o.store_id, ordered_at: o.event_time, delivered_at: d.event_time }),
            padRow: (o) => ({ order_id: o.order_id, store_id: o.store_id, ordered_at: o.event_time, delivered_at: null }),
          },
        },
        {
          id: 'calc', kind: 'op', op: 'calc', title: 'Calc', lines: ['d.order_id IS NULL'], sqlLines: [2, 8],
          cfg: {
            where: (r) => r.delivered_at == null,
            project: (r) => ({ order_id: r.order_id, store_id: r.store_id, ordered_at: r.ordered_at }),
          },
        },
        {
          id: 'sink', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: ['late_orders'], sqlLines: [1],
          cfg: { topic: 'late_orders', columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'ordered_at' }] },
        },
      ],
      edges: [
        { from: 'orders', to: 'join', mode: 'append', kinds: '+I', exchange: 'hash(order_id)' },
        { from: 'deliveries', to: 'join', mode: 'append', kinds: '+I', exchange: 'hash(order_id)' },
        { from: 'join', to: 'calc', mode: 'append', kinds: '+I' },
        { from: 'calc', to: 'sink', mode: 'append', kinds: '+I' },
      ],
    },
    inputs: [
      order(101, 7, '12:00:00'), order(102, 9, '12:05:00'), wmFrom('orders'),
      order(103, 7, '12:10:00'),
      delivery(101, 31, '12:31:00'), wmFrom('deliveries'),
      delivery(102, 18, '12:40:00'),
      order(104, 9, '12:58:00'), wmFrom('orders'),
      delivery(104, 31, '13:20:00'), wmFrom('deliveries'),
    ],
    pill: (r) => {
      if ('delivered_at' in r) return r.delivered_at ? `${r.order_id} delivered` : `${r.order_id} no delivery`;
      if ('ordered_at' in r) return `${r.order_id} late`;
      if ('driver_id' in r) return `d${r.order_id} ${hm(r.event_time)}`;
      return `${r.order_id} ${hm(r.event_time)}`;
    },
    narrate(node, ev) {
      if (node === 'orders' || node === 'deliveries') {
        if (ev.type === 'wm') return `The ${node} source emits a watermark for ${ev.time}.`;
        const r = ev.row;
        return node === 'orders'
          ? `The orders source reads order ${r.order_id}, placed at ${r.event_time}.`
          : `The deliveries source reads the delivery of order ${r.order_id}, at ${r.event_time}.`;
      }
      if (node === 'join') {
        if (ev.type === 'left') {
          const r = ev.row;
          if (ev.matches.length) return `Order ${r.order_id} matches a buffered delivery, so the joined row is emitted.`;
          return `Order ${r.order_id} has no delivery yet. It's buffered: a delivery up to ${U.fromSec(U.toSec(r.event_time) + 2700)} can still match it.`;
        }
        if (ev.type === 'right') {
          const r = ev.row;
          const late = U.toSec(r.event_time) - U.toSec(ev.matches[0]?.event_time || r.event_time);
          return ev.matches.length
            ? `The delivery of order ${r.order_id} comes ${Math.round(late / 60)} minutes after the order, within the interval. The joined row is emitted, and the buffered order is marked as matched. The delivery is buffered too, in case another order could match it.`
            : `No buffered order matches this delivery. It's buffered, in case a matching order arrives.`;
        }
        if (ev.type === 'hold') {
          const other = ev.side === 'left' ? 'deliveries' : 'orders';
          return ev.W
            ? `The ${ev.side === 'left' ? 'orders' : 'deliveries'} watermark moves to ${ev.time}, but the ${other} watermark is behind, so the join's watermark, the minimum of the two, stays at ${ev.W}.`
            : `The join records the ${ev.side === 'left' ? 'orders' : 'deliveries'} watermark, ${ev.time}. It has no watermark from ${other} yet, so its own watermark, the minimum of the two, can't advance.`;
        }
        if (ev.type === 'advance') {
          const parts = [`The join's watermark, the minimum of both inputs, moves to ${ev.W}.`];
          if (ev.padded.length) {
            parts.push(`That's more than 45 minutes after order ${ev.padded.map((r) => r.order_id).join(' and ')}, which never matched a delivery. No delivery can match ${ev.padded.length > 1 ? 'them' : 'it'} now, so ${ev.padded.length > 1 ? 'they are' : 'it is'} emitted with no delivery.`);
          }
          const freed = ev.expired.length + ev.freedRight.length;
          if (freed) parts.push(`${freed} buffered row${freed > 1 ? 's are' : ' is'} freed.`);
          if (!ev.padded.length && !freed) parts.push('No buffered row has expired yet.');
          parts.push(`The output watermark is held back by the 45-minute interval, to ${ev.outWm}.`);
          return parts.join(' ');
        }
      }
      if (node === 'calc') {
        if (ev.type === 'wm') return 'The watermark passes through the Calc.';
        return ev.type === 'pass'
          ? `Order ${ev.row.order_id} has no delivery, so it passes the WHERE clause.`
          : `Order ${ev.row.order_id} was delivered in time, so the WHERE clause drops it.`;
      }
      if (node === 'sink') {
        return ev.type === 'wm'
          ? 'Watermarks aren\'t written to Kafka.'
          : `The sink appends order ${ev.row.order_id} to late_orders.`;
      }
      return null;
    },
    explain: `
Sink(table=[late_orders], fields=[order_id, store_id, ordered_at], changelogMode=[NONE])
+- Calc(select=[order_id, store_id, event_time AS ordered_at], where=[IS NULL(order_id0)], changelogMode=[I])
   +- IntervalJoin(joinType=[LeftOuterJoin], windowBounds=[isRowTime=true, leftLowerBound=-2700000,
        leftUpperBound=0, leftTimeIndex=2, rightTimeIndex=1], where=[AND(=(order_id, order_id0),
        >=(event_time0, event_time), <=(event_time0, +(event_time, 2700000)))], select=[...], changelogMode=[I])
      :- Exchange(distribution=[hash[order_id]], changelogMode=[I])
      :  +- Calc(select=[order_id, store_id, Reinterpret(event_time) AS event_time], changelogMode=[I])
      :     +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])
      +- Exchange(distribution=[hash[order_id]], changelogMode=[I])
         +- Calc(select=[order_id, event_time], changelogMode=[I])
            +- TableSourceScan(table=[[deliveries, watermark=[...]]], fields=[...], changelogMode=[I])`,
    details: `
<p>An interval join correlates two streams on a key, but only for events that happen close together
in time. What makes it an interval join, and not a regular join, is the <code>BETWEEN</code> on the
event times of both sides, with a bounded interval.</p>
<p>The bound is what keeps the state small. Each order is buffered until the watermark passes its
time plus 45 minutes; after that, no delivery can match it, so it's freed. A regular join would have
to keep every order and every delivery forever.</p>
<p>Because this is a <code>LEFT JOIN</code>, every order comes out: joined with its delivery as soon
as both have arrived, or null-padded once the watermark proves no delivery can still match. The
null-padded row is only emitted when it's final, so the output stays append-only. The
<code>WHERE d.order_id IS NULL</code> then keeps only the orders that were never delivered.</p>
<p>A two-input operator's watermark is the minimum of its inputs' watermarks. If deliveries stop
arriving, the join's watermark stops too, and late orders are never reported. Watch for idle
inputs.</p>
<p>The join holds back its output watermark by the length of the interval, because it can still
emit rows (the null-padded orders) that far behind. In Flink 2.3, unmatched rows are cleaned up as
soon as the watermark passes their bound. Setting
<code>table.exec.interval-join.min-cleanup-interval</code> batches the clean-up, and delays the
null-padded rows.</p>`,
  };

  window.PATTERNS.push({
    id: 'correlation',
    nav: 'Event correlation',
    title: 'Event correlation',
    summary: '<p>Join two streams on a key, but only for events that happen within a bounded time of each other. The interval bounds the state, and the watermark decides when a row can no longer be matched.</p>',
    variants: [interval],
  });
})();
