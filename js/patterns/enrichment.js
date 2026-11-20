// Pattern: enrichment, with a lookup join, an event-time temporal join, and a LATERAL SNAPSHOT
// join. Verified against verification/enrichment.sql (Flink 2.3) and
// verification/lateral-snapshot.sql (Flink 2.4-SNAPSHOT).
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const money = { num: true, fmt: U.money };
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;

  // ---------- Lookup join ----------

  const lookup = {
    id: 'lookup',
    label: 'Lookup join',
    question: 'How long is the drive to each delivery address?',
    intro: 'Each delivery order asks a maps service for the drive time from its store. The connector caches the answers. Press Play, or step through with the arrow keys.',
    sql: `
INSERT INTO delivery_times
SELECT o.order_id, o.delivery_address, r.drive_minutes
FROM orders AS o
JOIN routes FOR SYSTEM_TIME AS OF o.proc_time AS r
  ON o.store_id = r.store_id
 AND o.delivery_address = r.address;`,
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

-- The maps service, behind a lookup connector. Any connector
-- that implements lookup is planned the same way.
CREATE TABLE routes (
  store_id       INT,
  address        STRING,
  drive_minutes  INT
) WITH (
  'connector' = '...',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.expire-after-write' = '1 h'
);

CREATE TABLE delivery_times (
  order_id          BIGINT,
  delivery_address  STRING,
  drive_minutes     INT
) WITH ('connector' = 'kafka', 'topic' = 'delivery_times');

-- Connection and format options are left out.`,
    matrix: { state: 'None in Flink: the connector caches lookups', watermarks: 'Not used', watermarksOut: 'Passed through' },
    facts: {
      state: 'None in Flink. The connector\'s cache, of recent lookups, lives in memory, and isn\'t checkpointed.',
      watermarks: 'Not used. The lookup happens in processing time: each order sees whatever the service answers when it\'s processed.',
      output: 'Append-only. The join adds columns to each order as it passes.',
    },
    graphLabel: 'Job graph: Kafka source orders into LookupJoin, which queries the maps service, then Kafka sink delivery_times. Both stream edges are append-only.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [3],
          cfg: { topic: 'orders', columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'delivery_address' }] },
        },
        {
          id: 'lookup', kind: 'op', op: 'lookupJoin', title: 'LookupJoin', lines: ['routes', 'AS OF proc_time'],
          panel: true, sqlLines: [2, 4, 5, 6],
          cfg: {
            ext: 'maps',
            lookupKey: (r) => `${r.store_id}|${r.delivery_address}`,
            table: { '7|12 Elm St': 14, '9|4 Oak Ave': 9 },
            cacheCols: ['store', 'address', 'minutes'],
            cacheCells: (k, v) => [...k.split('|'), v == null ? 'none' : v],
            joinRow: (o, minutes) => ({ order_id: o.order_id, delivery_address: o.delivery_address, drive_minutes: minutes }),
          },
        },
        { id: 'maps', kind: 'external', op: 'external', title: 'Maps service', lines: ['routes'] },
        {
          id: 'sink', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: ['delivery_times'], sqlLines: [1],
          cfg: { topic: 'delivery_times', columns: [{ f: 'order_id' }, { f: 'delivery_address' }, { f: 'drive_minutes', num: true }] },
        },
      ],
      edges: [
        { from: 'src', to: 'lookup', mode: 'append', kinds: '+I' },
        { from: 'lookup', to: 'maps', mode: 'lookup', label: 'lookup on a cache miss' },
        { from: 'lookup', to: 'sink', mode: 'append', kinds: '+I' },
      ],
    },
    inputs: [
      { value: { order_id: 101, store_id: 7, delivery_address: '12 Elm St' } },
      { value: { order_id: 102, store_id: 9, delivery_address: '4 Oak Ave' } },
      { value: { order_id: 103, store_id: 7, delivery_address: '12 Elm St' } },
      { value: { order_id: 104, store_id: 7, delivery_address: '90 Pine Rd' } },
    ],
    pill: (r) => (r.drive_minutes != null ? `${r.order_id} ${r.drive_minutes} min` : `${r.order_id} s${r.store_id}`),
    narrate(node, ev) {
      if (node === 'src') return `The source reads order ${ev.row.order_id}, from store ${ev.row.store_id}, to ${ev.row.delivery_address}.`;
      if (node === 'lookup') {
        const r = ev.row;
        if (ev.type === 'hit') return `Order ${r.order_id} goes to ${r.delivery_address} again, a repeat customer. The answer, ${ev.value} minutes, is in the cache, so the maps service isn't called.`;
        return ev.value == null
          ? `Order ${r.order_id}: no cached answer, so the connector asks the maps service. It has no route to ${r.delivery_address}, outside the delivery area. With an inner join, the order is dropped. The empty answer is cached too.`
          : `Order ${r.order_id}: no cached answer, so the connector asks the maps service, which says ${ev.value} minutes. The answer is cached, and the joined row is emitted.`;
      }
      if (node === 'sink') return `The sink appends order ${ev.row.order_id}: ${ev.row.drive_minutes} minutes' drive.`;
      return null;
    },
    explain: `
Sink(table=[delivery_times], fields=[order_id, delivery_address, drive_minutes], changelogMode=[NONE])
+- Calc(select=[order_id, delivery_address, drive_minutes], changelogMode=[I])
   +- LookupJoin(table=[routes], joinType=[InnerJoin], lookup=[store_id=store_id, address=delivery_address],
        select=[...], changelogMode=[I])
      +- Calc(select=[order_id, store_id, delivery_address], changelogMode=[I])
         +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])

Verified with the Apache Flink JDBC connector (4.1), which supports lookup joins.`,
    details: `
<p>A lookup join enriches each row by querying an external system as the row goes by.
<code>FOR SYSTEM_TIME AS OF o.proc_time</code> means "whatever the answer is right now". Flink keeps
no state for it, and the stream side stays stateless.</p>
<p>Use it when you can't stream the reference data into Flink: it's only available through a
request-response API, or it's too big to hold in state. Drive times are both. A maps service won't
send you every address, and there are far too many addresses to keep anyway.</p>
<p>The connector's cache matters a lot. Repeat customers order to the same addresses, and every
call to the service costs time and money, and counts against a rate limit. The cache lives in
memory, isn't checkpointed, and is lost on restart. By default, a partial cache also remembers
lookups that found nothing.</p>
<p>Because the answer depends on when each order is processed, a lookup join isn't deterministic.
Reprocess yesterday's orders and you get today's drive times. When the reference data can be
streamed, from a database through CDC for example, a temporal join is usually the better choice.</p>
<p>Many lookup connectors also support asynchronous lookups, so the operator doesn't wait for one
answer before sending the next request.</p>`,
  };

  // ---------- Shared by the two pizza-cost joins ----------

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
) WITH ('connector' = 'kafka', 'topic' = 'orders');

CREATE TABLE pizza_costs (
  pizza_id    INT,
  food_cost   DECIMAL(10, 2),
  valid_from  TIMESTAMP(3),
  PRIMARY KEY (pizza_id) NOT ENFORCED,
  WATERMARK FOR valid_from AS valid_from - INTERVAL '1' SECOND
) WITH ('connector' = 'upsert-kafka', 'topic' = 'pizza_costs');

CREATE TABLE order_costs (
  order_id   BIGINT,
  pizza_id   INT,
  total      DECIMAL(10, 2),
  food_cost  DECIMAL(10, 2)
) WITH ('connector' = 'kafka', 'topic' = 'order_costs');

-- Connection and format options are left out.`;

  const order = (order_id, pizza_id, total, event_time) => ({ at: 'orders', value: { order_id, pizza_id, total, event_time } });
  const cost = (pizza_id, food_cost, valid_from) => ({ at: 'costs', key: pizza_id, value: { food_cost, valid_from } });
  const wmFrom = (at) => ({ at, wm: true });

  const ordersNode = {
    id: 'orders', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [3],
    cfg: { topic: 'orders', delay: 1, timeField: 'event_time', columns: [{ f: 'order_id' }, { f: 'pizza_id', num: true }, { f: 'total', ...money }, { f: 'event_time' }] },
  };
  const costsNode = {
    id: 'costs', kind: 'source', op: 'upsertKafkaSource', title: 'Kafka source', lines: ['pizza_costs', 'upsert-kafka'], sqlLines: [4],
    cfg: { topic: 'pizza_costs', key: 'pizza_id', showKey: true, delay: 1, timeField: 'valid_from', columns: [{ f: 'food_cost', ...money }, { f: 'valid_from' }] },
  };
  const sinkNode = {
    id: 'sink', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: ['order_costs'], sqlLines: [1],
    cfg: { topic: 'order_costs', columns: [{ f: 'order_id' }, { f: 'pizza_id', num: true }, { f: 'total', ...money }, { f: 'food_cost', ...money }] },
  };
  const joinRow = (o, c) => ({ order_id: o.order_id, pizza_id: o.pizza_id, total: o.total, food_cost: c.food_cost });
  const pill = (r) => {
    if (r.food_cost != null && r.order_id != null) return `${r.order_id} P${r.pizza_id} ${U.money(r.food_cost)}`;
    if (r.food_cost != null) return `P${r.pizza_id} ${U.money(r.food_cost)}`;
    if (r.order_id != null) return `${r.order_id} P${r.pizza_id}`;
    return `P${r.pizza_id}`;
  };
  const sourceNarration = (node, ev) => {
    if (ev.type === 'wm') return `The ${node === 'orders' ? 'orders' : 'pizza_costs'} source emits a watermark for ${ev.time}: its newest event time minus 1 second.`;
    if (node === 'orders') return `The orders source reads order ${ev.row.order_id}: pizza ${ev.row.pizza_id}, placed at ${ev.row.event_time}.`;
    return `The pizza_costs source reads a new cost for pizza ${ev.key}: ${U.money(ev.row.food_cost)}, valid from ${ev.row.valid_from}. The planner treats it as an upsert (+U) for pizza ${ev.key}.`;
  };

  // ---------- Event-time temporal join ----------

  const temporal = {
    id: 'temporal',
    label: 'Temporal join',
    question: 'What did each pizza cost to make, when it was ordered?',
    blurb: 'Food costs change when ingredient prices do. <code>pizza_costs</code> is an upsert table with the current cost of each pizza, and <code>valid_from</code> says when each cost took effect.',
    intro: 'Two topics: orders, and pizza_costs. Each order should be joined with the cost that was valid when the order was placed. Press Play, or step through with the arrow keys.',
    sql: `
INSERT INTO order_costs
SELECT o.order_id, o.pizza_id, o.total, c.food_cost
FROM orders AS o
JOIN pizza_costs FOR SYSTEM_TIME AS OF o.event_time AS c
  ON o.pizza_id = c.pizza_id;`,
    ddl: ordersDdl,
    matrix: { state: 'Waiting orders, and the versions of each cost', watermarks: 'Holds results until both inputs catch up', watermarksOut: 'The minimum of both inputs' },
    facts: {
      state: 'Orders wait in state until the watermark reaches them. For each pizza, its cost versions, and one timer. When the timer fires, versions older than the latest one at or before the watermark are removed.',
      watermarks: 'They drive the join. An order is joined once the watermark, the minimum of both inputs, passes its time, so that every cost change up to then has arrived. If pizza_costs goes quiet, the join stalls.',
      output: 'Append-only. Each order is joined once, with the cost that was valid when it was placed.',
    },
    graphLabel: 'Job graph: Kafka sources orders (append-only) and pizza_costs (upsert), both hash partitioned by pizza_id into TemporalJoin, then Kafka sink order_costs.',
    graph: {
      nodes: [
        ordersNode, costsNode,
        {
          id: 'join', kind: 'op', op: 'temporalJoin', title: 'TemporalJoin', lines: ['AS OF o.event_time'],
          stateful: true, sqlLines: [2, 4, 5],
          cfg: { left: 'orders', key: 'pizza_id', leftId: 'order_id', leftTime: 'event_time', rightTime: 'valid_from', valueField: 'food_cost', leftName: 'order', rightName: 'cost', keyName: 'pizza', leftTopic: 'orders', rightTopic: 'pizza_costs', joinRow },
        },
        sinkNode,
      ],
      edges: [
        { from: 'orders', to: 'join', mode: 'append', kinds: '+I', exchange: 'hash(pizza_id)' },
        { from: 'costs', to: 'join', mode: 'upsert', kinds: '+U -D', exchange: 'hash(pizza_id)' },
        { from: 'join', to: 'sink', mode: 'append', kinds: '+I' },
      ],
    },
    inputs: [
      cost(1, 4.00, '10:00:00'), cost(2, 5.50, '10:00:00'), wmFrom('costs'),
      order(1001, 1, 14.00, '10:02:00'), order(1002, 2, 16.00, '10:04:00'), wmFrom('orders'),
      cost(1, 5.00, '10:05:00'), wmFrom('costs'),
      order(1003, 1, 14.00, '10:07:00'), wmFrom('orders'),
      cost(2, 6.00, '10:12:00'), wmFrom('costs'),
      order(1004, 2, 16.00, '10:13:00'), wmFrom('orders'),
    ],
    pill,
    narrate(node, ev) {
      if (node === 'orders' || node === 'costs') return sourceNarration(node, ev);
      if (node === 'join') {
        if (ev.type === 'buffer') return `Order ${ev.row.order_id} waits in state. A cost change for pizza ${ev.row.pizza_id} from before ${ev.row.event_time} could still arrive, so the join can't know the right cost yet.`;
        if (ev.type === 'version') return `The cost of pizza ${ev.row.pizza_id} from ${ev.row.valid_from} is stored as a new version, next to any older ones.`;
        if (ev.type === 'hold') {
          const lag = ev.side === 'left' ? 'pizza_costs' : 'orders';
          return ev.W
            ? `The ${ev.side === 'left' ? 'orders' : 'pizza_costs'} watermark moves to ${ev.time}, but ${lag} is behind, so the join's watermark stays at ${ev.W}, and nothing more can be joined.`
            : `The join records the watermark, ${ev.time}, but has none from ${lag} yet, so its own watermark can't advance.`;
        }
        const parts = [`The join's watermark, the minimum of both inputs, moves to ${ev.W}.`];
        if (ev.results.length) {
          parts.push(ev.results.map((r) => (r.version
            ? `Order ${r.left.order_id} (${r.left.event_time}) is joined with pizza ${r.left.pizza_id}'s cost as of then: ${U.money(r.version.food_cost)}, valid from ${r.version.valid_from}.`
            : `Order ${r.left.order_id} has no cost version, so nothing is emitted.`)).join(' '));
        } else {
          parts.push('No waiting order is at or before it yet.');
        }
        if (ev.removed.length) parts.push(`Pizza ${ev.removed[0].pizza_id}'s timer has fired, so the join also cleans up its versions: the ${ev.removed.map((v) => U.money(v.food_cost)).join(' and ')} version${ev.removed.length > 1 ? 's are' : ' is'} older than the latest one at or before the watermark, and can't be needed any more.`);
        return parts.join(' ');
      }
      if (node === 'sink') {
        return ev.type === 'wm' ? 'Watermarks aren\'t written to Kafka.' : `The sink appends order ${ev.row.order_id}: paid ${U.money(ev.row.total)}, cost ${U.money(ev.row.food_cost)} to make.`;
      }
      return null;
    },
    explain: `
Sink(table=[order_costs], fields=[order_id, pizza_id, total, food_cost], changelogMode=[NONE])
+- Calc(select=[order_id, pizza_id, total, food_cost], changelogMode=[I])
   +- TemporalJoin(joinType=[InnerJoin], where=[AND(=(pizza_id, pizza_id0),
        __TEMPORAL_JOIN_CONDITION(event_time, valid_from, __TEMPORAL_JOIN_CONDITION_PRIMARY_KEY(pizza_id0), ...))],
        select=[...], changelogMode=[I])
      :- Exchange(distribution=[hash[pizza_id]], changelogMode=[I])
      :  +- Calc(select=[order_id, pizza_id, total, Reinterpret(event_time) AS event_time], changelogMode=[I])
      :     +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])
      +- Exchange(distribution=[hash[pizza_id]], changelogMode=[I,UA,D])
         +- TableSourceScan(table=[[pizza_costs, watermark=[-(valid_from, 1000:INTERVAL SECOND)]]],
              fields=[pizza_id, food_cost, valid_from], changelogMode=[UA,D])`,
    details: `
<p><code>FOR SYSTEM_TIME AS OF o.event_time</code> asks for the version of the pizza's cost that was
valid when the order was placed. <code>pizza_costs</code> is a <em>versioned table</em>: it has a
primary key, and an event-time attribute that says when each version took effect.</p>
<p>To find the right version, the join has to wait until no earlier cost change can still arrive.
That's what the watermark tells it. Order 1001, at 10:02, is only joined once the watermark has
passed 10:02, and by then the 10:05 price rise has already arrived. It gets the 10:00 cost anyway,
because that was the one in force at 10:02. Run the same input again and you get the same result.</p>
<p>The price is latency, and a dependence on both watermarks. Costs rarely change, but the join's
watermark is the minimum of the two inputs. If <code>pizza_costs</code> goes quiet, its watermark
stops, the join stops emitting, and orders pile up in state. Set an idle timeout on the source, or
consider a LATERAL SNAPSHOT join.</p>
<p>The planner doesn't need a ChangelogNormalize here: the temporal join keeps its own versions of
the upsert table.</p>`,
  };

  // ---------- LATERAL SNAPSHOT join ----------

  const lateral = {
    id: 'lateral',
    label: 'LATERAL SNAPSHOT',
    flinkVersion: '2.4-SNAPSHOT',
    question: 'What does each pizza cost to make right now, as orders come in?',
    blurb: '<strong>Coming in Flink 2.4, which isn\'t released yet.</strong> Checked against the 2.4 snapshot build. A LATERAL SNAPSHOT join first loads the costs table, then joins each order with the current cost as soon as it arrives.',
    intro: 'The same two topics. Suppose the query was compiled at 10:00:00: the join loads pizza_costs until its watermark reaches that time, holding orders back, then switches to joining them. Press Play, or step through with the arrow keys.',
    sql: `
INSERT INTO order_costs
SELECT o.order_id, o.pizza_id, o.total, c.food_cost
FROM orders AS o
JOIN LATERAL SNAPSHOT(
       input   => TABLE pizza_costs,
       on_time => DESCRIPTOR(valid_from)) AS c
  ON o.pizza_id = c.pizza_id;`,
    ddl: ordersDdl,
    matrix: { state: 'The current costs; while loading, buffered orders and pending changes', watermarks: 'Ends the load phase, and decides when each cost change takes effect', watermarksOut: 'The orders watermark only, held back until loading ends. The costs watermark is never forwarded' },
    facts: {
      state: 'The current cost rows, per pizza, and cost changes waiting for the watermark. Orders are buffered only during the load phase. ChangelogNormalize also keeps the last row of every pizza.',
      watermarks: 'The costs watermark decides when the load phase ends, and when each cost change becomes visible. Orders don\'t wait for watermarks in the join phase, and the order watermark is forwarded.',
      output: 'Append-only. Each order is joined once, with the cost that\'s current when it\'s processed. Not deterministic.',
    },
    graphLabel: 'Job graph: Kafka source orders, and Kafka source pizza_costs through ChangelogNormalize, both hash partitioned by pizza_id into LateralSnapshotJoin, then Kafka sink order_costs.',
    graph: {
      nodes: [
        ordersNode, { ...costsNode, sqlLines: [5] },
        {
          id: 'cn', kind: 'op', op: 'changelogNormalize', title: 'ChangelogNormalize', lines: ['key: pizza_id'],
          stateful: true, planner: true, sqlLines: [5],
          cfg: { key: 'pizza_id', generateUpdateBefore: true, stateTitle: 'Last row per pizza_id', stateFields: ['pizza_id', 'food_cost', 'valid_from'], stateFormat: { food_cost: U.money } },
        },
        {
          id: 'join', kind: 'op', op: 'lateralSnapshotJoin', title: 'LateralSnapshotJoin', lines: ['load, then join'],
          stateful: true, sqlLines: [2, 4, 5, 6, 7],
          cfg: {
            left: 'orders', key: 'pizza_id', buildTime: 'valid_from', loadCompletedTime: '10:00:00',
            probeId: 'order_id', probeTime: 'event_time', probeName: 'order', valueField: 'food_cost', joinRow,
          },
        },
        { ...sinkNode, sqlLines: [1] },
      ],
      edges: [
        { from: 'orders', to: 'join', mode: 'append', kinds: '+I', exchange: 'hash(pizza_id)' },
        { from: 'costs', to: 'cn', mode: 'upsert', kinds: '+U -D', exchange: 'hash(pizza_id)' },
        { from: 'cn', to: 'join', mode: 'retract', kinds: '+I -U +U -D', exchange: 'hash(pizza_id)' },
        { from: 'join', to: 'sink', mode: 'append', kinds: '+I' },
      ],
    },
    inputs: [
      cost(1, 4.00, '09:00:00'), cost(2, 5.50, '09:30:00'), wmFrom('costs'),
      order(1001, 1, 14.00, '09:59:50'), order(1002, 2, 16.00, '09:59:55'), wmFrom('orders'),
      cost(1, 4.50, '10:00:30'), cost(3, 6.00, '10:01:00'), wmFrom('costs'),
      order(1003, 1, 14.00, '10:01:10'), order(1004, 3, 18.00, '10:01:20'), wmFrom('orders'),
      cost(4, 7.00, '10:02:00'), wmFrom('costs'),
      order(1005, 3, 18.00, '10:02:10'),
    ],
    pill,
    narrate(node, ev) {
      if (node === 'orders' || node === 'costs') return sourceNarration(node, ev);
      if (node === 'cn') {
        switch (ev.type) {
          case 'insert': return `Pizza ${ev.key} is new. ChangelogNormalize stores its cost and emits an insert (+I).`;
          case 'update': return `Pizza ${ev.key} already has a cost. ChangelogNormalize emits the old row as a retraction (−U), then the new one (+U). The join needs full retractions to remove the old cost.`;
          case 'wm': return 'The watermark passes through ChangelogNormalize.';
        }
        return null;
      }
      if (node === 'join') {
        switch (ev.type) {
          case 'buffer-change':
            return `The ${ev.kind} for pizza ${ev.row.pizza_id} (valid from ${ev.row.valid_from}) is buffered. It's applied once the costs watermark reaches ${ev.row.valid_from}.`;
          case 'buffer-probe':
            return `The join is still loading costs, so order ${ev.row.order_id} is buffered. Joining it now could use a cost table that isn't complete yet.`;
          case 'join':
            return `Order ${ev.row.order_id} is joined at once with the current cost of pizza ${ev.row.pizza_id}: ${U.money(ev.build.food_cost)}. It doesn't wait for any watermark.`;
          case 'no-match':
            return ev.pending.length
              ? `Pizza ${ev.row.pizza_id}'s cost (valid from ${ev.pending[0].valid_from}) has arrived, but the costs watermark hasn't reached it, so it isn't visible yet. No current row: with an inner join, order ${ev.row.order_id} is dropped.`
              : `There's no current cost for pizza ${ev.row.pizza_id}, so order ${ev.row.order_id} is dropped.`;
          case 'hold':
            return `The orders watermark, ${ev.time}, is held back: during the load phase the join emits nothing, not even watermarks.`;
          case 'build-wm': {
            const applied = ev.applied.length ? ` Changes that are now due are applied: ${ev.applied.map((c) => `${c.kind} pizza ${c.row.pizza_id} ${U.money(c.row.food_cost)}`).join(', ')}.` : ' No buffered change is due yet.';
            return `The costs watermark moves to ${ev.time}.${applied}${ev.phase === 'LOAD' ? ' It hasn\'t reached 10:00:00, so the join keeps loading.' : ' It\'s used internally, and never forwarded.'}`;
          }
          case 'flip': {
            const applied = ev.applied.map((c) => `${c.kind} pizza ${c.row.pizza_id} ${U.money(c.row.food_cost)}`).join(', ');
            const drained = ev.drained.length ? ` The buffered orders are joined with the current costs, and emitted, with the held-back orders watermark.` : '';
            return `The costs watermark reaches ${ev.time}, past 10:00:00, the time the query was compiled. Due changes are applied (${applied}), and the join switches from loading to joining.${drained}`;
          }
          case 'probe-wm':
            return `In the join phase, the orders watermark, ${ev.time}, is forwarded.`;
        }
      }
      if (node === 'sink') {
        return ev.type === 'wm' ? 'Watermarks aren\'t written to Kafka.' : `The sink appends order ${ev.row.order_id}: pizza ${ev.row.pizza_id}, cost ${U.money(ev.row.food_cost)} to make.`;
      }
      return null;
    },
    explain: `
Sink(table=[order_costs], fields=[order_id, pizza_id, total, food_cost], changelogMode=[NONE])
+- Calc(select=[order_id, pizza_id, total, food_cost], changelogMode=[I])
   +- LateralSnapshotJoin(joinType=[InnerJoin], where=[=(pizza_id, pizza_id0)], select=[...],
        loadCompletedCondition=[compile_time], loadCompletedTime=[...], changelogMode=[I])
      :- Exchange(distribution=[hash[pizza_id]], changelogMode=[I])
      :  +- Calc(select=[order_id, pizza_id, total], changelogMode=[I])
      :     +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])
      +- Exchange(distribution=[hash[pizza_id]], changelogMode=[I,UB,UA,D])
         +- ChangelogNormalize(key=[pizza_id], changelogMode=[I,UB,UA,D])
            +- Exchange(distribution=[hash[pizza_id]], changelogMode=[UA,D])
               +- TableSourceScan(table=[[pizza_costs, watermark=[...]]], fields=[...], changelogMode=[UA,D])`,
    details: `
<p>FLIP-579 adds the LATERAL SNAPSHOT join for enrichment with the <em>current</em> version of a
table. Each order is joined with the cost that's current when the order is processed, without
waiting for watermarks. Nothing stalls when <code>pizza_costs</code> goes quiet, and there's no
primary-key requirement on either side.</p>
<p>So that "current" means something, the join starts with a <em>load phase</em>. It applies cost
changes, and buffers orders, until the costs watermark reaches the load-completed time: by default,
the moment the query was compiled. Then it joins the buffered orders and switches to the
<em>join phase</em>. A static build side may never move its watermark that far, so set
<code>load_completed_idle_timeout</code>, or read it in bounded mode.</p>
<p>Cost changes are applied in the order of <code>on_time</code>, once the costs watermark reaches
them, in both phases. That's why order 1004 finds no cost for pizza 3 here: the cost has arrived,
but isn't visible yet. The planner adds a ChangelogNormalize because the join needs full
retractions to remove an old cost.</p>
<p>It's processing time. Order 1001 was placed at 09:59:50, but it's joined at the flip with the cost
current then, $4.50, not the $4.00 that a temporal join would give it. Run it twice and you can get
different results. When you need the cost as it was when each order was placed, use the event-time
temporal join.</p>
<p>The streaming syntax shown here, with named <code>input</code> and <code>on_time</code>
arguments, is what the 2.4 snapshot build accepts; <code>on_time</code> is required in streaming
mode. Details may still change before the release.</p>`,
  };

  window.PATTERNS.push({
    id: 'enrichment',
    nav: 'Enrichment',
    title: 'Enrichment',
    summary: '<p>Add context to each event from another table. Which join to use depends on where the reference data lives, and on which version of it you need: whatever it is now, or what it was when the event happened.</p>',
    variants: [lookup, temporal, lateral],
  });
})();
