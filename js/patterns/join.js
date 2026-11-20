// Pattern: a regular join between two keyed, changing tables (a live store leaderboard).
// Verified against verification/regular-join.sql (see regular-join.plan.txt).
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const money = { num: true, fmt: U.money };
  const order = (order_id, store_id, total, event_time) => ({ at: 'orders', value: { order_id, store_id, total, event_time } });
  const wmFrom = (at) => ({ at, wm: true });
  const store = (store_id, city, manager) => ({ at: 'stores', key: store_id, value: { city, manager } });
  const closed = (store_id) => ({ at: 'stores', key: store_id, value: null });
  const plural = (n, w) => `${n} ${w}${n === 1 ? '' : 's'}`;
  const totals = (r) => `${plural(r.orders, 'order')}, ${U.money(r.sales)}`;

  const leaderboard = {
    id: 'leaderboard',
    label: 'Aggregate, then join',
    question: 'How much has each store sold, and who manages it right now?',
    intro: 'Two inputs: the orders topic, and stores, a compacted topic with each store\'s city and manager. Press Play, or step through with the arrow keys.',
    sql: `
INSERT INTO store_leaderboard
SELECT s.store_id, s.city, s.manager, t.orders, t.sales
FROM (
  SELECT store_id, COUNT(*) AS orders, SUM(total) AS sales
  FROM orders
  GROUP BY store_id
) AS t
JOIN stores AS s
  ON t.store_id = s.store_id;`,
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

CREATE TABLE stores (
  store_id  INT,
  city      STRING,
  manager   STRING,
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'stores');

CREATE TABLE store_leaderboard (
  store_id  INT,
  city      STRING,
  manager   STRING,
  orders    BIGINT,
  sales     DECIMAL(38, 2),
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'store_leaderboard');

-- Connection and format options are left out.`,
    matrix: {
      state: 'An accumulator per key, and one row per key on each side of the join',
      watermarks: 'Not used',
      watermarksOut: 'None: stores has no watermark, so the join never emits one',
    },
    facts: {
      state: 'GroupAggregate keeps one accumulator per store. The join keeps one row per <code>store_id</code> on each side: the latest totals, and the latest store row. The number of stores bounds both, so the state doesn\'t grow with the orders.',
      watermarks: 'Not used. The join\'s watermark would be the minimum of its two inputs, and stores has no watermark, so none leaves the join.',
      output: 'Upsert, keyed by <code>store_id</code>. A change on either side updates the store\'s row: new totals, a new manager, or a delete when the store closes.',
    },
    graphLabel: 'Job graph: Kafka source orders, hash partitioned by store_id into GroupAggregate, then into Join (upsert). Kafka source stores (upsert-kafka) is hash partitioned by store_id into the same Join. The Join writes an upsert stream to Kafka sink store_leaderboard.',
    graph: {
      nodes: [
        {
          id: 'orders', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [5],
          cfg: { topic: 'orders', delay: 1, timeField: 'event_time', columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total', ...money }, { f: 'event_time' }] },
        },
        {
          id: 'agg', kind: 'op', op: 'groupAggregate', title: 'GroupAggregate', lines: ['GROUP BY store_id'],
          stateful: true, sqlLines: [4, 6],
          cfg: { key: 'store_id', keyLabel: 'store', generateUpdateBefore: false, aggs: [{ name: 'orders', fn: 'count' }, { name: 'sales', fn: 'sum', field: 'total' }] },
        },
        {
          id: 'stores', kind: 'source', op: 'upsertKafkaSource', title: 'Kafka source', lines: ['stores', 'upsert-kafka'], sqlLines: [8],
          cfg: { topic: 'stores', key: 'store_id', showKey: true, columns: [{ f: 'city' }, { f: 'manager' }] },
        },
        {
          id: 'join', kind: 'op', op: 'regularJoin', title: 'Join', lines: ['INNER, on store_id'],
          stateful: true, sqlLines: [8, 9],
          cfg: {
            left: 'agg', key: 'store_id', leftName: 'totals', rightName: 'store',
            joinRow: (t, s) => ({ store_id: t.store_id, city: s.city, manager: s.manager, orders: t.orders, sales: t.sales }),
            leftCell: (r) => totals(r),
            rightCell: (r) => `${r.city}, ${r.manager}`,
          },
        },
        {
          id: 'sink', kind: 'sink', op: 'upsertKafkaSink', title: 'Kafka sink', lines: ['store_leaderboard', 'upsert-kafka'], sqlLines: [1, 2],
          cfg: { topic: 'store_leaderboard', key: 'store_id', showKey: true, fields: ['city', 'manager', 'orders', 'sales'], columns: [{ f: 'city' }, { f: 'manager' }, { f: 'orders', num: true }, { f: 'sales', ...money }] },
        },
      ],
      edges: [
        { from: 'orders', to: 'agg', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' },
        { from: 'agg', to: 'join', mode: 'upsert', kinds: '+I +U', exchange: 'hash(store_id)' },
        { from: 'stores', to: 'join', mode: 'upsert', kinds: '+U -D', exchange: 'hash(store_id)' },
        { from: 'join', to: 'sink', mode: 'upsert', kinds: '+I +U -D' },
      ],
    },
    inputs: [
      store(7, 'Oakland', 'Sam'), store(9, 'Fresno', 'Priya'),
      order(101, 7, 18.00, '12:00:01'), order(102, 9, 24.50, '12:00:02'), order(103, 7, 31.00, '12:00:04'), wmFrom('orders'),
      store(7, 'Oakland', 'Alex'),
      store(12, 'San Jose', 'Kim'),
      order(104, 7, 13.00, '12:00:06'), order(105, 12, 22.00, '12:00:07'),
      closed(9),
    ],
    pill: (r) => {
      if (r.order_id != null) return `${r.order_id} s${r.store_id} ${U.money(r.total)}`;
      if (r.manager !== undefined && r.orders !== undefined) return r.manager ? `${r.store_id} ${r.manager} ${r.orders} ${U.money(r.sales)}` : `${r.store_id}`;
      if (r.orders !== undefined) return `s${r.store_id} ${r.orders} ${U.money(r.sales)}`;
      return r.manager ? `${r.store_id} ${r.manager}` : `${r.store_id}`;
    },
    narrate(node, ev) {
      if (node === 'orders') {
        if (ev.type === 'wm') return `The orders source emits a watermark for ${ev.time}.`;
        return `The orders source reads order ${ev.row.order_id}: store ${ev.row.store_id}, ${U.money(ev.row.total)}.`;
      }
      if (node === 'stores') {
        if (ev.type === 'tombstone') return `A tombstone for store ${ev.key}: the store has closed. The source emits a delete that carries only the key.`;
        return `The stores source reads store ${ev.key}: ${ev.row.city}, managed by ${ev.row.manager}. The planner treats it as an upsert (+U) for store ${ev.key}.`;
      }
      if (node === 'agg') {
        if (ev.type === 'wm') return 'The watermark passes through GroupAggregate, which has no use for it.';
        if (ev.type === 'first') return `Store ${ev.key}'s first order. GroupAggregate starts its totals, ${totals(ev.next)}, and emits an insert.`;
        return `Store ${ev.key}'s totals become ${totals(ev.next)}. The join only needs the latest value for each store, so GroupAggregate sends +U, with no −U.`;
      }
      if (node === 'join') {
        const k = ev.key;
        if (ev.type === 'hold') return `The join records the orders watermark, ${ev.time}. Its own watermark is the minimum of both inputs, and stores never sends one, so the join's watermark never advances, and nothing is forwarded to the sink.`;
        if (ev.type === 'delete') {
          return ev.other
            ? `Store ${k}'s row is removed from the join's state, and the join deletes store ${k}'s leaderboard row (−D). Its totals stay in state, in case the store reopens.`
            : `Store ${k}'s row is removed from the join's state. Nothing was joined with it, so nothing is emitted.`;
        }
        if (ev.side === 'left') {
          if (!ev.other) return `Store ${k}'s totals are stored. The join has no store row for ${k}, so nothing is emitted: it's an inner join.`;
          return `Store ${k}'s totals ${ev.prev ? 'replace the old ones' : 'are stored'}, and are joined with store ${k}'s row: ${ev.other.city}, ${ev.other.manager}. The joined row goes out as ${ev.kind}, the row kind that came in.`;
        }
        if (ev.prev && ev.other && ev.prev.manager !== ev.row.manager) {
          return `Store ${k}'s manager changes from ${ev.prev.manager} to ${ev.row.manager}. The join replaces the store row, and re-emits store ${k}'s leaderboard row with the new manager and the current totals, ${totals(ev.other)}. Updating a result that was already emitted is what only a regular join can do.`;
        }
        if (!ev.other) return `Store ${k}'s row (${ev.row.city}, ${ev.row.manager}) is stored. There are no totals for store ${k} yet, so nothing is emitted: it's an inner join.`;
        return `Store ${k}'s row is stored, and joined with its totals, ${totals(ev.other)}.`;
      }
      if (node === 'sink') {
        if (ev.type === 'wm') return 'Watermarks aren\'t written to Kafka.';
        if (ev.type === 'tombstone') return `The sink writes a tombstone for store ${ev.key}, so compaction removes it from the leaderboard.`;
        return `The sink writes store ${ev.key}'s row: ${ev.row.manager}, ${totals(ev.row)}. After compaction, only the latest row for store ${ev.key} is left.`;
      }
      return null;
    },
    explain: `
Sink(table=[store_leaderboard], fields=[store_id, city, manager, orders, sales], changelogMode=[NONE])
+- Calc(select=[store_id0 AS store_id, city, manager, orders, sales], changelogMode=[I,UA,D])
   +- Join(joinType=[InnerJoin], where=[=(store_id, store_id0)], select=[...],
        leftInputSpec=[JoinKeyContainsUniqueKey], rightInputSpec=[JoinKeyContainsUniqueKey],
        changelogMode=[I,UA,D])
      :- Exchange(distribution=[hash[store_id]], changelogMode=[I,UA])
      :  +- GroupAggregate(groupBy=[store_id], select=[store_id, COUNT(*) AS orders,
      :       SUM(total) AS sales], changelogMode=[I,UA])
      :     +- Exchange(distribution=[hash[store_id]], changelogMode=[I])
      :        +- Calc(select=[store_id, total], changelogMode=[I])
      :           +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])
      +- Exchange(distribution=[hash[store_id]], changelogMode=[I,UA,D])
         +- TableSourceScan(table=[[stores]], fields=[store_id, city, manager], changelogMode=[UA,D])`,
    details: `
<p>A regular join keeps both of its inputs in state, and re-emits results when either side
changes. That's exactly what this leaderboard needs: when store 7 gets a new manager, its row,
which was emitted long ago, has to change. None of the bounded joins can do that. A temporal join
uses the version that was valid at each row's time, and a lookup join or a LATERAL SNAPSHOT join
uses the version that's current when the row arrives. None of them revisits a result.</p>
<p>The cost of a regular join is its state, so what matters is what each side keeps. Here both
inputs are tables keyed by <code>store_id</code>, the join key. The plan calls this
<code>JoinKeyContainsUniqueKey</code> on both sides: the join keeps one row per store on each side,
and the number of stores bounds the state. A regular join is fine when both inputs are keyed tables
with a bounded set of keys. It's an unbounded event stream on either side that makes it grow
forever.</p>
<p>Two things Flink 2.3 does here that you might not expect:</p>
<ul>
  <li>There's no ChangelogNormalize on the stores side. The join already keeps the latest row for
  each key, so it takes the upsert stream from <code>upsert-kafka</code> directly.</li>
  <li>The join's output is an upsert stream (<code>[I, UA, D]</code>), not retractions. Its upsert
  key, <code>store_id</code>, matches the sink's primary key, so there's no SinkUpsertMaterializer
  either.</li>
</ul>
<p>Write the same query the other way round, joining first and aggregating after, and it looks
just as harmless:</p>
<pre class="code plan">SELECT s.store_id, s.city, s.manager, COUNT(*) AS orders, SUM(o.total) AS sales
FROM orders AS o
JOIN stores AS s ON o.store_id = s.store_id
GROUP BY s.store_id, s.city, s.manager;</pre>
<p>Now the orders side has no unique key (<code>NoUniqueKey</code> in the plan), so the join keeps
every order ever placed. The planner adds a ChangelogNormalize for stores, and the join emits
retractions: when store 7's manager changes, it retracts and re-emits every order store 7 has ever
had, and the aggregate has to undo and redo all of them. If you want each order's manager at the
time of the order, that's a temporal join.</p>
<p>If you set a state TTL, remember that it applies to both sides: once a store's row expires, a
new order for that store no longer finds it, and the inner join drops the store from the
leaderboard until its row is written again.</p>`,
  };


  // ---------- The contrasting case: join every order first, then aggregate ----------

  const groupRow = (r) => `${r.manager}: ${plural(r.orders, 'order')}, ${U.money(r.sales)}`;
  const groupCell = (r) => `${r.manager} ${r.orders} ${U.money(r.sales)}`;
  const joinFirst = {
    id: 'join-first',
    label: 'Join, then aggregate (avoid)',
    question: 'The same leaderboard, written the other way round',
    blurb: 'This query gives the same final answer, but it joins every order with its store, and then aggregates. Watch the join\'s state, and what one manager change does to the leaderboard topic.',
    intro: 'The same inputs as before, up to store 7\'s new manager. Press Play, or step through with the arrow keys.',
    sql: `
INSERT INTO store_leaderboard
SELECT s.store_id, s.city, s.manager,
       COUNT(*) AS orders, SUM(o.total) AS sales
FROM orders AS o
JOIN stores AS s
  ON o.store_id = s.store_id
GROUP BY s.store_id, s.city, s.manager
ON CONFLICT DO DEDUPLICATE;`,
    ddl: leaderboard.ddl,
    matrix: {
      state: 'Every order in the join, the latest store rows, an accumulator per group, and the materializer\'s rows',
      watermarks: 'Not used',
      watermarksOut: 'None: stores has no watermark, so the join never emits one',
    },
    facts: {
      state: 'Four operators keep state. The join keeps <strong>every order ever placed</strong>, because orders have no unique key, so it grows forever. ChangelogNormalize keeps the latest row per store, GroupAggregate an accumulator per store and manager, and the SinkUpsertMaterializer the rows for each store.',
      watermarks: 'Not used. As before, stores has no watermark, so none leaves the join.',
      output: 'Upsert, keyed by <code>store_id</code>, but noisy: every change to a store\'s total reaches the topic as a tombstone, then a new row. One manager change writes six records, two of them with wrong totals.',
    },
    graphLabel: 'Job graph: Kafka source orders, hash partitioned by store_id into Join. Kafka source stores (upsert-kafka) goes through ChangelogNormalize (retract) into the same Join. The Join sends retractions, hash partitioned by store, city and manager, into GroupAggregate, then into SinkUpsertMaterializer, then Kafka sink store_leaderboard (upsert).',
    graph: {
      nodes: [
        {
          id: 'orders', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [4],
          cfg: { topic: 'orders', columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total', ...money }] },
        },
        {
          id: 'stores', kind: 'source', op: 'upsertKafkaSource', title: 'Kafka source', lines: ['stores', 'upsert-kafka'], sqlLines: [5],
          cfg: { topic: 'stores', key: 'store_id', showKey: true, columns: [{ f: 'city' }, { f: 'manager' }] },
        },
        {
          id: 'cn', kind: 'op', op: 'changelogNormalize', title: 'ChangelogNormalize', lines: ['key: store_id'],
          stateful: true, planner: true, sqlLines: [5],
          cfg: { key: 'store_id', generateUpdateBefore: true, stateFields: ['store_id', 'city', 'manager'], stateTitle: 'Last row per store_id' },
        },
        {
          id: 'join', kind: 'op', op: 'regularJoin', title: 'Join', lines: ['INNER, on store_id'],
          stateful: true, sqlLines: [5, 6],
          cfg: {
            left: 'orders', key: 'store_id', keyLabel: 'store', leftUnique: false, leftName: 'order', rightName: 'store',
            stateTitle: 'Every order, one row per store',
            joinRow: (o, st) => ({ order_id: o.order_id, store_id: o.store_id, city: st.city, manager: st.manager, total: o.total }),
            leftCell: (r) => `#${r.order_id}, ${U.money(r.total)}`,
            rightCell: (r) => `${r.city}, ${r.manager}`,
          },
        },
        {
          id: 'agg', kind: 'op', op: 'groupAggregate', title: 'GroupAggregate', lines: ['GROUP BY store,', 'city, manager'],
          stateful: true, sqlLines: [2, 3, 7],
          cfg: { keys: ['store_id', 'city', 'manager'], keyLabel: 'group', keyCell: (r) => `${r.store_id} ${r.manager}`, generateUpdateBefore: true, aggs: [{ name: 'orders', fn: 'count' }, { name: 'sales', fn: 'sum', field: 'total' }] },
        },
        {
          id: 'sum', kind: 'op', op: 'upsertMaterializer', title: 'SinkUpsertMaterializer', lines: ['pk: store_id'],
          stateful: true, planner: true, sqlLines: [8],
          cfg: { key: 'store_id', keyLabel: 'store', upsertKey: (r) => `${r.store_id}|${r.city}|${r.manager}`, cell: groupCell },
        },
        {
          id: 'sink', kind: 'sink', op: 'upsertKafkaSink', title: 'Kafka sink', lines: ['store_leaderboard', 'upsert-kafka'], sqlLines: [1],
          cfg: { topic: 'store_leaderboard', key: 'store_id', showKey: true, fields: ['city', 'manager', 'orders', 'sales'], columns: [{ f: 'city' }, { f: 'manager' }, { f: 'orders', num: true }, { f: 'sales', ...money }] },
        },
      ],
      edges: [
        { from: 'orders', to: 'join', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' },
        { from: 'stores', to: 'cn', mode: 'upsert', kinds: '+U -D', exchange: 'hash(store_id)' },
        { from: 'cn', to: 'join', mode: 'retract', kinds: '+I -U +U -D', exchange: 'hash(store_id)' },
        { from: 'join', to: 'agg', mode: 'retract', kinds: '+I -U +U -D', exchange: 'hash(group key)' },
        { from: 'agg', to: 'sum', mode: 'retract', kinds: '+I -U +U -D', exchange: 'hash(store_id)' },
        { from: 'sum', to: 'sink', mode: 'upsert', kinds: '+I +U -D' },
      ],
    },
    inputs: [
      store(7, 'Oakland', 'Sam'), store(9, 'Fresno', 'Priya'),
      { at: 'orders', value: { order_id: 101, store_id: 7, total: 18.00 } },
      { at: 'orders', value: { order_id: 102, store_id: 9, total: 24.50 } },
      { at: 'orders', value: { order_id: 103, store_id: 7, total: 31.00 } },
      store(7, 'Oakland', 'Alex'),
    ],
    pill: (r) => {
      if (r.orders !== undefined) return `${r.store_id} ${r.manager} ${r.orders} ${U.money(r.sales)}`;
      if (r.order_id != null && r.manager) return `${r.order_id} ${r.manager}`;
      if (r.order_id != null) return `${r.order_id} s${r.store_id} ${U.money(r.total)}`;
      return r.manager ? `${r.store_id} ${r.manager}` : `${r.store_id}`;
    },
    narrate(node, ev) {
      if (node === 'orders') return `The orders source reads order ${ev.row.order_id}: store ${ev.row.store_id}, ${U.money(ev.row.total)}.`;
      if (node === 'stores') return `The stores source reads store ${ev.key}: ${ev.row.city}, managed by ${ev.row.manager}.`;
      if (node === 'cn') {
        if (ev.type === 'insert') return `ChangelogNormalize stores store ${ev.key}'s row, and emits an insert. The planner added it because the join needs to retract the old row when a store changes.`;
        if (ev.type === 'update') return `Store ${ev.key}'s manager changes from ${ev.prev.manager} to ${ev.row.manager}. ChangelogNormalize emits the pair the join needs: −U for the old row, then +U for the new one.`;
      }
      if (node === 'join') {
        const k = ev.key;
        if (ev.side === 'left') {
          return ev.other
            ? `Order ${ev.row.order_id} is added to the join's state, and joined with store ${k}: ${ev.other.manager}. The join keeps every order, because orders have no unique key. That's ${plural(ev.kept, 'order')} for store ${k} so far, and it never shrinks.`
            : `Order ${ev.row.order_id} is added to the join's state. There's no store row yet.`;
        }
        if (ev.type === 'delete') {
          return `The −U for ${ev.row.manager} reaches the join. It has to retract every order it ever joined with that row: ${plural(ev.others.length, 'retraction')} (−U), one per order of store ${k}.`;
        }
        if (ev.prev === null && !ev.others.length) return `Store ${k}'s row is stored. No orders yet, so nothing is emitted.`;
        return ev.others.length
          ? `The +U for ${ev.row.manager} is joined with every order of store ${k} again: ${plural(ev.others.length, 'new row')} (+U). Each one has to be aggregated all over again.`
          : `Store ${k}'s row is stored.`;
      }
      if (node === 'agg') {
        const r = ev.next;
        if (ev.type === 'first') return `First row for ${r.manager} at store ${r.store_id}. GroupAggregate starts the group, ${plural(r.orders, 'order')}, ${U.money(r.sales)}, and emits an insert.`;
        if (ev.type === 'delete') return `The group for ${ev.prev.manager} at store ${ev.prev.store_id} has no orders left, so GroupAggregate deletes it (−D).`;
        return ev.accumulate
          ? `The group for ${r.manager} becomes ${plural(r.orders, 'order')}, ${U.money(r.sales)}. The materializer needs retractions, so GroupAggregate sends −U for the old total, then +U.`
          : `An order is taken back out of ${r.manager}'s group: it drops to ${plural(r.orders, 'order')}, ${U.money(r.sales)}. GroupAggregate sends −U, then +U.`;
      }
      if (node === 'sum') {
        const k = ev.key;
        if (ev.type === 'emptied') return `The ${U.kindText(ev.kind)} empties the materializer's list for store ${k}, so it emits a delete. The leaderboard will briefly have no row for store ${k}.`;
        if (ev.type === 'add') return `The materializer adds ${groupRow(ev.row)} to the list for store ${k}, and emits it as ${ev.kind}.`;
        if (ev.type === 'fallback') return `The latest row for store ${k} was retracted, so the one before it is emitted again.`;
        if (ev.type === 'removed') return `An older row for store ${k} is removed. Nothing is emitted.`;
      }
      if (node === 'sink') {
        if (ev.type === 'tombstone') return `The sink writes a tombstone for store ${ev.key}. Anyone reading the topic now sees store ${ev.key} disappear.`;
        const r = ev.row;
        let note = '';
        if (r.manager === 'Sam' && r.sales === 31) note = ' That total was never true: it\'s halfway through undoing the old manager\'s orders.';
        else if (r.manager === 'Alex' && r.orders === 1) note = ' That total was never true either: only one of store 7\'s two orders has been redone so far.';
        else if (r.manager === 'Alex' && r.orders === 2) note = ' That\'s the right answer at last. The one manager change wrote six records to this topic, three of them tombstones and two of them wrong. The aggregate-first query wrote one.';
        return `The sink writes store ${ev.key}'s row: ${r.manager}, ${totals(r)}.${note}`;
      }
      return null;
    },
    explain: `
Sink(table=[store_leaderboard], fields=[store_id, city, manager, orders, sales],
     upsertMaterialize=[true], conflictStrategy=[DEDUPLICATE], changelogMode=[NONE])
+- GroupAggregate(groupBy=[store_id, city, manager], select=[store_id, city, manager,
     COUNT_RETRACT(*) AS orders, SUM_RETRACT(total) AS sales], changelogMode=[I,UB,UA,D])
   +- Exchange(distribution=[hash[store_id, city, manager]], changelogMode=[I,UB,UA,D])
      +- Calc(select=[store_id0 AS store_id, city, manager, total], changelogMode=[I,UB,UA,D])
         +- Join(joinType=[InnerJoin], where=[=(store_id, store_id0)], select=[...],
              leftInputSpec=[NoUniqueKey], rightInputSpec=[JoinKeyContainsUniqueKey],
              changelogMode=[I,UB,UA,D])
            :- Exchange(distribution=[hash[store_id]], changelogMode=[I])
            :  +- Calc(select=[store_id, total], changelogMode=[I])
            :     +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])
            +- Exchange(distribution=[hash[store_id]], changelogMode=[I,UB,UA,D])
               +- ChangelogNormalize(key=[store_id], changelogMode=[I,UB,UA,D])
                  +- Exchange(distribution=[hash[store_id]], changelogMode=[UA,D])
                     +- TableSourceScan(table=[[stores]], fields=[...], changelogMode=[UA,D])

upsertMaterialize=[true] is the SinkUpsertMaterializer. The sink shuffles by its primary key
(store_id) in front of it.`,
    details: `
<p>Both queries end with the same leaderboard. What differs is everything in between.</p>
<ul>
  <li><strong>The join keeps every order.</strong> Orders have no unique key
  (<code>NoUniqueKey</code> in the plan), so the join has to keep each one, in case its store row
  changes. Its state grows with every order the chain ever takes, and never shrinks. In the
  aggregate-first query, the join keeps one row of totals per store.</li>
  <li><strong>One change upstream fans out.</strong> When store 7's manager changes, the join
  retracts every order it ever joined with the old row, and joins every one of them again with
  the new row. GroupAggregate then undoes and redoes each order. Two orders here; years of orders
  for a real store.</li>
  <li><strong>The planner adds two operators.</strong> The join needs to retract the old store
  row, so a ChangelogNormalize appears on the stores side. And because the result is grouped by
  store, city <em>and</em> manager, its upsert key doesn't match the sink's primary key. Flink 2.3
  rejects that unless you add <code>ON CONFLICT</code>, and <code>DO DEDUPLICATE</code> adds a
  SinkUpsertMaterializer, with even more state.</li>
  <li><strong>The output topic flickers.</strong> The materializer turns each −U that empties its
  list into a delete. So every change to a store's total is written as a tombstone and then a new
  row, and the manager change writes six records, two of them with totals that were never true.
  The aggregate-first query writes one.</li>
</ul>
<p>Aggregate before you join, so the join works on small, keyed tables. If what you want is each
order's manager <em>at the time of the order</em>, that's a temporal join, not a regular one.</p>`,
  };

  window.PATTERNS.push({
    id: 'join',
    nav: 'Regular join',
    title: 'Regular join',
    summary: '<p>A regular join is only safe when both sides are upsert tables with a bounded number of keys. The second tab shows what happens when joining with an append-only stream instead: every event stays in state, forever.</p>',
    variants: [leaderboard, joinFirst],
  });
})();
