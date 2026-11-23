// Pattern: window join, a FULL OUTER join of two window aggregates.
// Verified against verification/window-join.sql (see window-join.plan.txt).
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const money = { num: true, fmt: U.money };
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;
  const list = (xs) => (xs.length < 2 ? xs.join('') : `${xs.slice(0, -1).join(', ')} and ${xs[xs.length - 1]}`);
  const order = (order_id, store_id, total, event_time) => ({ value: { order_id, store_id, total, event_time } });
  const ticket = (order_id, store_id, prep_minutes, event_time) => ({ at: 'ksrc', value: { order_id, store_id, prep_minutes, event_time } });
  const WM = { wm: true };
  const KWM = { wm: true, at: 'ksrc' };
  const count = (n) => (n == null ? 'null' : String(n));
  const result = (r) => `${r.placed == null ? 'no orders placed' : `${plural(r.placed, 'order')} placed`}, ${r.cooked == null ? 'none cooked' : `${r.cooked} cooked`}`;

  // Each side counts its rows per store and window, like the tumbling window pattern.
  const windowCfg = (countName, what) => ({ kind: 'tumble', size: 900, key: 'store_id', keyLabel: 'store', timeField: 'event_time', countName, what });
  const ordersCfg = windowCfg('placed');
  const kitchenCfg = windowCfg('cooked', (r) => `The ticket for order ${r.order_id}`);
  const windowRow = (r) => ({ store_id: r.store_id, window_start: r.window_start, window_end: r.window_end });

  const fullOuter = {
    id: 'fullouter',
    label: 'Full outer join',
    question: 'Was each store\'s kitchen keeping up with its orders? (Reported every 15 minutes.)',
    intro: 'Two topics: orders, and kitchen_tickets, written when a pizza comes out of the oven. Each is counted per store in 15-minute windows, and the counts are joined. Press Play, or step through with the arrow keys.',
    sql: `
INSERT INTO kitchen_load
SELECT COALESCE(o.store_id, k.store_id) AS store_id,
       COALESCE(o.window_start, k.window_start) AS window_start,
       COALESCE(o.window_end, k.window_end) AS window_end,
       o.placed, k.cooked
FROM (
  SELECT store_id, window_start, window_end, COUNT(*) AS placed
  FROM TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '15' MINUTES)
  GROUP BY store_id, window_start, window_end) o
FULL OUTER JOIN (
  SELECT store_id, window_start, window_end, COUNT(*) AS cooked
  FROM TUMBLE(TABLE kitchen_tickets, DESCRIPTOR(event_time), INTERVAL '15' MINUTES)
  GROUP BY store_id, window_start, window_end) k
ON o.store_id = k.store_id
AND o.window_start = k.window_start AND o.window_end = k.window_end;`,
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

CREATE TABLE kitchen_tickets (
  order_id      BIGINT,
  store_id      INT,
  prep_minutes  INT,
  event_time    TIMESTAMP(3),  -- when the pizza came out of the oven
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ('connector' = 'kafka', 'topic' = 'kitchen_tickets');

CREATE TABLE kitchen_load (
  store_id      INT,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  placed        BIGINT,
  cooked        BIGINT
) WITH ('connector' = 'kafka', 'topic' = 'kitchen_load');

-- Connection and format options are left out.`,
    matrix: { state: 'Rows per key and window, on each side, freed when the window fires', memory: 'Each LocalWindowAggregate keeps partial results in memory, not state, until a watermark or checkpoint sends them on', watermarks: 'Trigger results; the slower input holds back both', watermarksOut: 'The minimum of both inputs, after the windows it fires' },
    facts: {
      state: 'Each GlobalWindowAggregate keeps a count per store and window; the LocalWindowAggregates keep theirs in memory. The join keeps each side\'s counts per store and window until the window fires, then joins them and frees them. Nothing grows forever, so no state TTL is needed.',
      watermarks: 'They fire the windows. The join\'s watermark is the minimum of its two inputs, so a window is only joined once both sides have finished it, and a slow topic holds everything back.',
      output: 'Append-only. Each store and window comes out once, even when one side has no rows and its count is null.',
    },
    graphLabel: 'Job graph: Kafka sources orders and kitchen_tickets, each into a tumbling LocalWindowAggregate, hash partitioned by store_id into a GlobalWindowAggregate, both hash partitioned by store_id into a WindowJoin (full outer), then Kafka sink kitchen_load. Every edge is append-only.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [8],
          cfg: {
            topic: 'orders', delay: 1, timeField: 'event_time',
            columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total', ...money }, { f: 'event_time' }],
          },
        },
        {
          id: 'ksrc', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['kitchen_tickets'], sqlLines: [12],
          cfg: {
            topic: 'kitchen_tickets', delay: 1, timeField: 'event_time',
            columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'prep_minutes', num: true }, { f: 'event_time' }],
          },
        },
        {
          id: 'localO', kind: 'op', op: 'localWindowAggregate', title: 'LocalWindowAggregate', lines: ['TUMBLE 15 min', 'COUNT placed'],
          panel: true, sqlLines: [7, 8], cfg: ordersCfg,
        },
        {
          id: 'localK', kind: 'op', op: 'localWindowAggregate', title: 'LocalWindowAggregate', lines: ['TUMBLE 15 min', 'COUNT cooked'],
          panel: true, sqlLines: [11, 12], cfg: kitchenCfg,
        },
        {
          id: 'aggO', kind: 'op', op: 'globalWindowAggregate', title: 'GlobalWindowAggregate', lines: ['TUMBLE 15 min', 'merge, fire'],
          stateful: true, sqlLines: [7, 9], cfg: ordersCfg,
        },
        {
          id: 'aggK', kind: 'op', op: 'globalWindowAggregate', title: 'GlobalWindowAggregate', lines: ['TUMBLE 15 min', 'merge, fire'],
          stateful: true, sqlLines: [11, 13], cfg: kitchenCfg,
        },
        {
          id: 'join', kind: 'op', op: 'windowJoin', title: 'WindowJoin', lines: ['FULL OUTER', 'store_id, window'],
          stateful: true, sqlLines: [2, 3, 4, 5, 6, 10, 14, 15],
          cfg: {
            left: 'aggO', key: 'store_id', leftName: 'orders', rightName: 'kitchen', leftTopic: 'orders', rightTopic: 'kitchen_tickets',
            leftValue: (r) => `placed ${r.placed}`, rightValue: (r) => `cooked ${r.cooked}`,
            joinRow: (l, r) => ({ ...windowRow(l), placed: l.placed, cooked: r.cooked }),
            padLeft: (l) => ({ ...windowRow(l), placed: l.placed, cooked: null }),
            padRight: (r) => ({ ...windowRow(r), placed: null, cooked: r.cooked }),
          },
        },
        {
          id: 'sink', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: ['kitchen_load'], sqlLines: [1],
          cfg: {
            topic: 'kitchen_load',
            columns: [{ f: 'store_id', num: true }, { f: 'window_start' }, { f: 'window_end' },
              { f: 'placed', num: true, fmt: count }, { f: 'cooked', num: true, fmt: count }],
          },
        },
      ],
      edges: [
        { from: 'src', to: 'localO', mode: 'append', kinds: '+I' },
        { from: 'ksrc', to: 'localK', mode: 'append', kinds: '+I' },
        { from: 'localO', to: 'aggO', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' },
        { from: 'localK', to: 'aggK', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' },
        { from: 'aggO', to: 'join', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' },
        { from: 'aggK', to: 'join', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' },
        { from: 'join', to: 'sink', mode: 'append', kinds: '+I' },
      ],
    },
    // Window 12:00-12:15: store 7 places 3 and cooks 1; store 9 places 1 and cooks none, so its
    // cooked count is null. Window 12:15-12:30: store 7 places and cooks 1; store 9 places none
    // but cooks the order it placed earlier. The kitchen's watermark lags the orders', so each
    // window waits for it.
    inputs: [
      order(101, 7, 18.00, '12:02:10'), order(102, 9, 24.50, '12:04:30'), ticket(101, 7, 8, '12:10:15'),
      order(103, 7, 31.00, '12:09:40'), order(104, 7, 13.00, '12:13:20'), ticket(102, 9, 12, '12:16:40'),
      order(105, 7, 22.00, '12:17:05'), WM,
      ticket(103, 7, 7, '12:16:50'), KWM,
      order(106, 9, 19.00, '12:31:10'), WM,
      ticket(104, 7, 9, '12:31:30'), KWM,
    ],
    pill: (r) => {
      if (r.slice_end) return `s${r.store_id} ≤${r.slice_end.slice(0, 5)} ${r.placed != null ? `placed ${r.placed}` : `cooked ${r.cooked}`}`;
      if ('placed' in r && 'cooked' in r) return `s${r.store_id} ${r.window_start} ${count(r.placed)}/${count(r.cooked)}`;
      if (r.placed != null) return `s${r.store_id} ${r.window_start} placed ${r.placed}`;
      if (r.cooked != null) return `s${r.store_id} ${r.window_start} cooked ${r.cooked}`;
      return r.prep_minutes != null ? `${r.order_id} out ${r.event_time}` : `${r.order_id} s${r.store_id} ${r.event_time}`;
    },
    narrate(node, ev) {
      if ((node === 'src' || node === 'ksrc') && ev.type === 'read') {
        const r = ev.row;
        return node === 'src'
          ? `The orders source reads order ${r.order_id}: store ${r.store_id}, placed at ${r.event_time}.`
          : `The kitchen source reads the ticket for order ${r.order_id}: store ${r.store_id}, out of the oven at ${r.event_time}, after ${r.prep_minutes} minutes.`;
      }
      if ((node === 'src' || node === 'ksrc') && ev.type === 'wm') {
        return `The ${node === 'src' ? 'orders' : 'kitchen_tickets'} source emits a watermark for ${ev.time}: the newest event time it has read, minus 1 second.`;
      }
      if ((node === 'aggO' || node === 'aggK') && ev.type === 'fire') {
        const cfg = node === 'aggO' ? ordersCfg : kitchenCfg;
        const what = cfg.countName;
        const rows = ev.rows.map((x) => `store ${x.store_id}, ${x[what]} ${what}`).join('; ');
        const text = Ops.globalWindowAggregate.mergeText(ev, cfg);
        return `The watermark, ${ev.time}, arrives. ${text ? `${text} ` : ''}Then the ${ev.firings[0].start}–${ev.firings[0].end} window is complete: GlobalWindowAggregate emits a count per store (${rows}), then forwards the watermark.`;
      }
      if (node === 'join') {
        const side = ev.side === 'left' ? 'orders' : 'kitchen';
        switch (ev.type) {
          case 'buffer': return `Store ${ev.key}'s ${side === 'orders' ? 'placed' : 'cooked'} count for ${ev.win} waits in the join's state${ev.other ? ', next to the other side\'s' : ''}. It can't be joined until the combined watermark passes the window's end, when both sides are complete.`;
          case 'late': return `Store ${ev.key}'s count for ${ev.win} arrives after that window has been joined, so it's late, and dropped.`;
          case 'hold': return ev.W
            ? `The join records the ${side} watermark, ${ev.time}. The other input is still at ${ev.side === 'left' ? ev.wms.right : ev.wms.left}, so the combined watermark stays at ${ev.W}, and nothing is joined.`
            : `The join records the ${side} watermark, ${ev.time}. The ${ev.side === 'left' ? 'kitchen' : 'orders'} input hasn't sent one yet, so there's no combined watermark, and nothing can be joined. The slower input holds back both.`;
          case 'advance': return `The combined watermark moves to ${ev.W}, but no window has ended for both sides yet. WindowJoin forwards it.`;
          case 'fire': {
            const parts = ev.firings.map((f) => `store ${f.k}, ${result(f.rows[0])}${!f.left || !f.right ? ` (only the ${f.left ? 'orders' : 'kitchen'} side had rows, so the other count is null)` : ''}`);
            const wins = [...new Set(ev.firings.map((f) => `the window ending ${f.end}`))];
            return `The combined watermark moves to ${ev.W}, the lower of the two inputs, which passes ${list(wins)}. WindowJoin joins each store's counts: ${parts.join('; ')}. It frees that window's state, then forwards the watermark.`;
          }
        }
      }
      if (node === 'sink' && ev.type === 'append') {
        const r = ev.row;
        return `The sink appends store ${r.store_id}'s row for ${r.window_start}–${r.window_end}: ${result(r)}. It's final.`;
      }
      if (node === 'sink' && ev.type === 'wm') return `The watermark reaches the sink, which doesn't write it to Kafka.`;
      return null;
    },
    explain: `
Sink(table=[kitchen_load], fields=[store_id, window_start, window_end, placed, cooked], changelogMode=[NONE])
+- Calc(select=[COALESCE(store_id, store_id0) AS store_id, ..., placed, cooked], changelogMode=[I])
   +- WindowJoin(leftWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[15 min])],
        rightWindow=[TUMBLE(win_start=[window_start], win_end=[window_end], size=[15 min])],
        joinType=[FullOuterJoin], where=[=(store_id, store_id0)], select=[...], changelogMode=[I])
      :- Exchange(distribution=[hash[store_id]], changelogMode=[I])
      :  +- Calc(select=[store_id, window_start, window_end, placed], changelogMode=[I])
      :     +- GlobalWindowAggregate(groupBy=[store_id], window=[TUMBLE(slice_end=[$slice_end], size=[15 min])],
      :          select=[store_id, COUNT(count1$0) AS placed, start('w$) AS window_start,
      :                  end('w$) AS window_end], changelogMode=[I])
      :        +- Exchange(distribution=[hash[store_id]], changelogMode=[I])
      :           +- LocalWindowAggregate(groupBy=[store_id], window=[TUMBLE(time_col=[event_time], size=[15 min])],
      :                select=[store_id, COUNT(*) AS count1$0, slice_end('w$) AS $slice_end], changelogMode=[I])
      :              +- Calc(select=[store_id, event_time], changelogMode=[I])
      :                 +- TableSourceScan(table=[[orders, watermark=[...]]], fields=[...], changelogMode=[I])
      +- Exchange(distribution=[hash[store_id]], changelogMode=[I])
         +- Calc(select=[store_id, window_start, window_end, cooked], changelogMode=[I])
            +- GlobalWindowAggregate(... COUNT(count1$0) AS cooked ...), changelogMode=[I])
               +- Exchange(distribution=[hash[store_id]], changelogMode=[I])
                  +- LocalWindowAggregate(... COUNT(*) AS count1$0 ...), changelogMode=[I])
                     +- Calc(select=[store_id, event_time], changelogMode=[I])
                        +- TableSourceScan(table=[[kitchen_tickets, watermark=[...]]], fields=[...], changelogMode=[I])`,
    details: `
<p>A window join joins two inputs that are windowed the same way: same kind of window, same size.
The join condition has to include <code>window_start</code> and <code>window_end</code>, so rows
only meet rows from the same window. Here each side is a window aggregate, and the join puts each
store's two counts side by side.</p>
<p>Rows wait in the join's state until their window is complete on both sides. The join's
watermark is the lower of its two inputs, so it only joins a window once neither input can still
change it. The kitchen topic lags the orders here, and each window waits for it: a slow or idle
input holds back every result.</p>
<p>When the watermark passes a window's end, WindowJoin joins its rows, emits them, and frees the
window's state. A store that only appears on one side still comes out, with a null count for the
other side, as store 9 does twice. Because the join only emits a window once it's complete, that
null is final. A regular outer join would emit it straight away, and retract it if a match
turned up later.</p>
<p>That's the difference from the regular join recipe: no updates, and no state that grows
forever. The price is that results only come out when a window closes.</p>
<p>A window join can also join raw events, such as each order with its kitchen ticket, by
<code>order_id</code>. But the two only meet if they fall in the same window. Order 103 was placed
at 12:09:40 and came out of the oven at 12:16:50, so they'd be in different windows, and never
match. To pair up related events, use an interval join, like the event correlation recipe.</p>
<p>The graph leaves out the Calcs that only drop columns, and the one that applies
<code>COALESCE</code>.
These semantics come from <code>WindowJoinOperator</code> in Flink 2.3.</p>`,
  };

  window.PATTERNS.push({
    id: 'windowjoin',
    nav: 'Window join',
    title: 'Window join',
    summary: '<p>Join two streams that are windowed the same way, such as two counts per store every 15 minutes. Each window is joined once both inputs have finished it, so the output is append-only, even for an outer join, and the state is freed when the window fires.</p>',
    variants: [fullOuter],
  });
})();
