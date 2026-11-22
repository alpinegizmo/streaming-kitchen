// Pattern: windows (tumble, hop, cumulate, session).
// Verified against verification/windows.sql (see windows.plan.txt).
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const money = { num: true, fmt: U.money };
  const plural = (n, word) => `${n} ${word}${n === 1 ? '' : 's'}`;
  const result = (r) => `${plural(r.orders, 'order')}, ${U.money(r.sales)}`;
  const list = (xs) => (xs.length < 2 ? xs.join('') : `${xs.slice(0, -1).join(', ')} and ${xs[xs.length - 1]}`);

  const order = (order_id, store_id, total, event_time) => ({ value: { order_id, store_id, total, event_time } });
  const WM = { wm: true };

  const ddl = `
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

-- Connection and format options are left out.`;

  // The window TVF call, one argument per line. The first line after FROM is the table.
  const sql = (tvf) => `
INSERT INTO store_sales
SELECT window_start, window_end, store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM ${tvf.join('\n')}
GROUP BY window_start, window_end, store_id;`;

  const explainTail = `
            +- Calc(select=[store_id, total, event_time], changelogMode=[I])
               +- TableSourceScan(table=[[orders, watermark=[-(event_time, 1000:INTERVAL SECOND)],
                    watermarkEmitStrategy=[on-periodic]]], fields=[...], changelogMode=[I])`;

  const twoPhase = (global, local) => `
Sink(table=[store_sales], fields=[window_start, window_end, store_id, orders, sales], changelogMode=[NONE])
+- Calc(select=[window_start, window_end, store_id, orders, sales], changelogMode=[I])
   +- GlobalWindowAggregate(groupBy=[store_id], window=[${global}],
        select=[store_id, COUNT(count1$0) AS orders, SUM(sum$1) AS sales,
                start('w$) AS window_start, end('w$) AS window_end], changelogMode=[I])
      +- Exchange(distribution=[hash[store_id]], changelogMode=[I])
         +- LocalWindowAggregate(groupBy=[store_id], window=[${local}],
              select=[store_id, COUNT(*) AS count1$0, SUM(total) AS sum$1, slice_end('w$) AS $slice_end],
              changelogMode=[I])${explainTail}`;

  // A partial result from LocalWindowAggregate: store, the end of its slice, and its total.
  const pill = (r) => {
    if (r.window_start) return `s${r.store_id} ${r.window_start} ${U.money(r.sales)}`;
    if (r.slice_end) return `s${r.store_id} ≤${r.slice_end.slice(0, 5)} ${r.sales < 0 ? `−${U.money(-r.sales)}` : U.money(r.sales)}`;
    return r.status === 'cancelled' ? `${r.order_id} cancelled` : `${r.order_id} s${r.store_id} ${r.event_time}`;
  };
  // What GlobalWindowAggregate merges into state, before it fires, as the start of a caption.
  const merged = (ev, cfg) => {
    const text = Ops.globalWindowAggregate.mergeText(ev, { ...cfg, key: 'store_id', keyLabel: 'store', sumField: 'total' });
    return text ? `${text} ` : '';
  };

  // Builds one window variant. `fire` narrates the windows that a watermark fires.
  function variant(v) {
    const lines = v.tvf.length;
    return {
      id: v.id,
      label: v.label,
      question: v.question,
      blurb: v.blurb,
      intro: v.intro,
      sql: sql(v.tvf),
      ddl,
      facts: v.facts,
      matrix: v.matrix,
      graphLabel: v.op === 'sessionWindowAggregate'
        ? `Job graph: Kafka source orders, hash partitioned by store_id into WindowAggregate (${v.label.toLowerCase()}), then Kafka sink store_sales. Both edges are append-only.`
        : `Job graph: Kafka source orders into LocalWindowAggregate (${v.label.toLowerCase()}), hash partitioned by store_id into GlobalWindowAggregate, then Kafka sink store_sales. Every edge is append-only.`,
      graph: {
        nodes: [
          {
            id: 'src', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['orders'], sqlLines: [6],
            cfg: {
              topic: 'orders', delay: 1, timeField: 'event_time',
              columns: [{ f: 'order_id' }, { f: 'store_id', num: true }, { f: 'total', ...money }, { f: 'event_time' }],
            },
          },
          // Session windows are planned as one WindowAggregate; the others in two phases.
          ...(v.op === 'sessionWindowAggregate' ? [
            {
              id: 'win', kind: 'op', op: v.op, title: 'WindowAggregate', lines: [v.nodeLine],
              stateful: true, sqlLines: [2, 3, 4, 5, ...Array.from({ length: lines - 2 }, (_, i) => 7 + i), 5 + lines],
              cfg: { ...v.cfg, key: 'store_id', keyLabel: 'store', timeField: 'event_time', sumField: 'total' },
            },
          ] : [
            {
              id: 'local', kind: 'op', op: 'localWindowAggregate', title: 'LocalWindowAggregate', lines: [v.nodeLine, 'pre-aggregate'],
              panel: true, sqlLines: [3, 4, 5, ...Array.from({ length: lines - 2 }, (_, i) => 7 + i)],
              cfg: { ...v.cfg, key: 'store_id', keyLabel: 'store', timeField: 'event_time', sumField: 'total' },
            },
            {
              id: 'win', kind: 'op', op: 'globalWindowAggregate', title: 'GlobalWindowAggregate', lines: [v.nodeLine, 'merge, fire'],
              stateful: true, sqlLines: [2, 3, 4, 5 + lines],
              cfg: { ...v.cfg, key: 'store_id', keyLabel: 'store', timeField: 'event_time', sumField: 'total' },
            },
          ]),
          {
            id: 'sink', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: ['store_sales'], sqlLines: [1],
            cfg: {
              topic: 'store_sales',
              columns: [{ f: 'window_start' }, { f: 'window_end' }, { f: 'store_id', num: true }, { f: 'orders', num: true }, { f: 'sales', ...money }],
            },
          },
        ],
        edges: [
          ...(v.op === 'sessionWindowAggregate'
            ? [{ from: 'src', to: 'win', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' }]
            : [{ from: 'src', to: 'local', mode: 'append', kinds: '+I' }, { from: 'local', to: 'win', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' }]),
          { from: 'win', to: 'sink', mode: 'append', kinds: '+I' },
        ],
      },
      inputs: v.inputs,
      pill,
      narrate(node, ev) {
        if (node === 'src' && ev.type === 'read') {
          const r = ev.row;
          return `The source reads order ${r.order_id}: store ${r.store_id}, ${U.money(r.total)}, placed at ${r.event_time}.`;
        }
        if (node === 'src' && ev.type === 'wm') {
          return `The source emits a watermark for ${ev.time}: the newest event time it has read, minus 1 second.`;
        }
        // LocalWindowAggregate's own captions describe the partial results; the watermark that
        // fires windows says what it merged first, then what fires.
        if (node === 'local' && v.local) {
          const text = v.local(ev);
          if (text) return text;
        }
        if (node === 'win' && v.op === 'sessionWindowAggregate') {
          if (ev.type === 'wm') return `The watermark, ${ev.time}, hasn't reached any timer, so nothing fires. WindowAggregate forwards it.`;
          return ev.type === 'fire' ? v.fire(ev) : v.record(ev);
        }
        if (node === 'win' && ev.type === 'fire') return `The watermark, ${ev.time}, arrives. ${merged(ev, v.cfg)}${v.fire(ev)}`;
        if (node === 'sink' && ev.type === 'append') {
          const r = ev.row;
          return `The sink appends store ${r.store_id}'s result for ${r.window_start}–${r.window_end}: ${result(r)}. It's final, and will never be updated.`;
        }
        if (node === 'sink' && ev.type === 'wm') return `The watermark reaches the sink, which doesn't write it to Kafka.`;
        return null;
      },
      explain: v.explain,
      details: v.details,
    };
  }

  const sharedFacts = {
    watermarks: 'They drive the job. A window fires when the watermark passes its end. An order is late, and dropped, only once every window it belongs to has fired.',
  };

  // ---------- Tumble ----------

  const tumble = variant({
    id: 'tumble',
    label: 'Tumble',
    question: 'How much did each store sell, every 5 minutes?',
    intro: 'Stores 7 and 9 send orders. Windows are 5 minutes long, and the watermark trails the newest order by 1 second. Press Play, or step through with the arrow keys.',
    tvf: ['TUMBLE(', '  TABLE orders,', '  DESCRIPTOR(event_time),', "  INTERVAL '5' MINUTES)"],
    nodeLine: 'TUMBLE 5 min',
    cfg: { kind: 'tumble', size: 300 },
    inputs: [
      order(101, 7, 18.00, '12:01:10'), order(102, 9, 24.50, '12:02:30'), order(103, 7, 31.00, '12:03:45'), WM,
      order(104, 7, 13.00, '12:04:20'), order(106, 7, 27.50, '12:05:01'), WM,
      order(105, 7, 16.00, '12:04:58'), order(107, 9, 19.00, '12:07:30'), order(108, 7, 21.00, '12:09:10'), WM,
      order(109, 9, 30.00, '12:10:20'), WM,
    ],
    fire(ev) {
      const wins = [...new Set(ev.firings.map((f) => `${f.start}–${f.end}`))];
      const rows = ev.rows.map((x) => `store ${x.store_id}, ${result(x)}`).join('; ');
      return `Then the ${list(wins)} window is complete: GlobalWindowAggregate emits a final row for each store (${rows}), frees their state, and forwards the watermark.`;
    },
    matrix: { state: 'An accumulator per key and window, freed when the window fires', memory: 'LocalWindowAggregate keeps partial results in memory, not state, until a watermark or checkpoint sends them on', watermarks: 'Triggers results and frees state', watermarksOut: 'Passed through, after the windows it fires' },
    facts: {
      state: 'GlobalWindowAggregate keeps one accumulator per store and window (an order count and a running sum), and a timer at the end of each window. A window\'s state is freed as soon as it fires. LocalWindowAggregate\'s partial results are in memory, not in state.',
      ...sharedFacts,
      output: 'Append-only. Each window is emitted once, when it\'s final, so a plain Kafka topic can take it.',
    },
    explain: twoPhase('TUMBLE(slice_end=[$slice_end], size=[5 min])', 'TUMBLE(time_col=[event_time], size=[5 min])'),
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
before the shuffle, so only one partial result per store and window crosses the network. It keeps
them in memory, not in Flink state, and sends them all on when the watermark reaches the end of a
window, or at a checkpoint. GlobalWindowAggregate, after the hash exchange on
<code>store_id</code>, merges them into keyed state, decides what's late, and fires the timers. So
order 105, which is late, isn't dropped when it arrives: LocalWindowAggregate adds it to a partial
result, and GlobalWindowAggregate drops that partial result when it reaches it, a watermark later.
The graph leaves out two Calcs that only drop columns.</p>
<p>Under the hood, the state is kept per <em>slice</em>. For a tumbling window, a slice is the whole
window. Hopping and cumulating windows share slices between overlapping windows.</p>
<p>If late orders matter, give the watermark a longer delay, at the cost of later results, or
send them to a topic of their own, as the next recipe does.</p>`,
  });

  // ---------- Hop ----------

  const hop = variant({
    id: 'hop',
    label: 'Hop',
    question: 'How much did each store sell in the last 15 minutes, updated every 5?',
    blurb: 'A hopping window is 15 minutes long and starts every 5 minutes, so windows overlap and each order belongs to three of them. To keep one example readable, only store 7 sends orders here.',
    intro: 'Store 7 sends orders. Windows are 15 minutes long and start every 5 minutes. Flink keeps state per 5-minute slice, and builds each window from three slices. Press Play, or step through with the arrow keys.',
    tvf: ['HOP(', '  TABLE orders,', '  DESCRIPTOR(event_time),', "  INTERVAL '5' MINUTES,", "  INTERVAL '15' MINUTES)"],
    nodeLine: 'HOP 15 min, every 5',
    cfg: { kind: 'hop', size: 900, slide: 300 },
    inputs: [
      order(101, 7, 18.00, '12:01:10'), order(103, 7, 31.00, '12:03:45'), WM,
      order(106, 7, 27.50, '12:06:20'), WM,
      order(108, 7, 21.00, '12:09:10'), order(110, 7, 14.00, '12:12:30'), WM,
      order(111, 7, 25.00, '12:16:40'), WM,
      order(112, 7, 9.00, '12:21:05'), WM,
    ],
    local(ev) {
      if (ev.type !== 'new') return null;
      const end = U.toSec(`${ev.slice.slice(-5)}:00`);
      const wins = [0, 1, 2].map((i) => `${U.fromSec(end - 900 + i * 300).slice(0, 5)}–${U.fromSec(end + i * 300).slice(0, 5)}`);
      return `Order ${ev.row.order_id} belongs to three windows (${list(wins)}), but it's counted once, in store ${ev.key}'s partial result for the ${ev.slice} slice: ${result({ orders: ev.acc.count, sales: ev.acc.sum })}. LocalWindowAggregate keeps it in memory, not in Flink state.`;
    },
    fire(ev) {
      return ev.firings.map((f) => {
        const parts = [`The watermark passes ${f.end}, the end of the ${f.start}–${f.end} window.`];
        parts.push(f.row
          ? `It's assembled from the slices it spans that hold orders (${list(f.merged)}): ${result(f.row)}, and emitted.`
          : 'None of its slices hold orders, so nothing is emitted.');
        if (f.next) parts.push(`A timer is set for the next window, ${f.next}.`);
        if (f.freed.length) parts.push(`No later window needs the ${f.freed[0]} slice, so it's freed.`);
        return parts.join(' ');
      }).join(' ');
    },
    matrix: { state: 'An accumulator per key and slice, freed when no open window needs it', memory: 'LocalWindowAggregate keeps partial results in memory, not state, until a watermark or checkpoint sends them on', watermarks: 'Triggers results and frees state', watermarksOut: 'Passed through, after the windows it fires' },
    facts: {
      state: 'GlobalWindowAggregate keeps one accumulator per store and 5-minute slice, and a timer at the next window\'s end. Each order is stored once, though it belongs to three windows. A slice is freed when the last window that needs it has fired. LocalWindowAggregate\'s partial results are in memory, not in state.',
      ...sharedFacts,
      output: 'Append-only. Every 5 minutes, one final row per store for the 15 minutes just ended.',
    },
    explain: twoPhase('HOP(slice_end=[$slice_end], size=[15 min], slide=[5 min])', 'HOP(time_col=[event_time], size=[15 min], slide=[5 min])'),
    details: `
<p>A hopping (or sliding) window has a size and a slide. Here, every 5 minutes, it covers the last
15. Windows overlap, so each order is counted in three windows.</p>
<p>Flink doesn't store an order three times. It cuts time into <em>slices</em>, as long as the
greatest common divisor of the size and the slide: 5 minutes here. Each order goes into exactly one
slice, and when a window fires, Flink merges the three slices it spans. Then it frees the window's
first slice, which no later window needs.</p>
<p>The timers follow the data. An order sets a timer at the end of its slice, and each window that
fires with data sets a timer for the next one. Once a window comes out empty, no more timers are
set until another order arrives, so empty windows are never emitted. Windows aren't aligned to the
first order, either: the order at 12:01:10 is in 11:50–12:05 and 11:55–12:10, which started before
it.</p>
<p>Like a tumbling window, it's planned in two phases: LocalWindowAggregate sends on one partial
result per store and slice, and GlobalWindowAggregate builds the windows from them.</p>`,
  });

  // ---------- Cumulate ----------

  const cumulate = variant({
    id: 'cumulate',
    label: 'Cumulate',
    question: 'How much has each store sold so far this hour, updated every 15 minutes?',
    blurb: 'A cumulating window grows in steps from the start of the hour, and emits a running total at each step, until it reaches its full size. Only store 7 sends orders here.',
    intro: 'Store 7 sends orders. The window starts on the hour, and grows by 15 minutes at a time, up to an hour. Press Play, or step through with the arrow keys.',
    tvf: ['CUMULATE(', '  TABLE orders,', '  DESCRIPTOR(event_time),', "  INTERVAL '15' MINUTES,", "  INTERVAL '1' HOUR)"],
    nodeLine: 'CUMULATE 1 h, every 15',
    cfg: { kind: 'cumulate', size: 3600, step: 900 },
    inputs: [
      order(101, 7, 18.00, '12:05:10'), order(103, 7, 31.00, '12:11:30'), WM,
      order(106, 7, 27.50, '12:17:20'), WM,
      order(108, 7, 21.00, '12:24:10'), order(110, 7, 14.00, '12:38:30'), WM,
      order(111, 7, 25.00, '12:52:40'), WM,
      order(112, 7, 9.00, '13:03:05'), WM,
    ],
    fire(ev) {
      return ev.firings.map((f) => {
        const parts = [`The watermark passes ${f.end}.`];
        if (!f.merged.length) {
          parts.push(`The ${f.start}–${f.end} window fires: ${result(f.row)}. Its slice is the window's first slice, which every later step shares.`);
        } else {
          parts.push(`The ${list(f.merged)} slice is merged into the shared first slice, which now covers ${f.start}–${f.end}: ${result(f.row)}. That's emitted as the ${f.start}–${f.end} window.`);
        }
        if (f.next) parts.push(`A timer is set for the next step, ${f.next}.`);
        else parts.push('The window has reached its full hour, so its state is freed.');
        return parts.join(' ');
      }).join(' ');
    },
    matrix: { state: 'An accumulator per key and slice, freed when the hour is complete', memory: 'LocalWindowAggregate keeps partial results in memory, not state, until a watermark or checkpoint sends them on', watermarks: 'Triggers results and frees state', watermarksOut: 'Passed through, after the windows it fires' },
    facts: {
      state: 'GlobalWindowAggregate keeps one accumulator per store and 15-minute slice, and a timer for the next step. As each step fires, its slice is merged into the window\'s first slice, which holds the running total. It\'s all freed when the hour is complete. LocalWindowAggregate\'s partial results are in memory, not in state.',
      ...sharedFacts,
      output: 'Append-only. Every 15 minutes, a final total for the hour so far. Each row covers more time than the one before.',
    },
    explain: twoPhase('CUMULATE(slice_end=[$slice_end], max_size=[1 h], step=[15 min])', 'CUMULATE(time_col=[event_time], max_size=[1 h], step=[15 min])'),
    details: `
<p>A cumulating window answers "so far today" or "so far this hour" questions. All the windows in an
hour start on the hour, and they end at each step: 12:00–12:15, 12:00–12:30, 12:00–12:45 and
12:00–13:00. Each one is final when it's emitted, so the output is still append-only, unlike a
non-windowed running total.</p>
<p>The state is cut into 15-minute slices. When a step fires, Flink merges that step's slice into the
window's first slice, which is shared by all the windows of the hour, and emits it. So the first
slice always holds the total up to the last step that fired. When the hour is complete, it's
freed.</p>
<p>Once a window has data, a timer is set for every step until the end of the hour, even if no more
orders arrive.</p>
<p>Like a tumbling window, it's planned in two phases: LocalWindowAggregate sends on one partial
result per store and slice, and GlobalWindowAggregate builds the windows from them.</p>`,
  });

  // ---------- Session ----------

  const session = variant({
    id: 'session',
    label: 'Session',
    question: 'When was each store busy, and how much did it sell in each rush?',
    blurb: 'A session window groups orders that come in close together. A session ends after 10 minutes without an order. Each store has its own sessions.',
    intro: 'Stores 7 and 9 send orders. Each order keeps its store\'s session open for another 10 minutes. Press Play, or step through with the arrow keys.',
    tvf: ['SESSION(', '  TABLE orders PARTITION BY store_id,', '  DESCRIPTOR(event_time),', "  INTERVAL '10' MINUTES)"],
    op: 'sessionWindowAggregate',
    nodeLine: 'SESSION 10 min gap',
    cfg: { gap: 600 },
    inputs: [
      order(101, 7, 18.00, '12:01:10'), order(102, 9, 24.50, '12:02:30'), order(103, 7, 31.00, '12:06:00'), WM,
      order(104, 7, 13.00, '12:14:00'), order(105, 9, 19.00, '12:20:00'), WM,
      order(106, 7, 27.50, '12:22:00'), order(107, 7, 21.00, '12:40:00'), WM,
    ],
    record(ev) {
      const r = ev.row;
      if (ev.type === 'new-session') return `Order ${r.order_id} starts a session for store ${r.store_id}: ${ev.now}, its own time plus the 10-minute gap. A timer is set for the session's end.`;
      if (ev.type === 'merge') return `Order ${r.order_id}'s window, ${ev.own}, overlaps store ${r.store_id}'s session ${list(ev.was)}, so they merge into ${ev.now}: ${result({ orders: ev.acc.count, sales: ev.acc.sum })}. The timer moves to the new end.`;
      return `Order ${r.order_id}'s own time plus the 10-minute gap is already behind the watermark, so it's late, and dropped.`;
    },
    fire(ev) {
      const done = ev.rows.map((x) => `store ${x.store_id}'s ${x.window_start}–${x.window_end} session (${result(x)})`);
      return `The watermark reaches ${ev.time}, past the end of ${list(done)}. Ten quiet minutes have passed, so ${ev.rows.length > 1 ? 'they are' : 'it is'} complete. WindowAggregate emits ${ev.rows.length > 1 ? 'them' : 'it'}, and frees the state.`;
    },
    matrix: { state: 'A session per key, merged as rows arrive, freed when it fires', watermarks: 'Triggers results and frees state', watermarksOut: 'Passed through, after the sessions it fires' },
    facts: {
      state: 'One accumulator per store and open session, and a timer at the session\'s end. When sessions merge, so does their state. A session\'s state is freed when it fires.',
      watermarks: 'A session fires when the watermark passes its end: its last order plus the gap. An order is late, and dropped, if its own time plus the gap is already behind the watermark. One that misses its session but isn\'t that late starts a new session.',
      output: 'Append-only. One row per session, once it has ended.',
    },
    explain: `
Sink(table=[store_sales], fields=[window_start, window_end, store_id, orders, sales], changelogMode=[NONE])
+- Calc(select=[window_start, window_end, store_id, orders, sales], changelogMode=[I])
   +- WindowAggregate(groupBy=[store_id], window=[SESSION(time_col=[event_time], gap=[10 min],
        partition keys=[store_id])], select=[store_id, COUNT(*) AS orders, SUM(total) AS sales,
        start('w$) AS window_start, end('w$) AS window_end], changelogMode=[I])
      +- Exchange(distribution=[hash[store_id]], changelogMode=[I])${explainTail.replace(/\n {12}/g, '\n         ')}`,
    details: `
<p>Session windows don't have a fixed size. Each order opens a window that runs from its own time to
10 minutes later. If that overlaps a session the store already has, the two merge, and the session
gets longer. So a session's end is always its last order plus the gap.</p>
<p>Because a session can keep growing, Flink can only emit it once the watermark passes its end:
when 10 minutes have gone by, in event time, with no order from that store. Then the session fires,
and its state is freed.</p>
<p>With out-of-order data, an order can even land between two sessions and merge them into one.
Here the watermark trails by only a second, so that can't happen before the earlier session fires.</p>
<p><code>SESSION</code> takes a <code>PARTITION BY</code>, so every store has its own sessions. Unlike
the other windows, sessions can't be cut into slices, so the planner doesn't split the aggregation
into local and global phases: it's a single WindowAggregate after the hash exchange.</p>`,
  });

  // ---------- Updating input ----------
  //
  // The tumbling window again, over order_status, an upsert-kafka table. Cancelling an order
  // retracts it from its window, but only until that window fires.

  const statusRow = (order_id, store_id, status, total, event_time) => ({ key: order_id, value: { store_id, status, total, event_time } });
  const placed = (id, store, total, time) => statusRow(id, store, 'placed', total, time);
  const cancelled = (id, store, total, time) => statusRow(id, store, 'cancelled', total, time);

  const cfgUpdating = { kind: 'tumble', size: 300, key: 'store_id', keyLabel: 'store', timeField: 'event_time', sumField: 'total' };

  const updating = {
    id: 'updating',
    label: 'Updating input',
    question: 'How much did each store sell, every 5 minutes, not counting cancelled orders?',
    blurb: 'Now the input is <code>order_status</code>, an <code>upsert-kafka</code> table, and a cancelled order has to come back out of its window. It can, but only until the window fires.',
    intro: 'order_status is keyed by order_id. Its event_time is when the order was placed, and a status change keeps it. Windows are 5 minutes long, and the watermark trails the newest order by 1 second. Press Play, or step through with the arrow keys.',
    sql: `
INSERT INTO store_sales
SELECT window_start, window_end, store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM TUMBLE(
  TABLE order_status,
  DESCRIPTOR(event_time),
  INTERVAL '5' MINUTES)
WHERE status <> 'cancelled'
GROUP BY window_start, window_end, store_id;`,
    ddl: `
CREATE TABLE order_status (
  order_id    BIGINT,
  store_id    INT,
  status      STRING,
  total       DECIMAL(10, 2),
  event_time  TIMESTAMP(3),  -- when the order was placed
  PRIMARY KEY (order_id) NOT ENFORCED,
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ('connector' = 'upsert-kafka', 'topic' = 'order_status');

CREATE TABLE store_sales (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  store_id      INT,
  orders        BIGINT,
  sales         DECIMAL(38, 2)
) WITH ('connector' = 'kafka', 'topic' = 'store_sales');

-- Connection and format options are left out.`,
    matrix: { state: 'The last row per order, and an accumulator per key and window', memory: 'LocalWindowAggregate keeps partial results in memory, not state, until a watermark or checkpoint sends them on', watermarks: 'Trigger results, and set the deadline for retractions', watermarksOut: 'Passed through, after the windows it fires' },
    facts: {
      state: 'ChangelogNormalize keeps the last row of every order that hasn\'t been cancelled, so it can retract it. GlobalWindowAggregate keeps an accumulator per store and window, and frees it when the window fires. LocalWindowAggregate\'s partial results are in memory, not in state.',
      watermarks: 'They fire the windows, and so they set the deadline for cancellations. A cancellation counts only if it arrives before the watermark passes the end of the order\'s window.',
      output: 'Append-only, although the input is updating. Each window is still emitted once, when it fires, so a plain Kafka topic can take it.',
    },
    graphLabel: 'Job graph: upsert-kafka source order_status, hash partitioned by order_id into ChangelogNormalize with the filter, which emits a retract stream into LocalWindowAggregate (tumble), hash partitioned by store_id into GlobalWindowAggregate, then Kafka sink store_sales, which is append-only.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'upsertKafkaSource', title: 'Kafka source', lines: ['order_status', 'upsert-kafka'], sqlLines: [6],
          cfg: {
            topic: 'order_status', key: 'order_id', showKey: true, delay: 1, timeField: 'event_time',
            columns: [{ f: 'store_id', num: true }, { f: 'status' }, { f: 'total', ...money }, { f: 'event_time' }],
          },
        },
        {
          id: 'cn', kind: 'op', op: 'changelogNormalize', title: 'ChangelogNormalize', lines: ["status <> 'cancelled'"],
          stateful: true, planner: true, sqlLines: [6, 9],
          cfg: {
            key: 'order_id', condition: (r) => r.status !== 'cancelled', generateUpdateBefore: true,
            stateTitle: 'Last row per order_id',
            stateFields: ['order_id', 'store_id', 'total', 'event_time'],
            stateFormat: { total: U.money },
          },
        },
        {
          id: 'local', kind: 'op', op: 'localWindowAggregate', title: 'LocalWindowAggregate', lines: ['TUMBLE 5 min', 'pre-aggregate'],
          panel: true, sqlLines: [3, 4, 5, 7, 8],
          cfg: { kind: 'tumble', size: 300, key: 'store_id', keyLabel: 'store', timeField: 'event_time', sumField: 'total' },
        },
        {
          id: 'win', kind: 'op', op: 'globalWindowAggregate', title: 'GlobalWindowAggregate', lines: ['TUMBLE 5 min', 'merge, fire'],
          stateful: true, sqlLines: [2, 3, 4, 10],
          cfg: { kind: 'tumble', size: 300, key: 'store_id', keyLabel: 'store', timeField: 'event_time', sumField: 'total' },
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
        { from: 'src', to: 'cn', mode: 'upsert', kinds: '+U -D', exchange: 'hash(order_id)' },
        { from: 'cn', to: 'local', mode: 'retract', kinds: '+I -U +U -D' },
        // The plan labels this edge with its input's changelog mode, but every partial result is
        // a plain insert, with a negative count for retractions.
        { from: 'local', to: 'win', mode: 'retract', kinds: '+I -U +U -D', exchange: 'hash(store_id)' },
        { from: 'win', to: 'sink', mode: 'append', kinds: '+I' },
      ],
    },
    // 101 is cancelled in time, 103 too late, after its window has fired. 104 is store 7's only
    // order in the second window, so cancelling it leaves that window empty.
    inputs: [
      placed(101, 7, 18.00, '12:01:10'), placed(102, 9, 24.50, '12:02:30'), placed(103, 7, 31.00, '12:03:45'),
      cancelled(101, 7, 18.00, '12:01:10'),
      placed(104, 7, 13.00, '12:06:20'), WM,
      cancelled(103, 7, 31.00, '12:03:45'),
      placed(105, 9, 19.00, '12:08:30'), cancelled(104, 7, 13.00, '12:06:20'),
      placed(106, 9, 22.00, '12:10:40'), WM,
    ],
    pill,
    narrate(node, ev) {
      if (node === 'src' && ev.type === 'read') {
        const r = ev.row;
        return r.status === 'cancelled'
          ? `The source reads ${ev.key} → cancelled. It's still order ${ev.key}'s row, with the time it was placed, ${r.event_time}, so the watermark doesn't move. To the source it's just another upsert (+U).`
          : `The source reads ${ev.key} → placed: store ${r.store_id}, ${U.money(r.total)}, at ${r.event_time}. upsert-kafka can't tell whether order ${ev.key} is new, so the planner treats every value as an upsert (+U).`;
      }
      if (node === 'src' && ev.type === 'wm') {
        return `The source emits a watermark for ${ev.time}: the newest event time it has read, minus 1 second.`;
      }
      if (node === 'cn') {
        switch (ev.type) {
          case 'insert': return `Order ${ev.key} isn't in state, and it isn't cancelled. ChangelogNormalize stores the row and emits an insert (+I).`;
          case 'unmatched': return `Order ${ev.key} is now cancelled, so it fails the filter. ChangelogNormalize emits a delete (−D) with the row from state, which says when the order was placed (${ev.prev.event_time}) and what it cost, and clears the key.`;
          case 'wm': return `The watermark passes through ChangelogNormalize.`;
        }
      }
      if (node === 'local' && ev.type === 'flush' && ev.sent.some((x) => x.acc.count <= 0)) {
        return `${Ops.localWindowAggregate.say(ev, cfgUpdating)} The plan labels this edge as a retract stream, like the one coming in, but each partial result is a plain insert: a retraction only lowers its count and sum, here to zero or below.`;
      }
      if (node === 'win' && (ev.type === 'fire' || ev.type === 'merge')) {
        const lost = ev.late.map((l) => `Store ${l.k}'s result in store_sales still counts the cancelled order.`).join(' ');
        if (ev.type === 'merge') return `The watermark, ${ev.time}, arrives. ${merged(ev, cfgUpdating)}${lost}`;
        const wins = [...new Set(ev.firings.map((f) => `${f.start}–${f.end}`))];
        const rows = ev.firings.filter((f) => f.row).map((f) => `store ${f.k}, ${result(f.row)}`).join('; ');
        const empty = ev.firings.filter((f) => f.empty).map((f) => `store ${f.k}`);
        return `The watermark, ${ev.time}, arrives. ${merged(ev, cfgUpdating)}${lost ? `${lost} ` : ''}Then the ${list(wins)} window is complete: GlobalWindowAggregate emits a final row for ${rows}.`
          + (empty.length ? ` The count for ${list(empty)} is 0, so nothing is emitted for it.` : '')
          + ' It frees the window\'s state, and forwards the watermark.';
      }
      if (node === 'sink' && ev.type === 'append') {
        const r = ev.row;
        return `The sink appends store ${r.store_id}'s result for ${r.window_start}–${r.window_end}: ${result(r)}. It's final, and will never be updated.`;
      }
      if (node === 'sink' && ev.type === 'wm') return `The watermark reaches the sink, which doesn't write it to Kafka.`;
      return null;
    },
    explain: `
Sink(table=[store_sales], fields=[window_start, window_end, store_id, orders, sales], changelogMode=[NONE])
+- Calc(select=[window_start, window_end, store_id, orders, sales], changelogMode=[I])
   +- GlobalWindowAggregate(groupBy=[store_id], window=[TUMBLE(slice_end=[$slice_end], size=[5 min])],
        select=[store_id, COUNT_RETRACT(count1$0) AS orders, SUM_RETRACT((sum$1, count$2)) AS sales,
                start('w$) AS window_start, end('w$) AS window_end], changelogMode=[I])
      +- Exchange(distribution=[hash[store_id]], changelogMode=[I,UB,UA,D])
         +- LocalWindowAggregate(groupBy=[store_id], window=[TUMBLE(time_col=[event_time], size=[5 min])],
              select=[store_id, COUNT_RETRACT(*) AS count1$0, SUM_RETRACT(total) AS (sum$1, count$2),
                      slice_end('w$) AS $slice_end], changelogMode=[I,UB,UA,D])
            +- Calc(select=[store_id, total, event_time], changelogMode=[I,UB,UA,D])
               +- ChangelogNormalize(key=[order_id], condition=[<>(status, 'cancelled')], changelogMode=[I,UB,UA,D])
                  +- Exchange(distribution=[hash[order_id]], changelogMode=[UA,D])
                     +- TableSourceScan(table=[[order_status, watermark=[-(event_time, 1000:INTERVAL SECOND)],
                          watermarkEmitStrategy=[on-periodic]]], fields=[...], changelogMode=[UA,D])`,
    details: `
<p>Window aggregation accepts an updating input. When the input can retract, the planner switches
to <code>COUNT_RETRACT</code> and <code>SUM_RETRACT</code>, so a <code>−D</code> or
<code>−U</code> takes its row back out of the window's accumulator. The output is still
append-only. A window emits once, when it fires, so its result can go to a plain Kafka topic.</p>
<p>The upsert source only says what each order is now. To take a cancelled order back out of its
window, the aggregate needs the row it counted, so the planner puts a ChangelogNormalize in front.
It also moves the <code>WHERE</code> clause into it, which shows as <code>condition=</code> in the
plan. When an order is cancelled, it no longer matches, and ChangelogNormalize emits a
<code>−D</code> with the stored row, with the order's original total and event time. That's how the
retraction finds the window the order was counted in.</p>
<p>That's also why <code>event_time</code> has to be when the order was placed. If each status
change carried its own time, an update would retract the order from one window, and count it
again in a later one.</p>
<p>Retractions have a deadline: the watermark. Once a window fires, its result is in Kafka, and
its state is gone. A retraction that turns up later is late, and Flink drops it like any other
late record. Order 103's cancellation is lost this way, and store 7's 12:00–12:05 result still
counts it. If late cancellations matter, give the watermark a longer delay, at the cost of later
results. Or use a non-windowed aggregate, whose output is an updating stream, so it can always
be corrected.</p>
<p>When every order in a window is cancelled, its count drops to zero, and the window emits
nothing when it fires. Flink checks <code>COUNT(*)</code> before emitting, and nothing for that
window was emitted before, so there's nothing downstream to correct.</p>
<p>Only window aggregation works this way. Window Top-N, window deduplication, window joins and
<code>OVER</code> aggregations reject an updating input. The result of a regular join, or of a
non-windowed <code>GROUP BY</code>, can't be windowed at all, because it no longer has a time
attribute. A Debezium source (<code>debezium-json</code>) can be windowed, and needs no
ChangelogNormalize, because it already carries the old row in each update.</p>`,
  };

  window.PATTERNS.push({
    id: 'windows',
    nav: 'Windows',
    title: 'Windows',
    summary: '<p>Group events into windows of time, and emit one final result per window when the watermark says the window is complete. The output is append-only, and each window\'s state is freed when it fires.</p>',
    variants: [tumble, hop, cumulate, session, updating],
  });
})();
