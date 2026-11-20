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

  // Builds one window variant. `fire` narrates a watermark that fires windows.
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
      graphLabel: `Job graph: Kafka source orders, hash partitioned by store_id into WindowAggregate (${v.label.toLowerCase()}), then Kafka sink store_sales. Both edges are append-only.`,
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
            id: 'win', kind: 'op', op: v.op, title: 'WindowAggregate', lines: v.nodeLines,
            stateful: true, sqlLines: [2, 3, 4, 5, ...Array.from({ length: lines - 2 }, (_, i) => 7 + i), 5 + lines],
            cfg: { ...v.cfg, key: 'store_id', keyLabel: 'store', timeField: 'event_time', sumField: 'total' },
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
      inputs: v.inputs,
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
          if (ev.type === 'wm') return `The watermark, ${ev.time}, hasn't reached any timer, so nothing fires. WindowAggregate forwards it.`;
          if (ev.type === 'fire') return v.fire(ev);
          return v.record(ev);
        }
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
    op: 'windowAggregate',
    nodeLines: ['TUMBLE 5 min', 'local + global'],
    cfg: { kind: 'tumble', size: 300 },
    inputs: [
      order(101, 7, 18.00, '12:01:10'), order(102, 9, 24.50, '12:02:30'), order(103, 7, 31.00, '12:03:45'), WM,
      order(104, 7, 13.00, '12:04:20'), order(106, 7, 27.50, '12:05:01'), WM,
      order(105, 7, 16.00, '12:04:58'), order(107, 9, 19.00, '12:07:30'), order(108, 7, 21.00, '12:09:10'), WM,
      order(109, 9, 30.00, '12:10:20'), WM,
    ],
    record(ev) {
      const r = ev.row;
      if (ev.type === 'new-slice') return `Order ${r.order_id} falls in the ${ev.slice.start}–${ev.slice.end} window. Store ${r.store_id} has no accumulator for it yet, so one is created, with a timer for ${ev.timer}.`;
      if (ev.type === 'accumulate') return `Order ${r.order_id} also falls in the ${ev.slice.start}–${ev.slice.end} window. Store ${r.store_id}'s accumulator becomes ${result({ orders: ev.acc.count, sales: ev.acc.sum })}.`;
      return `Order ${r.order_id} was placed at ${r.event_time}, but the watermark is already at ${ev.wm}. Its ${ev.slice.start}–${ev.slice.end} window has fired and its state is gone, so the order is late, and dropped.`;
    },
    fire(ev) {
      const wins = [...new Set(ev.firings.map((f) => `${f.start}–${f.end}`))];
      const rows = ev.rows.map((x) => `store ${x.store_id}, ${result(x)}`).join('; ');
      return `The watermark reaches ${ev.time}, so the ${list(wins)} window is complete. WindowAggregate emits a final row for each store (${rows}), frees their state, and then forwards the watermark.`;
    },
    matrix: { state: 'An accumulator per key and window, freed when the window fires', watermarks: 'Triggers results and frees state', watermarksOut: 'Passed through, after the windows it fires' },
    facts: {
      state: 'One accumulator per store and window (an order count and a running sum), and a timer at the end of each window. A window\'s state is freed as soon as it fires.',
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
before the shuffle, so only one partial result per store and window crosses the network.
GlobalWindowAggregate, after the hash exchange on <code>store_id</code>, merges them in keyed
state and fires the timers. The graph draws them as one operator. It also leaves out two Calcs that
only drop columns.</p>
<p>Under the hood, the state is kept per <em>slice</em>. For a tumbling window, a slice is the whole
window. Hopping and cumulating windows share slices between overlapping windows.</p>
<p>If late orders matter, give the watermark a longer delay, at the cost of later results.
<code>CURRENT_WATERMARK(event_time)</code> can flag rows that are behind the watermark, but that
catches more than the window drops: a row behind the watermark still counts while its window is
open.</p>`,
  });

  // ---------- Hop ----------

  const hop = variant({
    id: 'hop',
    label: 'Hop',
    question: 'How much did each store sell in the last 15 minutes, updated every 5?',
    blurb: 'A hopping window is 15 minutes long and starts every 5 minutes, so windows overlap and each order belongs to three of them. To keep one example readable, only store 7 sends orders here.',
    intro: 'Store 7 sends orders. Windows are 15 minutes long and start every 5 minutes. Flink keeps state per 5-minute slice, and builds each window from three slices. Press Play, or step through with the arrow keys.',
    tvf: ['HOP(', '  TABLE orders,', '  DESCRIPTOR(event_time),', "  INTERVAL '5' MINUTES,", "  INTERVAL '15' MINUTES)"],
    op: 'windowAggregate',
    nodeLines: ['HOP 15 min, every 5', 'local + global'],
    cfg: { kind: 'hop', size: 900, slide: 300 },
    inputs: [
      order(101, 7, 18.00, '12:01:10'), order(103, 7, 31.00, '12:03:45'), WM,
      order(106, 7, 27.50, '12:06:20'), WM,
      order(108, 7, 21.00, '12:09:10'), order(110, 7, 14.00, '12:12:30'), WM,
      order(111, 7, 25.00, '12:16:40'), WM,
      order(112, 7, 9.00, '12:21:05'), WM,
    ],
    record(ev) {
      const r = ev.row;
      if (ev.type === 'new-slice') {
        const end = U.toSec(`${ev.slice.end}:00`);
        const wins = [0, 1, 2].map((i) => `${U.fromSec(end - 900 + i * 300).slice(0, 5)}–${U.fromSec(end + i * 300).slice(0, 5)}`);
        return `Order ${r.order_id} belongs to three windows (${list(wins)}), but it's stored once, in the ${ev.slice.start}–${ev.slice.end} slice. That slice is new, so an accumulator is created, with a timer for ${ev.timer}.`;
      }
      if (ev.type === 'accumulate') return `Order ${r.order_id} is added to the ${ev.slice.start}–${ev.slice.end} slice: ${result({ orders: ev.acc.count, sales: ev.acc.sum })}.`;
      return `Order ${r.order_id} is late, and dropped.`;
    },
    fire(ev) {
      return ev.firings.map((f) => {
        const parts = [`The watermark, ${ev.time}, passes ${f.end}, the end of the ${f.start}–${f.end} window.`];
        parts.push(f.row
          ? `It's assembled from the slices it spans that hold orders (${list(f.merged)}): ${result(f.row)}, and emitted.`
          : 'None of its slices hold orders, so nothing is emitted.');
        if (f.next) parts.push(`A timer is set for the next window, ${f.next}.`);
        if (f.freed.length) parts.push(`No later window needs the ${f.freed[0]} slice, so it's freed.`);
        return parts.join(' ');
      }).join(' ');
    },
    matrix: { state: 'An accumulator per key and slice, freed when no open window needs it', watermarks: 'Triggers results and frees state', watermarksOut: 'Passed through, after the windows it fires' },
    facts: {
      state: 'One accumulator per store and 5-minute slice, and a timer at the next window\'s end. Each order is stored once, though it belongs to three windows. A slice is freed when the last window that needs it has fired.',
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
<p>Like a tumbling window, it's planned in two phases, and drawn here as one operator.</p>`,
  });

  // ---------- Cumulate ----------

  const cumulate = variant({
    id: 'cumulate',
    label: 'Cumulate',
    question: 'How much has each store sold so far this hour, updated every 15 minutes?',
    blurb: 'A cumulating window grows in steps from the start of the hour, and emits a running total at each step, until it reaches its full size. Only store 7 sends orders here.',
    intro: 'Store 7 sends orders. The window starts on the hour, and grows by 15 minutes at a time, up to an hour. Press Play, or step through with the arrow keys.',
    tvf: ['CUMULATE(', '  TABLE orders,', '  DESCRIPTOR(event_time),', "  INTERVAL '15' MINUTES,", "  INTERVAL '1' HOUR)"],
    op: 'windowAggregate',
    nodeLines: ['CUMULATE 1 h, every 15', 'local + global'],
    cfg: { kind: 'cumulate', size: 3600, step: 900 },
    inputs: [
      order(101, 7, 18.00, '12:05:10'), order(103, 7, 31.00, '12:11:30'), WM,
      order(106, 7, 27.50, '12:17:20'), WM,
      order(108, 7, 21.00, '12:24:10'), order(110, 7, 14.00, '12:38:30'), WM,
      order(111, 7, 25.00, '12:52:40'), WM,
      order(112, 7, 9.00, '13:03:05'), WM,
    ],
    record(ev) {
      const r = ev.row;
      if (ev.type === 'new-slice') return `Order ${r.order_id} goes into the ${ev.slice.start}–${ev.slice.end} slice. It's the slice's first order, so an accumulator is created, with a timer for ${ev.timer}.`;
      if (ev.type === 'accumulate') return `Order ${r.order_id} is added to the ${ev.slice.start}–${ev.slice.end} slice: ${result({ orders: ev.acc.count, sales: ev.acc.sum })}.`;
      return `Order ${r.order_id} is late, and dropped.`;
    },
    fire(ev) {
      return ev.firings.map((f) => {
        const parts = [`The watermark, ${ev.time}, passes ${f.end}.`];
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
    matrix: { state: 'An accumulator per key and slice, freed when the hour is complete', watermarks: 'Triggers results and frees state', watermarksOut: 'Passed through, after the windows it fires' },
    facts: {
      state: 'One accumulator per store and 15-minute slice, and a timer for the next step. As each step fires, its slice is merged into the window\'s first slice, which holds the running total. It\'s all freed when the hour is complete.',
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
<p>Like a tumbling window, it's planned in two phases, and drawn here as one operator.</p>`,
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
    nodeLines: ['SESSION 10 min gap'],
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

  window.PATTERNS.push({
    id: 'windows',
    nav: 'Windows',
    title: 'Windows',
    summary: '<p>Group events into windows of time, and emit one final result per window when the watermark says the window is complete. The output is append-only, and each window\'s state is freed when it fires.</p>',
    variants: [tumble, hop, cumulate, session],
  });
})();
