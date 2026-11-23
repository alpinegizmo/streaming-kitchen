// Pattern: complex event processing with MATCH_RECOGNIZE.
// Verified against verification/cep.sql (see cep.plan.txt).
'use strict';

window.PATTERNS = window.PATTERNS || [];

(() => {
  const ticket = (store_id, prep_minutes, event_time) => ({ value: { store_id, prep_minutes, event_time } });
  const wmark = { wm: true };
  const hm = (t) => t.slice(0, 5);
  const mins = (r) => `${r.prep_minutes} min`;
  const at = (r) => `${r.prep_minutes} (${hm(r.event_time)})`;
  const seq = (rows) => rows.map((r) => r.prep_minutes).join(' → ');
  const list = (xs) => (xs.length < 2 ? xs.join('') : `${xs.slice(0, -1).join(', ')} and ${xs[xs.length - 1]}`);

  // What the pattern did with one store's rows, in a sentence.
  function storeSentence(k) {
    const parts = [`Store ${k.key} takes ${list(k.steps.map((s) => at(s.row)))}`];
    const last = k.steps[k.steps.length - 1];
    const longest = (ps) => ps.reduce((a, p) => (p.length > a.length ? p : a), []);
    const v = last.row.prep_minutes;
    if (last.match) {
      parts.push(`: ${seq(last.match)} is a match, so one row is emitted. The partial matches that overlap it are discarded (SKIP PAST LAST ROW).`);
    } else if (longest(last.partials).length > 1) {
      parts.push(`. That makes ${seq(longest(last.partials))} a partial match, waiting for a third, slower ticket.`);
    } else if (last.dropped.length) {
      const d = longest(last.dropped);
      parts.push(`. ${v} isn't slower than ${d[d.length - 1].prep_minutes}, so the partial match ${seq(d)} is discarded, and ${v} starts a new one.`);
    } else {
      parts.push(`. It's the store's first ticket, so it starts a partial match, as A.`);
    }
    return parts.join('');
  }

  // A row that was read after a later one, but is fed to the pattern first.
  function overtaken(k) {
    const fed = k.steps;
    for (let i = 0; i < fed.length; i++) {
      const later = [...fed.slice(i + 1), ...k.waiting].find((o) => o.seq < fed[i].seq);
      if (later) return `The ${fed[i].row.prep_minutes} was read after the ${later.row.prep_minutes}, but it's earlier in event time, so it goes first.`;
    }
    return null;
  }

  const cep = {
    id: 'match',
    label: 'MATCH_RECOGNIZE',
    question: 'Which kitchens are falling behind?',
    intro: 'Each kitchen ticket says how long a pizza took to make. A kitchen is falling behind when three tickets in a row each take longer than the one before.',
    sql: `
INSERT INTO slow_kitchens
SELECT *
FROM kitchen_tickets MATCH_RECOGNIZE (
  PARTITION BY store_id
  ORDER BY event_time
  MEASURES A.prep_minutes AS first_minutes,
           C.prep_minutes AS last_minutes,
           MATCH_ROWTIME() AS detected_at
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (A B C)
  DEFINE
    B AS B.prep_minutes > A.prep_minutes,
    C AS C.prep_minutes > B.prep_minutes
);`,
    ddl: `
CREATE TABLE kitchen_tickets (
  order_id      BIGINT,
  store_id      INT,
  prep_minutes  INT,
  event_time    TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ('connector' = 'kafka', 'topic' = 'kitchen_tickets');

CREATE TABLE slow_kitchens (
  store_id       INT,
  first_minutes  INT,
  last_minutes   INT,
  detected_at    TIMESTAMP(3)
) WITH ('connector' = 'kafka', 'topic' = 'slow_kitchens');

-- Connection and format options are left out.`,
    matrix: { state: 'Rows waiting for the watermark, and partial matches, per key', watermarks: 'Puts rows in event-time order; late rows are dropped', watermarksOut: 'Passed through, after the matches it completes' },
    facts: {
      state: 'Per store: the rows that are waiting for the watermark, and the partial matches. With PATTERN (A B C) and no gaps allowed, a store never has more than two partial matches.',
      watermarks: 'Rows are buffered until the watermark passes them, then fed to the pattern in event-time order. A row at or before the watermark is late, and dropped.',
      output: 'Append-only: one row per match, emitted once the match is complete.',
    },
    graphLabel: 'Job graph: Kafka source kitchen_tickets, hash partitioned by store_id into Match, then Kafka sink slow_kitchens. All edges are append-only.',
    graph: {
      nodes: [
        {
          id: 'src', kind: 'source', op: 'kafkaSource', title: 'Kafka source', lines: ['kitchen_tickets'], sqlLines: [3],
          cfg: { topic: 'kitchen_tickets', delay: 1, timeField: 'event_time', columns: [{ f: 'store_id', num: true }, { f: 'prep_minutes', num: true }, { f: 'event_time' }] },
        },
        {
          id: 'match', kind: 'op', op: 'match', title: 'Match', lines: ['PATTERN (A B C)'],
          stateful: true, stateKind: 'small', sqlLines: [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
          cfg: {
            key: 'store_id', keyLabel: 'store', timeField: 'event_time', valueField: 'prep_minutes', valueLabel: 'minutes',
            next: (prev, row) => row.prep_minutes > prev.prep_minutes,
            measures: (a, b, c) => ({ store_id: a.store_id, first_minutes: a.prep_minutes, last_minutes: c.prep_minutes, detected_at: c.event_time }),
          },
        },
        {
          id: 'sink', kind: 'sink', op: 'kafkaSink', title: 'Kafka sink', lines: ['slow_kitchens'], sqlLines: [1, 2],
          cfg: { topic: 'slow_kitchens', columns: [{ f: 'store_id', num: true }, { f: 'first_minutes', num: true }, { f: 'last_minutes', num: true }, { f: 'detected_at' }] },
        },
      ],
      edges: [
        { from: 'src', to: 'match', mode: 'append', kinds: '+I', exchange: 'hash(store_id)' },
        { from: 'match', to: 'sink', mode: 'append', kinds: '+I' },
      ],
    },
    inputs: [
      ticket(7, 14, '12:20:00'), ticket(9, 11, '12:22:00'), ticket(7, 9, '12:24:00'), wmark,
      ticket(9, 12, '12:27:00'), ticket(7, 18, '12:35:00'), ticket(7, 13, '12:29:00'), wmark,
      ticket(9, 9, '12:38:00'), ticket(7, 10, '12:41:00'), wmark,
      ticket(9, 15, '12:36:00'),
    ],
    pill: (r) => ('first_minutes' in r
      ? `${r.store_id}: ${r.first_minutes}→…→${r.last_minutes}`
      : `s${r.store_id} ${r.prep_minutes}m ${hm(r.event_time)}`),
    narrate(node, ev) {
      if (node === 'src') {
        if (ev.type === 'wm') return `The source emits a watermark for ${ev.time}: the newest event time it has read, minus 1 second.`;
        return `The source reads a ticket from store ${ev.row.store_id}: ${mins(ev.row)}, at ${ev.row.event_time}.`;
      }
      if (node === 'match') {
        if (ev.type === 'late') {
          return `This ticket, at ${ev.row.event_time}, is older than the watermark (${ev.wm}), so it's late, and dropped. It would have made 11 → 12 → 15 a match for store 9, so that match is lost.`;
        }
        if (ev.type === 'buffer') {
          if (ev.ahead.length) {
            return `This ticket, at ${hm(ev.row.event_time)}, arrives after the one at ${list(ev.ahead.map((r) => hm(r.event_time)))}: it's out of order. It's buffered, and sorted into place by event time.`;
          }
          return ev.n === 1
            ? `The ticket is buffered. It can't be matched until the watermark passes ${hm(ev.row.event_time)}, because an earlier ticket for store ${ev.key} could still arrive.`
            : `Store ${ev.key}'s ticket is buffered until the watermark passes ${hm(ev.row.event_time)}.`;
        }
        if (ev.type === 'wm') return `The watermark moves to ${ev.time}, but no buffered ticket is that old. It's forwarded.`;
        const parts = [`The watermark moves to ${ev.time}, and the tickets it has passed are fed to the pattern, in event-time order.`];
        for (const k of ev.keys) {
          parts.push(storeSentence(k));
          const o = overtaken(k);
          if (o) parts.push(o);
        }
        return parts.join(' ');
      }
      if (node === 'sink') {
        return ev.type === 'wm'
          ? 'Watermarks aren\'t written to Kafka.'
          : `The sink appends the match to slow_kitchens: store ${ev.row.store_id}, detected at ${ev.row.detected_at}, the time of its last ticket.`;
      }
      return null;
    },
    explain: `
Sink(table=[slow_kitchens], fields=[store_id, first_minutes, last_minutes, detected_at], changelogMode=[NONE])
+- Match(partitionBy=[store_id], orderBy=[event_time ASC], measures=[FINAL(A.prep_minutes) AS first_minutes,
     FINAL(C.prep_minutes) AS last_minutes, FINAL(MATCH_ROWTIME()) AS detected_at],
     rowsPerMatch=[ONE ROW PER MATCH], after=[SKIP PAST LAST ROW], pattern=[(('A', 'B'), 'C')],
     define=[{B=>(PREV(B.$2, 0), LAST(A.$2, 0)), C=>(PREV(C.$2, 0), LAST(B.$2, 0))}], changelogMode=[I])
   +- Exchange(distribution=[hash[store_id]], changelogMode=[I])
      +- TableSourceScan(table=[[kitchen_tickets, watermark=[-(event_time, 1000:INTERVAL SECOND)]]],
           fields=[order_id, store_id, prep_minutes, event_time], changelogMode=[I])`,
    details: `
<p><code>MATCH_RECOGNIZE</code> is row pattern recognition, from SQL:2016: a regular expression over
rows. <code>PATTERN (A B C)</code> asks for three rows in a row, and <code>DEFINE</code> says what
each one must satisfy. <code>A</code> has no condition, so any row can start a match.</p>
<p>A pattern is about order, so the rows have to be in order first. With
<code>ORDER BY event_time</code>, Flink buffers each row until the watermark passes it, then sorts
the buffered rows by event time and feeds them to the pattern. A ticket that arrives out of order
is put back in place, as long as the watermark hasn't passed it yet. A row at or before the
watermark is late; SQL has no side output for it, so it's dropped. Here that costs a match.</p>
<p>That buffering is also why results are delayed: store 7's match ends at 12:35, but it's only
emitted when the watermark passes 12:35. A larger watermark delay sorts more late rows into place,
and delays every match by the same amount.</p>
<p>The rest of the state is the partial matches: the matches that have started but aren't complete.
Without a quantifier, <code>PATTERN (A B C)</code> allows no rows in between, so a partial match
that can't take the next row is discarded straight away. A pattern with gaps or quantifiers, such
as <code>(A B+ C)</code> or <code>(A B*? C)</code>, can keep partial matches for a long time. Add
<code>WITHIN INTERVAL '30' MINUTE</code> to discard them once they're too old.</p>
<p><code>AFTER MATCH SKIP PAST LAST ROW</code> starts the next match after the last row of this one,
so matches don't overlap. With <code>SKIP TO NEXT ROW</code>, the partial match 13 → 18 would
survive store 7's match, and a slower ticket after 18 would complete a second, overlapping one.</p>
<p><code>MATCH_ROWTIME()</code> is the time of the match's last row, and it's still a time attribute,
so the output has a watermark, and can feed a window or another pattern.</p>`,
  };

  window.PATTERNS.push({
    id: 'cep',
    nav: 'Complex event processing',
    title: 'Complex event processing',
    summary: '<p>Find a sequence of events, such as three in a row that each get worse. <code>MATCH_RECOGNIZE</code> is a regular expression over rows. It needs the rows in event-time order, so the watermark decides when they can be matched.</p>',
    variants: [cep],
  });
})();
