// Simplified simulators for Flink SQL's physical operators.
//
// Each operator kind provides:
//   init(cfg)                    -> operator state (plain data, so it can be snapshotted)
//   input(st, rec, cfg)          -> sources only: turn a Kafka record into a changelog row
//   record(st, item, cfg)        -> { out: [items], ev, touched }
//   watermark(st, item, cfg)     -> { out: [items], ev }
//   view(st, cfg)                -> the state panel drawn under the operator, or null
//   facts(st, cfg, ctx)          -> optional: the state, as Flink would hold it, for verification
//                                   (tools/export-replay.mjs, verification/run-state.sh):
//                                   { state: ['key' or 'key|namespace'], timers: ['key@HH:MM:SS.mmm'],
//                                     values: { flinkStateName: { 'key' or 'key|namespace': value } } }
//                                   (timers left out when the panel doesn't show them; values in
//                                   the form F describes; ctx.retract: the input has retractions)
//   say(ev, cfg)                 -> a default caption; patterns can override it
//   stateKind                    -> optional: how the state is bounded (see Graph.STATE_KINDS):
//                                   'wm' (freed by watermark), 'small' (a small set of keys),
//                                   'keys' (grows with keys), 'rows' (grows with rows); or, for
//                                   a panel with several parts, { part: kind }. A pattern's node
//                                   sets its own stateKind for what depends on the data, such as
//                                   whether its set of keys is small.
//   stateParts                   -> optional: the parts of a mixed panel, in order; view() then
//                                   gives each row its part, as row.part
//
// An item is either a changelog row { t: 'rec', kind: '+I' | '-U' | '+U' | '-D', row }
// or a watermark { t: 'wm', time: 'HH:MM:SS' }.
//
// The semantics follow the Flink 2.3 runtime classes named in each comment.
'use strict';

const Ops = (() => {
  const rec = (kind, row) => ({ t: 'rec', kind, row });
  const wm = (time) => ({ t: 'wm', time });
  const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
  const pick = (row, fields) => Object.fromEntries(fields.map((f) => [f, row[f]]));
  // A Flink timestamp (time of day, in ms) as facts() writes it.
  const ms = (millis) => `${U.fromSec(Math.floor(millis / 1000))}.${String(millis % 1000).padStart(3, '0')}`;
  // State values for facts(), which StateCheck renders the way it renders Flink's: scalars as
  // text, a row in the Flink operator's input layout (with its kind, unless +I), a tuple, a map
  // (sorted), or a list (an array). A time is HH:MM:SS, with .mmm only when it isn't 0.
  const F = {
    row: (row, input = 0, kind = '+I') => ({ row, input, ...(kind !== '+I' ? { kind } : {}) }),
    tuple: (...items) => ({ tuple: items }),
    map: (entries) => ({ map: entries }),
    dec: (n) => Number(n).toFixed(2),
    time: (sec) => (Number.isInteger(sec) ? U.fromSec(sec) : ms(Math.round(sec * 1000))),
    count: (rows) => {
      const n = new Map();
      for (const r of rows) n.set(JSON.stringify(r), (n.get(JSON.stringify(r)) || 0) + 1);
      return [...n].map(([r, c]) => [JSON.parse(r), String(c)]);
    },
    group: (items, keyOf) => {
      const g = new Map();
      for (const i of items) { const k = keyOf(i); if (!g.has(k)) g.set(k, []); g.get(k).push(i); }
      return [...g];
    },
  };
  const values = (entries) => Object.fromEntries(entries);

  // Kafka source with an append-only format (connector = 'kafka').
  // Watermarks: bounded out-of-orderness, newest event time minus `delay` seconds.
  const kafkaSource = {
    init: () => ({ maxTs: null, lastWm: null }),
    input(st, r, cfg) {
      if (r.wm) {
        if (st.maxTs == null) return { out: [], ev: { type: 'no-wm' } };
        const t = U.fromSec(st.maxTs - (cfg.delay ?? 0));
        if (t === st.lastWm) return { out: [], ev: { type: 'no-wm' } };
        st.lastWm = t;
        return { out: [wm(t)], ev: { type: 'wm', time: t } };
      }
      const row = { ...r.value };
      if (cfg.timeField) st.maxTs = Math.max(st.maxTs ?? 0, U.toSec(row[cfg.timeField]));
      return { out: [rec('+I', row)], ev: { type: 'read', row } };
    },
    view: () => null,
    say(ev, cfg) {
      if (ev.type === 'wm') return `The source emits a watermark for ${ev.time}: the newest event time it has read, minus ${cfg.delay} second.`;
      return `The source reads the next record from ${cfg.topic} and emits it as an insert.`;
    },
  };

  // upsert-kafka source. It declares its changelog as [UA, D]: every value is an
  // upsert for its key, and a tombstone (null value) is a delete that carries only the key.
  const upsertKafkaSource = {
    init: () => ({ maxTs: null, lastWm: null }),
    input(st, r, cfg) {
      if (r.wm) return kafkaSource.input(st, r, cfg);
      if (r.value == null) {
        const row = { [cfg.key]: r.key };
        return { out: [rec('-D', row)], ev: { type: 'tombstone', key: r.key, row } };
      }
      const row = { [cfg.key]: r.key, ...r.value };
      if (cfg.timeField) st.maxTs = Math.max(st.maxTs ?? 0, U.toSec(row[cfg.timeField]));
      return { out: [rec('+U', row)], ev: { type: 'read', key: r.key, row } };
    },
    view: () => null,
    say(ev, cfg) {
      if (ev.type === 'wm') return kafkaSource.say(ev, cfg);
      if (ev.type === 'tombstone') return `A tombstone for key ${ev.key}. The source emits a delete that carries only the key.`;
      return `The source reads key ${ev.key} from ${cfg.topic}. It can't tell whether the key is new, so the planner treats the value as an upsert.`;
    },
  };

  // Calc: projection and filter, stateless. Watermarks pass through unchanged. It does note the
  // current watermark, which is what CURRENT_WATERMARK() returns (null before the first one), and
  // hands it to the WHERE clause.
  const calc = {
    init: () => ({ wm: null }),
    record(st, item, cfg) {
      if (cfg.where && !cfg.where(item.row, st.wm)) {
        return { out: [], ev: { type: 'drop', row: item.row, wm: st.wm } };
      }
      const row = cfg.project ? cfg.project(item.row) : item.row;
      return { out: [rec(item.kind, row)], ev: { type: 'pass', row, in: item.row, wm: st.wm } };
    },
    watermark(st, item) {
      st.wm = item.time;
      return { out: [wm(item.time)], ev: { type: 'wm', time: item.time } };
    },
    view: () => null,
    say(ev) {
      if (ev.type === 'wm') return `The watermark passes through unchanged. A stateless operator has no use for it.`;
      if (ev.type === 'drop') return `The row doesn't match the WHERE clause, so it's dropped.`;
      return `The row matches the WHERE clause, and the projection computes the output columns.`;
    },
  };

  // ChangelogNormalize: keeps the most recent row for each key, and turns an upsert stream
  // into a proper changelog. With a pushed-down filter it also emits a delete when a
  // row stops matching. Mirrors DeduplicateFunctionHelper.processLastRowOnChangelog[WithFilter]
  // with generateUpdateBefore = false and no state TTL.
  const changelogNormalize = {
    init: () => ({ rows: {} }),
    record(st, item, cfg) {
      const key = item.row[cfg.key];
      const prev = st.rows[key];
      const matches = (row) => !cfg.condition || cfg.condition(row);

      if (item.kind === '+I' || item.kind === '+U') {
        if (prev == null) {
          if (!matches(item.row)) return { out: [], ev: { type: 'skip-new', key, row: item.row } };
          st.rows[key] = item.row;
          return { out: [rec('+I', item.row)], ev: { type: 'insert', key, row: item.row }, touched: key };
        }
        if (same(prev, item.row)) {
          return { out: [], ev: { type: 'unchanged', key, row: item.row }, touched: key };
        }
        if (!matches(item.row)) {
          delete st.rows[key];
          return { out: [rec('-D', prev)], ev: { type: 'unmatched', key, row: item.row, prev } };
        }
        st.rows[key] = item.row;
        const out = cfg.generateUpdateBefore ? [rec('-U', prev), rec('+U', item.row)] : [rec('+U', item.row)];
        return { out, ev: { type: 'update', key, row: item.row, prev }, touched: key };
      }
      // -D or -U
      if (prev == null) return { out: [], ev: { type: 'delete-missing', key } };
      delete st.rows[key];
      return { out: [rec('-D', prev)], ev: { type: 'delete', key, prev } };
    },
    watermark: (st, item) => ({ out: [wm(item.time)], ev: { type: 'wm', time: item.time } }),
    view(st, cfg) {
      const fields = cfg.stateFields || [cfg.key];
      return {
        title: cfg.stateTitle || `Most recent row per ${cfg.key}`,
        cols: fields,
        rows: Object.entries(st.rows).map(([key, row]) => ({ key, cells: fields.map((f) => (cfg.stateFormat?.[f] ? cfg.stateFormat[f](row[f]) : row[f])) })),
      };
    },
    facts: (st) => ({
      state: Object.keys(st.rows), timers: [],
      values: { 'deduplicate-state': values(Object.entries(st.rows).map(([k, r]) => [k, F.row(r)])) },
    }),
    say(ev, cfg) {
      switch (ev.type) {
        case 'insert': return `First row for key ${ev.key}. ChangelogNormalize stores it and emits an insert.`;
        case 'update': return `Key ${ev.key} is already in state, so this is an update. The stored row is replaced.`;
        case 'unchanged': return `Same content as the stored row for key ${ev.key}, so nothing is emitted.`;
        case 'unmatched': return `Key ${ev.key} no longer matches the filter. ChangelogNormalize deletes the row it emitted earlier and clears the key.`;
        case 'skip-new': return `Key ${ev.key} doesn't match the filter, and was never emitted, so nothing happens.`;
        case 'delete': return `A delete for key ${ev.key}. ChangelogNormalize emits it with the full row from state, then clears the key.`;
        case 'delete-missing': return `A delete for key ${ev.key}, which isn't in state. Nothing to retract.`;
        case 'wm': return `The watermark passes through.`;
      }
      return '';
    },
  };

  // ---------- Windows ----------
  //
  // Times are whole seconds, so "the watermark reaches end - 1 ms" is simply wm >= end.
  const hm = (sec) => U.fromSec(sec).slice(0, 5);
  const hms = (sec) => U.fromSec(sec);
  const fired = (st, end) => st.wm != null && U.toSec(st.wm) >= end;
  const timerList = (st) => [...new Set(Object.values(st.timers).map((t) => t.t))].sort((a, b) => a - b);
  const add = (a, b) => Math.round((a + b) * 100) / 100;

  // Window aggregation over TUMBLE, HOP and CUMULATE window TVFs, which Flink plans in two
  // phases: LocalWindowAggregate before the hash exchange, and GlobalWindowAggregate after it.
  // Both work in slices: a tumbling window is one slice; hopping and cumulating windows share
  // slices between overlapping windows. Mirrors Flink 2.3's LocalSlicingWindowAggOperator,
  // WindowAggOperator with SliceSharedSyncStateWindowAggProcessor and
  // SliceUnsharedSyncStateWindowAggProcessor, RecordsWindowBuffer, LocalAggCombiner,
  // GlobalAggCombiner, and the slice assigners in SliceAssigners.
  //   cfg: { kind: 'tumble' | 'hop' | 'cumulate', size, slide (hop), step (cumulate),
  //          key, keyLabel, timeField, sumField, countName, sumName, what(row) }   (all durations
  //          in seconds; without a sumField there's only the count, named countName, 'orders' by
  //          default; what(row) names a row in captions, 'Order <order_id>' by default)
  const sliceSize = (cfg) => (cfg.kind === 'hop' ? cfg.slide : cfg.kind === 'cumulate' ? cfg.step : cfg.size);
  const sliceUnit = (cfg) => (cfg.kind === 'tumble' ? 'window' : 'slice');
  const sliceLabel = (cfg, end) => `${hm(end - sliceSize(cfg))}–${hm(end)}`;
  // Both phases look at their buffer only when the watermark reaches the next slice boundary
  // (getNextTriggerWatermark); before the first watermark, any watermark will do.
  const nextBoundary = (cfg, w) => Math.floor(w / sliceSize(cfg)) * sliceSize(cfg) + sliceSize(cfg);
  // A retraction can make a partial result negative: −1 order, −$31.00.
  const signed = (n, text) => (n < 0 ? `−${text(-n)}` : text(n));
  const accText = (cfg, a) => (cfg.sumField
    ? `${signed(a.count, (n) => `${n} ${n === 1 ? 'order' : 'orders'}`)}, ${U.money(a.sum)}`
    : `${cfg.countName || 'orders'} ${signed(a.count, String)}`);
  const what = (cfg, row) => (cfg.what ? cfg.what(row) : `Order ${row.order_id}`);

  // LocalWindowAggregate: pre-aggregates before the shuffle, so only one partial result per key
  // and slice crosses the network. The partial results are kept in memory, not in Flink state:
  //   - every record, retractions included, is added to its key and slice. Lateness isn't
  //     decided here: a record for a fired window is added like any other
  //   - when the watermark reaches the next slice boundary, and a buffered slice's window has
  //     ended, every partial result is sent on (RecordsWindowBuffer.advanceProgress), then the
  //     watermark. A checkpoint sends them on too (prepareSnapshotPreBarrier)
  //   - a partial result is a plain insert: the key, the count (and sum), and $slice_end
  const localWindowAggregate = {
    init: () => ({ buffer: {}, wm: null, next: null }),
    record(st, item, cfg) {
      const row = item.row;
      const k = row[cfg.key];
      const size = sliceSize(cfg);
      const end = Math.floor(U.toSec(row[cfg.timeField]) / size) * size + size;
      const retract = item.kind === '-U' || item.kind === '-D';
      const key = `${k}|${end}`;
      const created = !st.buffer[key];
      const b = st.buffer[key] || (st.buffer[key] = { k, end, count: 0, sum: 0 });
      b.count += retract ? -1 : 1;
      const v = cfg.sumField ? row[cfg.sumField] : 0;
      b.sum = add(b.sum, retract ? -v : v);
      return {
        out: [], absorb: true, touched: key,
        ev: { type: retract ? 'retract' : created ? 'new' : 'add', row, key: k, slice: sliceLabel(cfg, end), acc: { ...b }, behind: fired(st, end), wm: st.wm },
      };
    },
    watermark(st, item, cfg) {
      const w = U.toSec(item.time);
      st.wm = item.time;
      let sent = [];
      const checked = st.next == null || w >= st.next;
      if (checked) {
        const buffered = Object.values(st.buffer).sort((a, b) => a.end - b.end || a.k - b.k);
        if (buffered.length && buffered[0].end <= w) {
          sent = buffered;
          st.buffer = {};
        }
        st.next = nextBoundary(cfg, w);
      }
      const out = sent.map((b) => rec('+I', {
        [cfg.key]: b.k, [cfg.countName || 'orders']: b.count,
        ...(cfg.sumField ? { [cfg.sumName || 'sales']: b.sum } : {}), slice_end: hms(b.end),
      }));
      return {
        out: [...out, wm(item.time)],
        ev: { type: sent.length ? 'flush' : 'wm', time: item.time, checked, next: hm(st.next), sent: sent.map((b) => ({ k: b.k, slice: sliceLabel(cfg, b.end), due: b.end <= w, acc: b })) },
      };
    },
    view(st, cfg) {
      const kl = cfg.keyLabel || cfg.key;
      return {
        title: 'Partial results, in memory',
        cols: [kl, sliceUnit(cfg), cfg.countName || 'orders', ...(cfg.sumField ? [cfg.sumName || 'sales'] : [])],
        rows: Object.entries(st.buffer)
          .sort(([, a], [, b]) => a.end - b.end || a.k - b.k)
          .map(([key, b]) => ({ key, cells: [b.k, sliceLabel(cfg, b.end), signed(b.count, String), ...(cfg.sumField ? [U.money(b.sum)] : [])] })),
        footer: `not state. Sent on at ${st.next == null ? 'the first watermark' : `watermark ${hm(st.next)}`}, if a window ended`,
      };
    },
    // No keyed state and no timers: the partial results are in memory.
    facts: () => ({ state: [], timers: [] }),
    say(ev, cfg) {
      const unit = sliceUnit(cfg);
      const kl = cfg.keyLabel || cfg.key;
      switch (ev.type) {
        case 'new':
        case 'add': {
          const base = ev.type === 'new'
            ? `${what(cfg, ev.row)} starts ${kl} ${ev.key}'s partial result for the ${ev.slice} ${unit}: ${accText(cfg, ev.acc)}.`
            : `${what(cfg, ev.row)} is added to ${kl} ${ev.key}'s partial result for the ${ev.slice} ${unit}: ${accText(cfg, ev.acc)}.`;
          if (ev.behind) return `${base} The watermark (${ev.wm}) is already past the end of this ${unit}, but LocalWindowAggregate doesn't decide what's late: GlobalWindowAggregate will.`;
          return ev.type === 'new' ? `${base} LocalWindowAggregate keeps it in memory, not in Flink state.` : base;
        }
        case 'retract': return `The retraction takes ${what(cfg, ev.row).toLowerCase()} back out of ${kl} ${ev.key}'s partial result for the ${ev.slice} ${unit}: ${accText(cfg, ev.acc)}.${ev.behind ? ` The watermark (${ev.wm}) is already past the end of this ${unit}, but LocalWindowAggregate doesn't decide what's late: GlobalWindowAggregate will.` : ''}`;
        case 'wm': return ev.checked
          ? `The watermark, ${ev.time}, hasn't reached the end of any buffered ${unit}, so the partial results stay in memory. LocalWindowAggregate forwards the watermark.`
          : `The watermark, ${ev.time}, hasn't reached the next ${unit} boundary, so LocalWindowAggregate doesn't look at its partial results. It forwards the watermark.`;
        case 'flush': {
          const parts = ev.sent.map((x) => `${kl} ${x.k}, ${x.slice}: ${accText(cfg, x.acc)}`);
          const early = ev.sent.filter((x) => !x.due).map((x) => x.slice);
          return `The watermark, ${ev.time}, has reached the end of a buffered ${unit}, so LocalWindowAggregate sends on every partial result it holds (${parts.join('; ')})${early.length ? `, even ${[...new Set(early)].join(' and ')}, which isn't due yet` : ''}. Then it forwards the watermark. One row per ${kl} and ${unit} crosses the network, however many records went into it.`;
        }
      }
      return '';
    },
  };

  // GlobalWindowAggregate: after the hash exchange, merges the partial results in keyed state,
  // per key and slice, and fires the windows:
  //   - partial results wait in memory until a watermark that reaches the next slice boundary
  //     (as in the local phase), which first merges them into state, before it fires any timer.
  //     Lateness is decided then, against the watermark before this one: a partial result for a
  //     fired slice whose last window has fired is late, and dropped. Merging a slice registers
  //     a timer at its first unfired window end
  //   - tumble: a slice is a window; it fires, and is freed
  //   - hop: a window merges the slices it spans, is emitted unless empty, schedules the next
  //     window if it wasn't empty, and frees its first slice
  //   - cumulate: each step merges its slice into the window's first slice, which is shared,
  //     and emits it; the next step is scheduled until the window reaches its maximum size
  //   - a window whose COUNT(*) is zero when it fires isn't emitted (WindowIsEmptySupplier)
  const globalWindowAggregate = {
    stateKind: 'wm',
    init: () => ({ pending: [], slices: {}, timers: {}, wm: null, next: null }),
    record(st, item, cfg) {
      const p = item.row;
      st.pending.push(p);
      const end = U.toSec(p.slice_end);
      return { out: [], absorb: true, touched: `P${p[cfg.key]}|${end}`, ev: { type: 'receive', row: p, key: p[cfg.key], slice: sliceLabel(cfg, end), acc: { count: p[cfg.countName || 'orders'], sum: p[cfg.sumName || 'sales'] } } };
    },
    watermark(st, item, cfg) {
      const now = U.toSec(item.time);
      const size = sliceSize(cfg);
      const winStart = (end) => (cfg.kind === 'cumulate' ? Math.floor((end - 1) / cfg.size) * cfg.size : end - cfg.size);
      // 1. Merge the partial results that have arrived, with the old watermark deciding lateness.
      const merged = [];
      const late = [];
      if (st.next == null || now >= st.next) {
        if (st.pending.length && Math.min(...st.pending.map((p) => U.toSec(p.slice_end))) <= now) {
          for (const p of st.pending) {
            const k = p[cfg.key];
            const end = U.toSec(p.slice_end);
            const lastWindowEnd = cfg.kind === 'hop' ? end - size + cfg.size : cfg.kind === 'cumulate' ? winStart(end) + cfg.size : end;
            const count = p[cfg.countName || 'orders'];
            const sum = cfg.sumField ? p[cfg.sumName || 'sales'] : 0;
            if (fired(st, end)) {
              if (fired(st, lastWindowEnd)) { late.push({ k, slice: sliceLabel(cfg, end), acc: { count, sum } }); continue; }
              throw new Error('A partial result for a fired slice of an unfired window: the example data should avoid this case.');
            }
            const key = `${k}|${end}`;
            const created = !st.slices[key];
            const s = st.slices[key] || (st.slices[key] = { k, start: end - size, end, count: 0, sum: 0 });
            s.count += count;
            s.sum = add(s.sum, sum);
            let timer = end;
            while (fired(st, timer)) timer += size;
            const newTimer = !st.timers[`${k}|${timer}`];
            st.timers[`${k}|${timer}`] = { k, t: timer };
            merged.push({ k, slice: sliceLabel(cfg, end), acc: { ...s }, created, timer: hm(timer), timerSec: timer, newTimer });
          }
          st.pending = [];
        }
        st.next = nextBoundary(cfg, now);
      }
      // 2. Fire the timers the watermark has reached.
      st.wm = item.time;
      const out = [];
      const firings = [];
      for (;;) {
        const due = Object.entries(st.timers).filter(([, t]) => t.t <= now).sort(([, a], [, b]) => a.t - b.t || a.k - b.k)[0];
        if (!due) break;
        const [tkey, { k, t: end }] = due;
        delete st.timers[tkey];
        const f = { k, end: hm(end), merged: [], freed: [], next: null };
        const label = (s) => `${hm(s.start)}–${hm(s.end)}`;
        let acc;
        if (cfg.kind === 'tumble') {
          f.start = hm(end - cfg.size);
          acc = st.slices[`${k}|${end}`];
          delete st.slices[`${k}|${end}`];
          if (acc) f.freed.push(label(acc));
          if (acc && acc.count === 0) { f.empty = true; acc = null; }
        } else if (cfg.kind === 'hop') {
          const start = end - cfg.size;
          f.start = hm(start);
          acc = { count: 0, sum: 0 };
          for (let e = start + cfg.slide; e <= end; e += cfg.slide) {
            const s = st.slices[`${k}|${e}`];
            if (s) { acc.count += s.count; acc.sum = add(acc.sum, s.sum); f.merged.push(label(s)); }
          }
          if (acc.count === 0) acc = null;
          else { st.timers[`${k}|${end + cfg.slide}`] = { k, t: end + cfg.slide }; f.next = hm(end + cfg.slide); }
          const first = st.slices[`${k}|${start + cfg.slide}`];
          if (first) { f.freed.push(label(first)); delete st.slices[`${k}|${start + cfg.slide}`]; }
        } else {
          const start = Math.floor((end - 1) / cfg.size) * cfg.size;
          const firstEnd = start + cfg.step;
          f.start = hm(start);
          if (end !== firstEnd) {
            const cur = st.slices[`${k}|${end}`];
            const first = st.slices[`${k}|${firstEnd}`] || (st.slices[`${k}|${firstEnd}`] = { k, start, end: firstEnd, count: 0, sum: 0 });
            if (cur) {
              first.count += cur.count;
              first.sum = add(first.sum, cur.sum);
              f.merged.push(label(cur));
              delete st.slices[`${k}|${end}`];
            }
            first.cover = end;
          }
          acc = st.slices[`${k}|${firstEnd}`];
          if (acc && acc.count === 0) acc = null;
          if (end + cfg.step <= start + cfg.size) { st.timers[`${k}|${end + cfg.step}`] = { k, t: end + cfg.step }; f.next = hm(end + cfg.step); }
          if (end === start + cfg.size) { delete st.slices[`${k}|${firstEnd}`]; f.freed.push(`${hm(start)}–${hm(end)}`); }
        }
        if (acc) {
          f.row = { window_start: f.start, window_end: f.end, [cfg.key]: k, [cfg.countName || 'orders']: acc.count, ...(cfg.sumField ? { [cfg.sumName || 'sales']: acc.sum } : {}) };
          out.push(rec('+I', f.row));
        }
        firings.push(f);
      }
      const type = firings.length ? 'fire' : merged.length || late.length ? 'merge' : 'wm';
      return { out: [...out, wm(item.time)], ev: { type, time: item.time, merged, late, firings, rows: out.map((o) => o.row) } };
    },
    view(st, cfg) {
      const kl = cfg.keyLabel || cfg.key;
      const unit = sliceUnit(cfg);
      return {
        title: `Accumulator per ${kl} and ${unit}`,
        cols: [kl, unit, cfg.countName || 'orders', ...(cfg.sumField ? [cfg.sumName || 'sales'] : [])],
        rows: [
          ...Object.entries(st.slices)
            .sort(([, a], [, b]) => a.start - b.start || a.k - b.k)
            .map(([key, s]) => ({ key, cells: [s.k, `${hm(s.start)}–${hm(s.cover || s.end)}`, signed(s.count, String), ...(cfg.sumField ? [U.money(s.sum)] : [])] })),
          ...st.pending.map((p) => {
            const end = U.toSec(p.slice_end);
            return { key: `P${p[cfg.key]}|${end}`, cells: [p[cfg.key], `${sliceLabel(cfg, end)} arrived`, signed(p[cfg.countName || 'orders'], String), ...(cfg.sumField ? [U.money(p[cfg.sumName || 'sales'])] : [])] };
          }),
        ],
        footer: `timers: ${timerList(st).map(hm).join(', ') || 'none'}`,
      };
    },
    // State is per key and slice (the namespace is the slice end); partial results that haven't
    // been merged yet are in memory. A window's timer is set for 1 ms before its end, so it fires
    // when the watermark reaches the end. The accumulator: COUNT(*), then SUM (with its own count
    // of rows, when it can retract).
    facts: (st, cfg, ctx) => ({
      state: Object.values(st.slices).map((s) => `${s.k}|${hms(s.end)}`),
      timers: Object.values(st.timers).map((t) => `${t.k}@${ms(t.t * 1000 - 1)}`),
      values: {
        'window-aggs': values(Object.values(st.slices).map((s) => [`${s.k}|${hms(s.end)}`,
          F.tuple(String(s.count), ...(cfg.sumField ? [F.dec(s.sum), ...(ctx.retract ? [String(s.count)] : [])] : []))])),
      },
    }),
    // What the watermark merges into state before it fires anything, for captions.
    mergeText(ev, cfg) {
      const kl = cfg.keyLabel || cfg.key;
      const parts = [];
      if (ev.merged.length) {
        parts.push(`GlobalWindowAggregate first merges the partial results that arrived into its state (${ev.merged.map((m) => `${kl} ${m.k}, ${m.slice}: ${m.created ? '' : 'now '}${accText(cfg, m.acc)}`).join('; ')}).`);
        // Name the timers the merge sets, and say which are due at once: those are set and fired
        // while this one watermark is handled, so the panel never shows them.
        const secs = [...new Set(ev.merged.filter((m) => m.newTimer).map((m) => m.timerSec))].sort((a, b) => a - b);
        const and = (xs) => (xs.length < 2 ? xs.join('') : `${xs.slice(0, -1).join(', ')}${xs.length > 2 ? ',' : ''} and ${xs.at(-1)}`);
        if (secs.length) {
          parts.push(`It sets ${secs.length === 1 ? 'a timer' : 'timers'} for ${and(secs.map(hm))}.`);
          const due = secs.filter((t) => t <= U.toSec(ev.time)).map(hm);
          if (due.length) parts.push(`${due.length === 1 ? `The ${due[0]} timer is` : `The ${and(due)} timers are`} due at once, since the watermark has reached ${due.length === 1 ? 'it' : 'them'}.`);
        }
      }
      for (const l of ev.late) {
        parts.push(`${kl[0].toUpperCase()}${kl.slice(1)} ${l.k}'s partial result for ${l.slice} (${accText(cfg, l.acc)}) is late: that window fired at an earlier watermark, and its state is gone, so it's dropped.`);
      }
      return parts.join(' ');
    },
    say(ev, cfg) {
      const unit = sliceUnit(cfg);
      const kl = cfg.keyLabel || cfg.key;
      switch (ev.type) {
        case 'receive': return `${kl[0].toUpperCase()}${kl.slice(1)} ${ev.key}'s partial result for the ${ev.slice} ${unit} arrives (${accText(cfg, ev.acc)}). GlobalWindowAggregate holds it in memory until the watermark that follows.`;
        case 'merge': return `The watermark, ${ev.time}, arrives. ${this.mergeText(ev, cfg)} No timer is due yet, so nothing fires, and the watermark is forwarded.`;
        case 'fire': {
          const rows = ev.rows.map((x) => `${kl} ${x[cfg.key]}, ${accText(cfg, { count: x[cfg.countName || 'orders'], sum: x[cfg.sumName || 'sales'] })}`).join('; ');
          return `The watermark, ${ev.time}, arrives. ${this.mergeText(ev, cfg)} Then the window ending ${ev.firings[0].end} is complete: GlobalWindowAggregate emits ${rows || 'nothing, since it\'s empty'}, frees its state, and forwards the watermark.`;
        }
        case 'wm': return `The watermark, ${ev.time}, hasn't reached any timer, so nothing fires. GlobalWindowAggregate forwards it.`;
      }
      return '';
    },
  };

  // Session window aggregation (SESSION window TVF): one WindowAggregate, not two-phase.
  // Each record opens a window [t, t + gap), which merges with every session of the same key
  // that it overlaps. A session fires when the watermark passes its end, which is the last
  // record plus the gap. Mirrors UnsliceSyncStateWindowAggProcessor with a merging window set.
  //   cfg: { gap (seconds), key, keyLabel, timeField, sumField }
  const sessionWindowAggregate = {
    stateKind: 'wm',
    init: () => ({ sessions: [], wm: null }),
    record(st, item, cfg) {
      const row = item.row;
      const k = row[cfg.key];
      const ts = U.toSec(row[cfg.timeField]);
      const win = { start: ts, end: ts + cfg.gap };
      const overlapping = st.sessions.filter((s) => s.k === k && s.start <= win.end && s.end >= win.start);
      const merged = {
        k,
        start: Math.min(win.start, ...overlapping.map((s) => s.start)),
        end: Math.max(win.end, ...overlapping.map((s) => s.end)),
        count: 1 + overlapping.reduce((a, s) => a + s.count, 0),
        sum: overlapping.reduce((a, s) => add(a, s.sum), row[cfg.sumField]),
      };
      if (fired(st, merged.end)) return { out: [], ev: { type: 'late', row, wm: st.wm } };
      st.sessions = st.sessions.filter((s) => !overlapping.includes(s));
      st.sessions.push(merged);
      st.sessions.sort((a, b) => a.start - b.start || a.k - b.k);
      const key = `${k}|${merged.start}`;
      const fmt = (s) => `${hms(s.start)}–${hms(s.end)}`;
      return {
        out: [], absorb: true, touched: key,
        ev: { type: overlapping.length ? 'merge' : 'new-session', row, own: fmt(win), was: overlapping.map(fmt), now: fmt(merged), acc: { ...merged } },
      };
    },
    watermark(st, item, cfg) {
      st.wm = item.time;
      const now = U.toSec(item.time);
      const done = st.sessions.filter((s) => s.end <= now).sort((a, b) => a.end - b.end || a.k - b.k);
      st.sessions = st.sessions.filter((s) => !done.includes(s));
      const out = done.map((s) => rec('+I', { window_start: hms(s.start), window_end: hms(s.end), [cfg.key]: s.k, orders: s.count, sales: s.sum }));
      return { out: [...out, wm(item.time)], ev: { type: done.length ? 'fire' : 'wm', time: item.time, rows: out.map((o) => o.row) } };
    },
    view(st, cfg) {
      const kl = cfg.keyLabel || cfg.key;
      return {
        title: `Session per ${kl}`,
        cols: [kl, 'session', 'orders', 'sales'],
        rows: st.sessions.map((s) => ({ key: `${s.k}|${s.start}`, cells: [s.k, `${hms(s.start)}–${hms(s.end)}`, s.count, U.money(s.sum)] })),
        footer: `timers: ${[...new Set(st.sessions.map((s) => s.end))].sort((a, b) => a - b).map(hms).join(', ') || 'none'}`,
      };
    },
    // One entry per key and session (Flink keeps a map from each merged session to the window
    // that holds its accumulator; see StateCheck.state), and a session's timer is set for 1 ms
    // before its end.
    // Values aren't compared: Flink keeps each accumulator under the window the session started
    // as, which the simulator doesn't track. The output checks the counts and sums.
    facts: (st) => ({
      state: st.sessions.map((s) => `${s.k}|${hms(s.start)}–${hms(s.end)}`),
      timers: st.sessions.map((s) => `${s.k}@${ms(s.end * 1000 - 1)}`),
    }),
    say(ev) {
      switch (ev.type) {
        case 'new-session': return `The record starts a new session, ${ev.now}.`;
        case 'merge': return `The record's window overlaps an existing session, so they merge into ${ev.now}.`;
        case 'late': return `The record's own time plus the gap is already behind the watermark, so it's late, and dropped.`;
        case 'fire': return `The watermark passes the end of a session, which fires.`;
        case 'wm': return `No session has ended yet, so nothing fires. The watermark is forwarded.`;
      }
      return '';
    },
  };

  // ---------- OVER aggregation ----------
  //
  // OVER (PARTITION BY key ORDER BY event time RANGE BETWEEN <preceding> PRECEDING AND CURRENT
  // ROW). Mirrors RowTimeRangeBoundedPrecedingFunction in Flink 2.3, per key:
  //   - a row newer than the last timestamp this key has fired is buffered under its event time,
  //     with a timer at that time; any other row is late, and dropped (whatever the watermark)
  //   - a clean-up timer is (re)registered at newest time + 1.5 x preceding (+1 ms) when the
  //     current one is earlier than newest time + preceding (+1 ms)
  //   - when the watermark reaches a timer: if it's at or past the clean-up time, all of the key's
  //     state is cleared. Otherwise, rows more than <preceding> older than the timer are retracted
  //     from the accumulator, the rows at the timer's time are added, and each is emitted with the
  //     result; the retracted rows leave state, and the timer's time becomes the last fired time
  //     (also when a stale clean-up timer fires with no rows at its time)
  // Timer times are kept in seconds; a clean-up timer's extra millisecond is kept as +0.001.
  //   cfg: { key, keyLabel, timeField, preceding (seconds), sumField, idField, countName, sumName }
  const overAggregate = {
    stateKind: 'wm',
    init: () => ({ keys: {}, wm: null }),
    record(st, item, cfg) {
      const row = item.row;
      const k = row[cfg.key];
      const ts = U.toSec(row[cfg.timeField]);
      const s = st.keys[k] || (st.keys[k] = { rows: {}, count: 0, sum: 0, last: null, cleanup: null, timers: [] });
      if (s.last != null && ts <= s.last) return { out: [], ev: { type: 'late', key: k, row, last: hms(Math.floor(s.last)) } };
      const fresh = !s.rows[ts];
      (s.rows[ts] ||= []).push(row);
      if (fresh && !s.timers.includes(ts)) s.timers.push(ts);
      let cleanup = null;
      if (s.cleanup == null || s.cleanup < ts + cfg.preceding + 0.001) {
        s.cleanup = ts + cfg.preceding * 1.5 + 0.001;
        s.timers.push(s.cleanup);
        cleanup = hms(Math.floor(s.cleanup));
      }
      return { out: [], absorb: true, touched: `${k}|${row[cfg.idField]}`, ev: { type: 'buffer', key: k, row, timer: hms(ts), newTimer: fresh, cleanup, wm: st.wm, last: s.last != null ? hms(Math.floor(s.last)) : null } };
    },
    watermark(st, item, cfg) {
      st.wm = item.time;
      const now = U.toSec(item.time);
      const out = [];
      const firings = [];
      for (;;) {
        const due = Object.entries(st.keys).flatMap(([k, s]) => s.timers.filter((t) => t <= now).map((t) => ({ k, t })))
          .sort((a, b) => a.t - b.t || a.k - b.k)[0];
        if (!due) break;
        const s = st.keys[due.k];
        s.timers = s.timers.filter((t) => t !== due.t);
        const f = { k: Number(due.k), t: hms(Math.floor(due.t)), rows: [], retracted: [] };
        if (s.cleanup != null && s.cleanup <= due.t) {
          f.type = 'cleanup';
          delete st.keys[due.k];
          firings.push(f);
          continue;
        }
        const inputs = s.rows[due.t];
        if (inputs) {
          for (const [tsKey, rows] of Object.entries(s.rows)) {
            if (due.t - Number(tsKey) > cfg.preceding) {
              for (const r of rows) { s.count -= 1; s.sum = add(s.sum, -r[cfg.sumField]); f.retracted.push(r); }
              delete s.rows[tsKey];
            }
          }
          for (const r of inputs) { s.count += 1; s.sum = add(s.sum, r[cfg.sumField]); }
          for (const r of inputs) {
            const o = { ...r, [cfg.countName]: s.count, [cfg.sumName]: s.sum };
            f.rows.push(o);
            out.push(rec('+I', o));
          }
          f.type = 'emit';
        } else {
          f.type = 'stale';
        }
        s.last = due.t;
        firings.push(f);
      }
      return { out: [...out, wm(item.time)], ev: { type: firings.length ? 'fire' : 'wm', time: item.time, firings, rows: out.map((o) => o.row) } };
    },
    view(st, cfg) {
      const kl = cfg.keyLabel || cfg.key;
      const entries = Object.entries(st.keys).sort(([a], [b]) => a - b);
      const rows = entries.flatMap(([k, s]) => Object.entries(s.rows)
        .sort(([a], [b]) => a - b)
        .flatMap(([ts, rs]) => rs.map((r) => ({
          key: `${k}|${r[cfg.idField]}`,
          cells: [k, r[cfg.idField], r[cfg.timeField], U.money(r[cfg.sumField]), s.last != null && Number(ts) <= s.last ? 'in sum' : 'waiting'],
        }))));
      const sums = entries.filter(([, s]) => s.last != null)
        .map(([k, s]) => `${kl} ${k}, ${s.count} order${s.count === 1 ? '' : 's'}, ${U.money(s.sum)}`);
      const timers = entries.flatMap(([, s]) => s.timers.filter((t) => Number.isInteger(t))).sort((a, b) => a - b).map(hms);
      return {
        title: `Rows per ${kl}, within the range`,
        cols: [kl, cfg.idField, cfg.timeField, cfg.sumField, ''],
        rows,
        footer: `sums: ${sums.join('; ') || 'none yet'}. timers: ${[...new Set(timers)].join(', ') || 'none'}`,
      };
    },
    // Timers are at a row's event time, and the clean-up timer 1 ms after newest time + 1.5 x preceding.
    // The accumulator (COUNT(*), COUNT(total), $SUM0(total)) and the last fired time exist once a
    // key has fired; the rows, by event time, and the clean-up time from its first row.
    facts: (st) => {
      const keys = Object.entries(st.keys);
      const fired = keys.filter(([, s]) => s.last != null);
      return {
        state: Object.keys(st.keys),
        timers: keys.flatMap(([k, s]) => s.timers.map((t) => `${k}@${ms(Math.round(t * 1000))}`)),
        values: {
          accState: values(fired.map(([k, s]) => [k, F.tuple(String(s.count), String(s.count), F.dec(s.sum))])),
          lastTriggeringTsState: values(fired.map(([k, s]) => [k, F.time(s.last)])),
          cleanupTsState: values(keys.map(([k, s]) => [k, F.time(s.cleanup)])),
          inputState: values(keys.map(([k, s]) => [k, F.map(Object.entries(s.rows).map(([t, rows]) => [F.time(Number(t)), rows.map((r) => F.row(r))]))])),
        },
      };
    },
    say(ev) {
      switch (ev.type) {
        case 'buffer': return `The row is buffered under its event time, ${ev.timer}, until the watermark reaches it.`;
        case 'late': return `The row is no newer than the last time this key has emitted, ${ev.last}, so it's late, and dropped.`;
        case 'fire': return `The watermark reaches a buffered row's time. Its result is computed and emitted.`;
        case 'wm': return `The watermark hasn't reached any buffered row, so nothing is emitted. It's forwarded.`;
      }
      return '';
    },
  };

  // ---------- Deduplication ----------

  // ROW_NUMBER() ... ORDER BY proc_time ASC = 1: keep the first row to arrive for each key.
  // Mirrors ProcTimeDeduplicateKeepFirstRowFunction: a "seen" flag per key, and nothing else.
  const procTimeDedup = {
    init: () => ({ seen: {} }),
    record(st, item, cfg) {
      const k = item.row[cfg.key];
      if (st.seen[k]) return { out: [], touched: k, ev: { type: 'duplicate', key: k, row: item.row } };
      st.seen[k] = true;
      return { out: [rec('+I', item.row)], touched: k, ev: { type: 'first', key: k, row: item.row } };
    },
    watermark: (st, item) => ({ out: [wm(item.time)], ev: { type: 'wm', time: item.time } }),
    view(st, cfg) {
      return { title: `Seen, per ${cfg.key}`, cols: [cfg.key, 'seen'], rows: Object.keys(st.seen).map((k) => ({ key: k, cells: [k, 'yes'] })) };
    },
    facts: (st) => ({
      state: Object.keys(st.seen), timers: [],
      values: { 'deduplicate-state': values(Object.keys(st.seen).map((k) => [k, 'true'])) },
    }),
    say(ev) {
      if (ev.type === 'wm') return 'The watermark passes through. Processing-time deduplication has no use for it.';
      if (ev.type === 'duplicate') return `Key ${ev.key} has been seen before, so this copy is dropped.`;
      return `First copy of key ${ev.key}. It's emitted at once, and the key is marked as seen.`;
    },
  };

  // ROW_NUMBER() ... ORDER BY event_time ASC = 1: keep the earliest row by event time.
  // Mirrors RowTimeDeduplicateKeepFirstRowFunction (Flink 2.3, insert-only output):
  //   - a row older than the current watermark is late, and dropped
  //   - once a key has been emitted, later rows for it are ignored
  //   - otherwise the earliest row so far waits in state, with a timer at its event time;
  //     an earlier row replaces it (a tie keeps the one that arrived first)
  //   - when the watermark reaches the timer, the waiting row is emitted, and the key is
  //     marked as emitted
  const rowTimeDedup = {
    stateKind: { waiting: 'wm' }, stateParts: ['waiting', 'seen'],
    init: () => ({ keys: {}, wm: null }),
    record(st, item, cfg) {
      const row = item.row;
      const k = row[cfg.key];
      const ts = U.toSec(row[cfg.timeField]);
      if (st.wm != null && ts < U.toSec(st.wm)) return { out: [], ev: { type: 'late', key: k, row, wm: st.wm } };
      const s = st.keys[k];
      if (s && s.emitted) return { out: [], touched: k, ev: { type: 'already-emitted', key: k, row } };
      if (s && s.waiting && U.toSec(s.waiting[cfg.timeField]) <= ts) {
        return { out: [], touched: k, ev: { type: 'not-earlier', key: k, row, waiting: s.waiting } };
      }
      const replaced = s?.waiting || null;
      st.keys[k] = { waiting: row, emitted: false };
      return { out: [], absorb: true, touched: k, ev: { type: replaced ? 'replace' : 'wait', key: k, row, replaced } };
    },
    watermark(st, item, cfg) {
      st.wm = item.time;
      const now = U.toSec(item.time);
      const due = Object.entries(st.keys)
        .filter(([, s]) => s.waiting && U.toSec(s.waiting[cfg.timeField]) <= now)
        .sort(([, a], [, b]) => U.toSec(a.waiting[cfg.timeField]) - U.toSec(b.waiting[cfg.timeField]));
      const out = due.map(([k, s]) => {
        const r = s.waiting;
        st.keys[k] = { waiting: null, emitted: true };
        return rec('+I', r);
      });
      return { out: [...out, wm(item.time)], ev: { type: out.length ? 'fire' : 'wm', time: item.time, rows: out.map((o) => o.row) } };
    },
    view(st, cfg) {
      return {
        title: `Per ${cfg.key}`,
        cols: [cfg.key, 'waiting', 'emitted'],
        rows: Object.entries(st.keys).map(([k, s]) => ({ key: k, part: s.emitted ? 'seen' : 'waiting', cells: [k, s.waiting ? s.waiting[cfg.timeField] : '–', s.emitted ? 'yes' : 'no'] })),
        footer: `timers: ${Object.values(st.keys).filter((s) => s.waiting).map((s) => s.waiting[cfg.timeField]).sort().join(', ') || 'none'}`,
      };
    },
    facts: (st, cfg) => ({
      state: Object.keys(st.keys),
      timers: Object.entries(st.keys).filter(([, s]) => s.waiting).map(([k, s]) => `${k}@${ms(U.toSec(s.waiting[cfg.timeField]) * 1000)}`),
      values: {
        'waiting-to-emit-on-timer': values(Object.entries(st.keys).filter(([, s]) => s.waiting).map(([k, s]) => [k, F.row(s.waiting)])),
        'already-emitted-state-boolean': values(Object.entries(st.keys).filter(([, s]) => s.emitted).map(([k]) => [k, 'true'])),
      },
    }),
    say(ev) {
      switch (ev.type) {
        case 'late': return `The row is older than the watermark, so it's late, and dropped.`;
        case 'already-emitted': return `Key ${ev.key} has already been emitted, so this row is ignored.`;
        case 'not-earlier': return `Key ${ev.key} already has an earlier row waiting, so this one is ignored.`;
        case 'replace': return `This row is earlier than the one waiting for key ${ev.key}, so it replaces it.`;
        case 'wait': return `First row for key ${ev.key}. It waits in state, with a timer at its event time.`;
        case 'fire': return `The watermark reaches waiting rows, which are emitted.`;
        case 'wm': return `The watermark hasn't reached any waiting row. It's forwarded.`;
      }
      return '';
    },
  };

  // Rank with RetractStrategy: ROW_NUMBER() ... = 1 over an updating input, which the planner
  // can't turn into a Deduplicate. Mirrors RetractableTopNFunction in Flink 2.3, on its path for a
  // top-1 without the row number in the output (emitRecordsWithoutRowNumber and
  // retractRecordWithoutRowNumber). Every row of each partition is kept, sorted, because a
  // retraction of the first row has to promote the next:
  //   - a row that sorts first emits -D for the row it displaces (if any), then +I for itself
  //   - a row that doesn't sort first is stored, and nothing is emitted
  //   - retracting the first row emits -D for it, then +I for the next row (if any)
  //   - retracting any other row only removes it from state
  // Rows with equal sort keys keep their arrival order. Watermarks pass through unused.
  //   cfg: { key, keyLabel, sortField, desc, idField, stateFields, stateFormat }
  const retractRank = {
    stateKind: 'rows',
    init: () => ({ parts: {} }),
    record(st, item, cfg) {
      const row = item.row;
      const k = row[cfg.key];
      const rows = st.parts[k] || (st.parts[k] = []);
      const cmp = (a, b) => (U.toSec(a[cfg.sortField]) - U.toSec(b[cfg.sortField])) * (cfg.desc ? -1 : 1);
      const rowKey = (r) => `${k}|${r[cfg.idField]}`;
      if (item.kind === '+I' || item.kind === '+U') {
        const prev = rows[0];
        let i = rows.findIndex((r) => cmp(row, r) < 0);
        if (i < 0) i = rows.length;
        rows.splice(i, 0, row);
        const ev = { key: k, row, prev, rank: i + 1, size: rows.length };
        if (i > 0) return { out: [], absorb: true, touched: rowKey(row), ev: { ...ev, type: 'store' } };
        return { out: prev ? [rec('-D', prev), rec('+I', row)] : [rec('+I', row)], absorb: true, touched: rowKey(row), ev: { ...ev, type: prev ? 'displace' : 'first' } };
      }
      const i = rows.findIndex((r) => same(r, row));
      if (i < 0) return { out: [], absorb: true, ev: { type: 'retract-missing', key: k, row } };
      rows.splice(i, 1);
      const next = rows[0];
      if (!rows.length) delete st.parts[k];
      const ev = { key: k, row, next, rank: i + 1, size: rows.length };
      if (i > 0) return { out: [], absorb: true, ev: { ...ev, type: 'remove' } };
      return { out: next ? [rec('-D', row), rec('+I', next)] : [rec('-D', row)], absorb: true, touched: next && rowKey(next), ev: { ...ev, type: next ? 'promote' : 'empty' } };
    },
    watermark: (st, item) => ({ out: [wm(item.time)], ev: { type: 'wm', time: item.time } }),
    view(st, cfg) {
      const fields = cfg.stateFields;
      return {
        title: `Every row per ${cfg.keyLabel || cfg.key}, sorted by ${cfg.sortField}`,
        cols: [cfg.keyLabel || cfg.key, 'rank', ...fields],
        rows: Object.entries(st.parts).sort(([a], [b]) => a - b).flatMap(([k, rows]) => rows.map((r, i) => ({
          key: `${k}|${r[cfg.idField]}`,
          cells: [k, i + 1, ...fields.map((f) => (cfg.stateFormat?.[f] ? cfg.stateFormat[f](r[f]) : r[f]))],
        }))),
      };
    },
    // Every row, by sort key, in arrival order, and how many rows each sort key has.
    facts: (st, cfg) => ({
      state: Object.keys(st.parts), timers: [],
      values: {
        'data-state': values(Object.entries(st.parts).map(([k, rows]) => [k,
          F.map(F.group(rows, (r) => r[cfg.sortField]).map(([t, rs]) => [F.tuple(t), rs.map((r) => F.row(r))]))])),
        'sorted-map': values(Object.entries(st.parts).map(([k, rows]) => [k,
          F.map(F.group(rows, (r) => r[cfg.sortField]).map(([t, rs]) => [F.tuple(t), String(rs.length)]))])),
      },
    }),
    say(ev) {
      switch (ev.type) {
        case 'first': return `The first row for key ${ev.key}. Rank stores it and emits it.`;
        case 'displace': return `The row sorts ahead of the current first row for key ${ev.key}, which is deleted, and the new one is inserted.`;
        case 'store': return `The row doesn't sort first for key ${ev.key}. Rank stores it anyway, in case it's needed later, and emits nothing.`;
        case 'promote': return `The first row for key ${ev.key} is retracted. Rank deletes it, and inserts the next row in its place.`;
        case 'empty': return `The only row for key ${ev.key} is retracted. Rank deletes it, and the key is gone.`;
        case 'remove': return `A row that isn't first for key ${ev.key} is retracted. Rank removes it from state, and emits nothing.`;
        case 'wm': return `The watermark passes through. Rank doesn't use it.`;
      }
      return '';
    },
  };

  // Rank with AppendFastStrategy, as a top 1 not ordered by processing time: ROW_NUMBER() ... = 1
  // over an append-only input, ordered by something other than a time attribute, so the planner
  // can't turn it into a Deduplicate. Mirrors FastTop1Function (FastTop1Helper) in Flink 2.3:
  //   - the first row for a key is stored, whole, and emitted as +I
  //   - a later row replaces it only if its sort key is strictly ahead: -U (if downstream needs
  //     it), then +U; otherwise it's ignored
  // Watermarks pass through unused.
  //   cfg: { key, sortField, desc, generateUpdateBefore, stateFields, stateFormat }
  const fastTop1 = {
    init: () => ({ rows: {} }),
    record(st, item, cfg) {
      const row = item.row;
      const k = row[cfg.key];
      const prev = st.rows[k];
      if (!prev) {
        st.rows[k] = row;
        return { out: [rec('+I', row)], touched: k, ev: { type: 'first', key: k, row } };
      }
      const a = row[cfg.sortField], b = prev[cfg.sortField];
      const ahead = cfg.desc ? a > b : a < b;
      if (!ahead) return { out: [], touched: k, ev: { type: 'not-ahead', key: k, row, prev } };
      st.rows[k] = row;
      const out = cfg.generateUpdateBefore ? [rec('-U', prev), rec('+U', row)] : [rec('+U', row)];
      return { out, touched: k, ev: { type: 'replace', key: k, row, prev } };
    },
    watermark: (st, item) => ({ out: [wm(item.time)], ev: { type: 'wm', time: item.time } }),
    view(st, cfg) {
      const fields = cfg.stateFields;
      return {
        title: `First row per ${cfg.key}`,
        cols: fields,
        rows: Object.entries(st.rows).map(([k, r]) => ({ key: k, cells: fields.map((f) => (cfg.stateFormat?.[f] ? cfg.stateFormat[f](r[f]) : r[f])) })),
      };
    },
    facts: (st) => ({
      state: Object.keys(st.rows), timers: [],
      values: { 'Top1-Rank-State': values(Object.entries(st.rows).map(([k, r]) => [k, F.row(r)])) },
    }),
    say(ev, cfg) {
      switch (ev.type) {
        case 'first': return `The first row for key ${ev.key}. Rank stores the whole row and emits it.`;
        case 'not-ahead': return `Key ${ev.key} already has a row, and this one's ${cfg.sortField} doesn't sort ahead of it, so it's ignored.`;
        case 'replace': return `This row sorts ahead of the stored row for key ${ev.key}, so it replaces it, as an update.`;
        case 'wm': return `The watermark passes through. Rank doesn't use it.`;
      }
      return '';
    },
  };

  // Non-windowed GROUP BY (GroupAggregate). One accumulator per key, kept forever unless
  // state TTL is set. Mirrors GroupAggHelper.processElement in Flink 2.3:
  //   - the first row for a key emits +I
  //   - a change emits +U, preceded by -U (the old result) only if downstream needs it
  //   - an unchanged result emits nothing (when state TTL is off)
  //   - when the record count drops to zero, -D (the old result), and the key is cleared
  //   cfg: { key, keyLabel, aggs: [{ name, fn: 'count' | 'sum', field }], generateUpdateBefore }
  const groupAggregate = {
    init: () => ({ acc: {} }),
    record(st, item, cfg) {
      // A composite GROUP BY (cfg.keys) is shown as one key: the fields joined by a space.
      const k = cfg.keys ? cfg.keys.map((f) => item.row[f]).join(' ') : item.row[cfg.key];
      const accumulate = item.kind === '+I' || item.kind === '+U';
      let a = st.acc[k];
      const first = !a;
      if (first && !accumulate) return { out: [], absorb: true, ev: { type: 'retract-missing', key: k } };
      if (first) a = { n: 0, vals: Object.fromEntries(cfg.aggs.map((g) => [g.name, 0])), keyRow: cfg.keys ? pick(item.row, cfg.keys) : null };
      const keyFields = cfg.keys ? pick(item.row, cfg.keys) : { [cfg.key]: k };
      const value = (vals) => ({ ...keyFields, ...vals });
      const prev = value({ ...a.vals });
      const sign = accumulate ? 1 : -1;
      a.n += sign;
      for (const g of cfg.aggs) {
        const delta = g.fn === 'count' ? 1 : item.row[g.field];
        a.vals[g.name] = Math.round((a.vals[g.name] + sign * delta) * 100) / 100;
      }
      const next = value({ ...a.vals });
      const ev = { key: k, accumulate, prev, next, in: item.row };
      if (a.n === 0) {
        delete st.acc[k];
        return { out: first ? [] : [rec('-D', prev)], absorb: true, ev: { ...ev, type: 'delete' } };
      }
      st.acc[k] = a;
      if (first) return { out: [rec('+I', next)], absorb: true, touched: k, ev: { ...ev, type: 'first' } };
      if (same(prev, next)) return { out: [], absorb: true, touched: k, ev: { ...ev, type: 'unchanged' } };
      const out = cfg.generateUpdateBefore ? [rec('-U', prev), rec('+U', next)] : [rec('+U', next)];
      return { out, absorb: true, touched: k, ev: { ...ev, type: 'update' } };
    },
    watermark: (st, item) => ({ out: [wm(item.time)], ev: { type: 'wm', time: item.time } }),
    view(st, cfg) {
      return {
        title: `Accumulator per ${cfg.keyLabel || cfg.key}`,
        cols: [cfg.keyLabel || cfg.key, ...cfg.aggs.map((g) => g.name)],
        rows: Object.entries(st.acc).map(([k, a]) => ({
          key: k, cells: [cfg.keyCell && a.keyRow ? cfg.keyCell(a.keyRow) : k, ...cfg.aggs.map((g) => (g.fn === 'sum' ? U.money(a.vals[g.name]) : a.vals[g.name]))],
        })),
      };
    },
    // One field per aggregate: COUNT(*) a count; SUM a sum, and a count of rows when it can retract.
    facts: (st, cfg, ctx) => ({
      state: Object.keys(st.acc), timers: [],
      values: {
        accState: values(Object.entries(st.acc).map(([k, a]) => [k, F.tuple(...cfg.aggs.flatMap((g) => (g.fn === 'count'
          ? [String(a.vals[g.name])]
          : [F.dec(a.vals[g.name]), ...(ctx.retract ? [String(a.n)] : [])])))])),
      },
    }),
    say(ev) {
      switch (ev.type) {
        case 'first': return `First row for key ${ev.key}. GroupAggregate creates an accumulator and emits an insert.`;
        case 'update': return `The accumulator for key ${ev.key} changes, so GroupAggregate emits the new result.`;
        case 'unchanged': return `The result for key ${ev.key} didn't change, so nothing is emitted.`;
        case 'delete': return `Key ${ev.key} has no rows left, so GroupAggregate emits a delete and clears its state.`;
        case 'retract-missing': return `A retraction for key ${ev.key}, which has no state. It's ignored.`;
      }
      return '';
    },
  };

  // ---------- Joins ----------

  // The combined watermark of a two-input operator: the minimum of both inputs, and nothing
  // until both have sent one.
  const combined = (wms) => (wms.left != null && wms.right != null ? (U.toSec(wms.left) < U.toSec(wms.right) ? wms.left : wms.right) : null);
  const later = (a, b) => b == null || (a != null && U.toSec(a) > U.toSec(b));

  // Interval join (IntervalJoin, RowTimeIntervalJoin in Flink 2.3), as a LEFT OUTER join:
  // right.time - left.time must lie in [lower, upper]. Both sides are buffered by key. A left
  // row is buffered while the combined watermark W is below its time + upper, a right row while
  // W is below its time - lower. When W passes those bounds the rows are removed, and a left row
  // that never matched is emitted null-padded. With the default min-cleanup-interval of 0, that
  // happens as soon as W > time + upper. The output watermark is held back by the larger bound.
  //   cfg: { left, key, leftTime, rightTime, lower, upper (seconds), joinRow(l, r), padRow(l),
  //          leftName, rightName }
  const intervalJoin = {
    stateKind: 'wm',
    init: () => ({ left: [], right: [], wms: { left: null, right: null }, W: null, outWm: null }),
    record(st, item, cfg, ctx) {
      const row = item.row;
      const k = row[cfg.key];
      const W = st.W == null ? null : U.toSec(st.W);
      if (ctx.from === cfg.left) {
        const t = U.toSec(row[cfg.leftTime]);
        const matches = st.right.filter((r) => r.row[cfg.key] === k && r.t - t >= cfg.lower && r.t - t <= cfg.upper);
        const out = matches.map((r) => rec('+I', cfg.joinRow(row, r.row)));
        if (W == null || W < t + cfg.upper) {
          st.left.push({ row, t, joined: matches.length > 0 });
          return { out, absorb: !out.length, touched: `L${k}`, ev: { type: 'left', row, matches: matches.map((m) => m.row), buffered: true } };
        }
        if (!matches.length) out.push(rec('+I', cfg.padRow(row)));
        return { out, ev: { type: 'left', row, matches: matches.map((m) => m.row), buffered: false } };
      }
      const t = U.toSec(row[cfg.rightTime]);
      const matches = st.left.filter((l) => l.row[cfg.key] === k && t - l.t >= cfg.lower && t - l.t <= cfg.upper);
      matches.forEach((l) => { l.joined = true; });
      const out = matches.map((l) => rec('+I', cfg.joinRow(l.row, row)));
      const buffered = W == null || W < t - cfg.lower;
      if (buffered) st.right.push({ row, t });
      return { out, absorb: buffered && !out.length, touched: `R${k}`, ev: { type: 'right', row, matches: matches.map((m) => m.row), buffered } };
    },
    watermark(st, item, cfg, ctx) {
      const side = ctx.from === cfg.left ? 'left' : 'right';
      st.wms[side] = item.time;
      const W = combined(st.wms);
      if (!W || !later(W, st.W)) return { out: [], nodeWm: st.W, ev: { type: 'hold', side, time: item.time, W: st.W, wms: { ...st.wms } } };
      st.W = W;
      const w = U.toSec(W);
      const expired = st.left.filter((l) => l.t + cfg.upper < w);
      const padded = expired.filter((l) => !l.joined);
      st.left = st.left.filter((l) => !expired.includes(l));
      const freedRight = st.right.filter((r) => r.t - cfg.lower < w);
      st.right = st.right.filter((r) => !freedRight.includes(r));
      const out = padded.map((l) => rec('+I', cfg.padRow(l.row)));
      const outWm = U.fromSec(w - Math.max(cfg.upper, -cfg.lower));
      if (later(outWm, st.outWm)) { st.outWm = outWm; out.push(wm(outWm)); }
      return {
        out, nodeWm: W,
        ev: { type: 'advance', side, time: item.time, W, outWm, padded: padded.map((l) => l.row), expired: expired.map((l) => l.row), freedRight: freedRight.map((r) => r.row) },
      };
    },
    view(st, cfg) {
      return {
        title: 'Buffered rows, by key',
        cols: ['side', cfg.key, 'time', 'matched'],
        rows: [
          ...st.left.map((l) => ({ key: `L${l.row[cfg.key]}`, cells: [cfg.leftName, l.row[cfg.key], l.row[cfg.leftTime], l.joined ? 'yes' : 'no'] })),
          ...st.right.map((r) => ({ key: `R${r.row[cfg.key]}`, cells: [cfg.rightName, r.row[cfg.key], r.row[cfg.rightTime], ''] })),
        ],
        footer: `watermarks in: ${cfg.leftTopic} ${st.wms.left || 'none'}, ${cfg.rightTopic} ${st.wms.right || 'none'}`,
      };
    },
    // The panel shows no timers, so only the keys in state are compared.
    // The left rows, by time, each with whether it has matched. Flink also marks right rows that
    // have matched, which the simulator doesn't track, so only their keys are compared.
    facts: (st, cfg) => ({
      state: [...st.left, ...st.right].map((b) => String(b.row[cfg.key])),
      values: {
        IntervalJoinLeftCache: values(F.group(st.left, (l) => String(l.row[cfg.key])).map(([k, ls]) => [k,
          F.map(F.group(ls, (l) => F.time(l.t)).map(([t, g]) => [t, g.map((l) => F.tuple(F.row(l.row), String(l.joined)))]))])),
      },
    }),
    say(ev) {
      if (ev.type === 'hold') return 'The watermark is recorded, but the other input is behind, so the combined watermark doesn\'t move.';
      if (ev.type === 'advance') return `The combined watermark moves to ${ev.W}.`;
      return ev.matches.length ? 'The row matches a buffered row on the other side, and the joined row is emitted.' : 'No match yet, so the row is buffered.';
    },
  };

  // Window join (WindowJoin, run by WindowJoinOperator and WindowJoinHelper in Flink 2.3), as a
  // FULL OUTER join of two inputs windowed the same way, keyed by the join key:
  //   - a row whose window has already fired (the combined watermark has reached its end) is
  //     late, and dropped; otherwise it's added to its side's list for its key and window, and a
  //     timer is registered at the window's end
  //   - watermarks: the minimum of both inputs, and nothing until both have sent one
  //   - when the combined watermark reaches a window's end, its rows are joined: every matching
  //     pair, then each unmatched row padded with nulls (just padding, if one side has no rows),
  //     and both lists are cleared. The combined watermark is forwarded after the results.
  //   cfg: { left, key, joinRow(l, r), padLeft(l), padRight(r), leftName, rightName,
  //          leftValue(row), rightValue(row), leftTopic, rightTopic }
  const windowJoin = {
    stateKind: 'wm',
    init: () => ({ left: {}, right: {}, timers: {}, wms: { left: null, right: null }, W: null }),
    record(st, item, cfg, ctx) {
      const row = item.row;
      const side = ctx.from === cfg.left ? 'left' : 'right';
      const k = row[cfg.key];
      const end = U.toSec(`${row.window_end}:00`.slice(0, 8));
      const win = `${row.window_start}–${row.window_end}`;
      if (st.W != null && U.toSec(st.W) >= end) return { out: [], ev: { type: 'late', side, key: k, row, win, W: st.W } };
      const id = `${k}|${end}`;
      (st[side][id] ||= []).push(row);
      st.timers[id] = { k, end };
      return { out: [], absorb: true, touched: `${side}|${id}`, ev: { type: 'buffer', side, key: k, row, win, other: (st[side === 'left' ? 'right' : 'left'][id] || []).length } };
    },
    watermark(st, item, cfg, ctx) {
      const side = ctx.from === cfg.left ? 'left' : 'right';
      st.wms[side] = item.time;
      const W = combined(st.wms);
      if (!W || !later(W, st.W)) return { out: [], nodeWm: st.W, ev: { type: 'hold', side, time: item.time, W: st.W, wms: { ...st.wms } } };
      st.W = W;
      const w = U.toSec(W);
      const out = [];
      const firings = [];
      for (const [id, t] of Object.entries(st.timers).filter(([, t]) => t.end <= w).sort(([, a], [, b]) => a.end - b.end || a.k - b.k)) {
        delete st.timers[id];
        const ls = st.left[id], rs = st.right[id];
        const rows = [];
        if (ls && rs) {
          const used = new Set();
          for (const l of ls) {
            // Rows in one key and window always match: the join condition is the key and the window.
            rs.forEach((r, i) => { rows.push(cfg.joinRow(l, r)); used.add(i); });
            if (!rs.length) rows.push(cfg.padLeft(l));
          }
          rs.forEach((r, i) => { if (!used.has(i)) rows.push(cfg.padRight(r)); });
        } else if (ls) ls.forEach((l) => rows.push(cfg.padLeft(l)));
        else if (rs) rs.forEach((r) => rows.push(cfg.padRight(r)));
        delete st.left[id];
        delete st.right[id];
        rows.forEach((r) => out.push(rec('+I', r)));
        firings.push({ k: t.k, end: U.fromSec(t.end).slice(0, 5), rows, left: !!ls, right: !!rs });
      }
      out.push(wm(W));
      return { out, nodeWm: W, ev: { type: firings.length ? 'fire' : 'advance', side, time: item.time, W, firings, wms: { ...st.wms } } };
    },
    view(st, cfg) {
      const rows = [];
      for (const [side, name, value] of [['left', cfg.leftName, cfg.leftValue], ['right', cfg.rightName, cfg.rightValue]]) {
        for (const [id, rs] of Object.entries(st[side])) {
          rs.forEach((r, i) => rows.push({ id, key: `${side}|${id}`, i, cells: [name, r[cfg.key], `${r.window_start}–${r.window_end}`, value(r)] }));
        }
      }
      rows.sort((a, b) => a.id.split('|')[1] - b.id.split('|')[1] || a.id.split('|')[0] - b.id.split('|')[0]);
      const timers = [...new Set(Object.values(st.timers).map((t) => t.end))].sort((a, b) => a - b).map((t) => U.fromSec(t).slice(0, 5));
      return {
        title: `Rows per ${cfg.key} and window`,
        cols: ['side', cfg.key, 'window', 'value'],
        rows: rows.map(({ key, cells }) => ({ key, cells })),
        footer: `watermarks in: ${cfg.leftTopic} ${st.wms.left || 'none'}, ${cfg.rightTopic} ${st.wms.right || 'none'}; timers: ${timers.join(', ') || 'none'}`,
      };
    },
    // State is per key and window (the namespace is the window end), on each side, and a window's
    // timer is set for 1 ms before its end.
    facts: (st) => ({
      state: [...Object.keys(st.left), ...Object.keys(st.right)].map((id) => {
        const [k, end] = id.split('|');
        return `${k}|${hms(Number(end))}`;
      }),
      timers: Object.values(st.timers).map((t) => `${t.k}@${ms(t.end * 1000 - 1)}`),
      values: Object.fromEntries([['left-records', 'left', 0], ['right-records', 'right', 1]].map(([name, side, input]) => [name,
        values(Object.entries(st[side]).map(([id, rows]) => {
          const [k, end] = id.split('|');
          return [`${k}|${hms(Number(end))}`, rows.map((r) => F.row(r, input))];
        }))])),
    }),
    say(ev) {
      switch (ev.type) {
        case 'buffer': return `The row waits in state for its window, ${ev.win}, to end.`;
        case 'late': return `The row's window, ${ev.win}, has already fired, so it's late, and dropped.`;
        case 'hold': return 'The watermark is recorded, but the other input is behind, so the combined watermark doesn\'t move.';
        case 'advance': return `The combined watermark moves to ${ev.W}, but no window has ended yet.`;
        case 'fire': return `The combined watermark passes the end of a window, which is joined and cleared.`;
      }
      return '';
    },
  };

  // Regular inner join (Join, run by StreamingJoinOperator). Each side's state depends on the
  // input spec the planner chose: JoinKeyContainsUniqueKey keeps one row per join key, and
  // NoUniqueKey keeps every row (a multiset per join key). Mirrors
  // StreamingJoinOperator.processElement for an inner join in Flink 2.3:
  //   - +I / +U: add the row to this side (replacing the key's row, for a unique side); for each
  //     row the other side has for the key, emit the joined row with the input's row kind
  //   - -U / -D: remove the row from this side; for each row the other side has for the key,
  //     emit the joined row with the input's row kind (an upsert-kafka tombstone carries only
  //     the key)
  //   - no row on the other side: nothing is emitted (inner join)
  // Watermarks: the minimum of both inputs, and nothing until both have sent one.
  //   cfg: { left, key, joinRow(l, r), leftName, rightName, leftCell(row), rightCell(row),
  //          leftUnique, rightUnique (default true) }
  const regularJoin = {
    stateParts: ['left', 'right'],
    init: () => ({ left: {}, right: {}, wms: { left: null, right: null }, W: null }),
    record(st, item, cfg, ctx) {
      const side = ctx.from === cfg.left ? 'left' : 'right';
      const otherSide = side === 'left' ? 'right' : 'left';
      const unique = (cfg[`${side}Unique`] ?? true);
      const k = item.row[cfg.key];
      const accumulate = item.kind === '+I' || item.kind === '+U';
      const rows = st[side][k] || [];
      const prev = unique ? rows[0] || null : null;
      let next;
      if (accumulate) next = unique ? [item.row] : [...rows, item.row];
      else {
        const i = unique ? 0 : rows.findIndex((r) => same(r, item.row));
        next = rows.filter((_, j) => j !== i);
      }
      if (next.length) st[side][k] = next; else delete st[side][k];
      const others = st[otherSide][k] || [];
      const out = others.map((o) => rec(item.kind, side === 'left' ? cfg.joinRow(item.row, o) : cfg.joinRow(o, item.row)));
      return {
        out, absorb: !out.length && accumulate, touched: accumulate ? `${side}${k}|${next.length - 1}` : null,
        ev: { type: accumulate ? 'upsert' : 'delete', side, key: k, kind: item.kind, row: item.row, prev, other: others[0] || null, others, kept: next.length, joined: out.map((o) => o.row) },
      };
    },
    watermark(st, item, cfg, ctx) {
      const side = ctx.from === cfg.left ? 'left' : 'right';
      st.wms[side] = item.time;
      const W = combined(st.wms);
      if (!W || !later(W, st.W)) return { out: [], nodeWm: st.W, ev: { type: 'hold', side, time: item.time, W: st.W } };
      st.W = W;
      return { out: [wm(W)], nodeWm: W, ev: { type: 'advance', side, time: item.time, W } };
    },
    view(st, cfg) {
      return {
        title: cfg.stateTitle || `One row per ${cfg.key} on each side`,
        cols: ['side', cfg.keyLabel || cfg.key, 'row'],
        rows: ['left', 'right'].flatMap((side) => Object.entries(st[side]).flatMap(([k, rows]) => rows.map((r, i) => ({
          key: `${side}${k}|${i}`, part: side, cells: [cfg[`${side}Name`], k, cfg[`${side}Cell`](r)],
        })))),
      };
    },
    // A side with a unique key keeps one row per key; otherwise each distinct row, with its count.
    facts: (st, cfg) => ({
      state: [...Object.keys(st.left), ...Object.keys(st.right)], timers: [],
      values: Object.fromEntries([['left-records', 'left', 0], ['right-records', 'right', 1]].map(([name, side, input]) => [name,
        values(Object.entries(st[side]).map(([k, rows]) => [k, (cfg[`${side}Unique`] ?? true)
          ? F.row(rows[0], input)
          : F.map(F.count(rows).map(([r, n]) => [F.row(r, input), n]))]))])),
    }),
    say(ev) {
      if (ev.type === 'hold' || ev.type === 'advance') return 'The join records the watermark. Its own is the minimum of both inputs.';
      if (ev.type === 'delete') return ev.other ? 'The row is removed from state, and the joined row is deleted downstream.' : 'The row is removed from state. Nothing was joined with it, so nothing is emitted.';
      return ev.other ? 'The row replaces this side\'s row for its key, and is joined with the other side\'s row.' : 'The row is stored. The other side has no row for this key yet, so nothing is emitted.';
    },
  };

  // Lookup join (LookupJoin): for each row, the connector looks up the other table, through its
  // cache. Flink keeps no state of its own, and the cache isn't checkpointed. Inner join: a row
  // with no match is dropped.
  //   cfg: { lookupKey(row) -> string, cacheCols, cacheCells(key, value), table: { key: value },
  //          ext (id of the external node), joinRow(row, value) }
  const lookupJoin = {
    init: () => ({ cache: {} }),
    record(st, item, cfg) {
      const k = cfg.lookupKey(item.row);
      const hit = k in st.cache;
      if (!hit) st.cache[k] = cfg.table[k] ?? null;
      const value = st.cache[k];
      const out = value == null ? [] : [rec('+I', cfg.joinRow(item.row, value))];
      return { out, touched: k, also: hit ? null : cfg.ext, ev: { type: hit ? 'hit' : 'miss', key: k, row: item.row, value } };
    },
    watermark: (st, item) => ({ out: [wm(item.time)], ev: { type: 'wm', time: item.time } }),
    view(st, cfg) {
      return {
        title: 'Lookup cache (connector, not Flink state)',
        cols: cfg.cacheCols,
        rows: Object.entries(st.cache).map(([k, v]) => ({ key: k, cells: cfg.cacheCells(k, v) })),
      };
    },
    say(ev) {
      if (ev.type === 'hit') return 'The answer is in the cache, so no request is made.';
      return 'Not in the cache: the connector asks the external system, and caches the answer.';
    },
  };

  // Event-time temporal join (TemporalJoin, TemporalRowTimeJoinOperator in Flink 2.3). Left rows
  // wait in state. When the combined watermark W advances, every left row with time <= W is
  // joined with the right side's latest version at or before its time, then versions older than
  // the latest one at or before W are removed. Inner join: no version, no output.
  //   cfg: { left, key, leftTime, rightTime, joinRow(l, version), leftId, valueField }
  const temporalJoin = {
    stateKind: { waiting: 'wm', old: 'wm' }, stateParts: ['waiting', 'old', 'latest'],
    init: () => ({ left: [], versions: {}, timers: {}, wms: { left: null, right: null }, W: null, seq: 0 }),
    record(st, item, cfg, ctx) {
      const row = item.row;
      const k = row[cfg.key];
      // One timer per key, at the smallest time registered for it (registerSmallestTimer).
      const timer = (t) => { st.timers[k] = Math.min(st.timers[k] ?? Infinity, U.toSec(t)); };
      if (ctx.from === cfg.left) {
        st.left.push({ row, seq: ++st.seq });
        timer(row[cfg.leftTime]);
        return { out: [], absorb: true, touched: `L${row[cfg.leftId]}`, ev: { type: 'buffer', row } };
      }
      const t = row[cfg.rightTime];
      const vs = (st.versions[k] ||= []).filter((v) => v.row[cfg.rightTime] !== t);
      vs.push({ row, kind: item.kind });
      vs.sort((a, b) => U.toSec(a.row[cfg.rightTime]) - U.toSec(b.row[cfg.rightTime]));
      st.versions[k] = vs;
      timer(t);
      return { out: [], absorb: true, touched: `V${k}|${t}`, ev: { type: 'version', row } };
    },
    watermark(st, item, cfg, ctx) {
      const side = ctx.from === cfg.left ? 'left' : 'right';
      st.wms[side] = item.time;
      const W = combined(st.wms);
      if (!W || !later(W, st.W)) return { out: [], nodeWm: st.W, ev: { type: 'hold', side, time: item.time, W: st.W, wms: { ...st.wms } } };
      st.W = W;
      const w = U.toSec(W);
      // Timers fire in time order. Each one joins its key's orders up to W (in arrival order),
      // drops the versions older than the latest one at or before W, and re-registers a timer
      // only for the key's next waiting order (onEventTime / emitResultAndCleanUpState).
      const results = [];
      const removed = [];
      const firing = Object.entries(st.timers).filter(([, t]) => t <= w).sort((a, b) => a[1] - b[1]);
      for (const [k] of firing) {
        delete st.timers[k];
        const mine = (l) => String(l.row[cfg.key]) === k;
        const due = st.left.filter((l) => mine(l) && U.toSec(l.row[cfg.leftTime]) <= w).sort((a, b) => a.seq - b.seq);
        st.left = st.left.filter((l) => !due.includes(l));
        for (const l of due) {
          const t = U.toSec(l.row[cfg.leftTime]);
          const version = (st.versions[k] || []).filter((v) => U.toSec(v.row[cfg.rightTime]) <= t).pop();
          results.push({ left: l.row, version: version?.row || null });
        }
        const vs = st.versions[k] || [];
        const keepFrom = vs.reduce((idx, v, i) => (U.toSec(v.row[cfg.rightTime]) <= w ? i : idx), 0);
        removed.push(...vs.slice(0, keepFrom).map((v) => v.row));
        if (st.versions[k]) st.versions[k] = vs.slice(keepFrom);
        const next = st.left.filter(mine).map((l) => U.toSec(l.row[cfg.leftTime]));
        if (next.length) st.timers[k] = Math.min(...next);
      }
      const out = results.filter((r) => r.version).map((r) => rec('+I', cfg.joinRow(r.left, r.version)));
      out.push(wm(W));
      return { out, nodeWm: W, ev: { type: 'advance', side, time: item.time, W, results, removed, waiting: st.left.map((l) => l.row) } };
    },
    view(st, cfg) {
      const rows = [
        ...st.left.map((l) => ({ key: `L${l.row[cfg.leftId]}`, part: 'waiting', cells: [cfg.leftName, l.row[cfg.key], l.row[cfg.leftTime], `#${l.row[cfg.leftId]}`] })),
        // Every version but a key's newest is freed once the watermark passes the next one.
        ...Object.entries(st.versions).flatMap(([k, vs]) => vs.map((v, i) => ({
          key: `V${k}|${v.row[cfg.rightTime]}`, part: i === vs.length - 1 ? 'latest' : 'old', cells: [cfg.rightName, k, v.row[cfg.rightTime], U.money(v.row[cfg.valueField])],
        }))),
      ];
      return {
        title: 'Waiting orders, and versions',
        cols: ['kind', cfg.key, 'time', 'value'],
        rows,
        footer: `watermarks in: ${cfg.leftTopic} ${st.wms.left || 'none'}, ${cfg.rightTopic} ${st.wms.right || 'none'}; timers: ${Object.entries(st.timers).sort((a, b) => a[1] - b[1]).map(([k, t]) => `${cfg.keyName || cfg.key} ${k} at ${U.fromSec(t).slice(0, 5)}`).join(', ') || 'none'}`,
      };
    },
    facts: (st, cfg) => ({
      state: [...st.left.map((l) => String(l.row[cfg.key])), ...Object.keys(st.versions).filter((k) => st.versions[k].length)],
      timers: Object.entries(st.timers).map(([k, t]) => `${k}@${ms(t * 1000)}`),
      // The versions, by time, with their row kinds. The waiting orders are kept under a per-key
      // counter, which the simulator doesn't track, so their values aren't compared.
      values: {
        right: values(Object.entries(st.versions).filter(([, vs]) => vs.length)
          .map(([k, vs]) => [k, F.map(vs.map((v) => [v.row[cfg.rightTime], F.row(v.row, 1, v.kind)]))])),
      },
    }),
    say(ev) {
      if (ev.type === 'buffer') return 'The order waits in state until the watermark reaches its time.';
      if (ev.type === 'version') return 'A new version is added to the versioned table.';
      if (ev.type === 'hold') return 'The other input is behind, so the combined watermark doesn\'t move.';
      return `The combined watermark moves to ${ev.W}, and every order at or before it is joined.`;
    },
  };

  // A LATERAL SNAPSHOT join's build-side bookkeeping, per key, as in Flink 2.4: the build
  // watermark at which the key's change buffer was last written (bufferedAtWm; -Infinity before
  // any watermark). Touching a key applies its due changes only if the watermark has moved past
  // that point since (applyBufferedChangesUpToCurrentWm), so a change that arrives already behind
  // the watermark, such as a -U with the old row's time, waits for the next one.
  const buildWm = (st) => (st.wmBuild == null ? -Infinity : U.toSec(st.wmBuild));
  // How far a key's changes would be applied if it were touched now, or null.
  function applyLimit(st, k) {
    if (!(k in st.bufferedAt)) return null;
    const cur = buildWm(st);
    if (cur > st.bufferedAt[k]) return cur;
    if (st.phase === 'JOIN' && cur === -Infinity) return Infinity;
    return null;
  }
  // Applies a key's due changes, in row-time order (ties in arrival order). Returns them, and
  // what Flink's method returns: the limit if changes remain, null if none do, or the unchanged
  // bufferedAtWm when nothing was applied.
  function applyDue(st, cfg, k) {
    if (!(k in st.bufferedAt)) return { applied: [], r: null };
    const limit = applyLimit(st, k);
    if (limit == null) return { applied: [], r: st.bufferedAt[k] };
    const t = (r) => U.toSec(r[cfg.buildTime]);
    const due = st.changes.filter((c) => c.row[cfg.key] === k && t(c.row) <= limit).sort((a, b) => t(a.row) - t(b.row) || a.seq - b.seq);
    st.changes = st.changes.filter((c) => !due.includes(c));
    for (const c of due) {
      const rows = st.table[k] || [];
      if (c.kind === '+I' || c.kind === '+U') rows.push(c.row);
      else { const i = rows.findIndex((r) => same(r, c.row)); if (i >= 0) rows.splice(i, 1); }
      if (rows.length) st.table[k] = rows; else delete st.table[k];
    }
    const remain = st.changes.some((c) => c.row[cfg.key] === k);
    if (remain) st.bufferedAt[k] = limit; else delete st.bufferedAt[k];
    return { applied: due.map((c) => ({ kind: c.kind, row: c.row })), r: remain ? limit : null };
  }
  // Whether a buffered change would be applied if its key were touched now.
  const isDue = (st, cfg, c) => {
    const limit = applyLimit(st, c.row[cfg.key]);
    return limit != null && U.toSec(c.row[cfg.buildTime]) <= limit;
  };

  // LATERAL SNAPSHOT join (LateralSnapshotJoinOperator, Flink 2.4). Two phases:
  //   LOAD: probe rows are buffered; probe watermarks are held back. LOAD ends when the build
  //         watermark reaches loadCompletedTime (by default, when the query was compiled).
  //   JOIN: buffered probe rows are joined first (once a probe watermark is available), then
  //         every probe row is joined on arrival against the current build table, unless rows
  //         for its key are still buffered; probe watermarks are forwarded.
  // In both phases a build change is buffered under its row time, and takes effect once the build
  // watermark reaches it. Flink applies a key's due changes lazily, when the key is next touched
  // (applyBufferedChangesUpToCurrentWm): a probe row or a build row for it, or the timer of one of
  // its buffered probe rows. A build watermark applies nothing by itself. A change that arrives
  // behind the watermark waits for the watermark to advance (see applyLimit). Buffered probe rows are
  // joined by those timers, which fire on the first probe watermark of the join phase; with one
  // buffered row per key, that's arrival order. Build watermarks are never forwarded. Inner join:
  // no match, no output.
  //   cfg: { left, key, buildTime, loadCompletedTime, joinRow(probe, build), probeId, probeTime,
  //          valueField }
  const lateralSnapshotJoin = {
    stateKind: { change: 'wm', probe: 'wm' }, stateParts: ['table', 'change', 'probe'],
    init: () => ({ phase: 'LOAD', table: {}, changes: [], bufferedAt: {}, probes: [], wmProbe: null, wmBuild: null, forwarded: null, seq: 0 }),
    record(st, item, cfg, ctx) {
      const row = item.row;
      const k = row[cfg.key];
      const { applied, r } = applyDue(st, cfg, k);
      if (ctx.from !== cfg.left) {
        st.changes.push({ kind: item.kind, row, seq: ++st.seq });
        if (r == null || r !== buildWm(st)) st.bufferedAt[k] = buildWm(st);
        const behind = U.toSec(row[cfg.buildTime]) <= buildWm(st);
        return { out: [], absorb: true, touched: `C${st.seq}`, ev: { type: 'buffer-change', kind: item.kind, row, phase: st.phase, applied, behind } };
      }
      if (st.phase === 'LOAD' || st.probes.some((p) => p[cfg.key] === k)) {
        st.probes.push(row);
        return { out: [], absorb: true, touched: `P${row[cfg.probeId]}`, ev: { type: 'buffer-probe', row, phase: st.phase, applied } };
      }
      const matches = st.table[k] || [];
      return {
        out: matches.map((b) => rec('+I', cfg.joinRow(row, b))), touched: `T${k}`,
        ev: { type: matches.length ? 'join' : 'no-match', row, build: matches[0] || null, applied, pending: st.changes.filter((c) => c.row[cfg.key] === k).map((c) => c.row) },
      };
    },
    watermark(st, item, cfg, ctx) {
      // Joins the buffered probe rows, each after its key's due changes are applied.
      const drain = () => {
        const out = [];
        const applied = [];
        for (const p of st.probes) {
          applied.push(...applyDue(st, cfg, p[cfg.key]).applied);
          for (const b of st.table[p[cfg.key]] || []) out.push(rec('+I', cfg.joinRow(p, b)));
        }
        const drained = st.probes;
        st.probes = [];
        return { out, drained, applied };
      };
      if (ctx.from !== cfg.left) {
        st.wmBuild = item.time;
        const w = U.toSec(item.time);
        const due = st.changes.filter((c) => isDue(st, cfg, c)).map((c) => ({ kind: c.kind, row: c.row }));
        if (st.phase === 'LOAD' && w >= U.toSec(cfg.loadCompletedTime)) {
          st.phase = 'JOIN';
          if (st.wmProbe != null) {
            const { out, drained, applied } = drain();
            st.forwarded = st.wmProbe;
            out.push(wm(st.wmProbe));
            return { out, nodeWm: st.wmProbe, ev: { type: 'flip', time: item.time, due, applied, drained } };
          }
          return { out: [], nodeWm: null, ev: { type: 'flip', time: item.time, due, applied: [], drained: [] } };
        }
        return { out: [], nodeWm: null, ev: { type: 'build-wm', time: item.time, due, phase: st.phase } };
      }
      st.wmProbe = item.time;
      if (st.phase === 'LOAD') return { out: [], nodeWm: null, ev: { type: 'hold', time: item.time } };
      const { out, drained, applied } = drain();
      st.forwarded = item.time;
      out.push(wm(item.time));
      return { out, nodeWm: item.time, ev: { type: 'probe-wm', time: item.time, drained, applied } };
    },
    view(st, cfg) {
      return {
        title: `Build table, pending changes, buffered ${cfg.probeName}s`,
        cols: ['kind', cfg.key, 'value', 'time'],
        rows: [
          ...Object.entries(st.table).flatMap(([k, rows]) => rows.map((r) => ({ key: `T${k}`, part: 'table', cells: ['current', k, U.money(r[cfg.valueField]), r[cfg.buildTime]] }))),
          ...st.changes.map((c) => ({
            key: `C${c.seq}`, part: 'change',
            cells: [`${isDue(st, cfg, c) ? 'due' : 'pending'} ${c.kind.replace('-', '−')}`, c.row[cfg.key], U.money(c.row[cfg.valueField]), c.row[cfg.buildTime]],
          })),
          ...st.probes.map((p) => ({ key: `P${p[cfg.probeId]}`, part: 'probe', cells: [cfg.probeName, p[cfg.key], `#${p[cfg.probeId]}`, p[cfg.probeTime]] })),
        ],
        footer: st.phase === 'LOAD' ? `phase: LOAD, until the costs watermark reaches ${cfg.loadCompletedTime}` : 'phase: JOIN',
      };
    },
    // The build table (each row with its count), and the buffered changes by row time, with their
    // kinds. Buffered probe rows are kept under an internal counter, so only their keys are
    // compared. The panel shows no timers, so they aren't compared.
    facts: (st, cfg) => ({
      state: [...Object.keys(st.table), ...st.changes.map((c) => String(c.row[cfg.key])), ...st.probes.map((p) => String(p[cfg.key]))],
      values: {
        'build-table': values(Object.entries(st.table).map(([k, rows]) => [k, F.map(F.count(rows).map(([r, n]) => [F.row(r, 1), n]))])),
        'build-change-buffer': values(F.group(st.changes, (c) => String(c.row[cfg.key])).map(([k, cs]) => [k,
          F.map(F.group(cs, (c) => c.row[cfg.buildTime]).map(([t, g]) => [t, g.map((c) => F.row(c.row, 1, c.kind))]))])),
      },
    }),
    say(ev) {
      switch (ev.type) {
        case 'buffer-change': return 'The change is buffered. It\'s applied once the build-side watermark has reached its row time, the next time its key is needed.';
        case 'buffer-probe': return 'The probe row is buffered until the load phase ends.';
        case 'join': return 'The probe row is joined at once with the current build-side row.';
        case 'no-match': return 'No current build-side row for this key, so nothing is emitted.';
        case 'flip': return 'The build-side watermark reaches the load-completed time: the operator switches to the join phase.';
        case 'hold': return 'Probe-side watermarks are held back during the load phase.';
        default: return 'The watermark is processed.';
      }
    },
  };

  // MATCH_RECOGNIZE with an event-time ORDER BY (Match, run by CepOperator). Mirrors
  // CepOperator.processElement / onEventTime in Flink 2.3:
  //   - a row at or before the operator's watermark is late, and dropped
  //   - otherwise it's buffered, per key and timestamp, with a timer at its timestamp
  //   - when the watermark passes buffered rows, they're fed to the NFA in timestamp order
  // The NFA here handles PATTERN (A B C) with strict contiguity, an always-true A, and
  // AFTER MATCH SKIP PAST LAST ROW: every row starts a new partial match as A, each partial
  // match either takes the row as its next variable or is discarded, and a completed match
  // prunes every partial match that started at or before its last row (all of them).
  //   cfg: { key, timeField, valueField, next(prev, row) -> bool, measures(a, b, c) -> row }
  const match = {
    stateKind: { buffered: 'wm' }, stateParts: ['buffered', 'partial'],
    init: () => ({ keys: {}, wm: null, seq: 0 }),
    record(st, item, cfg) {
      const row = item.row;
      const k = row[cfg.key];
      const ts = U.toSec(row[cfg.timeField]);
      if (st.wm != null && ts <= U.toSec(st.wm)) return { out: [], ev: { type: 'late', key: k, row, wm: st.wm } };
      const s = (st.keys[k] ||= { buffer: [], partials: [] });
      s.buffer.push({ row, ts, seq: ++st.seq });
      s.buffer.sort((a, b) => a.ts - b.ts || a.seq - b.seq);
      return { out: [], absorb: true, touched: k, ev: { type: 'buffer', key: k, row, n: st.seq, ahead: s.buffer.filter((b) => b.ts > ts).map((b) => b.row) } };
    },
    watermark(st, item, cfg) {
      st.wm = item.time;
      const now = U.toSec(item.time);
      // Timers fire in timestamp order, so the key with the earliest due row goes first.
      const due = Object.entries(st.keys)
        .filter(([, s]) => s.buffer.length && s.buffer[0].ts <= now)
        .sort(([, a], [, b]) => a.buffer[0].ts - b.buffer[0].ts);
      const out = [];
      const keys = due.map(([k, s]) => {
        const steps = [];
        const ready = s.buffer.filter((b) => b.ts <= now);
        s.buffer = s.buffer.filter((b) => b.ts > now);
        for (const { row, seq } of ready) {
          let found = null;
          const next = [];
          const dropped = [];
          for (const p of s.partials) {
            if (!cfg.next(p[p.length - 1], row)) { dropped.push(p); continue; }
            if (p.length === 2) found = [...p, row];
            else next.push([...p, row]);
          }
          next.push([row]);
          if (found) {
            out.push(rec('+I', cfg.measures(...found)));
            s.partials = [];
          } else {
            s.partials = next;
          }
          steps.push({ row, seq, match: found, dropped, partials: U.clone(s.partials) });
        }
        return { key: k, steps, waiting: s.buffer.map((b) => ({ row: b.row, seq: b.seq })) };
      });
      out.push(wm(item.time));
      return { out, ev: { type: keys.length ? 'fire' : 'wm', time: item.time, keys, matches: out.length - 1 } };
    },
    view(st, cfg) {
      const v = (r) => r[cfg.valueField];
      return {
        title: `Per ${cfg.keyLabel || cfg.key}: buffered rows and partial matches`,
        cols: [cfg.keyLabel || cfg.key, 'kind', cfg.valueLabel || cfg.valueField, 'time'],
        rows: Object.entries(st.keys).flatMap(([k, s]) => [
          ...s.buffer.map((b) => ({ key: k, part: 'buffered', cells: [k, 'buffered', v(b.row), b.row[cfg.timeField].slice(0, 5)] })),
          ...s.partials.map((p) => ({ key: k, part: 'partial', cells: [k, 'partial', [...p.map(v), '?', '?'].slice(0, 3).join('→'), p[0][cfg.timeField].slice(0, 5)] })),
        ]),
        footer: st.wm ? `late: at or before ${st.wm}` : 'no watermark yet',
      };
    },
    // The panel doesn't show them, but each buffered row has a timer at its timestamp.
    // The buffered rows, by timestamp. The NFA's partial matches are in Flink's own form, so
    // only the keys are compared for them.
    facts: (st) => ({
      state: Object.keys(st.keys),
      timers: Object.entries(st.keys).flatMap(([k, s]) => s.buffer.map((b) => `${k}@${ms(b.ts * 1000)}`)),
      values: {
        eventQueuesStateName: values(Object.entries(st.keys).filter(([, s]) => s.buffer.length).map(([k, s]) => [k,
          F.map(F.group([...s.buffer].sort((a, b) => a.seq - b.seq), (b) => F.time(b.ts)).map(([t, bs]) => [t, bs.map((b) => F.row(b.row))]))])),
      },
    }),
    say(ev) {
      switch (ev.type) {
        case 'late': return `The row is at or before the watermark, so it's late, and dropped.`;
        case 'buffer': return `The row waits in state until the watermark passes it.`;
        case 'fire': return `The watermark passes buffered rows, which are fed to the pattern in event-time order.`;
        case 'wm': return `The watermark hasn't passed any buffered row. It's forwarded.`;
      }
      return '';
    },
  };

  // SinkUpsertMaterializer (the legacy one, which Flink 2.3 uses for ON CONFLICT DO DEDUPLICATE).
  // Added by the planner when the input's upsert key differs from the sink's primary key. For each
  // primary key it keeps a list of rows, one per upsert key, and the last one is what the sink has.
  // Mirrors SinkUpsertMaterializer.processElement:
  //   - +I / +U: replace the row with the same upsert key, or append it; emit it as +I if the
  //     list was empty, else +U
  //   - -U / -D: remove the row with the same upsert key; if the list is now empty, emit -D;
  //     if the removed row was the last one, emit the new last row as +U; otherwise nothing
  //   cfg: { key, upsertKey(row) -> string, cell(row) }
  const upsertMaterializer = {
    init: () => ({ rows: {} }),
    record(st, item, cfg) {
      const k = item.row[cfg.key];
      const list = st.rows[k] || [];
      const uk = cfg.upsertKey(item.row);
      const i = list.findIndex((r) => cfg.upsertKey(r) === uk);
      if (item.kind === '+I' || item.kind === '+U') {
        const kind = list.length ? '+U' : '+I';
        const next = i < 0 ? [...list, item.row] : list.map((r, j) => (j === i ? item.row : r));
        st.rows[k] = next;
        return { out: [rec(kind, item.row)], touched: k, ev: { type: 'add', key: k, kind, row: item.row, size: next.length } };
      }
      if (i < 0) return { out: [], ev: { type: 'missing', key: k, row: item.row } };
      const next = list.filter((_, j) => j !== i);
      if (!next.length) {
        delete st.rows[k];
        return { out: [rec('-D', item.row)], ev: { type: 'emptied', key: k, kind: item.kind, row: item.row } };
      }
      st.rows[k] = next;
      if (i === list.length - 1) return { out: [rec('+U', next[next.length - 1])], touched: k, ev: { type: 'fallback', key: k, row: next[next.length - 1] } };
      return { out: [], touched: k, ev: { type: 'removed', key: k, row: item.row } };
    },
    watermark: (st, item) => ({ out: [wm(item.time)], ev: { type: 'wm', time: item.time } }),
    view(st, cfg) {
      return {
        title: `Rows per ${cfg.keyLabel || cfg.key}, by upsert key`,
        cols: [cfg.keyLabel || cfg.key, 'rows'],
        rows: Object.entries(st.rows).map(([k, list]) => ({ key: k, cells: [k, list.map(cfg.cell).join('; ')] })),
      };
    },
    facts: (st) => ({
      state: Object.keys(st.rows), timers: [],
      values: { values: values(Object.entries(st.rows).map(([k, list]) => [k, list.map((r) => F.row(r))])) },
    }),
    say(ev) {
      switch (ev.type) {
        case 'add': return `The row is added to the list for key ${ev.key}, and emitted as ${ev.kind}.`;
        case 'emptied': return `The retraction empties the list for key ${ev.key}, so the materializer emits a delete.`;
        case 'fallback': return `The latest row for key ${ev.key} was retracted, so the one before it is emitted again.`;
        case 'removed': return `An older row for key ${ev.key} is removed. The sink's row doesn't change.`;
        case 'missing': return `A retraction for a row that isn't in the list. It's ignored.`;
      }
      return '';
    },
  };

  // An external system, such as the service behind a lookup join. It isn't part of the stream.
  const external = { init: () => null, view: () => null };

  // Kafka sink with a changelog format (debezium-json). +I and +U are written as creates
  // (op c, with an after image), -U and -D as deletes (op d, with a before image), as in
  // DebeziumJsonSerializationSchema. A consumer that applies them in order rebuilds the table.
  const changelogKafkaSink = {
    init: () => ({ log: [], table: {} }),
    record(st, item, cfg) {
      const row = cfg.fields ? pick(item.row, cfg.fields) : item.row;
      const key = row[cfg.key];
      const create = item.kind === '+I' || item.kind === '+U';
      st.log.push({ offset: st.log.length, value: { op: create ? 'c' : 'd', image: create ? 'after' : 'before', ...row } });
      if (create) st.table[key] = row;
      else delete st.table[key];
      return { out: [], ev: { type: create ? 'create' : 'delete', kind: item.kind, key, row } };
    },
    watermark: (st, item) => ({ out: [], ev: { type: 'wm', time: item.time } }),
    view: () => null,
    say(ev, cfg) {
      if (ev.type === 'wm') return `Watermarks aren't written to Kafka.`;
      if (ev.type === 'delete') return `The sink writes the ${ev.kind} to ${cfg.topic} as a Debezium delete (op d), with the old row as its before image.`;
      return `The sink writes the ${ev.kind} to ${cfg.topic} as a Debezium create (op c), with the row as its after image.`;
    },
  };

  // Kafka sink with an append-only format. The planner rejects anything but inserts.
  const kafkaSink = {
    init: () => ({ log: [] }),
    record(st, item, cfg) {
      if (item.kind !== '+I') throw new Error(`${cfg.topic}: an append-only sink can't take ${item.kind}`);
      const value = cfg.fields ? pick(item.row, cfg.fields) : item.row;
      st.log.push({ offset: st.log.length, value });
      return { out: [], ev: { type: 'append', row: value } };
    },
    watermark: (st, item) => ({ out: [], ev: { type: 'wm', time: item.time } }),
    view: () => null,
    say(ev, cfg) {
      if (ev.type === 'wm') return `Watermarks aren't written to Kafka. The sink just takes note of it.`;
      return `The sink appends the row to ${cfg.topic}.`;
    },
  };

  // upsert-kafka sink: inserts and updates become the latest value for their key;
  // deletes and retractions (-D, -U) become tombstones.
  const upsertKafkaSink = {
    init: () => ({ log: [], table: {} }),
    record(st, item, cfg) {
      const key = item.row[cfg.key];
      if (item.kind === '-D' || item.kind === '-U') {
        st.log.push({ offset: st.log.length, key, value: null });
        delete st.table[key];
        return { out: [], ev: { type: 'tombstone', key, kind: item.kind } };
      }
      const value = cfg.fields ? pick(item.row, cfg.fields) : item.row;
      st.log.push({ offset: st.log.length, key, value });
      st.table[key] = value;
      return { out: [], ev: { type: 'upsert', key, row: value, kind: item.kind } };
    },
    watermark: (st, item) => ({ out: [], ev: { type: 'wm', time: item.time } }),
    view: () => null,
    say(ev, cfg) {
      if (ev.type === 'wm') return `Watermarks aren't written to Kafka.`;
      if (ev.type === 'tombstone') return `The sink writes a tombstone for key ${ev.key} to ${cfg.topic}.`;
      return `The sink writes the new value for key ${ev.key} to ${cfg.topic}.`;
    },
  };

  return {
    kafkaSource, upsertKafkaSource, calc, changelogNormalize, localWindowAggregate, globalWindowAggregate, sessionWindowAggregate, overAggregate,
    groupAggregate, procTimeDedup, rowTimeDedup, retractRank, fastTop1, windowJoin,
    regularJoin, intervalJoin, lookupJoin, temporalJoin, lateralSnapshotJoin, match, upsertMaterializer, external,
    kafkaSink, upsertKafkaSink, changelogKafkaSink,
  };
})();
