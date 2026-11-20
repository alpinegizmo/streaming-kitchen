// Simplified simulators for Flink SQL's physical operators.
//
// Each operator kind provides:
//   init(cfg)                    -> operator state (plain data, so it can be snapshotted)
//   input(st, rec, cfg)          -> sources only: turn a Kafka record into a changelog row
//   record(st, item, cfg)        -> { out: [items], ev, touched }
//   watermark(st, item, cfg)     -> { out: [items], ev }
//   view(st, cfg)                -> the state panel drawn under the operator, or null
//   say(ev, cfg)                 -> a default caption; patterns can override it
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
    init: () => ({}),
    input(st, r, cfg) {
      if (r.value == null) {
        const row = { [cfg.key]: r.key };
        return { out: [rec('-D', row)], ev: { type: 'tombstone', key: r.key, row } };
      }
      const row = { [cfg.key]: r.key, ...r.value };
      return { out: [rec('+U', row)], ev: { type: 'read', key: r.key, row } };
    },
    view: () => null,
    say(ev, cfg) {
      if (ev.type === 'tombstone') return `A tombstone for key ${ev.key}. The source emits a delete that carries only the key.`;
      return `The source reads key ${ev.key} from ${cfg.topic}. It can't tell whether the key is new, so it emits an upsert.`;
    },
  };

  // Calc: projection and filter, stateless. Watermarks pass through unchanged.
  const calc = {
    init: () => null,
    record(st, item, cfg) {
      if (cfg.where && !cfg.where(item.row)) {
        return { out: [], ev: { type: 'drop', row: item.row } };
      }
      const row = cfg.project ? cfg.project(item.row) : item.row;
      return { out: [rec(item.kind, row)], ev: { type: 'pass', row, in: item.row } };
    },
    watermark: (st, item) => ({ out: [wm(item.time)], ev: { type: 'wm', time: item.time } }),
    view: () => null,
    say(ev) {
      if (ev.type === 'wm') return `The watermark passes through unchanged. A stateless operator has no use for it.`;
      if (ev.type === 'drop') return `The row doesn't match the WHERE clause, so it's dropped.`;
      return `The row matches the WHERE clause, and the projection computes the output columns.`;
    },
  };

  // ChangelogNormalize: keeps the last row for each key, and turns an upsert stream
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
        title: cfg.stateTitle || `Last row per ${cfg.key}`,
        cols: fields,
        rows: Object.entries(st.rows).map(([key, row]) => ({ key, cells: fields.map((f) => row[f]) })),
      };
    },
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

  // Window aggregation over TUMBLE, HOP and CUMULATE window TVFs. Flink plans these as
  // LocalWindowAggregate + GlobalWindowAggregate, drawn as one operator. State is kept per
  // slice, and windows are assembled from slices when they fire. Mirrors Flink 2.3's
  // AbstractSliceSyncStateWindowAggProcessor, SliceSharedSyncStateWindowAggProcessor,
  // SliceUnsharedSyncStateWindowAggProcessor and the slice assigners in SliceAssigners:
  //   - a record goes into one slice, and registers a timer at the first unfired window end
  //   - a record whose slice has fired, and whose last window has fired, is late, and dropped
  //   - tumble: a slice is a window; it fires, and is freed
  //   - hop: a window merges the slices it spans, is emitted unless empty, schedules the next
  //     window if it wasn't empty, and frees its first slice
  //   - cumulate: each step merges its slice into the window's first slice, which is shared,
  //     and emits it; the next step is scheduled until the window reaches its maximum size
  //   cfg: { kind: 'tumble' | 'hop' | 'cumulate', size, slide (hop), step (cumulate),
  //          key, keyLabel, timeField, sumField }   (all durations in seconds)
  const windowAggregate = {
    init: () => ({ slices: {}, timers: {}, wm: null }),
    record(st, item, cfg) {
      const row = item.row;
      const k = row[cfg.key];
      const ts = U.toSec(row[cfg.timeField]);
      const sliceSize = cfg.kind === 'hop' ? cfg.slide : cfg.kind === 'cumulate' ? cfg.step : cfg.size;
      const sliceEnd = Math.floor(ts / sliceSize) * sliceSize + sliceSize;
      const winStart = (end) => (cfg.kind === 'cumulate' ? Math.floor((end - 1) / cfg.size) * cfg.size : end - cfg.size);
      const lastWindowEnd = cfg.kind === 'hop' ? sliceEnd - sliceSize + cfg.size
        : cfg.kind === 'cumulate' ? winStart(sliceEnd) + cfg.size : sliceEnd;
      const slice = { start: hm(sliceEnd - sliceSize), end: hm(sliceEnd) };
      if (fired(st, sliceEnd)) {
        if (fired(st, lastWindowEnd)) return { out: [], ev: { type: 'late', row, slice, wm: st.wm } };
        throw new Error('A record for a fired slice of an unfired window: the example data should avoid this case.');
      }
      const key = `${k}|${sliceEnd}`;
      const created = !st.slices[key];
      const s = st.slices[key] || (st.slices[key] = { k, start: sliceEnd - sliceSize, end: sliceEnd, count: 0, sum: 0 });
      s.count += 1;
      s.sum = add(s.sum, row[cfg.sumField]);
      let timer = sliceEnd;
      while (fired(st, timer)) timer += sliceSize;
      st.timers[`${k}|${timer}`] = { k, t: timer };
      return { out: [], absorb: true, touched: key, ev: { type: created ? 'new-slice' : 'accumulate', row, slice, acc: { ...s }, timer: hm(timer) } };
    },
    watermark(st, item, cfg) {
      st.wm = item.time;
      const now = U.toSec(item.time);
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
          f.row = { window_start: f.start, window_end: f.end, [cfg.key]: k, orders: acc.count, sales: acc.sum };
          out.push(rec('+I', f.row));
        }
        firings.push(f);
      }
      return { out: [...out, wm(item.time)], ev: { type: firings.length ? 'fire' : 'wm', time: item.time, firings, rows: out.map((o) => o.row) } };
    },
    view(st, cfg) {
      const kl = cfg.keyLabel || cfg.key;
      return {
        title: `Accumulator per ${kl} and ${cfg.kind === 'tumble' ? 'window' : 'slice'}`,
        cols: [kl, cfg.kind === 'tumble' ? 'window' : 'slice', 'orders', 'sales'],
        rows: Object.entries(st.slices)
          .sort(([, a], [, b]) => a.start - b.start || a.k - b.k)
          .map(([key, s]) => ({ key, cells: [s.k, `${hm(s.start)}–${hm(s.cover || s.end)}`, s.count, U.money(s.sum)] })),
        footer: `timers: ${timerList(st).map(hm).join(', ') || 'none'}`,
      };
    },
    say(ev) {
      switch (ev.type) {
        case 'new-slice': return `The record opens the ${ev.slice.start}–${ev.slice.end} slice, with a timer for ${ev.timer}.`;
        case 'accumulate': return `The record is added to the ${ev.slice.start}–${ev.slice.end} slice.`;
        case 'late': return `The record's window has already fired, so it's late, and dropped.`;
        case 'fire': return `The watermark passes the end of a window, which fires.`;
        case 'wm': return `The watermark hasn't reached any timer, so nothing fires. It's forwarded.`;
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
    say(ev) {
      switch (ev.type) {
        case 'new-session': return `The record starts a new session, ${ev.now}.`;
        case 'merge': return `The record's window overlaps an existing session, so they merge into ${ev.now}.`;
        case 'late': return `The record's session has already fired, so it's late, and dropped.`;
        case 'fire': return `The watermark passes the end of a session, which fires.`;
        case 'wm': return `No session has ended yet, so nothing fires. The watermark is forwarded.`;
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
        rows: Object.entries(st.keys).map(([k, s]) => ({ key: k, cells: [k, s.waiting ? s.waiting[cfg.timeField] : '–', s.emitted ? 'yes' : 'no'] })),
        footer: `timers: ${Object.values(st.keys).filter((s) => s.waiting).map((s) => s.waiting[cfg.timeField]).sort().join(', ') || 'none'}`,
      };
    },
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
      const k = item.row[cfg.key];
      const accumulate = item.kind === '+I' || item.kind === '+U';
      let a = st.acc[k];
      const first = !a;
      if (first && !accumulate) return { out: [], absorb: true, ev: { type: 'retract-missing', key: k } };
      if (first) a = { n: 0, vals: Object.fromEntries(cfg.aggs.map((g) => [g.name, 0])) };
      const value = (vals) => ({ [cfg.key]: k, ...vals });
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
          key: k, cells: [k, ...cfg.aggs.map((g) => (g.fn === 'sum' ? U.money(a.vals[g.name]) : a.vals[g.name]))],
        })),
      };
    },
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
    kafkaSource, upsertKafkaSource, calc, changelogNormalize, windowAggregate, sessionWindowAggregate,
    groupAggregate, procTimeDedup, rowTimeDedup,
    kafkaSink, upsertKafkaSink, changelogKafkaSink,
  };
})();
