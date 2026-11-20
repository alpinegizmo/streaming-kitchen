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

  // Window aggregation over a TUMBLE window TVF (LocalWindowAggregate + GlobalWindowAggregate,
  // drawn as one operator). One accumulator per key and window, and a timer at the window's end.
  // A window fires when the watermark reaches its end (strictly: end - 1 ms); its result is
  // emitted as an insert and its state is freed. A record whose window has already fired is
  // late, and dropped. Mirrors AbstractSliceSyncStateWindowAggProcessor and TimeWindowUtil.
  //   cfg: { size (seconds), key, timeField, sumField }
  const windowAggregate = {
    init: () => ({ acc: {}, wm: null }),
    record(st, item, cfg) {
      const row = item.row;
      const ts = U.toSec(row[cfg.timeField]);
      const start = Math.floor(ts / cfg.size) * cfg.size;
      const end = start + cfg.size;
      const win = { start: U.fromSec(start).slice(0, 5), end: U.fromSec(end).slice(0, 5) };
      if (st.wm != null && U.toSec(st.wm) >= end) {
        return { out: [], ev: { type: 'late', row, win, wm: st.wm } };
      }
      const key = `${row[cfg.key]}|${start}`;
      const created = !st.acc[key];
      const a = st.acc[key] || (st.acc[key] = { k: row[cfg.key], start, end, count: 0, sum: 0 });
      a.count += 1;
      a.sum = Math.round((a.sum + row[cfg.sumField]) * 100) / 100;
      return { out: [], absorb: true, touched: key, ev: { type: created ? 'new-window' : 'accumulate', row, win, acc: { ...a } } };
    },
    watermark(st, item, cfg) {
      st.wm = item.time;
      const now = U.toSec(item.time);
      const fired = Object.entries(st.acc)
        .filter(([, a]) => now >= a.end)
        .sort(([, a], [, b]) => a.end - b.end || a.k - b.k);
      const out = fired.map(([key, a]) => {
        delete st.acc[key];
        return rec('+I', {
          window_start: U.fromSec(a.start).slice(0, 5), window_end: U.fromSec(a.end).slice(0, 5),
          [cfg.key]: a.k, orders: a.count, sales: a.sum,
        });
      });
      const windows = [...new Set(fired.map(([, a]) => `${U.fromSec(a.start).slice(0, 5)}–${U.fromSec(a.end).slice(0, 5)}`))];
      return { out: [...out, wm(item.time)], ev: { type: fired.length ? 'fire' : 'wm', time: item.time, windows, rows: out.map((o) => o.row) } };
    },
    view(st, cfg) {
      return {
        title: `Accumulator per ${cfg.keyLabel || cfg.key} and window`,
        cols: [cfg.keyLabel || cfg.key, 'window', 'orders', 'sales'],
        rows: Object.entries(st.acc)
          .sort(([, a], [, b]) => a.start - b.start || a.k - b.k)
          .map(([key, a]) => ({ key, cells: [a.k, `${U.fromSec(a.start).slice(0, 5)}–${U.fromSec(a.end).slice(0, 5)}`, a.count, U.money(a.sum)] })),
      };
    },
    say(ev) {
      switch (ev.type) {
        case 'new-window': return `The record opens a new accumulator for the ${ev.win.start}–${ev.win.end} window.`;
        case 'accumulate': return `The record is added to the accumulator for the ${ev.win.start}–${ev.win.end} window.`;
        case 'late': return `The record's window has already fired, so it's late, and dropped.`;
        case 'fire': return `The watermark passes the end of a window, which fires: its results are emitted and its state is freed.`;
        case 'wm': return `The watermark hasn't reached the end of any window, so nothing fires. It's forwarded.`;
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
    kafkaSource, upsertKafkaSource, calc, changelogNormalize, windowAggregate, groupAggregate,
    kafkaSink, upsertKafkaSink, changelogKafkaSink,
  };
})();
