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
      return {
        title: cfg.stateTitle || `Last row per ${cfg.key}`,
        rows: Object.entries(st.rows).map(([key, row]) => ({ key, text: cfg.cells ? cfg.cells(row) : JSON.stringify(row) })),
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

  return { kafkaSource, upsertKafkaSource, calc, changelogNormalize, kafkaSink, upsertKafkaSink };
})();
