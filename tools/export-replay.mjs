// Exports patterns as replay specs for verification/harness (Replay.java), which runs each one
// through real Flink and compares the sink output with the simulator's.
//
//   node tools/export-replay.mjs [pattern/variant ...]     (default: every tab that can be replayed)
//
// It loads the page's own util.js, operators.js, sim.js and pattern files into a VM, runs the
// simulator, and writes verification/runtime/<pattern>-<variant>.json with:
//   - the SQL, and every table from the DDL (columns, primary key, watermark, connector)
//   - the input sequence, in the order the simulator reads it: rows with their row kinds, and
//     watermarks with the exact times the simulator's sources emitted
//   - the rows each sink receives in the simulator, with their row kinds
//   - for each operator with facts() (see operators.js), every item it handled, what it emitted,
//     and its state and timers afterwards
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const REPO = join(dirname(fileURLToPath(import.meta.url)), '..');

// Load the scripts the way the page does: classic scripts sharing one global scope.
const ctx = { console, structuredClone };
ctx.window = ctx;
vm.createContext(ctx);
const html = readFileSync(join(REPO, 'index.html'), 'utf8');
const scripts = [...html.matchAll(/<script src="(js\/[^"]+)"/g)].map((m) => m[1])
  .filter((s) => /^js\/(util|operators|sim)\.js$|^js\/patterns\//.test(s));
for (const s of scripts) vm.runInContext(readFileSync(join(REPO, s), 'utf8'), ctx, { filename: s });
const PATTERNS = vm.runInContext('window.PATTERNS', ctx);
const Sim = vm.runInContext('Sim', ctx);
const Ops = vm.runInContext('Ops', ctx);

// The Flink operators a simulated operator stands for, in the order records pass through them,
// by the name the planner gives their transformations. The last one holds the state.
const FLINK_OPERATORS = {
  groupAggregate: ['GroupAggregate'],
  windowAggregate: ['LocalWindowAggregate', 'GlobalWindowAggregate'],
  sessionWindowAggregate: ['WindowAggregate'],
  overAggregate: ['OverAggregate'],
  changelogNormalize: ['ChangelogNormalize'],
  procTimeDedup: ['Deduplicate'],
  rowTimeDedup: ['Deduplicate'],
  retractRank: ['Rank'],
  fastTop1: ['Rank'],
  upsertMaterializer: ['SinkMaterializer'],
  match: ['Match'],
  regularJoin: ['Join'],
  intervalJoin: ['IntervalJoin'],
  windowJoin: ['WindowJoin'],
  temporalJoin: ['TemporalJoin'],
  lateralSnapshotJoin: ['LateralSnapshotJoin'],
};
// The simulator's names for output fields the planner names itself. OVER: COUNT(*) is w0$o0, and
// SUM(total) is computed from COUNT(total) (w0$o1, the same as the order count, since total is
// never null) and $SUM0(total) (w0$o2).
const FLINK_FIELDS = {
  overAggregate: (cfg) => ({ w0$o0: cfg.countName, w0$o1: cfg.countName, w0$o2: cfg.sumName }),
};
// Output fields the pattern's SQL takes from either side of an outer join, with COALESCE(left,
// right): the simulator's rows hold the coalesced value. The planner names the right side's copy
// with a 0 suffix.
const FLINK_COALESCE = {
  windowJoin: { store_id: 'store_id0', window_start: 'window_start0', window_end: 'window_end0' },
};
const item = (i) => (i.t === 'wm' ? { wm: i.time } : { kind: i.kind, row: i.row });

// Reads CREATE TABLE statements: one column, key or watermark per line, as the patterns write them.
function parseDdl(ddl) {
  const tables = {};
  for (const m of ddl.matchAll(/CREATE TABLE (\w+) \(([\s\S]*?)\n\) WITH \(([\s\S]*?)\);/g)) {
    const [, name, body, opts] = m;
    const t = { columns: [], pk: [], watermark: null, options: {} };
    for (const o of opts.matchAll(/'([^']+)'\s*=\s*'([^']*)'/g)) t.options[o[1]] = o[2];
    for (let line of body.split('\n')) {
      line = line.replace(/--.*$/, '').trim().replace(/,$/, '');
      if (!line) continue;
      let k;
      if ((k = line.match(/^PRIMARY KEY \(([^)]+)\)/))) t.pk = k[1].split(',').map((x) => x.trim());
      else if ((k = line.match(/^WATERMARK FOR (\w+) AS (.+)$/))) t.watermark = { column: k[1], expr: k[2] };
      else if ((k = line.match(/^(\w+)\s+AS\s+(.+)$/))) t.columns.push({ name: k[1], expr: k[2] });
      else if ((k = line.match(/^(\w+)\s+(.+)$/))) t.columns.push({ name: k[1], type: k[2].trim() });
    }
    t.mode = t.options.connector === 'upsert-kafka' ? 'upsert' : t.options.format === 'debezium-json' ? 'retract' : 'append';
    tables[name] = t;
  }
  return tables;
}

function exportOne(id) {
  const [pid, vid] = id.split('/');
  const pattern = PATTERNS.find((p) => p.id === pid);
  const variant = pattern?.variants.find((v) => v.id === vid);
  if (!variant) throw new Error(`no such pattern/variant: ${id}`);
  const tables = parseDdl(variant.ddl);
  const tableOf = (node) => Object.keys(tables).find((t) => tables[t].options.topic === node.cfg.topic);
  const byId = Object.fromEntries(variant.graph.nodes.map((n) => [n.id, n]));
  const sources = variant.graph.nodes.filter((n) => n.kind === 'source');
  const sinks = variant.graph.nodes.filter((n) => n.kind === 'sink');

  const { steps } = Sim.run(variant);
  const inputs = steps.filter((s) => s.via === null).map((s) => {
    const table = tableOf(byId[s.node]);
    const o = s.out[0];
    return o.t === 'wm' ? { table, wm: o.time } : { table, kind: o.kind, row: o.row };
  });
  const sinkIds = new Set(sinks.map((n) => n.id));
  const expected = steps.filter((s) => sinkIds.has(s.node) && s.item.t === 'rec')
    .map((s) => ({ sink: tableOf(byId[s.node]), kind: s.item.kind, row: s.item.row }));

  const upstreamTables = (id) => {
    const found = new Set();
    const walk = (to) => {
      if (byId[to].kind === 'source') found.add(tableOf(byId[to]));
      for (const e of variant.graph.edges) if (e.to === to && e.mode !== 'lookup') walk(e.from);
    };
    walk(id);
    return [...found].sort();
  };
  // What facts() needs to know about an operator's inputs: whether they carry retractions, which
  // changes the accumulators Flink keeps.
  const factsContext = (id) => ({
    retract: variant.graph.edges.some((e) => e.to === id && e.mode !== 'append' && e.mode !== 'lookup'),
  });
  // For each operator whose state can be checked: every item it received, what it emitted, and
  // its state afterwards (verification/run-state.sh runs the Flink operator on the same items).
  const operators = variant.graph.nodes.filter((n) => Ops[n.op].facts && FLINK_OPERATORS[n.op]).map((n) => ({
    node: n.id, op: n.op, title: n.title,
    flink: n.cfg?.flinkOperators || FLINK_OPERATORS[n.op],
    // The source tables upstream of it, to tell apart operators of the same kind.
    upstream: upstreamTables(n.id),
    fields: FLINK_FIELDS[n.op]?.(n.cfg || {}),
    coalesce: FLINK_COALESCE[n.op],
    steps: steps.filter((s) => s.node === n.id).map((s) => ({
      // The source tables behind the edge the item came on, to pick a two-input operator's input.
      from: upstreamTables(variant.graph.edges[s.via].from),
      item: item(s.item),
      out: s.out.map(item),
      facts: Ops[n.op].facts(s.snap.ops[n.id], n.cfg || {}, factsContext(n.id)),
      caption: s.caption,
    })),
  }));

  const spec = {
    pattern: pid, variant: vid, label: `${pattern.title} / ${variant.label}`,
    // Tabs checked on a snapshot build say so; the replay runs them on that build.
    flinkVersion: variant.flinkVersion || null,
    sql: variant.sql.trim(),
    sources: Object.fromEntries(sources.map((n) => [tableOf(n), tables[tableOf(n)]])),
    sinks: Object.fromEntries(sinks.map((n) => [tableOf(n), tables[tableOf(n)]])),
    inputs, expected, operators,
  };
  // A LATERAL SNAPSHOT join's load phase ends when the build side's watermark reaches the time the
  // query was compiled. The simulator assumes a compile time; the replay passes it explicitly.
  const lateral = variant.graph.nodes.find((n) => n.op === 'lateralSnapshotJoin');
  if (lateral) spec.loadCompletedTime = lateral.cfg.loadCompletedTime;
  const out = join(REPO, 'verification', 'runtime', `${pid}-${vid}.json`);
  mkdirSync(dirname(out), { recursive: true });
  writeFileSync(out, JSON.stringify(spec, null, 2) + '\n');
  console.log(`wrote verification/runtime/${pid}-${vid}.json (${inputs.length} inputs, ${expected.length} expected rows)`);
}

// Every tab, except the ones with an external system: a lookup join needs a real service.
const all = PATTERNS.flatMap((p) => p.variants.filter((v) => !v.graph.nodes.some((n) => n.kind === 'external')).map((v) => `${p.id}/${v.id}`));
for (const id of process.argv.slice(2).length ? process.argv.slice(2) : all) exportOne(id);
