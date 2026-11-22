// Exports patterns as replay specs for verification/harness (Replay.java), which runs each one
// through real Flink and compares the sink output with the simulator's.
//
//   node tools/export-replay.mjs [pattern/variant ...]     (default: the patterns listed below)
//
// It loads the page's own util.js, operators.js, sim.js and pattern files into a VM, runs the
// simulator, and writes verification/runtime/<pattern>-<variant>.json with:
//   - the SQL, and every table from the DDL (columns, primary key, watermark, connector)
//   - the input sequence, in the order the simulator reads it: rows with their row kinds, and
//     watermarks with the exact times the simulator's sources emitted
//   - the rows each sink receives in the simulator, with their row kinds
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const REPO = join(dirname(fileURLToPath(import.meta.url)), '..');
const DEFAULT = ['windows/tumble', 'dedup/eventtime', 'over/range', 'windowjoin/fullouter', 'enrichment/temporal'];

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

  const spec = {
    pattern: pid, variant: vid, label: `${pattern.title} / ${variant.label}`,
    sql: variant.sql.trim(),
    sources: Object.fromEntries(sources.map((n) => [tableOf(n), tables[tableOf(n)]])),
    sinks: Object.fromEntries(sinks.map((n) => [tableOf(n), tables[tableOf(n)]])),
    inputs, expected,
  };
  const out = join(REPO, 'verification', 'runtime', `${pid}-${vid}.json`);
  mkdirSync(dirname(out), { recursive: true });
  writeFileSync(out, JSON.stringify(spec, null, 2) + '\n');
  console.log(`wrote verification/runtime/${pid}-${vid}.json (${inputs.length} inputs, ${expected.length} expected rows)`);
}

for (const id of process.argv.slice(2).length ? process.argv.slice(2) : DEFAULT) exportOne(id);
