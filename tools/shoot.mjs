// Screenshots the site in headless Chrome, and reports console errors and exceptions.
//
//   node tools/shoot.mjs <out.png> [--url URL] [--width 1280] [--height 900] [--dark]
//        [--full] [--selector CSS] [--eval JS] [--wait MS] [--reduced-motion]
//
// --eval runs after the page loads (it may return a promise), then --wait passes,
// then the screenshot is taken: of the whole page with --full, of one element with
// --selector, otherwise of the viewport. Chrome is expected at its default macOS
// location; set CHROME to use another binary.
import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const REPO = join(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const flag = (name) => { const i = args.indexOf(name); return i < 0 ? false : (args.splice(i, 1), true); };
const opt = (name, dflt) => { const i = args.indexOf(name); return i < 0 ? dflt : args.splice(i, 2)[1]; };

const url = opt('--url', pathToFileURL(join(REPO, 'index.html')).href);
const width = Number(opt('--width', 1280));
const height = Number(opt('--height', 900));
const dark = flag('--dark');
const full = flag('--full');
const reduced = flag('--reduced-motion');
const selector = opt('--selector', null);
const evalJs = opt('--eval', null);
const wait = Number(opt('--wait', 300));
const out = resolve(args[0] || 'shot.png');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const chromeBin = process.env.CHROME || '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome';
const profile = mkdtempSync(join(tmpdir(), 'shoot-'));
const chrome = spawn(chromeBin, [
  '--headless=new', '--remote-debugging-port=0', `--user-data-dir=${profile}`,
  '--no-first-run', '--no-default-browser-check', '--hide-scrollbars', 'about:blank',
], { stdio: 'ignore' });

let ws;
let failed = false;
try {
  const portFile = join(profile, 'DevToolsActivePort');
  for (let i = 0; i < 100 && !existsSync(portFile); i++) await sleep(100);
  const port = readFileSync(portFile, 'utf8').split('\n')[0];
  const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
  const page = targets.find((t) => t.type === 'page');
  ws = new WebSocket(page.webSocketDebuggerUrl);
  await new Promise((r, j) => { ws.onopen = r; ws.onerror = j; });

  let id = 0;
  const pending = new Map();
  const listeners = [];
  ws.onmessage = (m) => {
    const msg = JSON.parse(m.data);
    if (msg.id && pending.has(msg.id)) {
      const { res, rej } = pending.get(msg.id);
      pending.delete(msg.id);
      msg.error ? rej(new Error(msg.error.message)) : res(msg.result);
    } else listeners.forEach((f) => f(msg));
  };
  const send = (method, params = {}) => new Promise((res, rej) => {
    const n = ++id;
    pending.set(n, { res, rej });
    ws.send(JSON.stringify({ id: n, method, params }));
  });

  listeners.push((msg) => {
    if (msg.method === 'Runtime.exceptionThrown') {
      failed = true;
      const d = msg.params.exceptionDetails;
      console.error('exception:', d.exception?.description || d.text);
    }
    if (msg.method === 'Runtime.consoleAPICalled' && ['error', 'warning'].includes(msg.params.type)) {
      if (msg.params.type === 'error') failed = true;
      console.error(`console.${msg.params.type}:`, msg.params.args.map((a) => a.value ?? a.description).join(' '));
    }
    if (msg.method === 'Log.entryAdded' && msg.params.entry.level === 'error') {
      console.error('log:', msg.params.entry.text, msg.params.entry.url || '');
    }
  });

  await send('Runtime.enable');
  await send('Log.enable');
  await send('Page.enable');
  await send('Emulation.setDeviceMetricsOverride', { width, height, deviceScaleFactor: 2, mobile: width < 600 });
  await send('Emulation.setEmulatedMedia', {
    features: [
      { name: 'prefers-color-scheme', value: dark ? 'dark' : 'light' },
      { name: 'prefers-reduced-motion', value: reduced ? 'reduce' : 'no-preference' },
    ],
  });
  const loaded = new Promise((r) => listeners.push((m) => m.method === 'Page.loadEventFired' && r()));
  await send('Page.navigate', { url });
  await loaded;
  await send('Runtime.evaluate', { expression: 'document.fonts.ready', awaitPromise: true });
  await sleep(200);

  if (evalJs) {
    const r = await send('Runtime.evaluate', { expression: evalJs, awaitPromise: true, returnByValue: true });
    if (r.exceptionDetails) { failed = true; console.error('eval failed:', r.exceptionDetails.exception?.description); }
    else if (r.result.value !== undefined) console.log('eval:', JSON.stringify(r.result.value));
  }
  await sleep(wait);

  const shot = { format: 'png' };
  if (selector) {
    const r = await send('Runtime.evaluate', {
      expression: `(() => { const e = document.querySelector(${JSON.stringify(selector)}); e.scrollIntoView({block: 'start'});
        const b = e.getBoundingClientRect(); return { x: b.x + scrollX, y: b.y + scrollY, width: b.width, height: b.height }; })()`,
      returnByValue: true,
    });
    await sleep(150);
    shot.clip = { ...r.result.value, scale: 1 };
    shot.captureBeyondViewport = true;
  } else if (full) {
    shot.captureBeyondViewport = true;
    const m = await send('Page.getLayoutMetrics');
    shot.clip = { x: 0, y: 0, width, height: Math.ceil(m.cssContentSize.height), scale: 1 };
  }
  const { data } = await send('Page.captureScreenshot', shot);
  writeFileSync(out, Buffer.from(data, 'base64'));
  console.log('wrote', out);
} finally {
  ws?.close();
  chrome.kill();
  await sleep(200);
  rmSync(profile, { recursive: true, force: true });
}
process.exit(failed ? 1 : 0);
