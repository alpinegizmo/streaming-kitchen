// Bundles the site into one standalone HTML file, for sharing: the stylesheet and every script
// are inlined, and the web fonts (latin subset) are embedded as data: URLs, so the file works
// offline and makes no requests.
//
//   node tools/bundle.mjs [out.html]        default: dist/flink-sql-patterns.html
//   node tools/bundle.mjs --no-fonts        keep the Google Fonts link instead of embedding
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const embedFonts = !args.includes('--no-fonts');
const out = resolve(args.find((a) => !a.startsWith('--')) || join(root, 'dist', 'flink-sql-patterns.html'));

let html = readFileSync(join(root, 'index.html'), 'utf8');
const read = (path) => readFileSync(join(root, path), 'utf8');

// Inline scripts. A literal "</script" inside a script would end the element early.
html = html.replace(/<script src="([^"]+)"><\/script>/g, (_, src) =>
  `<script>\n${read(src).replace(/<\/script/gi, '<\\/script')}\n</script>`);

// Inline the stylesheet.
html = html.replace(/<link rel="stylesheet" href="(css\/[^"]+)">/, (_, href) => `<style>\n${read(href)}\n</style>`);

if (embedFonts) {
  const link = html.match(/<link href="(https:\/\/fonts\.googleapis\.com\/css2[^"]+)" rel="stylesheet">/);
  if (!link) throw new Error('Google Fonts link not found in index.html');
  // A modern browser user agent makes Google Fonts serve woff2.
  const ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36';
  const res = await fetch(link[1].replace(/&amp;/g, '&'), { headers: { 'User-Agent': ua } });
  if (!res.ok) throw new Error(`Google Fonts CSS: HTTP ${res.status}`);
  const css = await res.text();
  // Keep only the latin subset: the page has no other scripts.
  const blocks = [...css.matchAll(/\/\* latin \*\/\s*(@font-face\s*\{[^}]+\})/g)].map((m) => m[1]);
  if (!blocks.length) throw new Error('no latin @font-face blocks in the Google Fonts CSS');
  const faces = [];
  for (const block of blocks) {
    const url = block.match(/url\((https:[^)]+\.woff2)\)/)[1];
    const font = await fetch(url);
    if (!font.ok) throw new Error(`font ${url}: HTTP ${font.status}`);
    const b64 = Buffer.from(await font.arrayBuffer()).toString('base64');
    faces.push(block.replace(url, `data:font/woff2;base64,${b64}`));
  }
  html = html
    .replace(/\s*<link rel="preconnect"[^>]*>/g, '')
    .replace(link[0], `<style>\n${faces.join('\n')}\n</style>`);
}

mkdirSync(dirname(out), { recursive: true });
writeFileSync(out, html);
console.log(`wrote ${out} (${Math.round(Buffer.byteLength(html) / 1024)} KB${embedFonts ? ', fonts embedded' : ''})`);
