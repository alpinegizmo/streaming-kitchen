// Mounts every registered pattern and builds the table of contents.
'use strict';

(() => {
  const host = document.getElementById('patterns');
  const toc = document.getElementById('toc-list');

  toc.append(U.el('li', {}, U.el('a', { href: '#reading' }, 'How to read the diagrams')));
  const mounted = {};
  for (const p of window.PATTERNS) {
    mounted[p.id] = Player.mount(host, p);
    const sub = p.variants.length > 1
      ? U.el('ol', {}, ...p.variants.map((v) => U.el('li', {}, U.el('a', { href: `#${p.id}-${v.id}` }, v.label))))
      : null;
    toc.append(U.el('li', {}, U.el('a', { href: `#${p.id}` }, p.nav || p.title), sub));
  }
  toc.append(U.el('li', {}, U.el('a', { href: '#summary' }, 'Summary')));

  // The summary table: one row per variant, built from the same specs as the animations.
  const modeText = { append: 'append-only', upsert: 'upsert', retract: 'retract' };
  const matrixRows = window.PATTERNS.flatMap((p) => p.variants.map((v, i) => {
    const sinkEdge = v.graph.edges.find((e) => v.graph.nodes.find((n) => n.id === e.to).kind === 'sink');
    const planner = v.graph.nodes.filter((n) => n.planner).map((n) => n.title);
    const href = p.variants.length > 1 ? `#${p.id}-${v.id}` : `#${p.id}`;
    const cell = (label, ...kids) => U.el('td', { 'data-label': label }, ...kids);
    return U.el('tr', { class: i === 0 ? 'group-start' : '' },
      U.el('th', { scope: 'row' }, U.el('a', { href },
        U.el('span', { class: 'm-pattern' }, p.nav || p.title),
        p.variants.length > 1 ? U.el('span', { class: 'm-variant' }, v.label) : null)),
      cell('State', v.matrix.state),
      cell('Watermarks', v.matrix.watermarks),
      cell('Output', U.el('span', { class: `badge mode-${sinkEdge.mode}` }, modeText[sinkEdge.mode])),
      cell('Added by the planner', planner.length ? planner.join(', ') : '–'));
  }));
  document.querySelector('#summary .matrix-wrap').append(U.el('table', {},
    U.el('thead', {}, U.el('tr', {}, ...['Pattern', 'State', 'Watermarks', 'Output', 'Added by the planner'].map((h) => U.el('th', { scope: 'col' }, h)))),
    U.el('tbody', {}, ...matrixRows)));

  // Deep links: #windows-hop opens the Hop tab of the windows section. Any link into the page
  // is scrolled to again once the graphs above have sized themselves, which moves everything down.
  function follow(e) {
    const hash = decodeURIComponent(location.hash.slice(1));
    if (!hash) return;
    const p = window.PATTERNS.find((x) => hash.startsWith(`${x.id}-`) && mounted[x.id].select(hash.slice(x.id.length + 1)));
    const target = p ? mounted[p.id].section : document.getElementById(hash);
    if (target) requestAnimationFrame(() => requestAnimationFrame(() => target.scrollIntoView({ behavior: e && !U.reducedMotion() ? 'smooth' : 'instant' })));
  }
  window.addEventListener('hashchange', follow);
  if (document.readyState === 'complete') follow();
  else window.addEventListener('load', () => follow(), { once: true });

  // Mark the section in view in the table of contents.
  const links = new Map([...toc.querySelectorAll('a')].map((a) => [a.getAttribute('href').slice(1), a]));
  const io = new IntersectionObserver((entries) => {
    for (const en of entries) {
      if (!en.isIntersecting) continue;
      links.forEach((a) => a.removeAttribute('aria-current'));
      links.get(en.target.id)?.setAttribute('aria-current', 'true');
    }
  }, { rootMargin: '-30% 0px -60% 0px' });
  document.querySelectorAll('#reading, .pattern, #summary').forEach((s) => io.observe(s));

  // The hero's ambient stream: a few orders and a watermark drifting by.
  const track = document.querySelector('.hero-track');
  if (track) {
    const items = [
      ['+I', '101 delivery $18.00'], ['+I', '102 pickup $24.50'], ['wm', 'WM 12:00:01'],
      ['+I', '103 delivery $31.00'], ['+I', '104 delivery $12.00'], ['+I', '105 pickup $22.00'],
      ['wm', 'WM 12:00:04'], ['+I', '106 delivery $27.50'], ['+I', '107 pickup $16.00'],
    ];
    // The stream flows right, so the oldest order leads on the right, and each
    // watermark trails the orders it follows.
    const row = () => [...items].reverse().map(([k, t]) => k === 'wm'
      ? U.el('span', { class: 'wm-chip' }, t)
      : U.el('span', { class: 'hero-pill' }, U.el('span', { class: 'rk rk-I' }, '+I'), t));
    track.append(...row(), ...row());
  }
})();
