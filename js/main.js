// Mounts every registered pattern and builds the table of contents.
'use strict';

(() => {
  const host = document.getElementById('patterns');
  // The contents list appears twice: in the sidebar on wide screens, and in a popover behind a
  // floating button otherwise.
  const tocs = [document.getElementById('toc-list'), document.getElementById('toc-pop-list')];
  const tocItem = (href, text, sub) => U.el('li', {}, U.el('a', { href }, text), sub);
  const tocAdd = (...args) => tocs.forEach((t) => t.append(tocItem(...args.map((a) => typeof a === 'function' ? a() : a))));

  tocAdd('#reading', 'How to read the diagrams');
  const mounted = {};
  for (const p of window.PATTERNS) {
    mounted[p.id] = Player.mount(host, p);
    const sub = () => p.variants.length > 1
      ? U.el('ol', {}, ...p.variants.map((v) => tocItem(`#${p.id}-${v.id}`, v.label)))
      : null;
    tocAdd(`#${p.id}`, p.nav || p.title, sub);
  }
  tocAdd('#summary', 'Summary');

  // The summary table: one row per variant, built from the same specs as the animations.
  const modeText = { append: 'append-only', upsert: 'upsert', retract: 'retract' };
  const matrixRows = window.PATTERNS.flatMap((p, pi) => p.variants.map((v, i) => {
    const sinkEdge = v.graph.edges.find((e) => v.graph.nodes.find((n) => n.id === e.to).kind === 'sink');
    const planner = v.graph.nodes.filter((n) => n.planner).map((n) => n.title);
    const href = p.variants.length > 1 ? `#${p.id}-${v.id}` : `#${p.id}`;
    const cell = (label, ...kids) => U.el('td', { 'data-label': label }, ...kids);
    // Every other pattern is shaded, so each pattern's rows read as one band.
    return U.el('tr', { class: [i === 0 && 'group-start', pi % 2 && 'shade'].filter(Boolean).join(' ') },
      U.el('th', { scope: 'row' }, U.el('a', { href },
        U.el('span', { class: 'm-pattern' }, p.nav || p.title),
        p.variants.length > 1 ? U.el('span', { class: 'm-variant' }, v.label) : null)),
      cell('State', v.matrix.state),
      cell('Watermarks used for', v.matrix.watermarks),
      // Operators that make a new watermark, instead of forwarding the one they got, stand out.
      U.el('td', { 'data-label': 'Output watermark', class: v.matrix.watermarksOut.startsWith('Passed through') ? '' : 'm-new-wm' }, v.matrix.watermarksOut),
      cell('Output', U.el('span', { class: `badge mode-${sinkEdge.mode}` }, modeText[sinkEdge.mode])),
      cell('Added by the planner', planner.length ? planner.join(', ') : '–'));
  }));
  document.querySelector('#summary .matrix-wrap').append(U.el('table', {},
    U.el('thead', {}, U.el('tr', {}, ...['Pattern', 'State', 'Watermarks used for', 'Output watermark', 'Output', 'Added by the planner'].map((h) => U.el('th', { scope: 'col' }, h)))),
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

  // Mark the section in view in both contents lists.
  const links = [...document.querySelectorAll('#toc-list a, #toc-pop-list a')];
  const io = new IntersectionObserver((entries) => {
    for (const en of entries) {
      if (!en.isIntersecting) continue;
      for (const a of links) {
        if (a.getAttribute('href') === `#${en.target.id}`) a.setAttribute('aria-current', 'true');
        else a.removeAttribute('aria-current');
      }
    }
  }, { rootMargin: '-30% 0px -60% 0px' });
  document.querySelectorAll('#reading, .pattern, #summary').forEach((s) => io.observe(s));

  // The floating contents button: choosing a link closes the popover, and opening it brings the
  // current section into view in the list.
  const pop = document.getElementById('toc-pop');
  pop.addEventListener('click', (e) => { if (e.target.closest('a')) pop.hidePopover(); });
  pop.addEventListener('toggle', (e) => {
    if (e.newState === 'open') pop.querySelector('[aria-current]')?.scrollIntoView({ block: 'nearest' });
  });

  // The theme toggle. With no choice saved, the page follows the system setting; choosing the
  // system's own theme forgets the choice, so the page goes back to following the system.
  const root = document.documentElement;
  const themeBtn = document.querySelector('.theme-toggle');
  const systemDark = window.matchMedia('(prefers-color-scheme: dark)');
  const isDark = () => (root.dataset.theme ? root.dataset.theme === 'dark' : systemDark.matches);
  const syncTheme = () => {
    themeBtn.setAttribute('aria-pressed', String(isDark()));
    themeBtn.title = isDark() ? 'Switch to the light theme' : 'Switch to the dark theme';
  };
  themeBtn.addEventListener('click', () => {
    const dark = !isDark();
    try {
      if (dark === systemDark.matches) { delete root.dataset.theme; localStorage.removeItem('theme'); }
      else { root.dataset.theme = dark ? 'dark' : 'light'; localStorage.setItem('theme', root.dataset.theme); }
    } catch { root.dataset.theme = dark ? 'dark' : 'light'; }
    syncTheme();
  });
  systemDark.addEventListener('change', syncTheme);
  syncTheme();

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
