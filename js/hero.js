// The hero's job graph in miniature: abstract records flow from the source, along an
// append-only edge, into a stateful operator, which updates a row of its state and sends a
// result along an upsert edge to the sink. Now and then a watermark passes through.
// The graph itself is static SVG in index.html; this only moves the records.
'use strict';

(() => {
  const svg = document.querySelector('.hero-graph');
  if (!svg) return;
  const layer = svg.querySelector('.hg-dots');
  const vals = [...svg.querySelectorAll('.hg-val')];
  const NS = 'http://www.w3.org/2000/svg';

  const SPEED = 34;                              // px per second
  const EDGES = [{ from: 102, to: 160 }, { from: 274, to: 332 }];
  const Y = 64;
  const MIN_W = 16, MAX_W = 86;                  // a state value's width

  const shape = (kind) => {
    const el = document.createElementNS(NS, kind === 'wm' ? 'rect' : 'circle');
    if (kind === 'wm') {
      el.setAttribute('class', 'hg-wm');
      el.setAttribute('width', 6); el.setAttribute('height', 20); el.setAttribute('rx', 3);
    } else {
      el.setAttribute('class', `hg-dot ${kind}`);
      el.setAttribute('r', 5);
    }
    layer.append(el);
    return el;
  };

  let items = [];
  const send = (edge, kind) => items.push({ edge, kind, x: EDGES[edge].from, el: shape(kind) });

  // Each state value eases toward a target; a record arriving at the operator picks a row,
  // gives it a new target, and highlights it.
  const state = vals.map((el) => {
    const w = Number(el.getAttribute('width'));
    return { el, w, target: w, hot: 0 };
  });
  const pending = [];                            // results waiting to leave the operator

  let sinceSpawn = 0, nextGap = 1.2, spawned = 0, row = 0;
  function step(dt) {
    sinceSpawn += dt;
    if (sinceSpawn >= nextGap) {
      sinceSpawn = 0;
      nextGap = 1.0 + Math.random() * 0.9;
      send(0, ++spawned % 6 === 0 ? 'wm' : 'mode-append');
    }
    for (const p of pending) p.t -= dt;
    while (pending.length && pending[0].t <= 0) send(1, pending.shift().kind);

    for (const it of items) {
      it.x += SPEED * dt;
      if (it.x < EDGES[it.edge].to) continue;
      it.gone = true;
      if (it.edge === 1) continue;               // into the sink
      if (it.kind === 'wm') { pending.push({ t: 0.1, kind: 'wm' }); continue; }
      row = (row + 1 + Math.floor(Math.random() * 2)) % state.length;
      const s = state[row];
      s.target = MIN_W + Math.random() * (MAX_W - MIN_W);
      s.hot = 0.6;
      pending.push({ t: 0.3, kind: 'mode-upsert' });
    }
    for (const it of items) if (it.gone) it.el.remove();
    items = items.filter((it) => !it.gone);
    for (const s of state) {
      s.w += (s.target - s.w) * Math.min(1, dt * 5);
      s.hot = Math.max(0, s.hot - dt);
    }
  }

  function paint() {
    for (const it of items) {
      const { from, to } = EDGES[it.edge];
      // Fade in as a record leaves a node, and out as it reaches the next.
      const opacity = Math.min(1, (it.x - from) / 8, (to - it.x) / 8);
      if (it.kind === 'wm') { it.el.setAttribute('x', it.x - 3); it.el.setAttribute('y', Y - 10); }
      else { it.el.setAttribute('cx', it.x); it.el.setAttribute('cy', Y); }
      it.el.setAttribute('opacity', Math.max(0, opacity).toFixed(2));
    }
    for (const s of state) {
      s.el.setAttribute('width', s.w.toFixed(1));
      s.el.classList.toggle('hot', s.hot > 0);
    }
  }

  // Start with records already on both edges.
  for (let t = 0; t < 6; t += 1 / 30) step(1 / 30);
  for (const s of state) s.hot = 0;
  paint();
  if (U.reducedMotion()) return;

  let visible = true, last = null, raf = 0;
  const frame = (now) => {
    if (last != null) step(Math.min((now - last) / 1000, 0.05));
    last = now;
    paint();
    raf = visible ? requestAnimationFrame(frame) : 0;
  };
  new IntersectionObserver(([en]) => {
    visible = en.isIntersecting;
    if (visible && !raf) { last = null; raf = requestAnimationFrame(frame); }
  }).observe(svg);
})();
