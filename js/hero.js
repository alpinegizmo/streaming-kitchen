// The hero's stream: a non-windowed aggregation in miniature. Orders drift right into a
// GroupAggregate, which keeps a running total for each store. Every order changes its store's
// total: its amount flashes beside the store's row, the sum rolls up, and the new total drifts
// out: +I the first time a store is seen, +U after that. A newer result for a store supersedes the
// one ahead of it, so that one fades.
'use strict';

(() => {
  const strip = document.querySelector('.hero-stream');
  if (!strip) return;

  let SPEED = 80;               // px per second; slower on a phone
  const GAP = 32;               // px between orders on the way in
  const money = (cents) => `$${(cents / 100).toFixed(2)}`;

  // One pass of input, repeated with new order ids. Store 12 first turns up a few orders in,
  // after the strip has started, so its +I is in view.
  const SCRIPT = [
    { store: 7, cents: 1800 }, { store: 9, cents: 2450 }, { store: 7, cents: 3100 },
    { store: 7, cents: 1300 }, { store: 9, cents: 1900 }, { store: 12, cents: 2200 },
    { store: 9, cents: 1600 }, { store: 7, cents: 2750 }, { store: 12, cents: 1450 },
    { store: 9, cents: 2100 },
  ];
  let next = 0;
  const nextOrder = () => ({ ...SCRIPT[next % SCRIPT.length], id: 101 + next++ });

  const pill = (kind, cls, text) =>
    U.el('span', { class: `hero-pill hs-item ${cls}` }, U.el('span', { class: `rk rk-${kind}` }, kind === 'UA' ? '+U' : '+I'), text);
  const render = (it) => {
    it.el.style.transform = `translate(${it.x}px, -50%) scale(${it.scale ?? 1})`;
    it.el.style.opacity = it.opacity ?? 1;
  };

  // The aggregate, centred in the strip: its state as a small table, headed by the query's own
  // column names, with one row per store in the order they were seen.
  const head = U.el('div', { class: 'hs-row hs-head' }, U.el('span', {}, 'store_id'), U.el('span', {}), U.el('span', {}, 'SUM(total)'));
  const agg = { el: U.el('div', { class: 'hs-agg' }, head), took: 0 };
  const state = new Map();      // store -> { cents, row, cells, anim }
  let emitted = 0;
  // The streams either side: append-only into the aggregate, upsert out of it, coloured like the
  // stream-type badges. Their texture drifts with the records.
  const flowIn = U.el('div', { class: 'hs-flow mode-append' });
  const flowOut = U.el('div', { class: 'hs-flow mode-upsert' });
  strip.append(flowIn, flowOut, agg.el);
  let drift = 0;
  let width = 0, aggW = 0, aggLeft = 0;
  const narrow = () => width < 600;
  const measure = () => {
    width = strip.clientWidth;
    aggW = agg.el.offsetWidth;
    SPEED = Math.min(80, Math.max(45, width / 14));
    aggLeft = Math.round((width - aggW) / 2);
    agg.el.style.left = `${aggLeft}px`;
    flowIn.style.width = `${aggLeft + 2}px`;
    flowOut.style.left = `${aggLeft + aggW - 2}px`;
  };

  let items = [];
  const add = (it) => { strip.insertBefore(it.el, agg.el); it.w = it.el.offsetWidth; items.push(it); return it; };
  // The incoming queue moves like a conveyor: tail is where the last order's left edge would be,
  // even after the aggregate has taken it in.
  let tail = 0;
  const spawn = () => {
    if (tail - GAP < 0) return;
    const o = nextOrder();
    const it = add({ ...o, el: pill('I', '', narrow() ? `store ${o.store} ${money(o.cents)}` : `${o.id} store ${o.store} ${money(o.cents)}`), phase: 'in' });
    it.x = tail = Math.min(tail - GAP, 0) - it.w;
  };

  // Adding an order to its store's sum: the order's amount shows (beside the row, or on a phone in
  // place of the sum), then the sum rolls up to its new value, and then the result goes out.
  const ADD = { show: 0.15, roll: 0.45, done: 0.9 };
  const ADD_NARROW = { show: 0.45, roll: 0.4, done: 1.1 };
  const timing = () => (narrow() ? ADD_NARROW : ADD);
  const accumulate = (o) => {
    let s = state.get(o.store);
    if (!s) {
      const cells = [U.el('span', {}, String(o.store)), U.el('span', { class: 'hs-delta' }), U.el('span', { class: 'hs-sum' })];
      s = { cents: 0, row: U.el('div', { class: 'hs-row' }, ...cells), cells, anim: null, seen: false };
      state.set(o.store, s);
      agg.el.append(s.row);
    }
    if (s.anim) finish(o.store, s);
    s.anim = { from: s.cents, to: s.cents + o.cents, delta: o.cents, t: 0 };
    s.cents += o.cents;
    agg.took = 0.35;
  };
  const finish = (store, s) => {
    const first = !s.seen;
    s.seen = true; s.anim = null;
    // The new result supersedes any earlier one for this store that's still in view.
    for (const it of items) if (it.result && it.store === store) { it.opacity = 0.4; it.el.classList.add('hs-stale'); }
    const res = add({
      el: pill(first ? 'I' : 'UA', 'hs-result', `store ${store} ${money(s.cents)}`),
      phase: 'out', age: 0, result: true, store,
    });
    res.x = aggLeft + aggW + 12;
    emitted++;
  };

  // Scale for a result popping out of the aggregate: from 0.6, overshooting a little, to 1.
  const popIn = (p) => { const c = 1.70158, q = p - 1; return 0.6 + 0.4 * (1 + (c + 1) * q * q * q + c * q * q); };

  function step(dt) {
    tail += SPEED * dt;
    drift += SPEED * dt;
    spawn();
    for (const it of items) {
      it.x += SPEED * dt;
      if (it.phase === 'out') {
        it.age += dt; it.scale = popIn(Math.min(it.age / 0.4, 1));
        if (it.x > width) it.gone = true;
      } else if (it.x >= aggLeft) {
        it.gone = true;
        accumulate(it);
      }
    }
    for (const it of items) if (it.gone) it.el.remove();
    items = items.filter((it) => !it.gone);
    for (const [store, s] of state) {
      if (s.anim && (s.anim.t += dt) >= timing().done) finish(store, s);
    }
    agg.took = Math.max(0, agg.took - dt);
  }

  function paint() {
    items.forEach(render);
    flowIn.style.backgroundPositionX = flowOut.style.backgroundPositionX = `${drift % 48}px`;
    const T = timing();
    for (const s of state.values()) {
      const a = s.anim, [, delta, sum] = s.cells;
      const p = a ? Math.min(Math.max((a.t - T.show) / T.roll, 0), 1) : 1;
      const ease = 1 - (1 - p) ** 3;
      const shown = a ? Math.round(a.from + (a.to - a.from) * ease) : s.cents;
      delta.textContent = a ? `+${money(a.delta)}` : '';
      // Beside the row the amount fades once it's been added; on a phone it stands in for the sum.
      const inPlace = narrow() && a && a.t < T.show;
      delta.style.opacity = !a ? 0 : narrow() ? (inPlace ? 1 : 0) : Math.min(1, (T.done - a.t) / 0.3);
      sum.textContent = inPlace ? '' : a && a.from === 0 && a.t < T.show ? '' : money(shown);
      s.row.classList.toggle('hot', !!a);
      s.row.classList.toggle('in-place', !!inPlace);
    }
    agg.el.classList.toggle('took', agg.took > 0);
  }

  measure();
  // Run ahead until two results are out, so the strip starts full.
  for (let t = 0; t < 120 && emitted < 2; t += 1 / 60) step(1 / 60);
  for (let t = 0; t < 1; t += 1 / 60) step(1 / 60);
  for (const [store, s] of state) if (s.anim) finish(store, s);
  agg.took = 0;
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
  }).observe(strip);
  window.addEventListener('resize', measure);
})();
