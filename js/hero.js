// The hero's stream: a tumbling window in miniature. Orders drift right into a ten-second window,
// which counts them; a watermark closes the window, and its result drifts out ahead of the
// watermark. Now and then an order turns up after the watermark has passed, and is dropped.
'use strict';

(() => {
  const strip = document.querySelector('.hero-stream');
  if (!strip) return;

  let SPEED = 80;               // px per second; slower on a phone, so a result stays in view
  const GAP = 32;               // px between items on the way in
  const LATE_GAP = 150;         // extra room behind a late order, so it can fall clear
  const WINDOW = 10;            // seconds
  const NOON = 12 * 3600;
  const pad = (n) => String(n).padStart(2, '0');
  const time = (s) => `${Math.floor(s / 3600)}:${pad(Math.floor(s / 60) % 60)}:${pad(s % 60)}`;
  const money = (cents) => `$${(cents / 100).toFixed(2)}`;

  // Two windows' worth of input, repeated: each pass shifts the times by 20 s and the ids by 7.
  // Order 106 is stamped 12:00:09 but arrives after the watermark for 12:00:10, so it's late.
  const SCRIPT = [
    { kind: 'order', id: 101, t: 1, cents: 1800 },
    { kind: 'order', id: 102, t: 4, cents: 2450 },
    { kind: 'order', id: 103, t: 7, cents: 3100 },
    { kind: 'wm', t: 10 },
    { kind: 'order', id: 104, t: 12, cents: 1200 },
    { kind: 'order', id: 105, t: 15, cents: 2200 },
    { kind: 'order', id: 106, t: 9, cents: 1600, late: true },
    { kind: 'order', id: 107, t: 18, cents: 2750 },
    { kind: 'wm', t: 20 },
  ];
  let next = 0;
  const nextInput = () => {
    const s = SCRIPT[next % SCRIPT.length], pass = Math.floor(next / SCRIPT.length);
    next++;
    return { ...s, t: NOON + s.t + pass * 20, id: s.id && s.id + pass * 7 };
  };

  const pill = (cls, ...kids) => U.el('span', { class: `hero-pill hs-item ${cls}` }, U.el('span', { class: 'rk rk-I' }, '+I'), ...kids);
  const render = (it) => {
    it.el.style.transform = `translate(${it.x}px, calc(-50% + ${it.y || 0}px)) scale(${it.scale ?? 1})`;
    it.el.style.opacity = it.opacity ?? 1;
  };

  // The window operator, centred in the strip.
  const label = U.el('span', { class: 'hs-label' });
  const value = U.el('span', { class: 'hs-value' });
  const win = { el: U.el('div', { class: 'hs-window' }, label, value), start: NOON, count: 0, cents: 0, flash: 0, took: 0 };
  strip.append(win.el);
  let watermark = -Infinity, width = 0, winW = 0, winLeft = 0;
  // On a phone the strip only has room for counts: orders lose their prices, and the window and
  // its result show how many orders, not how much.
  const orders = (n) => `${n} order${n === 1 ? '' : 's'}`;
  const showWindow = () => {
    const range = `${time(win.start)}–${time(win.start + WINDOW)}`;
    label.textContent = narrow() ? range : `window ${range}`;
    value.textContent = !win.count ? 'empty' : narrow() ? orders(win.count) : `${orders(win.count)}  ${money(win.cents)}`;
  };
  const narrow = () => width < 600;
  const measure = () => {
    width = strip.clientWidth;
    winW = win.el.offsetWidth;
    SPEED = Math.min(80, Math.max(45, width / 14));
    winLeft = Math.round((width - winW) / 2);
    win.el.style.left = `${winLeft}px`;
  };

  let items = [];
  const add = (it) => { strip.insertBefore(it.el, win.el); it.w = it.el.offsetWidth; items.push(it); return it; };
  // The incoming queue moves like a conveyor: tail is where the last input's left edge would be,
  // even after that input has stopped (a late order) or been taken in by the window.
  let tail = 0, tailGap = 0;
  const spawn = () => {
    if (tail - tailGap < 0) return;
    const s = nextInput();
    const el = s.kind === 'wm'
      ? U.el('span', { class: 'wm-chip hs-item' }, `WM ${time(s.t)}`)
      : pill('', narrow() ? `${s.id} ${time(s.t)}` : `${s.id} ${time(s.t)} ${money(s.cents)}`);
    const it = add({ ...s, el, phase: 'in' });
    it.x = tail = Math.min(tail - tailGap, 0) - it.w;
    tailGap = GAP + (s.late ? LATE_GAP : 0);
  };

  const fire = () => {
    const end = win.start + WINDOW;
    const res = add({
      el: narrow() ? pill('hs-result', orders(win.count))
        : pill('hs-result', U.el('span', {}, `${time(win.start)}–${time(end)}`),
          U.el('span', {}, orders(win.count)), U.el('span', {}, money(win.cents))),
      phase: 'out', age: 0, result: true,
    });
    res.x = winLeft + winW + 12;
    win.start = end; win.count = 0; win.cents = 0; win.flash = 0.7;
    showWindow();
  };

  // Scale for a result popping out of the window: from 0.6, overshooting a little, to 1.
  const popIn = (p) => { const c = 1.70158, q = p - 1; return 0.6 + 0.4 * (1 + (c + 1) * q * q * q + c * q * q); };

  function step(dt) {
    tail += SPEED * dt;
    spawn();
    for (const it of items) {
      if (it.phase === 'late') {
        // Bump off the window, then fall out of the stream.
        it.age += dt;
        it.x = it.x0 - 28 * Math.sin(Math.min(it.age / 0.3, 1) * Math.PI / 2);
        const fall = Math.max(0, it.age - 0.55);
        it.y = 160 * fall * fall;
        it.opacity = Math.max(0, 1 - fall / 0.6);
        if (it.age > 1.3) it.gone = true;
        continue;
      }
      it.x += SPEED * dt;
      if (it.phase === 'out') {
        if (it.result) { it.age += dt; it.scale = popIn(Math.min(it.age / 0.4, 1)); }
        if (it.x > width) it.gone = true;
        continue;
      }
      // Arriving at the window.
      if (it.kind === 'order' && it.t <= watermark && it.x + it.w >= winLeft - 4) {
        it.phase = 'late'; it.age = 0; it.x0 = it.x;
        it.el.classList.add('hs-late');
        it.el.append(U.el('span', { class: 'hs-late-tag' }, 'late'));
      } else if (it.x >= winLeft) {
        if (it.kind === 'order') {
          win.count++; win.cents += it.cents; win.took = 0.35; it.gone = true;
          showWindow();
        } else {
          // The watermark passes under the window and out the other side, after the result it triggers.
          watermark = it.t;
          it.phase = 'out';
          if (watermark >= win.start + WINDOW) fire();
        }
      }
    }
    for (const it of items) if (it.gone) it.el.remove();
    items = items.filter((it) => !it.gone);
    win.flash = Math.max(0, win.flash - dt);
    win.took = Math.max(0, win.took - dt);
  }

  function paint() {
    items.forEach(render);
    win.el.classList.toggle('fired', win.flash > 0);
    win.el.classList.toggle('took', win.took > 0);
  }

  width = strip.clientWidth;
  showWindow();
  measure();
  // Run ahead until the first window has closed, so the strip starts full and a result is in view.
  for (let t = 0; t < 120 && !items.some((i) => i.result && i.age > 0.6); t += 1 / 60) step(1 / 60);
  win.flash = 0; win.took = 0;
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
