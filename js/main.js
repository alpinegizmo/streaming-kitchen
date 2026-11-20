// Mounts every registered pattern and builds the table of contents.
'use strict';

(() => {
  const host = document.getElementById('patterns');
  const toc = document.getElementById('toc-list');

  toc.append(U.el('li', {}, U.el('a', { href: '#reading' }, 'How to read the diagrams')));
  for (const p of window.PATTERNS) {
    Player.mount(host, p);
    toc.append(U.el('li', {}, U.el('a', { href: `#${p.id}` }, p.nav || p.title)));
  }

  // Mark the section in view in the table of contents.
  const links = new Map([...toc.querySelectorAll('a')].map((a) => [a.getAttribute('href').slice(1), a]));
  const io = new IntersectionObserver((entries) => {
    for (const en of entries) {
      if (!en.isIntersecting) continue;
      links.forEach((a) => a.removeAttribute('aria-current'));
      links.get(en.target.id)?.setAttribute('aria-current', 'true');
    }
  }, { rootMargin: '-30% 0px -60% 0px' });
  document.querySelectorAll('#reading, .pattern').forEach((s) => io.observe(s));

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
