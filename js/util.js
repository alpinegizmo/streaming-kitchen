// Small DOM and formatting helpers shared by every module.
'use strict';

const U = (() => {
  const SVG_NS = 'http://www.w3.org/2000/svg';

  function apply(node, attrs) {
    for (const [k, v] of Object.entries(attrs || {})) {
      if (v == null || v === false) continue;
      if (k === 'class') node.setAttribute('class', v);
      else if (k === 'text') node.textContent = v;
      else if (k === 'html') node.innerHTML = v;
      else if (k.startsWith('on')) node.addEventListener(k.slice(2), v);
      else node.setAttribute(k, v === true ? '' : v);
    }
    return node;
  }

  function append(node, kids) {
    for (const kid of kids.flat()) {
      if (kid == null || kid === false) continue;
      node.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
    }
    return node;
  }

  return {
    el(tag, attrs, ...kids) {
      return append(apply(document.createElement(tag), attrs), kids);
    },
    svg(tag, attrs, ...kids) {
      return append(apply(document.createElementNS(SVG_NS, tag), attrs), kids);
    },
    clear(node) {
      while (node.firstChild) node.firstChild.remove();
      return node;
    },
    clone: (x) => structuredClone(x),
    // Money, with a true minus sign when it's negative: −$31.00.
    money: (n) => (Number(n) < 0 ? '−$' : '$') + Math.abs(Number(n)).toFixed(2),
    reducedMotion: () => window.matchMedia('(prefers-reduced-motion: reduce)').matches,
    // Kinds of changelog rows, as Flink prints them.
    kindClass: (k) => ({ '+I': 'rk-I', '-U': 'rk-UB', '+U': 'rk-UA', '-D': 'rk-D' })[k],
    kindText: (k) => k.replace('-', '−'),
    // Event times are 'HH:MM:SS' strings; seconds make arithmetic easy.
    toSec(t) {
      const [h, m, s] = t.split(':').map(Number);
      return h * 3600 + m * 60 + s;
    },
    fromSec(n) {
      const p = (x) => String(x).padStart(2, '0');
      return `${p(Math.floor(n / 3600))}:${p(Math.floor(n / 60) % 60)}:${p(n % 60)}`;
    },
    // Whether a column of cells holds numbers or times (an integer, a decimal, an amount of money,
    // or HH:MM[:SS]), which are right-aligned. Blank cells don't count either way.
    rightAligned(cells) {
      const vals = cells.map((c) => (c == null ? '' : String(c))).filter((c) => c !== '' && c !== '–');
      return vals.length > 0 && vals.every((c) => /^[−-]?\$?[−-]?\d[\d,]*(\.\d+)?$|^\d{1,2}:\d{2}(:\d{2}(\.\d+)?)?$/.test(c));
    },
    // Approximate width of a run of monospace text in the SVG, in px.
    monoWidth: (text, size = 11) => text.length * size * 0.6,
  };
})();
