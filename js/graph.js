// Lays out and draws a job graph as SVG: operators, edges with their stream type,
// state panels, watermark tags, and the record "pills" that travel along the edges.
//
// Two orientations: 'h' (left to right) for wide screens, 'v' (top to bottom) for phones.
'use strict';

const Graph = (() => {
  const BASE = { nodeH: 66, pad: 14, pillH: 20, rowH: 18, panelHead: 28 };
  // Horizontal: state panels hang under their operator. Vertical: a side column to the
  // right of each operator holds the planner note, the watermark tag and the state panel.
  const DIMS = {
    h: { ...BASE, nodeW: 176, gapH: 196, top: 52 },
    v: { ...BASE, nodeW: 140, gapV: 132, top: 16, side: 12 },
  };
  let G = DIMS.h;

  // Longest-path layering: sources in layer 0, everything else one past its deepest input.
  function layers(graph) {
    const depth = {};
    const visit = (id) => {
      if (depth[id] != null) return depth[id];
      const ins = graph.edges.filter((e) => e.to === id);
      depth[id] = ins.length ? 1 + Math.max(...ins.map((e) => visit(e.from))) : 0;
      return depth[id];
    };
    graph.nodes.forEach((n) => visit(n.id));
    const rows = {};
    const used = {};
    graph.nodes.forEach((n) => {
      const ins = graph.edges.filter((e) => e.to === n.id).map((e) => rows[e.from]);
      const want = ins.length ? ins.reduce((a, b) => a + b, 0) / ins.length : (used[depth[n.id]] ?? -1) + 1;
      rows[n.id] = want;
      used[depth[n.id]] = Math.max(used[depth[n.id]] ?? -1, want);
    });
    return { depth, rows };
  }

  const COL_HEAD = 17;
  const COL_GAP = 14;
  const CHAR = 11.5 * 0.6;

  // Sizes a state panel from every view it will show over the animation, so it never
  // has to grow while playing. Views are { title, cols: [header], rows: [{ key, cells }] }.
  function panelSpec(views) {
    const cols = views[0].cols;
    const rows = Math.max(1, ...views.map((v) => v.rows.length));
    const colW = cols.map((h, j) => CHAR * Math.max(h.length, ...views.flatMap((v) => v.rows.map((r) => String(r.cells[j]).length))));
    const titleW = Math.max(...views.map((v) => v.title.length)) * 6.4;
    const footW = Math.max(0, ...views.map((v) => (v.footer || '').length)) * 10.5 * 0.6;
    const width = Math.max(180, 24 + colW.reduce((a, b) => a + b, 0) + COL_GAP * (cols.length - 1), titleW + 24, footW + 24);
    return { rows, colW, width, footer: views.some((v) => v.footer) };
  }

  function panelHeight(spec) {
    return G.panelHead + COL_HEAD + spec.rows * G.rowH + (spec.footer ? G.rowH + 4 : 0) + 8;
  }

  // Every operator gets the same width, wide enough for the longest label.
  function nodeWidth(graph) {
    return Math.ceil(Math.max(G.nodeW, ...graph.nodes.flatMap((n) => [
      n.title.length * 8.2 + 28, ...(n.lines || []).map((t) => t.length * 6.9 + 24)])));
  }

  function layout(graph, orient, specs, wmNodes) {
    G = { ...DIMS[orient] };
    G.nodeW = nodeWidth(graph);
    const { depth, rows } = layers(graph);
    // Long pipelines get shorter edges, so they still fit a laptop screen.
    if (Math.max(...Object.values(depth)) >= 3) G.gapH = 156;
    const tallest = Math.max(0, ...graph.nodes.map((n) => (n.stateful ? panelHeight(specs[n.id]) : 0)));
    const widest = Math.max(0, ...graph.nodes.map((n) => (n.stateful ? specs[n.id].width : 0)));
    const pos = {};
    const panels = {};
    const side = {};
    // Vertical: how far down the side column each operator's state panel starts.
    const sideTop = (n) => (n.planner ? 20 : 0) + (wmNodes.has(n.id) ? 24 : 0);
    const gapV = orient === 'v'
      ? Math.max(G.gapV ?? 0, ...graph.nodes.map((n) => (n.stateful ? sideTop(n) + panelHeight(specs[n.id]) - G.nodeH + 76 : 0)))
      : 0;
    for (const n of graph.nodes) {
      const d = depth[n.id];
      const r = rows[n.id];
      let x, y;
      if (orient === 'h') {
        x = G.pad + d * (G.nodeW + G.gapH);
        y = G.top + r * (G.nodeH + tallest + 90);
      } else {
        x = G.pad + r * (G.nodeW + widest + 40);
        y = G.top + d * (G.nodeH + gapV);
      }
      pos[n.id] = { x, y, w: G.nodeW, h: G.nodeH, cx: x + G.nodeW / 2, cy: y + G.nodeH / 2 };
      if (n.stateful) {
        const spec = specs[n.id];
        const h = panelHeight(spec);
        panels[n.id] = orient === 'h'
          // A panel much wider than its operator reaches under the edge labels, so it drops below them.
          ? { x: x + G.nodeW / 2 - spec.width / 2, y: y + G.nodeH + ((spec.width - G.nodeW) / 2 > 46 ? 48 : 14), w: spec.width, h, colW: spec.colW }
          : { x: x + G.nodeW + G.side, y: y + sideTop(n), w: spec.width, h, colW: spec.colW };
      }
      if (orient === 'v') side[n.id] = { x: x + G.nodeW + G.side, wmY: y + (n.planner ? 20 : 0) };
    }
    const edges = graph.edges.map((e) => {
      const a = pos[e.from];
      const b = pos[e.to];
      let d, mid;
      if (orient === 'h') {
        const x1 = a.x + a.w, y1 = a.cy, x2 = b.x - 6, y2 = b.cy;
        d = y1 === y2 ? `M${x1},${y1} L${x2},${y2}` : `M${x1},${y1} C${x1 + 90},${y1} ${x2 - 90},${y2} ${x2},${y2}`;
        mid = { x: (x1 + x2) / 2, y: (y1 + y2) / 2 };
      } else {
        const x1 = a.cx, y1 = a.y + a.h, x2 = b.cx, y2 = b.y - 6;
        d = x1 === x2 ? `M${x1},${y1} L${x2},${y2}` : `M${x1},${y1} C${x1},${y1 + 60} ${x2},${y2 - 60} ${x2},${y2}`;
        mid = { x: (x1 + x2) / 2, y: (y1 + y2) / 2 };
      }
      // Vertical: keep the badge clear of the state panel beside the upstream operator.
      const panel = panels[e.from];
      const badgeY = orient === 'v' && panel ? Math.max(mid.y, panel.y + panel.h + 18) : mid.y;
      return { ...e, d, mid, badgeY };
    });
    let width = 0, height = 0, left = 0;
    for (const p of Object.values(pos)) { width = Math.max(width, p.x + p.w); height = Math.max(height, p.y + p.h); }
    for (const p of Object.values(panels)) {
      width = Math.max(width, p.x + p.w); height = Math.max(height, p.y + p.h); left = Math.min(left, p.x - G.pad);
    }
    if (orient === 'v') {
      width = Math.max(width, G.pad + G.nodeW + G.side + 120);
      for (const n of graph.nodes) if (wmNodes.has(n.id)) width = Math.max(width, side[n.id].x + 96);
    }
    return { pos, panels, side, edges, left, width: width + G.pad - left, height: height + 18 };
  }

  // A record or watermark, drawn as a small pill centred on (0, 0).
  function pill(item, label) {
    if (item.t === 'wm') {
      const text = `WM ${item.time}`;
      const w = U.monoWidth(text, 10.5) + 16;
      return U.svg('g', { class: 'pill pill-wm' },
        U.svg('rect', { x: -w / 2, y: -G.pillH / 2, width: w, height: G.pillH, rx: 3 }),
        U.svg('text', { x: 0, y: 4, 'text-anchor': 'middle', text }));
    }
    const kind = U.kindText(item.kind);
    const lw = U.monoWidth(label, 11);
    const cw = 24;
    const w = cw + lw + 14;
    return U.svg('g', { class: `pill ${U.kindClass(item.kind)}` },
      U.svg('rect', { class: 'pill-body', x: -w / 2, y: -G.pillH / 2, width: w, height: G.pillH, rx: G.pillH / 2 }),
      U.svg('rect', { class: 'pill-kind', x: -w / 2 + 2, y: -G.pillH / 2 + 2, width: cw, height: G.pillH - 4, rx: (G.pillH - 4) / 2 }),
      U.svg('text', { class: 'pill-kind-text', x: -w / 2 + 2 + cw / 2, y: 4, 'text-anchor': 'middle', text: kind }),
      U.svg('text', { class: 'pill-label', x: -w / 2 + cw + 8, y: 4, text: label }));
  }

  function create(container, variant, { orient, specs, wmNodes }) {
    const graph = variant.graph;
    const L = layout(graph, orient, specs, wmNodes || new Set());
    const label = (item) => (item.t === 'rec' ? variant.pill(item.row, item.kind) : '');

    const svg = U.svg('svg', {
      class: `jobgraph orient-${orient}`, viewBox: `${L.left} 0 ${L.width} ${L.height}`,
      role: 'img', 'aria-label': variant.graphLabel || 'Job graph',
      style: `max-width:${L.width}px; min-width:${Math.round(L.width * (orient === 'h' ? 0.86 : 0.72))}px`,
    });
    const defs = U.svg('defs', {},
      ...['append', 'upsert', 'retract'].map((m) => U.svg('marker', {
        id: `arrow-${m}-${variant.uid}`, viewBox: '0 0 10 10', refX: 8, refY: 5,
        markerWidth: 7, markerHeight: 7, orient: 'auto-start-reverse', class: `arrow mode-${m}`,
      }, U.svg('path', { d: 'M0,0 L10,5 L0,10 Z' }))));
    svg.append(defs);

    // Static layer: edges, badges and nodes.
    const edgeLayer = U.svg('g', { class: 'edges' });
    const paths = L.edges.map((e) => {
      const path = U.svg('path', { d: e.d, class: `edge mode-${e.mode}`, 'marker-end': `url(#arrow-${e.mode}-${variant.uid})` });
      edgeLayer.append(path);
      const modeText = { append: 'append-only', upsert: 'upsert', retract: 'retract' }[e.mode];
      const bw = U.monoWidth(modeText, 11) + 18;
      const bx = orient === 'h' ? e.mid.x : L.pos[e.from].x + G.nodeW + G.side + bw / 2;
      const by = orient === 'h' ? e.mid.y + 22 : e.badgeY;
      const badge = U.svg('g', { class: `edge-badge mode-${e.mode}`, transform: `translate(${bx},${by})` },
        U.svg('rect', { x: -bw / 2, y: -10, width: bw, height: 20, rx: 10 }),
        U.svg('text', { x: 0, y: 4, 'text-anchor': 'middle', text: modeText }),
        U.svg('text', { class: 'edge-kinds', x: 0, y: 25, 'text-anchor': 'middle', text: e.kinds.replace(/-/g, '−') }),
        e.exchange ? U.svg('text', { class: 'edge-exchange', x: 0, y: 41, 'text-anchor': 'middle', text: e.exchange }) : null);
      edgeLayer.append(badge);
      return path;
    });
    svg.append(edgeLayer);

    const nodeLayer = U.svg('g', { class: 'nodes' });
    const nodeEls = {};
    for (const n of graph.nodes) {
      const p = L.pos[n.id];
      const cls = ['node', `node-${n.kind}`, n.stateful && 'stateful', n.planner && 'planner'].filter(Boolean).join(' ');
      const g = U.svg('g', { class: cls, transform: `translate(${p.x},${p.y})` },
        U.svg('rect', { class: 'node-box', width: p.w, height: p.h, rx: 10 }),
        n.stateful ? U.svg('rect', { class: 'node-state-bar', x: 0, y: 8, width: 4, height: p.h - 16, rx: 2 }) : null,
        U.svg('text', { class: 'node-title', x: p.w / 2, y: n.lines?.length > 1 ? 22 : 28, 'text-anchor': 'middle', text: n.title }),
        ...(n.lines || []).map((t, i) => U.svg('text', { class: 'node-line', x: p.w / 2, y: (n.lines.length > 1 ? 40 : 46) + i * 15, 'text-anchor': 'middle', text: t })));
      if (n.planner) {
        g.append(orient === 'h'
          ? U.svg('text', { class: 'node-planner', x: p.w / 2, y: -8, 'text-anchor': 'middle', text: 'added by the planner' })
          : U.svg('text', { class: 'node-planner', x: p.w + G.side, y: 12, text: 'added by the planner' }));
      }
      nodeEls[n.id] = g;
      nodeLayer.append(g);
    }
    svg.append(nodeLayer);

    // Dynamic layers: redrawn for each snapshot.
    const stateLayer = U.svg('g', { class: 'state-layer' });
    const wmLayer = U.svg('g', { class: 'wm-layer' });
    const pillLayer = U.svg('g', { class: 'pill-layer' });
    const flyLayer = U.svg('g', { class: 'fly-layer' });
    svg.append(stateLayer, wmLayer, pillLayer, flyLayer);
    container.append(svg);

    // Where an item waits on an edge: just past the output port, stacked if several.
    function parkedPos(edgeIndex, i, w) {
      const path = paths[edgeIndex];
      if (orient === 'h') {
        const pt = path.getPointAtLength(Math.min(8 + w / 2, path.getTotalLength() - w / 2));
        return { x: pt.x, y: pt.y - i * (G.pillH + 4) };
      }
      return path.getPointAtLength(18 + i * (G.pillH + 6));
    }
    const pillWidth = (item) => (item.t === 'wm' ? U.monoWidth(`WM ${item.time}`, 10.5) + 16 : 24 + U.monoWidth(label(item), 11) + 14);

    function render(snap, info = {}) {
      U.clear(stateLayer); U.clear(wmLayer); U.clear(pillLayer);

      for (const n of graph.nodes) {
        nodeEls[n.id].classList.toggle('active', info.active === n.id);
      }

      // State panels.
      for (const n of graph.nodes) {
        if (!n.stateful) continue;
        const p = L.panels[n.id];
        const view = Ops[n.op].view(snap.ops[n.id], n.cfg || {});
        const g = U.svg('g', { class: 'state-panel', transform: `translate(${p.x},${p.y})` },
          U.svg('rect', { class: 'state-box', width: p.w, height: p.h, rx: 8 }),
          U.svg('text', { class: 'state-title', x: 12, y: 19, text: view.title }));
        const colX = p.colW.reduce((xs, w) => [...xs, xs[xs.length - 1] + w + COL_GAP], [12]);
        view.cols.forEach((h, j) => g.append(U.svg('text', { class: 'state-col', x: colX[j], y: G.panelHead + 9, text: h })));
        if (!view.rows.length) {
          g.append(U.svg('text', { class: 'state-empty', x: 12, y: G.panelHead + COL_HEAD + 13, text: 'empty' }));
        }
        view.rows.forEach((r, i) => {
          const y = G.panelHead + COL_HEAD + i * G.rowH;
          const hot = info.touched != null && String(info.touched) === String(r.key) && info.active === n.id;
          if (hot) g.append(U.svg('rect', { class: 'state-hot', x: 4, y: y - 1, width: p.w - 8, height: G.rowH, rx: 4 }));
          r.cells.forEach((c, j) => g.append(U.svg('text', { class: 'state-row', x: colX[j], y: y + 12, text: String(c) })));
        });
        if (view.footer) {
          g.append(U.svg('line', { class: 'state-rule', x1: 12, x2: p.w - 12, y1: p.h - G.rowH - 9, y2: p.h - G.rowH - 9 }));
          g.append(U.svg('text', { class: 'state-foot', x: 12, y: p.h - 12, text: view.footer }));
        }
        stateLayer.append(g);
      }

      // Watermark tags above each operator that has seen one.
      for (const [id, time] of Object.entries(snap.wm || {})) {
        const p = L.pos[id];
        const text = `WM ${time}`;
        const w = U.monoWidth(text, 10) + 12;
        const planner = graph.nodes.find((n) => n.id === id).planner;
        const x = orient === 'h' ? p.x + p.w - w : L.side[id].x;
        const y = orient === 'h' ? (planner ? p.y - 42 : p.y - 26) : L.side[id].wmY;
        wmLayer.append(U.svg('g', { class: 'wm-tag', transform: `translate(${x},${y})` },
          U.svg('rect', { width: w, height: 18, rx: 3 }),
          U.svg('text', { x: w / 2, y: 13, 'text-anchor': 'middle', text })));
      }

      // Items waiting on an edge for the next operator.
      for (const [ei, items] of Object.entries(snap.parked)) {
        items.forEach((item, i) => {
          if (info.hide && info.hide === item.uid) return;
          const pt = parkedPos(Number(ei), i, pillWidth(item));
          const el = pill(item, label(item));
          el.setAttribute('transform', `translate(${pt.x},${pt.y})`);
          if (info.fresh && info.fresh.has(item.uid)) el.classList.add('enter');
          pillLayer.append(el);
        });
      }
    }

    function animate(el, frames, ms) {
      if (ms <= 0) return Promise.resolve();
      return el.animate(frames, { duration: ms, easing: 'ease-in-out', fill: 'forwards' }).finished.catch(() => {});
    }

    // Moves an item from its parked spot along an edge into the next operator.
    function travel(item, edgeIndex, ms) {
      const path = paths[edgeIndex];
      const len = path.getTotalLength();
      const w = pillWidth(item);
      const start = parkedPos(edgeIndex, 0, w);
      const el = pill(item, label(item));
      flyLayer.append(el);
      const endLen = orient === 'h' ? Math.max(0, len - w / 2 - 4) : len - G.pillH / 2 - 4;
      const startLen = orient === 'h' ? Math.min(8 + w / 2, len - w / 2) : 18;
      const frames = [];
      const n = 12;
      for (let i = 0; i <= n; i++) {
        const l = startLen + (endLen - startLen) * (i / n);
        const pt = path.getPointAtLength(l);
        frames.push({ transform: `translate(${pt.x}px, ${pt.y}px)`, offset: i / n });
      }
      frames[0].transform = `translate(${start.x}px, ${start.y}px)`;
      el.setAttribute('transform', `translate(${start.x},${start.y})`);
      el.style.transform = frames[0].transform;
      return animate(el, frames, ms).then(() => el);
    }

    function fade(el, ms, cls) {
      if (!el) return Promise.resolve();
      el.classList.add(cls);
      const t = el.style.transform;
      return animate(el, [
        { transform: t, opacity: 1 },
        { transform: `${t} translateY(${cls === 'dropped' ? 18 : 0}px)`, opacity: 0 },
      ], ms).then(() => el.remove());
    }

    return { svg, render, travel, fade, clearFly: () => U.clear(flyLayer), layout: L };
  }

  return { create, panelSpec };
})();
