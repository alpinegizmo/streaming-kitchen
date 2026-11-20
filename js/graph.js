// Lays out and draws a job graph as SVG: operators, edges with their stream type,
// state panels, watermark tags, and the record "pills" that travel along the edges.
//
// Two orientations: 'h' (left to right) for wide screens, and 'v' for phones: a column of
// full-width operator cards, top to bottom, each with its state panel right under it.
'use strict';

const Graph = (() => {
  const BASE = { nodeH: 66, pad: 14, pillH: 20, rowH: 18, panelHead: 28 };
  const DIMS = {
    h: { ...BASE, nodeW: 176, gapH: 196, top: 52 },
    v: { ...BASE, width: 340, rail: 40 },
  };
  let G = DIMS.h;

  // Longest-path layering: sources in layer 0, everything else one past its deepest input.
  const isStream = (e) => e.mode !== 'lookup';

  function layers(graph) {
    const depth = {};
    const visit = (id) => {
      if (depth[id] != null) return depth[id];
      const ins = graph.edges.filter((e) => e.to === id && isStream(e));
      depth[id] = ins.length ? 1 + Math.max(...ins.map((e) => visit(e.from))) : 0;
      return depth[id];
    };
    const streamNodes = graph.nodes.filter((n) => n.kind !== 'external');
    streamNodes.forEach((n) => visit(n.id));
    const rows = {};
    const used = {};
    streamNodes.forEach((n) => {
      const ins = graph.edges.filter((e) => e.to === n.id && isStream(e)).map((e) => rows[e.from]);
      // An operator with several inputs lines up with its first input; the others curve in.
      const want = ins.length ? Math.min(...ins) : (used[depth[n.id]] ?? -1) + 1;
      rows[n.id] = want;
      used[depth[n.id]] = Math.max(used[depth[n.id]] ?? -1, want);
    });
    return { depth, rows };
  }

  const COL_HEAD = 17;
  const COL_GAP = 14;
  const CHAR = 11.5 * 0.6;
  const TITLE_CHAR = 6.4;
  const FOOT_CHAR = 10.5 * 0.6;
  const TITLE_LINE = 15;

  // Greedy word wrap to at most `chars` characters a line.
  function wrap(text, chars) {
    const lines = [];
    let cur = '';
    for (const word of text.split(' ')) {
      if (cur && cur.length + 1 + word.length > chars) { lines.push(cur); cur = word; } else cur = cur ? `${cur} ${word}` : word;
    }
    if (cur) lines.push(cur);
    return lines;
  }

  // Sizes a state panel from every view it will show over the animation, so it never
  // has to grow while playing. Views are { title, cols: [header], rows: [{ key, cells }], footer }.
  function panelSpec(views) {
    const cols = views[0].cols;
    const rows = Math.max(1, ...views.map((v) => v.rows.length));
    const colW = cols.map((h, j) => CHAR * Math.max(h.length, ...views.flatMap((v) => v.rows.map((r) => String(r.cells[j]).length))));
    return {
      rows, colW,
      colsW: 24 + colW.reduce((a, b) => a + b, 0) + COL_GAP * (cols.length - 1),
      titles: [...new Set(views.map((v) => v.title))],
      footers: [...new Set(views.map((v) => v.footer).filter(Boolean))],
    };
  }

  // Fits a panel into maxW by wrapping its title and footer. The columns never wrap.
  function fitPanel(spec, maxW = Infinity) {
    const titleW = Math.max(...spec.titles.map((t) => t.length)) * TITLE_CHAR + 24;
    const footW = Math.max(0, ...spec.footers.map((f) => f.length)) * FOOT_CHAR + 24;
    const width = Math.max(180, spec.colsW, Math.min(titleW, maxW), Math.min(footW, maxW));
    const titleChars = Math.floor((width - 24) / TITLE_CHAR);
    const footChars = Math.floor((width - 24) / FOOT_CHAR);
    return {
      ...spec, width, titleChars, footChars,
      titleLines: Math.max(...spec.titles.map((t) => wrap(t, titleChars).length)),
      footLines: Math.max(0, ...spec.footers.map((f) => wrap(f, footChars).length)),
    };
  }

  function panelHeight(fit) {
    return G.panelHead + (fit.titleLines - 1) * TITLE_LINE + COL_HEAD + fit.rows * G.rowH
      + (fit.footLines ? fit.footLines * G.rowH + 4 : 0) + 8;
  }

  // Every operator gets the same width, wide enough for the longest label.
  function nodeWidth(graph) {
    return Math.ceil(Math.max(G.nodeW, ...graph.nodes.flatMap((n) => [
      n.title.length * 8.2 + 28, ...(n.lines || []).map((t) => t.length * 6.9 + 24)])));
  }

  function layout(graph, orient, specs, wmNodes) {
    G = { ...DIMS[orient] };
    if (orient === 'v') return layoutV(graph, specs, wmNodes);
    G.nodeW = nodeWidth(graph);
    const fits = Object.fromEntries(Object.entries(specs).map(([id, spec]) => [id, fitPanel(spec, 400)]));
    const { depth, rows } = layers(graph);
    // Long pipelines get shorter edges, so they still fit a laptop screen.
    if (Math.max(...Object.values(depth)) >= 3) G.gapH = 156;
    const hasPanel = (n) => n.stateful || n.panel;
    const multiInput = (n) => graph.edges.filter((e) => e.to === n.id && isStream(e)).length > 1;
    const tallest = Math.max(0, ...graph.nodes.map((n) => (hasPanel(n) ? panelHeight(fits[n.id]) : 0)));
    // An external system (the lookup join's service) sits above the operator that calls it.
    const externals = graph.nodes.filter((n) => n.kind === 'external');
    // Lookup edges always run from the calling operator to the external system.
    const calledBy = Object.fromEntries(graph.edges.filter((e) => e.mode === 'lookup').map((e) => [e.from, e.to]));
    const pos = {};
    const panels = {};
    const top = externals.length ? G.top + G.nodeH + 56 : G.top;
    for (const n of graph.nodes.filter((x) => x.kind !== 'external')) {
      const x = G.pad + depth[n.id] * (G.nodeW + G.gapH);
      const y = top + rows[n.id] * (G.nodeH + tallest + 90);
      pos[n.id] = { x, y, w: G.nodeW, h: G.nodeH, cx: x + G.nodeW / 2, cy: y + G.nodeH / 2 };
      if (hasPanel(n)) {
        const fit = fits[n.id];
        // Centred under the operator, unless other inputs curve in from below: then it hangs
        // to the right, clear of them. A panel that reaches under edge labels drops below them.
        panels[n.id] = {
          x: multiInput(n) ? x : x + G.nodeW / 2 - fit.width / 2,
          y: y + G.nodeH + ((multiInput(n) ? fit.width - G.nodeW : (fit.width - G.nodeW) / 2) > 46 ? 48 : 14),
          w: fit.width, h: panelHeight(fit), fit,
        };
      }
    }
    for (const [caller, id] of Object.entries(calledBy)) {
      const c = pos[caller];
      pos[id] = { x: c.x, y: c.y - G.nodeH - 56, w: G.nodeW, h: G.nodeH, cx: c.cx, cy: c.y - G.nodeH - 56 + G.nodeH / 2 };
    }
    const edges = graph.edges.map((e) => {
      const a = pos[e.from];
      const b = pos[e.to];
      if (e.mode === 'lookup') {
        // A request/response line between the operator and the service it calls.
        return { ...e, d: `M${a.cx},${a.y} L${b.cx},${b.y + b.h}`, mid: { x: a.cx, y: (a.y + b.y + b.h) / 2 } };
      }
      const x1 = a.x + a.w, y1 = a.cy, x2 = b.x - 6, y2 = b.cy;
      const d = y1 === y2 ? `M${x1},${y1} L${x2},${y2}` : `M${x1},${y1} C${x1 + 90},${y1} ${x2 - 90},${y2} ${x2},${y2}`;
      return { ...e, d, mid: { x: (x1 + x2) / 2, y: (y1 + y2) / 2 } };
    });
    let width = 0, height = 0, left = 0;
    for (const p of Object.values(pos)) { width = Math.max(width, p.x + p.w); height = Math.max(height, p.y + p.h); }
    for (const p of Object.values(panels)) {
      width = Math.max(width, p.x + p.w); height = Math.max(height, p.y + p.h); left = Math.min(left, p.x - G.pad);
    }
    return { pos, panels, edges, left, width: width + G.pad - left, height: height + 18 };
  }

  // Vertical layout, for phones: one column of full-width cards in an order where every
  // operator comes right after its first input, so most edges are short and straight. A join's
  // other input comes earlier in the column, and its edge runs down a rail on the right and
  // into the join's side. Edge labels sit at the right of each gap; watermark tags and planner
  // notes sit just above their card.
  function layoutV(graph, specs, wmNodes) {
    const byId = Object.fromEntries(graph.nodes.map((n) => [n.id, n]));
    const ins = (id) => graph.edges.filter((e) => e.to === id && isStream(e));
    const outs = (id) => graph.edges.filter((e) => e.from === id && isStream(e));
    const order = [];
    const place = (id) => {
      if (order.includes(id)) return;
      const inputs = ins(id);
      inputs.slice(1).forEach((e) => place(e.from));
      if (inputs[0]) place(inputs[0].from);
      order.push(id);
    };
    graph.nodes.filter((n) => n.kind !== 'external' && !outs(n.id).length).forEach((n) => place(n.id));

    const rail = graph.nodes.some((n) => ins(n.id).length > 1) ? G.rail : 0;
    const fitIn = (W) => Object.fromEntries(Object.entries(specs).map(([id, spec]) => [id, fitPanel(spec, W - 2 * G.pad - rail)]));
    let fits = fitIn(G.width);
    const W = Math.max(G.width, ...Object.values(fits).map((f) => f.width + 2 * G.pad + rail));
    fits = fitIn(W);
    const colW = W - 2 * G.pad - rail;
    const railX = G.pad + colW + rail - 10;
    const calledBy = Object.fromEntries(graph.edges.filter((e) => e.mode === 'lookup').map((e) => [e.from, e.to]));

    const cardH = (n) => 36 + 15 * Math.max(1, (n.lines || []).length);
    const labels = (e) => 52 + (e.exchange ? 16 : 0);
    const above = (n) => (wmNodes.has(n.id) || n.planner ? 26 : 0);
    const pos = {};
    const panels = {};
    const bottom = {};
    let y = 12 + above(byId[order[0]]);
    order.forEach((id, i) => {
      const n = byId[id];
      if (i > 0) {
        const prev = order[i - 1];
        const out = outs(prev)[0];
        const railed = out && out.to !== id;
        y = bottom[prev] + 14 + (out ? labels(out) : 0) + (railed ? 18 : 0) + above(n) + 6;
      }
      const h = cardH(n);
      const w = calledBy[id] ? Math.round(colW * 0.56) : colW;
      pos[id] = { x: G.pad, y, w, h, cx: G.pad + w / 2, cy: y + h / 2 };
      if (calledBy[id]) {
        const x = G.pad + w + 20;
        pos[calledBy[id]] = { x, y, w: colW - w - 20, h, cx: x + (colW - w - 20) / 2, cy: y + h / 2 };
      }
      bottom[id] = y + h;
      if (specs[id]) {
        const fit = fits[id];
        panels[id] = { x: G.pad, y: y + h + 6, w: colW, h: panelHeight(fit), fit };
        bottom[id] = panels[id].y + panels[id].h;
      }
    });

    const r = 12;
    const edges = graph.edges.map((e) => {
      const a = pos[e.from];
      const b = pos[e.to];
      if (e.mode === 'lookup') return { ...e, d: `M${a.x + a.w},${a.cy} L${b.x},${b.cy}`, mid: { x: (a.x + a.w + b.x) / 2, y: a.cy } };
      const y1 = bottom[e.from];
      const badgeY = y1 + 24;
      if (order[order.indexOf(e.from) + 1] === e.to) {
        const x1 = a.cx, x2 = b.cx, y2 = b.y - 6;
        const d = x1 === x2 ? `M${x1},${y1} L${x2},${y2}` : `M${x1},${y1} C${x1},${y1 + 50} ${x2},${y2 - 50} ${x2},${y2}`;
        return { ...e, d, badgeY };
      }
      // Down from the operator, right to the rail, down the rail, and into the join's side.
      const yT = y1 + 14 + labels(e) + 4;
      const yE = b.y + b.h * 0.7;
      const d = `M${a.cx},${y1} V${yT - r} Q${a.cx},${yT} ${a.cx + r},${yT} H${railX - r} Q${railX},${yT} ${railX},${yT + r}`
        + ` V${yE - r} Q${railX},${yE} ${railX - r},${yE} H${b.x + b.w + 6}`;
      return { ...e, d, badgeY };
    });
    const last = order[order.length - 1];
    return { pos, panels, edges, left: 0, width: W, height: bottom[last] + 18, badgeRight: G.pad + colW };
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
      if (e.mode === 'lookup') {
        const path = U.svg('path', { d: e.d, class: 'edge mode-lookup' });
        edgeLayer.append(path);
        if (e.label && orient === 'h') {
          edgeLayer.append(U.svg('text', {
            class: 'edge-lookup-label', x: e.mid.x + 10, y: e.mid.y + 4, 'text-anchor': 'start', text: e.label,
          }));
        }
        return path;
      }
      const path = U.svg('path', { d: e.d, class: `edge mode-${e.mode}`, 'marker-end': `url(#arrow-${e.mode}-${variant.uid})` });
      edgeLayer.append(path);
      const modeText = { append: 'append-only', upsert: 'upsert', retract: 'retract' }[e.mode];
      const bw = U.monoWidth(modeText, 11) + 18;
      let bx = orient === 'h' ? e.mid.x : L.badgeRight - bw / 2;
      let by = orient === 'h' ? e.mid.y + 22 : e.badgeY;
      const intoJoin = graph.edges.filter((x) => x.to === e.to && isStream(x)).length > 1;
      if (orient === 'h' && intoJoin && L.pos[e.from].cy === L.pos[e.to].cy) {
        // Keep the straight input's badge clear of the other input curving in near the operator.
        bx = path.getPointAtLength(path.getTotalLength() * 0.32).x;
      } else if (orient === 'h' && L.pos[e.from].cy !== L.pos[e.to].cy) {
        // On a curved edge, the badge sits near the source, away from the operator's panel.
        const len = path.getTotalLength();
        const pt = path.getPointAtLength(Math.min(len * 0.22, 70));
        bx = pt.x + bw / 2 + 6;
        by = pt.y + (L.pos[e.from].cy > L.pos[e.to].cy ? 22 : -46);
      }
      // Vertical: the labels under the badge line up with its right end, the edge of the column.
      const [lx, anchor] = orient === 'h' ? [0, 'middle'] : [bw / 2 - 4, 'end'];
      const badge = U.svg('g', { class: `edge-badge mode-${e.mode}`, transform: `translate(${bx},${by})` },
        U.svg('rect', { x: -bw / 2, y: -10, width: bw, height: 20, rx: 10 }),
        U.svg('text', { x: 0, y: 4, 'text-anchor': 'middle', text: modeText }),
        U.svg('text', { class: 'edge-kinds', x: lx, y: 25, 'text-anchor': anchor, text: e.kinds.replace(/-/g, '−') }),
        e.exchange ? U.svg('text', { class: 'edge-exchange', x: lx, y: 41, 'text-anchor': anchor, text: e.exchange }) : null);
      edgeLayer.append(badge);
      return path;
    });
    svg.append(edgeLayer);

    const nodeLayer = U.svg('g', { class: 'nodes' });
    const nodeEls = {};
    for (const n of graph.nodes) {
      const p = L.pos[n.id];
      const cls = ['node', `node-${n.kind}`, n.stateful && 'stateful', n.planner && 'planner'].filter(Boolean).join(' ');
      // Horizontal: labels centred. Vertical: left-aligned, so the card reads like a list item.
      const [tx, anchor] = orient === 'h' ? [p.w / 2, 'middle'] : [16, 'start'];
      const titleY = orient === 'h' ? (n.lines?.length > 1 ? 22 : 28) : 22;
      const lineY = orient === 'h' ? (n.lines?.length > 1 ? 40 : 46) : 39;
      const g = U.svg('g', { class: cls, transform: `translate(${p.x},${p.y})` },
        U.svg('rect', { class: 'node-box', width: p.w, height: p.h, rx: 10 }),
        n.stateful ? U.svg('rect', { class: 'node-state-bar', x: 0, y: 8, width: 4, height: p.h - 16, rx: 2 }) : null,
        U.svg('text', { class: 'node-title', x: tx, y: titleY, 'text-anchor': anchor, text: n.title }),
        ...(n.lines || []).map((t, i) => U.svg('text', { class: 'node-line', x: tx, y: lineY + i * 15, 'text-anchor': anchor, text: t })));
      if (n.planner) {
        g.append(orient === 'h'
          ? U.svg('text', { class: 'node-planner', x: p.w / 2, y: -8, 'text-anchor': 'middle', text: 'added by the planner' })
          : U.svg('text', { class: 'node-planner', x: 2, y: -8, text: 'added by the planner' }));
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
        nodeEls[n.id].classList.toggle('active', info.active === n.id || info.also === n.id);
      }

      // State panels.
      for (const n of graph.nodes) {
        if (!n.stateful && !n.panel) continue;
        const p = L.panels[n.id];
        const view = Ops[n.op].view(snap.ops[n.id], n.cfg || {});
        const g = U.svg('g', { class: 'state-panel', transform: `translate(${p.x},${p.y})` },
          U.svg('rect', { class: 'state-box', width: p.w, height: p.h, rx: 8 }),
          ...wrap(view.title, p.fit.titleChars).map((t, i) => U.svg('text', { class: 'state-title', x: 12, y: 19 + i * TITLE_LINE, text: t })));
        const head = G.panelHead + (p.fit.titleLines - 1) * TITLE_LINE;
        const colX = p.fit.colW.reduce((xs, w) => [...xs, xs[xs.length - 1] + w + COL_GAP], [12]);
        view.cols.forEach((h, j) => g.append(U.svg('text', { class: 'state-col', x: colX[j], y: head + 9, text: h })));
        if (!view.rows.length) {
          g.append(U.svg('text', { class: 'state-empty', x: 12, y: head + COL_HEAD + 13, text: 'empty' }));
        }
        view.rows.forEach((r, i) => {
          const y = head + COL_HEAD + i * G.rowH;
          const hot = info.touched != null && String(info.touched) === String(r.key) && info.active === n.id;
          if (hot) g.append(U.svg('rect', { class: 'state-hot', x: 4, y: y - 1, width: p.w - 8, height: G.rowH, rx: 4 }));
          r.cells.forEach((c, j) => g.append(U.svg('text', { class: 'state-row', x: colX[j], y: y + 12, text: String(c) })));
        });
        if (view.footer) {
          const n = p.fit.footLines;
          g.append(U.svg('line', { class: 'state-rule', x1: 12, x2: p.w - 12, y1: p.h - n * G.rowH - 9, y2: p.h - n * G.rowH - 9 }));
          wrap(view.footer, p.fit.footChars).forEach((t, i) => g.append(U.svg('text', { class: 'state-foot', x: 12, y: p.h - 12 - (n - 1 - i) * G.rowH, text: t })));
        }
        stateLayer.append(g);
      }

      // Watermark tags above each operator that has seen one.
      for (const [id, time] of Object.entries(snap.wm || {})) {
        const p = L.pos[id];
        const text = `WM ${time}`;
        const w = U.monoWidth(text, 10) + 12;
        const planner = graph.nodes.find((n) => n.id === id).planner;
        const x = p.x + p.w - w;
        const y = orient === 'h' && planner ? p.y - 42 : p.y - 26;
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
