// Builds one pattern section and drives its animation.
'use strict';

const Player = (() => {
  let variantUid = 0;
  const NARROW = 640;

  // ---------- Kafka topics as tables ----------

  function topicTable({ title, note, cols, key, rows, current, hotKey, emptyText }) {
    const head = U.el('tr', {},
      U.el('th', { scope: 'col', class: 'num' }, 'offset'),
      key ? U.el('th', { scope: 'col' }, 'key') : null,
      ...cols.map((c) => U.el('th', { scope: 'col', class: c.num ? 'num' : null }, c.h || c.f)));
    const body = U.el('tbody');
    rows.forEach((r) => {
      const cls = [r.cls, r.offset === current && 'current'].filter(Boolean).join(' ');
      const tr = U.el('tr', { class: cls || null },
        U.el('td', { class: 'num' }, String(r.offset)),
        key ? U.el('td', { class: 'key' }, String(r.key)) : null);
      if (r.value == null) {
        tr.append(U.el('td', { colspan: cols.length, class: 'tombstone' }, 'null (tombstone)'));
      } else {
        cols.forEach((c) => tr.append(U.el('td', { class: c.num ? 'num' : null }, c.fmt ? c.fmt(r.value[c.f]) : String(r.value[c.f]))));
      }
      body.append(tr);
    });
    if (!rows.length) {
      body.append(U.el('tr', { class: 'empty' }, U.el('td', { colspan: cols.length + (key ? 2 : 1) }, emptyText || 'empty')));
    }
    return U.el('figure', { class: 'topic' },
      U.el('figcaption', {}, U.el('span', { class: 'topic-name' }, title), note ? U.el('span', { class: 'topic-note' }, note) : null),
      U.el('div', { class: 'table-scroll' }, U.el('table', {}, U.el('thead', {}, head), body)));
  }

  function compactedTable({ title, note, cols, key, table, hotKey }) {
    const head = U.el('tr', {}, U.el('th', { scope: 'col' }, key),
      ...cols.filter((c) => c.f !== key).map((c) => U.el('th', { scope: 'col', class: c.num ? 'num' : null }, c.h || c.f)));
    const body = U.el('tbody');
    const entries = Object.entries(table);
    entries.forEach(([k, v]) => {
      const tr = U.el('tr', { class: String(k) === String(hotKey) ? 'current' : null }, U.el('td', { class: 'key' }, k));
      cols.filter((c) => c.f !== key).forEach((c) => tr.append(U.el('td', { class: c.num ? 'num' : null }, c.fmt ? c.fmt(v[c.f]) : String(v[c.f]))));
      body.append(tr);
    });
    if (!entries.length) body.append(U.el('tr', { class: 'empty' }, U.el('td', { colspan: cols.length }, 'empty')));
    return U.el('figure', { class: 'topic topic-table' },
      U.el('figcaption', {}, U.el('span', { class: 'topic-name' }, title), U.el('span', { class: 'topic-note' }, note || 'latest value per key, as after compaction')),
      U.el('div', { class: 'table-scroll' }, U.el('table', {}, U.el('thead', {}, head), body)));
  }

  // ---------- One variant: code, facts, stage ----------

  function mountVariant(host, pattern, variant) {
    variant.uid = `v${++variantUid}`;
    const trace = Sim.run(variant);
    const steps = trace.steps;
    const N = steps.length;
    const snapAt = (k) => (k === 0 ? trace.initial : steps[k - 1].snap);
    const nodeById = Object.fromEntries(variant.graph.nodes.map((n) => [n.id, n]));

    // Size each state panel for its fullest moment.
    const specs = {};
    for (const n of variant.graph.nodes.filter((x) => x.stateful)) {
      specs[n.id] = Graph.panelSpec([trace.initial, ...steps.map((s) => s.snap)].map((s) => Ops[n.op].view(s.ops[n.id], n.cfg || {})));
    }

    // Operators that ever hold a watermark get a tag (and room for it in the vertical layout).
    const wmNodes = new Set(steps.flatMap((s) => Object.keys(s.snap.wm)));

    // Code and facts.
    const sqlPre = SQL.render(variant.sql, `SQL for ${pattern.title}`);
    const codeCol = U.el('div', { class: 'code-col' }, sqlPre,
      variant.ddl ? U.el('details', { class: 'ddl' }, U.el('summary', {}, 'Table definitions'), SQL.render(variant.ddl, 'Table definitions')) : null);
    const facts = U.el('dl', { class: 'facts' },
      U.el('div', {}, U.el('dt', {}, 'State'), U.el('dd', { html: variant.facts.state })),
      U.el('div', {}, U.el('dt', {}, 'Watermarks'), U.el('dd', { html: variant.facts.watermarks })),
      variant.facts.output ? U.el('div', {}, U.el('dt', {}, 'Output'), U.el('dd', { html: variant.facts.output })) : null);

    // Stage.
    const graphWrap = U.el('div', { class: 'graph-wrap' });
    const caption = U.el('p', { class: 'caption', 'aria-live': 'polite' });
    const counter = U.el('span', { class: 'counter' });
    const btn = (cls, label, icon, onclick) => U.el('button', { type: 'button', class: `ctl ${cls}`, 'aria-label': label, title: label, onclick, html: icon });
    const ICON = {
      restart: '<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M4 4v12M16 4 7 10l9 6z"/></svg>',
      back: '<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M13 4 6 10l7 6z"/></svg>',
      fwd: '<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M7 4l7 6-7 6z"/></svg>',
      play: '<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M6 3.5v13l10.5-6.5z"/></svg>',
      pause: '<svg viewBox="0 0 20 20" aria-hidden="true"><path d="M5.5 4h3v12h-3zM11.5 4h3v12h-3z"/></svg>',
    };
    const playBtn = U.el('button', { type: 'button', class: 'ctl play', onclick: () => (playing ? pause() : play()) });
    const speedSel = U.el('select', { class: 'speed', 'aria-label': 'Speed' },
      U.el('option', { value: '0.5' }, '0.5×'), U.el('option', { value: '1', selected: true }, '1×'), U.el('option', { value: '2' }, '2×'));
    const restartBtn = btn('restart', 'Back to the start', ICON.restart, () => { pause(); jump(0); });
    const backBtn = btn('back', 'Step back', ICON.back, () => { pause(); jump(Math.max(0, idx - 1)); });
    const fwdBtn = btn('fwd', 'Step forward', ICON.fwd, () => { pause(); forward(); });
    const scrub = U.el('input', { type: 'range', class: 'scrub', min: 0, max: N, value: 0, 'aria-label': 'Animation position' });
    scrub.addEventListener('input', () => { pause(); jump(Number(scrub.value)); });
    const controls = U.el('div', { class: 'controls' }, restartBtn, backBtn, playBtn, fwdBtn, speedSel, counter);
    const topics = U.el('div', { class: 'topics' });

    const stage = U.el('div', {
      class: 'stage', tabindex: 0,
      'aria-label': `${pattern.title} animation. Space plays or pauses, arrow keys step.`,
    }, graphWrap, U.el('div', { class: 'caption-row' }, caption, controls), scrub, topics);

    const details = variant.details || pattern.details;
    host.append(
      U.el('div', { class: 'code-and-facts' }, codeCol, facts),
      stage,
      U.el('details', { class: 'more' },
        U.el('summary', {}, 'Details'),
        U.el('div', { class: 'more-body', html: details || '' }),
        variant.explain ? U.el('div', { class: 'more-plan' },
          U.el('p', {}, 'The optimized plan from ', U.el('code', {}, 'EXPLAIN CHANGELOG_MODE'), ' on Flink 2.3:'),
          U.el('pre', { class: 'code plan' }, variant.explain.trim())) : null));

    // ---------- Rendering ----------

    let view = null;
    let orient = null;
    let idx = 0;
    let playing = false;
    let gen = 0;

    const speed = () => Number(speedSel.value);
    const travelMs = () => (U.reducedMotion() ? 0 : 700 / speed());
    const holdMs = () => (U.reducedMotion() ? 900 : 650) / speed();

    function build() {
      const want = graphWrap.clientWidth && graphWrap.clientWidth < NARROW ? 'v' : 'h';
      if (want === orient && view) return;
      orient = want;
      U.clear(graphWrap);
      view = Graph.create(graphWrap, variant, { orient, specs, wmNodes });
      draw(idx);
    }

    function freshUids(prev, next) {
      const old = new Set(Object.values(prev.parked).flat().map((i) => i.uid));
      return new Set(Object.values(next.parked).flat().map((i) => i.uid).filter((u) => !old.has(u)));
    }

    function draw(k, opts = {}) {
      const step = k > 0 ? steps[k - 1] : null;
      const snap = snapAt(k);
      view.render(snap, {
        active: step?.node, touched: step?.touched,
        fresh: opts.animate && k > 0 ? freshUids(snapAt(k - 1), snap) : null,
      });
      caption.textContent = step ? step.caption : variant.intro;
      SQL.mark(sqlPre, step ? nodeById[step.node].sqlLines : []);
      counter.textContent = `${k} / ${N}`;
      scrub.value = String(k);
      restartBtn.disabled = backBtn.disabled = k === 0;
      fwdBtn.disabled = k === N;
      drawTopics(k, step, snap);
    }

    function drawTopics(k, step, snap) {
      U.clear(topics);
      for (const src of variant.graph.nodes.filter((n) => n.kind === 'source')) {
        const cfg = src.cfg;
        const records = variant.inputs.filter((r) => !r.wm && (r.at || src.id) === src.id);
        const consumed = snap.consumed[src.id];
        const justRead = step && step.node === src.id && step.via == null && !step.item.wm;
        topics.append(topicTable({
          title: cfg.topic, note: 'input topic', cols: cfg.columns, key: cfg.showKey ? cfg.key : null,
          rows: records.map((r, i) => ({ offset: i, key: r.key, value: r.value, cls: i < consumed ? 'read' : i === consumed ? 'next' : null })),
          current: justRead ? consumed - 1 : null,
        }));
      }
      for (const sink of variant.graph.nodes.filter((n) => n.kind === 'sink')) {
        const cfg = sink.cfg;
        const st = snap.ops[sink.id];
        const atSink = step && step.node === sink.id && step.item.t === 'rec';
        topics.append(topicTable({
          title: cfg.topic, note: 'output topic', cols: cfg.columns, key: cfg.showKey ? cfg.key : null,
          rows: st.log, current: atSink ? st.log.length - 1 : null, emptyText: 'nothing written yet',
        }));
        if (st.table) {
          topics.append(compactedTable({
            title: cfg.tableTitle || cfg.topic, note: cfg.tableNote,
            cols: cfg.tableColumns || cfg.columns, key: cfg.key, table: st.table,
            hotKey: atSink ? step.ev.key : null,
          }));
        }
      }
    }

    // ---------- Navigation ----------

    function jump(k) {
      gen++;
      view.clearFly();
      idx = k;
      draw(idx);
    }

    async function forward() {
      if (idx >= N) return false;
      const my = ++gen;
      const step = steps[idx];
      const prev = snapAt(idx);
      const ms = travelMs();
      view.clearFly();
      let flying = null;
      if (step.via != null && ms > 0) {
        const item = prev.parked[step.via][0];
        view.render(prev, { hide: item.uid });
        flying = await view.travel(item, step.via, ms);
        if (my !== gen) return false;
      }
      idx += 1;
      draw(idx, { animate: true });
      if (flying) {
        const sink = nodeById[step.node].kind === 'sink';
        if (step.drop) view.fade(flying, ms * 0.8, 'dropped');
        else view.fade(flying, ms * 0.35, sink || step.absorb ? 'absorbed' : 'handed-off');
      }
      return true;
    }

    async function play() {
      if (idx >= N) jump(0);
      playing = true;
      playBtn.innerHTML = ICON.pause + '<span>Pause</span>';
      const my = gen + 1;
      while (playing && idx < N) {
        const ok = await forward();
        if (!ok || !playing) break;
        await new Promise((r) => setTimeout(r, holdMs()));
      }
      if (idx >= N) pause();
    }

    function pause() {
      playing = false;
      playBtn.innerHTML = ICON.play + '<span>Play</span>';
    }

    stage.addEventListener('keydown', (e) => {
      if (e.target.closest('select, input, button') && e.key !== 'Escape') {
        if (e.key === ' ' && e.target.tagName === 'BUTTON') return;
        if (e.target.tagName !== 'BUTTON' && e.target.tagName !== 'DIV') return;
      }
      if (e.key === 'ArrowRight') { pause(); forward(); }
      else if (e.key === 'ArrowLeft') { pause(); jump(Math.max(0, idx - 1)); }
      else if (e.key === ' ' || e.key === 'k') { playing ? pause() : play(); }
      else if (e.key === 'Home') { pause(); jump(0); }
      else if (e.key === 'End') { pause(); jump(N); }
      else return;
      e.preventDefault();
    });

    const io = new IntersectionObserver((entries) => {
      for (const en of entries) if (!en.isIntersecting && playing) pause();
    }, { threshold: 0.15 });
    io.observe(stage);

    const ro = new ResizeObserver(() => build());
    ro.observe(graphWrap);

    pause();
    build();

    return { destroy() { gen++; playing = false; io.disconnect(); ro.disconnect(); } };
  }

  // ---------- Pattern section ----------

  function mount(container, pattern) {
    const section = U.el('section', { class: 'pattern', id: pattern.id, 'aria-labelledby': `${pattern.id}-h` },
      U.el('header', { class: 'pattern-head' },
        U.el('h2', { id: `${pattern.id}-h` }, pattern.title),
        pattern.question ? U.el('p', { class: 'question' }, pattern.question) : null,
        U.el('div', { class: 'summary', html: pattern.summary })));
    const body = U.el('div', { class: 'pattern-body' });
    let current = null;

    function show(i) {
      if (current) current.destroy();
      U.clear(body);
      const v = pattern.variants[i];
      if (v.blurb) body.append(U.el('p', { class: 'variant-blurb', html: v.blurb }));
      if (v.question) body.append(U.el('p', { class: 'question' }, v.question));
      current = mountVariant(body, pattern, v);
    }

    if (pattern.variants.length > 1) {
      const tabs = U.el('div', { class: 'variants', role: 'tablist', 'aria-label': `${pattern.title} variants` });
      pattern.variants.forEach((v, i) => {
        const tab = U.el('button', {
          type: 'button', role: 'tab', class: 'variant-tab', id: `${pattern.id}-tab-${v.id}`,
          'aria-selected': i === 0 ? 'true' : 'false', 'aria-controls': `${pattern.id}-panel`,
          onclick: () => {
            tabs.querySelectorAll('[role=tab]').forEach((t) => t.setAttribute('aria-selected', String(t === tab)));
            show(i);
          },
        }, v.label);
        tabs.append(tab);
      });
      tabs.addEventListener('keydown', (e) => {
        const all = [...tabs.querySelectorAll('[role=tab]')];
        const at = all.indexOf(document.activeElement);
        if (at < 0 || !['ArrowLeft', 'ArrowRight'].includes(e.key)) return;
        const next = all[(at + (e.key === 'ArrowRight' ? 1 : all.length - 1)) % all.length];
        next.focus(); next.click();
      });
      section.append(tabs);
      body.setAttribute('role', 'tabpanel');
    }
    body.id = `${pattern.id}-panel`;
    section.append(body);
    container.append(section);
    show(0);
  }

  return { mount };
})();
