// Runs a pattern's input through its job graph and records a trace of steps.
//
// A step is one operator handling one item: a source reading a Kafka record, or an
// operator receiving a row or a watermark on one of its input edges. Items move
// depth first, the way chained operators in Flink hand a record straight down the
// chain before the next record is read.
//
// Every step carries a snapshot of the whole job's state after it ran, so the player
// can jump to any step (including backwards) by rendering that snapshot.
'use strict';

const Sim = (() => {
  let uid = 0;

  function run(variant) {
    const { nodes, edges } = variant.graph;
    const byId = Object.fromEntries(nodes.map((n) => [n.id, n]));
    const outEdges = (id) => edges.map((e, i) => ({ ...e, i })).filter((e) => e.from === id);

    const st = {
      ops: Object.fromEntries(nodes.map((n) => [n.id, Ops[n.op].init(n.cfg || {})])),
      parked: Object.fromEntries(edges.map((e, i) => [i, []])),
      consumed: Object.fromEntries(nodes.filter((n) => n.kind === 'source').map((n) => [n.id, 0])),
      wm: {},
    };
    const snapshot = () => U.clone(st);
    const steps = [];

    function caption(node, ev, item) {
      const op = Ops[node.op];
      const custom = variant.narrate && variant.narrate(node.id, ev, item);
      return custom || op.say(ev, node.cfg || {});
    }

    function emit(node, out) {
      const tagged = out.map((o) => ({ ...o, uid: ++uid }));
      for (const e of outEdges(node.id)) {
        for (const o of tagged) st.parked[e.i].push({ ...o, uid: `${o.uid}.${e.i}` });
      }
      return tagged;
    }

    function forward(node, out) {
      for (const e of outEdges(node.id)) {
        for (const o of out) deliver(byId[e.to], st.parked[e.i][0], e.i);
      }
    }

    function deliver(node, item, via) {
      st.parked[via].shift();
      const op = Ops[node.op];
      const cfg = node.cfg || {};
      const res = item.t === 'wm' ? op.watermark(st.ops[node.id], item, cfg) : op.record(st.ops[node.id], item, cfg);
      if (item.t === 'wm') st.wm[node.id] = item.time;
      const out = emit(node, res.out);
      steps.push({
        node: node.id, via, item, out, ev: res.ev, touched: res.touched,
        drop: node.kind !== 'sink' && out.length === 0,
        caption: caption(node, res.ev, item),
        snap: snapshot(),
      });
      forward(node, out);
    }

    const initial = snapshot();
    for (const input of variant.inputs) {
      const node = byId[input.at || nodes.find((n) => n.kind === 'source').id];
      const res = Ops[node.op].input(st.ops[node.id], input, node.cfg || {});
      if (!input.wm) st.consumed[node.id] += 1;
      if (input.wm && res.out.length === 0) continue;
      if (input.wm) st.wm[node.id] = res.out[0].time;
      const out = emit(node, res.out);
      steps.push({
        node: node.id, via: null, item: input, out, ev: res.ev, drop: false,
        caption: caption(node, res.ev, input),
        snap: snapshot(),
      });
      forward(node, out);
    }
    return { initial, steps };
  }

  return { run };
})();
