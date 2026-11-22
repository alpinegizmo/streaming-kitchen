import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerServiceImpl;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * Checks the state and timers the simulator shows against the real Flink operators.
 *
 * <p>Reads the 'operators' traces that tools/export-replay.mjs adds to each spec. For each traced
 * operator it plans the pattern's SQL (as Replay does), takes the transformations the planner
 * built for that operator, and runs each one in Flink's operator test harness, with the heap state
 * backend. It feeds them exactly the items the simulator's operator received, one at a time, and
 * after each one compares:
 *
 * <ul>
 *   <li>what the operator emitted, rows with their row kinds, and watermarks;
 *   <li>which keys (and namespaces, such as a window's slice end) are in state, across every state
 *       the operator registered;
 *   <li>every event-time and processing-time timer, by key and timestamp.
 * </ul>
 *
 * <p>Some operators hold records in memory before writing them to state, until a watermark or a
 * checkpoint: the window aggregations' RecordsWindowBuffer, and FastTop1Function's cache. Before
 * each inspection the harness takes a checkpoint (prepareSnapshotPreBarrier, then snapshotState),
 * which writes them to state.
 */
public class StateCheck {

    /** --dump: print every state value after every step, not just after a failing one. */
    static boolean dump;

    public static void main(String[] args) throws Exception {
        int failed = 0;
        int checked = 0;
        for (String arg : args) {
            if (arg.equals("--dump")) {
                dump = true;
                continue;
            }
            JsonNode spec = new ObjectMapper().readTree(Path.of(arg).toFile());
            if (!spec.has("operators") || spec.get("operators").isEmpty()) {
                continue;
            }
            checked++;
            System.out.printf("%n== %s (%s)%n", spec.get("label").asText(), Path.of(arg).getFileName());
            boolean pass;
            try {
                pass = run(spec);
            } catch (Exception e) {
                e.printStackTrace(System.out);
                pass = false;
            }
            if (!pass) {
                failed++;
            }
        }
        System.out.printf("%n%d of %d passed%n", checked - failed, checked);
        System.exit(failed == 0 ? 0 : 1);
    }

    static boolean run(JsonNode spec) throws Exception {
        Replay.Prepared p = Replay.prepare(spec);
        StreamStatementSet set = p.tEnv().createStatementSet();
        for (String insert : inserts(p.sql())) {
            set.addInsertSql(insert);
        }
        set.attachAsDataStream();
        List<Transformation<?>> all = topological(p.env().getTransformations());
        Set<String> tables = new TreeSet<>();
        spec.get("sources").fieldNames().forEachRemaining(tables::add);

        boolean pass = true;
        for (JsonNode op : spec.get("operators")) {
            List<String> names = new ArrayList<>();
            op.get("flink").forEach(n -> names.add(n.asText()));
            Set<String> upstream = strings(op.get("upstream"));
            List<Transformation<RowData>> chain = find(all, names, tables, upstream);
            System.out.printf("operator %s: %s%n", op.get("node").asText(),
                    String.join(" -> ", chain.stream().map(Transformation::getName).toList()));
            pass &= check(op, chain, tables);
        }
        return pass;
    }

    /** Every transformation the sinks depend on, inputs first. */
    static List<Transformation<?>> topological(List<Transformation<?>> sinks) {
        Set<Transformation<?>> seen = new LinkedHashSet<>();
        for (Transformation<?> t : sinks) {
            visit(t, seen);
        }
        return new ArrayList<>(seen);
    }

    static void visit(Transformation<?> t, Set<Transformation<?>> seen) {
        if (seen.contains(t)) {
            return;
        }
        for (Transformation<?> in : t.getInputs()) {
            visit(in, seen);
        }
        seen.add(t);
    }

    /** The INSERT statements of a query: one, or each one in an EXECUTE STATEMENT SET. */
    static List<String> inserts(String sql) {
        String s = sql.replaceAll("(?m)--.*$", "").strip();
        if (!s.toUpperCase().startsWith("EXECUTE STATEMENT SET")) {
            return List.of(s);
        }
        s = s.replaceFirst("(?is)^EXECUTE STATEMENT SET\\s*BEGIN", "").replaceFirst("(?is)END\\s*;?\\s*$", "");
        List<String> out = new ArrayList<>();
        for (String part : s.split(";")) {
            if (!part.isBlank()) {
                out.add(part.strip());
            }
        }
        return out;
    }

    /**
     * The transformations with the given names ("GroupAggregate" matches "GroupAggregate[4]"), in
     * order. When a job has several, the one fed by exactly the given source tables.
     */
    @SuppressWarnings("unchecked")
    static List<Transformation<RowData>> find(List<Transformation<?>> all, List<String> names, Set<String> tables, Set<String> upstream) {
        List<Transformation<RowData>> chain = new ArrayList<>();
        for (String name : names) {
            List<Transformation<?>> matches = all.stream()
                    .filter(t -> t.getName().equals(name) || t.getName().startsWith(name + "["))
                    .toList();
            if (matches.size() > 1) {
                matches = matches.stream().filter(t -> sourcesOf(t, tables).equals(upstream)).toList();
            }
            if (matches.size() != 1) {
                throw new IllegalStateException("expected one transformation named " + name + ", found "
                        + matches.stream().map(Transformation::getName).toList()
                        + " among " + all.stream().map(Transformation::getName).toList());
            }
            if (!(matches.get(0) instanceof OneInputTransformation) && !(matches.get(0) instanceof TwoInputTransformation)) {
                throw new IllegalStateException(name + " has neither one input nor two: " + matches.get(0).getClass().getSimpleName());
            }
            chain.add((Transformation<RowData>) matches.get(0));
        }
        return chain;
    }

    /** The source tables a transformation reads from, directly or not. */
    static Set<String> sourcesOf(Transformation<?> t, Set<String> tables) {
        Set<Transformation<?>> above = new LinkedHashSet<>();
        visit(t, above);
        Set<String> found = new TreeSet<>();
        above.forEach(a -> {
            if (tables.contains(a.getName())) {
                found.add(a.getName());
            }
        });
        return found;
    }

    /** One Flink operator, with one input or two, in a test harness. */
    static class Stage {
        final AbstractStreamOperatorTestHarness<RowData> harness;
        final List<RowType> ins = new ArrayList<>();
        final List<RowDataSerializer> inSerializers = new ArrayList<>();
        final List<Set<String>> inSources = new ArrayList<>();
        final RowType out;
        final RowType key;

        @SuppressWarnings({"unchecked", "rawtypes"})
        Stage(Transformation<RowData> t, Set<String> tables) throws Exception {
            for (Transformation<?> input : t.getInputs()) {
                RowType type = ((InternalTypeInfo<RowData>) input.getOutputType()).toRowType();
                ins.add(type);
                inSerializers.add(new RowDataSerializer(type));
                inSources.add(sourcesOf(input, tables));
            }
            this.out = ((InternalTypeInfo<RowData>) t.getOutputType()).toRowType();
            TypeInformation keyType;
            if (t instanceof OneInputTransformation) {
                OneInputTransformation<RowData, RowData> one = (OneInputTransformation<RowData, RowData>) t;
                keyType = one.getStateKeyType();
                KeySelector<RowData, ?> selector = one.getStateKeySelector();
                this.harness = selector != null
                        ? new KeyedOneInputStreamOperatorTestHarness(one.getOperatorFactory(), selector, keyType, inSerializers.get(0))
                        : new OneInputStreamOperatorTestHarness<>(one.getOperatorFactory(), 1, 1, 0);
            } else {
                // The two-input harnesses take an operator, not a factory.
                TwoInputTransformation<RowData, RowData, RowData> two = (TwoInputTransformation<RowData, RowData, RowData>) t;
                keyType = two.getStateKeyType();
                TwoInputStreamOperator<RowData, RowData, RowData> operator = operator(two.getOperatorFactory());
                this.harness = two.getStateKeySelector1() != null
                        ? new KeyedTwoInputStreamOperatorTestHarness(operator, two.getStateKeySelector1(), two.getStateKeySelector2(), keyType)
                        : new TwoInputStreamOperatorTestHarness<>(operator);
            }
            this.key = keyType instanceof InternalTypeInfo ? ((InternalTypeInfo<?>) keyType).toRowType() : null;
            harness.setStateBackend(new HashMapStateBackend());
            harness.setup(new RowDataSerializer(out));
            harness.open();
        }

        @SuppressWarnings("unchecked")
        static TwoInputStreamOperator<RowData, RowData, RowData> operator(StreamOperatorFactory<RowData> factory) {
            if (factory instanceof SimpleOperatorFactory) {
                return (TwoInputStreamOperator<RowData, RowData, RowData>) ((SimpleOperatorFactory<RowData>) factory).getOperator();
            }
            if (factory instanceof CodeGenOperatorFactory) {
                return (TwoInputStreamOperator<RowData, RowData, RowData>) ((CodeGenOperatorFactory<RowData>) factory)
                        .getGeneratedClass().newInstance(Thread.currentThread().getContextClassLoader());
            }
            throw new IllegalStateException("can't make an operator from " + factory.getClass().getSimpleName());
        }

        /** Which input reads from the given source tables. */
        int input(Set<String> from) {
            if (ins.size() == 1) {
                return 0;
            }
            for (int i = 0; i < inSources.size(); i++) {
                if (inSources.get(i).equals(from)) {
                    return i;
                }
            }
            throw new IllegalStateException("no input reads from " + from + "; the inputs read from " + inSources);
        }

        /**
         * Hands an item (a StreamRecord or a Watermark) to an input of the operator. A row arrives
         * as BinaryRowData, as it would after an exchange or from a source: some operators rely on
         * its layout (a global window aggregation reads the slice end, a BIGINT, as a compact
         * timestamp).
         */
        @SuppressWarnings("unchecked")
        void push(Object item, int input) throws Exception {
            Object o = item;
            if (item instanceof StreamRecord) {
                StreamRecord<RowData> r = (StreamRecord<RowData>) item;
                RowData binary = inSerializers.get(input).toBinaryRow(r.getValue()).copy();
                o = r.hasTimestamp() ? new StreamRecord<>(binary, r.getTimestamp()) : new StreamRecord<>(binary);
            }
            if (harness instanceof OneInputStreamOperatorTestHarness) {
                OneInputStreamOperatorTestHarness<RowData, RowData> h = (OneInputStreamOperatorTestHarness<RowData, RowData>) harness;
                if (o instanceof Watermark) {
                    h.processWatermark((Watermark) o);
                } else {
                    h.processElement((StreamRecord<RowData>) o);
                }
            } else {
                TwoInputStreamOperatorTestHarness<RowData, RowData, RowData> h = (TwoInputStreamOperatorTestHarness<RowData, RowData, RowData>) harness;
                if (o instanceof Watermark) {
                    if (input == 0) {
                        h.processWatermark1((Watermark) o);
                    } else {
                        h.processWatermark2((Watermark) o);
                    }
                } else if (input == 0) {
                    h.processElement1((StreamRecord<RowData>) o);
                } else {
                    h.processElement2((StreamRecord<RowData>) o);
                }
            }
        }

        /**
         * Writes what the operator holds in memory to state, as a checkpoint would, and takes what
         * it emitted. Window aggregations buffer records until prepareSnapshotPreBarrier; Rank's
         * FastTop1Function caches rows until snapshotState.
         */
        List<Object> drain(long checkpoint) throws Exception {
            harness.prepareSnapshotPreBarrier(checkpoint);
            harness.snapshot(checkpoint, checkpoint);
            List<Object> items = new ArrayList<>();
            for (Object o : harness.getOutput()) {
                if (o instanceof StreamRecord || o instanceof Watermark) {
                    items.add(o);
                }
            }
            harness.getOutput().clear();
            return items;
        }
    }

    static boolean check(JsonNode op, List<Transformation<RowData>> chain, Set<String> tables) throws Exception {
        List<Stage> stages = new ArrayList<>();
        for (Transformation<RowData> t : chain) {
            stages.add(new Stage(t, tables));
        }
        Stage first = stages.get(0);
        Stage last = stages.get(stages.size() - 1);

        // The simulator's names for output fields the planner names itself (w0$o0, ...).
        Map<String, String> fields = new java.util.HashMap<>();
        if (op.has("fields")) {
            op.get("fields").fields().forEachRemaining(f -> fields.put(f.getKey(), f.getValue().asText()));
        }
        Set<String> nulls = new TreeSet<>();
        // Fields the simulator shows coalesced from both sides of an outer join (see the exporter).
        Map<String, String> coalesce = new java.util.HashMap<>();
        if (op.has("coalesce")) {
            op.get("coalesce").fields().forEachRemaining(f -> coalesce.put(f.getKey(), f.getValue().asText()));
        }
        // The output fields the simulator models: a join's own output carries both sides' columns
        // (store_id0, ...), where the simulator's rows only have the ones the pattern shows.
        // Fields it never models are left out of the comparison (Replay checks the sink's rows).
        Set<String> modelled = new TreeSet<>();
        Map<String, String> simToFlink = new java.util.HashMap<>();
        fields.forEach((flink, sim) -> simToFlink.putIfAbsent(sim, flink));
        for (JsonNode step : op.get("steps")) {
            for (JsonNode o : step.get("out")) {
                if (o.has("row")) {
                    o.get("row").fieldNames().forEachRemaining(f -> modelled.add(f));
                }
            }
        }
        Set<String> unmodelled = new TreeSet<>();
        for (String f : last.out.getFieldNames()) {
            if (!modelled.contains(fields.getOrDefault(f, f))) {
                unmodelled.add(f);
            }
        }

        boolean pass = true;
        long checkpoint = 0;
        int n = 0;
        for (JsonNode step : op.get("steps")) {
            n++;
            // A two-input operator: the input that reads from the same source tables as the
            // simulator's input edge.
            int input = first.input(strings(step.get("from")));
            List<Object> items = List.of(toItem(step.get("item"), first.ins.get(input), nulls));
            for (Stage s : stages) {
                for (Object item : items) {
                    s.push(item, s == first ? input : 0);
                }
                items = s.drain(++checkpoint);
            }

            List<String> simOut = new ArrayList<>();
            step.get("out").forEach(o -> simOut.add(describeSim(o, last.out, fields, unmodelled)));
            List<String> flinkOut = new ArrayList<>();
            items.forEach(o -> flinkOut.add(describe(o, last.out, unmodelled, coalesce)));

            Set<String> simState = strings(step.get("facts").get("state"));
            // An operator whose panel shows no timers leaves them out of its facts.
            boolean showsTimers = step.get("facts").has("timers");
            Set<String> simTimers = strings(step.get("facts").get("timers"));
            Set<String> flinkState = new TreeSet<>();
            List<String> values = new ArrayList<>();
            state(last, op.get("op").asText(), flinkState, values);
            Set<String> flinkTimers = timers(last);

            // Values, for the states the simulator's facts name: every entry, as "state key = value".
            JsonNode simValues = step.get("facts").get("values");
            Set<String> simVals = new TreeSet<>();
            Set<String> flinkVals = new TreeSet<>();
            if (simValues != null) {
                simValues.fields().forEachRemaining(st -> st.getValue().fields().forEachRemaining(
                        e -> simVals.add(st.getKey() + " " + e.getKey() + " = " + renderSim(e.getValue(), first))));
                for (String v : values) {
                    String name = v.substring(0, v.indexOf(' '));
                    if (simValues.has(name)) {
                        flinkVals.add(v);
                    }
                }
            }

            boolean ok = simOut.equals(flinkOut) && simState.equals(flinkState) && (!showsTimers || simTimers.equals(flinkTimers))
                    && simVals.equals(flinkVals);
            pass &= ok;
            System.out.printf("  %s step %d: %s%n", ok ? "PASS" : "FAIL", n, describeSimItem(step.get("item")));
            if (!ok) {
                System.out.printf("      %s%n", step.get("caption").asText());
            }
            row("emitted", simOut, flinkOut);
            row("state", simState, flinkState);
            if (showsTimers) {
                row("timers", simTimers, flinkTimers);
            } else {
                System.out.printf("      %-8s flink %s (not shown in the simulator)%n", "timers", flinkTimers.isEmpty() ? "none" : String.join(", ", flinkTimers));
            }
            if (simValues != null) {
                if (simVals.equals(flinkVals)) {
                    List<String> names = new ArrayList<>();
                    simValues.fieldNames().forEachRemaining(names::add);
                    System.out.printf("      %-8s %d entries in %s%n", "values", simVals.size(), String.join(", ", names));
                } else {
                    Set<String> onlySim = new TreeSet<>(simVals);
                    onlySim.removeAll(flinkVals);
                    Set<String> onlyFlink = new TreeSet<>(flinkVals);
                    onlyFlink.removeAll(simVals);
                    System.out.printf("    ≠ %-8s%n", "values");
                    onlySim.forEach(v -> System.out.printf("        sim only    %s%n", v));
                    onlyFlink.forEach(v -> System.out.printf("        flink only  %s%n", v));
                }
            }
            if (!ok || dump) {
                values.forEach(v -> System.out.printf("      flink state: %s%n", v));
            }
        }
        for (Stage s : stages) {
            s.harness.close();
        }
        if (!unmodelled.isEmpty() && op.get("steps").findValues("row").size() > 0) {
            System.out.printf("  (output fields the simulator doesn't model, not compared: %s)%n", String.join(", ", unmodelled));
        }
        if (!nulls.isEmpty()) {
            System.out.printf("  (fields the simulator doesn't model, given to Flink as null: %s)%n", String.join(", ", nulls));
        }
        return pass;
    }

    static void row(String what, java.util.Collection<String> sim, java.util.Collection<String> flink) {
        boolean same = new ArrayList<>(sim).equals(new ArrayList<>(flink));
        if (same) {
            System.out.printf("      %-8s %s%n", what, sim.isEmpty() ? "none" : String.join(", ", sim));
        } else {
            System.out.printf("    ≠ %-8s sim   %s%n", what, sim.isEmpty() ? "none" : String.join(", ", sim));
            System.out.printf("      %-8s flink %s%n", "", flink.isEmpty() ? "none" : String.join(", ", flink));
        }
    }

    static Set<String> strings(JsonNode array) {
        Set<String> s = new TreeSet<>();
        if (array != null) {
            array.forEach(v -> s.add(v.asText()));
        }
        return s;
    }

    // ---- Flink state and timers ------------------------------------------------------------

    /**
     * Every (key, namespace) in every keyed state the operator registered, and their values.
     *
     * <p>One exception, for session windows: Flink keeps a session's accumulator under the window
     * it started as, and a per-key map from each merged session to that window
     * (MergingWindowSet). The simulator shows the merged sessions, so for those the map's keys
     * are listed, not the accumulators' namespaces.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void state(Stage s, String op, Set<String> keys, List<String> values) throws Exception {
        KeyedStateBackend<?> backend = s.harness.getOperator().getKeyedStateBackend();
        if (backend == null) {
            return;
        }
        if (!(backend instanceof HeapKeyedStateBackend)) {
            throw new IllegalStateException("expected the heap state backend, got " + backend.getClass().getSimpleName());
        }
        Map<String, StateTable<?, ?, ?>> tables = (Map<String, StateTable<?, ?, ?>>) field(backend, "registeredKVStates");
        for (Map.Entry<String, StateTable<?, ?, ?>> e : tables.entrySet()) {
            for (Object o : (Iterable) e.getValue()) {
                StateEntry<?, ?, ?> entry = (StateEntry<?, ?, ?>) o;
                String k = key(entry.getKey(), s.key) + namespace(entry.getNamespace());
                if (op.equals("sessionWindowAggregate")) {
                    if (e.getKey().equals("session-window-mapping")) {
                        for (Object w : ((Map<?, ?>) entry.getState()).keySet()) {
                            keys.add(key(entry.getKey(), s.key) + namespace(w));
                        }
                    }
                } else {
                    keys.add(k);
                }
                values.add(e.getKey() + " " + k + " = " + value(entry.getState(), e.getValue().getStateSerializer()));
            }
        }
        Collections.sort(values);
    }

    /** Every timer, from every timer service the operator registered, as key@time. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static Set<String> timers(Stage s) throws Exception {
        Set<String> out = new TreeSet<>();
        KeyedStateBackend<?> backend = s.harness.getOperator().getKeyedStateBackend();
        Object manager = field(s.harness.getOperator(), "timeServiceManager");
        if (backend == null || manager == null) {
            return out;
        }
        Map<String, InternalTimerServiceImpl<?, ?>> services = (Map<String, InternalTimerServiceImpl<?, ?>>) field(manager, "timerServices");
        for (InternalTimerServiceImpl service : services.values()) {
            service.forEachEventTimeTimer((ns, ts) -> out.add(key(backend.getCurrentKey(), s.key) + "@" + time((Long) ts, true)));
            service.forEachProcessingTimeTimer((ns, ts) -> out.add(key(backend.getCurrentKey(), s.key) + "@" + time((Long) ts, true) + " (processing time)"));
        }
        return out;
    }

    static Object field(Object target, String name) throws Exception {
        for (Class<?> c = target.getClass(); c != null; c = c.getSuperclass()) {
            try {
                Field f = c.getDeclaredField(name);
                f.setAccessible(true);
                return f.get(target);
            } catch (NoSuchFieldException e) {
                // keep looking in the superclass
            }
        }
        throw new NoSuchFieldException(name + " in " + target.getClass());
    }

    /** A key as the simulator writes it: the key fields' values, joined by a space. */
    static String key(Object k, RowType type) {
        if (k instanceof RowData && type != null) {
            RowData r = (RowData) k;
            List<String> parts = new ArrayList<>();
            for (int i = 0; i < type.getFieldCount(); i++) {
                parts.add(text(RowData.createFieldGetter(type.getTypeAt(i), i).getFieldOrNull(r), type.getTypeAt(i)));
            }
            return String.join(" ", parts);
        }
        return String.valueOf(k);
    }

    /** A namespace: none for VoidNamespace; a time of day for a window or slice end. */
    static String namespace(Object ns) {
        if (ns == null || ns instanceof VoidNamespace) {
            return "";
        }
        if (ns instanceof Long) {
            return "|" + time((Long) ns, false);
        }
        if (ns instanceof TimeWindow) {
            TimeWindow w = (TimeWindow) ns;
            return "|" + time(w.getStart(), false) + "–" + time(w.getEnd(), false);
        }
        return "|" + ns;
    }

    /**
     * A state value as text, decoded with its serializer: rows field by field, maps (sorted by
     * key), lists, tuples, windows, and epoch millis on the replay's day as times of day.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    static String value(Object v, Object ser) {
        if (v == null) {
            return "null";
        }
        try {
            if (v instanceof RowData && ser instanceof RowDataSerializer) {
                LogicalType[] types = (LogicalType[]) field(ser, "types");
                RowData r = (RowData) v;
                List<String> parts = new ArrayList<>();
                for (int i = 0; i < types.length; i++) {
                    parts.add(text(RowData.createFieldGetter(types[i], i).getFieldOrNull(r), types[i]));
                }
                // A stored row keeps its row kind: shown unless it's an insert.
                String kind = r.getRowKind() == RowKind.INSERT ? "" : r.getRowKind().shortString();
                return kind + "(" + String.join(", ", parts) + ")";
            }
            if (v instanceof Map) {
                Object ks = ser.getClass().getMethod("getKeySerializer").invoke(ser);
                Object vs = ser.getClass().getMethod("getValueSerializer").invoke(ser);
                List<String> parts = new ArrayList<>();
                for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                    parts.add(value(e.getKey(), ks) + "=" + value(e.getValue(), vs));
                }
                Collections.sort(parts);
                return "{" + String.join(", ", parts) + "}";
            }
            if (v instanceof List) {
                Object es = ser.getClass().getMethod("getElementSerializer").invoke(ser);
                List<String> parts = new ArrayList<>();
                for (Object e : (List<?>) v) {
                    parts.add(value(e, es));
                }
                return "[" + String.join(", ", parts) + "]";
            }
            if (v instanceof org.apache.flink.api.java.tuple.Tuple) {
                Object[] fs = (Object[]) ser.getClass().getMethod("getFieldSerializers").invoke(ser);
                org.apache.flink.api.java.tuple.Tuple t = (org.apache.flink.api.java.tuple.Tuple) v;
                List<String> parts = new ArrayList<>();
                for (int i = 0; i < t.getArity(); i++) {
                    parts.add(value(t.getField(i), fs[i]));
                }
                return "(" + String.join(", ", parts) + ")";
            }
        } catch (Exception e) {
            return v.toString();
        }
        if (v instanceof TimeWindow) {
            return namespace(v).substring(1);
        }
        if (v instanceof Long) {
            long m = (Long) v;
            long day = Replay.millis("00:00:00");
            if (m >= day && m < day + 86_400_000L) {
                return time(m, m % 1000 != 0);
            }
        }
        return String.valueOf(v);
    }

    /** Epoch millis as a time of day on the replay's day: HH:MM:SS, with .mmm when asked or needed. */
    static String time(long millis, boolean withMillis) {
        LocalTime t = LocalDateTime.ofEpochSecond(Math.floorDiv(millis, 1000), (int) Math.floorMod(millis, 1000) * 1_000_000, ZoneOffset.UTC).toLocalTime();
        String hms = t.format(Replay.HMS);
        long m = Math.floorMod(millis, 1000);
        return withMillis || m != 0 ? String.format("%s.%03d", hms, m) : hms;
    }

    // ---- Converting rows ---------------------------------------------------------------------

    /** A simulator item as a StreamRecord (with the operator's input type) or a Watermark. */
    static Object toItem(JsonNode item, RowType type, Set<String> nulls) {
        if (item.has("wm")) {
            return new Watermark(Replay.millis(item.get("wm").asText()));
        }
        JsonNode row = item.get("row");
        GenericRowData r = new GenericRowData(kind(item.get("kind").asText()), type.getFieldCount());
        long ts = Long.MIN_VALUE;
        for (int i = 0; i < type.getFieldCount(); i++) {
            String name = type.getFieldNames().get(i);
            JsonNode v = row.get(name);
            // A delete from an upsert source carries only its key; a column the pattern doesn't
            // use may be left out of its data, and a processing-time attribute isn't
            // materialized. Those are null.
            if (v == null) {
                nulls.add(name);
            }
            LogicalType ft = type.getTypeAt(i);
            Object internal = internal(v == null || v.isNull() ? null : v.asText(), ft);
            r.setField(i, internal);
            if (internal instanceof TimestampData && ts == Long.MIN_VALUE) {
                ts = ((TimestampData) internal).getMillisecond();
            }
        }
        return ts == Long.MIN_VALUE ? new StreamRecord<>(r) : new StreamRecord<>(r, ts);
    }

    static RowKind kind(String k) {
        switch (k) {
            case "-U": return RowKind.UPDATE_BEFORE;
            case "+U": return RowKind.UPDATE_AFTER;
            case "-D": return RowKind.DELETE;
            default: return RowKind.INSERT;
        }
    }

    static Object internal(String v, LogicalType t) {
        if (v == null) {
            return null;
        }
        switch (t.getTypeRoot()) {
            case BIGINT: return Long.parseLong(v);
            case INTEGER: return Integer.parseInt(v);
            case DOUBLE: return Double.parseDouble(v);
            case BOOLEAN: return Boolean.parseBoolean(v);
            case DECIMAL: {
                DecimalType d = (DecimalType) t;
                return DecimalData.fromBigDecimal(new BigDecimal(v), d.getPrecision(), d.getScale());
            }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampData.fromEpochMillis(Replay.millis(v));
            default: return StringData.fromString(v);
        }
    }

    /** An internal value in the same text form Replay compares sink rows in. */
    static String text(Object v, LogicalType t) {
        if (v == null) {
            return "null";
        }
        Object external = v;
        if (v instanceof DecimalData) {
            external = ((DecimalData) v).toBigDecimal();
        } else if (v instanceof TimestampData) {
            external = ((TimestampData) v).toLocalDateTime();
        } else if (v instanceof StringData) {
            external = v.toString();
        }
        return Replay.format(external, t.asSummaryString());
    }

    /** A Flink output item: +I[7, 1, 18.00], or wm 12:05:00. */
    @SuppressWarnings("unchecked")
    static String describe(Object o, RowType type, Set<String> skip, Map<String, String> coalesce) {
        if (o instanceof Watermark) {
            return "wm " + time(((Watermark) o).getTimestamp(), false);
        }
        RowData r = ((StreamRecord<RowData>) o).getValue();
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < type.getFieldCount(); i++) {
            String name = type.getFieldNames().get(i);
            if (skip.contains(name)) {
                continue;
            }
            int j = i;
            if (r.isNullAt(i) && coalesce.containsKey(name)) {
                j = type.getFieldIndex(coalesce.get(name));
            }
            parts.add(name + "=" + text(RowData.createFieldGetter(type.getTypeAt(j), j).getFieldOrNull(r), type.getTypeAt(j)));
        }
        return r.getRowKind().shortString() + "[" + String.join(", ", parts) + "]";
    }

    /** A simulator output item, with the fields of the Flink operator's output, in its order. */
    static String describeSim(JsonNode o, RowType type, Map<String, String> fields, Set<String> skip) {
        if (o.has("wm")) {
            return "wm " + Replay.pad(o.get("wm").asText());
        }
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < type.getFieldCount(); i++) {
            String name = type.getFieldNames().get(i);
            if (skip.contains(name)) {
                continue;
            }
            JsonNode v = o.get("row").get(fields.getOrDefault(name, name));
            parts.add(name + "=" + Replay.format(v == null || v.isNull() ? null : v.asText(), type.getTypeAt(i).asSummaryString()));
        }
        return o.get("kind").asText() + "[" + String.join(", ", parts) + "]";
    }

    /**
     * A value from the simulator's facts, in the text form value() gives Flink's: a row in the
     * Flink operator's input layout (and its kind, unless +I), a tuple, a map sorted by entry,
     * a list, or a scalar already written as Flink's would be.
     */
    static String renderSim(JsonNode v, Stage stage) {
        if (v == null || v.isNull()) {
            return "null";
        }
        if (v.isValueNode()) {
            return v.asText();
        }
        List<String> parts = new ArrayList<>();
        if (v.isArray()) {
            v.forEach(e -> parts.add(renderSim(e, stage)));
            return "[" + String.join(", ", parts) + "]";
        }
        if (v.has("row")) {
            RowType type = stage.ins.get(v.path("input").asInt(0));
            JsonNode row = v.get("row");
            for (int i = 0; i < type.getFieldCount(); i++) {
                JsonNode f = row.get(type.getFieldNames().get(i));
                parts.add(Replay.format(f == null || f.isNull() ? null : f.asText(), type.getTypeAt(i).asSummaryString()));
            }
            return (v.has("kind") ? v.get("kind").asText() : "") + "(" + String.join(", ", parts) + ")";
        }
        if (v.has("tuple")) {
            v.get("tuple").forEach(e -> parts.add(renderSim(e, stage)));
            return "(" + String.join(", ", parts) + ")";
        }
        if (v.has("map")) {
            v.get("map").forEach(e -> parts.add(renderSim(e.get(0), stage) + "=" + renderSim(e.get(1), stage)));
            Collections.sort(parts);
            return "{" + String.join(", ", parts) + "}";
        }
        throw new IllegalArgumentException("not a facts value: " + v);
    }

    static String describeSimItem(JsonNode item) {
        if (item.has("wm")) {
            return "watermark " + item.get("wm").asText();
        }
        return item.get("kind").asText() + " " + item.get("row");
    }
}
