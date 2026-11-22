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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
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
 * <p>Some operators buffer records in memory before writing them to state, until a watermark or a
 * checkpoint (the window aggregations' RecordsWindowBuffer). Before each inspection the harness
 * flushes them, as a checkpoint would (prepareSnapshotPreBarrier).
 */
public class StateCheck {

    public static void main(String[] args) throws Exception {
        int failed = 0;
        int checked = 0;
        for (String arg : args) {
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
            List<OneInputTransformation<RowData, RowData>> chain = find(all, names, tables, upstream);
            System.out.printf("operator %s: %s%n", op.get("node").asText(),
                    String.join(" -> ", chain.stream().map(Transformation::getName).toList()));
            pass &= check(op, chain);
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
    static List<OneInputTransformation<RowData, RowData>> find(List<Transformation<?>> all, List<String> names, Set<String> tables, Set<String> upstream) {
        List<OneInputTransformation<RowData, RowData>> chain = new ArrayList<>();
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
            if (!(matches.get(0) instanceof OneInputTransformation)) {
                throw new IllegalStateException(name + " isn't a one-input transformation: " + matches.get(0).getClass().getSimpleName());
            }
            chain.add((OneInputTransformation<RowData, RowData>) matches.get(0));
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

    /** One Flink operator in a test harness. */
    static class Stage {
        final OneInputTransformation<RowData, RowData> t;
        final OneInputStreamOperatorTestHarness<RowData, RowData> harness;
        final RowType in;
        final RowType out;
        final RowType key;
        final RowDataSerializer inSerializer;

        @SuppressWarnings({"unchecked", "rawtypes"})
        Stage(OneInputTransformation<RowData, RowData> t) throws Exception {
            this.t = t;
            this.in = ((InternalTypeInfo<RowData>) t.getInputs().get(0).getOutputType()).toRowType();
            this.inSerializer = new RowDataSerializer(in);
            this.out = ((InternalTypeInfo<RowData>) t.getOutputType()).toRowType();
            StreamOperatorFactory<RowData> factory = t.getOperatorFactory();
            KeySelector<RowData, ?> selector = t.getStateKeySelector();
            if (selector != null) {
                TypeInformation keyType = t.getStateKeyType();
                this.key = keyType instanceof InternalTypeInfo ? ((InternalTypeInfo<?>) keyType).toRowType() : null;
                this.harness = new KeyedOneInputStreamOperatorTestHarness(factory, selector, keyType, new RowDataSerializer(in));
            } else {
                this.key = null;
                this.harness = new OneInputStreamOperatorTestHarness<>(factory, 1, 1, 0);
            }
            harness.setStateBackend(new HashMapStateBackend());
            harness.setup(new RowDataSerializer(out));
            harness.open();
        }

        /**
         * Hands an item (a StreamRecord or a Watermark) to the operator. A row arrives as
         * BinaryRowData, as it would after an exchange or from a source: some operators rely on
         * its layout (a global window aggregation reads the slice end, a BIGINT, as a compact
         * timestamp).
         */
        @SuppressWarnings("unchecked")
        void push(Object item) throws Exception {
            if (item instanceof Watermark) {
                harness.processWatermark((Watermark) item);
            } else {
                StreamRecord<RowData> r = (StreamRecord<RowData>) item;
                RowData binary = inSerializer.toBinaryRow(r.getValue()).copy();
                harness.processElement(r.hasTimestamp() ? new StreamRecord<>(binary, r.getTimestamp()) : new StreamRecord<>(binary));
            }
        }

        /** Flushes buffered records to state, as a checkpoint would, and takes what the operator emitted. */
        List<Object> drain(long checkpoint) throws Exception {
            harness.prepareSnapshotPreBarrier(checkpoint);
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

    static boolean check(JsonNode op, List<OneInputTransformation<RowData, RowData>> chain) throws Exception {
        List<Stage> stages = new ArrayList<>();
        for (OneInputTransformation<RowData, RowData> t : chain) {
            stages.add(new Stage(t));
        }
        Stage first = stages.get(0);
        Stage last = stages.get(stages.size() - 1);

        boolean pass = true;
        long checkpoint = 0;
        int n = 0;
        for (JsonNode step : op.get("steps")) {
            n++;
            List<Object> items = List.of(toItem(step.get("item"), first.in));
            for (Stage s : stages) {
                for (Object item : items) {
                    s.push(item);
                }
                items = s.drain(++checkpoint);
            }

            List<String> simOut = new ArrayList<>();
            step.get("out").forEach(o -> simOut.add(describeSim(o, last.out)));
            List<String> flinkOut = new ArrayList<>();
            items.forEach(o -> flinkOut.add(describe(o, last.out)));

            Set<String> simState = strings(step.get("facts").get("state"));
            Set<String> simTimers = strings(step.get("facts").get("timers"));
            Set<String> flinkState = new TreeSet<>();
            List<String> values = new ArrayList<>();
            state(last, flinkState, values);
            Set<String> flinkTimers = timers(last);

            boolean ok = simOut.equals(flinkOut) && simState.equals(flinkState) && simTimers.equals(flinkTimers);
            pass &= ok;
            System.out.printf("  %s step %d: %s%n", ok ? "PASS" : "FAIL", n, describeSimItem(step.get("item")));
            if (!ok) {
                System.out.printf("      %s%n", step.get("caption").asText());
            }
            row("emitted", simOut, flinkOut);
            row("state", simState, flinkState);
            row("timers", simTimers, flinkTimers);
            if (!ok) {
                values.forEach(v -> System.out.printf("      flink state: %s%n", v));
            }
        }
        for (Stage s : stages) {
            s.harness.close();
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

    /** Every (key, namespace) in every keyed state the operator registered, and their values. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static void state(Stage s, Set<String> keys, List<String> values) throws Exception {
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
                keys.add(k);
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
        return "|" + ns;
    }

    static String value(Object v, Object serializer) {
        if (v instanceof RowData && serializer instanceof RowDataSerializer) {
            try {
                LogicalType[] types = (LogicalType[]) field(serializer, "types");
                RowData r = (RowData) v;
                List<String> parts = new ArrayList<>();
                for (int i = 0; i < types.length; i++) {
                    parts.add(text(RowData.createFieldGetter(types[i], i).getFieldOrNull(r), types[i]));
                }
                return "(" + String.join(", ", parts) + ")";
            } catch (Exception e) {
                return v.toString();
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
    static Object toItem(JsonNode item, RowType type) {
        if (item.has("wm")) {
            return new Watermark(Replay.millis(item.get("wm").asText()));
        }
        JsonNode row = item.get("row");
        GenericRowData r = new GenericRowData(kind(item.get("kind").asText()), type.getFieldCount());
        long ts = Long.MIN_VALUE;
        for (int i = 0; i < type.getFieldCount(); i++) {
            String name = type.getFieldNames().get(i);
            JsonNode v = row.get(name);
            if (v == null) {
                throw new IllegalStateException("the simulator's row has no field " + name + " (the Flink operator's input is " + type + "): " + row);
            }
            LogicalType ft = type.getTypeAt(i);
            Object internal = internal(v.isNull() ? null : v.asText(), ft);
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
    static String describe(Object o, RowType type) {
        if (o instanceof Watermark) {
            return "wm " + time(((Watermark) o).getTimestamp(), false);
        }
        RowData r = ((StreamRecord<RowData>) o).getValue();
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < type.getFieldCount(); i++) {
            parts.add(type.getFieldNames().get(i) + "=" + text(RowData.createFieldGetter(type.getTypeAt(i), i).getFieldOrNull(r), type.getTypeAt(i)));
        }
        return r.getRowKind().shortString() + "[" + String.join(", ", parts) + "]";
    }

    /** A simulator output item, with the fields of the Flink operator's output, in its order. */
    static String describeSim(JsonNode o, RowType type) {
        if (o.has("wm")) {
            return "wm " + Replay.pad(o.get("wm").asText());
        }
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < type.getFieldCount(); i++) {
            String name = type.getFieldNames().get(i);
            JsonNode v = o.get("row").get(name);
            parts.add(name + "=" + Replay.format(v == null || v.isNull() ? null : v.asText(), type.getTypeAt(i).asSummaryString()));
        }
        return o.get("kind").asText() + "[" + String.join(", ", parts) + "]";
    }

    static String describeSimItem(JsonNode item) {
        if (item.has("wm")) {
            return "watermark " + item.get("wm").asText();
        }
        return item.get("kind").asText() + " " + item.get("row");
    }
}
