import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * Replays a pattern through real Flink, and compares its sink output with the simulator's.
 *
 * <p>Reads specs written by tools/export-replay.mjs. For each one it runs the pattern's own SQL on
 * a local mini-cluster, at parallelism 1:
 *
 * <ul>
 *   <li>One rate-limited source replays the spec's inputs, in order. Each table gets its own
 *       stream, with watermarks emitted exactly where the simulator's source emitted them (a
 *       punctuated watermark generator), and declared as SOURCE_WATERMARK(). An upsert-kafka table
 *       becomes an upsert changelog stream with its primary key, so the planner adds a
 *       ChangelogNormalize, as it does for the real connector.
 *   <li>The source never finishes, so Flink never sends the end-of-input watermark, which would
 *       fire windows and timers the simulator leaves open. Once the output has settled, the job is
 *       cancelled.
 *   <li>Each sink is a 'capture' table (see CaptureSinkFactory) with the real sink's changelog
 *       mode, so the plan is the one EXPLAIN shows.
 * </ul>
 *
 * <p>Outputs are compared row by row, with row kinds. With more than one source, the order in
 * which the inputs reach an operator isn't strictly fixed in a real job, so those outputs are
 * compared as unordered sets of rows.
 */
public class Replay {

    static final int RATE = 20; // inputs per second: slow enough that they arrive in order
    static final LocalDate DAY = LocalDate.of(2026, 1, 1);
    static final DateTimeFormatter HMS = DateTimeFormatter.ofPattern("HH:mm:ss");

    /** One input: a row of a table, or a watermark for it. Values are kept as strings. */
    public static class Event implements Serializable {
        public String table;
        public boolean wm;
        public boolean filler;
        public long ts;
        public String kind;
        public String[] values;

        public Event() {}
    }

    /** Emits a watermark exactly where the spec has one. */
    public static class Punctuated implements WatermarkGenerator<Event> {
        @Override
        public void onEvent(Event e, long ts, WatermarkOutput out) {
            if (e.wm) {
                out.emitWatermark(new Watermark(e.ts));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput out) {}
    }

    public static void main(String[] args) throws Exception {
        int failed = 0;
        for (String arg : args) {
            boolean pass;
            try {
                pass = run(Path.of(arg));
            } catch (Exception e) {
                System.out.printf("ERROR: %s%n", e);
                pass = false;
            }
            if (!pass) {
                failed++;
            }
        }
        System.out.printf("%n%d of %d passed%n", args.length - failed, args.length);
        System.exit(failed == 0 ? 0 : 1);
    }

    static boolean run(Path path) throws Exception {
        JsonNode spec = new ObjectMapper().readTree(path.toFile());
        System.out.printf("%n== %s (%s)%n", spec.get("label").asText(), path.getFileName());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setBufferTimeout(0);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().set("table.local-time-zone", "UTC");

        // The inputs, in the simulator's order.
        List<Event> events = new ArrayList<>();
        for (JsonNode in : spec.get("inputs")) {
            Event e = new Event();
            e.table = in.get("table").asText();
            JsonNode table = spec.get("sources").get(e.table);
            if (in.has("wm")) {
                e.wm = true;
                e.ts = millis(in.get("wm").asText());
            } else {
                e.kind = in.get("kind").asText();
                List<JsonNode> cols = physical(table);
                e.values = new String[cols.size()];
                for (int i = 0; i < cols.size(); i++) {
                    JsonNode v = in.get("row").get(cols.get(i).get("name").asText());
                    e.values[i] = v == null || v.isNull() ? null : v.asText();
                }
                JsonNode wm = table.get("watermark");
                JsonNode t = wm == null || wm.isNull() ? null : in.get("row").get(wm.get("column").asText());
                e.ts = t == null || t.isNull() ? 0 : millis(t.asText());
            }
            events.add(e);
        }
        final List<Event> all = events;
        final int n = all.size();
        GeneratorFunction<Long, Event> generator = i -> {
            if (i < n) {
                return all.get(i.intValue());
            }
            Event filler = new Event();
            filler.filler = true;
            filler.table = "";
            return filler;
        };
        DataStream<Event> replay = env.fromSource(
                new DataGeneratorSource<>(generator, Long.MAX_VALUE, RateLimiterStrategy.perSecond(RATE), TypeInformation.of(Event.class)),
                WatermarkStrategy.noWatermarks(), "replay");

        // A stream, with its own watermarks, for each source table.
        for (Iterator<Map.Entry<String, JsonNode>> it = spec.get("sources").fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            final String name = entry.getKey();
            JsonNode table = entry.getValue();
            List<JsonNode> cols = physical(table);
            String[] names = cols.stream().map(c -> c.get("name").asText()).toArray(String[]::new);
            final String[] types = cols.stream().map(c -> c.get("type").asText()).toArray(String[]::new);
            TypeInformation<?>[] infos = new TypeInformation<?>[types.length];
            for (int i = 0; i < types.length; i++) {
                infos[i] = typeInfo(types[i]);
            }
            DataStream<Row> rows = replay
                    .filter(e -> !e.filler && e.table.equals(name))
                    .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forGenerator(c -> new Punctuated()).withTimestampAssigner((e, t) -> e.ts))
                    .filter(e -> !e.wm)
                    .map(e -> toRow(e, types))
                    .returns(Types.ROW_NAMED(names, infos))
                    .name(name);

            // A primary key's columns are NOT NULL, as they are implicitly in DDL.
            List<String> pk = new ArrayList<>();
            table.get("pk").forEach(k -> pk.add(k.asText()));
            Schema.Builder schema = Schema.newBuilder();
            for (JsonNode c : table.get("columns")) {
                String col = c.get("name").asText();
                if (c.has("expr")) {
                    schema.columnByExpression(col, c.get("expr").asText());
                } else {
                    schema.column(col, c.get("type").asText() + (pk.contains(col) ? " NOT NULL" : ""));
                }
            }
            JsonNode wm = table.get("watermark");
            if (wm != null && !wm.isNull()) {
                schema.watermark(wm.get("column").asText(), "SOURCE_WATERMARK()");
            }
            if (!pk.isEmpty()) {
                schema.primaryKey(pk);
            }
            Table t = "upsert".equals(table.get("mode").asText())
                    ? tEnv.fromChangelogStream(rows, schema.build(), ChangelogMode.upsert())
                    : tEnv.fromDataStream(rows, schema.build());
            tEnv.createTemporaryView(name, t);
        }

        // Capture sinks, with the real sinks' columns, keys and changelog modes.
        for (Iterator<Map.Entry<String, JsonNode>> it = spec.get("sinks").fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            JsonNode table = entry.getValue();
            List<String> parts = new ArrayList<>();
            for (JsonNode c : table.get("columns")) {
                parts.add(c.get("name").asText() + " " + c.get("type").asText());
            }
            if (table.get("pk").size() > 0) {
                List<String> pk = new ArrayList<>();
                table.get("pk").forEach(k -> pk.add(k.asText()));
                parts.add("PRIMARY KEY (" + String.join(", ", pk) + ") NOT ENFORCED");
            }
            CaptureSinkFactory.RESULTS.remove(entry.getKey());
            tEnv.executeSql(String.format("CREATE TABLE %s (%s) WITH ('connector' = 'capture', 'mode' = '%s', 'id' = '%s')",
                    entry.getKey(), String.join(", ", parts), table.get("mode").asText(), entry.getKey()));
        }

        String sql = spec.get("sql").asText().strip();
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        // The simulator assumes the query was compiled at a given time, which ends a LATERAL
        // SNAPSHOT join's load phase. In a replay, compile time is now, so it's passed explicitly.
        if (spec.has("loadCompletedTime")) {
            String time = spec.get("loadCompletedTime").asText();
            sql = sql.replaceFirst("(on_time\\s*=>\\s*DESCRIPTOR\\(\\w+\\))",
                    "$1, load_completed_time => CAST(TIMESTAMP '" + DAY + " " + pad(time) + "' AS TIMESTAMP_LTZ(3))");
            System.out.printf("(load_completed_time => %s added, as if the query were compiled then)%n", time);
        }
        JobClient job = tEnv.executeSql(sql).getJobClient().orElseThrow();

        // Wait for every input to be read, then for the output to settle, and stop the job.
        long start = System.currentTimeMillis();
        long readAll = start + n * 1000L / RATE + 1500;
        int last = -1;
        long stableSince = System.currentTimeMillis();
        while (System.currentTimeMillis() < start + 120_000) {
            Thread.sleep(200);
            int count = CaptureSinkFactory.RESULTS.values().stream().mapToInt(List::size).sum();
            if (count != last) {
                last = count;
                stableSince = System.currentTimeMillis();
            } else if (System.currentTimeMillis() > readAll && System.currentTimeMillis() - stableSince > 2000) {
                break;
            }
        }
        job.cancel().get();

        // Compare, sink by sink.
        boolean ordered = spec.get("sources").size() == 1;
        boolean pass = true;
        for (Iterator<Map.Entry<String, JsonNode>> it = spec.get("sinks").fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> entry = it.next();
            String sink = entry.getKey();
            List<JsonNode> cols = physical(entry.getValue());
            List<String> expected = new ArrayList<>();
            for (JsonNode e : spec.get("expected")) {
                if (e.get("sink").asText().equals(sink)) {
                    List<String> fields = new ArrayList<>();
                    for (JsonNode c : cols) {
                        JsonNode v = e.get("row").get(c.get("name").asText());
                        fields.add(format(v == null || v.isNull() ? null : v.asText(), c.get("type").asText()));
                    }
                    expected.add(e.get("kind").asText() + fields);
                }
            }
            List<String> actual = new ArrayList<>();
            for (Object[] rec : CaptureSinkFactory.RESULTS.getOrDefault(sink, List.of())) {
                List<String> fields = new ArrayList<>();
                for (int i = 0; i < cols.size(); i++) {
                    fields.add(format(rec[i + 1], cols.get(i).get("type").asText()));
                }
                actual.add(rec[0] + fields.toString());
            }
            List<String> e = new ArrayList<>(expected);
            List<String> a = new ArrayList<>(actual);
            if (!ordered) {
                e.sort(null);
                a.sort(null);
            }
            boolean same = e.equals(a);
            pass &= same;
            System.out.printf("sink %s: %s%s%n", sink, same ? "PASS" : "FAIL", ordered ? "" : " (compared unordered: two or more sources)");
            int rows = Math.max(expected.size(), actual.size());
            for (int i = 0; i < rows; i++) {
                String x = i < expected.size() ? expected.get(i) : "";
                String y = i < actual.size() ? actual.get(i) : "";
                System.out.printf("  %s %-58s %s%n", x.equals(y) ? " " : "≠", "sim   " + x, "flink " + y);
            }
        }
        return pass;
    }

    static List<JsonNode> physical(JsonNode table) {
        List<JsonNode> cols = new ArrayList<>();
        table.get("columns").forEach(c -> {
            if (!c.has("expr")) {
                cols.add(c);
            }
        });
        return cols;
    }

    static long millis(String time) {
        return LocalDateTime.of(DAY, LocalTime.parse(pad(time))).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    static String pad(String time) {
        return time.length() == 5 ? time + ":00" : time;
    }

    static TypeInformation<?> typeInfo(String type) {
        String t = type.toUpperCase();
        if (t.startsWith("BIGINT")) return Types.LONG;
        if (t.startsWith("INT")) return Types.INT;
        if (t.startsWith("DECIMAL")) return Types.BIG_DEC;
        if (t.startsWith("TIMESTAMP")) return Types.LOCAL_DATE_TIME;
        if (t.startsWith("DOUBLE")) return Types.DOUBLE;
        if (t.startsWith("BOOLEAN")) return Types.BOOLEAN;
        return Types.STRING;
    }

    static Row toRow(Event e, String[] types) {
        RowKind kind;
        switch (e.kind) {
            case "-U": kind = RowKind.UPDATE_BEFORE; break;
            case "+U": kind = RowKind.UPDATE_AFTER; break;
            case "-D": kind = RowKind.DELETE; break;
            default: kind = RowKind.INSERT;
        }
        Object[] values = new Object[types.length];
        for (int i = 0; i < types.length; i++) {
            values[i] = parse(e.values[i], types[i]);
        }
        return Row.ofKind(kind, values);
    }

    static Object parse(String v, String type) {
        if (v == null) return null;
        String t = type.toUpperCase();
        if (t.startsWith("BIGINT")) return Long.parseLong(v);
        if (t.startsWith("INT")) return Integer.parseInt(v);
        if (t.startsWith("DECIMAL")) return new BigDecimal(v);
        if (t.startsWith("TIMESTAMP")) return LocalDateTime.of(DAY, LocalTime.parse(pad(v)));
        if (t.startsWith("DOUBLE")) return Double.parseDouble(v);
        if (t.startsWith("BOOLEAN")) return Boolean.parseBoolean(v);
        return v;
    }

    /** One canonical text form per type, for values from the simulator (strings) and Flink. */
    static String format(Object v, String type) {
        if (v == null) return "null";
        String t = type.toUpperCase();
        if (t.startsWith("DECIMAL")) {
            int scale = Integer.parseInt(t.replaceAll(".*,\\s*(\\d+)\\).*", "$1"));
            return new BigDecimal(v.toString()).setScale(scale, RoundingMode.HALF_UP).toPlainString();
        }
        if (t.startsWith("TIMESTAMP")) {
            return v instanceof LocalDateTime ? ((LocalDateTime) v).format(HMS) : pad(v.toString());
        }
        if (t.startsWith("BIGINT") || t.startsWith("INT")) {
            return Long.toString(new BigDecimal(v.toString()).longValueExact());
        }
        return v.toString();
    }
}
