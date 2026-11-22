import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * A sink connector, 'capture', that records every row it receives, with its row kind, in
 * {@link #RESULTS}, keyed by the table's 'id' option. Its 'mode' option declares the changelog
 * mode of the real sink it stands in for, so the planner builds the same plan:
 * 'append' (kafka), 'upsert' (upsert-kafka, which needs a primary key) or 'retract'
 * (a changelog format such as debezium-json).
 */
public class CaptureSinkFactory implements DynamicTableSinkFactory {

    /** Captured rows per sink id: the row kind ("+I", "-U", "+U", "-D"), then the fields. */
    public static final Map<String, List<Object[]>> RESULTS = new ConcurrentHashMap<>();

    static final ConfigOption<String> MODE = ConfigOptions.key("mode").stringType().defaultValue("append");
    static final ConfigOption<String> ID = ConfigOptions.key("id").stringType().noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return "capture";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(ID);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(MODE);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        return new CaptureSink(helper.getOptions().get(MODE), helper.getOptions().get(ID), context.getPhysicalRowDataType());
    }

    static class CaptureSink implements DynamicTableSink {
        private final String mode;
        private final String id;
        private final DataType type;

        CaptureSink(String mode, String id, DataType type) {
            this.mode = mode;
            this.id = id;
            this.type = type;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requested) {
            switch (mode) {
                case "upsert": return ChangelogMode.upsert();
                case "retract": return ChangelogMode.all();
                default: return ChangelogMode.insertOnly();
            }
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return SinkV2Provider.of(new Capture(id, context.createDataStructureConverter(type)));
        }

        @Override
        public DynamicTableSink copy() {
            return new CaptureSink(mode, id, type);
        }

        @Override
        public String asSummaryString() {
            return "capture(" + mode + ")";
        }
    }

    static class Capture implements Sink<RowData> {
        private final String id;
        private final DynamicTableSink.DataStructureConverter converter;

        Capture(String id, DynamicTableSink.DataStructureConverter converter) {
            this.id = id;
            this.converter = converter;
        }

        @Override
        public SinkWriter<RowData> createWriter(WriterInitContext context) {
            converter.open(RuntimeConverter.Context.create(Capture.class.getClassLoader()));
            List<Object[]> out = RESULTS.computeIfAbsent(id, k -> Collections.synchronizedList(new ArrayList<>()));
            return new SinkWriter<>() {
                @Override
                public void write(RowData data, Context ctx) {
                    Row row = (Row) converter.toExternal(data);
                    Object[] rec = new Object[row.getArity() + 1];
                    rec[0] = data.getRowKind().shortString();
                    for (int i = 0; i < row.getArity(); i++) {
                        rec[i + 1] = row.getField(i);
                    }
                    out.add(rec);
                }

                @Override
                public void flush(boolean endOfInput) {}

                @Override
                public void close() {}
            };
        }
    }
}
