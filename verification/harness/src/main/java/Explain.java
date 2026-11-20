import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * Runs every statement in a SQL file. DDL is executed, and the result of each EXPLAIN is
 * printed in full. A failing statement prints "[ERROR] ..." and the rest still run.
 */
public class Explain {
    public static void main(String[] args) throws Exception {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        StringBuilder sql = new StringBuilder();
        for (String line : Files.readAllLines(Path.of(args[0]))) {
            if (!line.strip().startsWith("--")) {
                sql.append(line).append('\n');
            }
        }
        for (String statement : sql.toString().split(";\\s*(\\n|$)")) {
            if (statement.isBlank()) {
                continue;
            }
            try {
                TableResult result = env.executeSql(statement);
                if (statement.strip().toUpperCase().startsWith("EXPLAIN")) {
                    try (CloseableIterator<Row> rows = result.collect()) {
                        while (rows.hasNext()) {
                            System.out.println(rows.next().getField(0));
                        }
                    }
                }
            } catch (Exception e) {
                Throwable root = e;
                while (root.getCause() != null) {
                    root = root.getCause();
                }
                System.out.println("[ERROR] " + e.getMessage() + " | " + root);
            }
        }
    }
}
