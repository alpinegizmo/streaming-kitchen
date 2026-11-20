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
        // A statement set's INSERTs end in semicolons too, so its pieces are joined back up,
        // from STATEMENT SET through END.
        java.util.List<String> statements = new java.util.ArrayList<>();
        StringBuilder set = null;
        for (String piece : sql.toString().split(";\\s*(\\n|$)")) {
            if (set == null && piece.toUpperCase().contains("STATEMENT SET")) {
                set = new StringBuilder();
            }
            if (set == null) {
                statements.add(piece);
                continue;
            }
            set.append(piece).append(";\n");
            if (piece.strip().toUpperCase().endsWith("END")) {
                String joined = set.toString().strip();
                statements.add(joined.substring(0, joined.length() - 1));
                set = null;
            }
        }
        for (String statement : statements) {
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
