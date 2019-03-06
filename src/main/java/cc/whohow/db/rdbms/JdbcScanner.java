package cc.whohow.db.rdbms;

import cc.whohow.db.IgnoreFirstPredicate;
import cc.whohow.db.Predicates;
import cc.whohow.db.rdbms.query.Rows;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class JdbcScanner implements Callable<JsonNode> {
    private final Rdbms rdbms;
    private ExecutorService executor;
    private Predicate<JsonNode> catalogFilter = Predicates::all;
    private Predicate<JsonNode> schemaFilter = Predicates::all;
    private Predicate<JsonNode> tableFilter = Predicates::all;
    private BiPredicate<JsonNode, JsonNode> rowFilter = Predicates::all;
    private BiConsumer<JsonNode, JsonNode> consumer = JdbcScanner::ignore;

    public JdbcScanner(Rdbms rdbms) {
        this.rdbms = rdbms;
    }

    private static <T1, T2> void ignore(T1 a, T2 b) {
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public Predicate<JsonNode> getCatalogFilter() {
        return catalogFilter;
    }

    public void setCatalogFilter(Predicate<JsonNode> catalogFilter) {
        this.catalogFilter = catalogFilter;
    }

    public Predicate<JsonNode> getSchemaFilter() {
        return schemaFilter;
    }

    public void setSchemaFilter(Predicate<JsonNode> schemaFilter) {
        this.schemaFilter = schemaFilter;
    }

    public Predicate<JsonNode> getTableFilter() {
        return tableFilter;
    }

    public void setTableFilter(Predicate<JsonNode> tableFilter) {
        this.tableFilter = tableFilter;
    }

    public BiPredicate<JsonNode, JsonNode> getRowFilter() {
        return rowFilter;
    }

    public void setRowFilter(Predicate<JsonNode> rowFilter) {
        this.rowFilter = new IgnoreFirstPredicate(rowFilter);
    }

    public void setRowFilter(BiPredicate<JsonNode, JsonNode> rowFilter) {
        this.rowFilter = rowFilter;
    }

    public BiConsumer<JsonNode, JsonNode> getConsumer() {
        return consumer;
    }

    public void setConsumer(BiConsumer<JsonNode, JsonNode> consumer) {
        this.consumer = consumer;
    }

    @Override
    public JsonNode call() throws Exception {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        try {
            List<TableScanner> scanners = getTableScanners();
            ArrayNode stats = JsonNodeFactory.instance.arrayNode();
            if (executor == null) {
                for (TableScanner scanner : scanners) {
                    stats.add(scanner.call());
                }
            } else {
                for (Future<JsonNode> future : executor.invokeAll(scanners)) {
                    stats.add(future.get());
                }
            }
            result.set("stats", stats);
        } catch (Exception e) {
            result.put("error", e.getMessage());
            throw e;
        }
        return result;
    }

    private List<TableScanner> getTableScanners() throws SQLException {
        List<TableScanner> result = new ArrayList<>();
        ArrayNode catalogs = rdbms.getCatalogs();
        if (catalogs.size() == 0) {
            catalogs = JsonNodeFactory.instance.arrayNode(1);
            catalogs.addNull();
        }
        for (JsonNode catalog : catalogs) {
            if (catalog.isNull() || catalogFilter.test(catalog)) {
                String c = catalog.path("TABLE_CAT").textValue();
                ArrayNode schemas = rdbms.getSchemas(c);
                if (schemas.size() == 0) {
                    schemas = JsonNodeFactory.instance.arrayNode(1);
                    schemas.addNull();
                }
                for (JsonNode schema : schemas) {
                    if (schema.isNull() || schemaFilter.test(schema)) {
                        String s = catalog.path("TABLE_SCHEM").textValue();
                        ArrayNode tables = rdbms.getTables(c, s);
                        for (JsonNode table : tables) {
                            if (tableFilter.test(table)) {
                                result.add(new TableScanner(rdbms, table, rowFilter, consumer));
                            }
                        }
                    }
                }
            }
        }
        return result;
    }

    public static class TableScanner implements Callable<JsonNode> {
        private final Rdbms rdbms;
        private final JsonNode table;
        private final BiPredicate<JsonNode, JsonNode> rowFilter;
        private final BiConsumer<JsonNode, JsonNode> consumer;

        public TableScanner(Rdbms rdbms,
                            JsonNode table,
                            BiPredicate<JsonNode, JsonNode> rowFilter,
                            BiConsumer<JsonNode, JsonNode> consumer) {
            this.rdbms = rdbms;
            this.table = table;
            this.rowFilter = rowFilter;
            this.consumer = consumer;
        }

        @Override
        public JsonNode call() throws Exception {
            try (Rows rows = rdbms.getRows(table)) {
                int count = 0;
                int accept = 0;
                for (ObjectNode row : rows) {
                    count++;
                    if (rowFilter.test(table, row)) {
                        accept++;
                        consumer.accept(table, row);
                    }
                }
                ObjectNode result = JsonNodeFactory.instance.objectNode();
                result.set("table", table);
                result.put("count", count);
                result.put("accept", accept);
                return result;
            }
        }
    }
}
