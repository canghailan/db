package cc.whohow.db.cli;

import cc.whohow.db.CloseRunnable;
import cc.whohow.db.Database;
import cc.whohow.db.DatabaseScanner;
import cc.whohow.db.Predicates;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DatabaseScanTask implements Task {
    private final JsonNode configuration;
    private CloseRunnable closeRunnable = CloseRunnable.empty();
    private ExecutorService executor;
    private Writer output;

    public DatabaseScanTask(JsonNode configuration) {
        this.configuration = configuration;
    }

    @Override
    public JsonNode call() throws Exception {
        output = newOutput();
        executor = newExecutor();

        List<DatabaseScanner> scanners = newDatabaseScanner();
        BiPredicate<JsonNode, JsonNode> rowFilter = newRowFilter();
        for (DatabaseScanner scanner : scanners) {
            scanner.setRowFilter(rowFilter);
            scanner.setConsumer(this::write);
        }

        List<Future<JsonNode>> futures = executor.invokeAll(scanners);

        ArrayNode stats = JsonNodeFactory.instance.arrayNode();
        for (Future<JsonNode> future : futures) {
            stats.add(future.get());
        }

        output.flush();

        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.set("stats", stats);
        return result;
    }

    private Writer newOutput() throws FileNotFoundException {
        String output = configuration.path("output").asText("output.txt");
        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output), StandardCharsets.UTF_8));
        closeRunnable.compose(writer);
        return writer;
    }

    private ExecutorService newExecutor() {
        int worker = configuration.path("worker").asInt(1);
        return Executors.newFixedThreadPool(worker);
    }

    private List<DatabaseScanner> newDatabaseScanner() {
        JsonNode db = configuration.path("db");
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(db.fields(), 0), false)
                .map(Map.Entry::getValue)
                .map(this::newDatabaseScanner)
                .collect(Collectors.toList());
    }

    private DatabaseScanner newDatabaseScanner(JsonNode db) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(db.path("url").textValue());
        dataSource.setUsername(db.path("username").textValue());
        dataSource.setPassword(db.path("password").textValue());
        dataSource.setMaximumPoolSize(db.path("max").asInt(1));
        closeRunnable = closeRunnable.andThen(dataSource);

        DatabaseScanner databaseScanner = new DatabaseScanner(new Database(dataSource), executor);

        String catalog = db.path("catalog").textValue();
        if (catalog != null) {
            databaseScanner.setCatalogFilter(Predicates.include("TABLE_CAT", catalog));
        }
        String schema = db.path("schema").textValue();
        if (schema != null) {
            databaseScanner.setSchemaFilter(Predicates.include("TABLE_SCHEM", schema));
        }
        String table = db.path("table").textValue();
        if (table != null) {
            databaseScanner.setTableFilter(Predicates.include("TABLE_NAME", table));
        }
        return databaseScanner;
    }

    private BiPredicate<JsonNode, JsonNode> newRowFilter() {
        JsonNode rowFilter = configuration.path("rowFilter");
        String key = rowFilter.path("key").textValue();
        switch (rowFilter.path("type").asText("")) {
            case "pattern":
                return Predicates.row(Predicates.pattern(key, rowFilter.path("pattern").asText()));
            case "include":
                return Predicates.row(Predicates.include(key, rowFilter.path("include").asText()));
            case "exclude":
                return Predicates.row(Predicates.include(key, rowFilter.path("exclude").asText()));
            default:
                throw new IllegalArgumentException();
        }
    }

    private void write(JsonNode table, JsonNode row) {
        String tableName = Database.getQualifiedName(
                table.path("TABLE_CAT").textValue(),
                table.path("TABLE_SCHEM").textValue(),
                table.path("TABLE_NAME").textValue());
        try {
            output.write(tableName + "\t" + row + "\n");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (executor != null) {
                executor.shutdown();
                executor.awaitTermination(1, TimeUnit.MINUTES);
                executor = null;
            }
        } finally {
            closeRunnable.run();
        }
    }
}
