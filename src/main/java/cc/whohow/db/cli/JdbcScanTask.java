package cc.whohow.db.cli;

import cc.whohow.db.CloseRunnable;
import cc.whohow.db.ExecutorCloser;
import cc.whohow.db.rdbms.Rdbms;
import cc.whohow.db.rdbms.JdbcScanner;
import cc.whohow.db.Predicates;
import cc.whohow.db.rdbms.RowFilter;
import cc.whohow.db.rdbms.RowWriter;
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
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class JdbcScanTask implements Task {
    private final JsonNode configuration;
    private CloseRunnable closeRunnable = CloseRunnable.empty();

    public JdbcScanTask(JsonNode configuration) {
        this.configuration = configuration;
    }

    @Override
    public JsonNode call() throws Exception {
        List<JdbcScanner> scanners = newScanners();
        ExecutorService executor = newExecutor();
        BiPredicate<JsonNode, JsonNode> rowFilter = newRowFilter();
        BiConsumer<JsonNode, JsonNode> consumer = newConsumer();
        for (JdbcScanner scanner : scanners) {
            scanner.setExecutor(executor);
            scanner.setRowFilter(rowFilter);
            scanner.setConsumer(consumer);
        }

        List<Future<JsonNode>> futures = executor.invokeAll(scanners);

        ArrayNode stats = JsonNodeFactory.instance.arrayNode();
        for (Future<JsonNode> future : futures) {
            stats.add(future.get());
        }

        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.set("stats", stats);
        return result;
    }

    private RowWriter newConsumer() throws FileNotFoundException {
        String output = configuration.path("output").asText("output.txt");
        RowWriter rowWriter = new RowWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
                output), StandardCharsets.UTF_8)));
        closeRunnable = closeRunnable.andThen(rowWriter);
        return rowWriter;
    }

    private List<JdbcScanner> newScanners() {
        JsonNode db = configuration.path("db");
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(db.fields(), 0), false)
                .map(Map.Entry::getValue)
                .map(this::newScanner)
                .collect(Collectors.toList());
    }

    private ExecutorService newExecutor() {
        int worker = configuration.path("worker").asInt(1);
        ExecutorService executor = Executors.newFixedThreadPool(worker);
        closeRunnable = closeRunnable.andThen(new ExecutorCloser(executor));
        return executor;
    }

    private JdbcScanner newScanner(JsonNode db) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(db.path("url").textValue());
        dataSource.setUsername(db.path("username").textValue());
        dataSource.setPassword(db.path("password").textValue());
        dataSource.setMaximumPoolSize(db.path("max").asInt(1));
        closeRunnable = closeRunnable.compose(dataSource);

        JdbcScanner jdbcScanner = new JdbcScanner(new Rdbms(dataSource));

        String catalog = db.path("catalog").textValue();
        if (catalog != null) {
            jdbcScanner.setCatalogFilter(Predicates.include("TABLE_CAT", catalog));
        }
        String schema = db.path("schema").textValue();
        if (schema != null) {
            jdbcScanner.setSchemaFilter(Predicates.include("TABLE_SCHEM", schema));
        }
        String table = db.path("table").textValue();
        if (table != null) {
            jdbcScanner.setTableFilter(Predicates.include("TABLE_NAME", table));
        }
        return jdbcScanner;
    }

    private BiPredicate<JsonNode, JsonNode> newRowFilter() {
        JsonNode rowFilter = configuration.path("rowFilter");
        String key = rowFilter.path("key").textValue();
        switch (rowFilter.path("type").asText("")) {
            case "pattern":
                return new RowFilter(Predicates.pattern(key, rowFilter.path("pattern").asText()));
            case "include":
                return new RowFilter(Predicates.include(key, rowFilter.path("include").asText()));
            case "exclude":
                return new RowFilter(Predicates.include(key, rowFilter.path("exclude").asText()));
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public void close() throws Exception {
        closeRunnable.run();
    }
}
