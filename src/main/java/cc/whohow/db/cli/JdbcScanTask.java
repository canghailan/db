package cc.whohow.db.cli;

import cc.whohow.db.CloseRunnable;
import cc.whohow.db.ExecutorCloser;
import cc.whohow.db.IgnoreFirstPredicate;
import cc.whohow.db.Predicates;
import cc.whohow.db.rdbms.JdbcScanner;
import cc.whohow.db.rdbms.Rdbms;
import cc.whohow.db.rdbms.query.RowWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
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
    protected final JsonNode configuration;
    protected CloseRunnable closeRunnable = CloseRunnable.builder();

    public JdbcScanTask(JsonNode configuration) {
        this.configuration = configuration;
    }

    @Override
    public JsonNode call() throws Exception {
        List<JdbcScanner> scanners = buildScanners();
        ExecutorService workerExecutor = buildWorkerExecutor();
        BiPredicate<JsonNode, JsonNode> rowFilter = buildRowFilter();
        BiConsumer<JsonNode, JsonNode> consumer = buildConsumer();
        for (JdbcScanner scanner : scanners) {
            scanner.setExecutor(workerExecutor);
            scanner.setRowFilter(rowFilter);
            scanner.setConsumer(consumer);
        }

        ExecutorService executor = Executors.newFixedThreadPool(scanners.size());
        closeRunnable.andThen(new ExecutorCloser(executor));
        List<Future<JsonNode>> futures = executor.invokeAll(scanners);

        ArrayNode stats = JsonNodeFactory.instance.arrayNode();
        for (Future<JsonNode> future : futures) {
            stats.add(future.get());
        }

        ObjectNode result = JsonNodeFactory.instance.objectNode();
        result.set("stats", stats);
        return result;
    }

    protected List<JdbcScanner> buildScanners() {
        JsonNode db = configuration.path("db");
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(db.fields(), 0), false)
                .map(Map.Entry::getValue)
                .map(this::buildScanner)
                .collect(Collectors.toList());
    }

    protected JdbcScanner buildScanner(JsonNode db) {
        JdbcScanner jdbcScanner = new JdbcScanner(new Rdbms(buildDataSource(db)));

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

    protected DataSource buildDataSource(JsonNode db) {
        HikariDataSource dataSource = new DataSourceBuilder().apply(db);
        closeRunnable.compose(dataSource);
        return dataSource;
    }

    protected ExecutorService buildWorkerExecutor() {
        int worker = configuration.path("worker").asInt(1);
        ExecutorService executor = Executors.newFixedThreadPool(worker);
        closeRunnable.andThen(new ExecutorCloser(executor));
        return executor;
    }

    protected BiPredicate<JsonNode, JsonNode> buildRowFilter() {
        JsonNode rowFilter = configuration.path("rowFilter");
        String key = rowFilter.path("key").textValue();
        switch (rowFilter.path("type").asText("")) {
            case "pattern":
                return new IgnoreFirstPredicate(Predicates.pattern(key, rowFilter.path("pattern").asText()));
            case "include":
                return new IgnoreFirstPredicate(Predicates.include(key, rowFilter.path("include").asText()));
            case "exclude":
                return new IgnoreFirstPredicate(Predicates.include(key, rowFilter.path("exclude").asText()));
            default:
                throw new IllegalArgumentException();
        }
    }

    protected BiConsumer<JsonNode, JsonNode> buildConsumer() throws FileNotFoundException {
        RowWriter rowWriter = new RowWriter(new BufferedWriter(new OutputStreamWriter(buildOutput(), StandardCharsets.UTF_8)));
        closeRunnable.andThen(rowWriter);
        return rowWriter;
    }

    protected OutputStream buildOutput() throws FileNotFoundException {
        String output = configuration.path("output").asText("output.txt");
        return new FileOutputStream(output);
    }

    @Override
    public void close() throws Exception {
        closeRunnable.run();
    }
}
