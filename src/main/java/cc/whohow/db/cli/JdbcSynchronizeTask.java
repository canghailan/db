package cc.whohow.db.cli;

import cc.whohow.db.CloseRunnable;
import cc.whohow.db.rdbms.JdbcSynchronizer;
import cc.whohow.db.rdbms.StatefulQuery;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JdbcSynchronizeTask implements Task {
    protected final JsonNode configuration;
    protected CloseRunnable closeRunnable = CloseRunnable.builder();

    public JdbcSynchronizeTask(JsonNode configuration) {
        this.configuration = configuration;
    }

    @Override
    public JsonNode call() throws Exception {
        return new JdbcSynchronizer(buildDataSources(), buildQueryList(), buildContext()).call();
    }

    protected Map<String, DataSource> buildDataSources() {
        JsonNode db = configuration.path("db");

        Map<String, DataSource> dataSources = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> iterator = db.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> e = iterator.next();
            dataSources.put(e.getKey(), buildDataSource(e.getValue()));
        }
        return dataSources;
    }

    protected DataSource buildDataSource(JsonNode db) {
        HikariDataSource dataSource = new DataSourceBuilder().apply(db);
        closeRunnable.compose(dataSource);
        return dataSource;
    }

    protected List<StatefulQuery> buildQueryList() {
        JsonNode query = configuration.path("query");
        return new ObjectMapper().convertValue(query, new TypeReference<List<StatefulQuery>>() {
        });
    }

    protected ObjectNode buildContext() {
        JsonNode context = configuration.path("context");
        if (context.isObject()) {
            return (ObjectNode) context;
        }
        if (context.isNull() || context.isMissingNode()) {
            return JsonNodeFactory.instance.objectNode();
        }
        throw new IllegalArgumentException();
    }

    @Override
    public void close() throws Exception {
        closeRunnable.run();
    }
}
