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
    private final JsonNode configuration;
    private CloseRunnable closeRunnable = CloseRunnable.builder();

    public JdbcSynchronizeTask(JsonNode configuration) {
        this.configuration = configuration;
    }

    @Override
    public JsonNode call() throws Exception {
        Map<String, DataSource> dataSources = newDataSources();
        ObjectNode context = newContext();
        List<StatefulQuery> queryList = newQueryList();

        try {
            return new JdbcSynchronizer(dataSources, queryList, context).call();
        } finally {
            closeRunnable.run();
        }
    }

    private Map<String, DataSource> newDataSources() {
        JsonNode db = configuration.path("db");

        Map<String, DataSource> dataSources = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> iterator = db.fields();
        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> e = iterator.next();
            dataSources.put(e.getKey(), newDataSource(e.getValue()));
        }
        return dataSources;
    }

    private DataSource newDataSource(JsonNode db) {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setJdbcUrl(db.path("url").textValue());
        dataSource.setUsername(db.path("username").textValue());
        dataSource.setPassword(db.path("password").textValue());
        dataSource.setMaximumPoolSize(db.path("max").asInt(1));
        closeRunnable.compose(dataSource);
        return dataSource;
    }

    private List<StatefulQuery> newQueryList() {
        JsonNode query = configuration.path("query");
        return new ObjectMapper().convertValue(query, new TypeReference<List<StatefulQuery>>() {
        });
    }

    private ObjectNode newContext() {
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
