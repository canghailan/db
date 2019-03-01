package cc.whohow.db.rdbms;

import cc.whohow.db.CloseRunnable;
import cc.whohow.db.rdbms.query.NamedQuery;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;

public class JdbcSynchronizer implements Callable<JsonNode> {
    private Map<String, DataSource> dataSources;
    private Map<String, JdbcSynchronizeQuery> queries;
    private ObjectNode context;
    private CloseRunnable closeRunnable = CloseRunnable.builder();

    public JdbcSynchronizer(Map<String, DataSource> dataSources, List<JdbcSynchronizeQuery> queryList, ObjectNode context) {
        if (context == null) {
            context = JsonNodeFactory.instance.objectNode();
        }
        this.dataSources = dataSources;
        this.queries = new LinkedHashMap<>();
        for (JdbcSynchronizeQuery query : queryList) {
            if (query.getName() == null) {
                query.setName(UUID.randomUUID().toString());
            }
            if (queries.putIfAbsent(query.getName(), query) != null) {
                throw new IllegalArgumentException();
            }
        }
        this.context = context;
    }

    @Override
    public JsonNode call() throws Exception {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        try {
           for (JdbcSynchronizeQuery query : queries.values()) {
               run(query);
           }
        } catch (Exception e) {
            e.printStackTrace();
            result.put("error", e.getMessage());
        }
        return result;
    }

    private void run(JdbcSynchronizeQuery query) throws SQLException {
        if (query.isUpdate()) {
            runUpdate(query);
        } else {
            runQuery(query);
        }
    }

    private void runQuery(JdbcSynchronizeQuery query) throws SQLException {
        if (query.isStreaming()) {
            return;
        }
        DataSource dataSource = dataSources.get(query.getDataSource());
        NamedQuery namedQuery = new NamedQuery(query.getSql(), context);
        context.set(query.getName(), namedQuery.executeQuery(dataSource).toJSON());
    }

    private void runUpdate(JdbcSynchronizeQuery query) throws SQLException {
        if (query.getWith() != null && !query.getWith().isEmpty()) {
            runUpdateWith(query);
            return;
        }

        DataSource dataSource = dataSources.get(query.getDataSource());
        NamedQuery namedQuery = new NamedQuery(query.getSql(), context);
        try (Connection connection = dataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(namedQuery.getSQL())) {
                namedQuery.setParameters(statement);
                statement.executeUpdate();
                connection.commit();
            } catch (Throwable e) {
                connection.rollback();
            }
        }
    }

    private void runUpdateWith(JdbcSynchronizeQuery query) {
        List<ResultSet> resultSets = new ArrayList<>(query.getWith().size());
        for (ResultSet resultSet : resultSets) {

        }
    }

    private ResultSet withQuery(JdbcSynchronizeQuery query) throws SQLException {
        DataSource dataSource = dataSources.get(query.getDataSource());
        NamedQuery namedQuery = new NamedQuery(query.getSql(), context);
        Connection connection = dataSource.getConnection();
        closeRunnable.compose(connection);
        PreparedStatement statement = namedQuery.prepareQuery(connection);
        closeRunnable.compose(statement);
        ResultSet resultSet = statement.executeQuery();
        closeRunnable.compose(resultSet);
        return resultSet;
    }
}
