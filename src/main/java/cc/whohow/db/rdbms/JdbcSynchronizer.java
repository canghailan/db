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
import java.util.stream.Collectors;

public class JdbcSynchronizer implements Callable<JsonNode> {
    private Map<String, DataSource> dataSources;
    private Map<String, StatefulQuery> queries;
    private ObjectNode context;
    private CloseRunnable closeRunnable = CloseRunnable.builder();

    public JdbcSynchronizer(Map<String, DataSource> dataSources, List<StatefulQuery> queryList, ObjectNode context) {
        if (context == null) {
            context = JsonNodeFactory.instance.objectNode();
        }
        this.dataSources = dataSources;
        this.queries = new LinkedHashMap<>();
        for (StatefulQuery query : queryList) {
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
            for (StatefulQuery query : queries.values()) {
                run(query);
                System.out.println(context);
            }
        } catch (Exception e) {
            result.put("error", e.getMessage());
            throw e;
        } finally {
            closeRunnable.run();
        }
        return result;
    }

    private void run(StatefulQuery query) throws SQLException {
        if (query.isUpdate()) {
            runUpdate(query);
        } else {
            runQuery(query);
        }
    }

    private void runQuery(StatefulQuery query) throws SQLException {
        if (query.isStreaming()) {
            return;
        }
        DataSource dataSource = dataSources.get(query.getDataSource());
        NamedQuery namedQuery = new NamedQuery(query.getSql(), context);
        context.set(query.getName(), namedQuery.executeQuery(dataSource).toJSON());
    }

    private void runUpdate(StatefulQuery query) throws SQLException {
        if (query.getWith() != null && !query.getWith().isEmpty()) {
            runUpdateWith(query);
            return;
        }

        DataSource dataSource = dataSources.get(query.getDataSource());
        NamedQuery namedQuery = new NamedQuery(query.getSql(), context);
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(namedQuery.getSQL())) {
                namedQuery.setParameters(statement);
                statement.executeUpdate();
                connection.commit();
            } catch (Throwable e) {
                connection.rollback();
                throw e;
            }
        }
    }

    private void runUpdateWith(StatefulQuery query) throws SQLException {
        if (query.getWith().size() > 1) {
            throw new UnsupportedOperationException("TODO");
        }
        DataSource dataSource = dataSources.get(query.getDataSource());
        NamedQuery namedQuery = new NamedQuery(query.getSql(), context);

        List<StatefulQuery> queryList = query.getWith().stream()
                .map(queries::get)
                .collect(Collectors.toList());
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(namedQuery.getSQL())) {
                ResultSet resultSet = withQuery(queryList.get(0));
                ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                int[] indexMap = indexMapForQuery(namedQuery, resultSetMetaData);

                int count = 0;
                while (resultSet.next()) {
                    namedQuery.setParameters(statement);
                    for (int param = 0; param < indexMap.length; param++) {
                        int column = indexMap[param];
                        if (column < 0) {
                            continue;
                        }
                        statement.setObject(param + 1,
                                resultSet.getObject(column + 1), resultSetMetaData.getColumnType(column + 1));
                    }
                    statement.addBatch();
                    count++;
                    if (count % 1000 == 0) {
                        statement.executeBatch();
                    }
                }
                if (count % 1000 != 0) {
                    statement.executeBatch();
                }

                connection.commit();
            } catch (Throwable e) {
                connection.rollback();
                throw e;
            }
        }
    }

    private ResultSet withQuery(StatefulQuery query) throws SQLException {
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

    private int[] indexMapForQuery(NamedQuery query, ResultSetMetaData resultSetMetaData) throws SQLException {
        List<String> names = new ArrayList<>(resultSetMetaData.getColumnCount());
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            names.add(resultSetMetaData.getColumnLabel(i));
        }
        return query.getParameterNames().stream()
                .mapToInt(names::indexOf)
                .toArray();
    }
}
