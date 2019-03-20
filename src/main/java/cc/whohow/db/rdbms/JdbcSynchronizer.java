package cc.whohow.db.rdbms;

import cc.whohow.db.CloseRunnable;
import cc.whohow.db.Json;
import cc.whohow.db.rdbms.query.NamedQuery;
import cc.whohow.db.rdbms.query.Rows;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class JdbcSynchronizer implements Callable<JsonNode> {
    private static final Logger log = LoggerFactory.getLogger(JdbcSynchronizer.class);

    private Map<String, Rdbms> dataSources;
    private Map<String, StatefulQuery> queries;
    private ObjectNode context;
    private CloseRunnable closeRunnable = CloseRunnable.builder();

    public JdbcSynchronizer(Map<String, DataSource> dataSources, List<StatefulQuery> queryList, ObjectNode context) {
        if (context == null || context.isNull() || context.isMissingNode()) {
            context = Json.newObject();
        }
        this.dataSources = new LinkedHashMap<>();
        for (Map.Entry<String, DataSource> e : dataSources.entrySet()) {
            this.dataSources.put(e.getKey(), new Rdbms(e.getValue()));
        }
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
        try {
            for (StatefulQuery query : queries.values()) {
                log.debug("context:\n{}", context);
                run(query);
            }
            log.debug("context:\n{}", context);
            return context;
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            log.debug("close");
            closeRunnable.run();
        }
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

        Rdbms dataSource = dataSources.get(query.getDataSource());
        NamedQuery namedQuery = new NamedQuery(query.getSql(), context);
        log.debug("run query: {}\n{}\n{}", query.getName(), namedQuery.getSQL(), namedQuery.getParameterNames());

        try (Connection connection = dataSource.getDataSource().getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(namedQuery.getSQL(),
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY)) {
                statement.setFetchSize(dataSource.getFetchSize());
                List<String> parameterNames = namedQuery.getParameterNames();
                for (int i = 0; i < parameterNames.size(); i++) {
                    statement.setString(i + 1, getAsString(parameterNames.get(i)));
                }
                try (ResultSet resultSet = statement.executeQuery()) {
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                    ArrayNode array = Json.newArray();
                    while (resultSet.next()) {
                        ObjectNode row = Json.newObject();
                        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                            row.put(resultSetMetaData.getColumnLabel(i), resultSet.getString(i));
                        }
                        array.add(row);
                    }
                    context.set(query.getName(), array);
                }
            }
        }
    }

    private boolean isUpdateWith(StatefulQuery query) {
        return !(query.getWith() == null || query.getWith().isEmpty());
    }

    private void runUpdate(StatefulQuery query) throws SQLException {
        if (isUpdateWith(query)) {
            runUpdateWith(query);
            return;
        }

        Rdbms dataSource = dataSources.get(query.getDataSource());
        NamedQuery namedQuery = new NamedQuery(query.getSql(), context);
        log.debug("run update: {}\n{}\n{}", query.getName(), namedQuery.getSQL(), namedQuery.getParameterNames());

        try (Connection connection = dataSource.getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(namedQuery.getSQL())) {
                List<String> parameterNames = namedQuery.getParameterNames();
                for (int i = 0; i < parameterNames.size(); i++) {
                    statement.setString(i + 1, getAsString(parameterNames.get(i)));
                }
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

        Rdbms dataSource = dataSources.get(query.getDataSource());
        NamedQuery namedQuery = new NamedQuery(query.getSql(), context);
        log.debug("run update: {}\n{}\n{}", query.getName(), namedQuery.getSQL(), namedQuery.getParameterNames());

        List<StatefulQuery> with = query.getWith().stream()
                .map(queries::get)
                .collect(Collectors.toList());

        List<String> contextParameters = namedQuery.getParameterNames().stream()
                .map(this::getAsString)
                .collect(Collectors.toList());
        List<String> parameters = new ArrayList<>(contextParameters.size());

        try (Connection connection = dataSource.getDataSource().getConnection()) {
            connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(namedQuery.getSQL())) {
                try (Rows rows = withQuery(with.get(0))) {
                    ResultSet resultSet = rows.getResultSet();
                    int[] parameterColumns = getParameterColumnIndexes(namedQuery, resultSet.getMetaData());

                    int count = 0;
                    while (resultSet.next()) {
                        parameters.clear();
                        for (int i = 0; i < parameterColumns.length; i++) {
                            int c = parameterColumns[i];
                            if (c > 0) {
                                parameters.add(resultSet.getString(c));
                            } else {
                                parameters.add(contextParameters.get(i));
                            }
                        }
                        for (int i = 0; i < parameters.size(); i++) {
                            statement.setString(i + 1, parameters.get(i));
                        }
                        log.debug("parameters: {}", parameters);

                        statement.addBatch();
                        count++;
                        if (count % 1000 == 0) {
                            log.debug("execute: {}", count);
                            statement.executeBatch();
                            connection.commit();
                        }
                    }
                    if (count % 1000 != 0) {
                        log.debug("execute: {}", count);
                        statement.executeBatch();
                        connection.commit();
                    }
                }
            } catch (Throwable e) {
                connection.rollback();
                throw e;
            }
        }
    }

    private Rows withQuery(StatefulQuery query) throws SQLException {
        Rdbms dataSource = dataSources.get(query.getDataSource());
        NamedQuery namedQuery = new NamedQuery(query.getSql(), context);
        log.debug("run query: {}\n{}\n{}", query.getName(), namedQuery.getSQL(), namedQuery.getParameterNames());

        CloseRunnable closeRunnable = CloseRunnable.builder();
        try {
            Connection connection = dataSource.getDataSource().getConnection();
            closeRunnable.compose(connection);

            PreparedStatement statement = connection.prepareStatement(namedQuery.getSQL(),
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            closeRunnable.compose(statement);
            statement.setFetchSize(dataSource.getFetchSize());
            List<String> parameterNames = namedQuery.getParameterNames();
            for (int i = 0; i < parameterNames.size(); i++) {
                statement.setString(i + 1, getAsString(parameterNames.get(i)));
            }

            ResultSet resultSet = statement.executeQuery();
            closeRunnable.compose(resultSet);

            return new Rows(resultSet, closeRunnable);
        } catch (Throwable e) {
            closeRunnable.run();
            throw e;
        }
    }

    private int[] getParameterColumnIndexes(NamedQuery query, ResultSetMetaData resultSetMetaData) throws SQLException {
        Map<String, Integer> index = new HashMap<>(resultSetMetaData.getColumnCount());
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            index.put(resultSetMetaData.getColumnLabel(i), i);
        }
        return query.getParameterNames().stream()
                .map(index::get)
                .mapToInt(i -> (i == null) ? -1 : i)
                .toArray();
    }

    private String getAsString(String expression) {
        return get(expression).asText(null);
    }

    private JsonNode get(String expression) {
        return Json.get(context, expression);
    }
}
