package cc.whohow.db.rdbms;

import cc.whohow.db.CloseRunnable;
import cc.whohow.db.rdbms.query.Rows;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Rdbms {
    private final DataSource dataSource;
    private final ObjectNode metadata = JsonNodeFactory.instance.objectNode();

    public Rdbms(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static String getQualifiedName(String... names) {
        return Stream.of(names)
                .filter(Objects::nonNull)
                .collect(Collectors.joining("."));
    }

    public String getType() throws SQLException {
        JsonNode type = metadata.path("type");
        if (type.isMissingNode()) {
            synchronized (this){
                try (Connection connection = dataSource.getConnection()) {
                    String driverName = connection.getMetaData().getDriverName().toLowerCase();
                    if (driverName.contains("mysql")) {
                        metadata.put("type", "mysql");
                    } else {
                        metadata.put("type", "");
                    }
                }
                type = metadata.path("type");
            }
        }
        return type.textValue();
    }

    public int getFetchSize() throws SQLException {
        JsonNode fetchSize = metadata.path("fetchSize");
        if (fetchSize.isMissingNode()) {
            synchronized (this){
                if ("mysql".equalsIgnoreCase(getType())) {
                    metadata.put("fetchSize", Integer.MIN_VALUE);
                } else {
                    metadata.put("fetchSize", 1000);
                }
                fetchSize = metadata.path("fetchSize");
            }
        }
        return fetchSize.intValue();
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public ArrayNode getCatalogs() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (ResultSet rs = connection.getMetaData().getCatalogs()) {
                return new Rows(rs).toJSON();
            }
        }
    }

    public ArrayNode getSchemas() throws SQLException {
        return getSchemas(null);
    }

    public ArrayNode getSchemas(String catalog) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (ResultSet rs = connection.getMetaData().getSchemas(catalog, "")) {
                return new Rows(rs).toJSON();
            }
        }
    }

    public ArrayNode getTables() throws SQLException {
        return getTables(null, null);
    }

    public ArrayNode getTables(String catalog) throws SQLException {
        return getTables(catalog, null);
    }

    public ArrayNode getTables(String catalog, String schema) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            try (ResultSet rs = connection.getMetaData()
                    .getTables(catalog, schema, "", new String[]{"TABLE"})) {
                return new Rows(rs).toJSON();
            }
        }
    }

    public Rows getRows(JsonNode table) throws SQLException {
        return getRows(getQualifiedName(
                table.path("TABLE_CAT").textValue(),
                table.path("TABLE_SCHEM").textValue(),
                table.path("TABLE_NAME").textValue()));
    }

    public Rows getRows(String table) throws SQLException {
        return query("SELECT * FROM " + table);
    }

    public Rows getRows(Connection connection, JsonNode table) throws SQLException {
        return getRows(connection, getQualifiedName(
                table.path("TABLE_CAT").textValue(),
                table.path("TABLE_SCHEM").textValue(),
                table.path("TABLE_NAME").textValue()));
    }

    public Rows getRows(Connection connection, String table) throws SQLException {
        return query(connection, "SELECT * FROM " + table);
    }

    public Rows query(String sql, Object... parameters) throws SQLException {
        CloseRunnable closeRunnable = CloseRunnable.builder();
        try {
            Connection connection = dataSource.getConnection();
            closeRunnable.compose(connection);

            PreparedStatement statement = connection.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            closeRunnable.compose(statement);
            statement.setFetchSize(getFetchSize());
            for (int i = 0; i < parameters.length; i++) {
                statement.setObject(i + 1, parameters[i]);
            }

            ResultSet resultSet = statement.executeQuery();
            closeRunnable.compose(resultSet);
            return new Rows(resultSet, closeRunnable);
        } catch (Throwable e) {
            closeRunnable.run();
            throw e;
        }
    }

    public Rows query(Connection connection, String sql, Object... parameters) throws SQLException {
        CloseRunnable closeRunnable = CloseRunnable.builder();
        try {
            PreparedStatement statement = connection.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            closeRunnable.compose(statement);
            statement.setFetchSize(getFetchSize());
            for (int i = 0; i < parameters.length; i++) {
                statement.setObject(i + 1, parameters[i]);
            }

            ResultSet resultSet = statement.executeQuery();
            closeRunnable.compose(resultSet);
            return new Rows(resultSet, closeRunnable);
        } catch (Throwable e) {
            closeRunnable.run();
            throw e;
        }
    }
}
