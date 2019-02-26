package cc.whohow.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Database {
    private final DataSource dataSource;

    public Database(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static String getQualifiedName(String... names) {
        return Stream.of(names)
                .filter(Objects::nonNull)
                .collect(Collectors.joining("."));
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
        return getRowsQuery(table).executeQuery(dataSource);
    }

    public Rows getRows(String table) throws SQLException {
        return getRowsQuery(table).executeQuery(dataSource);
    }

    public Rows getRows(Connection connection, String table) throws SQLException {
        return getRowsQuery(table).executeQuery(connection);
    }

    public Rows getRows(Connection connection, JsonNode table) throws SQLException {
        return getRowsQuery(table).executeQuery(connection);
    }

    public Rows query(String sql, Object... parameters) throws SQLException {
        return new Query(sql, parameters).executeQuery(dataSource);
    }

    public Rows query(Connection connection, String sql, Object... parameters) throws SQLException {
        return new Query(sql, parameters).executeQuery(connection);
    }

    public Query getRowsQuery(JsonNode table) {
        return getRowsQuery(getQualifiedName(
                table.path("TABLE_CAT").textValue(),
                table.path("TABLE_SCHEM").textValue(),
                table.path("TABLE_NAME").textValue()));
    }

    public Query getRowsQuery(String table) {
        return new Query("SELECT * FROM " + table);
    }
}
