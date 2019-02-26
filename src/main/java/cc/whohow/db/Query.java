package cc.whohow.db;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Query {
    private final String sql;
    private final Object[] parameters;

    public Query(String sql, Object... parameters) {
        this.sql = sql;
        this.parameters = parameters;
    }

    public Rows executeQuery(DataSource dataSource) throws SQLException {
        CloseRunnable closeRunnable = CloseRunnable.empty();
        try {
            Connection connection = dataSource.getConnection();
            closeRunnable = closeRunnable.compose(connection);
            return executeQuery(connection, closeRunnable);
        } catch (SQLException e) {
            closeRunnable.run();
            throw e;
        }
    }

    public Rows executeQuery(Connection connection) throws SQLException {
        return executeQuery(connection, CloseRunnable.empty());
    }

    public Rows executeQuery(Connection connection, CloseRunnable closeRunnable) throws SQLException {
        try {
            PreparedStatement statement = connection.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            closeRunnable = closeRunnable.compose(statement);
            if (connection.getMetaData().getDriverName().toLowerCase().contains("mysql")) {
                statement.setFetchSize(Integer.MIN_VALUE);
            } else {
                statement.setFetchSize(1000);
            }
            for (int i = 0; i < parameters.length; i++) {
                statement.setObject(i + 1, parameters[i]);
            }

            ResultSet resultSet = statement.executeQuery();
            closeRunnable = closeRunnable.compose(resultSet);

            return new Rows(resultSet, closeRunnable);
        } catch (SQLException e) {
            closeRunnable.run();
            throw e;
        }
    }
}
