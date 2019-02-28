package cc.whohow.db.rdbms;

import cc.whohow.db.CloseRunnable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class SimpleQuery implements Query {
    private final String sql;
    private final Object[] parameters;

    public SimpleQuery(String sql, Object... parameters) {
        this.sql = sql;
        this.parameters = parameters;
    }

    @Override
    public Rows executeQuery(Connection connection, CloseRunnable closeRunnable) throws SQLException {
        closeRunnable = CloseRunnable.builder(closeRunnable);
        try {
            PreparedStatement statement = connection.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            closeRunnable.compose(statement);
            if (connection.getMetaData().getDriverName().toLowerCase().contains("mysql")) {
                statement.setFetchSize(Integer.MIN_VALUE);
            } else {
                statement.setFetchSize(1000);
            }
            for (int i = 0; i < parameters.length; i++) {
                statement.setObject(i + 1, parameters[i]);
            }

            ResultSet resultSet = statement.executeQuery();
            closeRunnable.compose(resultSet);

            return new Rows(resultSet, closeRunnable);
        } catch (SQLException e) {
            closeRunnable.run();
            throw e;
        }
    }

    @Override
    public String toString() {
        return sql + "\n" + Arrays.toString(parameters);
    }
}
