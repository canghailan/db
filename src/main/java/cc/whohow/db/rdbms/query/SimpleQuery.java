package cc.whohow.db.rdbms.query;

import cc.whohow.db.CloseRunnable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class SimpleQuery implements Query {
    protected String sql;
    protected Object[] parameters;

    protected SimpleQuery() {
    }

    public SimpleQuery(String sql, Object... parameters) {
        this.sql = sql;
        this.parameters = parameters;
    }

    @Override
    public String getSQL() {
        return sql;
    }

    @Override
    public Object[] getParameters() {
        return parameters;
    }

    @Override
    public void setParameters(PreparedStatement statement) throws SQLException {
        for (int i = 0; i < parameters.length; i++) {
            statement.setObject(i + 1, parameters[i]);
        }
    }

    @Override
    public PreparedStatement prepareQuery(Connection connection) throws SQLException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(sql,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            if (connection.getMetaData().getDriverName().toLowerCase().contains("mysql")) {
                statement.setFetchSize(Integer.MIN_VALUE);
            } else {
                statement.setFetchSize(1000);
            }
            setParameters(statement);
            return statement;
        } catch (Throwable e) {
            if (statement != null) {
                statement.close();
            }
            throw e;
        }
    }

    @Override
    public Rows executeQuery(Connection connection, CloseRunnable closeRunnable) throws SQLException {
        closeRunnable = CloseRunnable.builder(closeRunnable);
        try {
            PreparedStatement statement = prepareQuery(connection);
            closeRunnable.compose(statement);

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
