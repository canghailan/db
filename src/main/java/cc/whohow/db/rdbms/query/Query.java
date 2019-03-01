package cc.whohow.db.rdbms.query;

import cc.whohow.db.CloseRunnable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface Query {
    String getSQL();

    Object[] getParameters();

    void setParameters(PreparedStatement statement) throws SQLException;

    PreparedStatement prepareQuery(Connection connection) throws SQLException;

    default Rows executeQuery(DataSource dataSource) throws SQLException {
        Connection connection = dataSource.getConnection();
        return executeQuery(connection, CloseRunnable.of(connection));
    }

    default Rows executeQuery(Connection connection) throws SQLException {
        return executeQuery(connection, CloseRunnable.empty());
    }

    Rows executeQuery(Connection connection, CloseRunnable closeRunnable) throws SQLException;
}
