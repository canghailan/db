package cc.whohow.db.rdbms;

import cc.whohow.db.CloseRunnable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public interface Query {
    default Rows executeQuery(DataSource dataSource) throws SQLException {
        Connection connection = dataSource.getConnection();
        return executeQuery(connection, CloseRunnable.of(connection));
    }

    default Rows executeQuery(Connection connection) throws SQLException {
        return executeQuery(connection, CloseRunnable.empty());
    }

    Rows executeQuery(Connection connection, CloseRunnable closeRunnable) throws SQLException;
}
