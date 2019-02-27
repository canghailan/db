package cc.whohow.db.rdbms;

import cc.whohow.db.CloseRunnable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public interface Query {
    default Rows executeQuery(DataSource dataSource) throws SQLException {
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

    default Rows executeQuery(Connection connection) throws SQLException {
        return executeQuery(connection, CloseRunnable.empty());
    }

    Rows executeQuery(Connection connection, CloseRunnable closeRunnable) throws SQLException;
}
