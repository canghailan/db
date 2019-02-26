package cc.whohow.db;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;

public class RowsIterator implements Iterator<ObjectNode> {
    private final ResultSet resultSet;
    private final ResultSetMetaData resultSetMetaData;

    public RowsIterator(ResultSet resultSet) {
        try {
            this.resultSet = resultSet;
            this.resultSetMetaData = resultSet.getMetaData();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public ObjectNode next() {
        try {
            ObjectNode row = JsonNodeFactory.instance.objectNode();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                row.put(resultSetMetaData.getColumnLabel(i), resultSet.getString(i));
            }
            return row;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
