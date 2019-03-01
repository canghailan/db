package cc.whohow.db.rdbms.query;

import cc.whohow.db.rdbms.JdbcException;
import cc.whohow.db.rdbms.type.JdbcType;
import cc.whohow.db.rdbms.type.JdbcTypeFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;

public class RowsIterator implements Iterator<ObjectNode> {
    private final ResultSet resultSet;
    private final ResultSetMetaData resultSetMetaData;
    private final JdbcType[] columnTypes;

    public RowsIterator(ResultSet resultSet) {
        try {
            this.resultSet = resultSet;
            this.resultSetMetaData = resultSet.getMetaData();
            this.columnTypes = JdbcTypeFactory.getInstance().getJdbcTypes(resultSetMetaData);
        } catch (SQLException | ClassNotFoundException e) {
            throw new JdbcException(e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    @Override
    public ObjectNode next() {
        ObjectNode row = JsonNodeFactory.instance.objectNode();
        for (int i = 1; i < columnTypes.length; i++) {
            row.set(getName(i), getValue(i));
        }
        return row;
    }

    public int getRow() {
        try {
            return resultSet.getRow();
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public String getName(int index) {
        try {
            return resultSetMetaData.getColumnLabel(index);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    public JsonNode getValue(int index) {
        return columnTypes[index].getAsJSON(resultSet, index);
    }
}
