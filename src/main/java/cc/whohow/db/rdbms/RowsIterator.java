package cc.whohow.db.rdbms;

import cc.whohow.db.DateTime;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Iterator;
import java.util.function.IntFunction;

@SuppressWarnings("unchecked")
public class RowsIterator implements Iterator<ObjectNode> {
    private final ResultSet resultSet;
    private final ResultSetMetaData resultSetMetaData;
    private final IntFunction<JsonNode>[] getters;

    public RowsIterator(ResultSet resultSet) {
        try {
            this.resultSet = resultSet;
            this.resultSetMetaData = resultSet.getMetaData();
            this.getters = new IntFunction[resultSetMetaData.getColumnCount() + 1];
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                getters[i] = getterOf(resultSetMetaData.getColumnClassName(i));
            }
        } catch (SQLException e) {
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
        for (int i = 1; i < getters.length; i++) {
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
        return getters[index].apply(index);
    }

    private IntFunction<JsonNode> getterOf(String type) {
        switch (type) {
            case "java.lang.String":
                return this::getString;
            case "java.lang.Short":
            case "java.lang.Integer":
                return this::getInteger;
            case "java.lang.Long":
                return this::getLong;
            case "java.math.BigDecimal":
                return this::getBigDecimal;
            case "java.lang.Boolean":
                return this::getBoolean;
            default: {
                try {
                    Class<?> cls = Class.forName(type);
                    if (Number.class.isAssignableFrom(cls)) {
                        return this::getBigDecimal;
                    }
                    if (Date.class.isAssignableFrom(cls)) {
                        return this::getDate;
                    }
                    return this::getString;
                } catch (ClassNotFoundException e) {
                    throw new JdbcException(e);
                }
            }
        }
    }

    private JsonNode getString(int index) {
        try {
            String value = resultSet.getString(index);
            if (resultSet.wasNull()) {
                return JsonNodeFactory.instance.nullNode();
            }
            return JsonNodeFactory.instance.textNode(value);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    private JsonNode getBoolean(int index) {
        try {
            boolean value = resultSet.getBoolean(index);
            if (resultSet.wasNull()) {
                return JsonNodeFactory.instance.nullNode();
            }
            return JsonNodeFactory.instance.booleanNode(value);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    private JsonNode getInteger(int index) {
        try {
            int value = resultSet.getInt(index);
            if (resultSet.wasNull()) {
                return JsonNodeFactory.instance.nullNode();
            }
            return JsonNodeFactory.instance.numberNode(value);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    private JsonNode getLong(int index) {
        try {
            long value = resultSet.getLong(index);
            if (resultSet.wasNull()) {
                return JsonNodeFactory.instance.nullNode();
            }
            return JsonNodeFactory.instance.numberNode(value);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    private JsonNode getBigDecimal(int index) {
        try {
            BigDecimal value = resultSet.getBigDecimal(index);
            if (resultSet.wasNull()) {
                return JsonNodeFactory.instance.nullNode();
            }
            return JsonNodeFactory.instance.numberNode(value);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }

    private JsonNode getDate(int index) {
        try {
            java.sql.Timestamp value = resultSet.getTimestamp(index);
            if (resultSet.wasNull()) {
                return JsonNodeFactory.instance.nullNode();
            }
            return JsonNodeFactory.instance.textNode(DateTime.iso8601().format(
                    value.toLocalDateTime().atZone(ZoneOffset.systemDefault())));
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }
}
