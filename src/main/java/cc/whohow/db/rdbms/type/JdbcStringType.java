package cc.whohow.db.rdbms.type;

import cc.whohow.db.rdbms.JdbcException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcStringType extends AbstractJdbcType<String> {
    public JdbcStringType(int type, Class<?> typeClass) {
        super(type, typeClass);
    }

    public JdbcStringType(ResultSetMetaData resultSetMetaData, int column) throws SQLException, ClassNotFoundException {
        super(resultSetMetaData, column);
    }

    @Override
    public JsonNode toJSON(String value) {
        if (value == null) {
            return JsonNodeFactory.instance.nullNode();
        }
        return JsonNodeFactory.instance.textNode(value);
    }

    @Override
    public String fromJSON(JsonNode value) {
        if (value == null || value.isNull() || value.isMissingNode()) {
            return null;
        }
        return value.asText();
    }

    @Override
    public String get(ResultSet resultSet, int column) {
        try {
            return resultSet.getString(column);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }
}
