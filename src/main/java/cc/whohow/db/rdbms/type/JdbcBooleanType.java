package cc.whohow.db.rdbms.type;

import cc.whohow.db.rdbms.JdbcException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcBooleanType extends AbstractJdbcType<Boolean> {
    public JdbcBooleanType(int type, Class<?> typeClass) {
        super(type, typeClass);
    }

    public JdbcBooleanType(ResultSetMetaData resultSetMetaData, int column) throws SQLException, ClassNotFoundException {
        super(resultSetMetaData, column);
    }

    @Override
    public JsonNode toJSON(Boolean value) {
        if (value == null) {
            return JsonNodeFactory.instance.nullNode();
        }
        return JsonNodeFactory.instance.booleanNode(value);
    }

    @Override
    public Boolean fromJSON(JsonNode value) {
        if (value == null || value.isNull() || value.isMissingNode()) {
            return null;
        }
        return value.booleanValue();
    }

    @Override
    public Boolean get(ResultSet resultSet, int column) {
        try {
            boolean value = resultSet.getBoolean(column);
            if (resultSet.wasNull()) {
                return null;
            }
            return value;
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }
}
