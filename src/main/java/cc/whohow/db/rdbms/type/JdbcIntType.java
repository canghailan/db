package cc.whohow.db.rdbms.type;

import cc.whohow.db.rdbms.JdbcException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcIntType extends AbstractJdbcType<Integer> {
    public JdbcIntType(int type, Class<?> typeClass) {
        super(type, typeClass);
    }

    public JdbcIntType(ResultSetMetaData resultSetMetaData, int column) throws SQLException, ClassNotFoundException {
        super(resultSetMetaData, column);
    }

    @Override
    public JsonNode toJSON(Integer value) {
        return JsonNodeFactory.instance.numberNode(value);
    }

    @Override
    public Integer fromJSON(JsonNode value) {
        if (value == null || value.isNull() || value.isMissingNode()) {
            return null;
        }
        return value.intValue();
    }

    @Override
    public Integer get(ResultSet resultSet, int column) {
        try {
            int value = resultSet.getInt(column);
            if (resultSet.wasNull()) {
                return null;
            }
            return value;
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }
}
