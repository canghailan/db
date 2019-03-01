package cc.whohow.db.rdbms.type;

import cc.whohow.db.rdbms.JdbcException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcBigIntType extends AbstractJdbcType<Long> {
    public JdbcBigIntType(int type, Class<?> typeClass) {
        super(type, typeClass);
    }

    public JdbcBigIntType(ResultSetMetaData resultSetMetaData, int column) throws SQLException, ClassNotFoundException {
        super(resultSetMetaData, column);
    }

    @Override
    public JsonNode toJSON(Long value) {
        return JsonNodeFactory.instance.numberNode(value);
    }

    @Override
    public Long fromJSON(JsonNode value) {
        if (value == null || value.isNull() || value.isMissingNode()) {
            return null;
        }
        return value.longValue();
    }

    @Override
    public Long get(ResultSet resultSet, int column) {
        try {
            long value = resultSet.getLong(column);
            if (resultSet.wasNull()) {
                return null;
            }
            return value;
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }
}
