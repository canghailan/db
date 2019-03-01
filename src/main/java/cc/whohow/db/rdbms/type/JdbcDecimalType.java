package cc.whohow.db.rdbms.type;

import cc.whohow.db.rdbms.JdbcException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcDecimalType extends AbstractJdbcType<BigDecimal> {
    public JdbcDecimalType(int type, Class<?> typeClass) {
        super(type, typeClass);
    }

    public JdbcDecimalType(ResultSetMetaData resultSetMetaData, int column) throws SQLException, ClassNotFoundException {
        super(resultSetMetaData, column);
    }

    @Override
    public JsonNode toJSON(BigDecimal value) {
        return JsonNodeFactory.instance.numberNode(value);
    }

    @Override
    public BigDecimal fromJSON(JsonNode value) {
        if (value == null || value.isNull() || value.isMissingNode()) {
            return null;
        }
        return value.decimalValue();
    }

    @Override
    public BigDecimal get(ResultSet resultSet, int column) {
        try {
            return resultSet.getBigDecimal(column);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }
}
