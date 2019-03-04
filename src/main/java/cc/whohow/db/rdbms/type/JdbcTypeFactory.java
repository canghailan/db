package cc.whohow.db.rdbms.type;

import cc.whohow.db.ISO_8601;
import com.fasterxml.jackson.databind.JsonNode;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;

public class JdbcTypeFactory {
    private static final JdbcTypeFactory INSTANCE = new JdbcTypeFactory();

    public static JdbcTypeFactory getInstance() {
        return INSTANCE;
    }

    public JdbcType[] getJdbcTypes(ResultSetMetaData resultSetMetaData) throws SQLException, ClassNotFoundException {
        JdbcType[] jdbcTypes = new JdbcType[resultSetMetaData.getColumnCount() + 1];
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            jdbcTypes[i] = JdbcTypeFactory.getInstance().getJdbcType(resultSetMetaData, i);
        }
        return jdbcTypes;
    }

    public JdbcType<?> getJdbcType(ResultSetMetaData resultSetMetaData, int column) throws SQLException, ClassNotFoundException {
        String type = resultSetMetaData.getColumnClassName(column);
        switch (type) {
            case "java.lang.String":
                return new JdbcStringType(resultSetMetaData, column);
            case "java.lang.Short":
            case "java.lang.Integer":
                return new JdbcIntType(resultSetMetaData, column);
            case "java.lang.Long":
                return new JdbcBigIntType(resultSetMetaData, column);
            case "java.math.BigDecimal":
                return new JdbcDecimalType(resultSetMetaData, column);
            case "java.lang.Boolean":
                return new JdbcBooleanType(resultSetMetaData, column);
            default: {
                Class<?> cls = Class.forName(type);
                if (Number.class.isAssignableFrom(cls)) {
                    return new JdbcDecimalType(resultSetMetaData, column);
                }
                if (Date.class.isAssignableFrom(cls)) {
                    return new JdbcDateType(resultSetMetaData, column);
                }
                return new JdbcStringType(resultSetMetaData, column);
            }
        }
    }

    public JdbcType<?> getJdbcType(Class<?> cls) {
        if (cls == String.class) {
            return new JdbcStringType(Types.VARCHAR, cls);
        }
        if (cls == Integer.class || cls == int.class) {
            return new JdbcIntType(Types.INTEGER, cls);
        }
        if (cls == Long.class || cls == long.class) {
            return new JdbcBigIntType(Types.BIGINT, cls);
        }
        if (Number.class.isAssignableFrom(cls)) {
            return new JdbcDecimalType(Types.DECIMAL, cls);
        }
        if (Date.class.isAssignableFrom(cls)) {
            return new JdbcDateType(Types.TIMESTAMP, cls);
        }
        if (cls == Boolean.class || cls == boolean.class) {
            return new JdbcBooleanType(Types.BOOLEAN, cls);
        }
        return new JdbcStringType(Types.VARCHAR, cls);
    }

    public Object fromJSON(JsonNode json) {
        if (json == null) {
            return null;
        }
        switch (json.getNodeType()) {
            case STRING: {
                String value = json.textValue();
                if (ISO_8601.is(value)) {
                    return Date.from(ISO_8601.parse(value).toInstant());
                }
                return value;
            }
            case NUMBER:
                return json.decimalValue();
            case NULL:
            case MISSING:
                return null;
            case BOOLEAN:
                return json.booleanValue();
            default:
                return json.asText();
        }
    }
}
