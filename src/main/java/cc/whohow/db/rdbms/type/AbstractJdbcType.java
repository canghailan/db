package cc.whohow.db.rdbms.type;

import com.fasterxml.jackson.databind.JsonNode;

import java.sql.*;

public abstract class AbstractJdbcType<T> implements JdbcType<T> {
    protected final int type;
    protected final Class<?> typeClass;

    public AbstractJdbcType(int type, Class<?> typeClass) {
        this.type = type;
        this.typeClass = typeClass;
    }

    public AbstractJdbcType(ResultSetMetaData resultSetMetaData, int column) throws SQLException, ClassNotFoundException {
        this(resultSetMetaData.getColumnType(column),
                Class.forName(resultSetMetaData.getColumnClassName(column)));
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public SQLType getSQLType() {
        return JDBCType.valueOf(type);
    }

    @Override
    public Class<?> getTypeClass() {
        return typeClass;
    }

    @Override
    public JsonNode getAsJSON(ResultSet resultSet, int column) {
        return toJSON(get(resultSet, column));
    }
}
