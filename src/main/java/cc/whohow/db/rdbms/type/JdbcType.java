package cc.whohow.db.rdbms.type;

import com.fasterxml.jackson.databind.JsonNode;

import java.sql.ResultSet;
import java.sql.SQLType;

public interface JdbcType<T> {
    int getType();

    SQLType getSQLType();

    Class<?> getTypeClass();

    JsonNode toJSON(T value);

    T fromJSON(JsonNode value);

    T get(ResultSet resultSet, int column);

    JsonNode getAsJSON(ResultSet resultSet, int column);
}
