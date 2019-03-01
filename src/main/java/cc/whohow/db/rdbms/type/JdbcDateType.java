package cc.whohow.db.rdbms.type;

import cc.whohow.db.ISO_8601;
import cc.whohow.db.rdbms.JdbcException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZonedDateTime;

public class JdbcDateType extends AbstractJdbcType<Timestamp> {
    public JdbcDateType(int type, Class<?> typeClass) {
        super(type, typeClass);
    }

    public JdbcDateType(ResultSetMetaData resultSetMetaData, int column) throws SQLException, ClassNotFoundException {
        super(resultSetMetaData, column);
    }

    @Override
    public JsonNode toJSON(Timestamp value) {
        String text = ISO_8601.format(value);
        if (text == null) {
            return JsonNodeFactory.instance.nullNode();
        }
        return JsonNodeFactory.instance.textNode(text);
    }

    @Override
    public Timestamp fromJSON(JsonNode value) {
        if (value == null || value.isNull() || value.isMissingNode()) {
            return null;
        }
        if (value.isNumber()) {
            return Timestamp.from(Instant.ofEpochMilli(value.longValue()));
        }
        ZonedDateTime dateTime = ISO_8601.parse(value.textValue());
        if (dateTime == null) {
            return null;
        }
        return Timestamp.from(dateTime.toInstant());
    }

    @Override
    public Timestamp get(ResultSet resultSet, int column) {
        try {
            return resultSet.getTimestamp(column);
        } catch (SQLException e) {
            throw new JdbcException(e);
        }
    }
}
