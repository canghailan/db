package cc.whohow.db.rdbms;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.base.ParserMinimalBase;
import com.fasterxml.jackson.core.json.JsonReadContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class RowsParser extends ParserMinimalBase {
    // parser context
    protected ObjectCodec codec;
    protected JsonReadContext parsingContext;

    protected ResultSet resultSet;
    protected ResultSetMetaData resultSetMetaData;
    protected Runnable closeRunnable;

    protected JsonToken _nextToken;
    protected int column = 0;

    public RowsParser(ResultSet resultSet, Runnable closeRunnable) {
        try {
            this.resultSet = resultSet;
            this.resultSetMetaData = resultSet.getMetaData();
            this.closeRunnable = closeRunnable;
            this._nextToken = JsonToken.START_ARRAY;
            this.parsingContext = JsonReadContext
                    .createRootContext(0, 0, null);
        } catch (SQLException e) {
            closeRunnable.run();
            throw new JdbcException(e);
        }
    }

    @Override
    public JsonToken nextToken() throws IOException {
        _currToken = _nextToken;
        if (_currToken == null) {
            return null;
        }
        try {
            switch (_currToken) {
                case START_ARRAY: {
                    parsingContext = parsingContext.createChildArrayContext(resultSet.getRow(), 0);
                    if (resultSet.next()) {
                        _nextToken = JsonToken.START_OBJECT;
                    } else {
                        _nextToken = JsonToken.END_ARRAY;
                    }
                    break;
                }
                case START_OBJECT: {
                    parsingContext = parsingContext.createChildObjectContext(resultSet.getRow(), 0);
                    column++;
                    _nextToken = JsonToken.FIELD_NAME;
                    break;
                }
                case FIELD_NAME: {
                    parsingContext.setCurrentName(resultSetMetaData.getColumnLabel(column));
                    parsingContext.setCurrentValue(resultSet.getString(column));
                    if (parsingContext.getCurrentValue() == null) {
                        _nextToken = JsonToken.VALUE_NULL;
                    } else {
                        _nextToken = JsonToken.VALUE_STRING;
                    }
                    break;
                }
                case VALUE_STRING:
                case VALUE_NULL:
                case VALUE_TRUE:
                case VALUE_FALSE:
                case VALUE_NUMBER_INT:
                case VALUE_NUMBER_FLOAT: {
                    column++;
                    if (column > resultSetMetaData.getColumnCount()) {
                        _nextToken = JsonToken.END_OBJECT;
                    } else {
                        _nextToken = JsonToken.FIELD_NAME;
                    }
                    break;
                }
                case END_OBJECT: {
                    parsingContext = parsingContext.clearAndGetParent();
                    if (resultSet.next()) {
                        column = 0;
                        _nextToken = JsonToken.START_OBJECT;
                    } else {
                        _nextToken = JsonToken.END_ARRAY;
                    }
                    break;
                }
                case END_ARRAY: {
                    parsingContext = parsingContext.clearAndGetParent();
                    _nextToken = null;
                    break;
                }
                default: {
                    throw new IllegalStateException(_currToken.asString());
                }
            }
            return _currToken;
        } catch (SQLException e) {
            throw new JsonParseException(this, e.getMessage(), e);
        }
    }

    @Override
    protected void _handleEOF() throws JsonParseException {
    }

    @Override
    public String getCurrentName() throws IOException {
        return parsingContext.getCurrentName();
    }

    @Override
    public ObjectCodec getCodec() {
        return codec;
    }

    @Override
    public void setCodec(ObjectCodec codec) {
        this.codec = codec;
    }

    @Override
    public Version version() {
        return null;
    }

    @Override
    public void close() {
        if (closeRunnable != null) {
            closeRunnable.run();
        }
    }

    @Override
    public boolean isClosed() {
        try {
            return resultSet.isClosed();
        } catch (SQLException e) {
            throw new UncheckedIOException(new JsonParseException(this, e.getMessage(), e));
        }
    }

    @Override
    public JsonStreamContext getParsingContext() {
        return parsingContext;
    }

    @Override
    public JsonLocation getTokenLocation() {
        return getCurrentLocation();
    }

    @Override
    public JsonLocation getCurrentLocation() {
        try {
            return new JsonLocation(resultSet, -1, resultSet.getRow(), column);
        } catch (SQLException e) {
            return new JsonLocation(resultSet, -1, -1, -1);
        }
    }

    @Override
    public void overrideCurrentName(String name) {
        try {
            parsingContext.setCurrentName(name);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String getText() throws IOException {
        if (_currToken == null) {
            return null;
        }
        switch (_currToken) {
            case FIELD_NAME: {
                return parsingContext.getCurrentName();
            }
            case VALUE_STRING:
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT: {
                return (String) parsingContext.getCurrentValue();
            }
            default: {
                return _currToken.asString();
            }
        }
    }

    @Override
    public char[] getTextCharacters() throws IOException {
        return getText().toCharArray();
    }

    @Override
    public boolean hasTextCharacters() {
        return false;
    }

    @Override
    public Number getNumberValue() throws IOException {
        switch (getNumberType()) {
            case INT: {
                return getIntValue();
            }
            case LONG: {
                return getLongValue();
            }
            case FLOAT: {
                return getFloatValue();
            }
            case DOUBLE: {
                return getDoubleValue();
            }
            case BIG_INTEGER: {
                return getBigIntegerValue();
            }
            case BIG_DECIMAL: {
                return getDecimalValue();
            }
            default: {
                throw new AssertionError();
            }
        }
    }

    @Override
    public NumberType getNumberType() throws IOException {
        try {
            switch (resultSetMetaData.getColumnClassName(column)) {
                case "java.lang.Boolean":
                case "java.lang.Short":
                case "java.lang.Integer":
                    return NumberType.INT;
                case "java.lang.Long":
                    return NumberType.LONG;
                case "java.lang.Float":
                    return NumberType.FLOAT;
                case "java.lang.Double":
                    return NumberType.DOUBLE;
                case "java.math.BigInteger":
                    return NumberType.BIG_INTEGER;
                default:
                    return NumberType.BIG_DECIMAL;
            }
        } catch (SQLException e) {
            throw new JsonParseException(this, e.getMessage(), e);
        }
    }

    @Override
    public int getIntValue() throws IOException {
        try {
            return resultSet.getInt(column);
        } catch (SQLException e) {
            throw new JsonParseException(this, e.getMessage(), e);
        }
    }

    @Override
    public long getLongValue() throws IOException {
        try {
            return resultSet.getLong(column);
        } catch (SQLException e) {
            throw new JsonParseException(this, e.getMessage(), e);
        }
    }

    @Override
    public BigInteger getBigIntegerValue() throws IOException {
        try {
            return resultSet.getBigDecimal(column).toBigInteger();
        } catch (SQLException e) {
            throw new JsonParseException(this, e.getMessage(), e);
        }
    }

    @Override
    public float getFloatValue() throws IOException {
        try {
            return resultSet.getFloat(column);
        } catch (SQLException e) {
            throw new JsonParseException(this, e.getMessage(), e);
        }
    }

    @Override
    public double getDoubleValue() throws IOException {
        try {
            return resultSet.getDouble(column);
        } catch (SQLException e) {
            throw new JsonParseException(this, e.getMessage(), e);
        }
    }

    @Override
    public BigDecimal getDecimalValue() throws IOException {
        try {
            return resultSet.getBigDecimal(column);
        } catch (SQLException e) {
            throw new JsonParseException(this, e.getMessage(), e);
        }
    }

    @Override
    public int getTextLength() throws IOException {
        return getText().length();
    }

    @Override
    public int getTextOffset() throws IOException {
        return 0;
    }

    @Override
    public byte[] getBinaryValue(Base64Variant b64variant) throws IOException {
        try {
            return resultSet.getBytes(column);
        } catch (SQLException e) {
            throw new JsonParseException(this, e.getMessage(), e);
        }
    }
}
