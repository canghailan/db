package cc.whohow.db.rdbms.query;

import cc.whohow.db.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Rows implements Iterable<ObjectNode>, AutoCloseable {
    private final ResultSet resultSet;
    private final Runnable closeRunnable;

    public Rows(ResultSet resultSet) {
        this(resultSet, null);
    }

    public Rows(ResultSet resultSet, Runnable closeRunnable) {
        this.resultSet = resultSet;
        this.closeRunnable = closeRunnable;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public Runnable getCloseRunnable() {
        return closeRunnable;
    }

    @Override
    public Iterator<ObjectNode> iterator() {
        return new RowsIterator(resultSet);
    }

    public Stream<ObjectNode> stream() {
        return StreamSupport.stream(spliterator(), false).onClose(this::close);
    }

    public ArrayNode toJSON() {
        try {
            ArrayNode array = Json.newArray();
            for (JsonNode e : this) {
                array.add(e);
            }
            return array;
        } finally {
            close();
        }
    }

    @Override
    public void close() {
        if (closeRunnable != null) {
            closeRunnable.run();
        }
    }
}
