package cc.whohow.db;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
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

    @Override
    public Iterator<ObjectNode> iterator() {
        return new RowsIterator(resultSet);
    }

    public Stream<ObjectNode> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    public RowsParser parser() {
        return new RowsParser(resultSet, closeRunnable);
    }

    public ArrayNode toJSON() {
        try {
            ArrayNode array = JsonNodeFactory.instance.arrayNode();
            for (ObjectNode e : this) {
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
