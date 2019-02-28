package cc.whohow.db.mongo;

import cc.whohow.db.CloseRunnable;
import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import org.bson.BsonDocument;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Documents implements Iterable<JsonNode>, AutoCloseable {
    private final MongoCursor<BsonDocument> cursor;
    private final Runnable closeRunnable;

    public Documents(MongoIterable<BsonDocument> iterable) {
        this.cursor = iterable.iterator();
        this.closeRunnable = CloseRunnable.of(cursor);
    }

    public Documents(MongoCursor<BsonDocument> cursor, Runnable closeRunnable) {
        this.cursor = cursor;
        this.closeRunnable = closeRunnable;
    }

    @Override
    public Iterator<JsonNode> iterator() {
        return new DocumentIterator(cursor);
    }

    public Stream<JsonNode> stream() {
        return StreamSupport.stream(spliterator(), false).onClose(this::close);
    }

    @Override
    public void close() {
        if (closeRunnable != null) {
            closeRunnable.run();
        }
    }
}
