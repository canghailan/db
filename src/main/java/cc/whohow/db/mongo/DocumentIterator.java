package cc.whohow.db.mongo;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.client.MongoCursor;
import org.bson.BsonDocument;

import java.util.Iterator;

public class DocumentIterator implements Iterator<JsonNode> {
    private final MongoCursor<BsonDocument> cursor;

    public DocumentIterator(MongoCursor<BsonDocument> cursor) {
        this.cursor = cursor;
    }

    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public JsonNode next() {
        return Bson.toJSON(cursor.next());
    }
}
