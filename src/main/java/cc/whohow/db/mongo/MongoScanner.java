package cc.whohow.db.mongo;

import cc.whohow.db.IgnoreFirstPredicate;
import cc.whohow.db.Predicates;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.MongoClient;
import org.bson.BsonDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class MongoScanner implements Callable<JsonNode> {
    private final MongoClient mongo;
    private ExecutorService executor;
    private Predicate<JsonNode> databaseFilter = Predicates::all;
    private Predicate<JsonNode> collectionFilter = Predicates::all;
    private BiPredicate<JsonNode, JsonNode> documentFilter = Predicates::all;
    private BiConsumer<JsonNode, JsonNode> consumer = MongoScanner::ignore;

    public MongoScanner(MongoClient mongo) {
        this.mongo = mongo;
    }

    private static <T1, T2> void ignore(T1 a, T2 b) {
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    public Predicate<JsonNode> getDatabaseFilter() {
        return databaseFilter;
    }

    public void setDatabaseFilter(Predicate<JsonNode> databaseFilter) {
        this.databaseFilter = databaseFilter;
    }

    public Predicate<JsonNode> getCollectionFilter() {
        return collectionFilter;
    }

    public void setCollectionFilter(Predicate<JsonNode> collectionFilter) {
        this.collectionFilter = collectionFilter;
    }

    public BiPredicate<JsonNode, JsonNode> getDocumentFilter() {
        return documentFilter;
    }

    public void setDocumentFilter(Predicate<JsonNode> documentFilter) {
        this.documentFilter = new IgnoreFirstPredicate(documentFilter);
    }

    public void setDocumentFilter(BiPredicate<JsonNode, JsonNode> documentFilter) {
        this.documentFilter = documentFilter;
    }

    public BiConsumer<JsonNode, JsonNode> getConsumer() {
        return consumer;
    }

    public void setConsumer(BiConsumer<JsonNode, JsonNode> consumer) {
        this.consumer = consumer;
    }

    @Override
    public JsonNode call() throws Exception {
        ObjectNode result = JsonNodeFactory.instance.objectNode();
        try {
            List<CollectionScanner> scanners = getCollectionScanners();
            ArrayNode stats = JsonNodeFactory.instance.arrayNode();
            if (executor == null) {
                for (CollectionScanner scanner : scanners) {
                    stats.add(scanner.call());
                }
            } else {
                for (Future<JsonNode> future : executor.invokeAll(scanners)) {
                    stats.add(future.get());
                }
            }
            result.set("stats", stats);
        } catch (Exception e) {
            result.put("error", e.getMessage());
            throw e;
        }
        return result;
    }

    private Documents listDatabases() {
        return new Documents(mongo.listDatabases(BsonDocument.class).batchSize(1000));
    }

    private Documents listCollections(String databaseName) {
        return new Documents(mongo.getDatabase(databaseName).listCollections(BsonDocument.class).batchSize(1000));
    }

    private List<CollectionScanner> getCollectionScanners() {
        List<CollectionScanner> result = new ArrayList<>();
        try (Documents databases = listDatabases()) {
            for (JsonNode database : databases) {
                if (databaseFilter.test(database)) {
                    try (Documents collections = listCollections(database.path("name").textValue())) {
                        for (JsonNode collection : collections) {
                            if (collectionFilter.test(collection)) {
                                result.add(new CollectionScanner(mongo, database, collection, documentFilter, consumer));
                            }
                        }
                    }
                }
            }
            return result;
        }
    }

    public static class CollectionScanner implements Callable<JsonNode> {
        private final MongoClient mongo;
        private final JsonNode database;
        private final JsonNode collection;
        private final BiPredicate<JsonNode, JsonNode> documentFilter;
        private final BiConsumer<JsonNode, JsonNode> consumer;

        public CollectionScanner(MongoClient mongo,
                                 JsonNode database,
                                 JsonNode collection,
                                 BiPredicate<JsonNode, JsonNode> documentFilter,
                                 BiConsumer<JsonNode, JsonNode> consumer) {
            this.mongo = mongo;
            this.database = database;
            this.collection = collection;
            this.documentFilter = documentFilter;
            this.consumer = consumer;
        }

        @Override
        public JsonNode call() throws Exception {
            try (Documents documents = find()) {
                int count = 0;
                int accept = 0;
                for (JsonNode document : documents) {
                    count++;
                    if (documentFilter.test(collection, document)) {
                        accept++;
                        consumer.accept(collection, document);
                    }
                }
                ObjectNode result = JsonNodeFactory.instance.objectNode();
                result.set("collection", collection);
                result.put("count", count);
                result.put("accept", accept);
                return result;
            }
        }

        private Documents find() {
            return new Documents(mongo
                    .getDatabase(database.path("name").textValue())
                    .getCollection(collection.path("name").textValue())
                    .find(BsonDocument.class)
                    .batchSize(1000));
        }
    }
}
