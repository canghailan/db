package cc.whohow.db.rdbms;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class RowFilter implements BiPredicate<JsonNode, JsonNode> {
    private final Predicate<JsonNode> predicate;

    public RowFilter(Predicate<JsonNode> predicate) {
        this.predicate = predicate;
    }

    @Override
    public boolean test(JsonNode table, JsonNode row) {
        return predicate.test(row);
    }
}