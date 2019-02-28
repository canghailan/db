package cc.whohow.db;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

public class SecondFilter implements BiPredicate<JsonNode, JsonNode> {
    private final Predicate<JsonNode> predicate;

    public SecondFilter(Predicate<JsonNode> predicate) {
        this.predicate = predicate;
    }

    @Override
    public boolean test(JsonNode first, JsonNode second) {
        return predicate.test(second);
    }
}