package cc.whohow.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Json {
    public static ArrayNode newArray() {
        return JsonNodeFactory.instance.arrayNode();
    }

    public static ObjectNode newObject() {
        return JsonNodeFactory.instance.objectNode();
    }

    public static JsonNode get(JsonNode json, String expression) {
        if (expression.startsWith("/")) {
            return json.at(expression);
        }
        return json.path(expression);
    }
}
