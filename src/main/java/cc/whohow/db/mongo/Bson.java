package cc.whohow.db.mongo;

import cc.whohow.db.ISO_8601;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.bson.BsonDbPointer;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonValue;

import java.util.Base64;
import java.util.Map;

public class Bson {
    public static JsonNode toJSON(BsonValue bson) {
        switch (bson.getBsonType()) {
            case DOUBLE:
                return JsonNodeFactory.instance.nullNode();
            case STRING:
                return JsonNodeFactory.instance.textNode(
                        bson.asString().getValue());
            case DOCUMENT: {
                ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
                for (Map.Entry<String, BsonValue> e : bson.asDocument().entrySet()) {
                    objectNode.set(e.getKey(), toJSON(e.getValue()));
                }
                return objectNode;
            }
            case ARRAY: {
                ArrayNode arrayNode = JsonNodeFactory.instance.arrayNode();
                for (BsonValue e : bson.asArray()) {
                    arrayNode.add(toJSON(e));
                }
                return arrayNode;
            }
            case BINARY:
                return JsonNodeFactory.instance.textNode(
                        Base64.getEncoder().encodeToString(bson.asBinary().getData()));
            case UNDEFINED:
                return JsonNodeFactory.instance.nullNode();
            case OBJECT_ID:
                return JsonNodeFactory.instance.textNode(
                        bson.asObjectId().getValue().toHexString());
            case BOOLEAN:
                return JsonNodeFactory.instance.booleanNode(
                        bson.asBoolean().getValue());
            case DATE_TIME:
                return JsonNodeFactory.instance.textNode(ISO_8601.format(bson.asDateTime().getValue()));
            case NULL:
                return JsonNodeFactory.instance.nullNode();
            case REGULAR_EXPRESSION:
                return JsonNodeFactory.instance.textNode(
                        bson.asRegularExpression().getPattern());
            case DB_POINTER: {
                BsonDbPointer dbPointer = bson.asDBPointer();
                return JsonNodeFactory.instance.textNode(
                        dbPointer.getNamespace() + ":" + dbPointer.getId().toHexString());
            }
            case JAVASCRIPT:
                return JsonNodeFactory.instance.textNode(
                        bson.asJavaScript().getCode());
            case SYMBOL:
                return JsonNodeFactory.instance.textNode(
                        bson.asSymbol().getSymbol());
            case JAVASCRIPT_WITH_SCOPE: {
                BsonJavaScriptWithScope javaScriptWithScope = bson.asJavaScriptWithScope();
                return JsonNodeFactory.instance.textNode(
                        toJSON(javaScriptWithScope.getScope()).toString() + "\n" + javaScriptWithScope.getCode());
            }
            case INT32:
                return JsonNodeFactory.instance.numberNode(bson.asInt32().intValue());
            case TIMESTAMP:
                return JsonNodeFactory.instance.textNode(ISO_8601.format(bson.asTimestamp().getValue()));
            case INT64:
                return JsonNodeFactory.instance.numberNode(bson.asInt64().getValue());
            case DECIMAL128:
                return JsonNodeFactory.instance.numberNode(bson.asDecimal128().getValue().bigDecimalValue());
            default:
                throw new AssertionError();
        }
    }
}
