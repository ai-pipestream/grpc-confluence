package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.PropertyEntry;
import ai.pipestream.confluence.v1.PropertyValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.math.BigInteger;

import static ai.pipestream.confluence.ConfluenceRoutes.invalid;

/** Converts typed {@link PropertyValue} trees into the JSON a REST request carries. */
final class PropertyJson {

    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

    private PropertyJson() {
    }

    /**
     * The JSON form of a property value.
     *
     * @param value the typed value
     * @param field the request field, for error messages
     * @return the JSON value
     */
    static JsonNode toJson(PropertyValue value, String field) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> NODES.textNode(value.getStringValue());
            case INTEGER_VALUE -> NODES.numberNode(value.getIntegerValue());
            case BOOL_VALUE -> NODES.booleanNode(value.getBoolValue());
            case DOUBLE_VALUE -> {
                if (!Double.isFinite(value.getDoubleValue())) {
                    throw invalid(field + ": double_value must be finite");
                }
                yield NODES.numberNode(value.getDoubleValue());
            }
            case NULL_VALUE -> NODES.nullNode();
            case LIST_VALUE -> {
                ArrayNode array = NODES.arrayNode();
                for (PropertyValue item : value.getListValue().getValuesList()) {
                    array.add(toJson(item, field));
                }
                yield array;
            }
            case OBJECT_VALUE -> {
                ObjectNode object = NODES.objectNode();
                for (PropertyEntry entry : value.getObjectValue().getEntriesList()) {
                    if (object.has(entry.getKey())) {
                        throw invalid(field + ": duplicate object key " + entry.getKey());
                    }
                    object.set(entry.getKey(), toJson(entry.getValue(), field));
                }
                yield object;
            }
            case BIG_INTEGER_VALUE -> {
                try {
                    yield NODES.numberNode(new BigInteger(value.getBigIntegerValue()));
                } catch (NumberFormatException e) {
                    throw invalid(field + ": big_integer_value is not an integer");
                }
            }
            case DECIMAL_VALUE -> {
                try {
                    yield com.fasterxml.jackson.databind.node.DecimalNode.valueOf(
                            new BigDecimal(value.getDecimalValue()));
                } catch (NumberFormatException e) {
                    throw invalid(field + ": decimal_value is not a decimal number");
                }
            }
            case VALUE_NOT_SET -> throw invalid(field + ": a value is required");
        };
    }
}
