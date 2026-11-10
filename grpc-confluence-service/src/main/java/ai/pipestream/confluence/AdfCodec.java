package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.AdfDocument;
import ai.pipestream.confluence.v1.AdfMark;
import ai.pipestream.confluence.v1.AdfMarkType;
import ai.pipestream.confluence.v1.AdfNode;
import ai.pipestream.confluence.v1.AdfNodeType;
import ai.pipestream.confluence.v1.PropertyValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import com.google.protobuf.Message;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Converts between the typed {@link AdfDocument} and Atlassian Document
 * Format JSON.
 */
final class AdfCodec {

    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;
    /** Enums whose JSON values are lowerCamelCase. */
    private static final Set<String> CAMEL_ENUMS =
            Set.of("AdfNodeType", "AdfMarkType", "AdfAnnotationType");
    /** Enums whose JSON values are upper case. */
    private static final Set<String> UPPER_ENUMS = Set.of("AdfTaskState", "AdfMentionUserType");
    private static final Map<String, EnumNames> ENUMS = new ConcurrentHashMap<>();
    private static final OneofDescriptor NODE_ATTRS = AdfNode.getDescriptor().getOneofs().get(0);
    private static final OneofDescriptor MARK_ATTRS = AdfMark.getDescriptor().getOneofs().get(0);

    private AdfCodec() {
    }

    /** The JSON names of one enum's values, both ways. */
    private record EnumNames(Map<Integer, String> byNumber, Map<String, EnumValueDescriptor> byName) {
    }

    /** The document cannot be converted. */
    static final class AdfException extends IllegalArgumentException {
        @java.io.Serial
        private static final long serialVersionUID = 1L;

        AdfException(String message) {
            super(message);
        }
    }

    // ------------------------------------------------------------ to JSON --

    /**
     * The ADF JSON of a document.
     *
     * @param document the typed document
     * @return the {@code doc} node
     * @throws AdfException when the document is not valid ADF
     */
    static ObjectNode toJson(AdfDocument document) {
        ObjectNode doc = NODES.objectNode();
        doc.put("version", document.getVersion() == 0 ? 1 : document.getVersion());
        doc.put("type", "doc");
        ArrayNode content = doc.putArray("content");
        for (AdfNode node : document.getContentList()) {
            content.add(toJson(node));
        }
        return doc;
    }

    private static ObjectNode toJson(AdfNode node) {
        ObjectNode json = NODES.objectNode();
        json.put("type", enumName(AdfNodeType.getDescriptor(), node.getTypeValue(), "node type"));
        if (node.getType() == AdfNodeType.ADF_NODE_TYPE_TEXT) {
            json.put("text", node.getText());
        } else if (!node.getText().isEmpty()) {
            throw new AdfException("only a text node carries text, not " + json.get("type").asText());
        }
        FieldDescriptor attrs = node.getOneofFieldDescriptor(NODE_ATTRS);
        if (attrs != null) {
            requireArm(attrs, AdfNodeType.getDescriptor().findValueByNumber(node.getTypeValue()));
            json.set("attrs", attrsToJson((Message) node.getField(attrs)));
        }
        if (node.getContentCount() > 0) {
            ArrayNode content = json.putArray("content");
            for (AdfNode child : node.getContentList()) {
                content.add(toJson(child));
            }
        }
        if (node.getMarksCount() > 0) {
            ArrayNode marks = json.putArray("marks");
            for (AdfMark mark : node.getMarksList()) {
                marks.add(toJson(mark));
            }
        }
        return json;
    }

    private static ObjectNode toJson(AdfMark mark) {
        ObjectNode json = NODES.objectNode();
        json.put("type", enumName(AdfMarkType.getDescriptor(), mark.getTypeValue(), "mark type"));
        FieldDescriptor attrs = mark.getOneofFieldDescriptor(MARK_ATTRS);
        if (attrs != null) {
            requireArm(attrs, AdfMarkType.getDescriptor().findValueByNumber(mark.getTypeValue()));
            json.set("attrs", attrsToJson((Message) mark.getField(attrs)));
        }
        return json;
    }

    private static void requireArm(FieldDescriptor arm, EnumValueDescriptor type) {
        if (!arm.getName().equals(suffix(type).toLowerCase(Locale.ROOT))) {
            throw new AdfException("attrs " + arm.getName() + " do not belong to " + type.getName());
        }
    }

    private static ObjectNode attrsToJson(Message attrs) {
        ObjectNode json = NODES.objectNode();
        for (FieldDescriptor field : attrs.getDescriptorForType().getFields()) {
            if (field.isRepeated()) {
                int count = attrs.getRepeatedFieldCount(field);
                if (count > 0) {
                    ArrayNode array = json.putArray(field.getJsonName());
                    for (int i = 0; i < count; i++) {
                        array.add(valueToJson(field, attrs.getRepeatedField(field, i)));
                    }
                }
            } else if (!field.hasPresence() || attrs.hasField(field)) {
                json.set(field.getJsonName(), valueToJson(field, attrs.getField(field)));
            }
        }
        return json;
    }

    private static JsonNode valueToJson(FieldDescriptor field, Object value) {
        return switch (field.getJavaType()) {
            case STRING -> NODES.textNode((String) value);
            case BOOLEAN -> NODES.booleanNode((Boolean) value);
            case INT -> NODES.numberNode((Integer) value);
            case DOUBLE -> {
                double number = (Double) value;
                if (!Double.isFinite(number)) {
                    throw new AdfException(field.getName() + " must be finite");
                }
                yield number == Math.rint(number) && Math.abs(number) < 1e15
                        ? NODES.numberNode((long) number) : NODES.numberNode(number);
            }
            case ENUM -> NODES.textNode(enumName(field.getEnumType(),
                    ((EnumValueDescriptor) value).getNumber(), field.getName()));
            case MESSAGE -> value instanceof PropertyValue property
                    ? PropertyJson.toJson(property, field.getName())
                    : attrsToJson((Message) value);
            default -> throw new AdfException("unsupported attribute type of " + field.getName());
        };
    }

    // ---------------------------------------------------------- from JSON --

    /**
     * The typed form of an ADF JSON document.
     *
     * @param doc the {@code doc} node
     * @return the typed document
     * @throws AdfException when the JSON has a node, mark or attribute the model lacks
     */
    static AdfDocument fromJson(JsonNode doc) {
        if (!doc.isObject() || !"doc".equals(doc.path("type").asText())) {
            throw new AdfException("an ADF document is an object of type doc");
        }
        AdfDocument.Builder document = AdfDocument.newBuilder()
                .setVersion(doc.path("version").asInt(1));
        for (JsonNode child : doc.path("content")) {
            document.addContent(nodeFromJson(child));
        }
        return document.build();
    }

    private static AdfNode nodeFromJson(JsonNode json) {
        AdfNode.Builder node = AdfNode.newBuilder();
        EnumValueDescriptor type = enumValue(AdfNodeType.getDescriptor(), json.path("type").asText());
        node.setTypeValue(type.getNumber());
        for (Map.Entry<String, JsonNode> member : json.properties()) {
            JsonNode value = member.getValue();
            switch (member.getKey()) {
                case "type" -> {
                }
                case "text" -> node.setText(value.asText());
                case "content" -> value.forEach(child -> node.addContent(nodeFromJson(child)));
                case "marks" -> value.forEach(mark -> node.addMarks(markFromJson(mark)));
                case "attrs" -> setAttrs(node, type, value);
                default -> throw new AdfException("unknown member " + member.getKey()
                        + " on node " + type.getName());
            }
        }
        return node.build();
    }

    private static AdfMark markFromJson(JsonNode json) {
        AdfMark.Builder mark = AdfMark.newBuilder();
        EnumValueDescriptor type = enumValue(AdfMarkType.getDescriptor(), json.path("type").asText());
        mark.setTypeValue(type.getNumber());
        for (Map.Entry<String, JsonNode> member : json.properties()) {
            switch (member.getKey()) {
                case "type" -> {
                }
                case "attrs" -> setAttrs(mark, type, member.getValue());
                default -> throw new AdfException("unknown member " + member.getKey()
                        + " on mark " + type.getName());
            }
        }
        return mark.build();
    }

    private static void setAttrs(Message.Builder owner, EnumValueDescriptor type, JsonNode attrs) {
        FieldDescriptor arm = owner.getDescriptorForType()
                .findFieldByName(suffix(type).toLowerCase(Locale.ROOT));
        if (arm == null || arm.getContainingOneof() == null) {
            throw new AdfException(type.getName() + " takes no attrs");
        }
        owner.setField(arm, attrsFromJson(arm.getMessageType(), owner.newBuilderForField(arm), attrs));
    }

    private static Message attrsFromJson(Descriptor type, Message.Builder builder, JsonNode json) {
        if (!json.isObject()) {
            throw new AdfException(type.getName() + " must be a JSON object");
        }
        Map<String, FieldDescriptor> fields = new HashMap<>();
        type.getFields().forEach(field -> fields.put(field.getJsonName(), field));
        for (Map.Entry<String, JsonNode> member : json.properties()) {
            FieldDescriptor field = fields.get(member.getKey());
            if (field == null) {
                throw new AdfException("unknown attribute " + member.getKey() + " of " + type.getName());
            }
            JsonNode value = member.getValue();
            if (field.isRepeated()) {
                if (!value.isArray()) {
                    throw new AdfException(member.getKey() + " must be an array");
                }
                for (JsonNode item : value) {
                    builder.addRepeatedField(field, valueFromJson(field, builder, item));
                }
            } else if (!value.isNull() || isPropertyValue(field)) {
                builder.setField(field, valueFromJson(field, builder, value));
            }
        }
        return builder.build();
    }

    private static boolean isPropertyValue(FieldDescriptor field) {
        return field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                && field.getMessageType() == PropertyValue.getDescriptor();
    }

    private static Object valueFromJson(FieldDescriptor field, Message.Builder owner, JsonNode value) {
        String name = field.getName();
        switch (field.getJavaType()) {
            case STRING:
                if (value.isTextual()) {
                    return value.textValue();
                }
                break;
            case BOOLEAN:
                if (value.isBoolean()) {
                    return value.booleanValue();
                }
                break;
            case INT:
                if (value.isNumber() && value.doubleValue() == Math.rint(value.doubleValue())
                        && value.canConvertToInt()) {
                    return value.intValue();
                }
                break;
            case DOUBLE:
                if (value.isNumber()) {
                    return value.doubleValue();
                }
                break;
            case ENUM:
                if (value.isTextual()) {
                    return enumValue(field.getEnumType(), value.textValue());
                }
                break;
            case MESSAGE:
                if (isPropertyValue(field)) {
                    return ConfluenceMapper.toPropertyValue(value);
                }
                return attrsFromJson(field.getMessageType(), owner.newBuilderForField(field), value);
            default:
                break;
        }
        throw new AdfException("attribute " + name + " has the wrong JSON type");
    }

    // -------------------------------------------------------------- enums --

    private static String enumName(EnumDescriptor type, int number, String field) {
        String name = names(type).byNumber().get(number);
        if (name == null) {
            throw new AdfException(field + " is required");
        }
        return name;
    }

    private static EnumValueDescriptor enumValue(EnumDescriptor type, String name) {
        EnumValueDescriptor value = names(type).byName().get(name);
        if (value == null) {
            throw new AdfException("unknown " + type.getName() + " value " + name);
        }
        return value;
    }

    private static EnumNames names(EnumDescriptor type) {
        return ENUMS.computeIfAbsent(type.getFullName(), key -> {
            Map<Integer, String> byNumber = new HashMap<>();
            Map<String, EnumValueDescriptor> byName = new HashMap<>();
            for (EnumValueDescriptor value : type.getValues()) {
                if (value.getNumber() != 0) {
                    String name = jsonName(type, suffix(value));
                    byNumber.put(value.getNumber(), name);
                    byName.put(name, value);
                }
            }
            return new EnumNames(byNumber, byName);
        });
    }

    /** The enum value name without its type prefix, such as {@code BLOCK_CARD}. */
    private static String suffix(EnumValueDescriptor value) {
        StringBuilder prefix = new StringBuilder();
        String type = value.getType().getName();
        for (int i = 0; i < type.length(); i++) {
            char c = type.charAt(i);
            if (i > 0 && Character.isUpperCase(c)) {
                prefix.append('_');
            }
            prefix.append(Character.toUpperCase(c));
        }
        prefix.append('_');
        String name = value.getName();
        return name.startsWith(prefix.toString()) ? name.substring(prefix.length()) : name;
    }

    private static String jsonName(EnumDescriptor type, String suffix) {
        if (UPPER_ENUMS.contains(type.getName())) {
            return suffix;
        }
        String lower = suffix.toLowerCase(Locale.ROOT);
        if (!CAMEL_ENUMS.contains(type.getName())) {
            return lower.replace('_', '-');
        }
        StringBuilder camel = new StringBuilder();
        boolean upper = false;
        for (int i = 0; i < lower.length(); i++) {
            char c = lower.charAt(i);
            if (c == '_') {
                upper = true;
            } else {
                camel.append(upper ? Character.toUpperCase(c) : c);
                upper = false;
            }
        }
        return camel.toString();
    }
}
