package ai.pipestream.mcp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class McpJson {

    static final ObjectMapper MAPPER = new ObjectMapper();

    private McpJson() {
    }

    static String write(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            return String.valueOf(value);
        }
    }

    static Map<String, Object> objectSchema(Map<String, Object> properties, List<String> required) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", properties);
        if (required != null && !required.isEmpty()) {
            schema.put("required", required);
        }
        return schema;
    }

    static Map<String, Object> stringProp(String description) {
        return Map.of("type", "string", "description", description);
    }

    static Map<String, Object> boolProp(String description) {
        return Map.of("type", "boolean", "description", description);
    }

    static Map<String, Object> intProp(String description) {
        return Map.of("type", "integer", "description", description);
    }

    static String argString(Map<String, Object> args, String key, String fallback) {
        Object value = args == null ? null : args.get(key);
        if (value == null) {
            return fallback;
        }
        String text = value.toString();
        return text.isBlank() ? fallback : text;
    }

    static boolean argBool(Map<String, Object> args, String key, boolean fallback) {
        Object value = args == null ? null : args.get(key);
        if (value instanceof Boolean b) {
            return b;
        }
        if (value == null) {
            return fallback;
        }
        return Boolean.parseBoolean(value.toString());
    }

    static int argInt(Map<String, Object> args, String key, int fallback) {
        Object value = args == null ? null : args.get(key);
        if (value instanceof Number n) {
            return n.intValue();
        }
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
