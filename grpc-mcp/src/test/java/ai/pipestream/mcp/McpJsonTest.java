package ai.pipestream.mcp;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class McpJsonTest {

    @Test
    void schemaAndArgCoercion() {
        Map<String, Object> schema = McpJson.objectSchema(
                Map.of("id", McpJson.stringProp("Page id"),
                        "limit", McpJson.intProp("Cap"),
                        "flag", McpJson.boolProp("On")),
                List.of("id"));
        assertThat(schema.get("type")).isEqualTo("object");
        assertThat(schema.get("required")).isEqualTo(List.of("id"));

        assertThat(McpJson.argString(null, "id", "fallback")).isEqualTo("fallback");
        assertThat(McpJson.argString(Map.of("id", "  "), "id", "fallback")).isEqualTo("fallback");
        assertThat(McpJson.argString(Map.of("id", 12), "id", "fallback")).isEqualTo("12");

        assertThat(McpJson.argBool(null, "flag", true)).isTrue();
        assertThat(McpJson.argBool(Map.of("flag", Boolean.TRUE), "flag", false)).isTrue();
        assertThat(McpJson.argBool(Map.of("flag", "true"), "flag", false)).isTrue();

        assertThat(McpJson.argInt(null, "limit", 7)).isEqualTo(7);
        assertThat(McpJson.argInt(Map.of("limit", 3L), "limit", 7)).isEqualTo(3);
        assertThat(McpJson.argInt(Map.of("limit", "9"), "limit", 7)).isEqualTo(9);
        assertThat(McpJson.argInt(Map.of("limit", "nope"), "limit", 7)).isEqualTo(7);
        assertThat(McpJson.write(Map.of("ok", true))).contains("\"ok\"");
    }
}
