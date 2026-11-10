package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.AdfAlignment;
import ai.pipestream.confluence.v1.AdfAlignmentAttrs;
import ai.pipestream.confluence.v1.AdfDocument;
import ai.pipestream.confluence.v1.AdfHeadingAttrs;
import ai.pipestream.confluence.v1.AdfLinkAttrs;
import ai.pipestream.confluence.v1.AdfLocalIdAttrs;
import ai.pipestream.confluence.v1.AdfMark;
import ai.pipestream.confluence.v1.AdfMarkType;
import ai.pipestream.confluence.v1.AdfNode;
import ai.pipestream.confluence.v1.AdfNodeType;
import ai.pipestream.confluence.v1.AdfPanelAttrs;
import ai.pipestream.confluence.v1.AdfPanelType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The ADF codec: JSON to the typed model and back without loss, and the
 * typed model against the vendored ADF JSON schema.
 */
class AdfCodecTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path SCHEMA = Path.of("../docs/upstream/atlassian/adf-schema-full.json");

    private static JsonNode sample() throws IOException {
        try (InputStream in = AdfCodecTest.class.getResourceAsStream("/adf/every-node-and-mark.json")) {
            return JSON.readTree(in);
        }
    }

    @Test
    void jsonRoundTripsThroughTheTypedModel() throws Exception {
        JsonNode original = sample();
        AdfDocument typed = AdfCodec.fromJson(original);
        JsonNode back = JSON.readTree(AdfCodec.toJson(typed).toString());
        assertThat(back).isEqualTo(original);
        assertThat(AdfCodec.fromJson(back)).isEqualTo(typed);
    }

    @Test
    void theSampleUsesEveryNodeAndMarkType() throws Exception {
        Set<AdfNodeType> nodes = EnumSet.noneOf(AdfNodeType.class);
        Set<AdfMarkType> marks = EnumSet.noneOf(AdfMarkType.class);
        List<AdfNode> pending = new ArrayList<>(AdfCodec.fromJson(sample()).getContentList());
        while (!pending.isEmpty()) {
            AdfNode node = pending.removeLast();
            nodes.add(node.getType());
            node.getMarksList().forEach(mark -> marks.add(mark.getType()));
            pending.addAll(node.getContentList());
        }
        Set<AdfNodeType> allNodes = EnumSet.allOf(AdfNodeType.class);
        allNodes.removeAll(Set.of(AdfNodeType.ADF_NODE_TYPE_UNSPECIFIED, AdfNodeType.UNRECOGNIZED));
        Set<AdfMarkType> allMarks = EnumSet.allOf(AdfMarkType.class);
        allMarks.removeAll(Set.of(AdfMarkType.ADF_MARK_TYPE_UNSPECIFIED, AdfMarkType.UNRECOGNIZED));
        assertThat(nodes).isEqualTo(allNodes);
        assertThat(marks).isEqualTo(allMarks);
    }

    @Test
    void typedDocumentBecomesAdfJson() throws Exception {
        AdfDocument document = AdfDocument.newBuilder()
                .addContent(AdfNode.newBuilder()
                        .setType(AdfNodeType.ADF_NODE_TYPE_HEADING)
                        .setHeading(AdfHeadingAttrs.newBuilder().setLevel(1))
                        .addMarks(AdfMark.newBuilder().setType(AdfMarkType.ADF_MARK_TYPE_ALIGNMENT)
                                .setAlignment(AdfAlignmentAttrs.newBuilder()
                                        .setAlign(AdfAlignment.ADF_ALIGNMENT_END)))
                        .addContent(AdfNode.newBuilder().setType(AdfNodeType.ADF_NODE_TYPE_TEXT)
                                .setText("Release notes")))
                .addContent(AdfNode.newBuilder()
                        .setType(AdfNodeType.ADF_NODE_TYPE_PANEL)
                        .setPanel(AdfPanelAttrs.newBuilder().setPanelType(AdfPanelType.ADF_PANEL_TYPE_INFO))
                        .addContent(AdfNode.newBuilder().setType(AdfNodeType.ADF_NODE_TYPE_PARAGRAPH)
                                .addContent(AdfNode.newBuilder()
                                        .setType(AdfNodeType.ADF_NODE_TYPE_TEXT).setText("see ")
                                        .addMarks(AdfMark.newBuilder()
                                                .setType(AdfMarkType.ADF_MARK_TYPE_LINK)
                                                .setLink(AdfLinkAttrs.newBuilder()
                                                        .setHref("https://example.com"))))))
                .build();

        assertThat(JSON.readTree(AdfCodec.toJson(document).toString())).isEqualTo(JSON.readTree("""
                {"version":1,"type":"doc","content":[
                  {"type":"heading","attrs":{"level":1},
                   "marks":[{"type":"alignment","attrs":{"align":"end"}}],
                   "content":[{"type":"text","text":"Release notes"}]},
                  {"type":"panel","attrs":{"panelType":"info"},"content":[
                    {"type":"paragraph","content":[
                      {"type":"text","text":"see ",
                       "marks":[{"type":"link","attrs":{"href":"https://example.com"}}]}]}]}]}
                """));
    }

    @Test
    void attrsOfAnotherNodeTypeAreRefused() {
        AdfDocument document = AdfDocument.newBuilder().addContent(AdfNode.newBuilder()
                .setType(AdfNodeType.ADF_NODE_TYPE_PARAGRAPH)
                .setHeading(AdfHeadingAttrs.newBuilder().setLevel(1))).build();
        assertThatThrownBy(() -> AdfCodec.toJson(document))
                .isInstanceOf(AdfCodec.AdfException.class).hasMessageContaining("heading");
    }

    @Test
    void missingTypesAndRequiredEnumsAreRefused() {
        assertThatThrownBy(() -> AdfCodec.toJson(AdfDocument.newBuilder()
                .addContent(AdfNode.getDefaultInstance()).build()))
                .isInstanceOf(AdfCodec.AdfException.class).hasMessageContaining("node type");
        assertThatThrownBy(() -> AdfCodec.toJson(AdfDocument.newBuilder().addContent(AdfNode.newBuilder()
                .setType(AdfNodeType.ADF_NODE_TYPE_PANEL)
                .setPanel(AdfPanelAttrs.getDefaultInstance())).build()))
                .isInstanceOf(AdfCodec.AdfException.class).hasMessageContaining("panel_type");
        assertThatThrownBy(() -> AdfCodec.toJson(AdfDocument.newBuilder().addContent(AdfNode.newBuilder()
                .setType(AdfNodeType.ADF_NODE_TYPE_PARAGRAPH)
                .setParagraph(AdfLocalIdAttrs.getDefaultInstance()).setText("x")).build()))
                .isInstanceOf(AdfCodec.AdfException.class).hasMessageContaining("text");
    }

    @Test
    void unknownJsonIsRefusedInsteadOfDropped() throws Exception {
        assertThatThrownBy(() -> AdfCodec.fromJson(JSON.readTree(
                "{\"type\":\"doc\",\"version\":1,\"content\":[{\"type\":\"hologram\"}]}")))
                .isInstanceOf(AdfCodec.AdfException.class).hasMessageContaining("hologram");
        assertThatThrownBy(() -> AdfCodec.fromJson(JSON.readTree(
                "{\"type\":\"doc\",\"version\":1,\"content\":[{\"type\":\"heading\","
                        + "\"attrs\":{\"level\":1,\"glow\":true}}]}")))
                .isInstanceOf(AdfCodec.AdfException.class).hasMessageContaining("glow");
        assertThatThrownBy(() -> AdfCodec.fromJson(JSON.readTree("{\"type\":\"paragraph\"}")))
                .isInstanceOf(AdfCodec.AdfException.class);
    }

    /**
     * Every node and mark definition of the vendored schema has an enum
     * value, and every attribute it declares has a field of the same JSON
     * name on that type's attrs message; the model names nothing the schema
     * lacks.
     */
    @Test
    void theModelCoversTheVendoredSchema() throws Exception {
        JsonNode definitions = JSON.readTree(Files.readString(SCHEMA)).path("definitions");
        Set<String> schemaNodes = new TreeSet<>();
        Set<String> schemaMarks = new TreeSet<>();
        for (Map.Entry<String, JsonNode> definition : definitions.properties()) {
            JsonNode properties = definition.getValue().path("properties");
            String type = properties.path("type").path("enum").path(0).asText("");
            if (type.isEmpty()) {
                continue;
            }
            boolean mark = definition.getKey().endsWith("_mark");
            (mark ? schemaMarks : schemaNodes).add(type);
            if (type.equals("doc")) {
                continue;
            }
            Descriptor owner = mark ? AdfMark.getDescriptor() : AdfNode.getDescriptor();
            Set<String> declared = new TreeSet<>();
            JsonNode attrs = properties.path("attrs");
            attrs.path("properties").properties().forEach(p -> declared.add(p.getKey()));
            attrs.path("anyOf").forEach(variant ->
                    variant.path("properties").properties().forEach(p -> declared.add(p.getKey())));
            FieldDescriptor named = owner.findFieldByName(snake(type));
            FieldDescriptor arm = named == null || named.getContainingOneof() == null ? null : named;
            if (declared.isEmpty()) {
                assertThat(arm).as("%s has no attrs", type).isNull();
                continue;
            }
            assertThat(arm).as("attrs arm of %s", type).isNotNull();
            Set<String> modelled = new TreeSet<>();
            arm.getMessageType().getFields().forEach(field -> modelled.add(field.getJsonName()));
            assertThat(modelled).as("attributes of %s", type).containsAll(declared);
        }
        schemaNodes.remove("doc");
        assertThat(wireNames(AdfNodeType.getDescriptor())).isEqualTo(schemaNodes);
        assertThat(wireNames(AdfMarkType.getDescriptor())).isEqualTo(schemaMarks);
    }

    private static Set<String> wireNames(com.google.protobuf.Descriptors.EnumDescriptor type) {
        Set<String> names = new TreeSet<>();
        String prefix = type.getValues().get(0).getName().replace("UNSPECIFIED", "");
        type.getValues().stream().skip(1).forEach(value -> {
            String[] words = value.getName().substring(prefix.length()).toLowerCase().split("_");
            StringBuilder camel = new StringBuilder(words[0]);
            for (int i = 1; i < words.length; i++) {
                camel.append(Character.toUpperCase(words[i].charAt(0))).append(words[i].substring(1));
            }
            names.add(camel.toString());
        });
        return names;
    }

    private static String snake(String camel) {
        StringBuilder snake = new StringBuilder();
        for (char c : camel.toCharArray()) {
            if (Character.isUpperCase(c)) {
                snake.append('_');
            }
            snake.append(Character.toLowerCase(c));
        }
        return snake.toString();
    }
}
