package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.GetPageRequest;
import ai.pipestream.confluence.v1.ListAttachmentsRequest;
import ai.pipestream.confluence.v1.ListAttachmentsResponse;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.StreamSpacesRequest;
import ai.pipestream.confluence.v1.StreamSpacesResponse;
import com.fasterxml.jackson.databind.JsonNode;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Cheap read-only probes against the real Confluence workspace. Gated on
 * credentials ({@code CONFLUENCE_EMAIL} + {@code CONFLUENCE_API_TOKEN}, or
 * the {@code CONFLUENCE_USER} / {@code CONFLUENCE_TOKEN} aliases) and
 * excluded from the default {@code test} task; run with
 * {@code ./gradlew :grpc-confluence-service:liveSmokeTest}. Never prints
 * the token. Never runs a full {@code Sync} crawl.
 */
class ConfluenceLiveSmokeIT {

    private static final String DEFAULT_BASE_URL = "https://pipestreamai.atlassian.net/wiki";
    private static final ConfluenceValidator VALIDATOR = ConfluenceValidator.create();

    private static String credential(String canonical, String alias) {
        String value = System.getenv(canonical);
        if (value == null || value.isBlank()) {
            value = System.getenv(alias);
        }
        return value == null || value.isBlank() ? null : value;
    }

    @Test
    void listSpacesThenHomepageAndAttachments() throws Exception {
        String email = credential(ConfluenceConnectorConfig.ENV_EMAIL,
                ConfluenceConnectorConfig.ENV_EMAIL_ALIAS);
        String token = credential(ConfluenceConnectorConfig.ENV_API_TOKEN,
                ConfluenceConnectorConfig.ENV_API_TOKEN_ALIAS);
        assumeTrue(email != null && token != null,
                "no live Confluence credentials in the environment; skipping");
        String baseUrl = System.getenv(ConfluenceConnectorConfig.ENV_BASE_URL);
        if (baseUrl == null || baseUrl.isBlank()) {
            baseUrl = DEFAULT_BASE_URL;
        }

        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(baseUrl)
                .email(email)
                .apiToken(token)
                .pageSize(10)
                .build();
        ConfluenceGrpcService service = new ConfluenceGrpcService(config,
                new ConfluenceClient(config), ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES);
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(service)
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        try {
            ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub =
                    ConfluenceServiceGrpc.newBlockingStub(channel);
            List<StreamSpacesResponse> spaces = new ArrayList<>();
            stub.streamSpaces(StreamSpacesRequest.newBuilder().setLimit(1).build())
                    .forEachRemaining(spaces::add);

            assertThat(spaces).hasSize(1);
            Space space = spaces.get(0).getSpace();
            assertThat(space.getId()).isNotBlank();
            assertThat(space.getKey()).isNotBlank();
            assertThat(VALIDATOR.validate(space).violations()).isEmpty();
            System.out.println("[ConfluenceLiveSmokeIT] StreamSpaces limit=1 -> key="
                    + space.getKey() + " name=" + space.getName()
                    + " homepage=" + space.getHomepageId());

            if (space.getHomepageId().isBlank()) {
                return;
            }
            Page page = stub.getPage(GetPageRequest.newBuilder()
                    .setId(space.getHomepageId()).build()).getPage();
            assertThat(page.getId()).isEqualTo(space.getHomepageId());
            assertThat(page.getSpaceId()).isEqualTo(space.getId());
            assertThat(VALIDATOR.validate(page).violations()).isEmpty();

            List<ListAttachmentsResponse> attachments = new ArrayList<>();
            stub.listAttachments(ListAttachmentsRequest.newBuilder()
                    .setPageId(page.getId()).build()).forEachRemaining(attachments::add);
            assertThat(attachments).allSatisfy(row ->
                    assertThat(VALIDATOR.validate(row.getAttachment()).violations()).isEmpty());
            System.out.println("[ConfluenceLiveSmokeIT] homepage " + page.getId()
                    + " attachments=" + attachments.size());

            // Shape probe for include-collaborators / include-webresources:
            // the v2 OpenAPI document declares the flags but no response
            // schema. Prints field names and JSON node types only, never
            // values, so the contract can be typed from a real tenant.
            JsonNode raw = new ConfluenceClient(config).get("/api/v2/pages/" + page.getId(),
                    Map.of("include-collaborators", "true", "include-webresources", "true"));
            for (String field : List.of("collaborators", "webresources", "webResources")) {
                System.out.println("[ConfluenceLiveSmokeIT] shape " + field + " = "
                        + shape(raw.get(field), 0));
            }
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }

    /** Field names and node types of a JSON tree, with no values. */
    private static String shape(JsonNode node, int depth) {
        if (node == null || node.isMissingNode()) {
            return "<absent>";
        }
        if (depth > 4) {
            return "...";
        }
        if (node.isObject()) {
            StringBuilder out = new StringBuilder("{");
            node.properties().forEach(e -> out.append(e.getKey()).append(':')
                    .append(shape(e.getValue(), depth + 1)).append(", "));
            return out.append('}').toString();
        }
        if (node.isArray()) {
            return "[" + node.size() + "x " + (node.isEmpty() ? "?" : shape(node.get(0), depth + 1)) + "]";
        }
        return node.getNodeType().name().toLowerCase(java.util.Locale.ROOT);
    }
}
