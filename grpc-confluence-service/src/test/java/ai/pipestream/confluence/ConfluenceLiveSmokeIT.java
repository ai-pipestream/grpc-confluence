package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.GetPageRequest;
import ai.pipestream.confluence.v1.ListAttachmentsRequest;
import ai.pipestream.confluence.v1.ListAttachmentsResponse;
import ai.pipestream.confluence.v1.ListSpacesRequest;
import ai.pipestream.confluence.v1.ListSpacesResponse;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
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
            ListSpacesResponse response = stub.listSpaces(
                    ListSpacesRequest.newBuilder().setLimit(1).build());

            assertThat(response.getSpacesCount()).isEqualTo(1);
            Space space = response.getSpaces(0);
            assertThat(space.getId()).isNotBlank();
            assertThat(space.getKey()).isNotBlank();
            assertThat(VALIDATOR.validate(space).violations()).isEmpty();
            System.out.println("[ConfluenceLiveSmokeIT] ListSpaces limit=1 -> key="
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
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }
}
