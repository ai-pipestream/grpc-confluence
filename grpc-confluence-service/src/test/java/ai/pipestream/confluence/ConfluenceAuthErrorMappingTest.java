package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.GetPageRequest;
import ai.pipestream.confluence.v1.ListSpacesRequest;
import ai.pipestream.confluence.v1.ProbeConnectionRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Error mapping for the response shapes Confluence Cloud actually sends when
 * authentication fails, captured live on 2026-08-24 (see
 * {@code src/test/resources/live-captures/}). Cloud masks a bad or revoked
 * API token: v2 endpoints answer HTTP 404 with a generic NOT_FOUND error
 * document (identical to the anonymous response), and v1 endpoints answer
 * HTTP 403 "caller cannot access Confluence". The facade must surface those
 * as {@code NOT_FOUND} / {@code PERMISSION_DENIED} with the upstream body
 * preserved in the description so operators can tell what Cloud really said.
 */
class ConfluenceAuthErrorMappingTest {

    private FakeConfluenceServer fake;
    private Server server;
    private ManagedChannel channel;
    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub;

    @BeforeEach
    void startStack() throws Exception {
        fake = FakeConfluenceServer.start();
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("revoked-token")
                .build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ZERO);
        server = InProcessServerBuilder.forName("confluence-auth-mapping-test")
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, client,
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        channel = InProcessChannelBuilder.forName("confluence-auth-mapping-test").build();
        stub = ConfluenceServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void stopStack() {
        channel.shutdownNow();
        server.shutdownNow();
        fake.close();
    }

    @Test
    void maskedV2AuthFailureMapsToNotFoundWithUpstreamBody() {
        String body = capture("v2-spaces-auth-masked-404.json");
        fake.stub("/wiki/api/v2/spaces", new FakeConfluenceServer.Stub(404, body, Map.of()));

        assertThatThrownBy(() -> stub.listSpaces(ListSpacesRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
                    assertThat(e.getStatus().getDescription()).contains("NOT_FOUND");
                });
    }

    @Test
    void maskedV2AuthFailureOnGetPageMapsToNotFound() {
        String body = capture("v2-spaces-auth-masked-404.json");
        fake.stub("/wiki/api/v2/pages/123", new FakeConfluenceServer.Stub(404, body, Map.of()));

        assertThatThrownBy(() -> stub.getPage(GetPageRequest.newBuilder().setId("123").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
    }

    @Test
    void v1StyleForbiddenMapsToPermissionDeniedWithUpstreamMessage() {
        String body = capture("v1-space-forbidden-403.json");
        fake.stub("/wiki/api/v2/spaces", new FakeConfluenceServer.Stub(403, body, Map.of()));

        assertThatThrownBy(() -> stub.listSpaces(ListSpacesRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
                    assertThat(e.getStatus().getDescription())
                            .contains("caller cannot access Confluence");
                });
    }

    @Test
    void probeConnectionReturnsNotOkWithErrorMessageOnMaskedAuthFailure() {
        String body = capture("v2-spaces-auth-masked-404.json");
        fake.stub("/wiki/api/v2/spaces", new FakeConfluenceServer.Stub(404, body, Map.of()));

        var response = stub.probeConnection(ProbeConnectionRequest.getDefaultInstance());

        assertThat(response.getOk()).isFalse();
        assertThat(response.getErrorMessage()).contains("404").contains("NOT_FOUND");
        assertThat(response.getSpaceKeysList()).isEmpty();
    }

    @Test
    void probeConnectionReturnsOkWithSpaceKeysWhenTokenWorks() {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));

        var response = stub.probeConnection(ProbeConnectionRequest.getDefaultInstance());

        assertThat(response.getOk()).isTrue();
        assertThat(response.getSpaceKeysList()).containsExactly("ENG");
        assertThat(response.getErrorMessage()).isEmpty();
    }

    private static String capture(String name) {
        try (InputStream in = ConfluenceAuthErrorMappingTest.class
                .getResourceAsStream("/live-captures/" + name)) {
            if (in == null) {
                throw new IllegalStateException("missing live capture: " + name);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            throw new IllegalStateException("failed to read live capture: " + name, e);
        }
    }
}
