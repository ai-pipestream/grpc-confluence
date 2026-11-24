package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.UnresolvedReason;
import ai.pipestream.confluence.v1.WatchChangesResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** What one drain of the Forge queue does with an event whose re-read fails. */
class ConfluenceChangeDrainTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String QUEUE = "/x/queue";
    private static final String BY_ID = "/wiki/rest/api/group/by-id";

    private FakeConfluenceServer fake;
    private ConfluenceChangeGrpcService service;
    private final List<WatchChangesResponse> sent = new ArrayList<>();
    private ConfluenceChangeGrpcService.Emitter out;

    @BeforeEach
    void start() throws Exception {
        fake = FakeConfluenceServer.start();
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder().baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai").apiToken("token-123").pageSize(2).build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ZERO);
        service = new ConfluenceChangeGrpcService(new ConfluenceGrpcService(config, client,
                ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES),
                new ForgeEventQueue(fake.baseUrl().replace("/wiki", "") + QUEUE, "secret",
                        HttpClient.newHttpClient()),
                Clock.systemUTC());
        out = new ConfluenceChangeGrpcService.Emitter(new StreamObserver<>() {
            @Override
            public void onNext(WatchChangesResponse value) {
                sent.add(value);
            }

            @Override
            public void onError(Throwable t) {
                throw new AssertionError(t);
            }

            @Override
            public void onCompleted() {
                // Not ended by these tests.
            }
        }, "");
        fake.stub(QUEUE, "{\"events\":[" + queued("k1", "g1") + "," + queued("k2", "g2") + ","
                + queued("k3", "g3") + "]}");
        for (String id : List.of("g1", "g2", "g3")) {
            fake.stub(BY_ID + "?id=" + id,
                    "{\"id\":\"" + id + "\",\"name\":\"" + id + "\",\"type\":\"group\"}");
            fake.stub("/wiki/rest/api/group/" + id + "/membersByGroupId",
                    "{\"results\":[],\"start\":0,\"limit\":2,\"size\":0}");
        }
    }

    @AfterEach
    void stop() {
        fake.close();
    }

    private static String queued(String key, String groupId) {
        return "{\"key\":\"evt:" + key + "\",\"event\":{\"eventType\":\"avi:confluence:created:group\","
                + "\"group\":{\"id\":\"" + groupId + "\",\"name\":\"" + groupId + "\"}}}";
    }

    /** The keys of the one acknowledge request the drain sent; empty when it sent none. */
    private List<String> acknowledged() throws IOException {
        List<String> keys = new ArrayList<>();
        for (FakeConfluenceServer.RecordedRequest request : fake.requestsTo(QUEUE)) {
            JsonNode body = JSON.readTree(request.body());
            if (body.path("action").asText().equals("ack")) {
                body.path("keys").forEach(key -> keys.add(key.asText()));
            }
        }
        return keys;
    }

    @Test
    void aReadRejectedForGoodIsSentUnresolvedAndTheQueueMovesOn() throws Exception {
        fake.stub(BY_ID + "?id=g2", new FakeConfluenceServer.Stub(403, "{\"message\":\"no\"}", Map.of()));

        service.drain(SyncRequest.getDefaultInstance(), out);

        assertThat(sent).hasSize(3);
        assertThat(sent.get(0).getChange().getEntity().getGroup().getId()).isEqualTo("g1");
        assertThat(sent.get(1).getUnresolvedEvent().getContentId()).isEqualTo("g2");
        assertThat(sent.get(1).getUnresolvedReason())
                .isEqualTo(UnresolvedReason.UNRESOLVED_REASON_READ_REJECTED);
        assertThat(sent.get(2).getChange().getEntity().getGroup().getId()).isEqualTo("g3");
        assertThat(sent.get(0).getUnresolvedReason())
                .isEqualTo(UnresolvedReason.UNRESOLVED_REASON_UNSPECIFIED);
        assertThat(acknowledged()).containsExactly("evt:k1", "evt:k2", "evt:k3");
    }

    @Test
    void aReadThatMayPassLaterLeavesTheEventAndTheOnesBehindIt() throws Exception {
        fake.stub(BY_ID + "?id=g2", new FakeConfluenceServer.Stub(500, "{}", Map.of()));

        service.drain(SyncRequest.getDefaultInstance(), out);

        assertThat(sent).singleElement().satisfies(message ->
                assertThat(message.getChange().getEntity().getGroup().getId()).isEqualTo("g1"));
        assertThat(acknowledged()).containsExactly("evt:k1");
    }

    @Test
    void anEventNoReadCoversSaysSo() throws Exception {
        fake.stub(QUEUE, """
                {"events":[{"key":"evt:k1","event":{"eventType":"avi:confluence:created:user",
                "user":{"accountId":"a1"}}}]}""");

        service.drain(SyncRequest.getDefaultInstance(), out);

        assertThat(sent).singleElement().satisfies(message -> {
            assertThat(message.hasUnresolvedEvent()).isTrue();
            assertThat(message.getUnresolvedReason())
                    .isEqualTo(UnresolvedReason.UNRESOLVED_REASON_NOT_REREAD);
        });
        assertThat(acknowledged()).containsExactly("evt:k1");
    }

    @Test
    void onlyAnswersARetryCannotChangeArePermanent() {
        for (int status : List.of(400, 403, 404, 409, 410, 422)) {
            assertThat(ConfluenceChangeGrpcService.permanent(
                    new ConfluenceClient.ConfluenceApiException(status, "{}"))).as("HTTP %s", status)
                    .isEqualTo(UnresolvedReason.UNRESOLVED_REASON_READ_REJECTED);
        }
        for (int status : List.of(401, 408, 429, 500, 502, 503)) {
            assertThat(ConfluenceChangeGrpcService.permanent(
                    new ConfluenceClient.ConfluenceApiException(status, "{}"))).as("HTTP %s", status)
                    .isNull();
        }
        assertThat(ConfluenceChangeGrpcService.permanent(
                ConfluenceClient.MalformedResponseException.missing("/rest/api/group", "id",
                        JSON.createObjectNode())))
                .isEqualTo(UnresolvedReason.UNRESOLVED_REASON_READ_UNMAPPABLE);
        assertThat(ConfluenceChangeGrpcService.permanent(new IOException("connection reset")))
                .isNull();
    }
}
