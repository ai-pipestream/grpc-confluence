package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The whiteboards, databases, folders and Smart Links below a page are read
 * through the tree and database routes. Only a 404 on the first read of a
 * route means there is nothing to read (the tenant does not serve the route,
 * or the item is not visible to us). A 401, a 403, a 400, or a failure
 * partway through a list means the subtree could not be read: the crawl has
 * to fail, because a pass that ends OK without the subtree is taken for the
 * whole of the space and what it did not emit is treated as deleted.
 */
class TreeReadFailureTest {

    private FakeConfluenceServer fake;
    private InMemoryChangeSink sink;
    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void startFake() throws Exception {
        fake = FakeConfluenceServer.start();
        sink = new InMemoryChangeSink();
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                ConfluenceFixtures.pageJson("200", "100", "Design Doc", "2026-01-01T00:00:00.000Z")));
        for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                "properties")) {
            fake.stub("/wiki/api/v2/pages/200/" + sub, ConfluenceFixtures.emptyListJson());
        }
        // Below the page: a whiteboard and a database with one field and one entry.
        fake.stub("/wiki/api/v2/pages/200/direct-children", ConfluenceFixtures.listJson(null, "",
                child("w1", "whiteboard"), child("d1", "database")));
        fake.stub("/wiki/api/v2/whiteboards/w1", node("w1", "whiteboard", "Board"));
        fake.stub("/wiki/api/v2/databases/d1", node("d1", "database", "Tracker"));
        fake.stub("/wiki/api/v2/databases/d1/fields", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"fld1\",\"name\":\"Owner\",\"type\":\"text\"}"));
        fake.stub("/wiki/api/v2/databases/d1/entries", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"row1\"}"));
    }

    @AfterEach
    void stop() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
        fake.close();
    }

    @Test
    void theFixtureCrawlsTheWholeTree() throws Exception {
        crawler().crawl();

        assertThat(emittedIds()).contains("200", "w1", "d1", "database:d1/field/fld1",
                "database:d1/entry/row1");
    }

    @Test
    void aForbiddenChildListFailsTheCrawl() {
        fake.stub("/wiki/api/v2/pages/200/direct-children", refusal(403));

        ConfluenceClient.ConfluenceApiException failure = crawlFailure();

        assertThat(failure).as("a crawl whose direct-children read answered 403").isNotNull();
        assertThat(failure.status()).isEqualTo(403);
        assertThat(failure.getMessage()).contains("direct children",
                "/api/v2/pages/200/direct-children");
    }

    @Test
    void aRevokedTokenOnAChildReadFailsTheCrawl() {
        fake.stub("/wiki/api/v2/whiteboards/w1", refusal(401));

        ConfluenceClient.ConfluenceApiException failure = crawlFailure();

        assertThat(failure).as("a crawl whose whiteboard read answered 401").isNotNull();
        assertThat(failure.status()).isEqualTo(401);
        assertThat(failure.getMessage()).contains("whiteboard", "/api/v2/whiteboards/w1");
    }

    @Test
    void aRefusedDatabaseEntryListFailsTheCrawl() {
        fake.stub("/wiki/api/v2/databases/d1/entries", refusal(400));

        ConfluenceClient.ConfluenceApiException failure = crawlFailure();

        assertThat(failure).as("a crawl whose database entries read answered 400").isNotNull();
        assertThat(failure.status()).isEqualTo(400);
        assertThat(failure.getMessage()).contains("database entries",
                "/api/v2/databases/d1/entries");
    }

    @Test
    void aRefusedDatabaseFieldListFailsTheCrawl() {
        fake.stub("/wiki/api/v2/databases/d1/fields", refusal(403));

        ConfluenceClient.ConfluenceApiException failure = crawlFailure();

        assertThat(failure).as("a crawl whose database fields read answered 403").isNotNull();
        assertThat(failure.status()).isEqualTo(403);
    }

    @Test
    void aChildListThatFailsOnItsSecondPageFailsTheCrawl() {
        // Even a 404: the route served its first page, so this is not "not served".
        fake.stub("/wiki/api/v2/pages/200/direct-children", ConfluenceFixtures.listJson("p2",
                "/wiki/api/v2/pages/200/direct-children", child("w1", "whiteboard")));
        fake.stub("/wiki/api/v2/pages/200/direct-children?cursor=p2", refusal(404));

        ConfluenceClient.ConfluenceApiException failure = crawlFailure();

        assertThat(failure).as("a crawl whose direct-children walk broke on page two").isNotNull();
        assertThat(failure.status()).isEqualTo(404);
        assertThat(emittedIds()).as("rows of the first page are not passed off as the whole list")
                .doesNotContain("w1");
    }

    @Test
    void syncAnswersTheRefusalInsteadOfAResumeCursor() throws Exception {
        fake.stub("/wiki/api/v2/pages/200/direct-children", refusal(403));
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService();
        List<SyncResponse> responses = new ArrayList<>();

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.sync(SyncRequest.getDefaultInstance()).forEachRemaining(responses::add));

        assertThat(failure).as("Sync over a subtree that could not be read").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
        assertThat(failure.getStatus().getDescription())
                .contains("/api/v2/pages/200/direct-children");
        assertThat(responses).noneMatch(r -> r.getEventCase() == SyncResponse.EventCase.RESUME_CURSOR);
        assertThat(sink.completedRuns()).as("no reconcile over a pass that missed a subtree")
                .isEmpty();
    }

    @Test
    void aRouteTheTenantDoesNotServeIsSkipped() throws Exception {
        fake.stub("/wiki/api/v2/pages/200/direct-children", refusal(404));

        crawler().crawl();

        assertThat(emittedIds()).contains("200").doesNotContain("w1", "d1");
    }

    @Test
    void aChildThatIsGoneWhenItIsReadIsSkippedAndItsSiblingsAreKept() throws Exception {
        fake.stub("/wiki/api/v2/whiteboards/w1", refusal(404));

        crawler().crawl();

        assertThat(emittedIds()).contains("200", "d1", "database:d1/entry/row1")
                .doesNotContain("w1");
    }

    @Test
    void aDatabaseWhoseDataRoutesAreNotServedIsStillEmitted() throws Exception {
        fake.stub("/wiki/api/v2/databases/d1/fields", refusal(404));
        fake.stub("/wiki/api/v2/databases/d1/entries", refusal(404));

        crawler().crawl();

        assertThat(emittedIds()).contains("d1", "w1")
                .doesNotContain("database:d1/field/fld1", "database:d1/entry/row1");
    }

    private ConfluenceClient.ConfluenceApiException crawlFailure() {
        return catchThrowableOfType(ConfluenceClient.ConfluenceApiException.class,
                () -> crawler().crawl());
    }

    private List<String> emittedIds() {
        return sink.changes().stream().map(c -> c.getEntity())
                .map(ConfluenceEntity::getEntityId).toList();
    }

    private static FakeConfluenceServer.Stub refusal(int status) {
        return new FakeConfluenceServer.Stub(status, "{\"message\":\"refused\"}", Map.of());
    }

    private static String child(String id, String type) {
        return "{\"id\":\"" + id + "\",\"type\":\"" + type + "\",\"status\":\"current\"}";
    }

    private static String node(String id, String type, String title) {
        return "{\"id\":\"" + id + "\",\"type\":\"" + type + "\",\"title\":\"" + title
                + "\",\"spaceId\":\"100\",\"status\":\"current\"}";
    }

    private ConfluenceConnectorConfig config() {
        return ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("token-123")
                .build();
    }

    private ConfluenceCrawler crawler() {
        ConfluenceConnectorConfig config = config();
        return new ConfluenceCrawler(config,
                new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                        Duration.ZERO),
                sink);
    }

    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub startService() throws Exception {
        ConfluenceConnectorConfig config = config();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                // The service blocks on outbound flow control, as in production:
                // a direct executor would run it on the client thread and stall.
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config,
                        new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                                Duration.ZERO),
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES, sink))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return ConfluenceServiceGrpc.newBlockingStub(channel);
    }
}
