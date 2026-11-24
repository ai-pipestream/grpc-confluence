package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.ContentRef;
import ai.pipestream.confluence.v1.ContentType;
import ai.pipestream.confluence.v1.GetSpaceRequest;
import ai.pipestream.confluence.v1.ListAncestorsRequest;
import ai.pipestream.confluence.v1.ListGroupMembersRequest;
import ai.pipestream.confluence.v1.ProbeConnectionRequest;
import ai.pipestream.confluence.v1.ProbeConnectionResponse;
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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * A list walk ends on the page that has no next link, so an answer that is
 * not a list at all must not pass for an empty last page. A redirect with no
 * body (the base URL is {@code http://}, or the site was renamed), a 204
 * from something in between, and a 2xx body with no {@code results} array
 * were all read as "no results": Sync ended OK having emitted nothing, and a
 * probe reported the connection healthy.
 */
class NotAListAnswerTest {

    private FakeConfluenceServer fake;
    private InMemoryChangeSink sink;
    private Server server;
    private ManagedChannel channel;
    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub;

    @BeforeEach
    void start() throws Exception {
        fake = FakeConfluenceServer.start();
        sink = new InMemoryChangeSink();
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                ConfluenceFixtures.pageJson("201", "100", "Runbook", "2026-01-01T00:00:00.000Z")));
        for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                "properties")) {
            fake.stub("/wiki/api/v2/pages/201/" + sub, ConfluenceFixtures.emptyListJson());
        }
        ConfluenceConnectorConfig config = config();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                // The service blocks on outbound flow control, as in production:
                // a direct executor would run it on the client thread and stall.
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, client(config),
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES, sink))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = ConfluenceServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
        fake.close();
    }

    @Test
    void aRedirectOnTheSpaceListFailsTheSync() {
        fake.stub("/wiki/api/v2/spaces", redirect("https://renamed.example/wiki/api/v2/spaces"));

        StatusRuntimeException failure = syncFailure();

        assertThat(failure).as("Sync whose space list answered 302 with no body").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(failure.getStatus().getDescription())
                .contains("302", "https://renamed.example/wiki/api/v2/spaces");
        assertThat(sink.completedRuns()).isEmpty();
    }

    @Test
    void aRedirectOnTheSpaceListFailsTheProbe() {
        fake.stub("/wiki/api/v2/spaces", redirect("https://renamed.example/wiki/api/v2/spaces"));

        ProbeConnectionResponse probe = stub.probeConnection(
                ProbeConnectionRequest.getDefaultInstance());

        assertThat(probe.getOk()).as("a probe that was redirected away").isFalse();
        assertThat(probe.getErrorMessage()).contains("302");
    }

    @Test
    void aRedirectIsNotReadAsNoSuchSpace() {
        fake.stub("/wiki/api/v2/spaces", redirect("https://renamed.example/wiki/api/v2/spaces"));

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.getSpace(GetSpaceRequest.newBuilder().setKey("ENG").build()));

        assertThat(failure).isNotNull();
        assertThat(failure.getStatus().getCode())
                .as("GetSpace by key when the lookup was redirected")
                .isEqualTo(Status.Code.FAILED_PRECONDITION);
    }

    @Test
    void aNoContentAnswerOnAListFailsTheSync() {
        fake.stub("/wiki/api/v2/spaces", new FakeConfluenceServer.Stub(204, "", Map.of()));

        StatusRuntimeException failure = syncFailure();

        assertThat(failure).as("Sync whose space list answered 204").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(failure.getStatus().getDescription()).contains("/api/v2/spaces", "results");
    }

    @Test
    void aBodyWithoutResultsOnALaterPageFailsTheCrawlInsteadOfEndingTheWalk() {
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson("p2",
                ConfluenceFixtures.pageJson("201", "100", "Runbook", "2026-01-01T00:00:00.000Z")));
        fake.stub("/wiki/api/v2/pages?cursor=p2", "{\"message\":\"please retry\"}");
        ConfluenceConnectorConfig config = config();
        ConfluenceCrawler crawler = new ConfluenceCrawler(config, client(config), sink);

        IOException failure = catchThrowableOfType(IOException.class, crawler::crawl);

        assertThat(failure).as("a crawl whose second page of pages is not a list").isNotNull();
        assertThat(failure.getMessage()).contains("/api/v2/pages", "results");
        assertThat(sink.snapshots()).as("no sweep is reported complete").isEmpty();
    }

    @Test
    void anEmptyListIsStillAnEmptyList() {
        fake.stub("/wiki/api/v2/spaces", ConfluenceFixtures.emptyListJson());
        List<SyncResponse> responses = new ArrayList<>();

        stub.sync(SyncRequest.getDefaultInstance()).forEachRemaining(responses::add);

        assertThat(responses).singleElement()
                .satisfies(r -> assertThat(r.getEventCase())
                        .isEqualTo(SyncResponse.EventCase.RESUME_CURSOR));
    }

    @Test
    void aRedirectCarriesItsStatusAndWhereItPointed() {
        fake.stub("/wiki/api/v2/spaces", redirect("https://renamed.example/wiki/api/v2/spaces"));
        ConfluenceClient client = client(config());

        ConfluenceClient.ConfluenceApiException failure = catchThrowableOfType(
                ConfluenceClient.ConfluenceApiException.class, () -> client.get("/api/v2/spaces"));

        assertThat(failure).isNotNull();
        assertThat(failure.status()).isEqualTo(302);
        assertThat(failure.getMessage())
                .contains("https://renamed.example/wiki/api/v2/spaces", "not followed");
        assertThat(fake.requestsTo("/wiki/api/v2/spaces")).as("a redirect is not retried").hasSize(1);
    }

    @Test
    void aRedirectWithNoLocationSaysSo() {
        fake.stub("/wiki/api/v2/spaces", new FakeConfluenceServer.Stub(301, "", Map.of()));
        ConfluenceClient client = client(config());

        ConfluenceClient.ConfluenceApiException failure = catchThrowableOfType(
                ConfluenceClient.ConfluenceApiException.class, () -> client.get("/api/v2/spaces"));

        assertThat(failure).isNotNull();
        assertThat(failure.status()).isEqualTo(301);
        assertThat(failure.getMessage()).contains("no Location header");
    }

    @Test
    void aWriteThatIsRedirectedIsNotAWriteThatHappened() {
        fake.stub("POST", "/wiki/api/v2/pages", 307, "");
        ConfluenceClient client = client(config());

        ConfluenceClient.ConfluenceApiException failure = catchThrowableOfType(
                ConfluenceClient.ConfluenceApiException.class,
                () -> client.post("/api/v2/pages", ConfluenceClient.responseJson().createObjectNode()));

        assertThat(failure).as("a POST answered 307 with no body").isNotNull();
        assertThat(failure.status()).isEqualTo(307);
    }

    @Test
    void aListFailureNamesTheRouteWithoutItsCursor() {
        fake.stub("/wiki/api/v2/pages?cursor=opaque-cursor-value", "{\"message\":\"please retry\"}");
        ConfluenceClient client = client(config());

        ConfluenceClient.MalformedResponseException failure = catchThrowableOfType(
                ConfluenceClient.MalformedResponseException.class,
                () -> client.getPage("/api/v2/pages", Map.of("cursor", "opaque-cursor-value")));

        assertThat(failure).isNotNull();
        assertThat(failure.getMessage())
                .contains("/wiki/api/v2/pages", "results", "please retry")
                .doesNotContain("opaque-cursor-value", "http://");
    }

    @Test
    void anEmptyBodyIsCalledAnEmptyBody() {
        fake.stub("/wiki/api/v2/pages", new FakeConfluenceServer.Stub(200, "", Map.of()));
        ConfluenceClient client = client(config());

        ConfluenceClient.MalformedResponseException failure = catchThrowableOfType(
                ConfluenceClient.MalformedResponseException.class,
                () -> client.getList("/api/v2/pages", Map.of()));

        assertThat(failure).isNotNull();
        assertThat(failure.getMessage()).contains("an empty body");
    }

    @Test
    void resultsThatIsNotAnArrayIsNotAList() {
        fake.stub("/wiki/api/v2/pages", "{\"results\":{\"201\":\"page\"}}");
        ConfluenceClient client = client(config());

        assertThat(catchThrowableOfType(ConfluenceClient.MalformedResponseException.class,
                () -> client.getPage("/api/v2/pages", Map.of()))).isNotNull();
        assertThat(catchThrowableOfType(ConfluenceClient.MalformedResponseException.class,
                () -> client.getList("/api/v2/pages", Map.of()))).isNotNull();
    }

    @Test
    void aListReadReturnsTheBodyItChecked() throws Exception {
        ConfluenceClient client = client(config());

        assertThat(client.getList("/api/v2/pages", Map.of()).path("results")).hasSize(1);
        assertThat(ConfluenceClient.requireResults(client.get("/api/v2/pages"), "/api/v2/pages"))
                .hasSize(1);
    }

    @Test
    void aRedirectReportedByTheForgeAppIsAFailure() {
        fake.stub("POST", "/trigger", 200, "{\"status\":302,\"body\":null}");
        ConfluenceClient client = client(config());
        client.forgeApp(new ConfluenceClient.ForgeApp(fake.baseUrl().replace("/wiki", "") + "/trigger",
                "trigger-secret"));

        ConfluenceClient.ConfluenceApiException failure = catchThrowableOfType(
                ConfluenceClient.ConfluenceApiException.class,
                () -> client.getPage("/api/v2/app/properties", Map.of()));

        assertThat(failure).as("an app property list the Forge app saw redirected").isNotNull();
        assertThat(failure.status()).isEqualTo(302);
    }

    @Test
    void aForgeAppListAnswerWithoutResultsIsNotAnEmptyList() {
        fake.stub("POST", "/trigger", 200, "{\"status\":200,\"body\":{\"message\":\"no\"}}");
        ConfluenceClient client = client(config());
        client.forgeApp(new ConfluenceClient.ForgeApp(fake.baseUrl().replace("/wiki", "") + "/trigger",
                "trigger-secret"));

        ConfluenceClient.MalformedResponseException failure = catchThrowableOfType(
                ConfluenceClient.MalformedResponseException.class,
                () -> client.getPage("/api/v2/app/properties", Map.of()));

        assertThat(failure).isNotNull();
        assertThat(failure.getMessage()).contains("/wiki/api/v2/app/properties");
    }

    @Test
    void anOffsetPagedListWithoutResultsFailsTheCall() {
        fake.stub("/wiki/rest/api/group/g1/membersByGroupId", "{\"message\":\"please retry\"}");

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.listGroupMembers(ListGroupMembersRequest.newBuilder().setGroupId("g1")
                        .build()).forEachRemaining(r -> { }));

        assertThat(failure).as("ListGroupMembers whose answer is not a list").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(failure.getStatus().getDescription())
                .contains("/rest/api/group/g1/membersByGroupId", "results");
    }

    @Test
    void anAncestorsAnswerWithoutResultsFailsTheCall() {
        fake.stub("/wiki/api/v2/pages/201/ancestors", new FakeConfluenceServer.Stub(204, "", Map.of()));

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.listAncestors(ListAncestorsRequest.newBuilder()
                        .setContent(ContentRef.newBuilder()
                                .setType(ContentType.CONTENT_TYPE_PAGE).setId("201"))
                        .build()).forEachRemaining(r -> { }));

        assertThat(failure).as("ListAncestors whose answer is not a list").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(failure.getStatus().getDescription()).contains("/api/v2/pages/201/ancestors");
    }

    @Test
    void aSpaceThatIsNotThereIsStillNotFound() {
        fake.stub("/wiki/api/v2/spaces", ConfluenceFixtures.emptyListJson());

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.getSpace(GetSpaceRequest.newBuilder().setKey("NOPE").build()));

        assertThat(failure).isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    }

    private StatusRuntimeException syncFailure() {
        return catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.sync(SyncRequest.getDefaultInstance()).forEachRemaining(r -> { }));
    }

    private static FakeConfluenceServer.Stub redirect(String location) {
        return new FakeConfluenceServer.Stub(302, "", Map.of("location", location));
    }

    private ConfluenceConnectorConfig config() {
        return ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("token-123")
                .build();
    }

    private static ConfluenceClient client(ConfluenceConnectorConfig config) {
        return new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                Duration.ZERO);
    }
}
