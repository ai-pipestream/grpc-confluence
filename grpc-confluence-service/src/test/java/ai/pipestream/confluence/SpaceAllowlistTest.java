package ai.pipestream.confluence;

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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * A space allowlist names the spaces a pass must cover. Confluence answers
 * {@code GET /spaces?keys=...} with the spaces it has and can show, and says
 * nothing about a key that is misspelled, gone, or out of the credentials'
 * sight. A pass that crawls the rest and ends OK tells the caller the whole
 * allowlist was covered, and a full crawl then reconciles the missing space's
 * rows as deleted. The pass has to fail and name the keys.
 */
class SpaceAllowlistTest {

    private FakeConfluenceServer fake;
    private InMemoryChangeSink sink;
    private Server server;
    private ManagedChannel channel;

    @BeforeEach
    void startFake() throws Exception {
        fake = FakeConfluenceServer.start();
        sink = new InMemoryChangeSink();
        // Whatever keys are asked for, Confluence has ENG only.
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
    void aRequestedSpaceThatConfluenceDoesNotReturnFailsTheSync() throws Exception {
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService(config());
        List<SyncResponse> responses = new ArrayList<>();

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.sync(SyncRequest.newBuilder().addSpaceKeys("ENG").addSpaceKeys("DOCS")
                        .build()).forEachRemaining(responses::add));

        assertThat(failure).as("Sync of ENG and DOCS when Confluence returns ENG only").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
        assertThat(failure.getStatus().getDescription()).contains("DOCS").doesNotContain("ENG,");
        assertThat(responses).as("nothing is crawled for a pass that cannot cover its allowlist")
                .isEmpty();
    }

    @Test
    void aConfiguredSpaceThatConfluenceDoesNotReturnFailsTheSyncBeforeAnyReconcile()
            throws Exception {
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService(
                config("ENG", "DOCS", "HR"));

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.sync(SyncRequest.getDefaultInstance()).forEachRemaining(r -> { }));

        assertThat(failure).as("Sync of a configured allowlist with two keys not returned")
                .isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
        assertThat(failure.getStatus().getDescription()).contains("DOCS", "HR");
        assertThat(sink.completedRuns()).as("no reconcile over a pass that missed a space")
                .isEmpty();
        assertThat(sink.changes()).isEmpty();
    }

    @Test
    void anIncrementalSyncFailsTheSameWay() throws Exception {
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService(config());

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.sync(SyncRequest.newBuilder().addSpaceKeys("DOCS")
                        .setSinceCursor("2026-01-01T00:00:00Z").build())
                        .forEachRemaining(r -> { }));

        assertThat(failure).as("incremental Sync of a space Confluence does not return").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
        assertThat(failure.getStatus().getDescription()).contains("DOCS");
    }

    @Test
    void theCrawlerNamesTheMissingKeys() {
        ConfluenceConnectorConfig config = config("ENG", "DOCS");
        ConfluenceCrawler crawler = new ConfluenceCrawler(config, client(config), sink);

        IOException failure = catchThrowableOfType(IOException.class, crawler::crawl);

        assertThat(failure).as("a crawl of ENG and DOCS when Confluence returns ENG only")
                .isNotNull();
        assertThat(failure.getMessage()).contains("DOCS");
        assertThat(failure).isInstanceOfSatisfying(ConfluenceCrawler.SpacesNotFoundException.class,
                missing -> assertThat(missing.keys()).containsExactly("DOCS"));
        assertThat(sink.changes()).isEmpty();
    }

    @Test
    void aKeyInAnotherCaseIsNotTakenForTheSpace() {
        // Confluence may answer keys=eng with ENG; the allowlist names eng.
        ConfluenceConnectorConfig config = config("eng");
        ConfluenceCrawler crawler = new ConfluenceCrawler(config, client(config), sink);

        IOException failure = catchThrowableOfType(IOException.class, crawler::crawl);

        assertThat(failure).isInstanceOfSatisfying(ConfluenceCrawler.SpacesNotFoundException.class,
                missing -> assertThat(missing.keys()).containsExactly("eng"));
    }

    @Test
    void aKeyNamedTwiceIsReportedOnce() {
        ConfluenceConnectorConfig config = config("DOCS", "ENG", "DOCS");
        ConfluenceCrawler crawler = new ConfluenceCrawler(config, client(config), sink);

        IOException failure = catchThrowableOfType(IOException.class, crawler::crawl);

        assertThat(failure).isInstanceOfSatisfying(ConfluenceCrawler.SpacesNotFoundException.class,
                missing -> assertThat(missing.keys()).containsExactly("DOCS"));
    }

    @Test
    void anAllowlistThatConfluenceReturnsInFullIsCrawled() throws Exception {
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService(config());
        List<SyncResponse> responses = new ArrayList<>();

        stub.sync(SyncRequest.newBuilder().addSpaceKeys("ENG").build())
                .forEachRemaining(responses::add);

        assertThat(responses.getLast().getEventCase())
                .isEqualTo(SyncResponse.EventCase.RESUME_CURSOR);
        assertThat(sink.changes()).anyMatch(c -> c.getEntity().hasPage());
    }

    @Test
    void withoutAnAllowlistWhateverConfluenceReturnsIsCrawled() throws Exception {
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService(config());
        List<SyncResponse> responses = new ArrayList<>();

        stub.sync(SyncRequest.getDefaultInstance()).forEachRemaining(responses::add);

        assertThat(responses.getLast().getEventCase())
                .isEqualTo(SyncResponse.EventCase.RESUME_CURSOR);
        assertThat(sink.completedRuns()).hasSize(1);
    }

    private ConfluenceConnectorConfig config(String... spaces) {
        return ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("token-123")
                .spaces(spaces)
                .build();
    }

    private static ConfluenceClient client(ConfluenceConnectorConfig config) {
        return new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                Duration.ZERO);
    }

    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub startService(
            ConfluenceConnectorConfig config) throws Exception {
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
        return ConfluenceServiceGrpc.newBlockingStub(channel);
    }
}
