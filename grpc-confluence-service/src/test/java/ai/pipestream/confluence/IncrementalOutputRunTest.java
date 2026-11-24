package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import ai.pipestream.okf.OkfOutput;
import ai.pipestream.sync.v1.ConnectionOutput;
import ai.pipestream.sync.v1.ConnectionServiceGrpc;
import ai.pipestream.sync.v1.GetSettingsRequest;
import ai.pipestream.sync.v1.GetSettingsResponse;
import ai.pipestream.sync.v1.RuntimeSettings;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The OKF tree, zip and WARC are written when a pass completes. Every Sync
 * that returns OK has to complete its pass for the outputs: an incremental
 * Sync whose changes never reach the bundle leaves it showing the last full
 * crawl, and a Sync of a few requested spaces must not replace the bundle of
 * the whole connection with those spaces alone.
 */
class IncrementalOutputRunTest {

    private FakeConfluenceServer fake;
    private final List<Server> servers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();

    @BeforeEach
    void startFake() throws Exception {
        fake = FakeConfluenceServer.start();
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/spaces/200/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());
        for (String page : List.of("201", "202", "301")) {
            for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                    "properties")) {
                fake.stub("/wiki/api/v2/pages/" + page + "/" + sub, ConfluenceFixtures.emptyListJson());
            }
        }
    }

    @AfterEach
    void stop() {
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(Server::shutdownNow);
        fake.close();
    }

    @Test
    void anIncrementalSyncBringsItsChangesIntoTheBundleOfTheLastFullCrawl(@TempDir Path dir)
            throws Exception {
        Instant start = now();
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("201", "100", "Runbook", start.minus(2, ChronoUnit.HOURS)),
                page("202", "100", "Roadmap", start.minus(3, ChronoUnit.HOURS))));
        Path tree = dir.resolve("okf");
        Path zip = dir.resolve("okf.zip");
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService(
                new OkfChangeSink(new OkfOutput(tree, zip, dir.resolve("okf.warc.gz"))), null);

        String cursor = resumeCursor(stub, SyncRequest.newBuilder().setIncludeBodies(true).build());
        assertThat(Files.readString(tree.resolve("pages/201.md"))).contains("title: Runbook");
        assertThat(tree.resolve("pages/202.md")).exists();

        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("201", "100", "Runbook, second edition", start.plus(10, ChronoUnit.MINUTES)),
                page("202", "100", "Roadmap", start.minus(3, ChronoUnit.HOURS))));
        resumeCursor(stub, SyncRequest.newBuilder()
                .setSinceCursor(cursor).setIncludeBodies(true).build());

        assertThat(Files.readString(tree.resolve("pages/201.md")))
                .as("the page edited before the incremental Sync, in the OKF tree")
                .contains("Runbook, second edition");
        assertThat(zipEntries(zip))
                .as("the bundle after the incremental Sync")
                .anyMatch(name -> name.endsWith("pages/201.md"))
                .anyMatch(name -> name.endsWith("pages/202.md"));
    }

    @Test
    void anIncrementalSyncWithNoFullCrawlBeforeItWritesItsChangesAsAnIncrement(@TempDir Path dir)
            throws Exception {
        Instant start = now();
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("201", "100", "Runbook", start.plus(10, ChronoUnit.MINUTES)),
                page("202", "100", "Roadmap", start.minus(3, ChronoUnit.HOURS))));
        Path out = dir.resolve("out");
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService(null,
                ConnectionOutput.newBuilder().setDirectory(out.toString()).addFormats("okf").build());

        resumeCursor(stub, SyncRequest.newBuilder()
                .setSinceCursor(start.minus(1, ChronoUnit.HOURS).toString())
                .setIncludeBodies(true).build());

        assertThat(regularFiles(out))
                .as("what the incremental Sync wrote to its configured output")
                .anyMatch(path -> path.startsWith(out.resolve("increments"))
                        && path.endsWith(Path.of("okf", "pages", "201.md")))
                .anyMatch(path -> path.startsWith(out.resolve("increments"))
                        && path.endsWith("bundle.zip"))
                .anyMatch(path -> path.startsWith(out.resolve("increments"))
                        && path.endsWith("bundle.warc.gz"));
        // Never over the place of the full bundle: this pass saw one page.
        assertThat(out.resolve("bundle.zip")).doesNotExist();
    }

    @Test
    void aSyncOfRequestedSpacesDoesNotReplaceTheBundleOfTheWholeConnection(@TempDir Path dir)
            throws Exception {
        Instant start = now();
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering"),
                        ConfluenceFixtures.spaceJson("200", "OPS", "Operations")));
        // The fake serves one page list for every space.
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("201", "100", "Runbook", start.minus(2, ChronoUnit.HOURS)),
                page("301", "200", "On-call rota", start.minus(3, ChronoUnit.HOURS))));
        Path out = dir.resolve("out");
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService(null,
                ConnectionOutput.newBuilder().setDirectory(out.toString()).addFormats("okf").build());

        resumeCursor(stub, SyncRequest.newBuilder().setIncludeBodies(true).build());
        assertThat(zipEntries(out.resolve("bundle.zip")))
                .anyMatch(name -> name.endsWith("pages/201.md"))
                .anyMatch(name -> name.endsWith("pages/301.md"));

        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("201", "100", "Runbook", start.minus(2, ChronoUnit.HOURS))));
        resumeCursor(stub, SyncRequest.newBuilder()
                .addSpaceKeys("ENG").setIncludeBodies(true).build());

        assertThat(zipEntries(out.resolve("bundle.zip")))
                .as("the bundle of the whole connection after a Sync of space ENG alone")
                .anyMatch(name -> name.endsWith("pages/301.md"));
    }

    @Test
    void everySyncCompletesItsPassAndOnlyAFullCrawlOfTheConnectionIsAFullSweep() throws Exception {
        Instant start = now();
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("201", "100", "Runbook", start.minus(2, ChronoUnit.HOURS))));
        InMemoryChangeSink downstream = new InMemoryChangeSink();
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService(downstream, null);

        resumeCursor(stub, SyncRequest.getDefaultInstance());
        assertThat(downstream.completedRuns()).hasSize(1);
        assertThat(downstream.partialRuns()).isEmpty();
        String fullRun = downstream.completedRuns().get(0);

        resumeCursor(stub, SyncRequest.newBuilder().addSpaceKeys("ENG").build());
        assertThat(downstream.completedRuns()).containsExactly(fullRun);
        assertThat(downstream.partialRuns()).hasSize(1);

        String since = start.minus(1, ChronoUnit.HOURS).toString();
        resumeCursor(stub, SyncRequest.newBuilder().setSinceCursor(since).build());
        assertThat(downstream.completedRuns()).containsExactly(fullRun);
        assertThat(downstream.partialRuns()).hasSize(2).last().isEqualTo(since);
    }

    @Test
    void aFullCrawlThatSawNoSpaceAtAllDoesNotEmptyTheBundle(@TempDir Path dir) throws Exception {
        Instant start = now();
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("201", "100", "Runbook", start.minus(2, ChronoUnit.HOURS))));
        Path zip = dir.resolve("okf.zip");
        InMemoryChangeSink passes = new InMemoryChangeSink();
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService(
                new CompositeChangeSink(List.of(passes, new OkfChangeSink(
                        new OkfOutput(dir.resolve("okf"), zip, dir.resolve("okf.warc.gz"))))),
                null);
        resumeCursor(stub, SyncRequest.newBuilder().setIncludeBodies(true).build());

        fake.stub("/wiki/api/v2/spaces", ConfluenceFixtures.emptyListJson());
        String cursor = resumeCursor(stub, SyncRequest.newBuilder().setIncludeBodies(true).build());

        // No space swept: no cursor, so the next pass is a full crawl again.
        assertThat(cursor).isEmpty();
        assertThat(passes.completedRuns()).hasSize(1);
        assertThat(zipEntries(zip)).anyMatch(name -> name.endsWith("pages/201.md"));
    }

    private static String resumeCursor(ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub,
            SyncRequest request) {
        List<SyncResponse> responses = new ArrayList<>();
        stub.sync(request).forEachRemaining(responses::add);
        assertThat(responses.getLast().getEventCase())
                .isEqualTo(SyncResponse.EventCase.RESUME_CURSOR);
        return responses.getLast().getResumeCursor();
    }

    private static List<String> zipEntries(Path zip) throws IOException {
        try (ZipFile file = new ZipFile(zip.toFile())) {
            return file.stream().map(entry -> entry.getName()).toList();
        }
    }

    private static List<Path> regularFiles(Path root) throws IOException {
        if (!Files.exists(root)) {
            return List.of();
        }
        try (Stream<Path> walk = Files.walk(root)) {
            return walk.filter(Files::isRegularFile).toList();
        }
    }

    /** Confluence timestamps carry milliseconds. */
    private static Instant now() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    private static String page(String id, String spaceId, String title, Instant modified) {
        return ConfluenceFixtures.pageJson(id, spaceId, title, modified.toString());
    }

    /**
     * Starts the service with a process-wide {@code downstream} sink, or with
     * a catalog whose settings name {@code catalogOutput}.
     */
    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub startService(ChangeSink downstream,
            ConnectionOutput catalogOutput) throws Exception {
        ConnectionServiceGrpc.ConnectionServiceBlockingStub connections = null;
        if (catalogOutput != null) {
            String catalogName = InProcessServerBuilder.generateName();
            servers.add(InProcessServerBuilder.forName(catalogName)
                    .directExecutor()
                    .addService(new ConnectionServiceGrpc.ConnectionServiceImplBase() {
                        @Override
                        public void getSettings(GetSettingsRequest request,
                                StreamObserver<GetSettingsResponse> observer) {
                            observer.onNext(GetSettingsResponse.newBuilder()
                                    .setSettings(RuntimeSettings.newBuilder().setOutput(catalogOutput))
                                    .build());
                            observer.onCompleted();
                        }
                    })
                    .build()
                    .start());
            ManagedChannel catalogChannel =
                    InProcessChannelBuilder.forName(catalogName).directExecutor().build();
            channels.add(catalogChannel);
            connections = ConnectionServiceGrpc.newBlockingStub(catalogChannel);
        }
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("token-123")
                .build();
        String name = InProcessServerBuilder.generateName();
        servers.add(InProcessServerBuilder.forName(name)
                // The service blocks on outbound flow control, as in production:
                // a direct executor would run it on the client thread and stall.
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config,
                        new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                                Duration.ZERO),
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES, downstream,
                        connections, null))
                .build()
                .start());
        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        channels.add(channel);
        return ConfluenceServiceGrpc.newBlockingStub(channel);
    }
}
