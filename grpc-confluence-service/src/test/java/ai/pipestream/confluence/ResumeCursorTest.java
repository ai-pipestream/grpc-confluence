package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The resume cursor is where the next pass starts looking, so it has to be a
 * time before this pass listed anything. A cursor taken from the newest
 * modification time the pass happened to see sits after edits made while the
 * pass was running, and the next pass, which stops at the cursor, never
 * reaches them.
 */
class ResumeCursorTest {

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
        for (String page : List.of("201", "202", "203")) {
            for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                    "properties")) {
                fake.stub("/wiki/api/v2/pages/" + page + "/" + sub, ConfluenceFixtures.emptyListJson());
            }
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
    void anEditMadeWhileAFullCrawlRanReachesTheNextIncrementalPass() throws Exception {
        Instant start = now();
        // Page 201 is listed first. While the crawl goes on, 201 is edited
        // (start + 20 min) and then 202 is edited (start + 40 min); 202 is
        // listed after its edit, so the crawl sees a modification time that
        // is later than 201's unseen edit.
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("201", "Runbook", start.minus(1, ChronoUnit.HOURS)),
                page("202", "Roadmap", start.plus(40, ChronoUnit.MINUTES))));

        crawler().crawl();
        String cursor = sink.snapshots().get(0).getCursor();

        // The next pass walks newest-first and stops at the cursor.
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("202", "Roadmap", start.plus(40, ChronoUnit.MINUTES)),
                page("201", "Runbook, edited", start.plus(20, ChronoUnit.MINUTES))));
        sink.clear();
        crawler().crawlIncremental(cursor);

        assertThat(pageTitles(sink.changes())).contains("Runbook, edited");
        assertThat(Instant.parse(cursor))
                .as("the cursor of a pass that began at %s", start)
                .isBeforeOrEqualTo(start);
    }

    @Test
    void anEditMadeWhileAnIncrementalPassRanReachesTheNextOne() throws Exception {
        Instant start = now();
        String since = start.minus(1, ChronoUnit.DAYS).toString();
        // The pass lists 202, edited at start + 40 min. Page 201's edit at
        // start + 20 min lands after the pass has walked past the top of the
        // list, so this pass does not see it.
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("202", "Roadmap", start.plus(40, ChronoUnit.MINUTES)),
                page("203", "Old notes", start.minus(2, ChronoUnit.DAYS))));

        String cursor = crawler().crawlIncremental(since);

        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("202", "Roadmap", start.plus(40, ChronoUnit.MINUTES)),
                page("201", "Runbook, edited", start.plus(20, ChronoUnit.MINUTES)),
                page("203", "Old notes", start.minus(2, ChronoUnit.DAYS))));
        sink.clear();
        crawler().crawlIncremental(cursor);

        assertThat(pageTitles(sink.changes())).contains("Runbook, edited");
        assertThat(Instant.parse(cursor))
                .as("the cursor of a pass that began at %s", start)
                .isBeforeOrEqualTo(start);
    }

    @Test
    void aPassThatSeesNothingNewDoesNotMoveTheCursorPastItsOwnStart() throws Exception {
        Instant start = now();
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("203", "Old notes", start.minus(2, ChronoUnit.DAYS))));

        String cursor = crawler().crawlIncremental(start.minus(1, ChronoUnit.DAYS).toString());

        // The cursor still moves up to this pass, so the next one does not
        // walk the same day again, but never past the moment this pass began.
        assertThat(Instant.parse(cursor))
                .isAfter(start.minus(1, ChronoUnit.HOURS))
                .isBeforeOrEqualTo(start);
    }

    @Test
    void syncHandsOutACursorThatTheNextSyncCanResumeFromWithoutLosingAnEdit() throws Exception {
        Instant start = now();
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("201", "Runbook", start.minus(1, ChronoUnit.HOURS)),
                page("202", "Roadmap", start.plus(40, ChronoUnit.MINUTES))));
        ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub = startService();

        List<SyncResponse> full = new ArrayList<>();
        stub.sync(SyncRequest.getDefaultInstance()).forEachRemaining(full::add);
        assertThat(full.getLast().getEventCase()).isEqualTo(SyncResponse.EventCase.RESUME_CURSOR);
        String cursor = full.getLast().getResumeCursor();

        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                page("202", "Roadmap", start.plus(40, ChronoUnit.MINUTES)),
                page("201", "Runbook, edited", start.plus(20, ChronoUnit.MINUTES))));
        List<ConfluenceChange> next = new ArrayList<>();
        stub.sync(SyncRequest.newBuilder().setSinceCursor(cursor).setIncludeBodies(true).build())
                .forEachRemaining(r -> {
                    if (r.hasChange()) {
                        next.add(r.getChange());
                    }
                });

        assertThat(pageTitles(next)).contains("Runbook, edited");
        assertThat(Instant.parse(cursor)).isBeforeOrEqualTo(start);
        // The snapshot of the sweep carries the same cursor.
        assertThat(full.stream().filter(SyncResponse::hasSnapshot)
                .map(r -> r.getSnapshot().getCursor())).containsExactly(cursor);
    }

    /** Confluence timestamps carry milliseconds. */
    private static Instant now() {
        return Instant.now().truncatedTo(ChronoUnit.MILLIS);
    }

    private static String page(String id, String title, Instant modified) {
        return ConfluenceFixtures.pageJson(id, "100", title, modified.toString());
    }

    private static List<String> pageTitles(List<ConfluenceChange> changes) {
        return changes.stream()
                .filter(c -> c.getEntity().hasPage())
                .map(c -> c.getEntity().getPage().getTitle())
                .toList();
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
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        return ConfluenceServiceGrpc.newBlockingStub(channel);
    }
}
