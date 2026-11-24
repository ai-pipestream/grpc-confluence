package ai.pipestream.connect;

import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import com.google.protobuf.Timestamp;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class ConfluenceSourceConnectorsTest {

    private Server server;
    private HttpServer confluence;

    @AfterEach
    void stop() {
        if (server != null) {
            server.shutdownNow();
        }
        if (confluence != null) {
            confluence.stop(0);
        }
    }

    @Test
    void confluenceConnectorMetadata() {
        ConfluenceSourceConnector connector = new ConfluenceSourceConnector();
        connector.start(Map.of("topic", "confluence.changes"));
        assertThat(connector.version()).isEqualTo(ConnectVersion.VALUE);
        assertThat(connector.taskClass()).isEqualTo(ConfluenceSourceTask.class);
        assertThat(connector.taskConfigs(3)).hasSize(1);
        assertThat(connector.config().configKeys()).containsKey(ConfluenceSourceConnector.TOPIC);
        connector.stop();
    }

    @Test
    void confluenceGrpcPollEmitsProtobufBytes() throws Exception {
        server = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
                .addService(new ConfluenceServiceGrpc.ConfluenceServiceImplBase() {
                    @Override
                    public void sync(SyncRequest request, StreamObserver<SyncResponse> observer) {
                        observer.onNext(SyncResponse.newBuilder()
                                .setChange(ConfluenceChange.newBuilder()
                                        .setChangeId("c1")
                                        .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                                        .setEntity(ConfluenceEntity.newBuilder()
                                                .setEntityId("page-1")
                                                .setIngestedAt(Timestamp.newBuilder().setSeconds(1))
                                                .setPage(Page.newBuilder()
                                                        .setId("page-1")
                                                        .setSpaceId("100")
                                                        .setTitle("Design"))))
                                .build());
                        observer.onNext(SyncResponse.newBuilder().setResumeCursor("2024-03-02T00:00:00Z")
                                .build());
                        observer.onCompleted();
                    }
                })
                .build()
                .start();

        ConfluenceSourceTask task = new ConfluenceSourceTask();
        task.initialize(ConnectTestContexts.empty());
        task.start(Map.of(
                ConfluenceSourceConnector.TOPIC, "conf.changes",
                ConfluenceSourceConnector.GRPC_TARGET, "127.0.0.1:" + server.getPort(),
                ConfluenceSourceConnector.GRPC_PLAINTEXT, "true"));
        List<SourceRecord> records = task.poll();
        task.stop();

        assertThat(records).hasSize(1);
        ConfluenceChange parsed = ConfluenceChange.parseFrom((byte[]) records.get(0).value());
        assertThat(parsed.getEntity().getPage().getTitle()).isEqualTo("Design");
    }

    @Test
    void directModeOffsetIsFromBeforeTheCrawlSoAnEditMadeDuringItIsPolledNext() throws Exception {
        Instant start = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        AtomicReference<String> pages = new AtomicReference<>(
                pageList(page("201", "Runbook", start.minus(1, ChronoUnit.HOURS))));
        startConfluence(pages);
        Map<String, String> props = Map.of(
                ConfluenceSourceConnector.TOPIC, "conf.changes",
                ConfluenceSourceConnector.BASE_URL,
                "http://127.0.0.1:" + confluence.getAddress().getPort() + "/wiki",
                ConfluenceSourceConnector.EMAIL, "bot@pipestream.ai",
                ConfluenceSourceConnector.API_TOKEN, "token-123");

        ConfluenceSourceTask first = new ConfluenceSourceTask();
        first.initialize(ConnectTestContexts.empty());
        first.start(props);
        List<SourceRecord> crawled = first.poll();
        first.stop();
        assertThat(titles(crawled)).containsExactly("Runbook");
        String offset = (String) crawled.getLast().sourceOffset().get(ChangeSourceTask.CURSOR);

        // The page was edited right after the crawl began, once the crawl
        // had listed it. The next poll walks newest-first down to the offset.
        pages.set(pageList(page("201", "Runbook, edited", start.plusMillis(1))));
        ConfluenceSourceTask second = new ConfluenceSourceTask();
        second.initialize(ConnectTestContexts.withOffset(Map.of(ChangeSourceTask.CURSOR, offset)));
        second.start(props);
        List<SourceRecord> polled = second.poll();
        second.stop();

        assertThat(titles(polled)).containsExactly("Runbook, edited");
        assertThat(Instant.parse(offset)).isBeforeOrEqualTo(start);
    }

    private static List<String> titles(List<SourceRecord> records) throws Exception {
        List<String> titles = new java.util.ArrayList<>();
        for (SourceRecord record : records) {
            ConfluenceChange change = ConfluenceChange.parseFrom((byte[]) record.value());
            if (change.getEntity().hasPage()) {
                titles.add(change.getEntity().getPage().getTitle());
            }
        }
        return titles;
    }

    /**
     * A Confluence with one space: its page list is {@code pages}, every
     * other content list is empty, and the security routes are not served.
     */
    private void startConfluence(AtomicReference<String> pages) throws Exception {
        confluence = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        confluence.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            int status = 200;
            String body;
            if (path.equals("/wiki/api/v2/spaces")) {
                body = """
                        {"results": [{"id": "100", "key": "ENG", "name": "Engineering",
                          "type": "global", "status": "current",
                          "createdAt": "2024-01-02T03:04:05.000Z",
                          "_links": {"webui": "/spaces/ENG"}}]}""";
            } else if (path.equals("/wiki/api/v2/pages")) {
                body = pages.get();
            } else if (path.equals("/wiki/api/v2/blogposts")
                    || path.equals("/wiki/api/v2/spaces/100/properties")
                    || path.matches("/wiki/api/v2/pages/201/"
                            + "(footer-comments|inline-comments|attachments|labels|properties)")) {
                body = "{\"results\": []}";
            } else {
                status = 404;
                body = "{\"message\": \"not served\"}";
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            try (var out = exchange.getResponseBody()) {
                out.write(bytes);
            }
        });
        confluence.start();
    }

    private static String pageList(String... pages) {
        return "{\"results\": [" + String.join(",", pages) + "]}";
    }

    private static String page(String id, String title, Instant modified) {
        return """
                {"id": "%s", "status": "current", "title": "%s", "spaceId": "100",
                 "parentType": "page", "authorId": "acc-1",
                 "createdAt": "2024-02-01T10:00:00.000Z",
                 "version": {"createdAt": "%s", "message": "edit", "number": 3,
                             "minorEdit": false, "authorId": "acc-2"},
                 "body": {"storage": {"representation": "storage", "value": "<p>Hello</p>"}},
                 "_links": {"webui": "/spaces/ENG/pages/%s"}}""".formatted(id, title, modified, id);
    }
}
