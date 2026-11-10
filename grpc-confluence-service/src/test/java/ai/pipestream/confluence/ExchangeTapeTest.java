package ai.pipestream.confluence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Records against the in-process fake and replays the recording: the
 * replay answers the same calls without a server, holds no real value, and
 * refuses a request that differs from the recorded one.
 */
class ExchangeTapeTest {

    private static final String ACCOUNT = "712020:0a1b2c3d-1111-4222-8333-444455556666";
    private static final String EMAIL = "bot@pipestream.ai";
    private static final String TOKEN = "ATATT3xFfGF0T4JNjabcdefGHIJ1234567890";
    private static final byte[] FILE = "attachment bytes".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path recordings;
    private FakeConfluenceServer fake;

    @BeforeEach
    void start() throws IOException {
        fake = FakeConfluenceServer.start();
    }

    @AfterEach
    void stop() {
        fake.close();
    }

    private RecordingTape recording() throws IOException {
        RecordingScrubber scrubber = new RecordingScrubber();
        scrubber.site(java.net.URI.create(fake.baseUrl()).getAuthority());
        scrubber.email(EMAIL);
        scrubber.secret(TOKEN);
        return new RecordingTape(HttpClient.newHttpClient(), recordings, scrubber);
    }

    private static ObjectNode run(String baseUrl) {
        return RecordedExchange.JSON.createObjectNode().put("baseUrl", baseUrl);
    }

    /** The calls both the recording and the replay make; answers what they returned. */
    private static List<String> calls(ConfluenceClient client, ExchangeTape tape) throws Exception {
        List<String> seen = new ArrayList<>();
        tape.scenario("pages");
        JsonNode page = client.post("/api/v2/pages", RecordedExchange.JSON.readTree(
                "{\"title\":\"hello\",\"exact\":1.50}"));
        seen.add(page.toString());
        String author = page.path("authorId").asText();
        seen.add(client.get("/api/v2/users/" + author, Map.of("b", "2", "a", author)).toString());
        assertThatThrownBy(() -> client.get("/api/v2/pages/404"))
                .isInstanceOf(ConfluenceClient.ConfluenceApiException.class);
        seen.add(client.get("/api/v2/throttled").toString());
        tape.scenario("attachments");
        seen.add(client.uploadMultipart("POST", "/rest/api/content/100/child/attachment",
                Map.of("comment", "a comment"), "a.bin", "application/octet-stream",
                new ByteArrayInputStream(FILE)).toString());
        seen.add(new String(client.downloadAttachmentBytes("/download/a.bin"), StandardCharsets.UTF_8));
        tape.unordered(true);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var left = executor.submit(() -> client.get("/api/v2/left").toString());
            var right = executor.submit(() -> client.get("/api/v2/right").toString());
            seen.add(left.get());
            seen.add(right.get());
        } finally {
            tape.unordered(false);
        }
        return seen;
    }

    private void stubCalls() {
        fake.stub("POST", "/wiki/api/v2/pages", 200, "{\"id\":\"100\",\"authorId\":\"" + ACCOUNT
                + "\",\"exact\":1.50,\"version\":{\"authorId\":\"5b10a2844c20165700ede21a\"},"
                + "\"_links\":{\"base\":\"https://acme.atlassian.net/wiki\"}}");
        fake.stub("/wiki/api/v2/users/" + ACCOUNT, "{\"accountId\":\"" + ACCOUNT
                + "\",\"displayName\":\"Ada Lovelace\",\"email\":\"ada@acme.example\","
                + "\"profilePicture\":{\"path\":\"/wiki/aa-avatar/" + ACCOUNT + "\"},"
                + "\"bio\":\"Ada Lovelace wrote this\"}");
        fake.stubOnce("/wiki/api/v2/throttled",
                new FakeConfluenceServer.Stub(429, "slow down", Map.of("Retry-After", "0")));
        fake.stub("/wiki/api/v2/throttled", "{\"ok\":true}");
        fake.stub("/wiki/rest/api/content/100/child/attachment", "{\"results\":[{\"id\":\"att1\"}]}");
        fake.stub("/wiki/download/a.bin", new FakeConfluenceServer.Stub(302, "",
                Map.of("Location", "/wiki/media/a?token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJl")));
        fake.stub("/wiki/media/a", new FakeConfluenceServer.Stub(200, new String(FILE, StandardCharsets.UTF_8),
                Map.of("Content-Type", "application/octet-stream")));
        fake.stub("/wiki/api/v2/left", "{\"side\":\"left\"}");
        fake.stub("/wiki/api/v2/right", "{\"side\":\"right\"}");
    }

    @Test
    void clientCallsReplayFromTheirRecording() throws Exception {
        stubCalls();
        RecordingTape recording = recording();
        List<String> live = calls(new ConfluenceClient(fake.baseUrl(), EMAIL, TOKEN, Duration.ZERO,
                recording), recording);
        recording.finish(run(fake.baseUrl()));
        fake.close();

        assertThat(live.get(0)).contains(ACCOUNT).contains("acme.atlassian.net");
        try (Stream<Path> files = Files.walk(recordings)) {
            List<Path> saved = files.filter(Files::isRegularFile).sorted().toList();
            assertThat(saved).extracting(file -> recordings.relativize(file).toString()).contains(
                    "pages/001-POST-api-v2-pages.json", "pages/002-GET-api-v2-users-id.json",
                    "pages/004-GET-api-v2-throttled.json", "pages/005-GET-api-v2-throttled.json",
                    "attachments/001-POST-rest-api-content-id-child-attachment.json",
                    "attachments/002-GET-download-id.json", "run.json");
            for (Path file : saved) {
                String text = Files.readString(file);
                assertThat(RecordingScrubber.leaks(text)).as(file.toString()).isEmpty();
                assertThat(text).doesNotContain("acme", "pipestream.ai", "Ada", "Lovelace", "127.0.0.1",
                        "aa-avatar", TOKEN, "eyJ");
            }
        }

        ReplayTape replay = new ReplayTape(recordings);
        ConfluenceClient client = new ConfluenceClient(replay.run().path("baseUrl").asText(),
                "replay@example.invalid", "replay", Duration.ZERO, replay);
        List<String> replayed = calls(client, replay);
        replay.finish();
        assertThat(replay.violations()).isEmpty();
        assertThat(replayed).hasSameSizeAs(live);
        JsonNode page = RecordedExchange.JSON.readTree(replayed.get(0));
        assertThat(page.path("authorId").asText()).isEqualTo("account-1");
        assertThat(page.path("exact").toString()).isEqualTo("1.50");
        assertThat(page.path("_links").path("base").asText()).isEqualTo("https://site-2.example.invalid/wiki");
        JsonNode user = RecordedExchange.JSON.readTree(replayed.get(1));
        assertThat(user.path("displayName").asText()).isEqualTo("User 1");
        assertThat(user.path("bio").asText()).isEqualTo("User 1 wrote this");
        assertThat(replayed.get(3)).isEqualTo(live.get(3));
        assertThat(replayed.get(4)).isEqualTo(new String(FILE, StandardCharsets.UTF_8));
        assertThat(replayed.subList(5, 7)).isEqualTo(live.subList(5, 7));
    }

    @Test
    void aRequestThatDiffersFromTheRecordingIsRefused() throws Exception {
        stubCalls();
        RecordingTape recording = recording();
        ConfluenceClient live = new ConfluenceClient(fake.baseUrl(), EMAIL, TOKEN, Duration.ZERO,
                recording);
        recording.scenario("pages");
        live.post("/api/v2/pages", RecordedExchange.JSON.readTree("{\"title\":\"hello\"}"));
        live.get("/api/v2/left");
        recording.finish(run(fake.baseUrl()));

        ReplayTape replay = new ReplayTape(recordings);
        ConfluenceClient client = new ConfluenceClient(replay.run().path("baseUrl").asText(),
                "replay@example.invalid", "replay", Duration.ZERO, replay);
        replay.scenario("pages");
        assertThatThrownBy(() -> client.post("/api/v2/pages",
                RecordedExchange.JSON.readTree("{\"title\":\"other\"}")))
                .isInstanceOf(IOException.class).hasMessageContaining("body");
        assertThatThrownBy(() -> client.get("/api/v2/left"))
                .isInstanceOf(IOException.class).hasMessageContaining("001-POST-api-v2-pages.json");
        client.post("/api/v2/pages", RecordedExchange.JSON.readTree("{\"title\":\"hello\"}"));
        replay.finish();
        assertThat(replay.violations()).hasSize(3).last().asString()
                .contains("recorded but never requested: 002-GET-api-v2-left.json");
    }

    @Test
    void theScenariosReplayFromTheirRecording() throws Exception {
        fake.stub("/wiki/api/v2/spaces", "{\"results\":[{\"id\":\"7\",\"key\":\"DEMO\",\"name\":\"Demo\","
                + "\"authorId\":\"" + ACCOUNT + "\"}]}");
        fake.stub("/wiki/api/v2/spaces/7", "{\"id\":\"7\",\"key\":\"DEMO\",\"name\":\"Demo\"}");
        String page = "{\"id\":\"100\",\"title\":\"a page\",\"spaceId\":\"7\",\"authorId\":\"" + ACCOUNT
                + "\",\"status\":\"current\",\"version\":{\"number\":1},"
                + "\"body\":{\"storage\":{\"representation\":\"storage\",\"value\":\"<p>a page</p>\"}}}";
        fake.stub("POST", "/wiki/api/v2/pages", 200, page);
        fake.stub("GET", "/wiki/api/v2/pages/100", 200, page);
        RecordingTape recording = recording();
        WriteScenarios.Backend backend = new WriteScenarios.Backend("DEMO", "pipestream-live-write-test",
                true, "ac:demo:thing", "invitee@acme.example", List.of(TOKEN, EMAIL), recording);
        List<WriteScenarios.Row> live = runScenarios(fake.baseUrl(), recording, backend, true);
        fake.close();

        ReplayTape replay = new ReplayTape(recordings);
        JsonNode run = replay.run();
        assertThat(run.path("inviteEmail").asText()).endsWith("@example.invalid");
        List<WriteScenarios.Row> replayed = runScenarios(run.path("baseUrl").asText(), replay,
                ConfluenceWriteReplayTest.backend(replay, run), false);
        assertThat(replay.violations()).isEmpty();
        assertThat(replayed).extracting(WriteScenarios.Row::rpc, WriteScenarios.Row::label,
                WriteScenarios.Row::outcome).isEqualTo(live.stream().map(row ->
                org.assertj.core.groups.Tuple.tuple(row.rpc(), row.label(), row.outcome())).toList());
        assertThat(live).extracting(WriteScenarios.Row::rpc)
                .containsAll(ConfluenceWriteRoutes.rpcs());
    }

    private List<WriteScenarios.Row> runScenarios(String baseUrl, ExchangeTape tape,
            WriteScenarios.Backend backend, boolean record) throws Exception {
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder().baseUrl(baseUrl)
                .email(record ? EMAIL : "replay@example.invalid").apiToken(record ? TOKEN : "replay")
                .pageSize(25).build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ZERO, tape);
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, client,
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        try {
            WriteScenarios scenarios = new WriteScenarios(channel, backend);
            assertThat(scenarios.scenarios()).extracting(WriteScenarios.Scenario::name)
                    .isEqualTo(WriteScenarios.names());
            for (WriteScenarios.Scenario scenario : scenarios.scenarios()) {
                tape.scenario(scenario.name());
                scenario.body().run();
            }
            if (tape instanceof RecordingTape recording) {
                recording.finish(ConfluenceLiveWriteIT.runFile(baseUrl, backend, scenarios));
            } else if (tape instanceof ReplayTape replay) {
                replay.finish();
            }
            return scenarios.rows();
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
        }
    }
}
