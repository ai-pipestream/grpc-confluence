package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceEvent;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs {@link ChangeScenarios} against the real Confluence site and the
 * deployed Forge app's event queue, writes {@code build/live-changes.md},
 * and records every REST and queue exchange, scrubbed, under
 * {@code src/test/resources/recordings/confluence-changes} for
 * {@link ConfluenceChangeReplayTest}.
 *
 * <p>Gated on the live credentials, {@code CONFLUENCE_LIVE_CHANGES=1} and
 * {@link ForgeEventQueue#ENV_URL} / {@link ForgeEventQueue#ENV_SECRET}. The
 * space is {@code CONFLUENCE_SPACE_KEY} (default {@code CLAUDEGRPC}). Run
 * with {@code ./gradlew :grpc-confluence-service:liveChangesTest}.</p>
 */
class ConfluenceLiveChangeIT {

    /** Where the recording lives, relative to the module directory. */
    static final String DEFAULT_RECORDINGS = "src/test/resources/recordings/confluence-changes";
    private static final String DEFAULT_BASE_URL = "https://pipestreamai.atlassian.net/wiki";
    private static final String TAG = "[ConfluenceLiveChangeIT] ";

    private static String env(String name) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? null : value.strip();
    }

    private static String credential(String canonical, String alias) {
        String value = env(canonical);
        return value != null ? value : env(alias);
    }

    @Test
    void everyChangeStepAgainstTheLiveSite() throws Exception {
        String email = credential(ConfluenceConnectorConfig.ENV_EMAIL,
                ConfluenceConnectorConfig.ENV_EMAIL_ALIAS);
        String token = credential(ConfluenceConnectorConfig.ENV_API_TOKEN,
                ConfluenceConnectorConfig.ENV_API_TOKEN_ALIAS);
        String queueUrl = env(ForgeEventQueue.ENV_URL);
        String queueSecret = env(ForgeEventQueue.ENV_SECRET);
        assumeTrue(email != null && token != null, "no live Confluence credentials; skipping");
        assumeTrue("1".equals(env("CONFLUENCE_LIVE_CHANGES")),
                "CONFLUENCE_LIVE_CHANGES=1 is not set; skipping the live change run");
        assumeTrue(queueUrl != null && queueSecret != null, "no Forge event queue configured; skipping");
        String baseUrl = env(ConfluenceConnectorConfig.ENV_BASE_URL);
        if (baseUrl == null) {
            baseUrl = DEFAULT_BASE_URL;
        }
        String spaceKey = env("CONFLUENCE_SPACE_KEY");
        if (spaceKey == null) {
            spaceKey = "CLAUDEGRPC";
        }
        String run = "pipestream-live-change-"
                + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)
                        .format(Instant.now())
                + "-" + UUID.randomUUID().toString().substring(0, 8);
        Path out = Path.of(System.getProperty("liveChanges.report", "build/live-changes.md"));
        Path recordings = Path.of(System.getProperty("liveChanges.recordings", DEFAULT_RECORDINGS));

        URI queue = URI.create(queueUrl);
        RecordingScrubber scrubber = new RecordingScrubber();
        scrubber.site(URI.create(baseUrl).getAuthority());
        scrubber.site(queue.getAuthority());
        for (String segment : queue.getPath().split("/")) {
            if (segment.length() >= 16) {
                scrubber.secret(segment);
            }
        }
        scrubber.email(email);
        scrubber.secret(token);
        scrubber.secret(queueSecret);
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
        RecordingTape tape = new RecordingTape(http, recordings, scrubber);

        // Peeks at the queue off the recording, so a step can wait for Forge.
        ForgeEventQueue peek = new ForgeEventQueue(queueUrl, queueSecret, http);
        purge(peek);
        ChangeScenarios.Events events = new ChangeScenarios.Events() {
            @Override
            public void await(Predicate<ConfluenceEvent> match) throws Exception {
                ConfluenceLiveChangeIT.await(peek, match);
            }

            @Override
            public void awaitQueued(int count) throws Exception {
                ConfluenceLiveChangeIT.awaitQueued(peek, count);
            }

            @Override
            public void pause(Duration time) throws InterruptedException {
                TimeUnit.MILLISECONDS.sleep(time.toMillis());
            }
        };

        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(baseUrl).email(email).apiToken(token).pageSize(25).build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ofMillis(100), tape);
        ConfluenceGrpcService read = new ConfluenceGrpcService(config, client,
                ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES);
        ConfluenceChangeGrpcService service = new ConfluenceChangeGrpcService(read,
                new ForgeEventQueue(queueUrl, queueSecret, tape), Clock.systemUTC());
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(read).addService(service).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        ChangeScenarios scenarios = new ChangeScenarios(channel, client, service, spaceKey, run, events);
        Instant started = Instant.now();
        try {
            for (ChangeScenarios.Scenario scenario : scenarios.all()) {
                tape.scenario(scenario.id());
                scenarios.run(scenario);
                System.out.println(TAG + scenario.label() + ": " + scenarios.rows().getLast().outcome()
                        + " " + scenarios.rows().getLast().detail());
            }
        } finally {
            tape.scenario("cleanup");
            scenarios.cleanUp();
            channel.shutdownNow();
            server.shutdownNow();
            server.awaitTermination(10, TimeUnit.SECONDS);
        }
        String report = report(scenarios, started);
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, report, StandardCharsets.UTF_8);
        System.out.println(report);

        ObjectNode runFile = RecordedExchange.JSON.createObjectNode();
        runFile.put("baseUrl", config.baseUrl());
        runFile.put("queueUrl", queueUrl);
        runFile.put("spaceKey", spaceKey);
        runFile.put("run", run);
        scenarios.rows().forEach(row -> runFile.withArray("outcomes").addObject()
                .put("step", row.id()).put("outcome", row.outcome()));
        tape.finish(runFile);
        System.out.println(TAG + "recording written to " + recordings.toAbsolutePath());

        List<String> failures = new ArrayList<>();
        scenarios.rows().stream().filter(row -> row.outcome().equals("FAIL"))
                .forEach(row -> failures.add(row.label() + ": " + row.detail()));
        assertThat(failures).as("failed change steps (see %s)", out).isEmpty();
    }

    /** Acknowledges everything already queued, so the run starts from an empty queue. */
    private static void purge(ForgeEventQueue queue) throws Exception {
        List<ForgeEventQueue.Queued> batch;
        while (!(batch = queue.take(ForgeEventQueue.DEFAULT_TAKE)).isEmpty()) {
            queue.ack(batch.stream().map(ForgeEventQueue.Queued::key).toList());
        }
    }

    private static void await(ForgeEventQueue queue, Predicate<ConfluenceEvent> match)
            throws Exception {
        Instant until = Instant.now().plus(ChangeScenarios.EVENT_WAIT);
        while (Instant.now().isBefore(until)) {
            for (ForgeEventQueue.Queued queued : queue.take(ForgeEventQueue.DEFAULT_TAKE)) {
                if (match.test(queued.event())) {
                    return;
                }
            }
            TimeUnit.SECONDS.sleep(5);
        }
        throw new AssertionError("Forge queued no matching event within "
                + ChangeScenarios.EVENT_WAIT);
    }

    private static void awaitQueued(ForgeEventQueue queue, int count) throws Exception {
        Instant until = Instant.now().plus(ChangeScenarios.EVENT_WAIT);
        int queued = 0;
        while (Instant.now().isBefore(until)) {
            queued = queue.take(ForgeEventQueue.DEFAULT_TAKE).size();
            if (queued >= count) {
                return;
            }
            TimeUnit.SECONDS.sleep(5);
        }
        throw new AssertionError("only " + queued + " events were queued within "
                + ChangeScenarios.EVENT_WAIT);
    }

    private static String report(ChangeScenarios scenarios, Instant started) {
        StringBuilder out = new StringBuilder("# Confluence live change run\n\n- Run: ")
                .append(started).append(" (")
                .append(Duration.between(started, Instant.now()).toSeconds()).append(" s)\n\n")
                .append("| step | outcome | detail |\n|---|---|---|\n");
        scenarios.rows().forEach(row -> out.append("| ").append(row.label()).append(" | ")
                .append(row.outcome().equals("FAIL") ? "**FAIL**" : row.outcome())
                .append(" | ").append(row.detail().replace("|", "\\|").replace("\n", " "))
                .append(" |\n"));
        return out.toString();
    }
}
