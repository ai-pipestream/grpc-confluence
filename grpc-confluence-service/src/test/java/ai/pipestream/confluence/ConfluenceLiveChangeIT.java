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
 * space is {@code CONFLUENCE_SPACE_KEY} (default {@code CLAUDEGRPC}).
 * {@link ChangeScenarios#ENV_GROUP_MEMBER}{@code =1} also adds the caller to
 * the run's throwaway group and removes it. Run with
 * {@code ./gradlew :grpc-confluence-service:liveChangesTest}.</p>
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
        String run = ChangeScenarios.RUN_PREFIX
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
        List<Leftover> left = purge(peek);
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
        ForgeEventQueue recorded = new ForgeEventQueue(queueUrl, queueSecret, tape);
        ConfluenceChangeGrpcService service = new ConfluenceChangeGrpcService(read, recorded,
                Clock.systemUTC());
        ConfluenceChangeGrpcService second = new ConfluenceChangeGrpcService(read, recorded,
                Clock.systemUTC());
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(read).addService(service).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        boolean memberSteps = "1".equals(env(ChangeScenarios.ENV_GROUP_MEMBER));
        ChangeScenarios scenarios = new ChangeScenarios(channel, service, spaceKey, run, memberSteps,
                events, recorded, second);
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
        String report = report(scenarios, started, left);
        List<String> overdue = left.stream().filter(Leftover::overdue).map(Leftover::line).toList();
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, report, StandardCharsets.UTF_8);
        System.out.println(report);

        ObjectNode runFile = RecordedExchange.JSON.createObjectNode();
        runFile.put("baseUrl", config.baseUrl());
        runFile.put("queueUrl", queueUrl);
        runFile.put("spaceKey", spaceKey);
        runFile.put("run", run);
        runFile.put("groupMembers", memberSteps);
        scenarios.rows().forEach(row -> runFile.withArray("outcomes").addObject()
                .put("step", row.id()).put("outcome", row.outcome()));
        tape.finish(runFile);
        System.out.println(TAG + "recording written to " + recordings.toAbsolutePath());

        List<String> failures = new ArrayList<>();
        scenarios.rows().stream().filter(row -> row.outcome().equals("FAIL"))
                .forEach(row -> failures.add(row.label() + ": " + row.detail()));
        assertThat(failures).as("failed change steps (see %s)", out).isEmpty();
        assertThat(overdue).as("events still queued more than %s after they were stored",
                ChangeScenarios.EVENT_TTL.plus(ChangeScenarios.EXPIRY_LAG)).isEmpty();
    }

    /**
     * One event that was still queued when the run started.
     *
     * @param line its type and when it happened
     * @param age how long ago the Forge app stored it; {@code null} when unknown
     */
    private record Leftover(String line, Duration age) {

        /** Whether Forge should have dropped the event by now. */
        boolean overdue() {
            return age != null
                    && age.compareTo(ChangeScenarios.EVENT_TTL.plus(ChangeScenarios.EXPIRY_LAG)) > 0;
        }
    }

    /**
     * Acknowledges everything already queued, so the run starts from an empty queue.
     *
     * @return the events that were still queued
     */
    private static List<Leftover> purge(ForgeEventQueue queue) throws Exception {
        List<Leftover> left = new ArrayList<>();
        Instant now = Instant.now();
        List<ForgeEventQueue.Queued> batch;
        while (!(batch = queue.take(ForgeEventQueue.DEFAULT_TAKE)).isEmpty()) {
            for (ForgeEventQueue.Queued queued : batch) {
                ConfluenceEvent event = queued.event();
                Duration delay = ChangeScenarios.delay(queued);
                Instant stored = ChangeScenarios.stored(queued);
                left.add(new Leftover(event.getEventType() + (event.hasOccurredAt() ? " of "
                        + Instant.ofEpochSecond(event.getOccurredAt().getSeconds()) : "")
                        + (delay == null ? "" : ", queued " + delay.toSeconds() + " s after it happened"),
                        stored == null ? null : Duration.between(stored, now)));
            }
            queue.ack(batch.stream().map(ForgeEventQueue.Queued::key).toList());
        }
        return left;
    }

    private static void await(ForgeEventQueue queue, Predicate<ConfluenceEvent> match)
            throws Exception {
        Instant until = Instant.now().plus(ChangeScenarios.EVENT_WAIT);
        java.util.Set<String> seen = new java.util.TreeSet<>();
        while (Instant.now().isBefore(until)) {
            for (ForgeEventQueue.Queued queued : queue.take(ForgeEventQueue.DEFAULT_TAKE)) {
                if (match.test(queued.event())) {
                    return;
                }
                seen.add(queued.event().getEventType());
            }
            TimeUnit.SECONDS.sleep(15);
        }
        throw new AssertionError("Forge queued no matching event within "
                + ChangeScenarios.EVENT_WAIT + "; queued types seen " + seen);
    }

    private static void awaitQueued(ForgeEventQueue queue, int count) throws Exception {
        Instant until = Instant.now().plus(ChangeScenarios.EVENT_WAIT);
        int queued = 0;
        while (Instant.now().isBefore(until)) {
            queued = queue.take(ForgeEventQueue.DEFAULT_TAKE).size();
            if (queued >= count) {
                return;
            }
            TimeUnit.SECONDS.sleep(15);
        }
        throw new AssertionError("only " + queued + " events were queued within "
                + ChangeScenarios.EVENT_WAIT);
    }

    private static String report(ChangeScenarios scenarios, Instant started, List<Leftover> left) {
        StringBuilder out = new StringBuilder("# Confluence live change run\n\n- Run: ")
                .append(started).append(" (")
                .append(Duration.between(started, Instant.now()).toSeconds()).append(" s)\n\n")
                .append("| step | outcome | detail |\n|---|---|---|\n");
        scenarios.rows().forEach(row -> out.append("| ").append(row.label()).append(" | ")
                .append(row.outcome().equals("FAIL") ? "**FAIL**" : row.outcome())
                .append(" | ").append(row.detail().replace("|", "\\|").replace("\n", " "))
                .append(" |\n"));
        out.append("\n## Forge delivery\n\nEvents the run drained, by type:\n\n");
        scenarios.deliveries().forEach(line -> out.append("- ").append(line).append('\n'));
        out.append("\nEvents still queued when the run started (queued after the run before ended): ")
                .append(left.size()).append("\n\n");
        left.forEach(event -> out.append("- ").append(event.line()).append('\n'));
        Duration oldest = left.stream().map(Leftover::age).filter(java.util.Objects::nonNull)
                .max(Duration::compareTo).orElse(null);
        out.append("\nExpiry: Forge drops an event ").append(ChangeScenarios.EVENT_TTL.toDays())
                .append(" days after it is stored, up to ").append(ChangeScenarios.EXPIRY_LAG.toHours())
                .append(" h late. ");
        if (oldest == null) {
            out.append("Nothing was queued, so this run says nothing about it.\n");
        } else if (left.stream().anyMatch(Leftover::overdue)) {
            out.append("**FAIL**: the oldest queued event was stored ").append(oldest.toHours())
                    .append(" h ago.\n");
        } else {
            out.append("The oldest queued event was stored ").append(oldest.toHours())
                    .append(" h ago, inside that time.\n");
        }
        return out.toString();
    }
}
