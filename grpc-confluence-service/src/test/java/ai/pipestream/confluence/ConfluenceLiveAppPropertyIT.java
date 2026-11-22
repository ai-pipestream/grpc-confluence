package ai.pipestream.confluence;

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
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the app property scenario of {@link WriteScenarios} against the real
 * Confluence site through a deployed Forge app, writes one row per step to
 * {@code build/live-app-properties.md}, and records every exchange with the
 * app's trigger, scrubbed, under
 * {@code src/test/resources/recordings/confluence-app-properties} for
 * {@link ConfluenceWriteReplayTest}.
 *
 * <p>Gated on the credentials {@link ConfluenceLiveSmokeIT} uses and on
 * {@code CONFLUENCE_FORGE_APP_URL} and {@code CONFLUENCE_FORGE_APP_SECRET}.
 * Excluded from the default {@code test} task; run with
 * {@code ./gradlew :grpc-confluence-service:liveAppPropertiesTest}. The one
 * property the run sets is deleted at the end. The trigger URL and the
 * secret never reach the report, stdout or the recording.</p>
 */
class ConfluenceLiveAppPropertyIT {

    private static final String DEFAULT_BASE_URL = "https://pipestreamai.atlassian.net/wiki";
    private static final String TAG = "[ConfluenceLiveAppPropertyIT] ";

    private static String env(String name) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? null : value.strip();
    }

    private static String credential(String canonical, String alias) {
        String value = env(canonical);
        return value != null ? value : env(alias);
    }

    @Test
    void everyAppPropertyRpcThroughTheForgeApp() throws Exception {
        String email = credential(ConfluenceConnectorConfig.ENV_EMAIL,
                ConfluenceConnectorConfig.ENV_EMAIL_ALIAS);
        String token = credential(ConfluenceConnectorConfig.ENV_API_TOKEN,
                ConfluenceConnectorConfig.ENV_API_TOKEN_ALIAS);
        assumeTrue(email != null && token != null,
                "no live Confluence credentials in the environment; skipping");
        String triggerUrl = env(ConfluenceClient.ForgeApp.ENV_URL);
        String secret = env(ConfluenceClient.ForgeApp.ENV_SECRET);
        assumeTrue(triggerUrl != null && secret != null, ConfluenceClient.ForgeApp.ENV_URL + " and "
                + ConfluenceClient.ForgeApp.ENV_SECRET + " are not set; skipping the app property run");
        String baseUrl = env(ConfluenceConnectorConfig.ENV_BASE_URL);
        if (baseUrl == null) {
            baseUrl = DEFAULT_BASE_URL;
        }
        String run = "pipestream-live-app-"
                + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)
                        .format(Instant.now())
                + "-" + UUID.randomUUID().toString().substring(0, 8);
        Path out = Path.of(System.getProperty("liveAppProperties.report",
                "build/live-app-properties.md"));
        Path recordings = Path.of(System.getProperty("liveAppProperties.recordings",
                ConfluenceWriteReplayTest.APP_PROPERTY_RECORDINGS));

        RecordingScrubber scrubber = new RecordingScrubber();
        scrubber.site(URI.create(baseUrl).getAuthority());
        scrubber.email(email);
        scrubber.secret(token);
        scrubber.secret(secret);
        scrubber.forgeTrigger(triggerUrl);
        RecordingTape tape = new RecordingTape(
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build(),
                recordings, scrubber);
        URI trigger = URI.create(triggerUrl);
        WriteScenarios.Backend backend = new WriteScenarios.Backend("", run, false, null, null,
                List.of(token, email, secret, triggerUrl, trigger.getAuthority(), trigger.getRawPath()),
                tape, 0);

        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(baseUrl).email(email).apiToken(token).pageSize(25).build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ofMillis(100), tape);
        client.forgeApp(new ConfluenceClient.ForgeApp(triggerUrl, secret));
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, client,
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        WriteScenarios scenarios = WriteScenarios.appProperties(channel, backend);
        Instant started = Instant.now();
        try {
            for (WriteScenarios.Scenario scenario : scenarios.scenarios()) {
                tape.scenario(scenario.name());
                scenario.body().run();
            }
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
            server.awaitTermination(10, TimeUnit.SECONDS);
        }
        scenarios.leftBehind().forEach(line -> System.out.println(TAG + "left behind: " + line));

        String report = scenarios.report(URI.create(baseUrl).getHost(), started);
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, report, StandardCharsets.UTF_8);
        System.out.println(report);

        ObjectNode runFile = ConfluenceLiveWriteIT.runFile(config.baseUrl(), backend, scenarios);
        runFile.put("forgeAppUrl", triggerUrl);
        tape.finish(runFile);
        System.out.println(TAG + "recording written to " + recordings.toAbsolutePath());

        List<String> failures = new ArrayList<>();
        scenarios.rows().stream().filter(row -> !row.outcome().equals("PASS"))
                .forEach(row -> failures.add(row.label() + ": " + row.outcome() + " " + row.detail()));
        assertThat(failures).as("app property steps that did not pass (see %s)", out).isEmpty();
        assertThat(scenarios.leftBehind()).as("left behind").isEmpty();
    }
}
