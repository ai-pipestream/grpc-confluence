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
 * Runs {@link WriteScenarios} against the real Confluence workspace through
 * the in-process gRPC service, writes one row per rpc to
 * {@code build/live-write.md}, and records every HTTP exchange, scrubbed,
 * under {@code src/test/resources/recordings/confluence} for
 * {@link ConfluenceWriteReplayTest}.
 *
 * <p>Gated on the credentials {@link ConfluenceLiveSmokeIT} uses and on
 * {@code CONFLUENCE_LIVE_WRITE=1}; the target space is
 * {@code CONFLUENCE_SPACE_KEY} (default {@code CLAUDEGRPC}). Excluded from
 * the default {@code test} task; run with
 * {@code ./gradlew :grpc-confluence-service:liveWriteTest}.</p>
 *
 * <p>Everything is created under a fresh parent page named
 * {@code pipestream-live-write-<UTC timestamp>-<random>} and is deleted and
 * purged at the end; anything left behind is listed in the report. Tenant
 * level writes (space create, space roles, role assignments, the space
 * default classification, invitations, the admin key) run only with
 * {@code CONFLUENCE_LIVE_WRITE_ADMIN=1}; invitations also need
 * {@code CONFLUENCE_LIVE_WRITE_INVITE_EMAIL}. Custom content needs a type
 * in {@code CONFLUENCE_LIVE_WRITE_CUSTOM_CONTENT_TYPE}. The bulk permission
 * transitions and the Forge app properties are never run. Credential values
 * never reach the report, stdout or the recording.</p>
 */
class ConfluenceLiveWriteIT {

    private static final String DEFAULT_BASE_URL = "https://pipestreamai.atlassian.net/wiki";
    private static final String DEFAULT_SPACE_KEY = "CLAUDEGRPC";
    private static final String TAG = "[ConfluenceLiveWriteIT] ";

    private static String env(String name) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? null : value.strip();
    }

    private static String credential(String canonical, String alias) {
        String value = env(canonical);
        return value != null ? value : env(alias);
    }

    @Test
    void everyWriteRpcAgainstTheLiveWorkspace() throws Exception {
        String email = credential(ConfluenceConnectorConfig.ENV_EMAIL,
                ConfluenceConnectorConfig.ENV_EMAIL_ALIAS);
        String token = credential(ConfluenceConnectorConfig.ENV_API_TOKEN,
                ConfluenceConnectorConfig.ENV_API_TOKEN_ALIAS);
        assumeTrue(email != null && token != null,
                "no live Confluence credentials in the environment; skipping");
        assumeTrue("1".equals(env("CONFLUENCE_LIVE_WRITE")),
                "CONFLUENCE_LIVE_WRITE=1 is not set; skipping the live write run");
        String baseUrl = env(ConfluenceConnectorConfig.ENV_BASE_URL);
        if (baseUrl == null) {
            baseUrl = DEFAULT_BASE_URL;
        }
        String spaceKey = env("CONFLUENCE_SPACE_KEY");
        if (spaceKey == null) {
            spaceKey = DEFAULT_SPACE_KEY;
        }
        String run = "pipestream-live-write-"
                + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)
                        .format(Instant.now())
                + "-" + UUID.randomUUID().toString().substring(0, 8);
        Path out = Path.of(System.getProperty("liveWrite.report", "build/live-write.md"));
        Path recordings = Path.of(System.getProperty("liveWrite.recordings",
                ConfluenceWriteReplayTest.DEFAULT_RECORDINGS));

        RecordingScrubber scrubber = new RecordingScrubber();
        scrubber.site(URI.create(baseUrl).getAuthority());
        scrubber.email(email);
        scrubber.secret(token);
        RecordingTape tape = new RecordingTape(
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build(),
                recordings, scrubber);
        WriteScenarios.Backend backend = new WriteScenarios.Backend(spaceKey, run,
                "1".equals(env("CONFLUENCE_LIVE_WRITE_ADMIN")),
                env("CONFLUENCE_LIVE_WRITE_CUSTOM_CONTENT_TYPE"),
                env("CONFLUENCE_LIVE_WRITE_INVITE_EMAIL"), List.of(token, email), tape);

        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(baseUrl).email(email).apiToken(token).pageSize(25).build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ofMillis(100), tape);
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, client,
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        WriteScenarios scenarios = new WriteScenarios(channel, backend);
        Instant started = Instant.now();
        try {
            for (WriteScenarios.Scenario scenario : scenarios.scenarios()) {
                if (!scenario.name().equals(WriteScenarios.CLEANUP)) {
                    tape.scenario(scenario.name());
                    scenario.body().run();
                }
            }
        } finally {
            try {
                tape.scenario(WriteScenarios.CLEANUP);
                scenarios.scenarios().getLast().body().run();
            } finally {
                channel.shutdownNow();
                server.shutdownNow();
                server.awaitTermination(10, TimeUnit.SECONDS);
            }
        }
        scenarios.leftBehind().forEach(line -> System.out.println(TAG + "left behind: " + line));

        String report = scenarios.report(URI.create(baseUrl).getHost(), started);
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, report, StandardCharsets.UTF_8);
        System.out.println(report);
        System.out.println(TAG + "report written to " + out.toAbsolutePath());

        tape.finish(runFile(config.baseUrl(), backend, scenarios));
        System.out.println(TAG + "recording written to " + recordings.toAbsolutePath());

        List<String> failures = new ArrayList<>();
        scenarios.rows().stream().filter(row -> row.outcome().equals("FAIL"))
                .forEach(row -> failures.add(row.label() + ": " + row.detail()));
        assertThat(failures).as("failed write rpcs (see %s)", out).isEmpty();
    }

    /** What a replay needs to run the same steps: the backend values and each step's outcome. */
    static ObjectNode runFile(String baseUrl, WriteScenarios.Backend backend,
            WriteScenarios scenarios) {
        ObjectNode run = RecordedExchange.JSON.createObjectNode();
        run.put("baseUrl", baseUrl);
        run.put("spaceKey", backend.spaceKey());
        run.put("run", backend.run());
        run.put("admin", backend.admin());
        run.put("customContentType", backend.customContentType());
        run.put("inviteEmail", backend.inviteEmail());
        scenarios.rows().forEach(row -> run.withArray("outcomes").addObject()
                .put("rpc", row.rpc()).put("step", row.label()).put("outcome", row.outcome()));
        return run;
    }
}
