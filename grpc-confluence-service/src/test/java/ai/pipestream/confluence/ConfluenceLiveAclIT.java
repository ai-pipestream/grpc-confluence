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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs {@link AclScenarios} against the real Confluence workspace, writes
 * {@code build/live-acl.md}, and records every exchange, scrubbed, under
 * {@code src/test/resources/recordings/confluence-acl} for
 * {@link ConfluenceAclReplayTest}. Gated on the live credentials and
 * {@code CONFLUENCE_LIVE_ACL=1}; run with
 * {@code ./gradlew :grpc-confluence-service:liveAclTest}.
 */
class ConfluenceLiveAclIT {

    private static final String DEFAULT_BASE_URL = "https://pipestreamai.atlassian.net/wiki";
    private static final String DEFAULT_SPACE_KEY = "CLAUDEGRPC";

    private static String env(String name) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? null : value.strip();
    }

    @Test
    void everyAclRpcAgainstTheLiveWorkspace() throws Exception {
        String email = env(ConfluenceConnectorConfig.ENV_EMAIL) != null ? env(ConfluenceConnectorConfig.ENV_EMAIL)
                : env(ConfluenceConnectorConfig.ENV_EMAIL_ALIAS);
        String token = env(ConfluenceConnectorConfig.ENV_API_TOKEN) != null
                ? env(ConfluenceConnectorConfig.ENV_API_TOKEN) : env(ConfluenceConnectorConfig.ENV_API_TOKEN_ALIAS);
        assumeTrue(email != null && token != null, "no live Confluence credentials in the environment; skipping");
        assumeTrue("1".equals(env("CONFLUENCE_LIVE_ACL")), "CONFLUENCE_LIVE_ACL=1 is not set; skipping");
        String baseUrl = env(ConfluenceConnectorConfig.ENV_BASE_URL) != null
                ? env(ConfluenceConnectorConfig.ENV_BASE_URL) : DEFAULT_BASE_URL;
        String spaceKey = env("CONFLUENCE_SPACE_KEY") != null ? env("CONFLUENCE_SPACE_KEY") : DEFAULT_SPACE_KEY;
        String run = "pipestream-live-acl-" + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                .withZone(ZoneOffset.UTC).format(Instant.now()) + "-" + UUID.randomUUID().toString().substring(0, 8);
        Path recordings = Path.of(System.getProperty("liveAcl.recordings", ConfluenceAclReplayTest.DEFAULT_RECORDINGS));

        RecordingScrubber scrubber = new RecordingScrubber();
        scrubber.site(URI.create(baseUrl).getAuthority());
        scrubber.email(email);
        scrubber.secret(token);
        RecordingTape tape = new RecordingTape(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build(),
                recordings, scrubber);
        AclScenarios.Backend backend = new AclScenarios.Backend(spaceKey, run, List.of(token, email), tape, email);
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(baseUrl).email(email).apiToken(token).pageSize(25).build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                Duration.ofMillis(100), tape);
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, client, ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        AclScenarios scenarios = new AclScenarios(channel, backend);
        try {
            for (AclScenarios.Scenario scenario : scenarios.scenarios()) {
                if (!scenario.name().equals(AclScenarios.CLEANUP)) {
                    tape.scenario(scenario.name());
                    scenario.body().run();
                }
            }
        } finally {
            try {
                tape.scenario(AclScenarios.CLEANUP);
                scenarios.scenarios().getLast().body().run();
            } finally {
                channel.shutdownNow();
                server.shutdownNow();
                server.awaitTermination(10, TimeUnit.SECONDS);
            }
        }
        String report = scenarios.report(URI.create(baseUrl).getHost());
        Path out = Path.of(System.getProperty("liveAcl.report", "build/live-acl.md"));
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, report, StandardCharsets.UTF_8);
        System.out.println(report);

        ObjectNode runFile = RecordedExchange.JSON.createObjectNode();
        runFile.put("baseUrl", config.baseUrl());
        runFile.put("spaceKey", spaceKey);
        runFile.put("run", run);
        scenarios.rows().forEach(row -> runFile.withArray("outcomes").addObject()
                .put("rpc", row.rpc()).put("step", row.label()).put("outcome", row.outcome()));
        tape.finish(runFile);

        assertThat(scenarios.rows()).as("failed ACL rpcs (see %s)", out).noneMatch(row -> row.outcome().equals("FAIL"));
        assertThat(scenarios.leftBehind()).as("pages left behind").isEmpty();
    }
}
