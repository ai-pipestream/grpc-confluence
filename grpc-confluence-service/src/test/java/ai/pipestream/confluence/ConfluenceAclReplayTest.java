package ai.pipestream.confluence;

import com.fasterxml.jackson.databind.JsonNode;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs {@link AclScenarios} against the recording {@link ConfluenceLiveAclIT}
 * saved, with no network. Fails on a FAIL row, on a request that differs
 * from the recorded one, and on outcomes that differ from the recorded run.
 * Skipped until a recording exists.
 */
class ConfluenceAclReplayTest {

    /** Where the recording lives, relative to the module directory. */
    static final String DEFAULT_RECORDINGS = "src/test/resources/recordings/confluence-acl";

    private Server server;
    private ManagedChannel channel;

    @AfterEach
    void stop() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void everyAclRpcAgainstTheRecording() throws Exception {
        Path recordings = Path.of(System.getProperty("aclReplay.recordings", DEFAULT_RECORDINGS));
        assumeTrue(ReplayTape.recorded(recordings), "no recording under " + recordings
                + "; record one with :grpc-confluence-service:liveAclTest");
        ReplayTape tape = new ReplayTape(recordings);
        JsonNode run = tape.run();
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(run.path("baseUrl").asText()).email("replay@" + RecordingScrubber.PLACEHOLDER_DOMAIN)
                .apiToken("replay").pageSize(25).build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                Duration.ZERO, tape);
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, client, ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        AclScenarios scenarios = new AclScenarios(channel, new AclScenarios.Backend(
                run.path("spaceKey").asText(), run.path("run").asText(), List.of(), tape));
        for (AclScenarios.Scenario scenario : scenarios.scenarios()) {
            tape.scenario(scenario.name());
            scenario.body().run();
            tape.finish();
        }
        assertThat(tape.violations()).as("requests that differ from the recording").isEmpty();
        assertThat(scenarios.rows()).as("FAIL rows").noneMatch(row -> row.outcome().equals("FAIL"));
        List<String> recorded = new ArrayList<>();
        run.path("outcomes").forEach(row -> recorded.add(row.path("rpc").asText() + " | "
                + row.path("step").asText() + " | " + row.path("outcome").asText()));
        assertThat(scenarios.rows().stream().map(row -> row.rpc() + " | " + row.label() + " | " + row.outcome())
                .toList()).as("the outcome of every step, as recorded").isEqualTo(recorded);
    }
}
