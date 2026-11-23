package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceEvent;
import com.fasterxml.jackson.databind.JsonNode;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs {@link ChangeScenarios} against the recording {@link ConfluenceLiveChangeIT}
 * saved, with no network: the same steps and assertions, the queue and REST
 * answers served from the recording. Skipped when there is no recording.
 */
class ConfluenceChangeReplayTest {

    @Test
    void everyChangeStepAgainstTheRecording() throws Exception {
        Path recordings = Path.of(System.getProperty("changeReplay.recordings",
                ConfluenceLiveChangeIT.DEFAULT_RECORDINGS));
        assumeTrue(ReplayTape.recorded(recordings), "no recording under " + recordings
                + "; record one with :grpc-confluence-service:liveChangesTest");
        ReplayTape tape = new ReplayTape(recordings);
        JsonNode run = tape.run();
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(run.path("baseUrl").asText())
                .email("replay@" + RecordingScrubber.PLACEHOLDER_DOMAIN).apiToken("replay")
                .pageSize(25).build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ZERO, tape);
        ConfluenceGrpcService read = new ConfluenceGrpcService(config, client,
                ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES);
        ConfluenceChangeGrpcService service = new ConfluenceChangeGrpcService(read,
                new ForgeEventQueue(run.path("queueUrl").asText(), "replay", tape, time -> { }),
                Clock.systemUTC());
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(read).addService(service).build().start();
        ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
        ChangeScenarios scenarios = new ChangeScenarios(channel, client, service,
                run.path("spaceKey").asText(), run.path("run").asText(), new ChangeScenarios.Events() {
                    @Override
                    public void await(java.util.function.Predicate<ConfluenceEvent> match) {
                        // The recorded take answers already hold the event.
                    }

                    @Override
                    public void awaitQueued(int count) {
                        // The recorded take answers already hold the events.
                    }

                    @Override
                    public void pause(java.time.Duration time) {
                        // Replay does not wait.
                    }
                });
        try {
            for (ChangeScenarios.Scenario scenario : scenarios.all()) {
                tape.scenario(scenario.id());
                scenarios.run(scenario);
                tape.finish();
            }
            tape.scenario("cleanup");
            scenarios.cleanUp();
            tape.finish();
        } finally {
            channel.shutdownNow();
            server.shutdownNow();
            server.awaitTermination(10, TimeUnit.SECONDS);
        }
        assertThat(tape.violations()).as("requests that differ from the recording").isEmpty();
        List<String> outcomes = new ArrayList<>();
        scenarios.rows().forEach(row -> outcomes.add(row.id() + " " + row.outcome()));
        List<String> recorded = new ArrayList<>();
        run.path("outcomes").forEach(row -> recorded.add(row.path("step").asText() + " "
                + row.path("outcome").asText()));
        assertThat(outcomes).as("the outcome of every step, as recorded").isEqualTo(recorded);
        assertThat(scenarios.rows()).as("FAIL rows").noneMatch(row -> row.outcome().equals("FAIL"));
    }
}
