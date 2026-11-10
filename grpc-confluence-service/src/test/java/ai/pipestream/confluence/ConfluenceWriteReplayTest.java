package ai.pipestream.confluence;

import com.fasterxml.jackson.databind.JsonNode;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs {@link WriteScenarios} against the recording {@link ConfluenceLiveWriteIT}
 * saved, with no network: one test per scenario. A scenario fails on a FAIL
 * row, on a request that differs from the recorded one, and on a recorded
 * exchange that was never requested. Without a recording every scenario is
 * skipped.
 */
class ConfluenceWriteReplayTest {

    /** Where the recording lives, relative to the module directory. */
    static final String DEFAULT_RECORDINGS = "src/test/resources/recordings/confluence";

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

    @TestFactory
    List<DynamicTest> everyWriteScenarioAgainstTheRecording() throws IOException {
        Path recordings = Path.of(System.getProperty("writeReplay.recordings", DEFAULT_RECORDINGS));
        List<DynamicTest> tests = new ArrayList<>();
        if (!ReplayTape.recorded(recordings)) {
            for (String name : WriteScenarios.names()) {
                tests.add(DynamicTest.dynamicTest(name, () -> assumeTrue(false, "no recording under "
                        + recordings + "; record one with :grpc-confluence-service:liveWriteTest")));
            }
            return tests;
        }
        ReplayTape tape = new ReplayTape(recordings);
        JsonNode run = tape.run();
        WriteScenarios scenarios = start(tape, run);
        for (WriteScenarios.Scenario scenario : scenarios.scenarios()) {
            tests.add(DynamicTest.dynamicTest(scenario.name(), () -> {
                assumeTrue(tape.has(scenario.name()) || recordedWithoutRequests(run, scenario.name()),
                        "the recording has no scenario " + scenario.name()
                                + "; record again with :grpc-confluence-service:liveWriteTest");
                int rowsBefore = scenarios.rows().size();
                int violationsBefore = tape.violations().size();
                tape.scenario(scenario.name());
                scenario.body().run();
                tape.finish();
                List<String> violations = tape.violations();
                assertThat(violations.subList(violationsBefore, violations.size()))
                        .as("requests that differ from the recording").isEmpty();
                List<WriteScenarios.Row> rows = scenarios.rows();
                assertThat(rows.subList(rowsBefore, rows.size()))
                        .as("FAIL rows").noneMatch(row -> row.outcome().equals("FAIL"));
                if (scenario.name().equals(WriteScenarios.CLEANUP)) {
                    assertThat(outcomes(rows)).as("the outcome of every step, as recorded")
                            .isEqualTo(recordedOutcomes(run));
                }
            }));
        }
        return tests;
    }

    /** Starts the service on the tape and answers the scenarios that call it. */
    private WriteScenarios start(ReplayTape tape, JsonNode run) throws IOException {
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(run.path("baseUrl").asText()).email("replay@" + RecordingScrubber.PLACEHOLDER_DOMAIN)
                .apiToken("replay").pageSize(25).build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ZERO, tape);
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, client,
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        return new WriteScenarios(channel, backend(tape, run));
    }

    /** The backend the recording was made with. */
    static WriteScenarios.Backend backend(ExchangeTape tape, JsonNode run) {
        return new WriteScenarios.Backend(run.path("spaceKey").asText(), run.path("run").asText(),
                run.path("admin").asBoolean(), run.path("customContentType").asText(null),
                run.path("inviteEmail").asText(null), List.of(), tape);
    }

    /** Whether the recorded run went through a scenario that made no request. */
    private static boolean recordedWithoutRequests(JsonNode run, String scenario) {
        for (JsonNode name : run.path("scenarios")) {
            if (name.asText().equals(scenario)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> outcomes(List<WriteScenarios.Row> rows) {
        return rows.stream().map(row -> row.rpc() + " | " + row.label() + " | " + row.outcome()).toList();
    }

    private static List<String> recordedOutcomes(JsonNode run) {
        List<String> outcomes = new ArrayList<>();
        run.path("outcomes").forEach(row -> outcomes.add(row.path("rpc").asText() + " | "
                + row.path("step").asText() + " | " + row.path("outcome").asText()));
        return outcomes;
    }
}
