package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceEvent;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/** Every write rpc left to {@link ChangeScenarios} has a step there that names it. */
class ChangeSuiteCoverageTest {

    @Test
    void everyWriteRpcOfTheChangeSuiteHasAStepThatNamesIt() {
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl("https://site.example.invalid/wiki")
                .email("replay@" + RecordingScrubber.PLACEHOLDER_DOMAIN).apiToken("unused").build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ZERO);
        ConfluenceChangeGrpcService service = new ConfluenceChangeGrpcService(
                new ConfluenceGrpcService(config, client,
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES), null, Clock.systemUTC());
        ManagedChannel channel = InProcessChannelBuilder.forName("change-suite-coverage").build();
        try {
            List<String> labels = new ChangeScenarios(channel, service, "SPACE", "run-00000000", false,
                    new ChangeScenarios.Events() {
                        @Override
                        public void await(Predicate<ConfluenceEvent> match) {
                            // No step is run.
                        }

                        @Override
                        public void awaitQueued(int count) {
                            // No step is run.
                        }

                        @Override
                        public void pause(Duration time) {
                            // No step is run.
                        }
                    }).all().stream().map(ChangeScenarios.Scenario::label).toList();

            assertThat(ConfluenceWriteRoutes.rpcs()).containsAll(ConfluenceWriteRoutes.CHANGE_SUITE);
            for (String rpc : ConfluenceWriteRoutes.CHANGE_SUITE) {
                assertThat(labels).as("a step that names %s", rpc)
                        .anyMatch(label -> List.of(label.split("[ ,()]+")).contains(rpc));
            }
        } finally {
            channel.shutdownNow();
        }
    }
}
