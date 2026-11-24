package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import ai.pipestream.sync.v1.ConnectionOutput;
import ai.pipestream.sync.v1.ConnectionServiceGrpc;
import ai.pipestream.sync.v1.GetSettingsRequest;
import ai.pipestream.sync.v1.GetSettingsResponse;
import ai.pipestream.sync.v1.RuntimeSettings;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * An operator who configured an output expects every Sync to write to it.
 * Only "no output configured" lets a Sync run without one. A store that is
 * not loaded or cannot be opened, and a catalog that cannot say what the
 * output is, fail the Sync instead of letting it stream and return OK with
 * nothing written.
 */
class OutputBindingFailureTest {

    private FakeConfluenceServer fake;
    private Server catalogServer;
    private ManagedChannel catalogChannel;
    private Server server;
    private ManagedChannel channel;
    /** What the catalog answers GetSettings with; a status fails the call. */
    private volatile ConnectionOutput processOutput = ConnectionOutput.getDefaultInstance();
    private volatile Status settingsFailure;

    @BeforeEach
    void start() throws Exception {
        fake = FakeConfluenceServer.start();
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                ConfluenceFixtures.pageJson("201", "100", "Runbook", "2026-01-01T00:00:00.000Z")));
        for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                "properties")) {
            fake.stub("/wiki/api/v2/pages/201/" + sub, ConfluenceFixtures.emptyListJson());
        }

        String catalogName = InProcessServerBuilder.generateName();
        catalogServer = InProcessServerBuilder.forName(catalogName)
                .directExecutor()
                .addService(new ConnectionServiceGrpc.ConnectionServiceImplBase() {
                    @Override
                    public void getSettings(GetSettingsRequest request,
                            StreamObserver<GetSettingsResponse> observer) {
                        if (settingsFailure != null) {
                            observer.onError(settingsFailure.asRuntimeException());
                            return;
                        }
                        observer.onNext(GetSettingsResponse.newBuilder()
                                .setSettings(RuntimeSettings.newBuilder().setOutput(processOutput))
                                .build());
                        observer.onCompleted();
                    }
                })
                .build()
                .start();
        catalogChannel = InProcessChannelBuilder.forName(catalogName).directExecutor().build();

        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("token-123")
                .build();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                // The service blocks on outbound flow control, as in production:
                // a direct executor would run it on the client thread and stall.
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config,
                        new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                                Duration.ZERO),
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES, null,
                        ConnectionServiceGrpc.newBlockingStub(catalogChannel), null))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    @AfterEach
    void stop() {
        for (ManagedChannel c : new ManagedChannel[] {channel, catalogChannel}) {
            if (c != null) {
                c.shutdownNow();
            }
        }
        for (Server s : new Server[] {server, catalogServer}) {
            if (s != null) {
                s.shutdownNow();
            }
        }
        fake.close();
    }

    @Test
    void aConfiguredStoreThatIsNotLoadedFailsTheSync() {
        processOutput = ConnectionOutput.newBuilder()
                .setStore("tape")
                .setDirectory("/data/okf")
                .build();

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                this::sync);

        assertThat(failure).as("Sync with an output store that is not loaded").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(failure.getStatus().getDescription())
                .contains("configured output", "tape", "not loaded");
    }

    @Test
    void aConfiguredDirectoryThatCannotBeOpenedFailsTheSync(@TempDir Path dir) throws Exception {
        // A regular file stands where the output directory should be.
        Path file = Files.writeString(dir.resolve("not-a-directory"), "x");
        processOutput = ConnectionOutput.newBuilder()
                .setDirectory(file.resolve("out").toString())
                .build();

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                this::sync);

        assertThat(failure).as("Sync with an output directory that cannot be created").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
        assertThat(failure.getStatus().getDescription()).contains("configured output");
    }

    @Test
    void aCatalogThatCannotBeReachedFailsTheSync() {
        settingsFailure = Status.UNAVAILABLE.withDescription("catalog is restarting");

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                this::sync);

        assertThat(failure).as("Sync while the output settings cannot be read").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(failure.getStatus().getDescription())
                .contains("GetSettings", "catalog is restarting");
    }

    @Test
    void aCatalogThatRefusesTheSettingsReadFailsTheSync() {
        settingsFailure = Status.PERMISSION_DENIED.withDescription("no");

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                this::sync);

        assertThat(failure).as("Sync while the output settings are refused").isNotNull();
        // Not the catalog's own code: PERMISSION_DENIED would read as Confluence's.
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(failure.getStatus().getDescription()).contains("GetSettings", "PERMISSION_DENIED");
    }

    @Test
    void noConfiguredOutputLetsTheSyncRunWithoutOne() {
        List<SyncResponse> responses = sync();

        assertThat(responses.getLast().getEventCase())
                .isEqualTo(SyncResponse.EventCase.RESUME_CURSOR);
    }

    @Test
    void aConfiguredDirectoryIsWrittenTo(@TempDir Path dir) throws Exception {
        processOutput = ConnectionOutput.newBuilder()
                .setDirectory(dir.resolve("out").toString())
                .build();

        List<SyncResponse> responses = sync();

        assertThat(responses.getLast().getEventCase())
                .isEqualTo(SyncResponse.EventCase.RESUME_CURSOR);
        try (Stream<Path> written = Files.walk(dir.resolve("out"))) {
            assertThat(written.filter(Files::isRegularFile)).isNotEmpty();
        }
    }

    private List<SyncResponse> sync() {
        List<SyncResponse> responses = new ArrayList<>();
        ConfluenceServiceGrpc.newBlockingStub(channel).sync(SyncRequest.getDefaultInstance())
                .forEachRemaining(responses::add);
        return responses;
    }
}
