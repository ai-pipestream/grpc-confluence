package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceChangeServiceGrpc;
import ai.pipestream.confluence.v1.ConfluenceEventObject;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.WatchChangesRequest;
import ai.pipestream.confluence.v1.WatchChangesResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.Context;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A step of {@link ChangeScenarios} asks for a drain, waits for it, and then
 * reads what the stream delivered. The service tells the end of a drain from
 * its own thread, while the drain's messages still travel to the client. A
 * step that reads the stream before they arrive finds nothing, drains again,
 * and so sends the queue a request no recording holds.
 */
class WatchedChangesTest {

    private static final String QUEUE = "/x/queue";
    /** One group event: streamed as an unresolved event, with no Confluence read. */
    private static final String GROUP_EVENT = """
            {"events":[{"key":"k1","event":{"eventType":"avi:confluence:created:group",
            "group":{"id":"g1","name":"team"}}}]}""";
    private static final Duration WAIT = Duration.ofSeconds(30);
    /** Far longer than the service needs to tell a drain has ended. */
    private static final Duration LATE = Duration.ofMillis(400);

    private FakeConfluenceServer fake;
    private ConfluenceChangeGrpcService service;
    private Server server;
    private ManagedChannel channel;
    private Context.CancellableContext watch;

    @BeforeEach
    void start() throws Exception {
        fake = FakeConfluenceServer.start();
        fake.stub(QUEUE, GROUP_EVENT);
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl()).email("bot@pipestream.ai").apiToken("token-123").build();
        ConfluenceGrpcService read = new ConfluenceGrpcService(config,
                new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                        Duration.ZERO),
                ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES);
        service = new ConfluenceChangeGrpcService(read,
                new ForgeEventQueue(fake.baseUrl().replace("/wiki", "") + QUEUE, "secret",
                        HttpClient.newHttpClient()),
                Clock.systemUTC());
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(read).addService(service).build().start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    @AfterEach
    void stop() {
        if (watch != null) {
            watch.cancel(null);
        }
        channel.shutdownNow();
        server.shutdownNow();
        fake.close();
    }

    @Test
    void aDrainIsAwaitedUntilWhatItStreamedHasArrived() throws Exception {
        WatchedChanges watched = open(lateDelivery(LATE));
        List<WatchChangesResponse> streamed = new ArrayList<>();

        watched.awaitDrain(WAIT);
        watched.drainTo(streamed);

        assertThat(streamed).as("what had arrived when the opening drain was awaited")
                .extracting(m -> m.getUnresolvedEvent().getObject())
                .containsExactly(ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_GROUP);

        service.drainNow();
        watched.awaitDrain(WAIT);
        watched.drainTo(streamed);

        assertThat(streamed).as("what had arrived when the second drain was awaited")
                .extracting(WatchChangesResponse::getSequence).containsExactly(1L, 2L);
    }

    @Test
    void aDrainThatStreamedNothingIsAwaitedWithoutWaitingForAMessage() throws Exception {
        fake.stub(QUEUE, "{\"events\":[]}");
        WatchedChanges watched = open(lateDelivery(LATE));
        List<WatchChangesResponse> streamed = new ArrayList<>();

        assertThat(watched.awaitDrain(WAIT)).isEmpty();
        watched.drainTo(streamed);

        assertThat(streamed).isEmpty();
    }

    /** Opens a stream that drains when it opens and after that only when told to. */
    private WatchedChanges open(ClientInterceptor interceptor) {
        WatchedChanges watched = new WatchedChanges(service);
        Channel late = ClientInterceptors.intercept(channel, interceptor);
        WatchChangesRequest request = WatchChangesRequest.newBuilder()
                .setSync(SyncRequest.getDefaultInstance())
                .setSkipInitialPass(true)
                .setPollInterval(com.google.protobuf.Duration.newBuilder().setSeconds(86_400))
                .setEventInterval(com.google.protobuf.Duration.newBuilder().setSeconds(86_400))
                .build();
        watch = Context.current().withCancellation();
        watch.run(() -> ConfluenceChangeServiceGrpc.newStub(late).watchChanges(request, watched));
        return watched;
    }

    /** Hands every message of a stream to its observer late, as a loaded machine does. */
    private static ClientInterceptor lateDelivery(Duration delay) {
        return new ClientInterceptor() {
            @Override
            public <Q, R> ClientCall<Q, R> interceptCall(MethodDescriptor<Q, R> method,
                    CallOptions options, Channel next) {
                return new ForwardingClientCall.SimpleForwardingClientCall<>(
                        next.newCall(method, options)) {
                    @Override
                    public void start(Listener<R> listener, Metadata headers) {
                        super.start(new ForwardingClientCallListener
                                .SimpleForwardingClientCallListener<>(listener) {
                            @Override
                            public void onMessage(R message) {
                                try {
                                    Thread.sleep(delay.toMillis());
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                super.onMessage(message);
                            }
                        }, headers);
                    }
                };
            }
        };
    }
}
