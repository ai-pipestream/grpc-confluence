package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * With the Kafka sink on, a Sync that returns OK and a resume cursor says
 * that every change of the pass reached the topic: the next pass starts at
 * that cursor and never emits those changes again. A record the producer
 * could not deliver therefore has to fail the Sync. Here the broker address
 * is a closed port, so no record can be delivered.
 */
class KafkaDeliveryFailureTest {

    private FakeConfluenceServer fake;
    private KafkaProducer<String, byte[]> producer;
    private Server server;
    private ManagedChannel channel;
    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub;

    @BeforeEach
    void start() throws Exception {
        fake = FakeConfluenceServer.start();
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                ConfluenceFixtures.pageJson("201", "100", "Runbook",
                        java.time.Instant.now().toString())));
        for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                "properties")) {
            fake.stub("/wiki/api/v2/pages/201/" + sub, ConfluenceFixtures.emptyListJson());
        }

        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        }
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:" + closedPort);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // Give up on the unreachable broker quickly.
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "200");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "200");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "400");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        producer = new KafkaProducer<>(props);

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
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES,
                        new KafkaChangeSink(producer, "confluence-events", "confluence-snapshots")))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = ConfluenceServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
        producer.close(Duration.ZERO);
        fake.close();
    }

    @Test
    void aFullSyncWhoseChangesDidNotReachTheTopicFails() {
        List<SyncResponse> responses = new ArrayList<>();

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.sync(SyncRequest.getDefaultInstance()).forEachRemaining(responses::add));

        assertThat(failure).as("full Sync while the Kafka broker cannot be reached").isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(failure.getStatus().getDescription()).contains("kafka", "confluence-events");
        assertThat(responses).noneMatch(r -> r.getEventCase() == SyncResponse.EventCase.RESUME_CURSOR);
    }

    @Test
    void anIncrementalSyncWhoseChangesDidNotReachTheTopicFails() {
        List<SyncResponse> responses = new ArrayList<>();
        String since = java.time.Instant.now().minus(Duration.ofHours(1)).toString();

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.sync(SyncRequest.newBuilder().setSinceCursor(since).build())
                        .forEachRemaining(responses::add));

        assertThat(failure).as("incremental Sync while the Kafka broker cannot be reached")
                .isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(responses).noneMatch(r -> r.getEventCase() == SyncResponse.EventCase.RESUME_CURSOR);
    }
}
