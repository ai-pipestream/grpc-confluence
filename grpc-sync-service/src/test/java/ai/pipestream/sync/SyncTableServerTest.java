package ai.pipestream.sync;

import ai.pipestream.sync.v1.SyncTableServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SyncTableServerTest {

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
    void bindsEphemeralPortAndServesHealth() throws Exception {
        server = SyncTableServer.startNetty(new SyncTableGrpcService(new AssetStore()), 0);
        channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
                .usePlaintext()
                .build();
        assertThat(server.getPort()).isPositive();
        HealthCheckResponse response = HealthGrpc.newBlockingStub(channel)
                .check(HealthCheckRequest.getDefaultInstance());
        assertThat(response.getStatus()).isEqualTo(HealthCheckResponse.ServingStatus.SERVING);
        assertThat(SyncTableServiceGrpc.newBlockingStub(channel)).isNotNull();
    }
}
