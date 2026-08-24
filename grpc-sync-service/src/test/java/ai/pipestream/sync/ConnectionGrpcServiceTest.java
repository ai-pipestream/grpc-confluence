package ai.pipestream.sync;

import ai.pipestream.sync.v1.Connection;
import ai.pipestream.sync.v1.ConnectionKind;
import ai.pipestream.sync.v1.ConnectionServiceGrpc;
import ai.pipestream.sync.v1.ConnectionStatus;
import ai.pipestream.sync.v1.CreateConnectionRequest;
import ai.pipestream.sync.v1.DeleteConnectionRequest;
import ai.pipestream.sync.v1.GetConnectionRequest;
import ai.pipestream.sync.v1.ListConnectionsRequest;
import ai.pipestream.sync.v1.RecordProbeRequest;
import ai.pipestream.sync.v1.GetSettingsRequest;
import ai.pipestream.sync.v1.RuntimeSettings;
import ai.pipestream.sync.v1.UpdateConnectionRequest;
import ai.pipestream.sync.v1.UpdateSettingsRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectionGrpcServiceTest {

    private Server server;
    private ManagedChannel channel;
    private ConnectionServiceGrpc.ConnectionServiceBlockingStub stub;

    @BeforeEach
    void start() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new ConnectionGrpcService(new AssetStore()))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = ConnectionServiceGrpc.newBlockingStub(channel);
    }

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
    void createListGetRedactSecretsAndUpdate() {
        Connection created = stub.createConnection(CreateConnectionRequest.newBuilder()
                .setConnection(Connection.newBuilder()
                        .setKind(ConnectionKind.CONNECTION_KIND_CONFLUENCE)
                        .setDisplayName("Acme Wiki")
                        .setBaseUrl("https://acme.atlassian.net/wiki")
                        .setEmail("bot@acme.test")
                        .setToken("secret-token")
                        .addSpaceKeys("ENG"))
                .build()).getConnection();
        assertThat(created.getConnectionId()).isEqualTo("acme-wiki");
        assertThat(created.getToken()).isEmpty();
        assertThat(created.getHasToken()).isTrue();
        assertThat(created.getStatus()).isEqualTo(ConnectionStatus.CONNECTION_STATUS_PENDING);

        assertThat(stub.listConnections(ListConnectionsRequest.getDefaultInstance())
                .getConnectionsList()).extracting(Connection::getConnectionId)
                .containsExactly("acme-wiki");
        assertThat(stub.getConnection(GetConnectionRequest.newBuilder()
                .setConnectionId("acme-wiki").build()).getConnection().getToken()).isEmpty();

        Connection secret = stub.getConnection(GetConnectionRequest.newBuilder()
                .setConnectionId("acme-wiki")
                .setIncludeSecret(true)
                .build()).getConnection();
        assertThat(secret.getToken()).isEqualTo("secret-token");

        Connection updated = stub.updateConnection(UpdateConnectionRequest.newBuilder()
                .setConnection(Connection.newBuilder()
                        .setConnectionId("acme-wiki")
                        .setKind(ConnectionKind.CONNECTION_KIND_CONFLUENCE)
                        .setDisplayName("Acme")
                        .addSpaceKeys("DOCS"))
                .build()).getConnection();
        assertThat(updated.getDisplayName()).isEqualTo("Acme");
        assertThat(updated.getHasToken()).isTrue();
        assertThat(updated.getBaseUrl()).isEqualTo("https://acme.atlassian.net/wiki");
        assertThat(updated.getEmail()).isEqualTo("bot@acme.test");
        assertThat(stub.getConnection(GetConnectionRequest.newBuilder()
                .setConnectionId("acme-wiki")
                .setIncludeSecret(true)
                .build()).getConnection().getToken()).isEqualTo("secret-token");
    }

    @Test
    void recordProbeAndDelete() {
        stub.createConnection(CreateConnectionRequest.newBuilder()
                .setConnection(Connection.newBuilder()
                        .setConnectionId("eng")
                        .setKind(ConnectionKind.CONNECTION_KIND_CONFLUENCE)
                        .setDisplayName("Eng")
                        .setToken("t"))
                .build());
        Connection ready = stub.recordProbe(RecordProbeRequest.newBuilder()
                .setConnectionId("eng")
                .setOk(true)
                .build()).getConnection();
        assertThat(ready.getStatus()).isEqualTo(ConnectionStatus.CONNECTION_STATUS_READY);
        assertThat(ready.hasLastTestedAt()).isTrue();

        stub.deleteConnection(DeleteConnectionRequest.newBuilder()
                .setConnectionId("eng").build());
        assertThatThrownBy(() -> stub.getConnection(GetConnectionRequest.newBuilder()
                .setConnectionId("eng").build()))
                .isInstanceOf(StatusRuntimeException.class)
                .hasMessageContaining("NOT_FOUND");
    }

    @Test
    void getAndUpdateSettingsMergeEmptyFields() {
        assertThat(stub.getSettings(GetSettingsRequest.getDefaultInstance())
                .getSettings().getKafkaBootstrapServers()).isEmpty();

        RuntimeSettings first = stub.updateSettings(UpdateSettingsRequest.newBuilder()
                .setSettings(RuntimeSettings.newBuilder()
                        .setKafkaBootstrapServers("localhost:9092")
                        .setKafkaTopic("confluence.changes")
                        .setOutput(ai.pipestream.sync.v1.ConnectionOutput.newBuilder()
                                .setStore("filesystem")
                                .setDirectory("/tmp/okf")
                                .addFormats("okf")))
                .build()).getSettings();
        assertThat(first.getKafkaBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(first.getOutput().getDirectory()).isEqualTo("/tmp/okf");

        RuntimeSettings patched = stub.updateSettings(UpdateSettingsRequest.newBuilder()
                .setSettings(RuntimeSettings.newBuilder()
                        .setKafkaTopic("events"))
                .build()).getSettings();
        assertThat(patched.getKafkaBootstrapServers()).isEqualTo("localhost:9092");
        assertThat(patched.getKafkaTopic()).isEqualTo("events");
        assertThat(patched.getOutput().getDirectory()).isEqualTo("/tmp/okf");
    }
}
