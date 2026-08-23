package ai.pipestream.mcp;

import ai.pipestream.sync.AssetStore;
import ai.pipestream.sync.SyncTableGrpcService;
import ai.pipestream.sync.v1.Asset;
import ai.pipestream.sync.v1.SyncTableServiceGrpc;
import ai.pipestream.sync.v1.UpsertAssetRequest;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class McpHttpServerTest {

    private Server grpc;
    private McpHttpServer mcp;

    @AfterEach
    void stop() {
        if (mcp != null) {
            mcp.close();
        }
        if (grpc != null) {
            grpc.shutdownNow();
        }
    }

    @Test
    void streamableHttpListsAndCallsSyncTableTools() throws Exception {
        String name = InProcessServerBuilder.generateName();
        AssetStore store = new AssetStore();
        grpc = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new SyncTableGrpcService(store))
                .build()
                .start();
        var stub = SyncTableServiceGrpc.newBlockingStub(
                InProcessChannelBuilder.forName(name).directExecutor().build());
        stub.upsertAsset(UpsertAssetRequest.newBuilder()
                .setAsset(Asset.newBuilder()
                        .setAssetId("confluence:attachment:a1")
                        .setSource("confluence")
                        .setKind("attachment")
                        .setNativeId("a1")
                        .setAttachment(true)
                        .setTitle("notes.txt"))
                .build());

        mcp = McpHttpServer.start(0, "test-mcp", SyncTableMcpTools.of(stub));
        HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport
                .builder("http://127.0.0.1:" + mcp.port())
                .endpoint(McpHttpServer.ENDPOINT)
                .build();
        try (McpSyncClient client = McpClient.sync(transport)
                .requestTimeout(Duration.ofSeconds(15))
                .build()) {
            client.initialize();
            assertThat(client.listTools().tools())
                    .extracting(McpSchema.Tool::name)
                    .contains("sync_table_get_asset", "sync_table_list_assets");
            McpSchema.CallToolResult result = client.callTool(McpSchema.CallToolRequest.builder()
                    .name("sync_table_list_assets")
                    .arguments(Map.of("attachmentsOnly", true))
                    .build());
            assertThat(result.isError()).isFalse();
            assertThat(result.content().get(0).toString()).contains("confluence:attachment:a1");
        }
    }
}
