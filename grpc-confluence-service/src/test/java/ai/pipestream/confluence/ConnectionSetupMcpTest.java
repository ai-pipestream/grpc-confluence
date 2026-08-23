package ai.pipestream.confluence;

import ai.pipestream.mcp.ConnectionMcpTools;
import ai.pipestream.mcp.ConfluenceMcpTools;
import ai.pipestream.mcp.McpHttpServer;
import ai.pipestream.mcp.SyncTableMcpTools;
import ai.pipestream.sync.AssetStore;
import ai.pipestream.sync.ConnectionGrpcService;
import ai.pipestream.sync.SyncTableGrpcService;
import ai.pipestream.sync.v1.ConnectionServiceGrpc;
import ai.pipestream.sync.v1.SyncTableServiceGrpc;
import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import io.grpc.ManagedChannel;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every catalog setup action goes through MCP, which calls
 * {@code ConnectionService} / {@code ConfluenceService} gRPC — not the store.
 */
class ConnectionSetupMcpTest {

    private FakeConfluenceServer acme;
    private FakeConfluenceServer globex;
    private Server grpc;
    private ManagedChannel channel;
    private McpHttpServer mcp;

    @AfterEach
    void stop() {
        if (mcp != null) {
            mcp.close();
        }
        if (channel != null) {
            channel.shutdownNow();
        }
        if (grpc != null) {
            grpc.shutdownNow();
        }
        if (acme != null) {
            acme.close();
        }
        if (globex != null) {
            globex.close();
        }
    }

    @Test
    void mcpCreatesTwoConnectionsProbesSyncsAndListsLedger() throws Exception {
        acme = stubSite("100", "ENG", "Engineering", "200", "Acme Design");
        globex = stubSite("500", "FIN", "Finance", "600", "Globex Budget");

        AssetStore ledger = new AssetStore();
        String name = InProcessServerBuilder.generateName();
        grpc = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new SyncTableGrpcService(ledger))
                .addService(new ConnectionGrpcService(ledger))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        var connections = ConnectionServiceGrpc.newBlockingStub(channel);
        var syncTable = SyncTableServiceGrpc.newBlockingStub(channel);

        ConfluenceGrpcService confluence = new ConfluenceGrpcService(null, null,
                ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES, null, connections,
                new SyncTableChangeSink(channel, "unused"));
        Server confluenceServer = InProcessServerBuilder.forName(name + "-cf")
                .directExecutor()
                .addService(confluence)
                .build()
                .start();
        ManagedChannel confluenceChannel = InProcessChannelBuilder.forName(name + "-cf")
                .directExecutor().build();
        var confluenceStub = ConfluenceServiceGrpc.newBlockingStub(confluenceChannel);

        List<io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification> tools =
                new ArrayList<>();
        tools.addAll(ConnectionMcpTools.of(connections, confluenceStub));
        tools.addAll(ConfluenceMcpTools.of(confluenceStub));
        tools.addAll(SyncTableMcpTools.of(syncTable));
        mcp = McpHttpServer.start(0, "setup-mcp", tools);

        HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport
                .builder("http://127.0.0.1:" + mcp.port())
                .endpoint(McpHttpServer.ENDPOINT)
                .build();
        try (McpSyncClient client = McpClient.sync(transport)
                .requestTimeout(Duration.ofSeconds(20))
                .build()) {
            client.initialize();
            assertThat(client.listTools().tools())
                    .extracting(McpSchema.Tool::name)
                    .contains("connection_create", "connection_list", "connection_get",
                            "connection_update", "connection_set_output", "connection_test",
                            "connection_delete", "confluence_sync", "sync_table_list_assets");

            String acmeBody = text(client.callTool(tool("connection_create", Map.of(
                    "kind", "confluence",
                    "displayName", "Acme Wiki",
                    "baseUrl", acme.baseUrl(),
                    "email", "bot@acme.test",
                    "token", "acme-token",
                    "spaceKeys", "ENG"))));
            assertThat(acmeBody).contains("acme-wiki").contains("\"hasToken\":true")
                    .doesNotContain("acme-token");

            String globexBody = text(client.callTool(tool("connection_create", Map.of(
                    "kind", "confluence",
                    "displayName", "Globex",
                    "baseUrl", globex.baseUrl(),
                    "email", "bot@globex.test",
                    "token", "globex-token"))));
            assertThat(globexBody).contains("globex");

            assertThat(text(client.callTool(tool("connection_list", Map.of()))))
                    .contains("acme-wiki").contains("globex");
            assertThat(text(client.callTool(tool("connection_get",
                    Map.of("connectionId", "acme-wiki")))))
                    .contains("ENG").doesNotContain("acme-token");

            assertThat(text(client.callTool(tool("connection_update", Map.of(
                    "connectionId", "acme-wiki",
                    "kind", "confluence",
                    "displayName", "Acme",
                    "spaceKeys", "ENG,DOCS")))))
                    .contains("Acme");

            assertThat(text(client.callTool(tool("connection_set_output", Map.of(
                    "connectionId", "acme-wiki",
                    "store", "filesystem",
                    "formats", "okf,protobuf",
                    "directory", "/tmp/acme",
                    "prefix", "acme")))))
                    .contains("filesystem").contains("okf");

            assertThat(text(client.callTool(tool("connection_test", Map.of(
                    "connectionId", "acme-wiki",
                    "limit", 5)))))
                    .contains("\"ok\":true").contains("ENG");

            assertThat(text(client.callTool(tool("confluence_sync", Map.of(
                    "connectionId", "acme-wiki",
                    "limit", 20)))))
                    .contains("changes");
            assertThat(text(client.callTool(tool("confluence_sync", Map.of(
                    "connectionId", "globex",
                    "limit", 20)))))
                    .contains("changes");

            String assets = text(client.callTool(tool("sync_table_list_assets", Map.of(
                    "source", "confluence",
                    "connectionId", "acme-wiki",
                    "limit", 50))));
            assertThat(assets).contains("confluence:acme-wiki:page:200")
                    .doesNotContain("confluence:globex:page:600");

            String globexAssets = text(client.callTool(tool("sync_table_list_assets", Map.of(
                    "source", "confluence",
                    "connectionId", "globex",
                    "limit", 50))));
            assertThat(globexAssets).contains("confluence:globex:page:600")
                    .doesNotContain("confluence:acme-wiki:page:200");

            assertThat(text(client.callTool(tool("connection_delete",
                    Map.of("connectionId", "globex")))))
                    .contains("globex");
            assertThat(text(client.callTool(tool("connection_list", Map.of()))))
                    .contains("acme-wiki").doesNotContain("\"connectionId\":\"globex\"");
        } finally {
            confluenceChannel.shutdownNow();
            confluenceServer.shutdownNow();
        }
    }

    private FakeConfluenceServer stubSite(String spaceId, String key, String name,
            String pageId, String title) throws Exception {
        FakeConfluenceServer fake = FakeConfluenceServer.start();
        Instant modified = Instant.parse("2024-03-02T00:00:00Z");
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson(spaceId, key, name)));
        fake.stub("/wiki/api/v2/spaces/" + spaceId + "/properties",
                ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson(pageId, spaceId, title, modified.toString())));
        fake.stub("/wiki/api/v2/pages/" + pageId + "/footer-comments",
                ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/" + pageId + "/inline-comments",
                ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/" + pageId + "/attachments",
                ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/" + pageId + "/labels",
                ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/" + pageId + "/properties",
                ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());
        return fake;
    }

    private static McpSchema.CallToolRequest tool(String name, Map<String, Object> args) {
        return McpSchema.CallToolRequest.builder().name(name).arguments(args).build();
    }

    private static String text(McpSchema.CallToolResult result) {
        assertThat(result.isError()).isFalse();
        return result.content().get(0).toString();
    }
}
