package ai.pipestream.mcp;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.microsoft.v1.MicrosoftServiceGrpc;
import ai.pipestream.sync.v1.SyncTableServiceGrpc;
import io.grpc.ManagedChannel;
import io.modelcontextprotocol.server.McpServerFeatures;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Combined MCP process. Dials the Confluence, Microsoft, and SyncTable
 * gRPC proxies and exposes their streaming tools on {@code /mcp}.
 *
 * <p>{@code MCP_PORT} default 8090. Targets:
 * {@code CONFLUENCE_GRPC_TARGET}, {@code MICROSOFT_GRPC_TARGET},
 * {@code SYNC_TABLE_TARGET}.</p>
 */
public final class McpServerMain {

    /** Environment variable for the HTTP listen port. */
    public static final String ENV_PORT = "MCP_PORT";
    /** Fallback listen port when {@link #ENV_PORT} is unset or not a number. */
    public static final int DEFAULT_PORT = 8090;

    private static final System.Logger LOG = System.getLogger(McpServerMain.class.getName());

    private McpServerMain() {
    }

    /**
     * Dials the Confluence, Microsoft, and SyncTable gRPC targets, starts
     * streamable HTTP MCP on {@link #ENV_PORT} (default {@link #DEFAULT_PORT}),
     * and blocks until the process is killed.
     *
     * @param args unused
     * @throws Exception if the HTTP server fails to start
     */
    public static void main(String[] args) throws Exception {
        ManagedChannel confluence = GrpcTargets.channel(GrpcTargets.ENV_CONFLUENCE,
                GrpcTargets.DEFAULT_CONFLUENCE);
        ManagedChannel microsoft = GrpcTargets.channel(GrpcTargets.ENV_MICROSOFT,
                GrpcTargets.DEFAULT_MICROSOFT);
        ManagedChannel sync = GrpcTargets.channel(GrpcTargets.ENV_SYNC, GrpcTargets.DEFAULT_SYNC);
        List<McpServerFeatures.SyncToolSpecification> tools = new ArrayList<>();
        tools.addAll(ConfluenceMcpTools.of(ConfluenceServiceGrpc.newBlockingStub(confluence)));
        tools.addAll(MicrosoftMcpTools.of(MicrosoftServiceGrpc.newBlockingStub(microsoft)));
        tools.addAll(SyncTableMcpTools.of(SyncTableServiceGrpc.newBlockingStub(sync)));

        int port = parseInt(System.getenv(ENV_PORT), DEFAULT_PORT);
        McpHttpServer server = McpHttpServer.start(port, "pipestream-connectors", tools);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            GrpcTargets.shutdown(confluence);
            GrpcTargets.shutdown(microsoft);
            GrpcTargets.shutdown(sync);
        }, "mcp-shutdown"));
        LOG.log(System.Logger.Level.INFO, "mcp streamable-http on :{0}{1}", server.port(),
                McpHttpServer.ENDPOINT);
        new CountDownLatch(1).await();
    }

    private static int parseInt(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
