package ai.pipestream.mcp;

import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.VirtualThreadPool;

import java.util.List;

/**
 * Streamable HTTP MCP at {@code /mcp}, hosted on Jetty with a virtual-thread
 * pool. Protocol handlers are the official MCP Java SDK 2.0
 * {@link McpServer#sync} API (blocking, VT-friendly).
 */
public final class McpHttpServer implements AutoCloseable {

    /** Streamable HTTP path served by Jetty. */
    public static final String ENDPOINT = "/mcp";

    private final Server jetty;
    private final McpSyncServer mcp;
    private final int port;

    private McpHttpServer(Server jetty, McpSyncServer mcp, int port) {
        this.jetty = jetty;
        this.mcp = mcp;
        this.port = port;
    }

    /**
     * Returns the port Jetty actually bound.
     *
     * @return the local listen port, which may differ from the requested port when that was {@code 0}
     */
    public int port() {
        return port;
    }

    /**
     * Starts Jetty on {@code 0.0.0.0:port} and registers {@code tools} under
     * {@link #ENDPOINT} using the MCP Java SDK {@code sync} API.
     *
     * @param port listen port; {@code 0} selects an ephemeral port
     * @param name MCP {@code serverInfo} name
     * @param tools MCP tool specifications to expose
     * @return a running server whose {@link #port()} is the bound port
     * @throws Exception if Jetty fails to start
     */
    public static McpHttpServer start(int port, String name,
            List<McpServerFeatures.SyncToolSpecification> tools) throws Exception {
        HttpServletStreamableServerTransportProvider transport =
                HttpServletStreamableServerTransportProvider.builder()
                        .mcpEndpoint(ENDPOINT)
                        .build();
        var builder = McpServer.sync(transport)
                .serverInfo(name, "0.1.0")
                .capabilities(McpSchema.ServerCapabilities.builder().tools(true).build());
        for (McpServerFeatures.SyncToolSpecification tool : tools) {
            builder.toolCall(tool.tool(), tool.callHandler());
        }
        McpSyncServer mcp = builder.build();

        VirtualThreadPool threads = new VirtualThreadPool();
        Server jetty = new Server(threads);
        org.eclipse.jetty.server.ServerConnector connector =
                new org.eclipse.jetty.server.ServerConnector(jetty);
        connector.setHost("0.0.0.0");
        connector.setPort(port);
        jetty.addConnector(connector);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        ServletHolder holder = new ServletHolder(transport);
        holder.setAsyncSupported(true);
        context.addServlet(holder, ENDPOINT);
        context.addServlet(holder, ENDPOINT + "/*");
        jetty.setHandler(context);
        jetty.start();
        return new McpHttpServer(jetty, mcp, connector.getLocalPort());
    }

    @Override
    public void close() {
        try {
            mcp.close();
        } catch (Exception ignored) {
        }
        try {
            jetty.stop();
        } catch (Exception ignored) {
        }
    }
}
