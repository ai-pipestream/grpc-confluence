package ai.pipestream.mcp;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import io.grpc.Channel;
import io.modelcontextprotocol.server.McpServerFeatures;

import java.util.List;

/**
 * Contributes the {@code confluence_*} tools and {@code connection_test}
 * over {@code ConfluenceService} to the combined MCP server. Registered in
 * {@code META-INF/services/ai.pipestream.mcp.McpToolProvider}.
 */
public final class ConfluenceMcpToolProvider implements McpToolProvider {

    /** Environment variable for the Confluence gRPC {@code host:port}. */
    public static final String ENV_TARGET = "CONFLUENCE_GRPC_TARGET";
    /** Fallback Confluence target when {@link #ENV_TARGET} is unset. */
    public static final String DEFAULT_TARGET = "localhost:9095";

    /** ServiceLoader constructor. */
    public ConfluenceMcpToolProvider() {
    }

    @Override
    public String name() {
        return "confluence";
    }

    @Override
    public String targetEnvironmentVariable() {
        return ENV_TARGET;
    }

    @Override
    public String defaultTarget() {
        return DEFAULT_TARGET;
    }

    @Override
    public List<McpServerFeatures.SyncToolSpecification> tools(Channel channel) {
        return ConfluenceMcpTools.of(ConfluenceServiceGrpc.newBlockingStub(channel));
    }
}
