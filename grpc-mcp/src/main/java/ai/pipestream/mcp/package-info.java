/**
 * Streamable HTTP MCP endpoint (MCP Java SDK 2.0, spec 2025-11-25).
 * Tools call the Confluence, Microsoft, Connection, and SyncTable gRPC
 * services. {@code app_*} and {@code connection_*} set up the running app.
 * The HTTP server and tool handlers run on virtual threads; the MCP
 * client transport is the JDK {@link java.net.http.HttpClient}.
 */
package ai.pipestream.mcp;
