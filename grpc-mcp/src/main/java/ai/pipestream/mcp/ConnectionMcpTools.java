package ai.pipestream.mcp;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.ProbeConnectionRequest;
import ai.pipestream.sync.v1.Connection;
import ai.pipestream.sync.v1.ConnectionKind;
import ai.pipestream.sync.v1.ConnectionOutput;
import ai.pipestream.sync.v1.ConnectionServiceGrpc;
import ai.pipestream.sync.v1.CreateConnectionRequest;
import ai.pipestream.sync.v1.DeleteConnectionRequest;
import ai.pipestream.sync.v1.GetConnectionRequest;
import ai.pipestream.sync.v1.ListConnectionsRequest;
import ai.pipestream.sync.v1.UpdateConnectionRequest;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * MCP tools that call {@code ConnectionService} over gRPC. This is the
 * setup surface a future UI will use; there is no in-process catalog.
 */
/** MCP tools over {@code ConnectionService}. */
public final class ConnectionMcpTools {

    private ConnectionMcpTools() {
    }

    /**
     * Connection catalog tools plus {@code connection_test} via Confluence.
     *
     * @param connections {@code ConnectionService} stub
     * @param confluence {@code ConfluenceService} stub
     * @return tool specs
     */
    public static List<McpServerFeatures.SyncToolSpecification> of(
            ConnectionServiceGrpc.ConnectionServiceBlockingStub connections,
            ConfluenceServiceGrpc.ConfluenceServiceBlockingStub confluence) {
        return List.of(create(connections), list(connections), get(connections),
                update(connections), setOutput(connections), test(confluence),
                delete(connections));
    }

    private static McpServerFeatures.SyncToolSpecification create(
            ConnectionServiceGrpc.ConnectionServiceBlockingStub stub) {
        return tool("connection_create",
                "Create a catalog connection (Confluence or Microsoft). Token is write-only.",
                McpJson.objectSchema(schema(
                        "connectionId", "Optional id; generated when empty",
                        "kind", "confluence or microsoft",
                        "displayName", "Human name",
                        "baseUrl", "Confluence /wiki URL",
                        "email", "Atlassian email",
                        "token", "API token (stored, never returned)",
                        "spaceKeys", "Comma-separated space allowlist",
                        "tenantId", "Microsoft tenant",
                        "clientId", "Microsoft client id",
                        "clientSecret", "Microsoft client secret",
                        "siteId", "Microsoft site id",
                        "driveIds", "Comma-separated drive ids"),
                        List.of("kind", "displayName")),
                (exchange, request) -> {
                    Map<String, Object> args = request.arguments();
                    Connection created = stub.createConnection(CreateConnectionRequest.newBuilder()
                            .setConnection(fromArgs(args, false))
                            .build()).getConnection();
                    return text(McpJson.write(row(created)));
                });
    }

    private static McpServerFeatures.SyncToolSpecification list(
            ConnectionServiceGrpc.ConnectionServiceBlockingStub stub) {
        return tool("connection_list", "List catalog connections (secrets omitted)",
                McpJson.objectSchema(Map.of(
                        "kind", McpJson.stringProp("Optional confluence or microsoft")),
                        List.of()),
                (exchange, request) -> {
                    var response = stub.listConnections(ListConnectionsRequest.newBuilder()
                            .setKind(kind(McpJson.argString(request.arguments(), "kind", "")))
                            .build());
                    List<Map<String, Object>> rows = new ArrayList<>();
                    response.getConnectionsList().forEach(c -> rows.add(row(c)));
                    return text(McpJson.write(Map.of("connections", rows, "count", rows.size())));
                });
    }

    private static McpServerFeatures.SyncToolSpecification get(
            ConnectionServiceGrpc.ConnectionServiceBlockingStub stub) {
        return tool("connection_get", "Get one catalog connection (secrets omitted)",
                McpJson.objectSchema(Map.of(
                        "connectionId", McpJson.stringProp("Catalog id")),
                        List.of("connectionId")),
                (exchange, request) -> {
                    Connection connection = stub.getConnection(GetConnectionRequest.newBuilder()
                            .setConnectionId(McpJson.argString(request.arguments(),
                                    "connectionId", ""))
                            .build()).getConnection();
                    return text(McpJson.write(row(connection)));
                });
    }

    private static McpServerFeatures.SyncToolSpecification update(
            ConnectionServiceGrpc.ConnectionServiceBlockingStub stub) {
        return tool("connection_update",
                "Patch a catalog connection. Empty token leaves the stored secret.",
                McpJson.objectSchema(Map.of(
                        "connectionId", McpJson.stringProp("Catalog id"),
                        "displayName", McpJson.stringProp("Human name"),
                        "baseUrl", McpJson.stringProp("Confluence /wiki URL"),
                        "email", McpJson.stringProp("Atlassian email"),
                        "token", McpJson.stringProp("Replacement API token"),
                        "spaceKeys", McpJson.stringProp("Comma-separated space allowlist")),
                        List.of("connectionId")),
                (exchange, request) -> {
                    Connection updated = stub.updateConnection(UpdateConnectionRequest.newBuilder()
                            .setConnection(fromArgs(request.arguments(), true))
                            .build()).getConnection();
                    return text(McpJson.write(row(updated)));
                });
    }

    private static McpServerFeatures.SyncToolSpecification setOutput(
            ConnectionServiceGrpc.ConnectionServiceBlockingStub stub) {
        return tool("connection_set_output",
                "Bind filesystem or S3 output on a catalog connection",
                McpJson.objectSchema(Map.of(
                        "connectionId", McpJson.stringProp("Catalog id"),
                        "store", McpJson.stringProp("filesystem or s3"),
                        "formats", McpJson.stringProp("Comma list: okf,protobuf,json,microsoft-connector"),
                        "directory", McpJson.stringProp("Filesystem root"),
                        "prefix", McpJson.stringProp("Run prefix"),
                        "s3Bucket", McpJson.stringProp("S3 bucket"),
                        "s3Prefix", McpJson.stringProp("S3 key prefix"),
                        "s3Region", McpJson.stringProp("AWS region")),
                        List.of("connectionId")),
                (exchange, request) -> {
                    Map<String, Object> args = request.arguments();
                    String id = McpJson.argString(args, "connectionId", "");
                    Connection current = stub.getConnection(GetConnectionRequest.newBuilder()
                            .setConnectionId(id)
                            .build()).getConnection();
                    ConnectionOutput.Builder output = current.getOutput().toBuilder();
                    String store = McpJson.argString(args, "store", "");
                    if (!store.isEmpty()) {
                        output.setStore(store);
                    }
                    String formats = McpJson.argString(args, "formats", "");
                    if (!formats.isEmpty()) {
                        output.clearFormats();
                        for (String part : formats.split(",")) {
                            if (!part.isBlank()) {
                                output.addFormats(part.trim());
                            }
                        }
                    }
                    String directory = McpJson.argString(args, "directory", "");
                    if (!directory.isEmpty()) {
                        output.setDirectory(directory);
                    }
                    String prefix = McpJson.argString(args, "prefix", "");
                    if (!prefix.isEmpty()) {
                        output.setPrefix(prefix);
                    }
                    String bucket = McpJson.argString(args, "s3Bucket", "");
                    if (!bucket.isEmpty()) {
                        output.setS3Bucket(bucket);
                    }
                    String s3Prefix = McpJson.argString(args, "s3Prefix", "");
                    if (!s3Prefix.isEmpty()) {
                        output.setS3Prefix(s3Prefix);
                    }
                    String region = McpJson.argString(args, "s3Region", "");
                    if (!region.isEmpty()) {
                        output.setS3Region(region);
                    }
                    Connection updated = stub.updateConnection(UpdateConnectionRequest.newBuilder()
                            .setConnection(current.toBuilder().setOutput(output).setToken(""))
                            .build()).getConnection();
                    return text(McpJson.write(row(updated)));
                });
    }

    private static McpServerFeatures.SyncToolSpecification test(
            ConfluenceServiceGrpc.ConfluenceServiceBlockingStub confluence) {
        return tool("connection_test",
                "Probe a Confluence connection via ConfluenceService.ProbeConnection",
                McpJson.objectSchema(Map.of(
                        "connectionId", McpJson.stringProp("Catalog id"),
                        "baseUrl", McpJson.stringProp("Inline URL for an unsaved probe"),
                        "email", McpJson.stringProp("Inline email"),
                        "token", McpJson.stringProp("Inline token"),
                        "limit", McpJson.intProp("Max spaces to return as evidence")),
                        List.of()),
                (exchange, request) -> {
                    Map<String, Object> args = request.arguments();
                    var response = confluence.probeConnection(ProbeConnectionRequest.newBuilder()
                            .setConnectionId(McpJson.argString(args, "connectionId", ""))
                            .setBaseUrl(McpJson.argString(args, "baseUrl", ""))
                            .setEmail(McpJson.argString(args, "email", ""))
                            .setToken(McpJson.argString(args, "token", ""))
                            .setLimit(McpJson.argInt(args, "limit", 5))
                            .build());
                    return text(McpJson.write(Map.of(
                            "ok", response.getOk(),
                            "connectionId", response.getConnectionId(),
                            "spaceKeys", response.getSpaceKeysList(),
                            "errorMessage", response.getErrorMessage())));
                });
    }

    private static McpServerFeatures.SyncToolSpecification delete(
            ConnectionServiceGrpc.ConnectionServiceBlockingStub stub) {
        return tool("connection_delete", "Delete a catalog connection (ledger rows stay)",
                McpJson.objectSchema(Map.of(
                        "connectionId", McpJson.stringProp("Catalog id")),
                        List.of("connectionId")),
                (exchange, request) -> {
                    String id = McpJson.argString(request.arguments(), "connectionId", "");
                    stub.deleteConnection(DeleteConnectionRequest.newBuilder()
                            .setConnectionId(id)
                            .build());
                    return text(McpJson.write(Map.of("deleted", id)));
                });
    }

    private static Connection fromArgs(Map<String, Object> args, boolean requireId) {
        String id = McpJson.argString(args, "connectionId", "");
        if (requireId && id.isBlank()) {
            throw new IllegalArgumentException("connectionId is required");
        }
        Connection.Builder connection = Connection.newBuilder()
                .setConnectionId(id)
                .setKind(kind(McpJson.argString(args, "kind", "confluence")))
                .setDisplayName(McpJson.argString(args, "displayName", ""))
                .setBaseUrl(McpJson.argString(args, "baseUrl", ""))
                .setEmail(McpJson.argString(args, "email", ""))
                .setToken(McpJson.argString(args, "token", ""))
                .setTenantId(McpJson.argString(args, "tenantId", ""))
                .setClientId(McpJson.argString(args, "clientId", ""))
                .setClientSecret(McpJson.argString(args, "clientSecret", ""))
                .setSiteId(McpJson.argString(args, "siteId", ""));
        for (String space : csv(McpJson.argString(args, "spaceKeys", ""))) {
            connection.addSpaceKeys(space);
        }
        for (String drive : csv(McpJson.argString(args, "driveIds", ""))) {
            connection.addDriveIds(drive);
        }
        return connection.build();
    }

    private static Map<String, Object> schema(String... keysAndDescriptions) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndDescriptions.length; i += 2) {
            map.put(keysAndDescriptions[i], McpJson.stringProp(keysAndDescriptions[i + 1]));
        }
        return map;
    }

    private static List<String> csv(String raw) {
        if (raw.isBlank()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String part : raw.split(",")) {
            if (!part.isBlank()) {
                out.add(part.trim());
            }
        }
        return out;
    }

    private static ConnectionKind kind(String raw) {
        return switch (raw == null ? "" : raw.trim().toLowerCase()) {
            case "microsoft" -> ConnectionKind.CONNECTION_KIND_MICROSOFT;
            case "confluence" -> ConnectionKind.CONNECTION_KIND_CONFLUENCE;
            case "" -> ConnectionKind.CONNECTION_KIND_UNSPECIFIED;
            default -> ConnectionKind.CONNECTION_KIND_CONFLUENCE;
        };
    }

    private static Map<String, Object> row(Connection connection) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("connectionId", connection.getConnectionId());
        row.put("kind", connection.getKind().name());
        row.put("displayName", connection.getDisplayName());
        row.put("baseUrl", connection.getBaseUrl());
        row.put("email", connection.getEmail());
        row.put("hasToken", connection.getHasToken());
        row.put("spaceKeys", connection.getSpaceKeysList());
        row.put("status", connection.getStatus().name());
        row.put("lastError", connection.getLastError());
        row.put("outputStore", connection.getOutput().getStore());
        row.put("outputFormats", connection.getOutput().getFormatsList());
        row.put("outputDirectory", connection.getOutput().getDirectory());
        row.put("outputPrefix", connection.getOutput().getPrefix());
        return row;
    }

    private static McpServerFeatures.SyncToolSpecification tool(String name, String description,
            Map<String, Object> schema,
            BiFunction<McpSyncServerExchange, McpSchema.CallToolRequest, McpSchema.CallToolResult> handler) {
        return McpServerFeatures.SyncToolSpecification.builder()
                .tool(McpSchema.Tool.builder(name, schema).description(description).build())
                .callHandler(handler)
                .build();
    }

    private static McpSchema.CallToolResult text(String body) {
        return McpSchema.CallToolResult.builder()
                .content(List.of(McpSchema.TextContent.builder(body).build()))
                .build();
    }
}
