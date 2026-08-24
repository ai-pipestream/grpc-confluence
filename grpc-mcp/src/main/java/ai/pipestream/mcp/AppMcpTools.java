package ai.pipestream.mcp;

import ai.pipestream.sync.v1.Connection;
import ai.pipestream.sync.v1.ConnectionOutput;
import ai.pipestream.sync.v1.ConnectionServiceGrpc;
import ai.pipestream.sync.v1.GetSettingsRequest;
import ai.pipestream.sync.v1.ListConnectionsRequest;
import ai.pipestream.sync.v1.RuntimeSettings;
import ai.pipestream.sync.v1.UpdateSettingsRequest;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * MCP verbs that set up the running app through {@code ConnectionService}.
 * Process output and Kafka live on {@code RuntimeSettings}; sites live on
 * {@code Connection}. No secrets are returned.
 */
public final class AppMcpTools {

    private AppMcpTools() {
    }

    /**
     * Runtime setup tools over {@code ConnectionService}.
     *
     * @param connections generated stub
     * @return tool specs
     */
    public static List<McpServerFeatures.SyncToolSpecification> of(
            ConnectionServiceGrpc.ConnectionServiceBlockingStub connections) {
        return List.of(status(connections), setOutput(connections), setKafka(connections));
    }

    private static McpServerFeatures.SyncToolSpecification status(
            ConnectionServiceGrpc.ConnectionServiceBlockingStub stub) {
        return tool("app_status",
                "Show runtime setup: connections, default output, and Kafka (no secrets)",
                McpJson.objectSchema(Map.of(), List.of()),
                (exchange, request) -> {
                    RuntimeSettings settings = stub.getSettings(
                            GetSettingsRequest.getDefaultInstance()).getSettings();
                    List<Map<String, Object>> rows = new ArrayList<>();
                    stub.listConnections(ListConnectionsRequest.getDefaultInstance())
                            .getConnectionsList()
                            .forEach(connection -> rows.add(connectionRow(connection)));
                    Map<String, Object> body = new LinkedHashMap<>();
                    body.put("connectionCount", rows.size());
                    body.put("connections", rows);
                    body.put("output", outputRow(settings.getOutput()));
                    body.put("kafkaBootstrapServers", settings.getKafkaBootstrapServers());
                    body.put("kafkaTopic", settings.getKafkaTopic());
                    body.put("kafkaSnapshotsTopic", settings.getKafkaSnapshotsTopic());
                    body.put("confluenceTarget", target(GrpcTargets.ENV_CONFLUENCE,
                            GrpcTargets.DEFAULT_CONFLUENCE));
                    body.put("microsoftTarget", target(GrpcTargets.ENV_MICROSOFT,
                            GrpcTargets.DEFAULT_MICROSOFT));
                    body.put("syncTableTarget", target(GrpcTargets.ENV_SYNC,
                            GrpcTargets.DEFAULT_SYNC));
                    return text(McpJson.write(body));
                });
    }

    private static McpServerFeatures.SyncToolSpecification setOutput(
            ConnectionServiceGrpc.ConnectionServiceBlockingStub stub) {
        return tool("app_set_output",
                "Set the process default output store (filesystem or S3). Connections can override.",
                McpJson.objectSchema(Map.of(
                        "store", McpJson.stringProp("filesystem or s3"),
                        "formats", McpJson.stringProp("Comma list: okf,protobuf,json,microsoft-connector"),
                        "directory", McpJson.stringProp("Filesystem root"),
                        "prefix", McpJson.stringProp("Run prefix"),
                        "s3Bucket", McpJson.stringProp("S3 bucket"),
                        "s3Prefix", McpJson.stringProp("S3 key prefix"),
                        "s3Region", McpJson.stringProp("AWS region")),
                        List.of()),
                (exchange, request) -> {
                    Map<String, Object> args = request.arguments();
                    RuntimeSettings current = stub.getSettings(
                            GetSettingsRequest.getDefaultInstance()).getSettings();
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
                    RuntimeSettings stored = stub.updateSettings(UpdateSettingsRequest.newBuilder()
                            .setSettings(RuntimeSettings.newBuilder().setOutput(output))
                            .build()).getSettings();
                    return text(McpJson.write(Map.of("output", outputRow(stored.getOutput()))));
                });
    }

    private static McpServerFeatures.SyncToolSpecification setKafka(
            ConnectionServiceGrpc.ConnectionServiceBlockingStub stub) {
        return tool("app_set_kafka",
                "Set Kafka bootstrap and topics used when a process Kafka sink is not already env-bound",
                McpJson.objectSchema(Map.of(
                        "bootstrapServers", McpJson.stringProp("host:port list"),
                        "topic", McpJson.stringProp("Changes topic"),
                        "snapshotsTopic", McpJson.stringProp("Snapshots topic")),
                        List.of()),
                (exchange, request) -> {
                    Map<String, Object> args = request.arguments();
                    RuntimeSettings stored = stub.updateSettings(UpdateSettingsRequest.newBuilder()
                            .setSettings(RuntimeSettings.newBuilder()
                                    .setKafkaBootstrapServers(McpJson.argString(args,
                                            "bootstrapServers", ""))
                                    .setKafkaTopic(McpJson.argString(args, "topic", ""))
                                    .setKafkaSnapshotsTopic(McpJson.argString(args,
                                            "snapshotsTopic", "")))
                            .build()).getSettings();
                    return text(McpJson.write(Map.of(
                            "kafkaBootstrapServers", stored.getKafkaBootstrapServers(),
                            "kafkaTopic", stored.getKafkaTopic(),
                            "kafkaSnapshotsTopic", stored.getKafkaSnapshotsTopic())));
                });
    }

    private static Map<String, Object> connectionRow(Connection connection) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("connectionId", connection.getConnectionId());
        row.put("kind", connection.getKind().name());
        row.put("displayName", connection.getDisplayName());
        row.put("status", connection.getStatus().name());
        row.put("hasToken", connection.getHasToken());
        row.put("outputStore", connection.getOutput().getStore());
        return row;
    }

    private static Map<String, Object> outputRow(ConnectionOutput output) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("store", output.getStore());
        row.put("formats", output.getFormatsList());
        row.put("directory", output.getDirectory());
        row.put("prefix", output.getPrefix());
        row.put("s3Bucket", output.getS3Bucket());
        row.put("s3Prefix", output.getS3Prefix());
        row.put("s3Region", output.getS3Region());
        return row;
    }

    private static String target(String envName, String fallback) {
        String value = System.getenv(envName);
        return value == null || value.isBlank() ? fallback : value.trim();
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
