package ai.pipestream.mcp;

import ai.pipestream.sync.v1.GetAssetRequest;
import ai.pipestream.sync.v1.ListAssetsRequest;
import ai.pipestream.sync.v1.ListAssetsResponse;
import ai.pipestream.sync.v1.SyncTableServiceGrpc;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.McpSyncServerExchange;
import io.modelcontextprotocol.spec.McpSchema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/** MCP tools over {@code SyncTableService}. */
public final class SyncTableMcpTools {

    private SyncTableMcpTools() {
    }

    /**
     * Ledger get/list tools.
     *
     * @param stub {@code SyncTableService} stub
     * @return tool specs
     */
    public static List<McpServerFeatures.SyncToolSpecification> of(
            SyncTableServiceGrpc.SyncTableServiceBlockingStub stub) {
        return List.of(get(stub), list(stub));
    }

    private static McpServerFeatures.SyncToolSpecification get(
            SyncTableServiceGrpc.SyncTableServiceBlockingStub stub) {
        return tool("sync_table_get_asset", "Get one ledger row by asset_id",
                McpJson.objectSchema(Map.of("assetId", McpJson.stringProp("source:kind:native_id")),
                        List.of("assetId")),
                (exchange, request) -> {
                    var asset = stub.getAsset(GetAssetRequest.newBuilder()
                            .setAssetId(McpJson.argString(request.arguments(), "assetId", ""))
                            .build()).getAsset();
                    return text(McpJson.write(row(asset)));
                });
    }

    private static McpServerFeatures.SyncToolSpecification list(
            SyncTableServiceGrpc.SyncTableServiceBlockingStub stub) {
        return tool("sync_table_list_assets",
                "Stream ledger rows (filter by source, kind, attachments)",
                McpJson.objectSchema(Map.of(
                        "source", McpJson.stringProp("confluence or microsoft"),
                        "kind", McpJson.stringProp("page, attachment, drive_item, ..."),
                        "connectionId", McpJson.stringProp("Catalog connection filter"),
                        "attachmentsOnly", McpJson.boolProp("Only attachment rows"),
                        "limit", McpJson.intProp("Max rows")),
                        List.of()),
                (exchange, request) -> {
                    int limit = McpJson.argInt(request.arguments(), "limit", 100);
                    Iterator<ListAssetsResponse> stream = stub.listAssets(ListAssetsRequest.newBuilder()
                            .setSource(McpJson.argString(request.arguments(), "source", ""))
                            .setKind(McpJson.argString(request.arguments(), "kind", ""))
                            .setConnectionId(McpJson.argString(request.arguments(),
                                    "connectionId", ""))
                            .setAttachmentsOnly(McpJson.argBool(request.arguments(),
                                    "attachmentsOnly", false))
                            .setLimit(limit)
                            .build());
                    List<Map<String, Object>> rows = new ArrayList<>();
                    while (stream.hasNext()) {
                        rows.add(row(stream.next().getAsset()));
                    }
                    return text(McpJson.write(Map.of("assets", rows, "count", rows.size())));
                });
    }

    private static Map<String, Object> row(ai.pipestream.sync.v1.Asset asset) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("assetId", asset.getAssetId());
        row.put("source", asset.getSource());
        row.put("kind", asset.getKind());
        row.put("title", asset.getTitle());
        row.put("sourceUri", asset.getSourceUri());
        row.put("phase", asset.getPhase().name());
        row.put("status", asset.getStatus().name());
        row.put("attachment", asset.getAttachment());
        row.put("parentAssetId", asset.getParentAssetId());
        row.put("runId", asset.getRunId());
        row.put("connectionId", asset.getConnectionId());
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
