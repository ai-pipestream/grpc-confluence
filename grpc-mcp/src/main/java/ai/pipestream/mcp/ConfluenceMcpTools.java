package ai.pipestream.mcp;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.GetAttachmentRequest;
import ai.pipestream.confluence.v1.GetPageRequest;
import ai.pipestream.confluence.v1.ListAttachmentsRequest;
import ai.pipestream.confluence.v1.ListAttachmentsResponse;
import ai.pipestream.confluence.v1.ListSpacesRequest;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.spec.McpSchema;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** MCP tools over {@code ConfluenceService}. */
public final class ConfluenceMcpTools {

    private ConfluenceMcpTools() {
    }

    /**
     * Confluence list/get/sync tools.
     *
     * @param stub {@code ConfluenceService} stub
     * @return tool specs
     */
    public static List<McpServerFeatures.SyncToolSpecification> of(
            ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub) {
        return List.of(spaces(stub), page(stub), attachments(stub), sync(stub));
    }

    private static McpServerFeatures.SyncToolSpecification spaces(
            ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub) {
        return tool("confluence_list_spaces", "List Confluence spaces the credentials can see",
                McpJson.objectSchema(Map.of(
                        "limit", McpJson.intProp("Max spaces; 0 = no cap"),
                        "connectionId", McpJson.stringProp("Catalog connection; empty = default")),
                        List.of()),
                (exchange, request) -> {
                    int limit = McpJson.argInt(request.arguments(), "limit", 0);
                    var response = stub.listSpaces(ListSpacesRequest.newBuilder()
                            .setLimit(limit)
                            .setConnectionId(McpJson.argString(request.arguments(),
                                    "connectionId", ""))
                            .build());
                    List<Map<String, String>> spaces = new ArrayList<>();
                    response.getSpacesList().forEach(space -> spaces.add(Map.of(
                            "id", space.getId(),
                            "key", space.getKey(),
                            "name", space.getName())));
                    return text(McpJson.write(Map.of("spaces", spaces, "count", spaces.size())));
                });
    }

    private static McpServerFeatures.SyncToolSpecification page(
            ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub) {
        return tool("confluence_get_page", "Get one Confluence page by id",
                McpJson.objectSchema(Map.of("id", McpJson.stringProp("Page id")), List.of("id")),
                (exchange, request) -> {
                    String id = McpJson.argString(request.arguments(), "id", "");
                    var page = stub.getPage(GetPageRequest.newBuilder().setId(id).build()).getPage();
                    return text(McpJson.write(Map.of(
                            "id", page.getId(),
                            "title", page.getTitle(),
                            "spaceId", page.getSpaceId(),
                            "webUrl", page.getWebUrl())));
                });
    }

    private static McpServerFeatures.SyncToolSpecification attachments(
            ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub) {
        return tool("confluence_list_attachments",
                "Stream attachments of a Confluence page (or fetch one by id)",
                McpJson.objectSchema(Map.of(
                        "pageId", McpJson.stringProp("Page id to list"),
                        "attachmentId", McpJson.stringProp("Optional single attachment id"),
                        "includeContent", McpJson.boolProp("Inline bytes for a single attachment")),
                        List.of()),
                (exchange, request) -> {
                    String attachmentId = McpJson.argString(request.arguments(), "attachmentId", "");
                    if (!attachmentId.isEmpty()) {
                        var attachment = stub.getAttachment(GetAttachmentRequest.newBuilder()
                                .setId(attachmentId)
                                .setIncludeContent(McpJson.argBool(request.arguments(),
                                        "includeContent", false))
                                .build()).getAttachment();
                        return text(McpJson.write(Map.of(
                                "id", attachment.getId(),
                                "title", attachment.getTitle(),
                                "fileSize", attachment.getFileSize(),
                                "mediaType", attachment.getMediaType(),
                                "hasContent", attachment.hasContent())));
                    }
                    String pageId = McpJson.argString(request.arguments(), "pageId", "");
                    List<Map<String, Object>> items = new ArrayList<>();
                    Iterator<ListAttachmentsResponse> stream = stub.listAttachments(
                            ListAttachmentsRequest.newBuilder().setPageId(pageId).build());
                    while (stream.hasNext() && items.size() < 200) {
                        var attachment = stream.next().getAttachment();
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("id", attachment.getId());
                        row.put("title", attachment.getTitle());
                        row.put("fileSize", attachment.getFileSize());
                        items.add(row);
                    }
                    return text(McpJson.write(Map.of("attachments", items, "count", items.size())));
                });
    }

    private static McpServerFeatures.SyncToolSpecification sync(
            ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub) {
        return tool("confluence_sync",
                "Run one streaming Confluence Sync pass (initial crawl or incremental)",
                McpJson.objectSchema(Map.of(
                        "sinceCursor", McpJson.stringProp("Empty = full crawl"),
                        "includeBodies", McpJson.boolProp("Include page bodies"),
                        "limit", McpJson.intProp("Max change events to return"),
                        "connectionId", McpJson.stringProp("Catalog connection; empty = default")),
                        List.of()),
                (exchange, request) -> {
                    String since = McpJson.argString(request.arguments(), "sinceCursor", "");
                    int limit = McpJson.argInt(request.arguments(), "limit", 50);
                    Iterator<SyncResponse> stream = stub.sync(SyncRequest.newBuilder()
                            .setSinceCursor(since)
                            .setIncludeBodies(McpJson.argBool(request.arguments(),
                                    "includeBodies", false))
                            .setConnectionId(McpJson.argString(request.arguments(),
                                    "connectionId", ""))
                            .build());
                    List<Map<String, String>> events = new ArrayList<>();
                    String resume = "";
                    int changes = 0;
                    int snapshots = 0;
                    while (stream.hasNext()) {
                        SyncResponse event = stream.next();
                        if (event.hasChange()) {
                            changes++;
                            if (events.size() < limit) {
                                events.add(Map.of(
                                        "changeId", event.getChange().getChangeId(),
                                        "kind", event.getChange().getEntity().getEntityCase().name(),
                                        "entityId", event.getChange().getEntity().getEntityId(),
                                        "operation", event.getChange().getOperation().name()));
                            }
                        } else if (event.hasSnapshot()) {
                            snapshots++;
                        } else if (event.getEventCase() == SyncResponse.EventCase.RESUME_CURSOR) {
                            resume = event.getResumeCursor();
                        }
                    }
                    return text(McpJson.write(Map.of(
                            "changes", changes,
                            "snapshots", snapshots,
                            "resumeCursor", resume,
                            "sample", events)));
                });
    }

    private static McpServerFeatures.SyncToolSpecification tool(String name, String description,
            Map<String, Object> schema,
            java.util.function.BiFunction<io.modelcontextprotocol.server.McpSyncServerExchange,
                    McpSchema.CallToolRequest, McpSchema.CallToolResult> handler) {
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
