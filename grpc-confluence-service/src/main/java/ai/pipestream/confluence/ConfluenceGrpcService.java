package ai.pipestream.confluence;

import ai.pipestream.confluence.ConfluenceRoutes.Query;
import ai.pipestream.confluence.v1.*;
import ai.pipestream.sync.v1.Connection;
import ai.pipestream.sync.v1.ConnectionKind;
import ai.pipestream.sync.v1.ConnectionOutput;
import ai.pipestream.sync.v1.ConnectionServiceGrpc;
import ai.pipestream.sync.v1.GetConnectionRequest;
import ai.pipestream.sync.v1.GetSettingsRequest;
import ai.pipestream.sync.v1.RecordProbeRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static ai.pipestream.confluence.ConfluenceRoutes.invalid;
import static ai.pipestream.confluence.ConfluenceRoutes.requireId;
import static ai.pipestream.confluence.ConfluenceRoutes.values;

/**
 * The {@code ConfluenceService} gRPC facade: one thin method per rpc,
 * delegating to the crawler core. Every handler is plain blocking code; the
 * server runs handlers on virtual threads (see {@link ConfluenceServer}),
 * so REST round-trips park instead of pinning a carrier.
 *
 * <p>Each rpc maps to the Confluence Cloud REST v2 route named on it in
 * {@code confluence_service.proto}. Request filters, sorts, statuses and
 * body formats reach the API with their exact v2 query names and enum wire
 * values; a value the chosen route does not accept is answered with
 * {@code INVALID_ARGUMENT} instead of being dropped. {@code include-*}
 * expansions the route does not offer are ignored, as
 * {@code ContentIncludes} documents.</p>
 *
 * <p>Outgoing domain messages are checked by {@link ConfluenceValidator}
 * before they leave the process: the same identity, numeric-floor, and
 * cross-field rules the original proto options encoded.</p>
 */
public final class ConfluenceGrpcService extends ConfluenceServiceGrpc.ConfluenceServiceImplBase {

    /** Default cap for inlined attachment bytes: 25 MiB. */
    public static final long DEFAULT_ATTACHMENT_MAX_BYTES = 25L * 1024 * 1024;

    /**
     * The server implementation's default size of one {@code DownloadAttachment}
     * data message when the request leaves {@code chunk_bytes} at 0: 1 MiB.
     * This is gRPC framing chosen by this server, not a contract constant;
     * override it with the constructor that takes {@code downloadChunkBytes}.
     */
    public static final int DEFAULT_DOWNLOAD_CHUNK_BYTES = 1024 * 1024;

    /** {@code POST /users-bulk} accepts at most this many account ids per call. */
    static final int USERS_BULK_MAX_IDS = 250;
    /** {@code POST /content/convert-ids-to-types} accepts at most this many ids per call. */
    static final int CONVERT_IDS_MAX_IDS = 100;
    /** The largest {@code limit} any v2 list route accepts. */
    static final int MAX_LIMIT = 250;

    private static final ConfluenceValidator VALIDATOR = ConfluenceValidator.create();
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private static final Set<String> PAGE_INCLUDES = Set.of("include-labels",
            "include-properties", "include-operations", "include-likes", "include-versions",
            "include-favorited-by-current-user-status", "include-webresources",
            "include-collaborators", "include-direct-children");
    private static final Set<String> BLOG_POST_INCLUDES = Set.of("include-labels",
            "include-properties", "include-operations", "include-likes", "include-versions",
            "include-favorited-by-current-user-status", "include-webresources",
            "include-collaborators");
    /** {@code GET /attachments/{id}} and {@code GET /custom-content/{id}}. */
    private static final Set<String> ATTACHMENT_INCLUDES = Set.of("include-labels",
            "include-properties", "include-operations", "include-versions",
            "include-collaborators");
    private static final Set<String> COMMENT_INCLUDES = Set.of("include-properties",
            "include-operations", "include-likes", "include-versions");
    /** Whiteboards, databases, folders and Smart Links. */
    private static final Set<String> TREE_INCLUDES = Set.of("include-collaborators",
            "include-direct-children", "include-operations", "include-properties");

    private final ConfluenceConnectorConfig config;
    private final ConfluenceClient client;
    private final ConfluenceMapper mapper;
    private final long attachmentMaxBytes;
    private final int downloadChunkBytes;
    private final ChangeSink downstream;
    private final ConnectionServiceGrpc.ConnectionServiceBlockingStub connections;
    private final SyncTableChangeSink ledgerSink;

    /**
     * Creates a service with no downstream {@link ChangeSink}.
     *
     * @param config connector config
     * @param client the authorized REST client
     * @param attachmentMaxBytes inline attachment byte cap; must be positive
     */
    public ConfluenceGrpcService(ConfluenceConnectorConfig config, ConfluenceClient client,
            long attachmentMaxBytes) {
        this(config, client, attachmentMaxBytes, null);
    }

    /**
     * Creates a service. When {@code downstream} is non-null, {@code Sync} also
     * fans out to that sink.
     *
     * @param config connector config
     * @param client the authorized REST client
     * @param attachmentMaxBytes inline attachment byte cap; must be positive
     * @param downstream optional extra sink for {@code Sync} emissions; {@code null} for none
     */
    public ConfluenceGrpcService(ConfluenceConnectorConfig config, ConfluenceClient client,
            long attachmentMaxBytes, ChangeSink downstream) {
        this(config, client, attachmentMaxBytes, downstream, null, null);
    }

    /**
     * Creates a service that can resolve catalog connections over
     * {@code ConnectionService}. The default {@code config}/{@code client}
     * are used when {@code connection_id} is empty.
     *
     * @param config process-default connector config; {@code null} when only
     *        catalog connections are used
     * @param client process-default REST client; {@code null} when {@code config} is
     * @param attachmentMaxBytes inline attachment byte cap; must be positive
     * @param downstream optional extra sink for {@code Sync}
     * @param connections generated {@code ConnectionService} stub; {@code null}
     *        when the catalog is not configured
     * @param ledgerSink optional ledger sink so {@code Sync} can stamp
     *        {@code connection_id}; {@code null} if the ledger is not wired
     */
    public ConfluenceGrpcService(ConfluenceConnectorConfig config, ConfluenceClient client,
            long attachmentMaxBytes, ChangeSink downstream,
            ConnectionServiceGrpc.ConnectionServiceBlockingStub connections,
            SyncTableChangeSink ledgerSink) {
        this(config, client, attachmentMaxBytes, downstream, connections, ledgerSink,
                DEFAULT_DOWNLOAD_CHUNK_BYTES);
    }

    /**
     * Creates a service with an explicit {@code DownloadAttachment} chunk
     * default (the size of one data message when the request sets
     * {@code chunk_bytes = 0}).
     *
     * @param config process-default connector config; {@code null} when only
     *        catalog connections are used
     * @param client process-default REST client; {@code null} when {@code config} is
     * @param attachmentMaxBytes inline attachment byte cap; must be positive
     * @param downstream optional extra sink for {@code Sync}
     * @param connections generated {@code ConnectionService} stub; {@code null}
     *        when the catalog is not configured
     * @param ledgerSink optional ledger sink so {@code Sync} can stamp
     *        {@code connection_id}; {@code null} if the ledger is not wired
     * @param downloadChunkBytes server default bytes per download message; must be positive
     */
    public ConfluenceGrpcService(ConfluenceConnectorConfig config, ConfluenceClient client,
            long attachmentMaxBytes, ChangeSink downstream,
            ConnectionServiceGrpc.ConnectionServiceBlockingStub connections,
            SyncTableChangeSink ledgerSink, int downloadChunkBytes) {
        if (config == null && connections == null) {
            throw new IllegalArgumentException("config or ConnectionService stub is required");
        }
        this.config = config;
        this.client = client;
        this.mapper = config == null ? null : new ConfluenceMapper(config.baseUrl());
        if (attachmentMaxBytes <= 0) {
            throw new IllegalArgumentException("attachmentMaxBytes must be positive");
        }
        if (downloadChunkBytes <= 0) {
            throw new IllegalArgumentException("downloadChunkBytes must be positive");
        }
        this.attachmentMaxBytes = attachmentMaxBytes;
        this.downloadChunkBytes = downloadChunkBytes;
        this.downstream = downstream;
        this.connections = connections;
        this.ledgerSink = ledgerSink;
    }

    // ======================================================================
    // SPACES
    // ======================================================================

    @Override
    public void listSpaces(ListSpacesRequest request,
            StreamObserver<ListSpacesResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = spacesQuery(session, request.getKeysList(), request.getIdsList(),
                    request.getType(), request.getStatus(), request.getLabelsList(),
                    request.getFavoritedBy(), request.getNotFavoritedBy(), request.getSort(),
                    request.getDescriptionFormat(), request.getIncludeIcon());
            int cap = request.getLimit();
            List<Space> spaces = new ArrayList<>();
            walk(session, "/api/v2/spaces", query.map(), node -> {
                if (cap <= 0 || spaces.size() < cap) {
                    spaces.add(requireValid(session.mapper.toSpace(node)));
                }
            }, cap > 0 ? () -> spaces.size() >= cap : () -> false);
            observer.onNext(ListSpacesResponse.newBuilder().addAllSpaces(spaces).build());
            observer.onCompleted();
        });
    }

    @Override
    public void streamSpaces(StreamSpacesRequest request,
            StreamObserver<StreamSpacesResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = spacesQuery(session, request.getKeysList(), request.getIdsList(),
                    request.getType(), request.getStatus(), request.getLabelsList(),
                    request.getFavoritedBy(), request.getNotFavoritedBy(), request.getSort(),
                    request.getDescriptionFormat(), request.getIncludeIcon());
            int cap = request.getLimit();
            int[] sent = {0};
            walk(session, "/api/v2/spaces", query.map(), node -> {
                if (cap <= 0 || sent[0] < cap) {
                    observer.onNext(StreamSpacesResponse.newBuilder()
                            .setSpace(requireValid(session.mapper.toSpace(node))).build());
                    sent[0]++;
                }
            }, cap > 0 ? () -> sent[0] >= cap : () -> false);
            observer.onCompleted();
        });
    }

    /** {@code GET /spaces} query shared by ListSpaces and StreamSpaces. */
    private static Query spacesQuery(Session session, List<String> keys, List<String> ids,
            SpaceType type, SpaceStatus status, List<String> labels, String favoritedBy,
            String notFavoritedBy, SortOrder sort, SpaceDescriptionFormat descriptionFormat,
            boolean includeIcon) {
        Query query = new Query()
                .put("limit", String.valueOf(session.config.pageSize()))
                .list("keys", keys)
                .list("ids", ids)
                .list("labels", labels)
                .put("favorited-by", favoritedBy)
                .put("not-favorited-by", notFavoritedBy)
                .put("sort", ConfluenceRoutes.sort(sort, ConfluenceRoutes.SPACE_SORT, "GET /spaces"))
                .put("description-format", descriptionFormat(descriptionFormat))
                .flag("include-icon", includeIcon);
        if (type != SpaceType.SPACE_TYPE_UNSPECIFIED) {
            query.put("type", apiValue(type, "SPACE_TYPE_"));
        }
        if (status != SpaceStatus.SPACE_STATUS_UNSPECIFIED) {
            query.put("status", apiValue(status, "SPACE_STATUS_"));
        }
        return query;
    }

    private static String descriptionFormat(SpaceDescriptionFormat format) {
        return switch (format) {
            case SPACE_DESCRIPTION_FORMAT_PLAIN -> "plain";
            case SPACE_DESCRIPTION_FORMAT_VIEW -> "view";
            case SPACE_DESCRIPTION_FORMAT_UNSPECIFIED -> null;
            case UNRECOGNIZED -> throw invalid("unknown description format");
        };
    }

    @Override
    public void getSpace(GetSpaceRequest request, StreamObserver<GetSpaceResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String id = request.getId();
            if (id.isBlank()) {
                String key = requireId(request.getKey(), "id or key");
                JsonNode found = session.client.get("/api/v2/spaces",
                        new Query().put("keys", key).put("limit", "1").map())
                        .path("results").path(0);
                if (!found.isObject() || found.path("id").asText("").isEmpty()) {
                    throw Status.NOT_FOUND.withDescription("no space with key " + key)
                            .asRuntimeException();
                }
                id = found.path("id").asText();
            }
            SpaceIncludes include = request.getInclude();
            Query query = new Query()
                    .put("description-format", descriptionFormat(request.getDescriptionFormat()))
                    .flag("include-icon", include.getIcon())
                    .flag("include-operations", include.getOperations())
                    .flag("include-properties", include.getProperties())
                    .flag("include-permissions", include.getPermissions())
                    .flag("include-role-assignments", include.getRoleAssignments())
                    .flag("include-labels", include.getLabels());
            JsonNode node = session.client.get("/api/v2/spaces/" + id, query.map());
            reply(observer, GetSpaceResponse.newBuilder()
                    .setSpace(requireValid(session.mapper.toSpace(node))).build());
        });
    }

    // ======================================================================
    // PAGES AND BLOG POSTS
    // ======================================================================

    /** Statuses {@code GET /pages/{id}} accepts. */
    private static final Set<String> PAGE_SINGLE_STATUSES =
            values("current", "archived", "trashed", "deleted", "historical", "draft");
    /** Statuses {@code GET /pages} and {@code GET /spaces/{id}/pages} accept. */
    private static final Set<String> PAGE_LIST_STATUSES =
            values("current", "archived", "deleted", "trashed");
    /** Statuses {@code GET /blogposts/{id}} accepts. */
    private static final Set<String> BLOG_POST_SINGLE_STATUSES =
            values("current", "trashed", "deleted", "historical", "draft");
    /** Statuses {@code GET /blogposts} accepts. */
    private static final Set<String> BLOG_POST_LIST_STATUSES =
            values("current", "deleted", "trashed");

    @Override
    public void getPage(GetPageRequest request, StreamObserver<GetPageResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String route = "GET /pages/{id}";
            Query query = singleBodyQuery(request.getBodyFormat(), ConfluenceRoutes.SINGLE_BODY, route)
                    .flag("get-draft", request.getGetDraft())
                    .put("status", ConfluenceRoutes.contentStatuses(request.getStatusesList(),
                            PAGE_SINGLE_STATUSES, route));
            if (request.hasVersion()) {
                query.put("version", String.valueOf(request.getVersion()));
            }
            includes(query, request.getInclude(), PAGE_INCLUDES);
            JsonNode node = session.client.get("/api/v2/pages/" + requireId(request.getId(), "id"),
                    query.map());
            reply(observer, GetPageResponse.newBuilder()
                    .setPage(requireValid(session.mapper.toPage(node))).build());
        });
    }

    @Override
    public void getBlogPost(GetBlogPostRequest request,
            StreamObserver<GetBlogPostResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String route = "GET /blogposts/{id}";
            Query query = singleBodyQuery(request.getBodyFormat(), ConfluenceRoutes.SINGLE_BODY, route)
                    .flag("get-draft", request.getGetDraft())
                    .put("status", ConfluenceRoutes.blogPostStatuses(request.getStatusesList(),
                            BLOG_POST_SINGLE_STATUSES, route));
            if (request.hasVersion()) {
                query.put("version", String.valueOf(request.getVersion()));
            }
            includes(query, request.getInclude(), BLOG_POST_INCLUDES);
            JsonNode node = session.client.get(
                    "/api/v2/blogposts/" + requireId(request.getId(), "id"), query.map());
            reply(observer, GetBlogPostResponse.newBuilder()
                    .setBlogPost(requireValid(session.mapper.toBlogPost(node))).build());
        });
    }

    @Override
    public void listPages(ListPagesRequest request, StreamObserver<ListPagesResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            List<String> spaceIds = spaceIds(request.getSpaceId(), request.getSpaceIdsList());
            Query query = limit(session);
            String path;
            String route;
            if (!request.getLabelId().isBlank()) {
                route = "GET /labels/{id}/pages";
                path = "/api/v2/labels/" + request.getLabelId() + "/pages";
                reject(!request.getIdsList().isEmpty(), "ids", route);
                reject(!request.getStatusesList().isEmpty(), "statuses", route);
                reject(!request.getTitle().isEmpty(), "title", route);
                reject(request.getSubtype() != PageSubtype.PAGE_SUBTYPE_UNSPECIFIED, "subtype", route);
                reject(request.getRootOnly(), "root_only", route);
                query.list("space-id", spaceIds);
            } else if (request.getRootOnly()) {
                route = "GET /spaces/{id}/pages";
                path = "/api/v2/spaces/" + requireId(request.getSpaceId(), "space_id (root_only)")
                        + "/pages";
                reject(!request.getIdsList().isEmpty(), "ids", route);
                reject(!request.getSpaceIdsList().isEmpty(), "space_ids", route);
                reject(request.getSubtype() != PageSubtype.PAGE_SUBTYPE_UNSPECIFIED, "subtype", route);
                query.put("depth", "root")
                        .put("status", ConfluenceRoutes.contentStatuses(request.getStatusesList(),
                                PAGE_LIST_STATUSES, route))
                        .put("title", request.getTitle());
            } else {
                route = "GET /pages";
                path = "/api/v2/pages";
                query.list("id", request.getIdsList())
                        .list("space-id", spaceIds)
                        .put("status", ConfluenceRoutes.contentStatuses(request.getStatusesList(),
                                PAGE_LIST_STATUSES, route))
                        .put("title", request.getTitle());
                if (request.getSubtype() != PageSubtype.PAGE_SUBTYPE_UNSPECIFIED) {
                    query.put("subtype", apiValue(request.getSubtype(), "PAGE_SUBTYPE_"));
                }
            }
            query.put("sort", ConfluenceRoutes.sort(request.getSort(), ConfluenceRoutes.PAGE_SORT, route));
            listBodyFormat(query, request.getBodyFormat(), ConfluenceRoutes.PRIMARY_BODY, route);
            walk(session, path, query.map(), node -> observer.onNext(ListPagesResponse.newBuilder()
                    .setPage(requireValid(session.mapper.toPage(node))).build()));
            observer.onCompleted();
        });
    }

    @Override
    public void listBlogPosts(ListBlogPostsRequest request,
            StreamObserver<ListBlogPostsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            List<String> spaceIds = spaceIds(request.getSpaceId(), request.getSpaceIdsList());
            Query query = limit(session).list("space-id", spaceIds);
            String path;
            String route;
            if (!request.getLabelId().isBlank()) {
                route = "GET /labels/{id}/blogposts";
                path = "/api/v2/labels/" + request.getLabelId() + "/blogposts";
                reject(!request.getIdsList().isEmpty(), "ids", route);
                reject(!request.getStatusesList().isEmpty(), "statuses", route);
                reject(!request.getTitle().isEmpty(), "title", route);
            } else {
                route = "GET /blogposts";
                path = "/api/v2/blogposts";
                query.list("id", request.getIdsList())
                        .put("status", ConfluenceRoutes.blogPostStatuses(request.getStatusesList(),
                                BLOG_POST_LIST_STATUSES, route))
                        .put("title", request.getTitle());
            }
            query.put("sort", ConfluenceRoutes.sort(request.getSort(),
                    ConfluenceRoutes.BLOG_POST_SORT, route));
            listBodyFormat(query, request.getBodyFormat(), ConfluenceRoutes.PRIMARY_BODY, route);
            walk(session, path, query.map(), node -> observer.onNext(
                    ListBlogPostsResponse.newBuilder()
                            .setBlogPost(requireValid(session.mapper.toBlogPost(node))).build()));
            observer.onCompleted();
        });
    }

    /** {@code space_id} or {@code space_ids}; both set is ambiguous. */
    private static List<String> spaceIds(String spaceId, List<String> spaceIds) {
        if (spaceId.isBlank()) {
            return spaceIds;
        }
        if (!spaceIds.isEmpty()) {
            throw invalid("set space_id or space_ids, not both");
        }
        return List.of(spaceId);
    }

    // ======================================================================
    // ATTACHMENTS
    // ======================================================================

    /** Statuses the attachment list routes accept. */
    private static final Set<String> ATTACHMENT_STATUSES = values("current", "archived", "trashed");

    @Override
    public void getAttachment(GetAttachmentRequest request,
            StreamObserver<GetAttachmentResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            if (request.getMaxContentBytes() < 0) {
                throw invalid("max_content_bytes must not be negative");
            }
            Query query = new Query();
            if (request.hasVersion()) {
                query.put("version", String.valueOf(request.getVersion()));
            }
            includes(query, request.getInclude(), ATTACHMENT_INCLUDES);
            JsonNode node = session.client.get(
                    "/api/v2/attachments/" + requireId(request.getId(), "id"), query.map());
            Attachment attachment = requireValid(session.mapper.toAttachment(node));
            if (request.getIncludeContent()) {
                // The caller's ceiling when set; otherwise the server's configured inline cap.
                long cap = request.getMaxContentBytes() > 0
                        ? request.getMaxContentBytes() : attachmentMaxBytes;
                attachment = requireValid(withContent(session, attachment, cap));
            }
            reply(observer, GetAttachmentResponse.newBuilder().setAttachment(attachment).build());
        });
    }

    @Override
    public void listAttachments(ListAttachmentsRequest request,
            StreamObserver<ListAttachmentsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = limit(session);
            String path;
            String route;
            if (!request.getPageId().isBlank()) {
                path = "/api/v2/pages/" + request.getPageId() + "/attachments";
                route = "GET /pages/{id}/attachments";
            } else if (!request.getBlogPostId().isBlank()) {
                path = "/api/v2/blogposts/" + request.getBlogPostId() + "/attachments";
                route = "GET /blogposts/{id}/attachments";
            } else if (!request.getCustomContentId().isBlank()) {
                path = "/api/v2/custom-content/" + request.getCustomContentId() + "/attachments";
                route = "GET /custom-content/{id}/attachments";
            } else if (!request.getLabelId().isBlank()) {
                path = "/api/v2/labels/" + request.getLabelId() + "/attachments";
                route = "GET /labels/{id}/attachments";
            } else {
                path = "/api/v2/attachments";
                route = "GET /attachments";
            }
            if (!route.equals("GET /labels/{id}/attachments")) {
                query.put("status", ConfluenceRoutes.contentStatuses(request.getStatusesList(),
                                ATTACHMENT_STATUSES, route))
                        .put("mediaType", request.getMediaType())
                        .put("filename", request.getFilename());
            } else {
                reject(!request.getStatusesList().isEmpty(), "statuses", route);
                reject(!request.getMediaType().isEmpty(), "media_type", route);
                reject(!request.getFilename().isEmpty(), "filename", route);
            }
            query.put("sort", ConfluenceRoutes.sort(request.getSort(), ConfluenceRoutes.DATED_SORT, route));
            stream(session, path, query, observer, node -> ListAttachmentsResponse.newBuilder()
                    .setAttachment(requireValid(session.mapper.toAttachment(node))).build());
        });
    }

    @Override
    public void downloadAttachment(DownloadAttachmentRequest request,
            StreamObserver<DownloadAttachmentResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            if (request.getMaxBytes() < 0) {
                throw invalid("max_bytes must not be negative");
            }
            if (request.getChunkBytes() < 0) {
                throw invalid("chunk_bytes must not be negative");
            }
            Query query = new Query();
            if (request.hasVersion()) {
                query.put("version", String.valueOf(request.getVersion()));
            }
            JsonNode node = session.client.get(
                    "/api/v2/attachments/" + requireId(request.getId(), "id"), query.map());
            Attachment attachment = requireValid(session.mapper.toAttachment(node));
            long max = request.getMaxBytes();
            if (max > 0 && attachment.getFileSize() > max) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("attachment " + attachment.getId() + " is "
                                + attachment.getFileSize() + " bytes, above the requested max_bytes "
                                + max)
                        .asRuntimeException();
            }
            if (attachment.getDownloadUrl().isBlank()) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("attachment " + attachment.getId() + " has no download link")
                        .asRuntimeException();
            }
            int chunk = request.getChunkBytes() > 0 ? request.getChunkBytes() : downloadChunkBytes;
            FlowControl flow = FlowControl.of(observer);
            flow.send(DownloadAttachmentResponse.newBuilder().setAttachment(attachment).build());
            long total = 0;
            try (java.io.InputStream in = session.client.openAttachmentStream(
                    attachment.getDownloadUrl())) {
                byte[] buffer = new byte[chunk];
                while (true) {
                    int read = in.readNBytes(buffer, 0, chunk);
                    if (read <= 0) {
                        break;
                    }
                    total += read;
                    if (max > 0 && total > max) {
                        // file_size understated the binary: the caller's ceiling still holds.
                        throw Status.FAILED_PRECONDITION
                                .withDescription("attachment " + attachment.getId()
                                        + " exceeded the requested max_bytes " + max)
                                .asRuntimeException();
                    }
                    if (!flow.send(DownloadAttachmentResponse.newBuilder()
                            .setData(ByteString.copyFrom(buffer, 0, read)).build())) {
                        return; // the client cancelled; nothing left to answer
                    }
                    if (read < chunk) {
                        break;
                    }
                }
            }
            observer.onCompleted();
        });
    }

    /**
     * Outbound flow control for a server stream: {@link #send} parks the
     * (virtual) handler thread until the transport is ready, so a slow
     * client never makes the server buffer a whole binary in memory.
     */
    private static final class FlowControl {
        private final StreamObserver<DownloadAttachmentResponse> observer;
        private final io.grpc.stub.ServerCallStreamObserver<DownloadAttachmentResponse> call;
        private final Object ready = new Object();
        private volatile boolean cancelled;

        private FlowControl(StreamObserver<DownloadAttachmentResponse> observer) {
            this.observer = observer;
            this.call = observer instanceof io.grpc.stub.ServerCallStreamObserver<DownloadAttachmentResponse> c
                    ? c : null;
            if (call != null) {
                call.setOnReadyHandler(this::wake);
                call.setOnCancelHandler(() -> {
                    cancelled = true;
                    wake();
                });
            }
        }

        static FlowControl of(StreamObserver<DownloadAttachmentResponse> observer) {
            return new FlowControl(observer);
        }

        private void wake() {
            synchronized (ready) {
                ready.notifyAll();
            }
        }

        /** Sends once the transport is ready; false when the call was cancelled. */
        boolean send(DownloadAttachmentResponse message) throws InterruptedException {
            if (call != null) {
                synchronized (ready) {
                    while (!cancelled && !call.isReady()) {
                        ready.wait(1000);
                    }
                }
                if (cancelled || call.isCancelled()) {
                    return false;
                }
            }
            observer.onNext(message);
            return true;
        }
    }

    // ======================================================================
    // COMMENTS
    // ======================================================================

    /** Statuses {@code GET /pages/{id}/{footer,inline}-comments} accept. */
    private static final Set<String> PAGE_COMMENT_STATUSES =
            values("current", "archived", "trashed", "deleted", "historical", "draft");
    /** Statuses {@code GET /blogposts/{id}/{footer,inline}-comments} accept. */
    private static final Set<String> BLOG_POST_COMMENT_STATUSES =
            values("current", "deleted", "trashed", "historical", "draft");

    /** One comment route to walk and the kind its comments carry. */
    private record CommentRoute(String path, String route, CommentKind kind) {
    }

    /**
     * Scope resolution follows the request docs: {@code parent_comment_id},
     * else {@code container}, else the workspace. Kind UNSPECIFIED means
     * both kinds where a route serves both (footer first), except that
     * resolution filters only exist on inline comments and so select the
     * inline route alone; for replies it means footer, then inline when the
     * footer route does not know the comment.
     */
    @Override
    public void listComments(ListCommentsRequest request,
            StreamObserver<ListCommentsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            CommentKind kind = request.getKind();
            boolean resolution = !request.getResolutionStatusesList().isEmpty();
            if (resolution && kind == CommentKind.COMMENT_KIND_FOOTER) {
                throw invalid("resolution_statuses only apply to inline comments");
            }
            List<CommentRoute> routes = new ArrayList<>();
            boolean fallBackToInline = false;
            Set<String> statuses = null;
            if (!request.getParentCommentId().isBlank()) {
                String id = request.getParentCommentId();
                if (kind != CommentKind.COMMENT_KIND_INLINE && !resolution) {
                    routes.add(new CommentRoute("/api/v2/footer-comments/" + id + "/children",
                            "GET /footer-comments/{id}/children", CommentKind.COMMENT_KIND_FOOTER));
                }
                if (kind != CommentKind.COMMENT_KIND_FOOTER) {
                    routes.add(new CommentRoute("/api/v2/inline-comments/" + id + "/children",
                            "GET /inline-comments/{id}/children", CommentKind.COMMENT_KIND_INLINE));
                }
                fallBackToInline = routes.size() == 2;
                reject(!request.getStatusesList().isEmpty(), "statuses", "GET /{kind}-comments/{id}/children");
                reject(resolution, "resolution_statuses", "GET /{kind}-comments/{id}/children");
            } else if (request.hasContainer()) {
                ContentRef container = request.getContainer();
                String base = ConfluenceRoutes.contentPath(container, java.util.EnumSet.of(
                        ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST,
                        ContentType.CONTENT_TYPE_ATTACHMENT, ContentType.CONTENT_TYPE_CUSTOM_CONTENT),
                        "ListComments container");
                String segment = ConfluenceRoutes.segment(container.getType());
                boolean inlineCapable = container.getType() == ContentType.CONTENT_TYPE_PAGE
                        || container.getType() == ContentType.CONTENT_TYPE_BLOG_POST;
                if (!inlineCapable && (kind == CommentKind.COMMENT_KIND_INLINE || resolution)) {
                    throw invalid("inline comments exist on pages and blog posts only");
                }
                if (kind != CommentKind.COMMENT_KIND_INLINE && !resolution) {
                    routes.add(new CommentRoute(base + "/footer-comments",
                            "GET /" + segment + "/{id}/footer-comments", CommentKind.COMMENT_KIND_FOOTER));
                }
                if (inlineCapable && kind != CommentKind.COMMENT_KIND_FOOTER) {
                    routes.add(new CommentRoute(base + "/inline-comments",
                            "GET /" + segment + "/{id}/inline-comments", CommentKind.COMMENT_KIND_INLINE));
                }
                if (inlineCapable) {
                    statuses = container.getType() == ContentType.CONTENT_TYPE_PAGE
                            ? PAGE_COMMENT_STATUSES : BLOG_POST_COMMENT_STATUSES;
                } else {
                    reject(!request.getStatusesList().isEmpty(), "statuses",
                            "GET /" + segment + "/{id}/footer-comments");
                }
            } else {
                reject(!request.getStatusesList().isEmpty(), "statuses",
                        "GET /footer-comments, GET /inline-comments");
                reject(resolution, "resolution_statuses", "GET /inline-comments");
                if (kind != CommentKind.COMMENT_KIND_INLINE) {
                    routes.add(new CommentRoute("/api/v2/footer-comments", "GET /footer-comments",
                            CommentKind.COMMENT_KIND_FOOTER));
                }
                if (kind != CommentKind.COMMENT_KIND_FOOTER) {
                    routes.add(new CommentRoute("/api/v2/inline-comments", "GET /inline-comments",
                            CommentKind.COMMENT_KIND_INLINE));
                }
            }
            for (CommentRoute route : routes) {
                Query query = limit(session)
                        .put("sort", ConfluenceRoutes.sort(request.getSort(),
                                ConfluenceRoutes.DATED_SORT, route.route()));
                listBodyFormat(query, request.getBodyFormat(), ConfluenceRoutes.PRIMARY_BODY,
                        route.route());
                if (statuses != null) {
                    query.put("status", ConfluenceRoutes.contentStatuses(request.getStatusesList(),
                            statuses, route.route()));
                }
                if (route.kind() == CommentKind.COMMENT_KIND_INLINE && resolution) {
                    List<String> wire = new ArrayList<>();
                    for (InlineCommentResolutionStatus status : request.getResolutionStatusesList()) {
                        wire.add(ConfluenceRoutes.status(status.getValueDescriptor().getName(),
                                "INLINE_COMMENT_RESOLUTION_STATUS_",
                                values("resolved", "open", "dangling", "reopened"), route.route()));
                    }
                    query.list("resolution-status", wire);
                }
                Consumer<JsonNode> emit = node -> observer.onNext(ListCommentsResponse.newBuilder()
                        .setComment(requireValid(session.mapper.toComment(node, route.kind())))
                        .build());
                if (fallBackToInline && route.kind() == CommentKind.COMMENT_KIND_FOOTER) {
                    if (walkIfFound(session, route.path(), query.map(), emit)) {
                        break; // the parent is a footer comment: its replies are footer comments
                    }
                    continue;
                }
                walk(session, route.path(), query.map(), emit);
            }
            observer.onCompleted();
        });
    }

    @Override
    public void getComment(GetCommentRequest request, StreamObserver<GetCommentResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String id = requireId(request.getId(), "id");
            Query query = singleBodyQuery(request.getBodyFormat(), ConfluenceRoutes.SINGLE_BODY,
                    "GET /{kind}-comments/{id}");
            if (request.hasVersion()) {
                query.put("version", String.valueOf(request.getVersion()));
            }
            includes(query, request.getInclude(), COMMENT_INCLUDES);
            CommentKind kind = request.getKind();
            JsonNode node;
            if (kind == CommentKind.COMMENT_KIND_INLINE) {
                node = session.client.get("/api/v2/inline-comments/" + id, query.map());
            } else if (kind == CommentKind.COMMENT_KIND_FOOTER) {
                node = session.client.get("/api/v2/footer-comments/" + id, query.map());
            } else {
                try {
                    node = session.client.get("/api/v2/footer-comments/" + id, query.map());
                    kind = CommentKind.COMMENT_KIND_FOOTER;
                } catch (ConfluenceClient.ConfluenceApiException e) {
                    if (e.status() != 404) {
                        throw e;
                    }
                    node = session.client.get("/api/v2/inline-comments/" + id, query.map());
                    kind = CommentKind.COMMENT_KIND_INLINE;
                }
            }
            reply(observer, GetCommentResponse.newBuilder()
                    .setComment(requireValid(session.mapper.toComment(node, kind))).build());
        });
    }

    // ======================================================================
    // CUSTOM CONTENT
    // ======================================================================

    @Override
    public void listCustomContent(ListCustomContentRequest request,
            StreamObserver<ListCustomContentResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = limit(session).put("type", requireId(request.getType(), "type"));
            String path;
            String route;
            if (request.hasContainer()) {
                ContentRef container = request.getContainer();
                path = ConfluenceRoutes.contentPath(container, java.util.EnumSet.of(
                        ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST),
                        "ListCustomContent container (a space scope is space_ids)") + "/custom-content";
                route = "GET /" + ConfluenceRoutes.segment(container.getType()) + "/{id}/custom-content";
                reject(!request.getIdsList().isEmpty(), "ids", route);
                reject(!request.getSpaceIdsList().isEmpty(), "space_ids", route);
            } else {
                path = "/api/v2/custom-content";
                route = "GET /custom-content";
                query.list("id", request.getIdsList()).list("space-id", request.getSpaceIdsList());
            }
            query.put("sort", ConfluenceRoutes.sort(request.getSort(),
                    ConfluenceRoutes.CUSTOM_CONTENT_SORT, route));
            listBodyFormat(query, request.getBodyFormat(), ConfluenceRoutes.CUSTOM_BODY, route);
            stream(session, path, query, observer, node -> ListCustomContentResponse.newBuilder()
                    .setCustomContent(requireValid(session.mapper.toCustomContent(node))).build());
        });
    }

    @Override
    public void getCustomContent(GetCustomContentRequest request,
            StreamObserver<GetCustomContentResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = singleBodyQuery(request.getBodyFormat(),
                    ConfluenceRoutes.CUSTOM_SINGLE_BODY, "GET /custom-content/{id}");
            if (request.hasVersion()) {
                query.put("version", String.valueOf(request.getVersion()));
            }
            includes(query, request.getInclude(), ATTACHMENT_INCLUDES);
            JsonNode node = session.client.get(
                    "/api/v2/custom-content/" + requireId(request.getId(), "id"), query.map());
            reply(observer, GetCustomContentResponse.newBuilder()
                    .setCustomContent(requireValid(session.mapper.toCustomContent(node))).build());
        });
    }

    // ======================================================================
    // WHITEBOARDS, DATABASES, FOLDERS, SMART LINKS, CONTENT TREE
    // ======================================================================

    @Override
    public void getWhiteboard(GetWhiteboardRequest request,
            StreamObserver<GetWhiteboardResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode node = getTreeNode(session, "whiteboards", request.getId(), request.getInclude());
            reply(observer, GetWhiteboardResponse.newBuilder()
                    .setWhiteboard(requireValid(session.mapper.toWhiteboard(node))).build());
        });
    }

    @Override
    public void getDatabase(GetDatabaseRequest request, StreamObserver<GetDatabaseResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode node = getTreeNode(session, "databases", request.getId(), request.getInclude());
            reply(observer, GetDatabaseResponse.newBuilder()
                    .setDatabase(requireValid(session.mapper.toDatabase(node))).build());
        });
    }

    @Override
    public void getFolder(GetFolderRequest request, StreamObserver<GetFolderResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode node = getTreeNode(session, "folders", request.getId(), request.getInclude());
            reply(observer, GetFolderResponse.newBuilder()
                    .setFolder(requireValid(session.mapper.toFolder(node))).build());
        });
    }

    @Override
    public void getSmartLink(GetSmartLinkRequest request,
            StreamObserver<GetSmartLinkResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode node = getTreeNode(session, "embeds", request.getId(), request.getInclude());
            reply(observer, GetSmartLinkResponse.newBuilder()
                    .setSmartLink(requireValid(session.mapper.toSmartLink(node))).build());
        });
    }

    /** {@code GET /{whiteboards|databases|folders|embeds}/{id}} with the tree include-* flags. */
    private static JsonNode getTreeNode(Session session, String segment, String id,
            ContentIncludes include) throws IOException, InterruptedException {
        Query query = new Query();
        includes(query, include, TREE_INCLUDES);
        return session.client.get("/api/v2/" + segment + "/" + requireId(id, "id"), query.map());
    }

    @Override
    public void listChildren(ListChildrenRequest request,
            StreamObserver<ListChildrenResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            ContentRef parent = request.getParent();
            String path;
            String route;
            Set<SortField> sortable;
            if (parent.getType() == ContentType.CONTENT_TYPE_CUSTOM_CONTENT) {
                path = "/api/v2/custom-content/" + requireId(parent.getId(), "parent id") + "/children";
                route = "GET /custom-content/{id}/children";
                sortable = ConfluenceRoutes.CHILD_CUSTOM_CONTENT_SORT;
            } else {
                path = ConfluenceRoutes.contentPath(parent, ConfluenceRoutes.TREE_NODES,
                        "ListChildren") + "/direct-children";
                route = "GET /" + ConfluenceRoutes.segment(parent.getType()) + "/{id}/direct-children";
                sortable = ConfluenceRoutes.TREE_SORT;
            }
            Query query = limit(session).put("sort",
                    ConfluenceRoutes.sort(request.getSort(), sortable, route));
            stream(session, path, query, observer, node -> ListChildrenResponse.newBuilder()
                    .setEntry(requireValid(session.mapper.toTreeEntry(node))).build());
        });
    }

    @Override
    public void listDescendants(ListDescendantsRequest request,
            StreamObserver<ListDescendantsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String path = ConfluenceRoutes.contentPath(request.getRoot(), ConfluenceRoutes.TREE_NODES,
                    "ListDescendants") + "/descendants";
            if (request.getDepth() < 0) {
                throw invalid("depth must not be negative");
            }
            Query query = limit(session);
            if (request.getDepth() > 0) {
                query.put("depth", String.valueOf(request.getDepth()));
            }
            stream(session, path, query, observer, node -> ListDescendantsResponse.newBuilder()
                    .setEntry(requireValid(session.mapper.toTreeEntry(node))).build());
        });
    }

    /**
     * The API answers ancestors top-down, at most {@code limit} at a time,
     * and continues "by calling this endpoint with the ID of the first
     * ancestor": when a call comes back full, the walk repeats from the
     * highest ancestor (on its own kind's route) and prepends the result.
     */
    @Override
    public void listAncestors(ListAncestorsRequest request,
            StreamObserver<ListAncestorsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            List<JsonNode> chain = new ArrayList<>();
            java.util.Set<String> seen = new java.util.HashSet<>();
            String path = ConfluenceRoutes.contentPath(request.getContent(), ConfluenceRoutes.TREE_NODES,
                    "ListAncestors");
            while (path != null) {
                JsonNode body = session.client.get(path + "/ancestors",
                        Map.of("limit", String.valueOf(MAX_LIMIT)));
                List<JsonNode> batch = new ArrayList<>();
                body.path("results").forEach(batch::add);
                chain.addAll(0, batch);
                path = null;
                if (batch.size() >= MAX_LIMIT) {
                    JsonNode top = batch.getFirst();
                    String segment = ancestorSegment(top.path("type").asText(""));
                    String id = top.path("id").asText("");
                    if (segment != null && !id.isEmpty() && seen.add(segment + "/" + id)) {
                        path = "/api/v2/" + segment + "/" + id;
                    }
                }
            }
            for (JsonNode node : chain) {
                observer.onNext(ListAncestorsResponse.newBuilder()
                        .setAncestor(session.mapper.toAncestor(node)).build());
            }
            observer.onCompleted();
        });
    }

    /** The route segment of an ancestor's {@code type}; null when unknown. */
    private static String ancestorSegment(String type) {
        return switch (type) {
            case "page" -> "pages";
            case "whiteboard" -> "whiteboards";
            case "database" -> "databases";
            case "embed" -> "embeds";
            case "folder" -> "folders";
            default -> null;
        };
    }

    // ======================================================================
    // DATABASE DATA
    // ======================================================================

    @Override
    public void listDatabaseFields(ListDatabaseFieldsRequest request,
            StreamObserver<ListDatabaseFieldsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            stream(session, "/api/v2/databases/" + requireId(request.getDatabaseId(), "database_id")
                    + "/fields", limit(session), observer, node -> ListDatabaseFieldsResponse.newBuilder()
                    .setField(session.mapper.toDatabaseField(node)).build());
        });
    }

    @Override
    public void listDatabaseEntries(ListDatabaseEntriesRequest request,
            StreamObserver<ListDatabaseEntriesResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = limit(session);
            for (DatabaseEntryFilter filter : request.getFiltersList()) {
                putEntryFilter(query, filter);
            }
            if (request.hasSort()) {
                String field = requireId(request.getSort().getFieldId(), "sort.field_id");
                query.put("sort", (request.getSort().getDescending() ? "-" : "") + field);
            }
            stream(session, "/api/v2/databases/" + requireId(request.getDatabaseId(), "database_id")
                    + "/entries", query, observer, node -> ListDatabaseEntriesResponse.newBuilder()
                    .setEntry(session.mapper.toDatabaseEntry(node)).build());
        });
    }

    /**
     * One filter in the spec's bracket grammar: {@code filter[f]=eq:v},
     * {@code filter[f]=in:a,b}, {@code filter[f]=$empty} and
     * {@code filter[f][gt|gte|lt|lte]=v}. Explicit {@code eq:} is always
     * used so a literal that looks like an operator stays a literal.
     */
    static void putEntryFilter(Query query, DatabaseEntryFilter filter) {
        String field = requireId(filter.getFieldId(), "filter.field_id");
        String scoped = "filter[" + field + "]";
        List<String> values = filter.getValuesList();
        String name;
        String value;
        switch (filter.getOperator()) {
            case DATABASE_ENTRY_FILTER_OPERATOR_UNSPECIFIED, DATABASE_ENTRY_FILTER_OPERATOR_EQUALS -> {
                name = scoped;
                value = "eq:" + single(filter);
            }
            case DATABASE_ENTRY_FILTER_OPERATOR_IN -> {
                if (values.isEmpty()) {
                    throw invalid("filter on " + field + ": IN needs at least one value");
                }
                for (String member : values) {
                    if (member.indexOf(',') >= 0) {
                        throw invalid("filter on " + field + ": an IN member cannot contain a comma"
                                + " (the API has no escape); use EQUALS for that literal");
                    }
                }
                name = scoped;
                value = "in:" + String.join(",", values);
            }
            case DATABASE_ENTRY_FILTER_OPERATOR_EMPTY -> {
                if (!values.isEmpty()) {
                    throw invalid("filter on " + field + ": EMPTY takes no values");
                }
                name = scoped;
                value = "$empty";
            }
            case DATABASE_ENTRY_FILTER_OPERATOR_GREATER_THAN -> {
                name = scoped + "[gt]";
                value = single(filter);
            }
            case DATABASE_ENTRY_FILTER_OPERATOR_GREATER_THAN_OR_EQUAL -> {
                name = scoped + "[gte]";
                value = single(filter);
            }
            case DATABASE_ENTRY_FILTER_OPERATOR_LESS_THAN -> {
                name = scoped + "[lt]";
                value = single(filter);
            }
            case DATABASE_ENTRY_FILTER_OPERATOR_LESS_THAN_OR_EQUAL -> {
                name = scoped + "[lte]";
                value = single(filter);
            }
            default -> throw invalid("unknown filter operator on " + field);
        }
        if (query.has(name)) {
            throw invalid("two filters on " + name + "; the API takes one per field and operator");
        }
        query.put(name, value);
    }

    private static String single(DatabaseEntryFilter filter) {
        if (filter.getValuesCount() != 1) {
            throw invalid("filter on " + filter.getFieldId() + ": " + filter.getOperator()
                    + " takes exactly one value");
        }
        return filter.getValues(0);
    }

    @Override
    public void getDatabaseEntry(GetDatabaseEntryRequest request,
            StreamObserver<GetDatabaseEntryResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode node = session.client.get("/api/v2/databases/"
                    + requireId(request.getDatabaseId(), "database_id") + "/entries/"
                    + requireId(request.getEntryId(), "entry_id"));
            reply(observer, GetDatabaseEntryResponse.newBuilder()
                    .setEntry(session.mapper.toDatabaseEntry(node)).build());
        });
    }

    // ======================================================================
    // SYNC AND PROBE
    // ======================================================================

    @Override
    public void sync(SyncRequest request, StreamObserver<SyncResponse> observer) {
        try {
            Session session = session(request.getConnectionId());
            ConfluenceConnectorConfig effective = request.getSpaceKeysList().isEmpty()
                    ? session.config
                    : new ConfluenceConnectorConfig(session.config.baseUrl(), session.config.email(),
                            session.config.apiToken(), request.getSpaceKeysList(),
                            session.config.pageSize(), session.config.bodyFormat());
            Object lock = new Object();
            AtomicReference<String> newestSnapshotCursor = new AtomicReference<>("");
            AtomicReference<String> runId = new AtomicReference<>("");
            ChangeSink observerSink = new ChangeSink() {
                @Override
                public void emit(ConfluenceChange change) {
                    runId.compareAndSet("", change.getCursor());
                    ConfluenceChange out = request.getIncludeBodies() ? change : stripBodies(change);
                    VALIDATOR.requireValid(out);
                    synchronized (lock) {
                        observer.onNext(SyncResponse.newBuilder().setChange(out).build());
                    }
                }

                @Override
                public void snapshot(ConfluenceSnapshot snapshot) {
                    VALIDATOR.requireValid(snapshot);
                    newestSnapshotCursor.accumulateAndGet(snapshot.getCursor(),
                            ConfluenceGrpcService::laterCursor);
                    synchronized (lock) {
                        observer.onNext(SyncResponse.newBuilder().setSnapshot(snapshot).build());
                    }
                }
            };
            List<ChangeSink> extras = new ArrayList<>();
            extras.add(observerSink);
            if (ledgerSink != null) {
                SyncTableChangeSink bound = ledgerSink.boundTo(session.connectionId);
                if (request.getSpaceKeysList().isEmpty()) {
                    extras.add(bound);
                } else {
                    // A request-scoped subset crawl cannot prove absence outside
                    // its spaces: letting completeRun reconcile would soft-delete
                    // every other space's ledger rows for this connection.
                    extras.add(new ChangeSink() {
                        @Override
                        public void emit(ConfluenceChange change) {
                            bound.emit(change);
                        }

                        @Override
                        public void snapshot(ConfluenceSnapshot snapshot) {
                            bound.snapshot(snapshot);
                        }
                    });
                }
            }
            OutputChangeSink runtimeOutput = bindOutput(session);
            if (runtimeOutput != null) {
                extras.add(runtimeOutput);
            }
            if (downstream != null) {
                extras.add(downstream);
            }
            ChangeSink sink = extras.size() == 1 ? extras.get(0) : new CompositeChangeSink(extras);
            ConfluenceCrawler crawler = new ConfluenceCrawler(effective, session.client, sink);
            try {
                String resumeCursor;
                if (request.getSinceCursor().isBlank()) {
                    crawler.crawl();
                    resumeCursor = newestSnapshotCursor.get();
                    sink.completeRun(runId.get());
                } else {
                    resumeCursor = crawler.crawlIncremental(request.getSinceCursor());
                }
                synchronized (lock) {
                    observer.onNext(SyncResponse.newBuilder().setResumeCursor(resumeCursor).build());
                    observer.onCompleted();
                }
            } finally {
                if (runtimeOutput != null) {
                    runtimeOutput.close();
                }
            }
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void probeConnection(ProbeConnectionRequest request,
            StreamObserver<ProbeConnectionResponse> observer) {
        String connectionId = request.getConnectionId();
        try {
            Session session;
            if (!request.getBaseUrl().isBlank() && !request.getEmail().isBlank()
                    && !request.getToken().isBlank()) {
                ConfluenceConnectorConfig inline = ConfluenceConnectorConfig.builder()
                        .baseUrl(request.getBaseUrl())
                        .email(request.getEmail())
                        .apiToken(request.getToken())
                        .build();
                session = new Session(connectionId, inline, new ConfluenceClient(inline),
                        new ConfluenceMapper(inline.baseUrl()),
                        ConnectionOutput.getDefaultInstance());
            } else {
                session = session(connectionId);
            }
            int cap = request.getLimit() > 0 ? request.getLimit() : 5;
            List<String> keys = new ArrayList<>();
            try {
                walk(session, "/api/v2/spaces", Map.of(
                        "limit", String.valueOf(session.config.pageSize()),
                        "description-format", "view"), node -> {
                    if (keys.size() < cap) {
                        Space space = requireValid(session.mapper.toSpace(node));
                        keys.add(space.getKey().isBlank() ? space.getId() : space.getKey());
                    }
                }, () -> keys.size() >= cap);
            } catch (IOException e) {
                // The probe itself ran; per the ProbeConnectionResponse contract
                // a bad token or unreachable Confluence is an ok=false result,
                // not an rpc failure. Only catalog-resolution problems (unknown
                // connection, wrong kind, missing credentials) fail the rpc.
                recordProbe(connectionId, false, String.valueOf(e.getMessage()));
                observer.onNext(ProbeConnectionResponse.newBuilder()
                        .setOk(false)
                        .setConnectionId(connectionId)
                        .setErrorMessage(String.valueOf(e.getMessage()))
                        .build());
                observer.onCompleted();
                return;
            }
            recordProbe(connectionId, true, null);
            observer.onNext(ProbeConnectionResponse.newBuilder()
                    .setOk(true)
                    .setConnectionId(connectionId)
                    .addAllSpaceKeys(keys)
                    .build());
            observer.onCompleted();
        } catch (Throwable t) {
            recordProbe(connectionId, false, String.valueOf(t.getMessage()));
            fail(observer, t);
        }
    }

    private void recordProbe(String connectionId, boolean ok, String errorMessage) {
        if (connections == null || connectionId.isBlank()) {
            return;
        }
        try {
            RecordProbeRequest.Builder record = RecordProbeRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .setOk(ok);
            if (!ok) {
                record.setErrorMessage(errorMessage);
            }
            connections.recordProbe(record.build());
        } catch (RuntimeException ignored) {
            // catalog update is best-effort
        }
    }

    /**
     * Inlines the binary, failing with FAILED_PRECONDITION above {@code cap}
     * bytes (declared size first, then the bytes actually read, so an
     * understated {@code file_size} is still caught without buffering past
     * the cap).
     */
    private static Attachment withContent(Session session, Attachment attachment, long cap)
            throws IOException, InterruptedException {
        if (attachment.getFileSize() > cap) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("attachment " + attachment.getId() + " is "
                            + attachment.getFileSize() + " bytes, above the " + cap
                            + " byte inline cap; use DownloadAttachment instead")
                    .asRuntimeException();
        }
        if (attachment.getDownloadUrl().isBlank()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("attachment " + attachment.getId() + " has no download link")
                    .asRuntimeException();
        }
        byte[] bytes;
        try (java.io.InputStream in = session.client.openAttachmentStream(attachment.getDownloadUrl())) {
            bytes = in.readNBytes((int) Math.min(Integer.MAX_VALUE - 8L, cap + 1));
        }
        if (bytes.length > cap) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("attachment " + attachment.getId() + " downloaded above the "
                            + cap + " byte inline cap; use DownloadAttachment instead")
                    .asRuntimeException();
        }
        return attachment.toBuilder().setContent(ByteString.copyFrom(bytes)).build();
    }

    // ======================================================================
    // REQUEST PLUMBING
    // ======================================================================

    /** One rpc body: blocking, may throw anything {@link #fail} maps. */
    @FunctionalInterface
    private interface Handler {
        void run() throws Exception;
    }

    /** Runs an rpc body, mapping any failure onto the observer. */
    private static void handle(StreamObserver<?> observer, Handler body) {
        try {
            body.run();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    /** Sends the single message of a unary rpc and completes it. */
    private static <T> void reply(StreamObserver<T> observer, T message) {
        observer.onNext(message);
        observer.onCompleted();
    }

    /** Walks a list route, streaming each mapped result, then completes the stream. */
    private <T> void stream(Session session, String path, Query query, StreamObserver<T> observer,
            Function<JsonNode, T> map) throws IOException, InterruptedException {
        walk(session, path, query.map(), node -> observer.onNext(map.apply(node)));
        observer.onCompleted();
    }

    /** A query carrying the connection's list page size. */
    private static Query limit(Session session) {
        return new Query().put("limit", String.valueOf(session.config.pageSize()));
    }

    /** INVALID_ARGUMENT when a request field is set that the chosen route does not accept. */
    private static void reject(boolean set, String field, String route) {
        if (set) {
            throw invalid(field + " is not accepted by " + route);
        }
    }

    /** A single-entity read's body format: UNSPECIFIED = storage, per the request docs. */
    private static Query singleBodyQuery(BodyFormat format, Set<BodyFormat> accepted, String route) {
        BodyFormat effective = format == BodyFormat.BODY_FORMAT_UNSPECIFIED
                ? BodyFormat.BODY_FORMAT_STORAGE_XHTML : format;
        return new Query().put("body-format", ConfluenceRoutes.bodyFormat(effective, accepted, route));
    }

    /** A list's body format: UNSPECIFIED = metadata-only, so nothing is sent. */
    private static void listBodyFormat(Query query, BodyFormat format, Set<BodyFormat> accepted,
            String route) {
        if (format != BodyFormat.BODY_FORMAT_UNSPECIFIED) {
            query.put("body-format", ConfluenceRoutes.bodyFormat(format, accepted, route));
        }
    }

    /** The {@code include-*} flags of {@code include} that the route offers. */
    private static void includes(Query query, ContentIncludes include, Set<String> offered) {
        include(query, offered, "include-labels", include.getLabels());
        include(query, offered, "include-properties", include.getProperties());
        include(query, offered, "include-operations", include.getOperations());
        include(query, offered, "include-likes", include.getLikes());
        include(query, offered, "include-versions", include.getVersions());
        include(query, offered, "include-favorited-by-current-user-status",
                include.getFavoritedByCurrentUser());
        include(query, offered, "include-direct-children", include.getDirectChildren());
        include(query, offered, "include-collaborators", include.getCollaborators());
        include(query, offered, "include-webresources", include.getWebResources());
    }

    private static void include(Query query, Set<String> offered, String flag, boolean set) {
        if (set && offered.contains(flag)) {
            query.put(flag, "true");
        }
    }

    /** The v2 query value of a proto enum: its name without the prefix, lower-cased. */
    private static String apiValue(ProtocolMessageEnum value, String prefix) {
        String name = value.getValueDescriptor().getName();
        if (!name.startsWith(prefix)) {
            throw invalid("enum " + name + " lacks prefix " + prefix);
        }
        return name.substring(prefix.length()).toLowerCase(Locale.ROOT);
    }

    private void walk(Session session, String path, Map<String, String> query,
            Consumer<JsonNode> onNode, java.util.function.BooleanSupplier done)
            throws IOException, InterruptedException {
        String url = path;
        Map<String, String> params = query;
        while (url != null && !done.getAsBoolean()) {
            ConfluenceClient.ResultPage resultPage = session.client.getPage(url, params);
            resultPage.body().path("results").forEach(onNode);
            url = resultPage.nextUrl();
            params = Map.of();
        }
    }

    /**
     * Walks a list route like {@link #walk}, but answers {@code false}
     * instead of failing when the first page is a 404 (the id is not of
     * this route's kind).
     */
    private boolean walkIfFound(Session session, String path, Map<String, String> query,
            Consumer<JsonNode> onNode) throws IOException, InterruptedException {
        ConfluenceClient.ResultPage first;
        try {
            first = session.client.getPage(path, query);
        } catch (ConfluenceClient.ConfluenceApiException e) {
            if (e.status() == 404) {
                return false;
            }
            throw e;
        }
        first.body().path("results").forEach(onNode);
        if (first.nextUrl() != null) {
            walk(session, first.nextUrl(), Map.of(), onNode);
        }
        return true;
    }

    private void walk(Session session, String path, Map<String, String> query,
            Consumer<JsonNode> onNode) throws IOException, InterruptedException {
        walk(session, path, query, onNode, () -> false);
    }

    private Session session(String connectionId) {
        if (connectionId == null || connectionId.isBlank()
                || connectionId.equals(SyncTableChangeSink.DEFAULT_CONNECTION_ID)) {
            if (config == null || client == null) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("connection_id is required")
                        .asRuntimeException();
            }
            return new Session(SyncTableChangeSink.DEFAULT_CONNECTION_ID, config, client,
                    mapper, ConnectionOutput.getDefaultInstance());
        }
        if (connections == null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("ConnectionService is not configured")
                    .asRuntimeException();
        }
        Connection row = connections.getConnection(GetConnectionRequest.newBuilder()
                .setConnectionId(connectionId)
                .setIncludeSecret(true)
                .build()).getConnection();
        if (row.getKind() != ConnectionKind.CONNECTION_KIND_UNSPECIFIED
                && row.getKind() != ConnectionKind.CONNECTION_KIND_CONFLUENCE) {
            throw Status.INVALID_ARGUMENT
                    .withDescription("connection " + connectionId + " is not Confluence")
                    .asRuntimeException();
        }
        if (row.getBaseUrl().isBlank() || row.getEmail().isBlank() || row.getToken().isBlank()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("connection " + connectionId + " is missing credentials")
                    .asRuntimeException();
        }
        ConfluenceConnectorConfig resolved = ConfluenceConnectorConfig.builder()
                .baseUrl(row.getBaseUrl())
                .email(row.getEmail())
                .apiToken(row.getToken())
                .spaces(row.getSpaceKeysList())
                .build();
        return new Session(row.getConnectionId(), resolved, new ConfluenceClient(resolved),
                new ConfluenceMapper(resolved.baseUrl()), row.getOutput());
    }

    private OutputChangeSink bindOutput(Session session) {
        if (OutputChangeSink.configured(session.output)) {
            return OutputChangeSink.from(session.output);
        }
        if (connections == null) {
            return null;
        }
        try {
            ConnectionOutput output = connections.getSettings(GetSettingsRequest.getDefaultInstance())
                    .getSettings()
                    .getOutput();
            return OutputChangeSink.configured(output) ? OutputChangeSink.from(output) : null;
        } catch (RuntimeException e) {
            return null;
        }
    }

    private record Session(String connectionId, ConfluenceConnectorConfig config,
            ConfluenceClient client, ConfluenceMapper mapper, ConnectionOutput output) {
    }

    private static ConfluenceChange stripBodies(ConfluenceChange change) {
        ConfluenceEntity entity = change.getEntity();
        ConfluenceEntity stripped = switch (entity.getEntityCase()) {
            case PAGE -> entity.toBuilder()
                    .setPage(entity.getPage().toBuilder().clearBody()).build();
            case BLOG_POST -> entity.toBuilder()
                    .setBlogPost(entity.getBlogPost().toBuilder().clearBody()).build();
            case COMMENT -> entity.toBuilder()
                    .setComment(entity.getComment().toBuilder().clearBody()).build();
            default -> entity;
        };
        return stripped == entity ? change : change.toBuilder().setEntity(stripped).build();
    }

    private static String laterCursor(String a, String b) {
        if (a == null || a.isBlank()) {
            return b == null ? "" : b;
        }
        if (b == null || b.isBlank()) {
            return a;
        }
        return java.time.Instant.parse(a).compareTo(java.time.Instant.parse(b)) >= 0 ? a : b;
    }

    private static <T extends Message> T requireValid(T message) {
        VALIDATOR.requireValid(message);
        return message;
    }

    private static void fail(StreamObserver<?> observer, Throwable t) {
        Status status;
        if (t instanceof io.grpc.StatusRuntimeException e) {
            observer.onError(e);
            return;
        } else if (t instanceof ConfluenceClient.ConfluenceApiException e) {
            status = (switch (e.status()) {
                case 400 -> Status.INVALID_ARGUMENT;
                case 401, 403 -> Status.PERMISSION_DENIED;
                case 404 -> Status.NOT_FOUND;
                case 429 -> Status.RESOURCE_EXHAUSTED;
                default -> Status.INTERNAL;
            }).withDescription(e.getMessage());
        } else if (t instanceof InterruptedException e) {
            Thread.currentThread().interrupt();
            status = Status.CANCELLED.withDescription("interrupted: " + e.getMessage());
        } else if (t instanceof IOException e) {
            status = Status.UNAVAILABLE.withDescription("Confluence unreachable: " + e.getMessage());
        } else {
            status = Status.INTERNAL.withDescription(String.valueOf(t.getMessage()));
        }
        observer.onError(status.asRuntimeException());
    }
}
