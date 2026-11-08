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
public final class ConfluenceGrpcService
        implements ConfluenceServiceGrpc.AsyncService, io.grpc.BindableService {

    /**
     * The service definition, with every server-streaming rpc behind
     * {@link FlowControlInterceptor} (isReady-aware sends that stop on
     * cancellation).
     *
     * @return the bound service
     */
    @Override
    public io.grpc.ServerServiceDefinition bindService() {
        return io.grpc.ServerInterceptors.intercept(ConfluenceServiceGrpc.bindService(this),
                new FlowControlInterceptor());
    }


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
    /** {@code POST /user/access/check-access-by-email} accepts at most this many emails per call. */
    static final int CHECK_ACCESS_MAX_EMAILS = 100;
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
            FlowControl<DownloadAttachmentResponse> flow = FlowControl.of(observer);
            if (!flow.send(DownloadAttachmentResponse.newBuilder().setAttachment(attachment).build())) {
                return; // the client cancelled; nothing left to answer
            }
            try (java.io.InputStream in = session.client.openAttachmentStream(
                    attachment.getDownloadUrl())) {
                // The running total is checked too: file_size may understate the binary.
                if (!streamChunks(in, chunk, max, "attachment " + attachment.getId(), flow,
                        bytes -> DownloadAttachmentResponse.newBuilder().setData(bytes).build())) {
                    return; // the client cancelled; nothing left to answer
                }
            }
            observer.onCompleted();
        });
    }

    @Override
    public void downloadAttachmentThumbnail(DownloadAttachmentThumbnailRequest request,
            StreamObserver<DownloadAttachmentThumbnailResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String id = requireId(request.getId(), "id");
            long max = request.getMaxBytes();
            if (max < 0) {
                throw invalid("max_bytes must not be negative");
            }
            if (request.getChunkBytes() < 0) {
                throw invalid("chunk_bytes must not be negative");
            }
            if (request.getWidth() < 0 || request.getHeight() < 0) {
                throw invalid("width and height must not be negative");
            }
            Query query = new Query();
            if (request.hasVersion()) {
                query.put("version", String.valueOf(request.getVersion()));
            }
            if (request.getWidth() > 0) {
                query.put("width", String.valueOf(request.getWidth()));
            }
            if (request.getHeight() > 0) {
                query.put("height", String.valueOf(request.getHeight()));
            }
            int chunk = request.getChunkBytes() > 0 ? request.getChunkBytes() : downloadChunkBytes;
            ConfluenceClient.StreamedBinary binary = session.client.openBinaryStream(
                    "/api/v2/attachments/" + id + "/thumbnail/download", query.map());
            try (java.io.InputStream in = binary.body()) {
                if (max > 0 && binary.contentLength() > max) {
                    throw Status.FAILED_PRECONDITION
                            .withDescription("thumbnail of attachment " + id + " is "
                                    + binary.contentLength() + " bytes, above the requested max_bytes "
                                    + max)
                            .asRuntimeException();
                }
                FlowControl<DownloadAttachmentThumbnailResponse> flow = FlowControl.of(observer);
                if (!flow.send(DownloadAttachmentThumbnailResponse.newBuilder()
                        .setThumbnail(AttachmentThumbnail.newBuilder()
                                .setAttachmentId(id)
                                .setMediaType(binary.contentType())
                                .setSizeBytes(Math.max(0L, binary.contentLength())))
                        .build())) {
                    return;
                }
                if (!streamChunks(in, chunk, max, "thumbnail of attachment " + id, flow,
                        bytes -> DownloadAttachmentThumbnailResponse.newBuilder().setData(bytes).build())) {
                    return; // the client cancelled; nothing left to answer
                }
            }
            observer.onCompleted();
        });
    }

    /**
     * Re-chunks {@code in} onto a flow-controlled stream, enforcing the
     * caller's ceiling ({@code max}, 0 = no cap) on the running total.
     *
     * @return false when the client cancelled
     */
    private static <T> boolean streamChunks(java.io.InputStream in, int chunk, long max,
            String what, FlowControl<T> flow, Function<ByteString, T> wrap)
            throws IOException, InterruptedException {
        byte[] buffer = new byte[chunk];
        long total = 0;
        while (true) {
            int read = in.readNBytes(buffer, 0, chunk);
            if (read <= 0) {
                return true;
            }
            total += read;
            if (max > 0 && total > max) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(what + " exceeded the requested max_bytes " + max)
                        .asRuntimeException();
            }
            if (!flow.send(wrap.apply(ByteString.copyFrom(buffer, 0, read)))) {
                return false;
            }
            if (read < chunk) {
                return true;
            }
        }
    }

    /**
     * A binary download's sender. Readiness is enforced for every stream by
     * {@link FlowControlInterceptor} (the send parks the handler's virtual
     * thread until the transport is ready, so a slow client never makes the
     * server buffer a whole binary); this wrapper turns a cancelled call
     * into a {@code false} answer so the download loop stops quietly.
     */
    private static final class FlowControl<T> {
        private final StreamObserver<T> observer;
        private final io.grpc.Context context = io.grpc.Context.current();

        private FlowControl(StreamObserver<T> observer) {
            this.observer = observer;
        }

        static <T> FlowControl<T> of(StreamObserver<T> observer) {
            return new FlowControl<>(observer);
        }

        /** Sends once the transport is ready; false when the call was cancelled. */
        boolean send(T message) {
            if (context.isCancelled()) {
                return false;
            }
            try {
                observer.onNext(message);
            } catch (io.grpc.StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.CANCELLED) {
                    return false;
                }
                throw e;
            }
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
            boolean attachmentScope = request.getParentCommentId().isBlank() && request.hasContainer()
                    && request.getContainer().getType() == ContentType.CONTENT_TYPE_ATTACHMENT;
            reject(request.hasVersion() && !attachmentScope, "version",
                    "every comment route but GET /attachments/{id}/footer-comments");
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
                if (attachmentScope && request.hasVersion()) {
                    query.put("version", String.valueOf(request.getVersion()));
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
            if (!request.getSpaceId().isBlank()) {
                route = "GET /spaces/{id}/custom-content";
                if (request.hasContainer()) {
                    throw invalid("space_id and container are mutually exclusive");
                }
                path = "/api/v2/spaces/" + request.getSpaceId() + "/custom-content";
                reject(!request.getIdsList().isEmpty(), "ids", route);
                reject(!request.getSpaceIdsList().isEmpty(), "space_ids", route);
                reject(request.getSort().getField() != SortField.SORT_FIELD_UNSPECIFIED, "sort", route);
                listBodyFormat(query, request.getBodyFormat(), ConfluenceRoutes.CUSTOM_BODY, route);
                stream(session, path, query, observer, node -> ListCustomContentResponse.newBuilder()
                        .setCustomContent(requireValid(session.mapper.toCustomContent(node))).build());
                return;
            }
            if (request.hasContainer()) {
                ContentRef container = request.getContainer();
                path = ConfluenceRoutes.contentPath(container, java.util.EnumSet.of(
                        ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST),
                        "ListCustomContent container (a space scope is space_id)") + "/custom-content";
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

    @Override
    public void listDatabaseViews(ListDatabaseViewsRequest request,
            StreamObserver<ListDatabaseViewsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            stream(session, "/api/v2/databases/" + requireId(request.getDatabaseId(), "database_id")
                    + "/views", limit(session), observer, node -> ListDatabaseViewsResponse.newBuilder()
                    .setView(session.mapper.toDatabaseView(node)).build());
        });
    }

    // ======================================================================
    // LABELS AND VERSIONS
    // ======================================================================

    @Override
    public void listLabels(ListLabelsRequest request, StreamObserver<ListLabelsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = limit(session);
            String path;
            String route;
            Set<String> prefixes;
            switch (request.getScopeCase()) {
                case SPACE_ID -> {
                    path = "/api/v2/spaces/" + requireId(request.getSpaceId(), "space_id") + "/labels";
                    route = "GET /spaces/{id}/labels";
                    prefixes = values("my", "team");
                }
                case CONTENT_SPACE_ID -> {
                    path = "/api/v2/spaces/" + requireId(request.getContentSpaceId(), "content_space_id")
                            + "/content/labels";
                    route = "GET /spaces/{id}/content/labels";
                    prefixes = values("my", "team");
                }
                case CONTENT -> {
                    path = ConfluenceRoutes.contentPath(request.getContent(), java.util.EnumSet.of(
                            ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST,
                            ContentType.CONTENT_TYPE_ATTACHMENT, ContentType.CONTENT_TYPE_CUSTOM_CONTENT),
                            "ListLabels") + "/labels";
                    route = "GET /" + ConfluenceRoutes.segment(request.getContent().getType())
                            + "/{id}/labels";
                    prefixes = values("my", "team", "global", "system");
                }
                default -> {
                    path = "/api/v2/labels";
                    route = "GET /labels";
                    prefixes = values("my", "team", "global", "system");
                }
            }
            List<String> wirePrefixes = new ArrayList<>();
            for (LabelPrefix prefix : request.getPrefixesList()) {
                wirePrefixes.add(ConfluenceRoutes.status(prefix.getValueDescriptor().getName(),
                        "LABEL_PREFIX_", prefixes, route));
            }
            query.put("sort", ConfluenceRoutes.sort(request.getSort(), ConfluenceRoutes.LABEL_SORT, route));
            Consumer<JsonNode> emit = node -> observer.onNext(ListLabelsResponse.newBuilder()
                    .setLabel(requireValid(session.mapper.toLabel(node))).build());
            if (path.equals("/api/v2/labels")) {
                query.list("label-id", request.getLabelIdsList()).list("prefix", wirePrefixes);
                walk(session, path, query.map(), emit);
            } else {
                reject(!request.getLabelIdsList().isEmpty(), "label_ids", route);
                // The scoped routes take a single prefix: one walk per distinct
                // prefix, in request order. A label has one prefix, so the runs
                // are disjoint and their concatenation is the union.
                List<String> distinct = distinctNonBlank(wirePrefixes);
                if (distinct.isEmpty()) {
                    walk(session, path, query.map(), emit);
                }
                for (String prefix : distinct) {
                    Map<String, String> params = new TreeMap<>(query.map());
                    params.put("prefix", prefix);
                    walk(session, path, params, emit);
                }
            }
            observer.onCompleted();
        });
    }

    @Override
    public void listVersions(ListVersionsRequest request,
            StreamObserver<ListVersionsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            ContentRef content = request.getContent();
            String path = ConfluenceRoutes.contentPath(content, java.util.EnumSet.of(
                    ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST,
                    ContentType.CONTENT_TYPE_ATTACHMENT, ContentType.CONTENT_TYPE_FOOTER_COMMENT,
                    ContentType.CONTENT_TYPE_INLINE_COMMENT, ContentType.CONTENT_TYPE_CUSTOM_CONTENT),
                    "ListVersions") + "/versions";
            String route = "GET /" + ConfluenceRoutes.segment(content.getType()) + "/{id}/versions";
            Query query = limit(session).put("sort",
                    ConfluenceRoutes.sort(request.getSort(), ConfluenceRoutes.VERSION_SORT, route));
            if (content.getType() == ContentType.CONTENT_TYPE_ATTACHMENT) {
                reject(request.getBodyFormat() != BodyFormat.BODY_FORMAT_UNSPECIFIED, "body_format", route);
            } else {
                listBodyFormat(query, request.getBodyFormat(),
                        content.getType() == ContentType.CONTENT_TYPE_CUSTOM_CONTENT
                                ? ConfluenceRoutes.CUSTOM_BODY : ConfluenceRoutes.PRIMARY_BODY, route);
            }
            stream(session, path, query, observer, node -> ListVersionsResponse.newBuilder()
                    .setVersion(requireValid(session.mapper.toVersionEntry(node))).build());
        });
    }

    // ======================================================================
    // TASKS AND USERS
    // ======================================================================

    @Override
    public void listTasks(ListTasksRequest request, StreamObserver<ListTasksResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = limit(session)
                    .list("task-id", request.getTaskIdsList())
                    .list("space-id", request.getSpaceIdsList())
                    .list("page-id", request.getPageIdsList())
                    .list("blogpost-id", request.getBlogPostIdsList())
                    .list("created-by", request.getCreatedByList())
                    .list("assigned-to", request.getAssignedToList())
                    .list("completed-by", request.getCompletedByList());
            listBodyFormat(query, request.getBodyFormat(), ConfluenceRoutes.PRIMARY_BODY, "GET /tasks");
            if (request.getExcludeBlankTasks()) {
                query.put("include-blank-tasks", "false");
            }
            if (request.getStatus() != TaskStatus.TASK_STATUS_UNSPECIFIED) {
                query.put("status", apiValue(request.getStatus(), "TASK_STATUS_"));
            }
            epochMillis(query, "created-at-from", request.hasCreatedAtFrom(), request.getCreatedAtFrom());
            epochMillis(query, "created-at-to", request.hasCreatedAtTo(), request.getCreatedAtTo());
            epochMillis(query, "due-at-from", request.hasDueAtFrom(), request.getDueAtFrom());
            epochMillis(query, "due-at-to", request.hasDueAtTo(), request.getDueAtTo());
            epochMillis(query, "completed-at-from", request.hasCompletedAtFrom(),
                    request.getCompletedAtFrom());
            epochMillis(query, "completed-at-to", request.hasCompletedAtTo(), request.getCompletedAtTo());
            stream(session, "/api/v2/tasks", query, observer, node -> ListTasksResponse.newBuilder()
                    .setTask(requireValid(session.mapper.toTask(node))).build());
        });
    }

    /** The task date filters are epoch milliseconds on the wire. */
    private static void epochMillis(Query query, String name, boolean set,
            com.google.protobuf.Timestamp at) {
        if (set) {
            query.put(name, String.valueOf(Math.addExact(Math.multiplyExact(at.getSeconds(), 1000L),
                    at.getNanos() / 1_000_000L)));
        }
    }

    @Override
    public void getTask(GetTaskRequest request, StreamObserver<GetTaskResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = singleBodyQuery(request.getBodyFormat(), ConfluenceRoutes.PRIMARY_BODY,
                    "GET /tasks/{id}");
            JsonNode node = session.client.get("/api/v2/tasks/" + requireId(request.getId(), "id"),
                    query.map());
            reply(observer, GetTaskResponse.newBuilder()
                    .setTask(requireValid(session.mapper.toTask(node))).build());
        });
    }

    @Override
    public void batchGetUsers(BatchGetUsersRequest request,
            StreamObserver<BatchGetUsersResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            List<String> ids = distinctNonBlank(request.getAccountIdsList());
            BatchGetUsersResponse.Builder response = BatchGetUsersResponse.newBuilder();
            for (int from = 0; from < ids.size(); from += USERS_BULK_MAX_IDS) {
                com.fasterxml.jackson.databind.node.ObjectNode body = JSON.createObjectNode();
                com.fasterxml.jackson.databind.node.ArrayNode accountIds = body.putArray("accountIds");
                ids.subList(from, Math.min(ids.size(), from + USERS_BULK_MAX_IDS))
                        .forEach(accountIds::add);
                JsonNode result = session.client.post("/api/v2/users-bulk", body);
                // POST /users-bulk declares no cursor or limit, so its shared
                // MultiEntityLinks.next has no defined continuation for a POST.
                // Fail loudly rather than answer with a silently partial set.
                if (session.client.nextPageUrl(result).isPresent()) {
                    throw Status.INTERNAL.withDescription("POST /users-bulk answered with a "
                            + "_links.next continuation, which the v2 spec does not define for this "
                            + "route; refusing to return a partial user set").asRuntimeException();
                }
                result.path("results").forEach(node ->
                        response.addUsers(requireValid(session.mapper.toUser(node))));
            }
            reply(observer, response.build());
        });
    }

    /** Request order kept, duplicates and blanks dropped. */
    private static List<String> distinctNonBlank(List<String> values) {
        java.util.LinkedHashSet<String> distinct = new java.util.LinkedHashSet<>();
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                distinct.add(value);
            }
        }
        return new ArrayList<>(distinct);
    }

    // ======================================================================
    // CLASSIFICATION AND DATA POLICIES
    // ======================================================================

    @Override
    public void listClassificationLevels(ListClassificationLevelsRequest request,
            StreamObserver<ListClassificationLevelsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode body = session.client.get("/api/v2/classification-levels");
            // The route answers a bare array; tolerate a {results:[...]} wrapper too.
            JsonNode levels = body.isArray() ? body : body.path("results");
            ListClassificationLevelsResponse.Builder response = ListClassificationLevelsResponse.newBuilder();
            levels.forEach(node -> response.addLevels(requireValid(
                    session.mapper.toClassificationLevel(node))));
            reply(observer, response.build());
        });
    }

    @Override
    public void getClassificationLevel(GetClassificationLevelRequest request,
            StreamObserver<GetClassificationLevelResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = new Query();
            String path;
            switch (request.getTargetCase()) {
                case CONTENT -> {
                    ContentRef content = request.getContent();
                    path = ConfluenceRoutes.contentPath(content, java.util.EnumSet.of(
                            ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST,
                            ContentType.CONTENT_TYPE_DATABASE, ContentType.CONTENT_TYPE_WHITEBOARD),
                            "GetClassificationLevel") + "/classification-level";
                    String route = "GET /" + ConfluenceRoutes.segment(content.getType())
                            + "/{id}/classification-level";
                    boolean statusRoute = content.getType() == ContentType.CONTENT_TYPE_PAGE
                            || content.getType() == ContentType.CONTENT_TYPE_BLOG_POST;
                    if (request.getStatus() != ContentStatus.CONTENT_STATUS_UNSPECIFIED) {
                        reject(!statusRoute, "status", route);
                        query.put("status", ConfluenceRoutes.status(
                                request.getStatus().getValueDescriptor().getName(), "CONTENT_STATUS_",
                                values("current", "draft", "archived"), route));
                    }
                }
                case SPACE_ID -> {
                    path = "/api/v2/spaces/" + requireId(request.getSpaceId(), "space_id")
                            + "/classification-level/default";
                    reject(request.getStatus() != ContentStatus.CONTENT_STATUS_UNSPECIFIED, "status",
                            "GET /spaces/{id}/classification-level/default");
                }
                default -> throw invalid("content or space_id is required");
            }
            JsonNode node = session.client.get(path, query.map());
            if (node.path("id").asText("").isEmpty()) {
                throw Status.NOT_FOUND.withDescription("no classification level applied")
                        .asRuntimeException();
            }
            reply(observer, GetClassificationLevelResponse.newBuilder()
                    .setLevel(requireValid(session.mapper.toClassificationLevel(node))).build());
        });
    }

    @Override
    public void listDataPolicySpaces(ListDataPolicySpacesRequest request,
            StreamObserver<ListDataPolicySpacesResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = limit(session)
                    .list("ids", request.getIdsList())
                    .list("keys", request.getKeysList())
                    .put("sort", ConfluenceRoutes.sort(request.getSort(), ConfluenceRoutes.SPACE_SORT,
                            "GET /data-policies/spaces"));
            stream(session, "/api/v2/data-policies/spaces", query, observer, node ->
                    ListDataPolicySpacesResponse.newBuilder()
                            .setDataPolicy(requireValid(session.mapper.toDataPolicy(node))).build());
        });
    }

    @Override
    public void getDataPolicyMetadata(GetDataPolicyMetadataRequest request,
            StreamObserver<GetDataPolicyMetadataResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode node = session.client.get("/api/v2/data-policies/metadata");
            reply(observer, GetDataPolicyMetadataResponse.newBuilder()
                    .setDataPolicyMetadata(session.mapper.toDataPolicyMetadata(node)).build());
        });
    }

    // ======================================================================
    // PROPERTIES
    // ======================================================================

    @Override
    public void listContentProperties(ListContentPropertiesRequest request,
            StreamObserver<ListContentPropertiesResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            ContentRef content = request.getContent();
            String path;
            if (content.getType() == ContentType.CONTENT_TYPE_FOOTER_COMMENT
                    || content.getType() == ContentType.CONTENT_TYPE_INLINE_COMMENT) {
                // Both comment kinds share one properties route.
                path = "/api/v2/comments/" + requireId(content.getId(), "content id");
            } else {
                path = ConfluenceRoutes.contentPath(content, java.util.EnumSet.of(
                        ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST,
                        ContentType.CONTENT_TYPE_ATTACHMENT, ContentType.CONTENT_TYPE_CUSTOM_CONTENT,
                        ContentType.CONTENT_TYPE_WHITEBOARD, ContentType.CONTENT_TYPE_DATABASE,
                        ContentType.CONTENT_TYPE_FOLDER, ContentType.CONTENT_TYPE_SMART_LINK),
                        "ListContentProperties");
            }
            Query query = limit(session).put("key", request.getKey()).put("sort",
                    ConfluenceRoutes.sort(request.getSort(), ConfluenceRoutes.PROPERTY_SORT,
                            "GET /{content-type}/{id}/properties"));
            stream(session, path + "/properties", query, observer, node ->
                    ListContentPropertiesResponse.newBuilder()
                            .setProperty(requireValid(session.mapper.toContentProperty(node))).build());
        });
    }

    @Override
    public void listSpaceProperties(ListSpacePropertiesRequest request,
            StreamObserver<ListSpacePropertiesResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = limit(session).put("key", request.getKey());
            stream(session, "/api/v2/spaces/" + requireId(request.getSpaceId(), "space_id")
                    + "/properties", query, observer, node -> ListSpacePropertiesResponse.newBuilder()
                    .setProperty(requireValid(session.mapper.toSpaceProperty(node))).build());
        });
    }

    // ======================================================================
    // PERMISSIONS (READ SIDE)
    // ======================================================================

    @Override
    public void getRestrictionState(GetRestrictionStateRequest request,
            StreamObserver<GetRestrictionStateResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode node = session.client.get(ConfluenceRoutes.contentPath(request.getContent(),
                    ConfluenceRoutes.PERMISSION_TARGETS, "GetRestrictionState") + "/restriction-state");
            reply(observer, GetRestrictionStateResponse.newBuilder()
                    .setRestrictionState(session.mapper.toRestrictionState(node)).build());
        });
    }

    @Override
    public void listContentPermissions(ListContentPermissionsRequest request,
            StreamObserver<ListContentPermissionsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String path = ConfluenceRoutes.contentPath(request.getContent(),
                    ConfluenceRoutes.PERMISSION_TARGETS, "ListContentPermissions") + "/permissions";
            Query query = limit(session);
            if (request.getOperation() != ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_UNSPECIFIED) {
                query.put("operation", apiValue(request.getOperation(), "CONTENT_PERMISSION_OPERATION_"));
            }
            if (request.getPrincipalType()
                    != ContentPermissionPrincipalType.CONTENT_PERMISSION_PRINCIPAL_TYPE_UNSPECIFIED) {
                query.put("principal-type", apiValue(request.getPrincipalType(),
                        "CONTENT_PERMISSION_PRINCIPAL_TYPE_"));
            } else if (!request.getPrincipalId().isEmpty()) {
                throw invalid("principal_id requires principal_type");
            }
            query.put("principal-id", request.getPrincipalId());
            stream(session, path, query, observer, node -> ListContentPermissionsResponse.newBuilder()
                    .setPermission(session.mapper.toContentPermission(node)).build());
        });
    }

    @Override
    public void listSpacePermissionAssignments(ListSpacePermissionAssignmentsRequest request,
            StreamObserver<ListSpacePermissionAssignmentsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            stream(session, "/api/v2/spaces/" + requireId(request.getSpaceId(), "space_id")
                    + "/permissions", limit(session), observer, node ->
                    ListSpacePermissionAssignmentsResponse.newBuilder()
                            .setAssignment(session.mapper.toSpacePermissionAssignment(node)).build());
        });
    }

    @Override
    public void listSpaceRoles(ListSpaceRolesRequest request,
            StreamObserver<ListSpaceRolesResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = limit(session).put("space-id", request.getSpaceId());
            roleFilters(query, request.getRoleType(), request.hasPrincipal(), request.getPrincipal());
            stream(session, "/api/v2/space-roles", query, observer, node ->
                    ListSpaceRolesResponse.newBuilder()
                            .setRole(session.mapper.toSpaceRole(node)).build());
        });
    }

    @Override
    public void listSpaceRoleAssignments(ListSpaceRoleAssignmentsRequest request,
            StreamObserver<ListSpaceRoleAssignmentsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            Query query = limit(session).put("role-id", request.getRoleId());
            roleFilters(query, request.getRoleType(), request.hasPrincipal(), request.getPrincipal());
            stream(session, "/api/v2/spaces/" + requireId(request.getSpaceId(), "space_id")
                    + "/role-assignments", query, observer, node ->
                    ListSpaceRoleAssignmentsResponse.newBuilder()
                            .setAssignment(session.mapper.toSpaceRoleAssignment(node)).build());
        });
    }

    /** {@code role-type} and the paired {@code principal-type}/{@code principal-id} (upper-case wire). */
    private static void roleFilters(Query query, RoleType roleType, boolean hasPrincipal,
            Principal principal) {
        if (roleType != RoleType.ROLE_TYPE_UNSPECIFIED) {
            query.put("role-type", apiValue(roleType, "ROLE_TYPE_").toUpperCase(Locale.ROOT));
        }
        if (hasPrincipal) {
            if (principal.getPrincipalType() == PrincipalType.PRINCIPAL_TYPE_UNSPECIFIED
                    || principal.getPrincipalId().isEmpty()) {
                throw invalid("principal needs both principal_type and principal_id");
            }
            query.put("principal-type", apiValue(principal.getPrincipalType(), "PRINCIPAL_TYPE_")
                    .toUpperCase(Locale.ROOT));
            query.put("principal-id", principal.getPrincipalId());
        }
    }

    @Override
    public void getSpaceRoleMode(GetSpaceRoleModeRequest request,
            StreamObserver<GetSpaceRoleModeResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode node = session.client.get("/api/v2/space-role-mode");
            reply(observer, GetSpaceRoleModeResponse.newBuilder()
                    .setMode(ConfluenceMapper.toSpaceRoleMode(node)).build());
        });
    }

    // ======================================================================
    // CONTENT
    // ======================================================================

    @Override
    public void resolveContentTypes(ResolveContentTypesRequest request,
            StreamObserver<ResolveContentTypesResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            List<String> ids = distinctNonBlank(request.getIdsList());
            ResolveContentTypesResponse.Builder response = ResolveContentTypesResponse.newBuilder();
            for (int from = 0; from < ids.size(); from += CONVERT_IDS_MAX_IDS) {
                com.fasterxml.jackson.databind.node.ObjectNode body = JSON.createObjectNode();
                com.fasterxml.jackson.databind.node.ArrayNode contentIds = body.putArray("contentIds");
                ids.subList(from, Math.min(ids.size(), from + CONVERT_IDS_MAX_IDS))
                        .forEach(contentIds::add);
                JsonNode results = session.client.post("/api/v2/content/convert-ids-to-types", body)
                        .path("results");
                for (Map.Entry<String, JsonNode> entry : results.properties()) {
                    String wire = entry.getValue().asText("");
                    if (wire.isEmpty()) {
                        continue;
                    }
                    ResolvedContentType.Builder resolved = ResolvedContentType.newBuilder()
                            .setId(entry.getKey());
                    ContentType type = contentType(wire);
                    if (type == ContentType.CONTENT_TYPE_UNSPECIFIED) {
                        resolved.setCustomType(wire);
                    } else {
                        resolved.setType(type);
                    }
                    response.addResults(resolved);
                }
            }
            reply(observer, response.build());
        });
    }

    /** A v2 content type string to the built-in kind; UNSPECIFIED for custom content types. */
    static ContentType contentType(String wire) {
        return switch (wire) {
            case "page" -> ContentType.CONTENT_TYPE_PAGE;
            case "blogpost" -> ContentType.CONTENT_TYPE_BLOG_POST;
            case "attachment" -> ContentType.CONTENT_TYPE_ATTACHMENT;
            case "footer-comment" -> ContentType.CONTENT_TYPE_FOOTER_COMMENT;
            case "inline-comment" -> ContentType.CONTENT_TYPE_INLINE_COMMENT;
            case "whiteboard" -> ContentType.CONTENT_TYPE_WHITEBOARD;
            case "database" -> ContentType.CONTENT_TYPE_DATABASE;
            case "folder" -> ContentType.CONTENT_TYPE_FOLDER;
            case "embed" -> ContentType.CONTENT_TYPE_SMART_LINK;
            default -> ContentType.CONTENT_TYPE_UNSPECIFIED;
        };
    }

    // ======================================================================
    // FULL READ SURFACE: version details, likes, operations, properties by
    // id, app properties, space permission catalogue and transitions, admin
    // key, email access check
    // ======================================================================

    /** Content kinds with a {@code /versions/{version-number}} route. */
    private static final Set<ContentType> VERSIONED = java.util.EnumSet.of(
            ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST,
            ContentType.CONTENT_TYPE_ATTACHMENT, ContentType.CONTENT_TYPE_FOOTER_COMMENT,
            ContentType.CONTENT_TYPE_INLINE_COMMENT, ContentType.CONTENT_TYPE_CUSTOM_CONTENT);
    /** Content kinds with {@code /likes/users} and {@code /likes/count} routes. */
    private static final Set<ContentType> LIKEABLE = java.util.EnumSet.of(
            ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST,
            ContentType.CONTENT_TYPE_FOOTER_COMMENT, ContentType.CONTENT_TYPE_INLINE_COMMENT);
    /** Content kinds with an {@code /operations} route (every built-in kind). */
    private static final Set<ContentType> OPERABLE = java.util.EnumSet.of(
            ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST,
            ContentType.CONTENT_TYPE_ATTACHMENT, ContentType.CONTENT_TYPE_CUSTOM_CONTENT,
            ContentType.CONTENT_TYPE_FOOTER_COMMENT, ContentType.CONTENT_TYPE_INLINE_COMMENT,
            ContentType.CONTENT_TYPE_WHITEBOARD, ContentType.CONTENT_TYPE_DATABASE,
            ContentType.CONTENT_TYPE_FOLDER, ContentType.CONTENT_TYPE_SMART_LINK);
    /** Content kinds with a {@code /properties} route (comments share {@code /comments}). */
    private static final Set<ContentType> PROPERTY_HOLDERS = java.util.EnumSet.of(
            ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST,
            ContentType.CONTENT_TYPE_ATTACHMENT, ContentType.CONTENT_TYPE_CUSTOM_CONTENT,
            ContentType.CONTENT_TYPE_WHITEBOARD, ContentType.CONTENT_TYPE_DATABASE,
            ContentType.CONTENT_TYPE_FOLDER, ContentType.CONTENT_TYPE_SMART_LINK);

    @Override
    public void getVersion(GetVersionRequest request, StreamObserver<GetVersionResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            if (request.getNumber() <= 0) {
                throw invalid("number must be a positive version number");
            }
            String path = ConfluenceRoutes.contentPath(request.getContent(), VERSIONED, "GetVersion")
                    + "/versions/" + request.getNumber();
            reply(observer, GetVersionResponse.newBuilder().setVersion(requireValid(
                    session.mapper.toDetailedVersion(session.client.get(path)))).build());
        });
    }

    @Override
    public void listLikes(ListLikesRequest request, StreamObserver<ListLikesResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String path = ConfluenceRoutes.contentPath(request.getContent(), LIKEABLE, "ListLikes")
                    + "/likes/users";
            stream(session, path, limit(session), observer, node -> ListLikesResponse.newBuilder()
                    .setLike(requireValid(session.mapper.toLike(node))).build());
        });
    }

    @Override
    public void getLikeCount(GetLikeCountRequest request,
            StreamObserver<GetLikeCountResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String path = ConfluenceRoutes.contentPath(request.getContent(), LIKEABLE, "GetLikeCount")
                    + "/likes/count";
            JsonNode count = session.client.get(path).path("count");
            if (!count.isIntegralNumber()) {
                throw Status.INTERNAL.withDescription("GET " + path + " answered without an integer count")
                        .asRuntimeException();
            }
            reply(observer, GetLikeCountResponse.newBuilder().setCount(count.longValue()).build());
        });
    }

    @Override
    public void listOperations(ListOperationsRequest request,
            StreamObserver<ListOperationsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String path = switch (request.getTargetCase()) {
                case CONTENT -> ConfluenceRoutes.contentPath(request.getContent(), OPERABLE,
                        "ListOperations");
                case SPACE_ID -> "/api/v2/spaces/" + requireId(request.getSpaceId(), "space_id");
                case TARGET_NOT_SET -> throw invalid("content or space_id is required");
            };
            reply(observer, ListOperationsResponse.newBuilder().addAllOperations(
                    session.mapper.toOperations(session.client.get(path + "/operations"))).build());
        });
    }

    @Override
    public void getContentProperty(GetContentPropertyRequest request,
            StreamObserver<GetContentPropertyResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            ContentRef content = request.getContent();
            String path;
            if (content.getType() == ContentType.CONTENT_TYPE_FOOTER_COMMENT
                    || content.getType() == ContentType.CONTENT_TYPE_INLINE_COMMENT) {
                // Both comment kinds share one properties route.
                path = "/api/v2/comments/" + requireId(content.getId(), "content id");
            } else {
                path = ConfluenceRoutes.contentPath(content, PROPERTY_HOLDERS, "GetContentProperty");
            }
            JsonNode node = session.client.get(path + "/properties/"
                    + requireId(request.getPropertyId(), "property_id"));
            reply(observer, GetContentPropertyResponse.newBuilder()
                    .setProperty(requireValid(session.mapper.toContentProperty(node))).build());
        });
    }

    @Override
    public void getSpaceProperty(GetSpacePropertyRequest request,
            StreamObserver<GetSpacePropertyResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode node = session.client.get("/api/v2/spaces/" + requireId(request.getSpaceId(), "space_id")
                    + "/properties/" + requireId(request.getPropertyId(), "property_id"));
            reply(observer, GetSpacePropertyResponse.newBuilder()
                    .setProperty(requireValid(session.mapper.toSpaceProperty(node))).build());
        });
    }

    @Override
    public void listAppProperties(ListAppPropertiesRequest request,
            StreamObserver<ListAppPropertiesResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            stream(session, "/api/v2/app/properties", limit(session), observer, node ->
                    ListAppPropertiesResponse.newBuilder()
                            .setProperty(requireValid(session.mapper.toAppProperty(node))).build());
        });
    }

    @Override
    public void getAppProperty(GetAppPropertyRequest request,
            StreamObserver<GetAppPropertyResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String key = java.net.URLEncoder.encode(requireId(request.getKey(), "key"),
                    java.nio.charset.StandardCharsets.UTF_8).replace("+", "%20");
            JsonNode node = session.client.get("/api/v2/app/properties/" + key);
            reply(observer, GetAppPropertyResponse.newBuilder()
                    .setProperty(requireValid(session.mapper.toAppProperty(node))).build());
        });
    }

    @Override
    public void getSpaceRole(GetSpaceRoleRequest request, StreamObserver<GetSpaceRoleResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode node = session.client.get("/api/v2/space-roles/" + requireId(request.getId(), "id"));
            reply(observer, GetSpaceRoleResponse.newBuilder()
                    .setRole(session.mapper.toSpaceRole(node)).build());
        });
    }

    @Override
    public void listSpacePermissions(ListSpacePermissionsRequest request,
            StreamObserver<ListSpacePermissionsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            stream(session, "/api/v2/space-permissions", limit(session), observer, node ->
                    ListSpacePermissionsResponse.newBuilder()
                            .setPermission(session.mapper.toSpacePermission(node)).build());
        });
    }

    /**
     * The combinations route pages through a body-level {@code cursor}
     * (not {@code _links.next}): each page's cursor is sent back as the
     * {@code cursor} query parameter until the API answers none.
     */
    @Override
    public void listSpacePermissionTransitionCombinations(
            ListSpacePermissionTransitionCombinationsRequest request,
            StreamObserver<ListSpacePermissionTransitionCombinationsResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            String path = "/api/v2/space-permissions/transition/combinations";
            Set<String> seen = new java.util.HashSet<>();
            String cursor = null;
            do {
                Query query = limit(session).put("cursor", cursor);
                JsonNode page = session.client.get(path, query.map());
                com.google.protobuf.Timestamp generatedAt =
                        ConfluenceMapper.timestamp(page.path("generatedAt").asText(""));
                for (JsonNode node : page.path("results")) {
                    ListSpacePermissionTransitionCombinationsResponse.Builder out =
                            ListSpacePermissionTransitionCombinationsResponse.newBuilder()
                                    .setCombination(session.mapper.toBulkTransitionCombination(node));
                    if (generatedAt != null) {
                        out.setGeneratedAt(generatedAt);
                    }
                    observer.onNext(out.build());
                }
                JsonNode next = page.path("cursor");
                cursor = next.isTextual() && !next.asText().isBlank() ? next.asText() : null;
                if (cursor != null && !seen.add(cursor)) {
                    throw Status.INTERNAL.withDescription("GET " + path
                            + " repeated the cursor " + cursor + "; refusing to loop").asRuntimeException();
                }
            } while (cursor != null);
            observer.onCompleted();
        });
    }

    @Override
    public void getSpacePermissionTransitionTask(GetSpacePermissionTransitionTaskRequest request,
            StreamObserver<GetSpacePermissionTransitionTaskResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            JsonNode node = session.client.get("/api/v2/space-permissions/transition/tasks/"
                    + requireId(request.getTaskId(), "task_id"));
            reply(observer, GetSpacePermissionTransitionTaskResponse.newBuilder()
                    .setTask(requireValid(session.mapper.toBulkTransitionTaskStatus(node))).build());
        });
    }

    @Override
    public void getAdminKey(GetAdminKeyRequest request, StreamObserver<GetAdminKeyResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            reply(observer, GetAdminKeyResponse.newBuilder().setAdminKey(requireValid(
                    session.mapper.toAdminKey(session.client.get("/api/v2/admin-key")))).build());
        });
    }

    @Override
    public void checkAccessByEmail(CheckAccessByEmailRequest request,
            StreamObserver<CheckAccessByEmailResponse> observer) {
        handle(observer, () -> {
            Session session = session(request.getConnectionId());
            List<String> emails = distinctNonBlank(request.getEmailsList());
            CheckAccessByEmailResponse.Builder response = CheckAccessByEmailResponse.newBuilder();
            for (int from = 0; from < emails.size(); from += CHECK_ACCESS_MAX_EMAILS) {
                com.fasterxml.jackson.databind.node.ObjectNode body = JSON.createObjectNode();
                com.fasterxml.jackson.databind.node.ArrayNode chunk = body.putArray("emails");
                emails.subList(from, Math.min(emails.size(), from + CHECK_ACCESS_MAX_EMAILS))
                        .forEach(chunk::add);
                JsonNode result = session.client.post("/api/v2/user/access/check-access-by-email", body);
                result.path("emailsWithoutAccess").forEach(email -> {
                    if (email.isTextual()) {
                        response.addEmailsWithoutAccess(email.asText());
                    }
                });
                result.path("invalidEmails").forEach(email -> {
                    if (email.isTextual()) {
                        response.addInvalidEmails(email.asText());
                    }
                });
            }
            reply(observer, response.build());
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
        ConfluenceClient.CursorGuard guard = new ConfluenceClient.CursorGuard(path);
        while (url != null && !done.getAsBoolean()) {
            requireNotCancelled();
            ConfluenceClient.ResultPage resultPage = session.client.getPage(url, params);
            resultPage.body().path("results").forEach(onNode);
            url = guard.check(resultPage.nextUrl());
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

    /** CANCELLED once the client has gone: stop paging instead of reading on for nobody. */
    private static void requireNotCancelled() {
        if (io.grpc.Context.current().isCancelled()) {
            throw Status.CANCELLED.withDescription("the client cancelled the call")
                    .asRuntimeException();
        }
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
        } else if (t instanceof ConfluenceClient.RepeatedCursorException e) {
            status = Status.INTERNAL.withDescription(e.getMessage());
        } else if (t instanceof ConfluenceClient.UnsafePathException e) {
            status = Status.INVALID_ARGUMENT.withDescription(e.getMessage());
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
