package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.BodyFormat;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.confluence.v1.GetAttachmentRequest;
import ai.pipestream.confluence.v1.GetAttachmentResponse;
import ai.pipestream.confluence.v1.GetBlogPostRequest;
import ai.pipestream.confluence.v1.GetBlogPostResponse;
import ai.pipestream.confluence.v1.GetPageRequest;
import ai.pipestream.confluence.v1.GetPageResponse;
import ai.pipestream.confluence.v1.ListBlogPostsRequest;
import ai.pipestream.confluence.v1.ListBlogPostsResponse;
import ai.pipestream.confluence.v1.ListPagesRequest;
import ai.pipestream.confluence.v1.ListPagesResponse;
import ai.pipestream.confluence.v1.ListSpacesRequest;
import ai.pipestream.confluence.v1.ListSpacesResponse;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * The {@code ConfluenceService} gRPC facade: one thin method per rpc,
 * delegating to the crawler core. Every handler is plain blocking code; the
 * server runs handlers on virtual threads (see {@link ConfluenceServer}),
 * so REST round-trips park instead of pinning a carrier.
 *
 * <p>Outgoing domain messages are checked by {@link ConfluenceValidator}
 * before they leave the process: the same identity, numeric-floor, and
 * cross-field rules the original proto options encoded.</p>
 */
public final class ConfluenceGrpcService extends ConfluenceServiceGrpc.ConfluenceServiceImplBase {

    /** Default cap for inlined attachment bytes: 25 MiB. */
    public static final long DEFAULT_ATTACHMENT_MAX_BYTES = 25L * 1024 * 1024;

    private static final ConfluenceValidator VALIDATOR = ConfluenceValidator.create();

    private final ConfluenceConnectorConfig config;
    private final ConfluenceClient client;
    private final ConfluenceMapper mapper;
    private final long attachmentMaxBytes;
    private final ChangeSink downstream;

    public ConfluenceGrpcService(ConfluenceConnectorConfig config, ConfluenceClient client,
            long attachmentMaxBytes) {
        this(config, client, attachmentMaxBytes, null);
    }

    public ConfluenceGrpcService(ConfluenceConnectorConfig config, ConfluenceClient client,
            long attachmentMaxBytes, ChangeSink downstream) {
        this.config = Objects.requireNonNull(config, "config");
        this.client = Objects.requireNonNull(client, "client");
        this.mapper = new ConfluenceMapper(config.baseUrl());
        if (attachmentMaxBytes <= 0) {
            throw new IllegalArgumentException("attachmentMaxBytes must be positive");
        }
        this.attachmentMaxBytes = attachmentMaxBytes;
        this.downstream = downstream;
    }

    @Override
    public void listSpaces(ListSpacesRequest request,
            StreamObserver<ListSpacesResponse> observer) {
        try {
            Map<String, String> query = new TreeMap<>();
            query.put("limit", String.valueOf(config.pageSize()));
            query.put("description-format", "view");
            if (!request.getKeysList().isEmpty()) {
                query.put("keys", String.join(",", request.getKeysList()));
            }
            int cap = request.getLimit();
            List<Space> spaces = new ArrayList<>();
            walk("/api/v2/spaces", query, node -> {
                if (cap <= 0 || spaces.size() < cap) {
                    spaces.add(requireValid(mapper.toSpace(node)));
                }
            }, cap > 0 ? () -> spaces.size() >= cap : () -> false);
            observer.onNext(ListSpacesResponse.newBuilder().addAllSpaces(spaces).build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void getPage(GetPageRequest request, StreamObserver<GetPageResponse> observer) {
        try {
            JsonNode node = client.get("/api/v2/pages/" + request.getId(),
                    Map.of("body-format", bodyFormatOrStorage(request.getBodyFormat())));
            observer.onNext(GetPageResponse.newBuilder()
                    .setPage(requireValid(mapper.toPage(node))).build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void getBlogPost(GetBlogPostRequest request,
            StreamObserver<GetBlogPostResponse> observer) {
        try {
            JsonNode node = client.get("/api/v2/blogposts/" + request.getId(),
                    Map.of("body-format", bodyFormatOrStorage(request.getBodyFormat())));
            observer.onNext(GetBlogPostResponse.newBuilder()
                    .setBlogPost(requireValid(mapper.toBlogPost(node))).build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void listPages(ListPagesRequest request, StreamObserver<ListPagesResponse> observer) {
        try {
            walk("/api/v2/pages", contentQuery(request.getSpaceId(), request.getBodyFormat()),
                    node -> observer.onNext(ListPagesResponse.newBuilder()
                            .setPage(requireValid(mapper.toPage(node))).build()));
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void listBlogPosts(ListBlogPostsRequest request,
            StreamObserver<ListBlogPostsResponse> observer) {
        try {
            walk("/api/v2/blogposts", contentQuery(request.getSpaceId(), request.getBodyFormat()),
                    node -> observer.onNext(ListBlogPostsResponse.newBuilder()
                            .setBlogPost(requireValid(mapper.toBlogPost(node))).build()));
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void getAttachment(GetAttachmentRequest request,
            StreamObserver<GetAttachmentResponse> observer) {
        try {
            JsonNode node = client.get("/api/v2/attachments/" + request.getId());
            Attachment attachment = requireValid(mapper.toAttachment(node));
            if (request.getIncludeContent()) {
                attachment = requireValid(withContent(attachment));
            }
            observer.onNext(GetAttachmentResponse.newBuilder().setAttachment(attachment).build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void sync(SyncRequest request, StreamObserver<SyncResponse> observer) {
        try {
            ConfluenceConnectorConfig effective = request.getSpaceKeysList().isEmpty()
                    ? config
                    : new ConfluenceConnectorConfig(config.baseUrl(), config.email(),
                            config.apiToken(), request.getSpaceKeysList(), config.pageSize(),
                            config.bodyFormat());
            Object lock = new Object();
            AtomicReference<String> newestSnapshotCursor = new AtomicReference<>("");
            ChangeSink observerSink = new ChangeSink() {
                @Override
                public void emit(ConfluenceChange change) {
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
            ChangeSink sink = downstream == null ? observerSink
                    : new CompositeChangeSink(List.of(observerSink, downstream));
            ConfluenceCrawler crawler = new ConfluenceCrawler(effective, client, sink);
            String resumeCursor;
            if (request.getSinceCursor().isBlank()) {
                crawler.crawl();
                resumeCursor = newestSnapshotCursor.get();
            } else {
                resumeCursor = crawler.crawlIncremental(request.getSinceCursor());
            }
            synchronized (lock) {
                observer.onNext(SyncResponse.newBuilder().setResumeCursor(resumeCursor).build());
                observer.onCompleted();
            }
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    private Attachment withContent(Attachment attachment) throws IOException, InterruptedException {
        if (attachment.getFileSize() > attachmentMaxBytes) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("attachment " + attachment.getId() + " is "
                            + attachment.getFileSize() + " bytes, above the " + attachmentMaxBytes
                            + " byte inline cap; fetch it from its download_url instead")
                    .asRuntimeException();
        }
        if (attachment.getDownloadUrl().isBlank()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("attachment " + attachment.getId() + " has no download link")
                    .asRuntimeException();
        }
        byte[] bytes = client.downloadAttachmentBytes(attachment.getDownloadUrl());
        if (bytes.length > attachmentMaxBytes) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("attachment " + attachment.getId() + " downloaded to "
                            + bytes.length + " bytes, above the " + attachmentMaxBytes
                            + " byte inline cap; fetch it from its download_url instead")
                    .asRuntimeException();
        }
        return attachment.toBuilder().setContent(ByteString.copyFrom(bytes)).build();
    }

    private Map<String, String> contentQuery(String spaceId, BodyFormat bodyFormat) {
        Map<String, String> query = new TreeMap<>();
        query.put("limit", String.valueOf(config.pageSize()));
        if (!spaceId.isBlank()) {
            query.put("space-id", spaceId);
        }
        if (bodyFormat != BodyFormat.BODY_FORMAT_UNSPECIFIED) {
            query.put("body-format", representation(bodyFormat));
        }
        return query;
    }

    private void walk(String path, Map<String, String> query, Consumer<JsonNode> onNode,
            java.util.function.BooleanSupplier done) throws IOException, InterruptedException {
        String url = path;
        Map<String, String> params = query;
        while (url != null && !done.getAsBoolean()) {
            ConfluenceClient.ResultPage resultPage = client.getPage(url, params);
            resultPage.body().path("results").forEach(onNode);
            url = resultPage.nextUrl();
            params = Map.of();
        }
    }

    private void walk(String path, Map<String, String> query, Consumer<JsonNode> onNode)
            throws IOException, InterruptedException {
        walk(path, query, onNode, () -> false);
    }

    private static String bodyFormatOrStorage(BodyFormat format) {
        return format == BodyFormat.BODY_FORMAT_UNSPECIFIED ? "storage" : representation(format);
    }

    private static String representation(BodyFormat format) {
        return switch (format) {
            case BODY_FORMAT_STORAGE_XHTML -> "storage";
            case BODY_FORMAT_ATLAS_DOC_FORMAT -> "atlas_doc_format";
            case BODY_FORMAT_RENDERED_XHTML -> "view";
            case BODY_FORMAT_EXPORT_XHTML -> "export_view";
            case BODY_FORMAT_ANONYMOUS_EXPORT_XHTML -> "anonymous_export_view";
            case BODY_FORMAT_STYLED_XHTML -> "styled_view";
            case BODY_FORMAT_EDITOR -> "editor";
            case BODY_FORMAT_WIKI -> "wiki";
            case BODY_FORMAT_RAW -> "raw";
            case BODY_FORMAT_PLAIN_TEXT -> "plain";
            default -> throw Status.INVALID_ARGUMENT
                    .withDescription("unsupported body format: " + format)
                    .asRuntimeException();
        };
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
