package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.BlogPost;
import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ChangeSource;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.CommentKind;
import ai.pipestream.confluence.v1.ContentAccessChain;
import ai.pipestream.confluence.v1.ContentAccessOwner;
import ai.pipestream.confluence.v1.ContentPermission;
import ai.pipestream.confluence.v1.ContentPermissionPrincipalType;
import ai.pipestream.confluence.v1.ContentRef;
import ai.pipestream.confluence.v1.ContentType;
import ai.pipestream.confluence.v1.DatabaseEntry;
import ai.pipestream.confluence.v1.DatabaseField;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.confluence.v1.ContentProperty;
import ai.pipestream.confluence.v1.Group;
import ai.pipestream.confluence.v1.GroupMember;
import ai.pipestream.confluence.v1.Label;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.PrincipalType;
import ai.pipestream.confluence.v1.RestrictionState;
import ai.pipestream.confluence.v1.SecurityCompleteness;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.SpaceAccess;
import ai.pipestream.confluence.v1.SpacePermissionAssignment;
import ai.pipestream.confluence.v1.SpacePermissionPrincipalType;
import ai.pipestream.confluence.v1.SpaceProperty;
import ai.pipestream.confluence.v1.SpaceRoleAssignment;
import ai.pipestream.confluence.v1.User;
import ai.pipestream.confluence.ConfluenceClient.CursorGuard;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Timestamp;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The crawl orchestration: enumerate spaces (or the configured allowlist),
 * then per space sweep pages and blog posts - bodies in the configured
 * format - plus each item's footer and inline comments, attachments
 * (metadata; bytes come from {@link ConfluenceClient#downloadAttachmentBytes}
 * on demand), labels, and properties. Every entity leaves as a
 * {@code ConfluenceChange{UPSERT}} and each completed space leaves one
 * {@code ConfluenceSnapshot} with per-kind counts and the resume cursor.
 *
 * <p>Full crawls emit {@code source=CRAWL} with the run id as the change
 * cursor. {@link #crawlIncremental(String)} lists pages and blog posts with
 * {@code sort=-modified-date} (a v2 list-endpoint parameter per the spec's
 * PageSortOrder/BlogPostSortOrder), walks newest-first, stops at the stored
 * cursor timestamp, and emits {@code source=CQL_INCREMENTAL}.</p>
 *
 * <p>Security-trimming data rides along: per space its permission
 * assignments and (role-based-access tenants) role assignments, per page,
 * blog post, whiteboard, database and folder its restriction state and
 * direct content permissions. The non-page content of the tree
 * (whiteboards, databases, folders, Smart Links) is discovered through
 * the direct children of each crawled page, recursively through the
 * non-page nodes, and every crawled database also emits its field
 * definitions and entries. These sub-entities have no id of their own,
 * so their {@code entity_id} is composed from the owner and the item's
 * identity, for example {@code page:123/permission/read/user/abc}. Each
 * owner also gets its owner chain ({@code page:123/access}: the item, then
 * its restrictable ancestors, nearest first, each with its restriction
 * state and direct permissions, read once per run), each space its grants
 * in one record ({@code space:<id>/access}), and every group a permission
 * names is emitted with its members ({@code group:<id>/member/<account id>})
 * and every named user with what the API shows of them, email included.</p>
 *
 * <p>Security data fails closed: each owner is emitted with a
 * {@link SecurityCompleteness} on its envelope, and every security read
 * also emits the owner's {@code restriction-state} record carrying the
 * same marker. A security route answering 404 is not served (complete,
 * nothing to emit); any other 4xx, on the first page or a later one,
 * marks the owner UNAVAILABLE (logged at WARN) and keeps what was read.
 * Non-security routes a tenant does not serve (direct children, the
 * experimental database routes) are logged at WARN and skipped.</p>
 *
 * <p>Work fans out on virtual threads; the sink must be thread-safe.</p>
 */
public final class ConfluenceCrawler {

    private static final System.Logger LOG = System.getLogger(ConfluenceCrawler.class.getName());

    /** The most ancestors one {@code ancestors} read returns. */
    private static final int ANCESTORS_LIMIT = 250;
    /** {@code POST /users-bulk} accepts at most this many account ids per call. */
    private static final int USERS_BULK_MAX_IDS = 250;
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ConfluenceConnectorConfig config;
    private final ConfluenceClient client;
    private final ConfluenceMapper mapper;
    private final ChangeSink sink;
    /** Group ids named by a permission this run. */
    private final Set<String> groupIds = ConcurrentHashMap.newKeySet();
    /** Account ids named by a permission this run. */
    private final Set<String> accountIds = ConcurrentHashMap.newKeySet();
    /** The security reads of each owner this run, by {@code <segment>/<id>}. */
    private final Map<String, CompletableFuture<OwnerRead>> ownerReads = new ConcurrentHashMap<>();

    /** The restriction state and direct permissions of one owner, and whether both were read in full. */
    private record OwnerRead(RestrictionState state, List<ContentPermission> permissions, boolean complete) {
    }

    /**
     * Creates a crawler that maps REST entities and emits them to {@code sink}.
     *
     * @param config crawl scope and credentials
     * @param client the authorized REST client
     * @param sink where changes and snapshots go
     */
    public ConfluenceCrawler(ConfluenceConnectorConfig config, ConfluenceClient client,
            ChangeSink sink) {
        this.config = Objects.requireNonNull(config, "config");
        this.client = Objects.requireNonNull(client, "client");
        this.mapper = new ConfluenceMapper(config.baseUrl());
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    /**
     * A full crawl of every space the credentials can see, or of the
     * configured space-key allowlist.
     *
     * @throws IOException if a REST call fails
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public void crawl() throws IOException, InterruptedException {
        String runId = UUID.randomUUID().toString();
        groupIds.clear();
        accountIds.clear();
        ownerReads.clear();
        CursorGuard spacesGuard = new CursorGuard("GET /spaces");
        for (JsonNode spaceNode : listAll("/api/v2/spaces", spacesQuery(), spacesGuard)) {
            Space space = mapper.toSpace(spaceNode);
            if (config.hasSpaceAllowlist() && !config.spaces().contains(space.getKey())) {
                continue;
            }
            crawlSpace(space, runId, ChangeSource.CHANGE_SOURCE_CRAWL, null, runId);
        }
        crawlPrincipals(ChangeSource.CHANGE_SOURCE_CRAWL, runId);
    }

    /**
     * An incremental sync: only pages and blog posts (and their sub-entities)
     * modified after {@code sinceRfc3339} are emitted in full, walking the v2
     * list endpoints newest-first via {@code sort=-modified-date} and
     * stopping at the cursor.
     *
     * <p>Security-trimming data does not bump a content version, so every
     * run also re-reads it for every in-scope owner: the space (permission
     * and role assignments, emitted with the space), and each page and blog
     * post through one extra metadata-only list walk per space and kind
     * (no bodies), with two reads per item ({@code restriction-state} and
     * the {@code permissions} pages). Under each page the non-page tree is
     * revisited: one {@code direct-children} walk per page and one read per
     * whiteboard, database, folder or Smart Link, which is re-emitted (and,
     * for databases, its fields and entries re-read) only when its version
     * is newer than the cursor; its security data is re-read every run.</p>
     *
     * @param sinceRfc3339 the cursor a previous crawl left on its snapshot
     *        (an RFC3339 timestamp)
     * @return the new cursor: the newest modification timestamp observed, or
     *         {@code sinceRfc3339} unchanged when nothing moved
     * @throws IOException if a REST call fails
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public String crawlIncremental(String sinceRfc3339) throws IOException, InterruptedException {
        Objects.requireNonNull(sinceRfc3339, "sinceRfc3339");
        Timestamp since = ConfluenceMapper.timestamp(sinceRfc3339);
        AtomicReference<Timestamp> newest = new AtomicReference<>(since);
        groupIds.clear();
        accountIds.clear();
        ownerReads.clear();
        CursorGuard spacesGuard = new CursorGuard("GET /spaces");
        for (JsonNode spaceNode : listAll("/api/v2/spaces", spacesQuery(), spacesGuard)) {
            Space space = mapper.toSpace(spaceNode);
            if (config.hasSpaceAllowlist() && !config.spaces().contains(space.getKey())) {
                continue;
            }
            Timestamp spaceNewest = crawlSpace(space, UUID.randomUUID().toString(),
                    ChangeSource.CHANGE_SOURCE_CQL_INCREMENTAL, since, sinceRfc3339);
            newest.accumulateAndGet(spaceNewest, ConfluenceCrawler::later);
        }
        crawlPrincipals(ChangeSource.CHANGE_SOURCE_CQL_INCREMENTAL, sinceRfc3339);
        Timestamp result = newest.get();
        return result == null ? sinceRfc3339 : rfc3339(result);
    }

    /**
     * One space sweep. When {@code stopAt} is non-null the sweep is
     * incremental: content lists walk newest-first and stop at that
     * timestamp, and the security data of the unchanged content is re-read.
     * The space itself is emitted after its security data, carrying its
     * {@link SecurityCompleteness}. Returns the newest modification
     * timestamp observed.
     */
    private Timestamp crawlSpace(Space space, String runId, ChangeSource source, Timestamp stopAt,
            String changeCursor) throws IOException, InterruptedException {
        Instant started = Instant.now();
        Map<String, Long> counts = new ConcurrentHashMap<>();
        AtomicReference<Timestamp> newest = new AtomicReference<>(stopAt);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Set<String> seenTree = ConcurrentHashMap.newKeySet();

        SecurityCompleteness spaceSecurity = crawlSpaceSecurity(space, source, changeCursor, counts);
        emit(entity(space.getId()).setSpace(space).setSecurityCompleteness(spaceSecurity).build(),
                source, changeCursor);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            executor.submit(() -> {
                try {
                    crawlContent(space, "page", source, stopAt, changeCursor, counts, newest,
                            seenTree);
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            executor.submit(() -> {
                try {
                    crawlContent(space, "blogpost", source, stopAt, changeCursor, counts, newest,
                            seenTree);
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            // Space properties ride along; they are one cheap call per space.
            executor.submit(() -> {
                try {
                    for (JsonNode node : listAll("/api/v2/spaces/" + space.getId() + "/properties",
                            Map.of("limit", String.valueOf(config.pageSize())))) {
                        SpaceProperty property = mapper.toSpaceProperty(node);
                        emit(entity(property.getId()).setSpaceProperty(property).build(), source,
                                changeCursor);
                        count(counts, "space_property");
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            executor.shutdown();
            awaitTermination(executor);
        }
        if (failure.get() != null) {
            rethrow(failure.get());
        }

        if (source == ChangeSource.CHANGE_SOURCE_CRAWL) {
            ConfluenceSnapshot snapshot = ConfluenceSnapshot.newBuilder()
                    .setSnapshotId(runId + "-" + space.getKey())
                    .setSpaceKey(space.getKey())
                    .putAllEntityCounts(sortedCounts(counts))
                    .setCursor(rfc3339(newest.get()))
                    .setStartedAt(timestamp(started))
                    .setCompletedAt(timestamp(Instant.now()))
                    .build();
            ConfluenceValidator.create().requireValid(snapshot);
            sink.snapshot(snapshot);
        }
        return newest.get();
    }

    /**
     * Pages or blog posts of one space, newest-first when incremental, with
     * all sub-entities; when incremental, then the security re-read (and,
     * for pages, the tree revisit) of every item the first walk did not
     * crawl in full.
     */
    private void crawlContent(Space space, String kind, ChangeSource source, Timestamp stopAt,
            String changeCursor, Map<String, Long> counts, AtomicReference<Timestamp> newest,
            Set<String> seenTree) throws IOException, InterruptedException {
        Map<String, String> query = new TreeMap<>();
        query.put("space-id", space.getId());
        query.put("limit", String.valueOf(config.pageSize()));
        query.put("body-format", config.bodyFormat());
        if (stopAt != null) {
            query.put("sort", "-modified-date");
        }
        String segment = "page".equals(kind) ? "pages" : "blogposts";
        String path = "/api/v2/" + segment;
        Set<String> crawled = ConcurrentHashMap.newKeySet();

        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            String url = path;
            Map<String, String> params = query;
            boolean reachedCursor = false;
            CursorGuard guard = new CursorGuard("GET /" + segment);
            while (url != null && !reachedCursor && failure.get() == null) {
                ConfluenceClient.ResultPage resultPage = client.getPage(url, params);
                url = guard.check(resultPage.nextUrl());
                params = Map.of(); // the next URL already carries cursor and query
                for (JsonNode node : resultPage.body().path("results")) {
                    Timestamp modified = ConfluenceMapper.timestamp(
                            node.path("version").path("createdAt").asText(""));
                    if (stopAt != null && modified != null && compare(modified, stopAt) <= 0) {
                        // Content at or older than the cursor: the sweep is done.
                        reachedCursor = true;
                        break;
                    }
                    if (modified != null) {
                        newest.accumulateAndGet(modified, ConfluenceCrawler::later);
                    }
                    crawled.add(node.path("id").asText(""));
                    executor.submit(() -> {
                        try {
                            if ("page".equals(kind)) {
                                crawlPage(node, source, changeCursor, counts, seenTree, stopAt);
                            } else {
                                crawlBlogPost(node, source, changeCursor, counts);
                            }
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                    });
                }
            }
            if (stopAt != null && failure.get() == null) {
                revisit(space, kind, segment, source, changeCursor, counts, seenTree, stopAt,
                        crawled, executor, failure);
            }
            executor.shutdown();
            awaitTermination(executor);
        }
        if (failure.get() != null) {
            rethrow(failure.get());
        }
    }

    /**
     * The incremental security re-read: a metadata-only walk of the
     * space's pages or blog posts (no {@code body-format}, so no bodies);
     * every item not crawled in full this run gets its restriction state and
     * permissions re-read, and every page its non-page tree revisited.
     */
    private void revisit(Space space, String kind, String segment, ChangeSource source,
            String changeCursor, Map<String, Long> counts, Set<String> seenTree, Timestamp stopAt,
            Set<String> crawled, ExecutorService executor, AtomicReference<Throwable> failure)
            throws IOException, InterruptedException {
        Map<String, String> query = new TreeMap<>();
        query.put("space-id", space.getId());
        query.put("limit", String.valueOf(config.pageSize()));
        String url = "/api/v2/" + segment;
        Map<String, String> params = query;
        CursorGuard guard = new CursorGuard("GET /" + segment + " (security re-read)");
        while (url != null && failure.get() == null) {
            ConfluenceClient.ResultPage resultPage = client.getPage(url, params);
            url = guard.check(resultPage.nextUrl());
            params = Map.of();
            for (JsonNode node : resultPage.body().path("results")) {
                String id = node.path("id").asText("");
                if (id.isEmpty() || !crawled.add(id)) {
                    continue; // already crawled in full this run
                }
                executor.submit(() -> {
                    try {
                        crawlSecurity(segment, kind, id, space.getId(), source, changeCursor, counts);
                        if ("page".equals(kind)) {
                            crawlTreeChildren("pages", id, space.getId(), source, changeCursor,
                                    counts, seenTree, stopAt);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });
            }
        }
    }

    private void crawlPage(JsonNode node, ChangeSource source, String changeCursor,
            Map<String, Long> counts, Set<String> seenTree, Timestamp stopAt)
            throws IOException, InterruptedException {
        Page page = mapper.toPage(node);
        SecurityCompleteness security = crawlSecurity("pages", "page", page.getId(),
                page.getSpaceId(), source, changeCursor, counts);
        emit(entity(page.getId()).setPage(page).setSecurityCompleteness(security).build(), source,
                changeCursor);
        count(counts, "page");
        crawlComments("/api/v2/pages/" + page.getId(), source, changeCursor, counts);
        crawlSubEntities("/api/v2/pages/" + page.getId(), "page:" + page.getId(), source,
                changeCursor, counts);
        crawlTreeChildren("pages", page.getId(), page.getSpaceId(), source, changeCursor, counts,
                seenTree, stopAt);
    }

    private void crawlBlogPost(JsonNode node, ChangeSource source, String changeCursor,
            Map<String, Long> counts) throws IOException, InterruptedException {
        BlogPost blogPost = mapper.toBlogPost(node);
        SecurityCompleteness security = crawlSecurity("blogposts", "blogpost", blogPost.getId(),
                blogPost.getSpaceId(), source, changeCursor, counts);
        emit(entity(blogPost.getId()).setBlogPost(blogPost).setSecurityCompleteness(security)
                .build(), source, changeCursor);
        count(counts, "blogpost");
        crawlComments("/api/v2/blogposts/" + blogPost.getId(), source, changeCursor, counts);
        crawlSubEntities("/api/v2/blogposts/" + blogPost.getId(), "blogpost:" + blogPost.getId(),
                source, changeCursor, counts);
    }

    /** Footer plus inline comments of one page or blog post. */
    private void crawlComments(String contentPath, ChangeSource source, String changeCursor,
            Map<String, Long> counts) throws IOException, InterruptedException {
        for (CommentKind commentKind : List.of(CommentKind.COMMENT_KIND_FOOTER,
                CommentKind.COMMENT_KIND_INLINE)) {
            String route = commentKind == CommentKind.COMMENT_KIND_FOOTER
                    ? "footer-comments" : "inline-comments";
            for (JsonNode node : listAll(contentPath + "/" + route,
                    Map.of("body-format", config.bodyFormat(),
                            "limit", String.valueOf(config.pageSize())))) {
                Comment comment = mapper.toComment(node, commentKind);
                emit(entity(comment.getId()).setComment(comment).build(), source, changeCursor);
                count(counts, "comment");
            }
        }
    }

    /**
     * Attachments (metadata only), labels and content properties of one page or blog post.
     * A label is shared by every item that carries it, so its record is keyed
     * {@code <owner>/label/<labelId>}: one record per (item, label) pair keeps the
     * association and the entity ids unique within a run.
     */
    private void crawlSubEntities(String contentPath, String owner, ChangeSource source,
            String changeCursor,
            Map<String, Long> counts) throws IOException, InterruptedException {
        for (JsonNode node : listAll(contentPath + "/attachments",
                Map.of("limit", String.valueOf(config.pageSize())))) {
            Attachment attachment = mapper.toAttachment(node);
            emit(entity(attachment.getId()).setAttachment(attachment).build(), source,
                    changeCursor);
            count(counts, "attachment");
        }
        for (JsonNode node : listAll(contentPath + "/labels",
                Map.of("limit", String.valueOf(config.pageSize())))) {
            Label label = mapper.toLabel(node);
            emit(entity(owner + "/label/" + label.getId()).setLabel(label).build(), source, changeCursor);
            count(counts, "label");
        }
        for (JsonNode node : listAll(contentPath + "/properties",
                Map.of("limit", String.valueOf(config.pageSize())))) {
            ContentProperty property = mapper.toContentProperty(node);
            emit(entity(property.getId()).setContentProperty(property).build(), source,
                    changeCursor);
            count(counts, "content_property");
        }
    }


    // ======================================================================
    // SECURITY TRIMMING AND THE NON-PAGE CONTENT TREE
    // ======================================================================

    /**
     * Space permission assignments and role assignments of one space.
     *
     * @return whether both lists were read in full
     */
    private SecurityCompleteness crawlSpaceSecurity(Space space, ChangeSource source,
            String changeCursor, Map<String, Long> counts) throws IOException, InterruptedException {
        String owner = "space:" + space.getId();
        SpaceAccess.Builder access = SpaceAccess.newBuilder().setSpaceId(space.getId());
        Map<String, String> limit = Map.of("limit", String.valueOf(config.pageSize()));
        SecurityRead<List<JsonNode>> permissions = listSecurity(
                "/api/v2/spaces/" + space.getId() + "/permissions", limit, "space permissions");
        for (JsonNode node : permissions.value()) {
            SpacePermissionAssignment assignment = mapper.toSpacePermissionAssignment(node);
            if (assignment.getPrincipal().getType() == SpacePermissionPrincipalType.SPACE_PERMISSION_PRINCIPAL_TYPE_GROUP) {
                groupIds.add(assignment.getPrincipal().getId());
            } else if (assignment.getPrincipal().getType()
                    == SpacePermissionPrincipalType.SPACE_PERMISSION_PRINCIPAL_TYPE_USER) {
                accountIds.add(assignment.getPrincipal().getId());
            }
            String identity = !assignment.getId().isEmpty() ? assignment.getId()
                    : wire(assignment.getPrincipal().getType().name(), "SPACE_PERMISSION_PRINCIPAL_TYPE_")
                            + "/" + assignment.getPrincipal().getId() + "/"
                            + wire(assignment.getOperation().getKey().name(), "SPACE_PERMISSION_OPERATION_KEY_")
                            + "/" + wire(assignment.getOperation().getTargetType().name(),
                                    "SPACE_PERMISSION_OPERATION_TARGET_TYPE_");
            emit(entity(owner + "/permission/" + identity)
                    .setSpacePermissionAssignment(assignment).build(), source, changeCursor);
            access.addPermissions(assignment);
            count(counts, "space_permission_assignment");
        }
        // Role-based-access tenants only; classic tenants answer 404 (not served).
        SecurityRead<List<JsonNode>> roles = listSecurity(
                "/api/v2/spaces/" + space.getId() + "/role-assignments", limit,
                "space role assignments");
        for (JsonNode node : roles.value()) {
            SpaceRoleAssignment assignment = mapper.toSpaceRoleAssignment(node);
            if (assignment.getPrincipal().getPrincipalType() == PrincipalType.PRINCIPAL_TYPE_GROUP) {
                groupIds.add(assignment.getPrincipal().getPrincipalId());
            } else if (assignment.getPrincipal().getPrincipalType() == PrincipalType.PRINCIPAL_TYPE_USER) {
                accountIds.add(assignment.getPrincipal().getPrincipalId());
            }
            emit(entity(owner + "/role-assignment/" + assignment.getRoleId() + "/"
                    + wire(assignment.getPrincipal().getPrincipalType().name(), "PRINCIPAL_TYPE_")
                    + "/" + assignment.getPrincipal().getPrincipalId())
                    .setSpaceRoleAssignment(assignment).build(), source, changeCursor);
            access.addRoleAssignments(assignment);
            count(counts, "space_role_assignment");
        }
        SecurityCompleteness completeness = completeness(permissions.complete() && roles.complete());
        emit(entity(owner + "/access").setSpaceAccess(access).setSecurityCompleteness(completeness).build(),
                source, changeCursor);
        return completeness;
    }

    /**
     * Direct permissions and restriction state of one page, blog post,
     * whiteboard, database or folder. The restriction-state record is
     * emitted on every call, after the permission entries and even when
     * its route failed, as the owner's per-run {@link SecurityCompleteness}
     * marker; a failed read leaves its state UNSPECIFIED.
     *
     * @param segment the v2 route segment ({@code pages}, ...)
     * @param kind the entity-id owner prefix ({@code page}, ...)
     * @param spaceId the space the owner is in
     * @return whether every security read of the owner was complete
     */
    private SecurityCompleteness crawlSecurity(String segment, String kind, String id,
            String spaceId, ChangeSource source, String changeCursor, Map<String, Long> counts)
            throws IOException, InterruptedException {
        String owner = kind + ":" + id;
        OwnerRead read = readOwner(segment, id);
        for (ContentPermission permission : read.permissions()) {
            String principalId = permission.getPrincipal().getPrincipalId();
            if (permission.getPrincipal().getPrincipalType()
                    == ContentPermissionPrincipalType.CONTENT_PERMISSION_PRINCIPAL_TYPE_GROUP) {
                groupIds.add(principalId);
            } else if (permission.getPrincipal().getPrincipalType()
                    == ContentPermissionPrincipalType.CONTENT_PERMISSION_PRINCIPAL_TYPE_USER) {
                accountIds.add(principalId);
            }
            emit(entity(owner + "/permission/"
                    + wire(permission.getOperation().name(), "CONTENT_PERMISSION_OPERATION_") + "/"
                    + wire(permission.getPrincipal().getPrincipalType().name(),
                            "CONTENT_PERMISSION_PRINCIPAL_TYPE_")
                    + "/" + principalId)
                    .setContentPermission(permission).build(), source, changeCursor);
            count(counts, "content_permission");
        }
        SecurityCompleteness completeness = completeness(read.complete());
        emit(entity(owner + "/restriction-state").setRestrictionState(read.state())
                .setSecurityCompleteness(completeness).build(), source, changeCursor);
        count(counts, "restriction_state");
        crawlAccessChain(segment, kind, id, spaceId, source, changeCursor, counts);
        return completeness;
    }

    /**
     * The restriction state and direct permissions of one owner, read once
     * per run: a later call for the same owner waits for and shares the
     * first read.
     */
    private OwnerRead readOwner(String segment, String id) throws IOException, InterruptedException {
        CompletableFuture<OwnerRead> mine = new CompletableFuture<>();
        CompletableFuture<OwnerRead> first = ownerReads.putIfAbsent(segment + "/" + id, mine);
        if (first != null) {
            try {
                return first.get();
            } catch (ExecutionException e) {
                rethrow(e.getCause());
                throw new IllegalStateException(e.getCause());
            }
        }
        try {
            String path = "/api/v2/" + segment + "/" + id;
            SecurityRead<List<JsonNode>> permissions = listSecurity(path + "/permissions",
                    Map.of("limit", String.valueOf(config.pageSize())), "content permissions");
            SecurityRead<JsonNode> state = getSecurity(path + "/restriction-state", "restriction state");
            OwnerRead read = new OwnerRead(state.value() == null ? RestrictionState.getDefaultInstance()
                    : mapper.toRestrictionState(state.value()),
                    permissions.value().stream().map(mapper::toContentPermission).toList(),
                    permissions.complete() && state.complete());
            mine.complete(read);
            return read;
        } catch (IOException | InterruptedException | RuntimeException e) {
            mine.completeExceptionally(e);
            ownerReads.remove(segment + "/" + id, mine);
            throw e;
        }
    }

    /**
     * The owner chain of one content item: the item, then each ancestor
     * that can carry restrictions, nearest first, emitted as
     * {@code <kind>:<id>/access}. Blog posts have no tree, so their chain
     * is the item alone. A 4xx other than 404 on the ancestors read marks
     * the record UNAVAILABLE.
     */
    private void crawlAccessChain(String segment, String kind, String id, String spaceId,
            ChangeSource source, String changeCursor, Map<String, Long> counts)
            throws IOException, InterruptedException {
        ContentType type = contentType(kind);
        ContentAccessChain.Builder chain = ContentAccessChain.newBuilder().setSpaceId(spaceId);
        boolean complete = addOwner(chain, type, id);
        if (type != ContentType.CONTENT_TYPE_BLOG_POST) {
            List<JsonNode> ancestors = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            String path = "/api/v2/" + segment + "/" + id;
            while (path != null) {
                SecurityRead<JsonNode> read = getSecurity(path + "/ancestors",
                        Map.of("limit", String.valueOf(ANCESTORS_LIMIT)), "ancestors");
                complete &= read.complete();
                List<JsonNode> batch = new ArrayList<>();
                if (read.value() != null) {
                    read.value().path("results").forEach(batch::add);
                }
                ancestors.addAll(0, batch);
                path = null;
                if (batch.size() >= ANCESTORS_LIMIT) {
                    JsonNode top = batch.getFirst();
                    ContentType topType = contentType(top.path("type").asText(""));
                    String topId = top.path("id").asText("");
                    if (topType != null && !topId.isEmpty() && seen.add(topType + "/" + topId)) {
                        path = "/api/v2/" + segmentOf(topType) + "/" + topId;
                    }
                }
            }
            for (JsonNode ancestor : ancestors.reversed()) {
                ContentType ancestorType = contentType(ancestor.path("type").asText(""));
                String ancestorId = ancestor.path("id").asText("");
                if (ancestorType != null && ancestorType != ContentType.CONTENT_TYPE_BLOG_POST
                        && !ancestorId.isEmpty()) {
                    complete &= addOwner(chain, ancestorType, ancestorId);
                }
            }
        }
        emit(entity(kind + ":" + id + "/access").setAccessChain(chain)
                .setSecurityCompleteness(completeness(complete)).build(), source, changeCursor);
        count(counts, "access_chain");
    }

    /** Adds one owner with its security reads to {@code chain}; whether they were complete. */
    private boolean addOwner(ContentAccessChain.Builder chain, ContentType type, String id)
            throws IOException, InterruptedException {
        OwnerRead read = readOwner(segmentOf(type), id);
        chain.addOwners(ContentAccessOwner.newBuilder()
                .setContent(ContentRef.newBuilder().setType(type).setId(id))
                .setRestrictionState(read.state()).addAllPermissions(read.permissions()));
        return read.complete();
    }

    /** The content kind of a tree node type as the API names it; null for others (Smart Links). */
    private static ContentType contentType(String apiType) {
        return switch (apiType) {
            case "page" -> ContentType.CONTENT_TYPE_PAGE;
            case "blogpost" -> ContentType.CONTENT_TYPE_BLOG_POST;
            case "whiteboard" -> ContentType.CONTENT_TYPE_WHITEBOARD;
            case "database" -> ContentType.CONTENT_TYPE_DATABASE;
            case "folder" -> ContentType.CONTENT_TYPE_FOLDER;
            default -> null;
        };
    }

    /** The v2 route segment of a tree node kind. */
    private static String segmentOf(ContentType type) {
        return switch (type) {
            case CONTENT_TYPE_PAGE -> "pages";
            case CONTENT_TYPE_WHITEBOARD -> "whiteboards";
            case CONTENT_TYPE_DATABASE -> "databases";
            case CONTENT_TYPE_FOLDER -> "folders";
            case CONTENT_TYPE_BLOG_POST -> "blogposts";
            default -> throw new IllegalArgumentException("no owner route for " + type);
        };
    }

    /**
     * Every group and user a permission named this run: each group with
     * its members ({@code group:<id>/member/<account id>}), then each named
     * user that is not a member of one of those groups. A group whose read
     * fails with a 4xx other than 404 is emitted UNAVAILABLE.
     */
    private void crawlPrincipals(ChangeSource source, String changeCursor)
            throws IOException, InterruptedException {
        Set<String> emittedUsers = new HashSet<>();
        for (String groupId : new java.util.TreeSet<>(groupIds)) {
            if (groupId.isEmpty()) {
                continue;
            }
            SecurityRead<JsonNode> read = getSecurity("/rest/api/group/by-id", Map.of("id", groupId),
                    "group");
            Group group = read.value() == null ? Group.newBuilder().setId(groupId).build()
                    : ConfluenceMapper.toGroup(read.value());
            boolean complete = read.complete() && read.value() != null;
            if (complete) {
                SecurityRead<List<JsonNode>> members = walkOffsets(
                        "/rest/api/group/" + groupId + "/membersByGroupId", "group members");
                complete = members.complete();
                for (JsonNode node : members.value()) {
                    User user = mapper.toUser(node);
                    emit(entity("group:" + groupId + "/member/" + user.getAccountId())
                            .setGroupMember(GroupMember.newBuilder().setGroupId(groupId).setUser(user))
                            .build(), source, changeCursor);
                    if (emittedUsers.add(user.getAccountId())) {
                        emit(entity(user.getAccountId()).setUser(user).build(), source, changeCursor);
                    }
                }
            }
            emit(entity(groupId).setGroup(group).setSecurityCompleteness(completeness(complete)).build(),
                    source, changeCursor);
        }
        List<String> rest = new java.util.TreeSet<>(accountIds).stream()
                .filter(id -> !id.isEmpty() && !emittedUsers.contains(id)).toList();
        for (int from = 0; from < rest.size(); from += USERS_BULK_MAX_IDS) {
            ObjectNode body = JSON.createObjectNode();
            rest.subList(from, Math.min(rest.size(), from + USERS_BULK_MAX_IDS))
                    .forEach(body.putArray("accountIds")::add);
            JsonNode users;
            try {
                users = client.post("/api/v2/users-bulk", body);
            } catch (ConfluenceClient.ConfluenceApiException e) {
                if (!unserved(e)) {
                    throw e;
                }
                LOG.log(System.Logger.Level.WARNING, "users-bulk unavailable for {0} account ids: HTTP {1}",
                        rest.size() - from, e.status());
                return;
            }
            for (JsonNode node : users.path("results")) {
                User user = mapper.toUser(node);
                if (emittedUsers.add(user.getAccountId())) {
                    emit(entity(user.getAccountId()).setUser(user).build(), source, changeCursor);
                }
            }
        }
    }

    /**
     * Walks a v1 offset-paged list ({@code start}/{@code limit}) until a
     * short page; a 4xx other than 429 keeps what was read and marks it
     * incomplete.
     */
    private SecurityRead<List<JsonNode>> walkOffsets(String path, String what)
            throws IOException, InterruptedException {
        List<JsonNode> results = new ArrayList<>();
        int pageSize = config.pageSize();
        for (int start = 0; ; start += pageSize) {
            JsonNode page;
            try {
                page = client.get(path, Map.of("start", String.valueOf(start),
                        "limit", String.valueOf(pageSize)));
            } catch (ConfluenceClient.ConfluenceApiException e) {
                if (!unserved(e)) {
                    throw e;
                }
                LOG.log(System.Logger.Level.WARNING, "{0} at {1} unavailable after {2} entries: HTTP {3}",
                        what, path, results.size(), e.status());
                return new SecurityRead<>(results, false);
            }
            JsonNode batch = page.path("results");
            batch.forEach(results::add);
            if (batch.size() < pageSize) {
                return new SecurityRead<>(results, true);
            }
        }
    }

    /**
     * The non-page direct children of a tree node: each whiteboard,
     * database, folder or Smart Link is read, given its security data,
     * emitted (with its {@link SecurityCompleteness}) and, for databases,
     * its fields and entries, then descended into. Pages are skipped: the
     * space's page list already crawls them. When {@code stopAt} is set
     * (incremental), a node whose version is not newer than it is not
     * re-emitted and its database data is not re-read, but its security
     * data is and its children are still visited.
     */
    private void crawlTreeChildren(String segment, String id, String spaceId, ChangeSource source,
            String changeCursor, Map<String, Long> counts, Set<String> seenTree, Timestamp stopAt)
            throws IOException, InterruptedException {
        for (JsonNode child : listAllOptional("/api/v2/" + segment + "/" + id + "/direct-children",
                Map.of("limit", String.valueOf(config.pageSize())), "direct children")) {
            String type = child.path("type").asText("");
            String childId = child.path("id").asText("");
            String childSegment = switch (type) {
                case "whiteboard" -> "whiteboards";
                case "database" -> "databases";
                case "folder" -> "folders";
                case "embed" -> "embeds";
                default -> null; // pages are crawled from the space list
            };
            if (childSegment == null || childId.isEmpty() || !seenTree.add(type + ":" + childId)) {
                continue;
            }
            JsonNode node = getOptional("/api/v2/" + childSegment + "/" + childId, type);
            if (node == null) {
                continue;
            }
            Timestamp modified = ConfluenceMapper.timestamp(
                    node.path("version").path("createdAt").asText(""));
            boolean changed = stopAt == null || modified == null || compare(modified, stopAt) > 0;
            // Smart Links have no restriction or permission routes.
            SecurityCompleteness security = "embed".equals(type)
                    ? SecurityCompleteness.SECURITY_COMPLETENESS_UNSPECIFIED
                    : crawlSecurity(childSegment, type, childId, spaceId, source, changeCursor, counts);
            if (changed) {
                ConfluenceEntity.Builder entity = entity(childId).setSecurityCompleteness(security);
                switch (type) {
                    case "whiteboard" -> entity.setWhiteboard(mapper.toWhiteboard(node));
                    case "database" -> entity.setDatabase(mapper.toDatabase(node));
                    case "folder" -> entity.setFolder(mapper.toFolder(node));
                    default -> entity.setSmartLink(mapper.toSmartLink(node));
                }
                emit(entity.build(), source, changeCursor);
                count(counts, "embed".equals(type) ? "smart_link" : type);
                if ("database".equals(type)) {
                    crawlDatabaseData(childId, source, changeCursor, counts);
                }
            }
            crawlTreeChildren(childSegment, childId, spaceId, source, changeCursor, counts, seenTree,
                    stopAt);
        }
    }

    /** The field definitions and entries of one database (experimental upstream). */
    private void crawlDatabaseData(String databaseId, ChangeSource source, String changeCursor,
            Map<String, Long> counts) throws IOException, InterruptedException {
        String path = "/api/v2/databases/" + databaseId;
        String owner = "database:" + databaseId;
        Map<String, String> limit = Map.of("limit", String.valueOf(config.pageSize()));
        for (JsonNode node : listAllOptional(path + "/fields", limit, "database fields")) {
            DatabaseField field = mapper.toDatabaseField(node);
            emit(entity(owner + "/field/" + field.getId()).setDatabaseFieldDefinition(field).build(),
                    source, changeCursor);
            count(counts, "database_field");
        }
        for (JsonNode node : listAllOptional(path + "/entries", limit, "database entries")) {
            DatabaseEntry entry = mapper.toDatabaseEntry(node);
            emit(entity(owner + "/entry/" + entry.getId()).setDatabaseEntry(entry).build(),
                    source, changeCursor);
            count(counts, "database_entry");
        }
    }

    /** An enum name's wire-ish form for an entity id: prefix stripped, lower-cased. */
    private static String wire(String enumName, String prefix) {
        String bare = enumName.startsWith(prefix) ? enumName.substring(prefix.length()) : enumName;
        return bare.toLowerCase(Locale.ROOT);
    }

    /** A 4xx other than 429: the tenant does not serve the route (or this item) to us. */
    private static boolean unserved(ConfluenceClient.ConfluenceApiException e) {
        return e.status() >= 400 && e.status() < 500 && e.status() != 429;
    }

    private static SecurityCompleteness completeness(boolean complete) {
        return complete ? SecurityCompleteness.SECURITY_COMPLETENESS_COMPLETE
                : SecurityCompleteness.SECURITY_COMPLETENESS_UNAVAILABLE;
    }

    /**
     * A security read: what was read, and whether it is all of it.
     *
     * @param value the entries (or body) read; possibly partial
     * @param complete false when a 4xx other than 404 cut the read short
     */
    private record SecurityRead<T>(T value, boolean complete) {
    }

    /**
     * Walks a paged security list. A 404 on the first page means the
     * tenant does not serve the route: empty and complete. Any other 4xx
     * (on any page, or a 404 after the first) keeps what was read and
     * marks it incomplete, logged at WARN; 429 and 5xx still fail the crawl.
     */
    private SecurityRead<List<JsonNode>> listSecurity(String path, Map<String, String> query,
            String what) throws IOException, InterruptedException {
        List<JsonNode> results = new ArrayList<>();
        String url = path;
        Map<String, String> params = query;
        CursorGuard guard = new CursorGuard(path);
        boolean first = true;
        while (url != null) {
            ConfluenceClient.ResultPage page;
            try {
                page = client.getPage(url, params);
            } catch (ConfluenceClient.ConfluenceApiException e) {
                if (!unserved(e)) {
                    throw e;
                }
                if (first && e.status() == 404) {
                    LOG.log(System.Logger.Level.INFO, "{0} not served at {1}: HTTP 404", what, path);
                    return new SecurityRead<>(results, true);
                }
                LOG.log(System.Logger.Level.WARNING,
                        "{0} at {1} unavailable after {2} entries: HTTP {3}; marking the owner's "
                                + "security data UNAVAILABLE", what, path, results.size(), e.status());
                return new SecurityRead<>(results, false);
            }
            page.body().path("results").forEach(results::add);
            url = guard.check(page.nextUrl());
            params = Map.of();
            first = false;
        }
        return new SecurityRead<>(results, true);
    }

    /** One security GET: 404 = not served (complete, no value); other 4xx = UNAVAILABLE (WARN). */
    private SecurityRead<JsonNode> getSecurity(String path, String what)
            throws IOException, InterruptedException {
        return getSecurity(path, Map.of(), what);
    }

    /** {@link #getSecurity(String, String)} with a query. */
    private SecurityRead<JsonNode> getSecurity(String path, Map<String, String> query, String what)
            throws IOException, InterruptedException {
        try {
            return new SecurityRead<>(client.get(path, query), true);
        } catch (ConfluenceClient.ConfluenceApiException e) {
            if (!unserved(e)) {
                throw e;
            }
            if (e.status() == 404) {
                LOG.log(System.Logger.Level.INFO, "{0} not served at {1}: HTTP 404", what, path);
                return new SecurityRead<>(null, true);
            }
            LOG.log(System.Logger.Level.WARNING, "{0} at {1} unavailable: HTTP {2}; marking the "
                    + "owner's security data UNAVAILABLE", what, path, e.status());
            return new SecurityRead<>(null, false);
        }
    }

    /** {@link #listAll}, or empty (logged at WARN) when the tenant does not serve the route. */
    private List<JsonNode> listAllOptional(String path, Map<String, String> query, String what)
            throws IOException, InterruptedException {
        try {
            return listAll(path, query);
        } catch (ConfluenceClient.ConfluenceApiException e) {
            if (!unserved(e)) {
                throw e;
            }
            LOG.log(System.Logger.Level.WARNING, "skipping {0} at {1}: HTTP {2}", what, path,
                    e.status());
            return List.of();
        }
    }

    /** One GET, or {@code null} (logged at WARN) when the tenant does not serve the route. */
    private JsonNode getOptional(String path, String what) throws IOException, InterruptedException {
        try {
            return client.get(path);
        } catch (ConfluenceClient.ConfluenceApiException e) {
            if (!unserved(e)) {
                throw e;
            }
            LOG.log(System.Logger.Level.WARNING, "skipping {0} at {1}: HTTP {2}", what, path,
                    e.status());
            return null;
        }
    }

    // ======================================================================
    // PLUMBING
    // ======================================================================

    private Map<String, String> spacesQuery() {
        Map<String, String> query = new TreeMap<>();
        query.put("limit", String.valueOf(config.pageSize()));
        query.put("description-format", "view");
        if (config.hasSpaceAllowlist()) {
            query.put("keys", String.join(",", config.spaces()));
        }
        return query;
    }

    /** Walks a cursor-paginated list endpoint to exhaustion, following _links.next. */
    private List<JsonNode> listAll(String path, Map<String, String> query)
            throws IOException, InterruptedException {
        return listAll(path, query, new CursorGuard(path));
    }

    private List<JsonNode> listAll(String path, Map<String, String> query, CursorGuard guard)
            throws IOException, InterruptedException {
        List<JsonNode> results = new ArrayList<>();
        String url = path;
        Map<String, String> params = query;
        while (url != null) {
            ConfluenceClient.ResultPage page = client.getPage(url, params);
            page.body().path("results").forEach(results::add);
            url = guard.check(page.nextUrl());
            params = Map.of();
        }
        return results;
    }

    private void emit(ConfluenceEntity entity, ChangeSource source, String cursor) {
        ConfluenceChange change = ConfluenceChange.newBuilder()
                .setChangeId(UUID.randomUUID().toString())
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(entity)
                .setCursor(cursor)
                .setSource(source)
                .setOccurredAt(timestamp(Instant.now()))
                .build();
        ConfluenceValidator.create().requireValid(change);
        sink.emit(change);
    }

    private static ConfluenceEntity.Builder entity(String entityId) {
        return ConfluenceEntity.newBuilder()
                .setEntityId(entityId)
                .setIngestedAt(timestamp(Instant.now()));
    }

    private static void count(Map<String, Long> counts, String kind) {
        counts.merge(kind, 1L, Long::sum);
    }

    private static Map<String, Long> sortedCounts(Map<String, Long> counts) {
        return new TreeMap<>(counts);
    }

    /** Timestamp comparison, seconds first (protobuf-java only, no Timestamps util). */
    private static int compare(Timestamp a, Timestamp b) {
        int bySeconds = Long.compare(a.getSeconds(), b.getSeconds());
        return bySeconds != 0 ? bySeconds : Integer.compare(a.getNanos(), b.getNanos());
    }

    /** The later of two timestamps (null-safe). */
    private static Timestamp later(Timestamp a, Timestamp b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return compare(a, b) >= 0 ? a : b;
    }

    private static String rfc3339(Timestamp timestamp) {
        if (timestamp == null) {
            return "";
        }
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()).toString();
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    private static void awaitTermination(ExecutorService executor) throws InterruptedException {
        while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            // keep waiting; virtual-thread tasks are short-lived HTTP reads
        }
    }

    private static void rethrow(Throwable t) throws IOException, InterruptedException {
        if (t instanceof IOException e) {
            throw e;
        }
        if (t instanceof UncheckedIOException e) {
            throw e.getCause();
        }
        if (t instanceof InterruptedException e) {
            throw e;
        }
        if (t instanceof RuntimeException e) {
            throw e;
        }
        throw new IOException("crawl task failed", t);
    }
}
