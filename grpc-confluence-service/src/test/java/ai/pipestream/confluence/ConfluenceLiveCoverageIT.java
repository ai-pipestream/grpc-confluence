package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.*;
import com.google.protobuf.Message;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Calls every rpc of {@code ConfluenceService} once against the real
 * Confluence workspace and writes a coverage report
 * ({@code build/live-coverage.md}, also printed to stdout).
 *
 * <p>Gated on the same credentials as {@link ConfluenceLiveSmokeIT}
 * ({@code CONFLUENCE_EMAIL} + {@code CONFLUENCE_API_TOKEN}, or the
 * {@code CONFLUENCE_USER} / {@code CONFLUENCE_TOKEN} aliases; base URL from
 * {@code CONFLUENCE_BASE_URL}), optionally {@code CONFLUENCE_SPACES} to pick
 * the candidate spaces. Excluded from the default {@code test} task; run with
 * {@code ./gradlew :grpc-confluence-service:liveCoverageTest}.</p>
 *
 * <p>Ids are discovered first (space, page, blog post, attachment, comments,
 * tree nodes, label, task, user, ...); an entity kind the tenant does not
 * have is reported SKIPPED. The test fails only on defects in this
 * codebase: INTERNAL / UNKNOWN that is not an upstream 5xx, a mapping
 * (validation) failure, a download byte mismatch, duplicate Sync entity ids,
 * or a non-gRPC exception. Permission and not-found answers are reported,
 * not failed. Credential values never reach the report or stdout.</p>
 */
class ConfluenceLiveCoverageIT {

    private static final String DEFAULT_BASE_URL = "https://pipestreamai.atlassian.net/wiki";
    private static final ConfluenceValidator VALIDATOR = ConfluenceValidator.create();
    private static final String TAG = "[ConfluenceLiveCoverageIT] ";

    /** Max items read from a list stream before the call is cancelled. */
    private static final int LIST_CAP = 5;
    /** Max candidate spaces probed when picking the richest one. */
    private static final int SPACE_CANDIDATES = 5;
    /** Max tree entries read while discovering whiteboards, databases, ... */
    private static final int TREE_CAP = 200;
    /** Attachments above this size are not downloaded. */
    private static final long DOWNLOAD_CAP = 10L * 1024 * 1024;
    /** Safety valve on the one Sync pass (SyncRequest has no bound of its own). */
    private static final int SYNC_MESSAGE_CAP = 5000;
    private static final long SYNC_DEADLINE_MINUTES = 10;
    private static final long CALL_DEADLINE_SECONDS = 90;

    /** Served upstream to app (Connect/Forge) credentials only. */
    private static final Set<String> APP_CREDENTIAL_RPCS = Set.of(
            "ListDataPolicySpaces", "GetDataPolicyMetadata", "ListAppProperties", "GetAppProperty");
    /** Need an admin user (or an enabled admin key) upstream. */
    private static final Set<String> ADMIN_RPCS = Set.of(
            "GetAdminKey", "ListSpacePermissionTransitionCombinations",
            "GetSpacePermissionTransitionTask");
    /** Access-control owners: Sync stamps security_completeness on these. */
    private static final Set<ConfluenceEntity.EntityCase> SECURITY_OWNERS = Set.of(
            ConfluenceEntity.EntityCase.SPACE, ConfluenceEntity.EntityCase.PAGE,
            ConfluenceEntity.EntityCase.BLOG_POST, ConfluenceEntity.EntityCase.WHITEBOARD,
            ConfluenceEntity.EntityCase.DATABASE, ConfluenceEntity.EntityCase.FOLDER,
            ConfluenceEntity.EntityCase.RESTRICTION_STATE);

    /** The v2 route each rpc is served by (the main one, for the report). */
    private static final Map<String, String> ROUTES = Map.ofEntries(
            Map.entry("ListSpaces", "GET /spaces"),
            Map.entry("StreamSpaces", "GET /spaces"),
            Map.entry("GetSpace", "GET /spaces/{id}"),
            Map.entry("GetPage", "GET /pages/{id}"),
            Map.entry("GetBlogPost", "GET /blogposts/{id}"),
            Map.entry("ListPages", "GET /pages, GET /labels/{id}/pages"),
            Map.entry("ListBlogPosts", "GET /blogposts, GET /spaces/{id}/blogposts"),
            Map.entry("GetAttachment", "GET /attachments/{id}"),
            Map.entry("ListAttachments", "GET /pages/{id}/attachments"),
            Map.entry("DownloadAttachment", "GET /attachments/{id} + download link"),
            Map.entry("DownloadAttachmentThumbnail", "GET /attachments/{id}/thumbnail/download"),
            Map.entry("ListComments", "GET /pages/{id}/{footer|inline}-comments"),
            Map.entry("GetComment", "GET /{footer|inline}-comments/{id}"),
            Map.entry("ListCustomContent", "GET /spaces/{id}/custom-content"),
            Map.entry("GetCustomContent", "GET /custom-content/{id}"),
            Map.entry("GetWhiteboard", "GET /whiteboards/{id}"),
            Map.entry("GetDatabase", "GET /databases/{id}"),
            Map.entry("GetFolder", "GET /folders/{id}"),
            Map.entry("GetSmartLink", "GET /embeds/{id}"),
            Map.entry("ListChildren", "GET /{type}/{id}/direct-children"),
            Map.entry("ListDescendants", "GET /{type}/{id}/descendants"),
            Map.entry("ListAncestors", "GET /{type}/{id}/ancestors"),
            Map.entry("ListDatabaseFields", "GET /databases/{id}/fields"),
            Map.entry("ListDatabaseEntries", "GET /databases/{id}/entries"),
            Map.entry("GetDatabaseEntry", "GET /databases/{id}/entries/{entryId}"),
            Map.entry("ListDatabaseViews", "GET /databases/{id}/views"),
            Map.entry("ListLabels", "GET /spaces/{id}/content/labels, GET /{type}/{id}/labels"),
            Map.entry("ListVersions", "GET /{type}/{id}/versions"),
            Map.entry("GetVersion", "GET /{type}/{id}/versions/{n}"),
            Map.entry("ListLikes", "GET /{type}/{id}/likes/users"),
            Map.entry("GetLikeCount", "GET /{type}/{id}/likes/count"),
            Map.entry("ListOperations", "GET /{type}/{id}/operations, GET /spaces/{id}/operations"),
            Map.entry("ListTasks", "GET /tasks"),
            Map.entry("GetTask", "GET /tasks/{id}"),
            Map.entry("BatchGetUsers", "POST /users-bulk"),
            Map.entry("CheckAccessByEmail", "POST /user/access/check-access-by-email"),
            Map.entry("ListClassificationLevels", "GET /classification-levels"),
            Map.entry("GetClassificationLevel", "GET /{type}/{id}/classification-level"),
            Map.entry("ListDataPolicySpaces", "GET /data-policies/spaces"),
            Map.entry("GetDataPolicyMetadata", "GET /data-policies/metadata"),
            Map.entry("ListContentProperties", "GET /{type}/{id}/properties"),
            Map.entry("ListSpaceProperties", "GET /spaces/{id}/properties"),
            Map.entry("GetContentProperty", "GET /{type}/{id}/properties/{propertyId}"),
            Map.entry("GetSpaceProperty", "GET /spaces/{id}/properties/{propertyId}"),
            Map.entry("ListAppProperties", "GET /app/properties"),
            Map.entry("GetAppProperty", "GET /app/properties/{key}"),
            Map.entry("GetRestrictionState", "GET /{type}/{id}/restriction-state"),
            Map.entry("ListContentPermissions", "GET /{type}/{id}/permissions"),
            Map.entry("ListSpacePermissionAssignments", "GET /spaces/{id}/permissions"),
            Map.entry("ListSpaceRoles", "GET /space-roles"),
            Map.entry("GetSpaceRole", "GET /space-roles/{id}"),
            Map.entry("ListSpacePermissions", "GET /space-permissions"),
            Map.entry("ListSpacePermissionTransitionCombinations",
                    "GET /space-permissions/transition/combinations"),
            Map.entry("GetSpacePermissionTransitionTask", "GET /space-permissions/transition/tasks/{taskId}"),
            Map.entry("ListSpaceRoleAssignments", "GET /spaces/{id}/role-assignments"),
            Map.entry("GetSpaceRoleMode", "GET /space-role-mode"),
            Map.entry("GetAdminKey", "GET /admin-key"),
            Map.entry("ResolveContentTypes", "POST /content/convert-ids-to-types"),
            Map.entry("Sync", "crawl of one space"),
            Map.entry("ProbeConnection", "GET /spaces"));

    private record Row(String rpc, String label, String outcome, String detail, boolean failure) {
    }

    private final List<Row> rows = new ArrayList<>();
    private final Map<String, String> discovered = new LinkedHashMap<>();
    private final List<String> syncLines = new ArrayList<>();
    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub;
    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub syncStub;
    private String email;
    private String token;

    private static String credential(String canonical, String alias) {
        String value = System.getenv(canonical);
        if (value == null || value.isBlank()) {
            value = System.getenv(alias);
        }
        return value == null || value.isBlank() ? null : value;
    }

    @Test
    void everyRpcAgainstTheLiveWorkspace() throws Exception {
        String email = credential(ConfluenceConnectorConfig.ENV_EMAIL,
                ConfluenceConnectorConfig.ENV_EMAIL_ALIAS);
        String token = credential(ConfluenceConnectorConfig.ENV_API_TOKEN,
                ConfluenceConnectorConfig.ENV_API_TOKEN_ALIAS);
        assumeTrue(email != null && token != null,
                "no live Confluence credentials in the environment; skipping");
        String baseUrl = System.getenv(ConfluenceConnectorConfig.ENV_BASE_URL);
        if (baseUrl == null || baseUrl.isBlank()) {
            baseUrl = DEFAULT_BASE_URL;
        }
        List<String> spaceKeys = new ArrayList<>();
        String spacesEnv = System.getenv(ConfluenceConnectorConfig.ENV_SPACES);
        if (spacesEnv != null) {
            for (String part : Strings.split(spacesEnv, ',')) {
                if (!part.isBlank()) {
                    spaceKeys.add(part.strip());
                }
            }
        }
        Path out = Path.of(System.getProperty("liveCoverage.report", "build/live-coverage.md"));
        List<String> failures = run(baseUrl, email, token, spaceKeys, out);
        assertThat(failures).as("defects found by the live coverage run (see %s)", out).isEmpty();
    }

    /**
     * The whole run against {@code baseUrl}: exercises every rpc, writes and
     * prints the report, and answers the failure lines (empty when clean).
     */
    List<String> run(String baseUrl, String email, String token, List<String> spaceKeys, Path out)
            throws Exception {
        this.email = email;
        this.token = token;
        // Small REST pages for the per-rpc probes; the crawl gets the default.
        ConfluenceConnectorConfig probeConfig = ConfluenceConnectorConfig.builder()
                .baseUrl(baseUrl).email(email).apiToken(token).pageSize(25).build();
        ConfluenceConnectorConfig syncConfig = ConfluenceConnectorConfig.builder()
                .baseUrl(baseUrl).email(email).apiToken(token).build();
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        String probeName = InProcessServerBuilder.generateName();
        String syncName = InProcessServerBuilder.generateName();
        Server probeServer = InProcessServerBuilder.forName(probeName).executor(executor)
                .addService(new ConfluenceGrpcService(probeConfig, new ConfluenceClient(probeConfig),
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        Server syncServer = InProcessServerBuilder.forName(syncName).executor(executor)
                .addService(new ConfluenceGrpcService(syncConfig, new ConfluenceClient(syncConfig),
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        ManagedChannel probeChannel = InProcessChannelBuilder.forName(probeName).build();
        ManagedChannel syncChannel = InProcessChannelBuilder.forName(syncName).build();
        Instant started = Instant.now();
        try {
            stub = ConfluenceServiceGrpc.newBlockingStub(probeChannel);
            syncStub = ConfluenceServiceGrpc.newBlockingStub(syncChannel);
            exercise(spaceKeys);
        } finally {
            probeChannel.shutdownNow();
            syncChannel.shutdownNow();
            probeServer.shutdownNow();
            syncServer.shutdownNow();
            probeServer.awaitTermination(10, TimeUnit.SECONDS);
            syncServer.awaitTermination(10, TimeUnit.SECONDS);
            executor.shutdownNow();
        }

        // Every rpc of the service must have been called (or skipped for lack of data).
        Set<String> seen = new LinkedHashSet<>();
        rows.forEach(row -> seen.add(row.rpc()));
        for (MethodDescriptor<?, ?> method : ConfluenceServiceGrpc.getServiceDescriptor().getMethods()) {
            String rpc = method.getBareMethodName();
            if (!seen.contains(rpc)) {
                rows.add(new Row(rpc, rpc, "FAIL", "not exercised by this test", true));
            }
        }

        String report = report(URI.create(baseUrl).getHost(), started);
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, report, StandardCharsets.UTF_8);
        System.out.println(report);
        System.out.println(TAG + "report written to " + out.toAbsolutePath());

        List<String> failures = new ArrayList<>();
        rows.stream().filter(Row::failure)
                .forEach(row -> failures.add(scrub(row.label() + ": " + row.outcome() + " " + row.detail())));
        return failures;
    }

    // ======================================================================
    // THE RUN
    // ======================================================================

    // Exercises every rpc, the deprecated ListSpaces included.
    @SuppressWarnings("deprecation")
    private void exercise(List<String> spaceKeys) {
        // ------------------------------------------------------- spaces --
        int candidates = spaceKeys.isEmpty() ? SPACE_CANDIDATES : spaceKeys.size();
        List<StreamSpacesResponse> spaceRows = stream("StreamSpaces", "StreamSpaces", candidates,
                s -> s.streamSpaces(StreamSpacesRequest.newBuilder().addAllKeys(spaceKeys)
                        .setLimit(candidates).build()),
                list -> "keys " + join(list, r -> r.getSpace().getKey()));
        unary("ListSpaces", "ListSpaces (deprecated)",
                s -> s.listSpaces(ListSpacesRequest.newBuilder().addAllKeys(spaceKeys).setLimit(1).build()),
                r -> r.getSpacesCount() + " space(s)");
        if (spaceRows == null || spaceRows.isEmpty()) {
            skipAll("no space visible to the credentials");
            return;
        }
        Space space = null;
        Space syncSpace = null;
        int best = -1;
        int smallest = Integer.MAX_VALUE;
        for (StreamSpacesResponse row : spaceRows) {
            Space candidate = row.getSpace();
            int size = countContent(candidate.getId());
            if (size > best) {
                best = size;
                space = candidate;
            }
            if (size > 0 && size < smallest) {
                smallest = size;
                syncSpace = candidate;
            }
        }
        if (syncSpace == null) {
            syncSpace = space;
        }
        discovered.put("space (probes)", space.getKey() + " (" + space.getId() + ") \"" + space.getName()
                + "\", " + best + " pages+blog posts counted (capped at 50)");
        discovered.put("space (Sync)", syncSpace.getKey() + " (" + syncSpace.getId() + ")");
        String spaceId = space.getId();

        unary("GetSpace", "GetSpace [id, all includes]",
                s -> s.getSpace(GetSpaceRequest.newBuilder().setId(spaceId)
                        .setInclude(SpaceIncludes.newBuilder().setIcon(true).setOperations(true)
                                .setProperties(true).setPermissions(true).setRoleAssignments(true)
                                .setLabels(true))
                        .build()),
                r -> "key " + r.getSpace().getKey() + ", " + r.getSpace().getPermissionsCount()
                        + " permissions, " + r.getSpace().getSpacePropertiesCount() + " properties");
        String spaceKey = space.getKey();
        unary("GetSpace", "GetSpace [key]",
                s -> s.getSpace(GetSpaceRequest.newBuilder().setKey(spaceKey).build()),
                r -> "id " + r.getSpace().getId());

        // ------------------------------------------- pages and blog posts --
        List<ListPagesResponse> pages = stream("ListPages", "ListPages [space]", LIST_CAP,
                s -> s.listPages(ListPagesRequest.newBuilder().setSpaceId(spaceId).build()),
                list -> join(list, r -> r.getPage().getId()));
        String pageId = !space.getHomepageId().isBlank() ? space.getHomepageId()
                : pages == null || pages.isEmpty() ? null : pages.get(0).getPage().getId();
        Page page = pageId == null ? null : unary("GetPage", "GetPage [all includes]",
                s -> s.getPage(GetPageRequest.newBuilder().setId(pageId)
                        .setInclude(ContentIncludes.newBuilder().setLabels(true).setProperties(true)
                                .setOperations(true).setLikes(true).setVersions(true)
                                .setFavoritedByCurrentUser(true).setDirectChildren(true)
                                .setCollaborators(true))
                        .build()),
                r -> "\"" + r.getPage().getTitle() + "\" v" + r.getPage().getVersion().getNumber()).getPage();
        if (pageId == null) {
            skip("GetPage", "no page in the space");
        } else {
            discovered.put("page", pageId + (page == null ? "" : " \"" + page.getTitle() + "\""));
        }

        List<ListBlogPostsResponse> blogs = stream("ListBlogPosts", "ListBlogPosts [space]", LIST_CAP,
                s -> s.listBlogPosts(ListBlogPostsRequest.newBuilder().setSpaceId(spaceId).build()),
                list -> join(list, r -> r.getBlogPost().getId()));
        if (blogs != null && blogs.isEmpty()) {
            blogs = stream("ListBlogPosts", "ListBlogPosts [workspace]", 1,
                    s -> s.listBlogPosts(ListBlogPostsRequest.getDefaultInstance()),
                    list -> join(list, r -> r.getBlogPost().getId()));
        }
        String blogId = blogs == null || blogs.isEmpty() ? null : blogs.get(0).getBlogPost().getId();
        if (blogId == null) {
            skip("GetBlogPost", "no blog post found");
        } else {
            discovered.put("blog post", blogId + " \"" + blogs.get(0).getBlogPost().getTitle() + "\"");
            unary("GetBlogPost", "GetBlogPost",
                    s -> s.getBlogPost(GetBlogPostRequest.newBuilder().setId(blogId)
                            .setInclude(ContentIncludes.newBuilder().setLabels(true).setOperations(true))
                            .build()),
                    r -> "\"" + r.getBlogPost().getTitle() + "\"");
        }

        // --------------------------------------------------- attachments --
        Attachment attachment = attachments(pageId);

        // ------------------------------------------------------ comments --
        String footerId = comment(pageId, CommentKind.COMMENT_KIND_FOOTER, "footer");
        String inlineId = comment(pageId, CommentKind.COMMENT_KIND_INLINE, "inline");

        // ------------------------------------------------- content tree --
        Map<String, ContentTreeEntry> tree = tree(pageId, pages);
        String customType = null;
        for (String type : tree.keySet()) {
            if (type.startsWith("custom:")) {
                customType = type.substring("custom:".length());
                break;
            }
        }
        customContent(spaceId, customType, tree);
        treeNode("GetWhiteboard", tree.get("whiteboard"), id ->
                s -> s.getWhiteboard(GetWhiteboardRequest.newBuilder().setId(id).build()),
                r -> "\"" + r.getWhiteboard().getTitle() + "\"");
        treeNode("GetDatabase", tree.get("database"), id ->
                s -> s.getDatabase(GetDatabaseRequest.newBuilder().setId(id).build()),
                r -> "\"" + r.getDatabase().getTitle() + "\"");
        treeNode("GetFolder", tree.get("folder"), id ->
                s -> s.getFolder(GetFolderRequest.newBuilder().setId(id).build()),
                r -> "\"" + r.getFolder().getTitle() + "\"");
        treeNode("GetSmartLink", tree.get("embed"), id ->
                s -> s.getSmartLink(GetSmartLinkRequest.newBuilder().setId(id).build()),
                r -> "\"" + r.getSmartLink().getTitle() + "\"");
        database(tree.get("database"));

        if (pageId == null) {
            skip("ListChildren", "no page");
            skip("ListAncestors", "no page");
        } else {
            stream("ListChildren", "ListChildren [page]", LIST_CAP,
                    s -> s.listChildren(ListChildrenRequest.newBuilder().setParent(ref(
                            ContentType.CONTENT_TYPE_PAGE, pageId)).build()),
                    list -> join(list, r -> r.getEntry().getType() + ":" + r.getEntry().getId()));
            ContentTreeEntry deep = tree.get("deep-page");
            String ancestorOf = deep == null ? pageId : deep.getId();
            stream("ListAncestors", "ListAncestors [page]", LIST_CAP,
                    s -> s.listAncestors(ListAncestorsRequest.newBuilder().setContent(ref(
                            ContentType.CONTENT_TYPE_PAGE, ancestorOf)).build()),
                    list -> "of " + ancestorOf + ": " + join(list, r -> r.getAncestor().getId()));
        }

        // -------------------------------------------------------- labels --
        List<ListLabelsResponse> labels = stream("ListLabels", "ListLabels [space content]", LIST_CAP,
                s -> s.listLabels(ListLabelsRequest.newBuilder().setContentSpaceId(spaceId).build()),
                list -> join(list, r -> r.getLabel().getName()));
        if (pageId != null) {
            stream("ListLabels", "ListLabels [page]", LIST_CAP,
                    s -> s.listLabels(ListLabelsRequest.newBuilder().setContent(ref(
                            ContentType.CONTENT_TYPE_PAGE, pageId)).build()),
                    list -> join(list, r -> r.getLabel().getName()));
        }
        if (labels != null && !labels.isEmpty()) {
            Label label = labels.get(0).getLabel();
            discovered.put("label", label.getId() + " \"" + label.getName() + "\"");
            stream("ListPages", "ListPages [label]", 1,
                    s -> s.listPages(ListPagesRequest.newBuilder().setLabelId(label.getId()).build()),
                    list -> join(list, r -> r.getPage().getId()));
        }

        // --------------------------------- per-content sub-resources (page) --
        if (pageId == null) {
            for (String rpc : List.of("ListVersions", "GetVersion", "ListLikes", "GetLikeCount",
                    "ListContentProperties", "GetContentProperty", "GetRestrictionState",
                    "ListContentPermissions", "GetClassificationLevel")) {
                skip(rpc, "no page");
            }
        } else {
            ContentRef pageRef = ref(ContentType.CONTENT_TYPE_PAGE, pageId);
            List<ListVersionsResponse> versions = stream("ListVersions", "ListVersions [page]", LIST_CAP,
                    s -> s.listVersions(ListVersionsRequest.newBuilder().setContent(pageRef).build()),
                    list -> "numbers " + join(list, r -> String.valueOf(r.getVersion().getNumber())));
            long number = page != null && page.getVersion().getNumber() > 0 ? page.getVersion().getNumber()
                    : versions != null && !versions.isEmpty() ? versions.get(0).getVersion().getNumber() : 1;
            unary("GetVersion", "GetVersion [page]",
                    s -> s.getVersion(GetVersionRequest.newBuilder().setContent(pageRef).setNumber(number).build()),
                    r -> "v" + r.getVersion().getNumber());
            stream("ListLikes", "ListLikes [page]", LIST_CAP,
                    s -> s.listLikes(ListLikesRequest.newBuilder().setContent(pageRef).build()),
                    list -> "");
            unary("GetLikeCount", "GetLikeCount [page]",
                    s -> s.getLikeCount(GetLikeCountRequest.newBuilder().setContent(pageRef).build()),
                    r -> "count " + r.getCount());
            List<ListContentPropertiesResponse> props = stream("ListContentProperties",
                    "ListContentProperties [page]", LIST_CAP,
                    s -> s.listContentProperties(ListContentPropertiesRequest.newBuilder()
                            .setContent(pageRef).build()),
                    list -> join(list, r -> r.getProperty().getId()));
            if (props == null || props.isEmpty()) {
                skip("GetContentProperty", "the page has no content property");
            } else {
                String propertyId = props.get(0).getProperty().getId();
                unary("GetContentProperty", "GetContentProperty [page]",
                        s -> s.getContentProperty(GetContentPropertyRequest.newBuilder().setContent(pageRef)
                                .setPropertyId(propertyId).build()),
                        r -> "id " + r.getProperty().getId());
            }
            unary("GetRestrictionState", "GetRestrictionState [page]",
                    s -> s.getRestrictionState(GetRestrictionStateRequest.newBuilder().setContent(pageRef).build()),
                    r -> r.getRestrictionState().getState().name());
            stream("ListContentPermissions", "ListContentPermissions [page]", LIST_CAP,
                    s -> s.listContentPermissions(ListContentPermissionsRequest.newBuilder()
                            .setContent(pageRef).build()),
                    list -> "");
            unary("GetClassificationLevel", "GetClassificationLevel [page]",
                    s -> s.getClassificationLevel(GetClassificationLevelRequest.newBuilder()
                            .setContent(pageRef).build()),
                    r -> "\"" + r.getLevel().getName() + "\"");
            unary("ListOperations", "ListOperations [page]",
                    s -> s.listOperations(ListOperationsRequest.newBuilder().setContent(pageRef).build()),
                    r -> r.getOperationsCount() + " operations");
        }
        unary("ListOperations", "ListOperations [space]",
                s -> s.listOperations(ListOperationsRequest.newBuilder().setSpaceId(spaceId).build()),
                r -> r.getOperationsCount() + " operations");
        unary("GetClassificationLevel", "GetClassificationLevel [space default]",
                s -> s.getClassificationLevel(GetClassificationLevelRequest.newBuilder()
                        .setSpaceId(spaceId).build()),
                r -> "\"" + r.getLevel().getName() + "\"");
        unary("ListClassificationLevels", "ListClassificationLevels",
                s -> s.listClassificationLevels(ListClassificationLevelsRequest.getDefaultInstance()),
                r -> r.getLevelsCount() + " levels");

        // --------------------------------------------------------- tasks --
        List<ListTasksResponse> tasks = stream("ListTasks", "ListTasks [space]", LIST_CAP,
                s -> s.listTasks(ListTasksRequest.newBuilder().addSpaceIds(spaceId).build()),
                list -> join(list, r -> r.getTask().getId()));
        if (tasks != null && tasks.isEmpty()) {
            tasks = stream("ListTasks", "ListTasks [workspace]", 1,
                    s -> s.listTasks(ListTasksRequest.getDefaultInstance()),
                    list -> join(list, r -> r.getTask().getId()));
        }
        if (tasks == null || tasks.isEmpty()) {
            skip("GetTask", "no task found");
        } else {
            String taskId = tasks.get(0).getTask().getId();
            discovered.put("task", taskId);
            unary("GetTask", "GetTask",
                    s -> s.getTask(GetTaskRequest.newBuilder().setId(taskId).build()),
                    r -> r.getTask().getStatus().name());
        }

        // --------------------------------------------------------- users --
        String accountId = page != null && !page.getAuthorId().isBlank() ? page.getAuthorId()
                : space.getAuthorId();
        if (accountId.isBlank()) {
            skip("BatchGetUsers", "no account id discovered");
        } else {
            discovered.put("user (page author)", accountId);
            unary("BatchGetUsers", "BatchGetUsers [page author]",
                    s -> s.batchGetUsers(BatchGetUsersRequest.newBuilder().addAccountIds(accountId).build()),
                    r -> r.getUsersCount() + " user(s)"
                            + (r.getUsersCount() > 0 ? ", " + r.getUsers(0).getAccountType().name() : ""));
        }
        unary("CheckAccessByEmail", "CheckAccessByEmail [the configured account]",
                s -> s.checkAccessByEmail(CheckAccessByEmailRequest.newBuilder().addEmails(email).build()),
                r -> r.getEmailsWithoutAccessCount() + " without access, " + r.getInvalidEmailsCount()
                        + " invalid");

        // -------------------------------------------- data policy and app --
        stream("ListDataPolicySpaces", "ListDataPolicySpaces", 1,
                s -> s.listDataPolicySpaces(ListDataPolicySpacesRequest.newBuilder().addIds(spaceId).build()),
                list -> "");
        unary("GetDataPolicyMetadata", "GetDataPolicyMetadata",
                s -> s.getDataPolicyMetadata(GetDataPolicyMetadataRequest.getDefaultInstance()),
                r -> "");
        List<ListAppPropertiesResponse> appProps = stream("ListAppProperties", "ListAppProperties",
                LIST_CAP, s -> s.listAppProperties(ListAppPropertiesRequest.getDefaultInstance()),
                list -> "");
        String appKey = appProps == null || appProps.isEmpty()
                || appProps.get(0).getProperty().getCustomKey().isBlank() ? "pipestream-coverage-probe"
                : appProps.get(0).getProperty().getCustomKey();
        unary("GetAppProperty", "GetAppProperty [" + appKey + "]",
                s -> s.getAppProperty(GetAppPropertyRequest.newBuilder().setKey(appKey).build()),
                r -> "");

        // ------------------------------------------- space sub-resources --
        List<ListSpacePropertiesResponse> spaceProps = stream("ListSpaceProperties", "ListSpaceProperties",
                LIST_CAP, s -> s.listSpaceProperties(ListSpacePropertiesRequest.newBuilder()
                        .setSpaceId(spaceId).build()),
                list -> join(list, r -> r.getProperty().getId()));
        if (spaceProps == null || spaceProps.isEmpty()) {
            skip("GetSpaceProperty", "the space has no space property");
        } else {
            String propertyId = spaceProps.get(0).getProperty().getId();
            unary("GetSpaceProperty", "GetSpaceProperty",
                    s -> s.getSpaceProperty(GetSpacePropertyRequest.newBuilder().setSpaceId(spaceId)
                            .setPropertyId(propertyId).build()),
                    r -> "id " + r.getProperty().getId());
        }
        stream("ListSpacePermissionAssignments", "ListSpacePermissionAssignments", LIST_CAP,
                s -> s.listSpacePermissionAssignments(ListSpacePermissionAssignmentsRequest.newBuilder()
                        .setSpaceId(spaceId).build()),
                list -> "");
        List<ListSpaceRolesResponse> roles = stream("ListSpaceRoles", "ListSpaceRoles", LIST_CAP,
                s -> s.listSpaceRoles(ListSpaceRolesRequest.getDefaultInstance()),
                list -> join(list, r -> r.getRole().getName()));
        if (roles == null || roles.isEmpty()) {
            skip("GetSpaceRole", "no space role listed");
        } else {
            String roleId = roles.get(0).getRole().getId();
            unary("GetSpaceRole", "GetSpaceRole",
                    s -> s.getSpaceRole(GetSpaceRoleRequest.newBuilder().setId(roleId).build()),
                    r -> "\"" + r.getRole().getName() + "\"");
        }
        stream("ListSpaceRoleAssignments", "ListSpaceRoleAssignments", LIST_CAP,
                s -> s.listSpaceRoleAssignments(ListSpaceRoleAssignmentsRequest.newBuilder()
                        .setSpaceId(spaceId).build()),
                list -> "");
        stream("ListSpacePermissions", "ListSpacePermissions", LIST_CAP,
                s -> s.listSpacePermissions(ListSpacePermissionsRequest.getDefaultInstance()),
                list -> join(list, r -> r.getPermission().getId()));
        stream("ListSpacePermissionTransitionCombinations", "ListSpacePermissionTransitionCombinations",
                LIST_CAP, s -> s.listSpacePermissionTransitionCombinations(
                        ListSpacePermissionTransitionCombinationsRequest.getDefaultInstance()),
                list -> "");
        unary("GetSpacePermissionTransitionTask",
                "GetSpacePermissionTransitionTask [synthetic id; no task is discoverable read-only]",
                s -> s.getSpacePermissionTransitionTask(GetSpacePermissionTransitionTaskRequest.newBuilder()
                        .setTaskId("0").build()),
                r -> "");
        unary("GetSpaceRoleMode", "GetSpaceRoleMode",
                s -> s.getSpaceRoleMode(GetSpaceRoleModeRequest.getDefaultInstance()),
                r -> r.getMode().name());
        unary("GetAdminKey", "GetAdminKey",
                s -> s.getAdminKey(GetAdminKeyRequest.getDefaultInstance()),
                r -> "an admin key is active");

        // ------------------------------------------------------- content --
        List<String> ids = new ArrayList<>();
        for (String id : new String[] {pageId, blogId, attachment == null ? null : attachment.getId(),
                footerId, inlineId}) {
            if (id != null) {
                ids.add(id);
            }
        }
        if (ids.isEmpty()) {
            skip("ResolveContentTypes", "no content id discovered");
        } else {
            unary("ResolveContentTypes", "ResolveContentTypes [" + ids.size() + " ids]",
                    s -> s.resolveContentTypes(ResolveContentTypesRequest.newBuilder().addAllIds(ids).build()),
                    r -> join(r.getResultsList(), x -> x.getId() + "=" + x.getType().name()));
        }

        // -------------------------------------------------------- probe --
        unary("ProbeConnection", "ProbeConnection [configured credentials]",
                s -> s.probeConnection(ProbeConnectionRequest.newBuilder().setLimit(3).build()),
                r -> "ok=" + r.getOk() + ", " + r.getSpaceKeysCount() + " key(s)"
                        + (r.getOk() ? "" : ", " + r.getErrorMessage()));

        // --------------------------------------------------------- sync --
        sync(syncSpace);
    }

    /** Pages + blog posts of a space, each counted up to 25 (one REST page). */
    private int countContent(String spaceId) {
        return quietCount(s -> s.listPages(ListPagesRequest.newBuilder().setSpaceId(spaceId).build()))
                + quietCount(s -> s.listBlogPosts(ListBlogPostsRequest.newBuilder().setSpaceId(spaceId).build()));
    }

    private int quietCount(Function<ConfluenceServiceGrpc.ConfluenceServiceBlockingStub,
            Iterator<? extends Message>> call) {
        Context.CancellableContext ctx = Context.current().withCancellation();
        int[] n = {0};
        try {
            ctx.run(() -> {
                Iterator<? extends Message> it = call.apply(stub());
                while (n[0] < 25 && it.hasNext()) {
                    it.next();
                    n[0]++;
                }
            });
        } catch (StatusRuntimeException e) {
            // Candidate ranking only; the real calls below report errors.
        } finally {
            ctx.cancel(null);
        }
        return n[0];
    }

    private Attachment attachments(String pageId) {
        List<ListAttachmentsResponse> list = null;
        if (pageId != null) {
            list = stream("ListAttachments", "ListAttachments [page]", LIST_CAP,
                    s -> s.listAttachments(ListAttachmentsRequest.newBuilder().setPageId(pageId).build()),
                    l -> join(l, r -> r.getAttachment().getId()));
        }
        if (list == null || list.isEmpty()) {
            list = stream("ListAttachments", "ListAttachments [workspace]", LIST_CAP,
                    s -> s.listAttachments(ListAttachmentsRequest.getDefaultInstance()),
                    l -> join(l, r -> r.getAttachment().getId()));
        }
        Attachment chosen = null;
        if (list != null) {
            for (ListAttachmentsResponse row : list) {
                Attachment a = row.getAttachment();
                if (a.getFileSize() > DOWNLOAD_CAP) {
                    continue;
                }
                boolean image = a.getMediaType().startsWith("image/");
                if (chosen == null || image && !chosen.getMediaType().startsWith("image/")) {
                    chosen = a;
                }
            }
        }
        if (chosen == null) {
            for (String rpc : List.of("GetAttachment", "DownloadAttachment", "DownloadAttachmentThumbnail")) {
                skip(rpc, list == null || list.isEmpty() ? "no attachment found"
                        : "no attachment at most " + DOWNLOAD_CAP + " bytes among the first " + list.size());
            }
            return null;
        }
        Attachment attachment = chosen;
        discovered.put("attachment", attachment.getId() + " \"" + attachment.getTitle() + "\" "
                + attachment.getMediaType() + ", " + attachment.getFileSize() + " bytes");
        Attachment meta = unary("GetAttachment", "GetAttachment",
                s -> s.getAttachment(GetAttachmentRequest.newBuilder().setId(attachment.getId()).build()),
                r -> r.getAttachment().getFileSize() + " bytes").getAttachment();
        if (meta == null) {
            meta = attachment;
        }
        long expected = meta.getFileSize();
        download("DownloadAttachment", "DownloadAttachment",
                s -> s.downloadAttachment(DownloadAttachmentRequest.newBuilder().setId(attachment.getId())
                        .setMaxBytes(DOWNLOAD_CAP).build()),
                DownloadAttachmentResponse::hasAttachment,
                r -> r.getAttachment().getFileSize(),
                DownloadAttachmentResponse::getData, expected);
        download("DownloadAttachmentThumbnail", "DownloadAttachmentThumbnail",
                s -> s.downloadAttachmentThumbnail(DownloadAttachmentThumbnailRequest.newBuilder()
                        .setId(attachment.getId()).setMaxBytes(DOWNLOAD_CAP).build()),
                DownloadAttachmentThumbnailResponse::hasThumbnail,
                r -> r.getThumbnail().getSizeBytes(),
                DownloadAttachmentThumbnailResponse::getData, -1);
        return attachment;
    }

    /**
     * Reads a download stream to the end and checks the byte count against
     * the size the head message declares (and, for attachments, against the
     * attachment metadata).
     */
    private <T extends Message> void download(String rpc, String label,
            Function<ConfluenceServiceGrpc.ConfluenceServiceBlockingStub, Iterator<T>> call,
            java.util.function.Predicate<T> isHead, java.util.function.ToLongFunction<T> declared,
            Function<T, com.google.protobuf.ByteString> data, long metadataSize) {
        Context.CancellableContext ctx = Context.current().withCancellation();
        Context previous = ctx.attach();
        try {
            Iterator<T> it = call.apply(stub());
            long bytes = 0;
            long head = -1;
            int messages = 0;
            boolean dataBeforeHead = false;
            while (it.hasNext()) {
                T message = it.next();
                messages++;
                if (isHead.test(message)) {
                    if (!violations(rpc, label, message)) {
                        return;
                    }
                    head = declared.applyAsLong(message);
                } else {
                    if (head < 0) {
                        dataBeforeHead = true;
                    }
                    bytes += data.apply(message).size();
                }
            }
            List<String> problems = new ArrayList<>();
            if (head < 0 || dataBeforeHead) {
                problems.add("the descriptor message did not come first");
            }
            if (head > 0 && head != bytes) {
                problems.add("declared " + head + " bytes, streamed " + bytes);
            }
            if (metadataSize > 0 && metadataSize != bytes) {
                problems.add("metadata file_size " + metadataSize + ", streamed " + bytes);
            }
            String detail = bytes + " bytes in " + messages + " messages" + (head == 0 ? " (size not declared)" : "");
            if (problems.isEmpty()) {
                rows.add(new Row(rpc, label, "OK", detail, false));
            } else {
                rows.add(new Row(rpc, label, "BYTE_MISMATCH", detail + "; " + String.join("; ", problems), true));
            }
        } catch (Throwable t) {
            error(rpc, label, t);
        } finally {
            ctx.detach(previous);
            ctx.cancel(null);
        }
    }

    private String comment(String pageId, CommentKind kind, String name) {
        List<ListCommentsResponse> list = null;
        if (pageId != null) {
            list = stream("ListComments", "ListComments [" + name + ", page]", LIST_CAP,
                    s -> s.listComments(ListCommentsRequest.newBuilder()
                            .setContainer(ref(ContentType.CONTENT_TYPE_PAGE, pageId)).setKind(kind).build()),
                    l -> join(l, r -> r.getComment().getId()));
        }
        if (list != null && list.isEmpty()) {
            list = stream("ListComments", "ListComments [" + name + ", workspace]", 1,
                    s -> s.listComments(ListCommentsRequest.newBuilder().setKind(kind).build()),
                    l -> join(l, r -> r.getComment().getId()));
        }
        if (list == null || list.isEmpty()) {
            skip("GetComment", "GetComment [" + name + "]", "no " + name + " comment found");
            return null;
        }
        String id = list.get(0).getComment().getId();
        discovered.put(name + " comment", id);
        unary("GetComment", "GetComment [" + name + "]",
                s -> s.getComment(GetCommentRequest.newBuilder().setId(id).setKind(kind).build()),
                r -> "on " + firstNonBlank(r.getComment().getPageId(), r.getComment().getBlogPostId(),
                        r.getComment().getAttachmentId(), r.getComment().getCustomContentId()));
        return id;
    }

    /**
     * Walks the descendants of the page (and, if a kind is still missing, of
     * a few root pages) and keeps the first entry of each tree type, keyed by
     * type ("page", "whiteboard", "database", "folder", "embed", or
     * "custom:{type}"), plus "deep-page": a page with a parent.
     */
    private Map<String, ContentTreeEntry> tree(String pageId, List<ListPagesResponse> pages) {
        Map<String, ContentTreeEntry> found = new LinkedHashMap<>();
        if (pageId == null) {
            skip("ListDescendants", "no page");
            return found;
        }
        List<String> roots = new ArrayList<>();
        roots.add(pageId);
        if (pages != null) {
            for (ListPagesResponse row : pages) {
                if (row.getPage().getParentId().isBlank() && !roots.contains(row.getPage().getId())) {
                    roots.add(row.getPage().getId());
                }
            }
        }
        for (int i = 0; i < roots.size() && i < 3; i++) {
            String root = roots.get(i);
            List<ListDescendantsResponse> list = stream("ListDescendants",
                    "ListDescendants [page " + root + "]", TREE_CAP,
                    s -> s.listDescendants(ListDescendantsRequest.newBuilder()
                            .setRoot(ref(ContentType.CONTENT_TYPE_PAGE, root)).setDepth(5).build()),
                    l -> {
                        Map<String, Integer> byType = new TreeMap<>();
                        l.forEach(r -> byType.merge(r.getEntry().getType(), 1, Integer::sum));
                        return byType.toString();
                    });
            if (list == null) {
                break;
            }
            for (ListDescendantsResponse row : list) {
                ContentTreeEntry entry = row.getEntry();
                String type = switch (entry.getType()) {
                    case "page", "whiteboard", "database", "folder", "embed" -> entry.getType();
                    default -> "custom:" + entry.getType();
                };
                found.putIfAbsent(type, entry);
                if (type.equals("page") && !entry.getParentId().isBlank()) {
                    found.putIfAbsent("deep-page", entry);
                }
            }
            if (found.keySet().containsAll(List.of("whiteboard", "database", "folder", "embed"))) {
                break;
            }
        }
        found.forEach((type, entry) -> {
            if (!type.equals("page") && !type.equals("deep-page")) {
                discovered.put(type, entry.getId() + " \"" + entry.getTitle() + "\"");
            }
        });
        return found;
    }

    private void customContent(String spaceId, String customType, Map<String, ContentTreeEntry> tree) {
        if (customType == null) {
            skip("ListCustomContent", "no custom content type found in the content tree");
            skip("GetCustomContent", "no custom content found");
            return;
        }
        List<ListCustomContentResponse> list = stream("ListCustomContent",
                "ListCustomContent [" + customType + ", space]", LIST_CAP,
                s -> s.listCustomContent(ListCustomContentRequest.newBuilder().setType(customType)
                        .setSpaceId(spaceId).build()),
                l -> join(l, r -> r.getCustomContent().getId()));
        String id = list != null && !list.isEmpty() ? list.get(0).getCustomContent().getId()
                : tree.get("custom:" + customType).getId();
        unary("GetCustomContent", "GetCustomContent [" + customType + "]",
                s -> s.getCustomContent(GetCustomContentRequest.newBuilder().setId(id).build()),
                r -> "\"" + r.getCustomContent().getTitle() + "\"");
    }

    private <T extends Message> void treeNode(String rpc, ContentTreeEntry entry,
            Function<String, Function<ConfluenceServiceGrpc.ConfluenceServiceBlockingStub, T>> call,
            Function<T, String> detail) {
        if (entry == null) {
            skip(rpc, "none found in the content tree");
            return;
        }
        unary(rpc, rpc, call.apply(entry.getId()), detail);
    }

    private void database(ContentTreeEntry database) {
        if (database == null) {
            for (String rpc : List.of("ListDatabaseFields", "ListDatabaseEntries", "GetDatabaseEntry",
                    "ListDatabaseViews")) {
                skip(rpc, "no database found in the content tree");
            }
            return;
        }
        String id = database.getId();
        stream("ListDatabaseFields", "ListDatabaseFields", LIST_CAP,
                s -> s.listDatabaseFields(ListDatabaseFieldsRequest.newBuilder().setDatabaseId(id).build()),
                l -> join(l, r -> r.getField().getName()));
        List<ListDatabaseEntriesResponse> entries = stream("ListDatabaseEntries", "ListDatabaseEntries",
                LIST_CAP, s -> s.listDatabaseEntries(ListDatabaseEntriesRequest.newBuilder()
                        .setDatabaseId(id).build()),
                l -> join(l, r -> r.getEntry().getId()));
        if (entries == null || entries.isEmpty()) {
            skip("GetDatabaseEntry", "the database has no entry");
        } else {
            String entryId = entries.get(0).getEntry().getId();
            unary("GetDatabaseEntry", "GetDatabaseEntry",
                    s -> s.getDatabaseEntry(GetDatabaseEntryRequest.newBuilder().setDatabaseId(id)
                            .setEntryId(entryId).build()),
                    r -> r.getEntry().getValuesCount() + " values");
        }
        stream("ListDatabaseViews", "ListDatabaseViews", LIST_CAP,
                s -> s.listDatabaseViews(ListDatabaseViewsRequest.newBuilder().setDatabaseId(id).build()),
                l -> join(l, r -> r.getView().getName()));
    }

    /**
     * One Sync pass over one space (no bodies): counts by kind, owners whose
     * security data is not COMPLETE, and duplicate entity ids (a failure).
     */
    private void sync(Space space) {
        Map<String, Long> counts = new TreeMap<>();
        Map<String, ConfluenceEntity> byId = new LinkedHashMap<>();
        List<String> duplicates = new ArrayList<>();
        List<String> incomplete = new ArrayList<>();
        Map<String, Long> snapshotCounts = new TreeMap<>();
        boolean[] resumed = {false};
        int[] messages = {0};
        Context.CancellableContext ctx = Context.current().withCancellation();
        try {
            ctx.run(() -> {
                Iterator<SyncResponse> it = syncStub.withDeadlineAfter(SYNC_DEADLINE_MINUTES, TimeUnit.MINUTES)
                        .sync(SyncRequest.newBuilder().addSpaceKeys(space.getKey()).build());
                while (messages[0] < SYNC_MESSAGE_CAP && it.hasNext()) {
                    SyncResponse response = it.next();
                    messages[0]++;
                    switch (response.getEventCase()) {
                        case CHANGE -> {
                            ConfluenceEntity entity = response.getChange().getEntity();
                            ConfluenceEntity.EntityCase kind = entity.getEntityCase();
                            counts.merge(kind.name().toLowerCase(java.util.Locale.ROOT), 1L, Long::sum);
                            List<ConfluenceValidator.Violation> v = validate(response);
                            if (!v.isEmpty()) {
                                rows.add(new Row("Sync", "Sync [validation " + entity.getEntityId() + "]",
                                        "MAPPING", clip(v.toString()), true));
                            }
                            if (SECURITY_OWNERS.contains(kind) && entity.getSecurityCompleteness()
                                    != SecurityCompleteness.SECURITY_COMPLETENESS_COMPLETE
                                    && incomplete.size() < 50) {
                                incomplete.add(kind.name().toLowerCase(java.util.Locale.ROOT) + " "
                                        + entity.getEntityId() + ": " + entity.getSecurityCompleteness().name());
                            }
                            ConfluenceEntity previous = byId.putIfAbsent(entity.getEntityId(), entity);
                            if (previous != null) {
                                boolean same = previous.toBuilder().clearIngestedAt().build()
                                        .equals(entity.toBuilder().clearIngestedAt().build());
                                duplicates.add(entity.getEntityId() + " ("
                                        + previous.getEntityCase().name().toLowerCase(java.util.Locale.ROOT)
                                        + " then " + kind.name().toLowerCase(java.util.Locale.ROOT)
                                        + (same ? ", identical payload" : ", different payload") + ")");
                            }
                        }
                        case SNAPSHOT -> response.getSnapshot().getEntityCountsMap()
                                .forEach((k, n) -> snapshotCounts.merge(k, n, Long::sum));
                        case RESUME_CURSOR -> resumed[0] = true;
                        default -> {
                        }
                    }
                }
            });
            boolean truncated = !resumed[0];
            String detail = "space " + space.getKey() + ": " + messages[0] + " messages, " + byId.size()
                    + " distinct entity ids" + (truncated ? ", TRUNCATED at the " + SYNC_MESSAGE_CAP
                    + "-message cap (no resume cursor)" : ", resume cursor received");
            if (duplicates.isEmpty()) {
                rows.add(new Row("Sync", "Sync [one space, no bodies]", "OK", detail, false));
            } else {
                rows.add(new Row("Sync", "Sync [one space, no bodies]", "DUPLICATE_IDS",
                        detail + "; " + duplicates.size() + " duplicate entity ids", true));
            }
        } catch (Throwable t) {
            error("Sync", "Sync [one space, no bodies]", t);
        } finally {
            ctx.cancel(null);
        }
        syncLines.add("Space: " + space.getKey() + " (" + space.getId() + "), " + messages[0] + " messages.");
        syncLines.add("");
        syncLines.add("Entity counts by kind (from the change stream): " + (counts.isEmpty() ? "none" : ""));
        counts.forEach((k, n) -> syncLines.add("- " + k + ": " + n));
        syncLines.add("");
        syncLines.add("Snapshot entity_counts: " + (snapshotCounts.isEmpty() ? "none" : ""));
        snapshotCounts.forEach((k, n) -> syncLines.add("- " + k + ": " + n));
        syncLines.add("");
        syncLines.add("Access-control owners whose security_completeness is not COMPLETE: "
                + (incomplete.isEmpty() ? "none" : ""));
        incomplete.forEach(line -> syncLines.add("- " + line));
        syncLines.add("");
        syncLines.add("Duplicate entity_ids (failure): " + (duplicates.isEmpty() ? "none" : duplicates.size()));
        duplicates.stream().limit(50).forEach(line -> syncLines.add("- " + line));
        if (duplicates.size() > 50) {
            syncLines.add("- ... " + (duplicates.size() - 50) + " more");
        }
    }

    // ======================================================================
    // CALL HELPERS AND OUTCOME CLASSIFICATION
    // ======================================================================

    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub() {
        return stub.withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS);
    }

    private <T extends Message> UnaryResult<T> unary(String rpc, String label,
            Function<ConfluenceServiceGrpc.ConfluenceServiceBlockingStub, T> call, Function<T, String> detail) {
        try {
            T response = call.apply(stub());
            if (!violations(rpc, label, response)) {
                return new UnaryResult<>(null);
            }
            rows.add(new Row(rpc, label, "OK", clip(detail.apply(response)), false));
            return new UnaryResult<>(response);
        } catch (Throwable t) {
            error(rpc, label, t);
            return new UnaryResult<>(null);
        }
    }

    /** A unary response, or null; field getters answer null when the call failed. */
    private static final class UnaryResult<T extends Message> {
        private final T response;

        UnaryResult(T response) {
            this.response = response;
        }

        Page getPage() {
            return response instanceof GetPageResponse r ? r.getPage() : null;
        }

        Attachment getAttachment() {
            return response instanceof GetAttachmentResponse r ? r.getAttachment() : null;
        }
    }

    private <T extends Message> List<T> stream(String rpc, String label, int max,
            Function<ConfluenceServiceGrpc.ConfluenceServiceBlockingStub, Iterator<T>> call,
            Function<List<T>, String> detail) {
        Context.CancellableContext ctx = Context.current().withCancellation();
        List<T> out = new ArrayList<>();
        try {
            ctx.run(() -> {
                Iterator<T> it = call.apply(stub());
                while (out.size() < max && it.hasNext()) {
                    out.add(it.next());
                }
            });
            for (T item : out) {
                if (!violations(rpc, label, item)) {
                    return null;
                }
            }
            String text = detail.apply(out);
            rows.add(new Row(rpc, label, "OK", out.size() + (out.size() >= max ? "+ (capped)" : "")
                    + " item(s)" + (text.isEmpty() ? "" : ": " + clip(text)), false));
            return out;
        } catch (Throwable t) {
            error(rpc, label, t);
            return null;
        } finally {
            ctx.cancel(null);
        }
    }

    /** Client-side re-validation of what the server sent; false (and a failure row) when invalid. */
    private boolean violations(String rpc, String label, Message message) {
        List<ConfluenceValidator.Violation> v = validate(message);
        if (v.isEmpty()) {
            return true;
        }
        rows.add(new Row(rpc, label, "MAPPING", clip(v.toString()), true));
        return false;
    }

    /**
     * The validator checks domain messages and ignores the rpc wrappers, so
     * validate the wrapper and each domain message it carries.
     */
    private static List<ConfluenceValidator.Violation> validate(Message message) {
        List<ConfluenceValidator.Violation> out = new ArrayList<>(VALIDATOR.validate(message).violations());
        message.getAllFields().forEach((field, value) -> {
            if (field.getJavaType() != com.google.protobuf.Descriptors.FieldDescriptor.JavaType.MESSAGE
                    || field.isMapField()) {
                return;
            }
            if (field.isRepeated()) {
                for (Object item : (List<?>) value) {
                    out.addAll(VALIDATOR.validate((Message) item).violations());
                }
            } else {
                out.addAll(VALIDATOR.validate((Message) value).violations());
            }
        });
        return out;
    }

    private void error(String rpc, String label, Throwable t) {
        if (!(t instanceof StatusRuntimeException e)) {
            rows.add(new Row(rpc, label, "EXCEPTION", clip(t.getClass().getName() + ": " + t.getMessage()), true));
            return;
        }
        Status status = e.getStatus();
        String description = status.getDescription() == null ? "" : status.getDescription();
        int http = httpStatus(description);
        String detail = ROUTES.getOrDefault(rpc, "?") + (http > 0 ? ", HTTP " + http : "") + ": " + description;
        String note = APP_CREDENTIAL_RPCS.contains(rpc) ? " [needs app credentials]"
                : ADMIN_RPCS.contains(rpc) ? " [needs admin]" : "";
        switch (status.getCode()) {
            case NOT_FOUND, PERMISSION_DENIED, UNAUTHENTICATED, UNIMPLEMENTED, INVALID_ARGUMENT ->
                    rows.add(new Row(rpc, label, status.getCode().name(), clip(detail) + note, false));
            case INTERNAL, UNKNOWN -> {
                if (http >= 500) {
                    rows.add(new Row(rpc, label, "UPSTREAM_" + http, clip(detail) + note, false));
                } else if (http > 0 && !note.isEmpty()) {
                    rows.add(new Row(rpc, label, status.getCode().name(), clip(detail) + note
                            + " (reported, not failed)", false));
                } else {
                    rows.add(new Row(rpc, label, status.getCode().name(), clip(detail), true));
                }
            }
            case FAILED_PRECONDITION -> {
                // The server's own validator rejected what the mapper produced.
                boolean mapping = description.startsWith("confluence validation failed");
                rows.add(new Row(rpc, label, mapping ? "MAPPING" : "FAILED_PRECONDITION", clip(detail) + note,
                        mapping));
            }
            default -> rows.add(new Row(rpc, label, status.getCode().name(), clip(detail) + note, false));
        }
    }

    /** The HTTP status in a "Confluence 403: ..." description, or 0. */
    private static int httpStatus(String description) {
        String prefix = "Confluence ";
        int at = description.indexOf(prefix);
        if (at < 0) {
            return 0;
        }
        int value = 0;
        int digits = 0;
        for (int i = at + prefix.length(); i < description.length() && digits < 3; i++) {
            char c = description.charAt(i);
            if (c < '0' || c > '9') {
                break;
            }
            value = value * 10 + (c - '0');
            digits++;
        }
        return digits == 3 ? value : 0;
    }

    private void skip(String rpc, String reason) {
        skip(rpc, rpc, reason);
    }

    private void skip(String rpc, String label, String reason) {
        rows.add(new Row(rpc, label, "SKIPPED", "no data: " + reason, false));
    }

    private void skipAll(String reason) {
        for (MethodDescriptor<?, ?> method : ConfluenceServiceGrpc.getServiceDescriptor().getMethods()) {
            String rpc = method.getBareMethodName();
            if (rows.stream().noneMatch(row -> row.rpc().equals(rpc))) {
                skip(rpc, reason);
            }
        }
    }

    private static ContentRef ref(ContentType type, String id) {
        return ContentRef.newBuilder().setType(type).setId(id).build();
    }

    private static <T> String join(List<T> list, Function<T, String> f) {
        List<String> parts = new ArrayList<>();
        list.forEach(item -> parts.add(f.apply(item)));
        return String.join(", ", parts);
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (!value.isBlank()) {
                return value;
            }
        }
        return "?";
    }

    private static String clip(String text) {
        String flat = text == null ? "" : text.replace('\n', ' ').replace('\r', ' ');
        return flat.length() <= 300 ? flat : flat.substring(0, 300) + "...";
    }

    // ======================================================================
    // REPORT
    // ======================================================================

    private String report(String host, Instant started) {
        StringBuilder md = new StringBuilder();
        md.append("# Confluence live coverage\n\n");
        md.append("- Site: ").append(host).append('\n');
        md.append("- Run: ").append(started).append(" (")
                .append(java.time.Duration.between(started, Instant.now()).toSeconds()).append(" s)\n");
        long failures = rows.stream().filter(Row::failure).count();
        Map<String, Integer> outcomes = new TreeMap<>();
        rows.forEach(row -> outcomes.merge(row.outcome(), 1, Integer::sum));
        md.append("- Calls: ").append(rows.size()).append(", outcomes ").append(outcomes)
                .append(", failures ").append(failures).append("\n\n");

        md.append("## Discovered ids\n\n| entity | id / notes |\n|---|---|\n");
        discovered.forEach((k, v) -> md.append("| ").append(cell(k)).append(" | ").append(cell(v)).append(" |\n"));

        md.append("\n## RPCs\n\n| rpc | outcome | detail |\n|---|---|---|\n");
        for (Row row : rows) {
            md.append("| ").append(cell(row.label())).append(" | ")
                    .append(row.failure() ? "**" + row.outcome() + " (FAIL)**" : row.outcome())
                    .append(" | ").append(cell(row.detail())).append(" |\n");
        }

        md.append("\n## Sync\n\n");
        syncLines.forEach(line -> md.append(cell(line)).append('\n'));

        md.append("\n## Needs other credentials\n\n");
        md.append("- App (Connect/Forge) credentials only: ").append(String.join(", ", new java.util.TreeSet<>(
                APP_CREDENTIAL_RPCS))).append(". With an API token they answer PERMISSION_DENIED / NOT_FOUND;"
                + " reported, never failed.\n");
        md.append("- Admin user (or an enabled admin key): ").append(String.join(", ",
                new java.util.TreeSet<>(ADMIN_RPCS))).append(".\n");

        md.append("\n## Failures\n\n");
        if (failures == 0) {
            md.append("none\n");
        } else {
            rows.stream().filter(Row::failure).forEach(row -> md.append("- ").append(cell(row.label()))
                    .append(": ").append(row.outcome()).append(' ').append(cell(row.detail())).append('\n'));
        }
        return scrub(md.toString());
    }

    private static String cell(String text) {
        return clip(text).replace("|", "\\|");
    }

    /** Belt and braces: the credential values never reach the report. */
    private String scrub(String text) {
        String out = text;
        if (token != null && !token.isBlank()) {
            out = out.replace(token, "***");
        }
        if (email != null && !email.isBlank()) {
            out = out.replace(email, "***");
        }
        return out;
    }
}
