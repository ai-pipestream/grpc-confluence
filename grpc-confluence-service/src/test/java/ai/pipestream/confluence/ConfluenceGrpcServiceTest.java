package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.*;
import ai.pipestream.confluence.v1.BodyFormat;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.ContentIncludes;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.confluence.v1.GetAttachmentRequest;
import ai.pipestream.confluence.v1.GetAttachmentResponse;
import ai.pipestream.confluence.v1.ListAttachmentsRequest;
import ai.pipestream.confluence.v1.ListAttachmentsResponse;
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
import ai.pipestream.confluence.v1.SortField;
import ai.pipestream.confluence.v1.SortOrder;
import ai.pipestream.confluence.v1.SpaceStatus;
import ai.pipestream.confluence.v1.SpaceType;
import ai.pipestream.confluence.v1.StreamSpacesRequest;
import ai.pipestream.confluence.v1.StreamSpacesResponse;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import ai.pipestream.confluence.ConfluenceValidator;
import com.google.protobuf.DescriptorProtos;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;
import io.grpc.reflection.v1alpha.ServerReflectionRequest;
import io.grpc.reflection.v1alpha.ServerReflectionResponse;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The facade end to end against the fake REST server: an in-process gRPC
 * server (reflection on, virtual-thread executor like the real launcher) plus
 * the generated blocking stub, so every test exercises the full wire path.
 */
class ConfluenceGrpcServiceTest {

    private static final Instant T1 = Instant.parse("2024-03-01T00:00:00Z");
    private static final Instant T2 = Instant.parse("2024-03-02T00:00:00Z");
    private static final Instant T3 = Instant.parse("2024-03-03T00:00:00Z");
    private static final ConfluenceValidator VALIDATOR = ConfluenceValidator.create();

    private FakeConfluenceServer fake;
    private ConfluenceConnectorConfig config;
    private ConfluenceClient client;
    private Server server;
    private ManagedChannel channel;
    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub;

    @BeforeEach
    void startStack() throws Exception {
        fake = FakeConfluenceServer.start();
        config = ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("token-123")
                .build();
        client = new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                Duration.ZERO);
        startServer(new ConfluenceGrpcService(config, client,
                ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES));
    }

    private void startServer(ConfluenceGrpcService service) throws Exception {
        server = InProcessServerBuilder.forName("confluence-facade-test")
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(service)
                .addService(ProtoReflectionService.newInstance())
                .build().start();
        channel = InProcessChannelBuilder.forName("confluence-facade-test").build();
        stub = ConfluenceServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void stopStack() {
        channel.shutdownNow();
        server.shutdownNow();
        fake.close();
    }

    // ======================================================================
    // READ RPCS
    // ======================================================================

    @Test
    void listSpacesReturnsAllOrFilteredByKeys() {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering"),
                        ConfluenceFixtures.spaceJson("101", "DOCS", "Documentation")));
        // The API applies the keys filter server-side; the fake mirrors that
        // with an exact-query stub.
        fake.stub("/wiki/api/v2/spaces?keys=ENG&limit=100",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));

        ListSpacesResponse all = stub.listSpaces(ListSpacesRequest.getDefaultInstance());
        assertThat(all.getSpacesList()).extracting(s -> s.getKey())
                .containsExactlyInAnyOrder("ENG", "DOCS");
        all.getSpacesList().forEach(space -> assertThat(VALIDATOR.validate(space).violations())
                .as("space %s validates", space.getKey()).isEmpty());

        ListSpacesResponse filtered = stub.listSpaces(ListSpacesRequest.newBuilder()
                .addKeys("ENG").build());
        assertThat(filtered.getSpacesList()).extracting(s -> s.getKey()).containsExactly("ENG");
        assertThat(fake.requestsTo("/wiki/api/v2/spaces").getLast().query()).contains("keys=ENG");
    }

    @Test
    void streamSpacesStreamsEachSpaceAndForwardsFilters() {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering"),
                        ConfluenceFixtures.spaceJson("101", "DOCS", "Documentation")));

        List<StreamSpacesResponse> all = new ArrayList<>();
        stub.streamSpaces(StreamSpacesRequest.getDefaultInstance()).forEachRemaining(all::add);
        assertThat(all).extracting(r -> r.getSpace().getKey())
                .containsExactlyInAnyOrder("ENG", "DOCS");

        List<StreamSpacesResponse> capped = new ArrayList<>();
        stub.streamSpaces(StreamSpacesRequest.newBuilder()
                .setLimit(1)
                .addKeys("ENG")
                .setType(SpaceType.SPACE_TYPE_KNOWLEDGE_BASE)
                .setStatus(SpaceStatus.SPACE_STATUS_TRASHED)
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_NAME)
                        .setDescending(true))
                .build()).forEachRemaining(capped::add);
        assertThat(capped).hasSize(1);
        String query = fake.requestsTo("/wiki/api/v2/spaces").getLast().query();
        assertThat(query).contains("keys=ENG", "type=knowledge_base", "status=trashed",
                "sort=-name");
    }

    @Test
    void getPageReturnsBodyInStorageFormatByDefault() {
        fake.stub("/wiki/api/v2/pages/200",
                ConfluenceFixtures.pageJson("200", "100", "Design Doc", T2.toString()));

        GetPageResponse response = stub.getPage(GetPageRequest.newBuilder().setId("200").build());

        assertThat(response.getPage().getTitle()).isEqualTo("Design Doc");
        assertThat(response.getPage().getBody().getStorage().getValue())
                .isEqualTo("<p>Hello Design Doc</p>");
        assertThat(response.getPage().getBody().getStorage().getFormat())
                .isEqualTo(BodyFormat.BODY_FORMAT_STORAGE_XHTML);
        assertThat(VALIDATOR.validate(response.getPage()).violations()).isEmpty();
        assertThat(fake.requestsTo("/wiki/api/v2/pages/200").getLast().query())
                .contains("body-format=storage");
    }

    @Test
    void getPageMapsCollaboratorsAndWebResourcesWhenIncluded() {
        // Shapes as a live tenant returns them (ConfluenceLiveSmokeIT, 2026-11-08).
        String base = ConfluenceFixtures.pageJson("200", "100", "Design Doc", T2.toString());
        String page = "{\"collaborators\":[{\"accountId\":\"acc-1\"},"
                + "{\"accountId\":\"acc-2\"}],\"webResources\":{\"tags\":\"<script/>\","
                + "\"superbatch\":{\"tags\":\"<link/>\"}}," + base.substring(base.indexOf('{') + 1);
        fake.stub("/wiki/api/v2/pages/200", page);

        GetPageResponse response = stub.getPage(GetPageRequest.newBuilder().setId("200")
                .setInclude(ContentIncludes.newBuilder().setCollaborators(true)
                        .setWebResources(true))
                .build());

        assertThat(response.getPage().getCollaboratorAccountIdsList())
                .containsExactly("acc-1", "acc-2");
        assertThat(response.getPage().getWebResources().getTags()).isEqualTo("<script/>");
        assertThat(response.getPage().getWebResources().getSuperbatch().getTags())
                .isEqualTo("<link/>");
        assertThat(fake.requestsTo("/wiki/api/v2/pages/200").getLast().query())
                .contains("include-collaborators=true", "include-webresources=true");
    }

    @Test
    void getBlogPostReturnsTheWrappedPost() {
        fake.stub("/wiki/api/v2/blogposts/300",
                ConfluenceFixtures.blogPostJson("300", "100", "Release notes", T1.toString()));

        GetBlogPostResponse response = stub.getBlogPost(
                GetBlogPostRequest.newBuilder().setId("300").build());

        assertThat(response.getBlogPost().getTitle()).isEqualTo("Release notes");
        assertThat(response.getBlogPost().getBody().getStorage().getValue())
                .isEqualTo("<p>Post Release notes</p>");
        assertThat(VALIDATOR.validate(response.getBlogPost()).violations()).isEmpty();
    }

    @Test
    void unknownPageIsNotFound() {
        assertThatThrownBy(() -> stub.getPage(GetPageRequest.newBuilder().setId("nope").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
    }

    @Test
    void listPagesStreamsAcrossRestPagesWithoutBodies() {
        // No body-format requested: the API answers metadata-only entries.
        fake.stub("/wiki/api/v2/pages?limit=100&space-id=100",
                ConfluenceFixtures.pageListJson("cursor-2",
                        ConfluenceFixtures.pageJsonMetadataOnly("201", "100", "One", T1.toString()),
                        ConfluenceFixtures.pageJsonMetadataOnly("202", "100", "Two", T2.toString())));
        fake.stub("/wiki/api/v2/pages?cursor=cursor-2",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJsonMetadataOnly("203", "100", "Three",
                                T3.toString())));

        List<ListPagesResponse> pages = new ArrayList<>();
        stub.listPages(ListPagesRequest.newBuilder().setSpaceId("100").build())
                .forEachRemaining(pages::add);

        // The stream hid the REST pagination: three pages from two REST responses.
        assertThat(pages).hasSize(3);
        assertThat(pages).extracting(p -> p.getPage().getTitle())
                .containsExactly("One", "Two", "Three");
        // Metadata-only listing: no body-format requested, so bodies stay unset.
        assertThat(pages).allSatisfy(p -> assertThat(p.getPage().hasBody()).isFalse());
        assertThat(fake.requestsTo("/wiki/api/v2/pages").getFirst().query())
                .doesNotContain("body-format");
    }

    @Test
    void listBlogPostsStreamsWithBodiesWhenRequested() {
        fake.stub("/wiki/api/v2/blogposts",
                ConfluenceFixtures.blogPostListJson(null,
                        ConfluenceFixtures.blogPostJson("300", "100", "Release notes",
                                T1.toString())));

        List<ListBlogPostsResponse> posts = new ArrayList<>();
        stub.listBlogPosts(ListBlogPostsRequest.newBuilder()
                        .setSpaceId("100")
                        .setBodyFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML)
                        .build())
                .forEachRemaining(posts::add);

        assertThat(posts).hasSize(1);
        assertThat(posts.get(0).getBlogPost().getBody().getStorage().getValue())
                .isEqualTo("<p>Post Release notes</p>");
        assertThat(fake.requestsTo("/wiki/api/v2/blogposts").getFirst().query())
                .contains("body-format=storage");
    }

    // ======================================================================
    // SPACES, PAGES, BLOG POSTS: request fields reach the v2 query
    // ======================================================================

    private static void assertInvalid(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class,
                e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void listSpacesForwardsEveryFilter() {
        fake.stub("/wiki/api/v2/spaces", ConfluenceFixtures.spaceListJson(null,
                ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));

        stub.listSpaces(ListSpacesRequest.newBuilder()
                .addKeys("ENG").addKeys("DOCS").addIds("100").addLabels("team")
                .setType(SpaceType.SPACE_TYPE_GLOBAL).setStatus(SpaceStatus.SPACE_STATUS_ARCHIVED)
                .setFavoritedBy("acc-1").setNotFavoritedBy("acc-2")
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_KEY))
                .setDescriptionFormat(SpaceDescriptionFormat.SPACE_DESCRIPTION_FORMAT_PLAIN)
                .setIncludeIcon(true)
                .build());

        java.util.Map<String, String> expected = new java.util.HashMap<>(java.util.Map.of(
                "keys", "ENG,DOCS", "ids", "100", "labels", "team",
                "type", "global", "status", "archived", "favorited-by", "acc-1",
                "not-favorited-by", "acc-2", "sort", "key", "description-format", "plain"));
        expected.put("include-icon", "true");
        expected.put("limit", "100");
        assertThat(fake.lastTo("/wiki/api/v2/spaces").params())
                .containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    void listSpacesRejectsASortTheRouteDoesNotOffer() {
        assertInvalid(() -> stub.listSpaces(ListSpacesRequest.newBuilder()
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_TITLE)).build()));
    }

    @Test
    void getSpaceByIdSendsIncludesAndMapsExpansions() {
        String space = ConfluenceFixtures.spaceJson("100", "ENG", "Engineering");
        fake.stub("/wiki/api/v2/spaces/100", "{\"labels\":{\"results\":[{\"id\":\"l1\","
                + "\"name\":\"team\",\"prefix\":\"global\"}]},"
                + "\"permissions\":{\"results\":[{\"id\":\"sp-1\",\"principal\":{\"type\":\"user\","
                + "\"id\":\"acc-9\"},\"operation\":{\"key\":\"read\",\"targetType\":\"space\"}}]},"
                + space.substring(space.indexOf('{') + 1));

        GetSpaceResponse response = stub.getSpace(GetSpaceRequest.newBuilder().setId("100")
                .setDescriptionFormat(SpaceDescriptionFormat.SPACE_DESCRIPTION_FORMAT_VIEW)
                .setInclude(SpaceIncludes.newBuilder().setIcon(true).setOperations(true)
                        .setProperties(true).setPermissions(true).setRoleAssignments(true)
                        .setLabels(true))
                .build());

        assertThat(response.getSpace().getKey()).isEqualTo("ENG");
        assertThat(response.getSpace().getLabelsList()).extracting(l -> l.getName())
                .containsExactly("team");
        assertThat(response.getSpace().getPermissionsList()).singleElement().satisfies(p -> {
            assertThat(p.getPrincipal().getId()).isEqualTo("acc-9");
            assertThat(p.getOperation().getKey())
                    .isEqualTo(SpacePermissionOperationKey.SPACE_PERMISSION_OPERATION_KEY_READ);
        });
        assertThat(fake.lastTo("/wiki/api/v2/spaces/100").params()).containsExactlyInAnyOrderEntriesOf(
                java.util.Map.of("description-format", "view", "include-icon", "true",
                        "include-operations", "true", "include-properties", "true",
                        "include-permissions", "true", "include-role-assignments", "true",
                        "include-labels", "true"));
    }

    @Test
    void getSpaceByKeyResolvesTheIdFirst() {
        fake.stub("/wiki/api/v2/spaces?keys=ENG&limit=1", ConfluenceFixtures.spaceListJson(null,
                ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100", ConfluenceFixtures.spaceJson("100", "ENG", "Engineering"));

        GetSpaceResponse response = stub.getSpace(GetSpaceRequest.newBuilder().setKey("ENG").build());

        assertThat(response.getSpace().getId()).isEqualTo("100");
        assertThat(fake.lastTo("/wiki/api/v2/spaces/100").query()).isEmpty();
    }

    @Test
    void getSpaceByUnknownKeyIsNotFound() {
        fake.stub("/wiki/api/v2/spaces", ConfluenceFixtures.emptyListJson());
        assertThatThrownBy(() -> stub.getSpace(GetSpaceRequest.newBuilder().setKey("NOPE").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
    }

    @Test
    void getPageForwardsVersionDraftStatusesAndEveryInclude() {
        String page = ConfluenceFixtures.pageJson("200", "100", "Design Doc", T2.toString());
        fake.stub("/wiki/api/v2/pages/200", "{\"labels\":{\"results\":[{\"id\":\"l1\","
                + "\"name\":\"design\",\"prefix\":\"global\"}]},\"likes\":{\"results\":"
                + "[{\"accountId\":\"acc-7\"}]},\"isFavoritedByCurrentUser\":true,"
                + "\"directChildren\":{\"results\":[{\"id\":\"w1\",\"type\":\"whiteboard\","
                + "\"title\":\"Board\",\"status\":\"current\"}]},"
                + page.substring(page.indexOf('{') + 1));

        GetPageResponse response = stub.getPage(GetPageRequest.newBuilder().setId("200")
                .setBodyFormat(BodyFormat.BODY_FORMAT_EXPORT_XHTML)
                .setVersion(3).setGetDraft(true)
                .addStatuses(ContentStatus.CONTENT_STATUS_CURRENT)
                .addStatuses(ContentStatus.CONTENT_STATUS_HISTORICAL)
                .setInclude(ContentIncludes.newBuilder().setLabels(true).setProperties(true)
                        .setOperations(true).setLikes(true).setVersions(true)
                        .setFavoritedByCurrentUser(true).setDirectChildren(true)
                        .setCollaborators(true).setWebResources(true))
                .build());

        Page result = response.getPage();
        assertThat(result.getLabelsList()).extracting(l -> l.getName()).containsExactly("design");
        assertThat(result.getLikesList()).extracting(l -> l.getAccountId()).containsExactly("acc-7");
        assertThat(result.getIsFavoritedByCurrentUser()).isTrue();
        assertThat(result.getDirectChildrenList()).extracting(c -> c.getType())
                .containsExactly("whiteboard");
        java.util.Map<String, String> expected = new java.util.HashMap<>(java.util.Map.of(
                "body-format", "export_view", "version", "3", "get-draft", "true",
                "status", "current,historical", "include-labels", "true",
                "include-properties", "true", "include-operations", "true",
                "include-likes", "true", "include-versions", "true"));
        expected.putAll(java.util.Map.of("include-favorited-by-current-user-status", "true",
                "include-direct-children", "true", "include-collaborators", "true",
                "include-webresources", "true"));
        assertThat(fake.lastTo("/wiki/api/v2/pages/200").params())
                .containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    void getPageRejectsAStatusTheRouteDoesNotAccept() {
        assertInvalid(() -> stub.getPage(GetPageRequest.newBuilder().setId("200")
                .addStatuses(ContentStatus.CONTENT_STATUS_ANY).build()));
        assertInvalid(() -> stub.getPage(GetPageRequest.newBuilder().setId("200")
                .setBodyFormat(BodyFormat.BODY_FORMAT_WIKI).build()));
    }

    @Test
    void getBlogPostForwardsVersionDraftStatusesAndOfferedIncludesOnly() {
        fake.stub("/wiki/api/v2/blogposts/300",
                ConfluenceFixtures.blogPostJson("300", "100", "Release notes", T1.toString()));

        stub.getBlogPost(GetBlogPostRequest.newBuilder().setId("300")
                .setBodyFormat(BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT)
                .setVersion(2).setGetDraft(true)
                .addStatuses(BlogPostContentStatus.BLOG_POST_CONTENT_STATUS_DRAFT)
                .setInclude(ContentIncludes.newBuilder().setLikes(true).setDirectChildren(true))
                .build());

        // include-direct-children is a page-only flag: ignored for blog posts.
        assertThat(fake.lastTo("/wiki/api/v2/blogposts/300").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("body-format", "atlas_doc_format",
                        "version", "2", "get-draft", "true", "status", "draft",
                        "include-likes", "true"));
    }

    @Test
    void listPagesForwardsEveryWorkspaceFilter() {
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson(null,
                ConfluenceFixtures.pageJson("200", "100", "Design Doc", T2.toString())));

        List<ListPagesResponse> pages = new ArrayList<>();
        stub.listPages(ListPagesRequest.newBuilder()
                .addSpaceIds("100").addSpaceIds("101").addIds("200").addIds("201")
                .addStatuses(ContentStatus.CONTENT_STATUS_CURRENT)
                .addStatuses(ContentStatus.CONTENT_STATUS_ARCHIVED)
                .setTitle("Design Doc").setSubtype(PageSubtype.PAGE_SUBTYPE_LIVE)
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_CREATED_DATE)
                        .setDescending(true))
                .setBodyFormat(BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT)
                .build()).forEachRemaining(pages::add);

        assertThat(pages).extracting(p -> p.getPage().getId()).containsExactly("200");
        assertThat(fake.lastTo("/wiki/api/v2/pages").params()).containsExactlyInAnyOrderEntriesOf(
                java.util.Map.of("limit", "100", "space-id", "100,101", "id", "200,201",
                        "status", "current,archived", "title", "Design Doc", "subtype", "live",
                        "sort", "-created-date", "body-format", "atlas_doc_format"));
    }

    @Test
    void listPagesRootOnlyRoutesThroughTheSpace() {
        fake.stub("/wiki/api/v2/spaces/100/pages", ConfluenceFixtures.pageListJson(null,
                ConfluenceFixtures.pageJsonMetadataOnly("200", "100", "Home", T1.toString())));

        List<ListPagesResponse> pages = new ArrayList<>();
        stub.listPages(ListPagesRequest.newBuilder().setSpaceId("100").setRootOnly(true)
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_TITLE)).build())
                .forEachRemaining(pages::add);

        assertThat(pages).hasSize(1);
        assertThat(fake.lastTo("/wiki/api/v2/spaces/100/pages").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100",
                        "depth", "root", "sort", "title"));
        assertInvalid(() -> stub.listPages(ListPagesRequest.newBuilder().setRootOnly(true).build())
                .hasNext());
    }

    @Test
    void listPagesByLabelRoutesThroughTheLabel() {
        fake.stub("/wiki/api/v2/labels/l1/pages", ConfluenceFixtures.pageListJson(null,
                ConfluenceFixtures.pageJsonMetadataOnly("200", "100", "Home", T1.toString())));

        List<ListPagesResponse> pages = new ArrayList<>();
        stub.listPages(ListPagesRequest.newBuilder().setLabelId("l1").setSpaceId("100")
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_ID)).build())
                .forEachRemaining(pages::add);

        assertThat(pages).hasSize(1);
        assertThat(fake.lastTo("/wiki/api/v2/labels/l1/pages").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100",
                        "space-id", "100", "sort", "id"));
        // The label route has no title filter: rejected, not silently dropped.
        assertInvalid(() -> stub.listPages(ListPagesRequest.newBuilder().setLabelId("l1")
                .setTitle("x").build()).hasNext());
        // List routes only offer storage and atlas_doc_format bodies.
        assertInvalid(() -> stub.listPages(ListPagesRequest.newBuilder()
                .setBodyFormat(BodyFormat.BODY_FORMAT_RENDERED_XHTML).build()).hasNext());
    }

    @Test
    void listBlogPostsForwardsFiltersAndRoutesLabels() {
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.blogPostListJson(null,
                ConfluenceFixtures.blogPostJson("300", "100", "Release notes", T1.toString())));
        fake.stub("/wiki/api/v2/labels/l1/blogposts", ConfluenceFixtures.blogPostListJson(null,
                ConfluenceFixtures.blogPostJson("300", "100", "Release notes", T1.toString())));

        stub.listBlogPosts(ListBlogPostsRequest.newBuilder().addIds("300").addSpaceIds("100")
                .addStatuses(BlogPostContentStatus.BLOG_POST_CONTENT_STATUS_TRASHED)
                .setTitle("Release notes")
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_MODIFIED_DATE))
                .build()).forEachRemaining(r -> { });
        assertThat(fake.lastTo("/wiki/api/v2/blogposts").params()).containsExactlyInAnyOrderEntriesOf(
                java.util.Map.of("limit", "100", "id", "300", "space-id", "100",
                        "status", "trashed", "title", "Release notes", "sort", "modified-date"));

        List<ListBlogPostsResponse> labelled = new ArrayList<>();
        stub.listBlogPosts(ListBlogPostsRequest.newBuilder().setLabelId("l1").build())
                .forEachRemaining(labelled::add);
        assertThat(labelled).extracting(r -> r.getBlogPost().getId()).containsExactly("300");
        // Blog posts are not sortable by title.
        assertInvalid(() -> stub.listBlogPosts(ListBlogPostsRequest.newBuilder()
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_TITLE)).build())
                .hasNext());
    }

    // ======================================================================
    // ATTACHMENTS
    // ======================================================================

    @Test
    void getAttachmentMetadataOnlyByDefault() {
        fake.stub("/wiki/api/v2/attachments/a1", ConfluenceFixtures.attachmentJson("a1", "200"));

        GetAttachmentResponse response = stub.getAttachment(
                GetAttachmentRequest.newBuilder().setId("a1").build());

        assertThat(response.getAttachment().getTitle()).isEqualTo("diagram.png");
        assertThat(response.getAttachment().getFileSize()).isEqualTo(12345L);
        assertThat(response.getAttachment().hasContent()).isFalse();
        assertThat(VALIDATOR.validate(response.getAttachment()).violations()).isEmpty();
    }

    @Test
    void getAttachmentWithContentDownloadsThroughTheClient() {
        fake.stub("/wiki/api/v2/attachments/a1", ConfluenceFixtures.attachmentJson("a1", "200"));
        fake.stub("/wiki/download/attachments/a1/diagram.png", "png-bytes");

        GetAttachmentResponse response = stub.getAttachment(GetAttachmentRequest.newBuilder()
                .setId("a1").setIncludeContent(true).build());

        assertThat(response.getAttachment().hasContent()).isTrue();
        assertThat(response.getAttachment().getContent().toString(StandardCharsets.UTF_8))
                .isEqualTo("png-bytes");
    }

    @Test
    void getAttachmentAboveTheInlineCapIsFailedPrecondition() throws Exception {
        server.shutdown();
        channel.shutdownNow();
        startServer(new ConfluenceGrpcService(config, client, 100));
        fake.stub("/wiki/api/v2/attachments/a1", ConfluenceFixtures.attachmentJson("a1", "200"));

        assertThatThrownBy(() -> stub.getAttachment(GetAttachmentRequest.newBuilder()
                .setId("a1").setIncludeContent(true).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> {
                            assertThat(e.getStatus().getCode())
                                    .isEqualTo(Status.Code.FAILED_PRECONDITION);
                            assertThat(e.getStatus().getDescription()).contains("inline cap");
                        });
    }

    @Test
    void listAttachmentsStreamsPageAttachments() {
        fake.stub("/wiki/api/v2/pages/200/attachments",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.attachmentJson("a1", "200")));

        List<ListAttachmentsResponse> items = new ArrayList<>();
        stub.listAttachments(ListAttachmentsRequest.newBuilder().setPageId("200").build())
                .forEachRemaining(items::add);

        assertThat(items).extracting(r -> r.getAttachment().getId()).containsExactly("a1");
        assertThat(items.get(0).getAttachment().getTitle()).isEqualTo("diagram.png");
        assertThat(VALIDATOR.validate(items.get(0).getAttachment()).violations()).isEmpty();
    }

    @Test
    void listAttachmentsStreamsBlogPostAttachments() {
        fake.stub("/wiki/api/v2/blogposts/300/attachments",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.attachmentJson("a2", "300")));

        List<ListAttachmentsResponse> items = new ArrayList<>();
        stub.listAttachments(ListAttachmentsRequest.newBuilder().setBlogPostId("300").build())
                .forEachRemaining(items::add);

        assertThat(items).extracting(r -> r.getAttachment().getId()).containsExactly("a2");
    }

    @Test
    void listAttachmentsWithoutAContainerListsTheWorkspace() {
        fake.stub("/wiki/api/v2/attachments", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.attachmentJson("a1", "200")));

        List<ListAttachmentsResponse> items = new ArrayList<>();
        stub.listAttachments(ListAttachmentsRequest.newBuilder()
                .addStatuses(ContentStatus.CONTENT_STATUS_CURRENT)
                .addStatuses(ContentStatus.CONTENT_STATUS_TRASHED)
                .setMediaType("image/png").setFilename("diagram.png")
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_MODIFIED_DATE)
                        .setDescending(true))
                .build()).forEachRemaining(items::add);

        assertThat(items).extracting(r -> r.getAttachment().getId()).containsExactly("a1");
        assertThat(fake.lastTo("/wiki/api/v2/attachments").params()).containsExactlyInAnyOrderEntriesOf(
                java.util.Map.of("limit", "100", "status", "current,trashed",
                        "mediaType", "image/png", "filename", "diagram.png",
                        "sort", "-modified-date"));
    }

    @Test
    void listAttachmentsOfCustomContentAndLabels() {
        fake.stub("/wiki/api/v2/custom-content/cc1/attachments", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.attachmentJson("a3", "200")));
        fake.stub("/wiki/api/v2/labels/l1/attachments", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.attachmentJson("a4", "200")));

        List<ListAttachmentsResponse> custom = new ArrayList<>();
        stub.listAttachments(ListAttachmentsRequest.newBuilder().setCustomContentId("cc1")
                .setFilename("diagram.png").build()).forEachRemaining(custom::add);
        assertThat(custom).extracting(r -> r.getAttachment().getId()).containsExactly("a3");
        assertThat(fake.lastTo("/wiki/api/v2/custom-content/cc1/attachments").params())
                .containsEntry("filename", "diagram.png");

        List<ListAttachmentsResponse> labelled = new ArrayList<>();
        stub.listAttachments(ListAttachmentsRequest.newBuilder().setLabelId("l1")
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_CREATED_DATE))
                .build()).forEachRemaining(labelled::add);
        assertThat(labelled).extracting(r -> r.getAttachment().getId()).containsExactly("a4");
        assertThat(fake.lastTo("/wiki/api/v2/labels/l1/attachments").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100",
                        "sort", "created-date"));
        // The label route has no media type filter; attachments have no title sort.
        assertInvalid(() -> stub.listAttachments(ListAttachmentsRequest.newBuilder()
                .setLabelId("l1").setMediaType("image/png").build()).hasNext());
        assertInvalid(() -> stub.listAttachments(ListAttachmentsRequest.newBuilder()
                .setPageId("200").setSort(SortOrder.newBuilder()
                        .setField(SortField.SORT_FIELD_TITLE)).build()).hasNext());
    }

    @Test
    void getAttachmentForwardsVersionAndIncludesAndHonoursMaxContentBytes() {
        fake.stub("/wiki/api/v2/attachments/a1", ConfluenceFixtures.attachmentJson("a1", "200"));
        fake.stub("/wiki/download/attachments/a1/diagram.png", "png-bytes");

        GetAttachmentResponse response = stub.getAttachment(GetAttachmentRequest.newBuilder()
                .setId("a1").setVersion(2).setIncludeContent(true).setMaxContentBytes(20_000)
                .setInclude(ContentIncludes.newBuilder().setLabels(true).setVersions(true)
                        .setLikes(true))
                .build());

        assertThat(response.getAttachment().getContent().toString(StandardCharsets.UTF_8))
                .isEqualTo("png-bytes");
        // include-likes is not offered on attachments: ignored.
        assertThat(fake.lastTo("/wiki/api/v2/attachments/a1").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("version", "2",
                        "include-labels", "true", "include-versions", "true"));

        // file_size 12345 is above a 100-byte request ceiling.
        assertThatThrownBy(() -> stub.getAttachment(GetAttachmentRequest.newBuilder()
                .setId("a1").setIncludeContent(true).setMaxContentBytes(100).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(
                        e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION));
    }

    private List<DownloadAttachmentResponse> download(DownloadAttachmentRequest request) {
        List<DownloadAttachmentResponse> parts = new ArrayList<>();
        stub.downloadAttachment(request).forEachRemaining(parts::add);
        return parts;
    }

    @Test
    void downloadAttachmentStreamsMetadataThenChunks() {
        fake.stub("/wiki/api/v2/attachments/a1", ConfluenceFixtures.attachmentJson("a1", "200"));
        fake.stub("/wiki/download/attachments/a1/diagram.png", "png-bytes");

        List<DownloadAttachmentResponse> parts = download(DownloadAttachmentRequest.newBuilder()
                .setId("a1").setVersion(1).setChunkBytes(4).build());

        assertThat(parts.get(0).getPartCase())
                .isEqualTo(DownloadAttachmentResponse.PartCase.ATTACHMENT);
        assertThat(parts.get(0).getAttachment().getId()).isEqualTo("a1");
        assertThat(parts.get(0).getAttachment().hasContent()).isFalse();
        assertThat(parts.subList(1, parts.size()))
                .extracting(p -> p.getData().toString(StandardCharsets.UTF_8))
                .containsExactly("png-", "byte", "s");
        assertThat(fake.lastTo("/wiki/api/v2/attachments/a1").params())
                .containsExactlyEntriesOf(java.util.Map.of("version", "1"));
    }

    @Test
    void downloadAttachmentUsesTheConfiguredChunkDefault() throws Exception {
        server.shutdown();
        channel.shutdownNow();
        startServer(new ConfluenceGrpcService(config, client,
                ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES, null, null, null, 5));
        fake.stub("/wiki/api/v2/attachments/a1", ConfluenceFixtures.attachmentJson("a1", "200"));
        fake.stub("/wiki/download/attachments/a1/diagram.png", "png-bytes");

        List<DownloadAttachmentResponse> parts = download(
                DownloadAttachmentRequest.newBuilder().setId("a1").build());

        assertThat(parts.subList(1, parts.size()))
                .extracting(p -> p.getData().toString(StandardCharsets.UTF_8))
                .containsExactly("png-b", "ytes");
        assertThat(ConfluenceGrpcService.DEFAULT_DOWNLOAD_CHUNK_BYTES).isEqualTo(1024 * 1024);
    }

    @Test
    void downloadAttachmentAboveMaxBytesFailsBeforeAnyChunk() {
        fake.stub("/wiki/api/v2/attachments/a1", ConfluenceFixtures.attachmentJson("a1", "200"));
        fake.stub("/wiki/download/attachments/a1/diagram.png", "png-bytes");

        List<DownloadAttachmentResponse> received = new ArrayList<>();
        assertThatThrownBy(() -> stub.downloadAttachment(DownloadAttachmentRequest.newBuilder()
                .setId("a1").setMaxBytes(12344).build()).forEachRemaining(received::add))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(
                        e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION));
        assertThat(received).noneMatch(DownloadAttachmentResponse::hasData);
        assertThat(fake.requestsTo("/wiki/download/attachments/a1/diagram.png")).isEmpty();

        // At the declared size the download goes through (0 would mean no cap).
        assertThat(download(DownloadAttachmentRequest.newBuilder().setId("a1")
                .setMaxBytes(12345).build())).hasSize(2);
    }

    // ======================================================================
    // COMMENTS AND CUSTOM CONTENT
    // ======================================================================

    private static final String CUSTOM_JSON = """
            {"id":"cc1","type":"ac:app:thing","status":"current","title":"Thing",
             "spaceId":"100","pageId":"200","authorId":"acc-1",
             "createdAt":"2024-02-01T10:00:00.000Z",
             "version":{"number":4,"createdAt":"2024-02-02T10:00:00.000Z"},
             "body":{"raw":{"representation":"raw","value":"{\\"a\\":1}"}},
             "_links":{"webui":"/spaces/ENG/customcontent/cc1"}}
            """;

    private List<Comment> comments(ListCommentsRequest request) {
        List<Comment> out = new ArrayList<>();
        stub.listComments(request).forEachRemaining(r -> out.add(r.getComment()));
        return out;
    }

    @Test
    void listCommentsOfAPageWalksFooterThenInlineAndSetsKind() {
        fake.stub("/wiki/api/v2/pages/200/footer-comments", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.footerCommentJson("c1", "200")));
        fake.stub("/wiki/api/v2/pages/200/inline-comments", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.inlineCommentJson("c2", "200")));

        List<Comment> comments = comments(ListCommentsRequest.newBuilder()
                .setContainer(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE)
                        .setId("200"))
                .setBodyFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML)
                .addStatuses(ContentStatus.CONTENT_STATUS_CURRENT)
                .addStatuses(ContentStatus.CONTENT_STATUS_ARCHIVED)
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_CREATED_DATE)
                        .setDescending(true))
                .build());

        assertThat(comments).extracting(Comment::getId, Comment::getKind).containsExactly(
                org.assertj.core.groups.Tuple.tuple("c1", CommentKind.COMMENT_KIND_FOOTER),
                org.assertj.core.groups.Tuple.tuple("c2", CommentKind.COMMENT_KIND_INLINE));
        assertThat(comments.get(1).getInlineProperties().getInlineOriginalSelection())
                .isEqualTo("teh");
        java.util.Map<String, String> expected = java.util.Map.of("limit", "100",
                "body-format", "storage", "status", "current,archived", "sort", "-created-date");
        assertThat(fake.lastTo("/wiki/api/v2/pages/200/footer-comments").params())
                .containsExactlyInAnyOrderEntriesOf(expected);
        assertThat(fake.lastTo("/wiki/api/v2/pages/200/inline-comments").params())
                .containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    void listCommentsResolutionFilterSelectsTheInlineRoute() {
        fake.stub("/wiki/api/v2/blogposts/300/inline-comments", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.inlineCommentJson("c2", "300")));

        List<Comment> comments = comments(ListCommentsRequest.newBuilder()
                .setContainer(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_BLOG_POST)
                        .setId("300"))
                .addResolutionStatuses(InlineCommentResolutionStatus.INLINE_COMMENT_RESOLUTION_STATUS_OPEN)
                .addResolutionStatuses(InlineCommentResolutionStatus.INLINE_COMMENT_RESOLUTION_STATUS_REOPENED)
                .build());

        assertThat(comments).extracting(Comment::getKind)
                .containsExactly(CommentKind.COMMENT_KIND_INLINE);
        assertThat(fake.requestsTo("/wiki/api/v2/blogposts/300/footer-comments")).isEmpty();
        assertThat(fake.lastTo("/wiki/api/v2/blogposts/300/inline-comments").params())
                .containsEntry("resolution-status", "open,reopened");
        // Blog post comments have no archived status.
        assertInvalid(() -> comments(ListCommentsRequest.newBuilder()
                .setContainer(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_BLOG_POST)
                        .setId("300"))
                .addStatuses(ContentStatus.CONTENT_STATUS_ARCHIVED).build()));
    }

    @Test
    void listCommentsOfAnAttachmentAreFooterOnly() {
        fake.stub("/wiki/api/v2/attachments/a1/footer-comments", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.footerCommentJson("c5", "200")));

        assertThat(comments(ListCommentsRequest.newBuilder()
                .setContainer(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_ATTACHMENT)
                        .setId("a1")).build()))
                .extracting(Comment::getId, Comment::getKind)
                .containsExactly(org.assertj.core.groups.Tuple.tuple("c5",
                        CommentKind.COMMENT_KIND_FOOTER));
        assertInvalid(() -> comments(ListCommentsRequest.newBuilder()
                .setContainer(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_ATTACHMENT)
                        .setId("a1"))
                .setKind(CommentKind.COMMENT_KIND_INLINE).build()));
        assertInvalid(() -> comments(ListCommentsRequest.newBuilder()
                .setContainer(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_WHITEBOARD)
                        .setId("w1")).build()));
    }

    @Test
    void listCommentRepliesFallBackFromFooterToInline() {
        // c2 is an inline comment: the footer children route does not know it.
        fake.stub("/wiki/api/v2/inline-comments/c2/children", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.inlineCommentJson("c3", "200")));

        List<Comment> replies = comments(ListCommentsRequest.newBuilder()
                .setParentCommentId("c2")
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_MODIFIED_DATE))
                .build());

        assertThat(replies).extracting(Comment::getId, Comment::getKind).containsExactly(
                org.assertj.core.groups.Tuple.tuple("c3", CommentKind.COMMENT_KIND_INLINE));
        assertThat(fake.requestsTo("/wiki/api/v2/footer-comments/c2/children")).hasSize(1);
        assertThat(fake.lastTo("/wiki/api/v2/inline-comments/c2/children").params())
                .containsEntry("sort", "modified-date");
    }

    @Test
    void listCommentsOfTheWorkspaceWalksBothKinds() {
        fake.stub("/wiki/api/v2/footer-comments", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.footerCommentJson("c1", "200")));
        fake.stub("/wiki/api/v2/inline-comments", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.inlineCommentJson("c2", "200")));

        assertThat(comments(ListCommentsRequest.getDefaultInstance()))
                .extracting(Comment::getKind)
                .containsExactly(CommentKind.COMMENT_KIND_FOOTER, CommentKind.COMMENT_KIND_INLINE);
        assertThat(comments(ListCommentsRequest.newBuilder()
                .setKind(CommentKind.COMMENT_KIND_INLINE).build())).hasSize(1);
        // The workspace routes have no status filter.
        assertInvalid(() -> comments(ListCommentsRequest.newBuilder()
                .addStatuses(ContentStatus.CONTENT_STATUS_CURRENT).build()));
    }

    @Test
    void getCommentTriesFooterThenInline() {
        fake.stub("/wiki/api/v2/inline-comments/c2", ConfluenceFixtures.inlineCommentJson("c2", "200"));

        GetCommentResponse response = stub.getComment(GetCommentRequest.newBuilder().setId("c2")
                .setVersion(1)
                .setInclude(ContentIncludes.newBuilder().setLikes(true).setLabels(true))
                .build());

        assertThat(response.getComment().getKind()).isEqualTo(CommentKind.COMMENT_KIND_INLINE);
        assertThat(fake.requestsTo("/wiki/api/v2/footer-comments/c2")).hasSize(1);
        // include-labels is not offered on comments: ignored.
        assertThat(fake.lastTo("/wiki/api/v2/inline-comments/c2").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("body-format", "storage",
                        "version", "1", "include-likes", "true"));

        fake.stub("/wiki/api/v2/footer-comments/c1", ConfluenceFixtures.footerCommentJson("c1", "200"));
        assertThat(stub.getComment(GetCommentRequest.newBuilder().setId("c1")
                .setKind(CommentKind.COMMENT_KIND_FOOTER).build()).getComment().getKind())
                .isEqualTo(CommentKind.COMMENT_KIND_FOOTER);
    }

    @Test
    void listCustomContentForwardsTypeIdsSpacesSortAndBody() {
        fake.stub("/wiki/api/v2/custom-content", ConfluenceFixtures.listJson(null, "", CUSTOM_JSON));
        fake.stub("/wiki/api/v2/pages/200/custom-content",
                ConfluenceFixtures.listJson(null, "", CUSTOM_JSON));

        List<CustomContent> items = new ArrayList<>();
        stub.listCustomContent(ListCustomContentRequest.newBuilder().setType("ac:app:thing")
                .addIds("cc1").addSpaceIds("100")
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_TITLE))
                .setBodyFormat(BodyFormat.BODY_FORMAT_RAW).build())
                .forEachRemaining(r -> items.add(r.getCustomContent()));

        assertThat(items).singleElement().satisfies(c -> {
            assertThat(c.getType()).isEqualTo("ac:app:thing");
            assertThat(c.getBody().getRaw().getFormat()).isEqualTo(BodyFormat.BODY_FORMAT_RAW);
        });
        assertThat(fake.lastTo("/wiki/api/v2/custom-content").params()).containsExactlyInAnyOrderEntriesOf(
                java.util.Map.of("limit", "100", "type", "ac:app:thing", "id", "cc1",
                        "space-id", "100", "sort", "title", "body-format", "raw"));

        stub.listCustomContent(ListCustomContentRequest.newBuilder().setType("ac:app:thing")
                .setContainer(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE)
                        .setId("200")).build()).forEachRemaining(r -> { });
        assertThat(fake.lastTo("/wiki/api/v2/pages/200/custom-content").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100",
                        "type", "ac:app:thing"));
        assertInvalid(() -> stub.listCustomContent(ListCustomContentRequest.getDefaultInstance())
                .hasNext());
    }

    @Test
    void getCustomContentForwardsBodyVersionAndIncludes() {
        fake.stub("/wiki/api/v2/custom-content/cc1", CUSTOM_JSON);

        GetCustomContentResponse response = stub.getCustomContent(GetCustomContentRequest.newBuilder()
                .setId("cc1").setBodyFormat(BodyFormat.BODY_FORMAT_ANONYMOUS_EXPORT_XHTML)
                .setVersion(4).setInclude(ContentIncludes.newBuilder().setCollaborators(true))
                .build());

        assertThat(response.getCustomContent().getVersion().getNumber()).isEqualTo(4);
        assertThat(fake.lastTo("/wiki/api/v2/custom-content/cc1").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of(
                        "body-format", "anonymous_export_view", "version", "4",
                        "include-collaborators", "true"));
    }

    // ======================================================================
    // SYNC
    // ======================================================================

    /** Stubs one ENG space with one page (plus full sub-entities) and one blog post. */
    private void stubWorkspace() {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());

        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson("200", "100", "Design Doc", T2.toString())));
        fake.stub("/wiki/api/v2/pages/200/footer-comments",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.footerCommentJson("c1", "200")));
        fake.stub("/wiki/api/v2/pages/200/inline-comments", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/200/attachments",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.attachmentJson("a1", "200")));
        fake.stub("/wiki/api/v2/pages/200/labels", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/200/properties", ConfluenceFixtures.emptyListJson());

        fake.stub("/wiki/api/v2/blogposts",
                ConfluenceFixtures.blogPostListJson(null,
                        ConfluenceFixtures.blogPostJson("300", "100", "Release notes",
                                T1.toString())));
        for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                "properties")) {
            fake.stub("/wiki/api/v2/blogposts/300/" + sub, ConfluenceFixtures.emptyListJson());
        }
    }

    private List<SyncResponse> sync(SyncRequest request) {
        List<SyncResponse> events = new ArrayList<>();
        stub.sync(request).forEachRemaining(events::add);
        return events;
    }

    @Test
    void syncFullCrawlStreamsChangesSnapshotAndResumeCursor() {
        stubWorkspace();

        List<SyncResponse> events = sync(SyncRequest.getDefaultInstance());

        // Every change is a well-formed UPSERT envelope (the programmatic
        // validator holds on the streamed wire).
        List<ConfluenceChange> changes = events.stream()
                .filter(SyncResponse::hasChange).map(SyncResponse::getChange).toList();
        assertThat(changes).isNotEmpty();
        assertThat(changes).allSatisfy(change -> {
            assertThat(change.getChangeId()).isNotBlank();
            assertThat(change.getEntity().getEntityId()).isNotBlank();
            assertThat(VALIDATOR.validate(change).violations()).isEmpty();
        });
        assertThat(changes).extracting(c -> c.getEntity().getEntityCase())
                .contains(ConfluenceEntity.EntityCase.SPACE,
                        ConfluenceEntity.EntityCase.PAGE,
                        ConfluenceEntity.EntityCase.BLOG_POST,
                        ConfluenceEntity.EntityCase.ATTACHMENT,
                        ConfluenceEntity.EntityCase.COMMENT);

        // One snapshot for the ENG sweep.
        List<ConfluenceSnapshot> snapshots = events.stream()
                .filter(SyncResponse::hasSnapshot).map(SyncResponse::getSnapshot).toList();
        assertThat(snapshots).hasSize(1);
        assertThat(snapshots.get(0).getSpaceKey()).isEqualTo("ENG");

        // The terminal message carries the resume cursor: the newest
        // modification observed (the page at T2).
        assertThat(events.getLast().getEventCase()).isEqualTo(SyncResponse.EventCase.RESUME_CURSOR);
        assertThat(Instant.parse(events.getLast().getResumeCursor())).isEqualTo(T2);
    }

    @Test
    void syncWithoutBodiesStripsContentBodies() {
        stubWorkspace();

        List<SyncResponse> events = sync(SyncRequest.newBuilder().setIncludeBodies(false).build());

        ConfluenceEntity page = events.stream()
                .filter(SyncResponse::hasChange).map(SyncResponse::getChange)
                .map(ConfluenceChange::getEntity)
                .filter(e -> e.getEntityCase() == ConfluenceEntity.EntityCase.PAGE)
                .findFirst().orElseThrow();
        assertThat(page.getPage().hasBody()).isFalse();
        assertThat(page.getPage().getTitle()).isEqualTo("Design Doc");
    }

    @Test
    void syncIncrementalResumesFromTheCursor() {
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson("203", "100", "Newest", T3.toString()),
                        ConfluenceFixtures.pageJson("202", "100", "Middle", T2.toString())));
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());
        for (String sub : List.of("footer-comments", "inline-comments", "attachments", "labels",
                "properties")) {
            fake.stub("/wiki/api/v2/pages/203/" + sub, ConfluenceFixtures.emptyListJson());
        }

        List<SyncResponse> events = sync(SyncRequest.newBuilder()
                .setSinceCursor(T2.toString()).setIncludeBodies(true).build());

        List<ConfluenceChange> changes = events.stream()
                .filter(SyncResponse::hasChange).map(SyncResponse::getChange).toList();
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).getEntity().getEntityId()).isEqualTo("203");
        assertThat(changes.get(0).getEntity().getPage().hasBody()).isTrue();
        assertThat(events.getLast().getEventCase()).isEqualTo(SyncResponse.EventCase.RESUME_CURSOR);
        assertThat(Instant.parse(events.getLast().getResumeCursor())).isEqualTo(T3);
    }

    // ======================================================================
    // REFLECTION
    // ======================================================================

    @Test
    void reflectionServesTheServiceDescriptor() throws Exception {
        ServerReflectionGrpc.ServerReflectionStub reflection =
                ServerReflectionGrpc.newStub(channel);
        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<ServerReflectionResponse> response = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        StreamObserver<ServerReflectionRequest> requests = reflection.serverReflectionInfo(
                new StreamObserver<>() {
                    @Override
                    public void onNext(ServerReflectionResponse value) {
                        response.set(value);
                    }

                    @Override
                    public void onError(Throwable t) {
                        error.set(t);
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });
        requests.onNext(ServerReflectionRequest.newBuilder()
                .setFileContainingSymbol("ai.pipestream.confluence.v1.ConfluenceService")
                .build());
        requests.onCompleted();

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(error.get()).isNull();
        assertThat(response.get().hasFileDescriptorResponse()).isTrue();
        List<String> files = new ArrayList<>();
        for (com.google.protobuf.ByteString bytes
                : response.get().getFileDescriptorResponse().getFileDescriptorProtoList()) {
            files.add(DescriptorProtos.FileDescriptorProto.parseFrom(bytes).getName());
        }
        assertThat(files).contains(
                "ai/pipestream/confluence/v1/confluence_service.proto");
    }
}
