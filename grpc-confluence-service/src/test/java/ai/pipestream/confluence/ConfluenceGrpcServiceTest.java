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
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.reflection.v1.ServerReflectionGrpc;
import io.grpc.reflection.v1.ServerReflectionRequest;
import io.grpc.reflection.v1.ServerReflectionResponse;
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
                .addService(ProtoReflectionServiceV1.newInstance())
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

    // Covers the deprecated ListSpaces rpc, which the service still serves.
    @SuppressWarnings("deprecation")
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
    void aRepeatedNextLinkIsInternalNotASpin() {
        fake.stub("/wiki/api/v2/pages", ConfluenceFixtures.pageListJson("same",
                ConfluenceFixtures.pageJson("200", "100", "One", T2.toString())));
        fake.stub("/wiki/api/v2/pages?cursor=same", ConfluenceFixtures.pageListJson("same",
                ConfluenceFixtures.pageJson("201", "100", "Two", T2.toString())));

        assertThatThrownBy(() -> stub.listPages(ListPagesRequest.getDefaultInstance())
                .forEachRemaining(r -> { }))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL));
        assertThat(fake.requestsTo("/wiki/api/v2/pages")).hasSize(2);
    }

    @Test
    void streamsWaitForTheClientAndStopPagingWhenItCancels() throws Exception {
        String[] cursors = {null, "c1", "c2", "c3", "c4", "c5"};
        for (int i = 0; i < 5; i++) {
            fake.stub(i == 0 ? "/wiki/api/v2/pages" : "/wiki/api/v2/pages?cursor=" + cursors[i],
                    ConfluenceFixtures.pageListJson(i == 4 ? null : cursors[i + 1],
                            ConfluenceFixtures.pageJson("20" + i, "100", "P" + i, T2.toString())));
        }
        CountDownLatch first = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        AtomicReference<io.grpc.stub.ClientCallStreamObserver<ListPagesRequest>> call =
                new AtomicReference<>();
        ConfluenceServiceGrpc.newStub(channel).listPages(ListPagesRequest.getDefaultInstance(),
                new io.grpc.stub.ClientResponseObserver<ListPagesRequest, ListPagesResponse>() {
                    @Override
                    public void beforeStart(
                            io.grpc.stub.ClientCallStreamObserver<ListPagesRequest> requestStream) {
                        call.set(requestStream);
                        requestStream.disableAutoRequestWithInitial(1);
                    }

                    @Override
                    public void onNext(ListPagesResponse value) {
                        first.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                        closed.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        closed.countDown();
                    }
                });
        assertThat(first.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(300);
        // One message was asked for: the server read at most the page it is holding back.
        assertThat(fake.requestsTo("/wiki/api/v2/pages")).hasSizeLessThanOrEqualTo(2);
        call.get().cancel("done", null);
        assertThat(closed.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(300);
        assertThat(fake.requestsTo("/wiki/api/v2/pages")).hasSizeLessThanOrEqualTo(2);
    }

    @Test
    void idsThatWouldRewriteTheRouteAreInvalidArguments() {
        assertInvalid(() -> stub.getPage(GetPageRequest.newBuilder()
                .setId("200/../../../../rest/api/user/current").build()));
        assertInvalid(() -> stub.getPage(GetPageRequest.newBuilder()
                .setId("200?get-draft=true").build()));
        assertThat(fake.requests()).isEmpty();
    }

    // Covers the deprecated ListSpaces rpc, which the service still serves.
    @SuppressWarnings("deprecation")
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

    // Covers the deprecated ListSpaces rpc, which the service still serves.
    @SuppressWarnings("deprecation")
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
    void getAttachmentHasNoInlineCapByDefault() {
        long thirtyMiB = 30L * 1024 * 1024;
        fake.stub("/wiki/api/v2/attachments/a1", ConfluenceFixtures.attachmentJson("a1", "200")
                .replace("\"fileSize\": 12345", "\"fileSize\": " + thirtyMiB));
        fake.stub("/wiki/download/attachments/a1/diagram.png", "png-bytes");

        GetAttachmentResponse response = stub.getAttachment(GetAttachmentRequest.newBuilder()
                .setId("a1").setIncludeContent(true).build());

        assertThat(ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES).isZero();
        assertThat(response.getAttachment().getFileSize()).isEqualTo(thirtyMiB);
        assertThat(response.getAttachment().hasContent()).isTrue();
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
    // WHITEBOARDS, DATABASES, FOLDERS, SMART LINKS, CONTENT TREE, DATABASE DATA
    // ======================================================================

    private static String treeNodeJson(String id, String type, String title) {
        return """
                {"id":"%s","type":"%s","status":"current","title":"%s","parentId":"200",
                 "parentType":"page","position":3,"authorId":"acc-1","ownerId":"acc-2",
                 "createdAt":"2024-02-01T10:00:00.000Z","spaceId":"100",
                 "embedUrl":"https://example.com/x",
                 "version":{"number":2,"createdAt":"2024-02-02T10:00:00.000Z"},
                 "collaborators":[{"accountId":"acc-5"}],
                 "_links":{"webui":"/spaces/ENG/%ss/%s"}}
                """.formatted(id, type, title, type, id);
    }

    @Test
    void getWhiteboardDatabaseFolderAndSmartLinkSendTheTreeIncludes() {
        fake.stub("/wiki/api/v2/whiteboards/w1", treeNodeJson("w1", "whiteboard", "Board"));
        fake.stub("/wiki/api/v2/databases/d1", treeNodeJson("d1", "database", "Tracker"));
        fake.stub("/wiki/api/v2/folders/f1", treeNodeJson("f1", "folder", "Folder"));
        fake.stub("/wiki/api/v2/embeds/e1", treeNodeJson("e1", "embed", "Link"));
        ContentIncludes all = ContentIncludes.newBuilder().setLabels(true).setProperties(true)
                .setOperations(true).setLikes(true).setVersions(true).setDirectChildren(true)
                .setCollaborators(true).build();
        java.util.Map<String, String> expected = java.util.Map.of("include-collaborators", "true",
                "include-direct-children", "true", "include-operations", "true",
                "include-properties", "true");

        Whiteboard whiteboard = stub.getWhiteboard(GetWhiteboardRequest.newBuilder().setId("w1")
                .setInclude(all).build()).getWhiteboard();
        Database database = stub.getDatabase(GetDatabaseRequest.newBuilder().setId("d1")
                .setInclude(all).build()).getDatabase();
        Folder folder = stub.getFolder(GetFolderRequest.newBuilder().setId("f1")
                .setInclude(all).build()).getFolder();
        SmartLink link = stub.getSmartLink(GetSmartLinkRequest.newBuilder().setId("e1")
                .setInclude(all).build()).getSmartLink();

        assertThat(whiteboard.getTitle()).isEqualTo("Board");
        assertThat(whiteboard.getCollaboratorAccountIdsList()).containsExactly("acc-5");
        assertThat(database.getOwnerId()).isEqualTo("acc-2");
        assertThat(folder.getParentType()).isEqualTo(ParentContentType.PARENT_CONTENT_TYPE_PAGE);
        assertThat(link.getEmbedUrl()).isEqualTo("https://example.com/x");
        for (String path : List.of("whiteboards/w1", "databases/d1", "folders/f1", "embeds/e1")) {
            assertThat(fake.lastTo("/wiki/api/v2/" + path).params())
                    .as(path).containsExactlyInAnyOrderEntriesOf(expected);
        }
    }

    @Test
    void listChildrenRoutesByParentKind() {
        fake.stub("/wiki/api/v2/folders/f1/direct-children", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"200\",\"status\":\"current\",\"title\":\"Child\",\"type\":\"page\","
                        + "\"spaceId\":\"100\",\"childPosition\":7}"));
        fake.stub("/wiki/api/v2/custom-content/cc1/children", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"cc2\",\"status\":\"current\",\"title\":\"Sub\",\"type\":\"ac:x\","
                        + "\"spaceId\":\"100\"}"));

        List<ContentTreeEntry> children = new ArrayList<>();
        stub.listChildren(ListChildrenRequest.newBuilder()
                .setParent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_FOLDER).setId("f1"))
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_CHILD_POSITION)
                        .setDescending(true))
                .build()).forEachRemaining(r -> children.add(r.getEntry()));
        assertThat(children).singleElement().satisfies(e -> {
            assertThat(e.getType()).isEqualTo("page");
            assertThat(e.getChildPosition()).isEqualTo(7);
        });
        assertThat(fake.lastTo("/wiki/api/v2/folders/f1/direct-children").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100",
                        "sort", "-child-position"));

        List<ContentTreeEntry> custom = new ArrayList<>();
        stub.listChildren(ListChildrenRequest.newBuilder()
                .setParent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_CUSTOM_CONTENT)
                        .setId("cc1")).build()).forEachRemaining(r -> custom.add(r.getEntry()));
        assertThat(custom).extracting(ContentTreeEntry::getId).containsExactly("cc2");
        // Custom content children cannot be sorted by child position; blog posts have no tree.
        assertInvalid(() -> stub.listChildren(ListChildrenRequest.newBuilder()
                .setParent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_CUSTOM_CONTENT)
                        .setId("cc1"))
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_CHILD_POSITION))
                .build()).hasNext());
        assertInvalid(() -> stub.listChildren(ListChildrenRequest.newBuilder()
                .setParent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_BLOG_POST)
                        .setId("300")).build()).hasNext());
    }

    @Test
    void listDescendantsForwardsDepth() {
        fake.stub("/wiki/api/v2/pages/200/descendants", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"w1\",\"status\":\"current\",\"title\":\"Board\",\"type\":\"whiteboard\","
                        + "\"parentId\":\"200\",\"depth\":1,\"childPosition\":0}"));

        List<ContentTreeEntry> entries = new ArrayList<>();
        stub.listDescendants(ListDescendantsRequest.newBuilder()
                .setRoot(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE).setId("200"))
                .setDepth(5).build()).forEachRemaining(r -> entries.add(r.getEntry()));

        assertThat(entries).singleElement().satisfies(e -> {
            assertThat(e.getParentId()).isEqualTo("200");
            assertThat(e.getDepth()).isEqualTo(1);
        });
        assertThat(fake.lastTo("/wiki/api/v2/pages/200/descendants").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100", "depth", "5"));
    }

    @Test
    void listAncestorsWalksUpFromTheHighestAncestorWhenABatchIsFull() {
        // A full batch of 250 (top-down) means more ancestors above the first one.
        String[] full = new String[250];
        full[0] = "{\"id\":\"f9\",\"type\":\"folder\"}";
        for (int i = 1; i < full.length; i++) {
            full[i] = "{\"id\":\"p" + i + "\",\"type\":\"page\"}";
        }
        fake.stub("/wiki/api/v2/pages/200/ancestors", ConfluenceFixtures.listJson(null, "", full));
        fake.stub("/wiki/api/v2/folders/f9/ancestors", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"p0\",\"type\":\"page\"}"));

        List<Ancestor> ancestors = new ArrayList<>();
        stub.listAncestors(ListAncestorsRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE).setId("200"))
                .build()).forEachRemaining(r -> ancestors.add(r.getAncestor()));

        assertThat(ancestors).hasSize(251);
        assertThat(ancestors.get(0).getId()).isEqualTo("p0");
        assertThat(ancestors.get(1).getId()).isEqualTo("f9");
        assertThat(ancestors.get(1).getType()).isEqualTo(AncestorType.ANCESTOR_TYPE_FOLDER);
        assertThat(ancestors.getLast().getId()).isEqualTo("p249");
        assertThat(fake.lastTo("/wiki/api/v2/pages/200/ancestors").params())
                .containsExactlyEntriesOf(java.util.Map.of("limit", "250"));
    }

    @Test
    void listDatabaseFieldsMapsTypedSettings() {
        fake.stub("/wiki/api/v2/databases/d1/fields", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"f1\",\"name\":\"Due\",\"type\":\"date\",\"settings\":{\"includesTime\":true}}",
                "{\"id\":\"f2\",\"name\":\"Status\",\"type\":\"select\",\"settings\":{\"options\":"
                        + "[{\"id\":\"o1\",\"name\":\"Open\"}]}}"));

        List<DatabaseField> fields = new ArrayList<>();
        stub.listDatabaseFields(ListDatabaseFieldsRequest.newBuilder().setDatabaseId("d1").build())
                .forEachRemaining(r -> fields.add(r.getField()));

        assertThat(fields).extracting(DatabaseField::getType).containsExactly(
                DatabaseFieldType.DATABASE_FIELD_TYPE_DATE, DatabaseFieldType.DATABASE_FIELD_TYPE_SELECT);
        assertThat(fields.get(0).getDate().getIncludesTime()).isTrue();
        assertThat(fields.get(1).getSelect().getOptionsCount()).isEqualTo(1);
    }

    @Test
    void listDatabaseEntriesSendsTheBracketFilterGrammarAndSort() {
        fake.stub("/wiki/api/v2/databases/d1/entries", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"e1\",\"databaseId\":\"d1\",\"values\":{\"f3\":{\"type\":\"number\","
                        + "\"value\":\"42\"},\"f4\":{\"type\":\"checkbox\",\"value\":true}}}"));

        List<DatabaseEntry> entries = new ArrayList<>();
        stub.listDatabaseEntries(ListDatabaseEntriesRequest.newBuilder().setDatabaseId("d1")
                .addFilters(DatabaseEntryFilter.newBuilder().setFieldId("f1")
                        .setOperator(DatabaseEntryFilterOperator.DATABASE_ENTRY_FILTER_OPERATOR_EQUALS)
                        .addValues("in:literal"))
                .addFilters(DatabaseEntryFilter.newBuilder().setFieldId("f2")
                        .setOperator(DatabaseEntryFilterOperator.DATABASE_ENTRY_FILTER_OPERATOR_IN)
                        .addValues("o1").addValues("o2"))
                .addFilters(DatabaseEntryFilter.newBuilder().setFieldId("f3")
                        .setOperator(DatabaseEntryFilterOperator.DATABASE_ENTRY_FILTER_OPERATOR_GREATER_THAN_OR_EQUAL)
                        .addValues("10"))
                .addFilters(DatabaseEntryFilter.newBuilder().setFieldId("f3")
                        .setOperator(DatabaseEntryFilterOperator.DATABASE_ENTRY_FILTER_OPERATOR_LESS_THAN)
                        .addValues("100"))
                .addFilters(DatabaseEntryFilter.newBuilder().setFieldId("f5")
                        .setOperator(DatabaseEntryFilterOperator.DATABASE_ENTRY_FILTER_OPERATOR_EMPTY))
                .setSort(DatabaseEntrySort.newBuilder().setFieldId("f3").setDescending(true))
                .build()).forEachRemaining(r -> entries.add(r.getEntry()));

        assertThat(entries).singleElement().satisfies(e -> {
            assertThat(e.getValuesMap().get("f3").getNumber()).isEqualTo("42");
            assertThat(e.getValuesMap().get("f4").getCheckbox()).isTrue();
        });
        java.util.Map<String, String> expected = new java.util.HashMap<>(java.util.Map.of(
                "limit", "100", "filter[f1]", "eq:in:literal", "filter[f2]", "in:o1,o2",
                "filter[f3][gte]", "10", "filter[f3][lt]", "100", "filter[f5]", "$empty",
                "sort", "-f3"));
        assertThat(fake.lastTo("/wiki/api/v2/databases/d1/entries").params())
                .containsExactlyInAnyOrderEntriesOf(expected);

        assertInvalid(() -> stub.listDatabaseEntries(ListDatabaseEntriesRequest.newBuilder()
                .setDatabaseId("d1").addFilters(DatabaseEntryFilter.newBuilder().setFieldId("f2")
                        .setOperator(DatabaseEntryFilterOperator.DATABASE_ENTRY_FILTER_OPERATOR_IN)
                        .addValues("a,b")).build()).hasNext());
        assertInvalid(() -> stub.listDatabaseEntries(ListDatabaseEntriesRequest.newBuilder()
                .setDatabaseId("d1").addFilters(DatabaseEntryFilter.newBuilder().setFieldId("f2")
                        .addValues("a").addValues("b")).build()).hasNext());
    }

    @Test
    void getDatabaseEntryReadsOneRow() {
        fake.stub("/wiki/api/v2/databases/d1/entries/e1",
                "{\"id\":\"e1\",\"databaseId\":\"d1\",\"values\":{\"f3\":{\"type\":\"number\","
                        + "\"value\":\"7\"}}}");

        DatabaseEntry entry = stub.getDatabaseEntry(GetDatabaseEntryRequest.newBuilder()
                .setDatabaseId("d1").setEntryId("e1").build()).getEntry();

        assertThat(entry.getId()).isEqualTo("e1");
        assertThat(entry.getDatabaseId()).isEqualTo("d1");
        assertThat(entry.getValuesMap().get("f3").getNumber()).isEqualTo("7");
    }

    // ======================================================================
    // LABELS, VERSIONS, TASKS, USERS, CLASSIFICATION, DATA POLICIES, PROPERTIES
    // ======================================================================

    @Test
    void listLabelsRoutesEachScope() {
        String label = ConfluenceFixtures.labelJson("l1", "design");
        for (String path : List.of("/wiki/api/v2/labels", "/wiki/api/v2/spaces/100/labels",
                "/wiki/api/v2/spaces/100/content/labels", "/wiki/api/v2/attachments/a1/labels")) {
            fake.stub(path, ConfluenceFixtures.listJson(null, "", label));
        }
        SortOrder byName = SortOrder.newBuilder().setField(SortField.SORT_FIELD_NAME).build();

        List<Label> all = new ArrayList<>();
        stub.listLabels(ListLabelsRequest.newBuilder().addLabelIds("l1").addLabelIds("l2")
                .addPrefixes(LabelPrefix.LABEL_PREFIX_GLOBAL).addPrefixes(LabelPrefix.LABEL_PREFIX_TEAM)
                .setSort(byName).build()).forEachRemaining(r -> all.add(r.getLabel()));
        assertThat(all).extracting(Label::getName).containsExactly("design");
        assertThat(fake.lastTo("/wiki/api/v2/labels").params()).containsExactlyInAnyOrderEntriesOf(
                java.util.Map.of("limit", "100", "label-id", "l1,l2", "prefix", "global,team",
                        "sort", "name"));

        stub.listLabels(ListLabelsRequest.newBuilder().setSpaceId("100")
                .addPrefixes(LabelPrefix.LABEL_PREFIX_MY).build()).forEachRemaining(r -> { });
        assertThat(fake.lastTo("/wiki/api/v2/spaces/100/labels").params()).containsEntry("prefix", "my");
        stub.listLabels(ListLabelsRequest.newBuilder().setContentSpaceId("100").build())
                .forEachRemaining(r -> { });
        assertThat(fake.requestsTo("/wiki/api/v2/spaces/100/content/labels")).hasSize(1);
        stub.listLabels(ListLabelsRequest.newBuilder().setContent(ContentRef.newBuilder()
                .setType(ContentType.CONTENT_TYPE_ATTACHMENT).setId("a1"))
                .addPrefixes(LabelPrefix.LABEL_PREFIX_SYSTEM).build()).forEachRemaining(r -> { });
        assertThat(fake.lastTo("/wiki/api/v2/attachments/a1/labels").params())
                .containsEntry("prefix", "system");

        // Space label routes only know my/team; scoped routes take no label ids.
        assertInvalid(() -> stub.listLabels(ListLabelsRequest.newBuilder().setSpaceId("100")
                .addPrefixes(LabelPrefix.LABEL_PREFIX_GLOBAL).build()).hasNext());
        assertInvalid(() -> stub.listLabels(ListLabelsRequest.newBuilder().setSpaceId("100")
                .addLabelIds("l1").build()).hasNext());
    }

    @Test
    void listVersionsCarriesTheEntitySnapshot() {
        fake.stub("/wiki/api/v2/pages/200/versions", ConfluenceFixtures.listJson(null, "",
                "{\"createdAt\":\"2024-02-02T10:00:00.000Z\",\"message\":\"fix\",\"number\":2,"
                        + "\"minorEdit\":true,\"authorId\":\"acc-1\",\"page\":{\"id\":\"200\","
                        + "\"title\":\"Design Doc\",\"body\":{\"storage\":{\"representation\":"
                        + "\"storage\",\"value\":\"<p>v2</p>\"}}}}"));
        fake.stub("/wiki/api/v2/inline-comments/c2/versions", ConfluenceFixtures.listJson(null, "",
                "{\"number\":1,\"comment\":{\"id\":\"c2\",\"title\":\"\"}}"));

        List<Version> versions = new ArrayList<>();
        stub.listVersions(ListVersionsRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE).setId("200"))
                .setBodyFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML)
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_MODIFIED_DATE)
                        .setDescending(true))
                .build()).forEachRemaining(r -> versions.add(r.getVersion()));

        assertThat(versions).singleElement().satisfies(v -> {
            assertThat(v.getNumber()).isEqualTo(2);
            assertThat(v.getMinorEdit()).isTrue();
            assertThat(v.getEntity().getTitle()).isEqualTo("Design Doc");
            assertThat(v.getEntity().getBody().getStorage().getValue()).isEqualTo("<p>v2</p>");
        });
        assertThat(fake.lastTo("/wiki/api/v2/pages/200/versions").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100",
                        "body-format", "storage", "sort", "-modified-date"));

        List<Version> commentVersions = new ArrayList<>();
        stub.listVersions(ListVersionsRequest.newBuilder().setContent(ContentRef.newBuilder()
                .setType(ContentType.CONTENT_TYPE_INLINE_COMMENT).setId("c2")).build())
                .forEachRemaining(r -> commentVersions.add(r.getVersion()));
        assertThat(commentVersions.get(0).getEntity().getId()).isEqualTo("c2");
        // Attachment versions take no body format; versions sort by modified date only.
        assertInvalid(() -> stub.listVersions(ListVersionsRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_ATTACHMENT)
                        .setId("a1"))
                .setBodyFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML).build()).hasNext());
        assertInvalid(() -> stub.listVersions(ListVersionsRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE).setId("200"))
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_TITLE)).build())
                .hasNext());
    }

    @Test
    void listTasksForwardsEveryFilterWithEpochMillis() {
        String task = "{\"id\":\"t1\",\"localId\":\"x\",\"spaceId\":\"100\",\"pageId\":\"200\","
                + "\"status\":\"incomplete\",\"createdBy\":\"acc-1\",\"assignedTo\":\"acc-2\","
                + "\"dueAt\":\"2024-03-01T00:00:00.000Z\"}";
        fake.stub("/wiki/api/v2/tasks", ConfluenceFixtures.listJson(null, "", task));
        com.google.protobuf.Timestamp t1 = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(T1.getEpochSecond()).setNanos(5_000_000).build();
        com.google.protobuf.Timestamp t2 = com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(T2.getEpochSecond()).build();

        List<Task> tasks = new ArrayList<>();
        stub.listTasks(ListTasksRequest.newBuilder()
                .setBodyFormat(BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT).setExcludeBlankTasks(true)
                .setStatus(TaskStatus.TASK_STATUS_INCOMPLETE)
                .addTaskIds("t1").addSpaceIds("100").addPageIds("200").addBlogPostIds("300")
                .addCreatedBy("acc-1").addAssignedTo("acc-2").addCompletedBy("acc-3")
                .setCreatedAtFrom(t1).setCreatedAtTo(t2).setDueAtFrom(t1).setDueAtTo(t2)
                .setCompletedAtFrom(t1).setCompletedAtTo(t2)
                .build()).forEachRemaining(r -> tasks.add(r.getTask()));

        assertThat(tasks).singleElement().satisfies(t -> {
            assertThat(t.getStatus()).isEqualTo(TaskStatus.TASK_STATUS_INCOMPLETE);
            assertThat(t.getAssignedTo()).isEqualTo("acc-2");
        });
        String from = String.valueOf(T1.toEpochMilli() + 5);
        String to = String.valueOf(T2.toEpochMilli());
        java.util.Map<String, String> expected = new java.util.HashMap<>(java.util.Map.of(
                "limit", "100", "body-format", "atlas_doc_format", "include-blank-tasks", "false",
                "status", "incomplete", "task-id", "t1", "space-id", "100", "page-id", "200",
                "blogpost-id", "300", "created-by", "acc-1", "assigned-to", "acc-2"));
        expected.putAll(java.util.Map.of("completed-by", "acc-3", "created-at-from", from,
                "created-at-to", to, "due-at-from", from, "due-at-to", to,
                "completed-at-from", from, "completed-at-to", to));
        assertThat(fake.lastTo("/wiki/api/v2/tasks").params()).containsExactlyInAnyOrderEntriesOf(expected);
    }

    @Test
    void getTaskDefaultsToStorage() {
        fake.stub("/wiki/api/v2/tasks/t1", "{\"id\":\"t1\",\"status\":\"complete\",\"body\":"
                + "{\"storage\":{\"representation\":\"storage\",\"value\":\"<p>do</p>\"}}}");

        Task task = stub.getTask(GetTaskRequest.newBuilder().setId("t1").build()).getTask();

        assertThat(task.getStatus()).isEqualTo(TaskStatus.TASK_STATUS_COMPLETE);
        assertThat(task.getBody().getStorage().getValue()).isEqualTo("<p>do</p>");
        assertThat(fake.lastTo("/wiki/api/v2/tasks/t1").params())
                .containsExactlyEntriesOf(java.util.Map.of("body-format", "storage"));
    }

    @Test
    void batchGetUsersChunksToTheApiMaximum() throws Exception {
        fake.stub("/wiki/api/v2/users-bulk", "{\"results\":[{\"accountId\":\"acc-1\","
                + "\"accountType\":\"atlassian\",\"accountStatus\":\"active\","
                + "\"displayName\":\"Ada\",\"publicName\":\"ada\"}]}");
        BatchGetUsersRequest.Builder request = BatchGetUsersRequest.newBuilder();
        for (int i = 0; i < 300; i++) {
            request.addAccountIds("acc-" + i);
        }
        request.addAccountIds("acc-0"); // duplicates collapse

        BatchGetUsersResponse response = stub.batchGetUsers(request.build());

        assertThat(response.getUsersList()).hasSize(2);
        assertThat(response.getUsers(0).getAccountType()).isEqualTo(AccountType.ACCOUNT_TYPE_ATLASSIAN);
        List<FakeConfluenceServer.RecordedRequest> calls = fake.requestsTo("/wiki/api/v2/users-bulk");
        assertThat(calls).hasSize(2).allSatisfy(c -> assertThat(c.method()).isEqualTo("POST"));
        com.fasterxml.jackson.databind.ObjectMapper json = new com.fasterxml.jackson.databind.ObjectMapper();
        assertThat(json.readTree(calls.get(0).body()).path("accountIds")).hasSize(250);
        assertThat(json.readTree(calls.get(1).body()).path("accountIds")).hasSize(50);
        assertThat(json.readTree(calls.get(1).body()).path("accountIds").get(0).asText())
                .isEqualTo("acc-250");
    }

    @Test
    void classificationLevelsListAndResolvePerTarget() {
        String level = "{\"id\":\"cl1\",\"status\":\"PUBLISHED\",\"order\":1,\"name\":\"Internal\","
                + "\"description\":\"d\",\"guideline\":\"g\",\"color\":\"RED_BOLD\"}";
        fake.stub("/wiki/api/v2/classification-levels", "[" + level + "]");
        fake.stub("/wiki/api/v2/pages/200/classification-level", level);
        fake.stub("/wiki/api/v2/whiteboards/w1/classification-level", level);
        fake.stub("/wiki/api/v2/spaces/100/classification-level/default", level);

        ListClassificationLevelsResponse levels = stub.listClassificationLevels(
                ListClassificationLevelsRequest.getDefaultInstance());
        assertThat(levels.getLevelsList()).singleElement().satisfies(l -> {
            assertThat(l.getStatus()).isEqualTo(ClassificationLevelStatus.CLASSIFICATION_LEVEL_STATUS_PUBLISHED);
            assertThat(l.getColor()).isEqualTo(ClassificationLevelColor.CLASSIFICATION_LEVEL_COLOR_RED_BOLD);
        });

        stub.getClassificationLevel(GetClassificationLevelRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE).setId("200"))
                .setStatus(ContentStatus.CONTENT_STATUS_DRAFT).build());
        assertThat(fake.lastTo("/wiki/api/v2/pages/200/classification-level").params())
                .containsExactlyEntriesOf(java.util.Map.of("status", "draft"));
        assertThat(stub.getClassificationLevel(GetClassificationLevelRequest.newBuilder()
                .setSpaceId("100").build()).getLevel().getName()).isEqualTo("Internal");
        stub.getClassificationLevel(GetClassificationLevelRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_WHITEBOARD)
                        .setId("w1")).build());
        // Whiteboard classification has no status parameter; folders have no classification.
        assertInvalid(() -> stub.getClassificationLevel(GetClassificationLevelRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_WHITEBOARD)
                        .setId("w1"))
                .setStatus(ContentStatus.CONTENT_STATUS_ARCHIVED).build()));
        assertInvalid(() -> stub.getClassificationLevel(GetClassificationLevelRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_FOLDER)
                        .setId("f1")).build()));
    }

    @Test
    void dataPolicyRpcsMapTheirRoutes() {
        fake.stub("/wiki/api/v2/data-policies/spaces", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"100\",\"key\":\"ENG\",\"name\":\"Engineering\","
                        + "\"dataPolicy\":{\"anyContentBlocked\":true}}"));
        fake.stub("/wiki/api/v2/data-policies/metadata", "{\"anyContentBlocked\":true}");

        List<DataPolicy> policies = new ArrayList<>();
        stub.listDataPolicySpaces(ListDataPolicySpacesRequest.newBuilder().addIds("100")
                .addKeys("ENG").setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_KEY)
                        .setDescending(true)).build())
                .forEachRemaining(r -> policies.add(r.getDataPolicy()));

        assertThat(policies).singleElement().satisfies(p -> {
            assertThat(p.getKey()).isEqualTo("ENG");
            assertThat(p.getAnyContentBlocked()).isTrue();
        });
        assertThat(fake.lastTo("/wiki/api/v2/data-policies/spaces").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100", "ids", "100",
                        "keys", "ENG", "sort", "-key"));
        assertThat(stub.getDataPolicyMetadata(GetDataPolicyMetadataRequest.getDefaultInstance())
                .getDataPolicyMetadata().getAnyContentBlocked()).isTrue();
    }

    @Test
    void contentAndSpacePropertiesForwardKeyAndSort() {
        String property = ConfluenceFixtures.propertyJson("p1", "editor", "\"v2\"");
        fake.stub("/wiki/api/v2/comments/c1/properties", ConfluenceFixtures.listJson(null, "", property));
        fake.stub("/wiki/api/v2/embeds/e1/properties", ConfluenceFixtures.listJson(null, "", property));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.spacePropertyJson("sp1", "theme", "{\"a\":1}")));

        List<ContentProperty> properties = new ArrayList<>();
        stub.listContentProperties(ListContentPropertiesRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_FOOTER_COMMENT)
                        .setId("c1"))
                .setKey("editor").setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_KEY)
                        .setDescending(true)).build())
                .forEachRemaining(r -> properties.add(r.getProperty()));
        assertThat(properties).singleElement().satisfies(p -> {
            assertThat(p.getKey()).isEqualTo(PropertyKey.PROPERTY_KEY_EDITOR);
            assertThat(p.getValue().getStringValue()).isEqualTo("v2");
        });
        assertThat(fake.lastTo("/wiki/api/v2/comments/c1/properties").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100", "key", "editor",
                        "sort", "-key"));
        stub.listContentProperties(ListContentPropertiesRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_SMART_LINK)
                        .setId("e1")).build()).forEachRemaining(r -> { });
        assertThat(fake.requestsTo("/wiki/api/v2/embeds/e1/properties")).hasSize(1);

        List<SpaceProperty> spaceProperties = new ArrayList<>();
        stub.listSpaceProperties(ListSpacePropertiesRequest.newBuilder().setSpaceId("100")
                .setKey("theme").build()).forEachRemaining(r -> spaceProperties.add(r.getProperty()));
        assertThat(spaceProperties.get(0).getCustomKey()).isEqualTo("theme");
        assertThat(fake.lastTo("/wiki/api/v2/spaces/100/properties").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100", "key", "theme"));
    }

    // ======================================================================
    // PERMISSIONS, ROLES, CONTENT TYPES
    // ======================================================================

    @Test
    void getRestrictionStateServesThePermissionTargets() {
        fake.stub("/wiki/api/v2/folders/f1/restriction-state",
                "{\"state\":\"VIEW_RESTRICTED\",\"createdAt\":\"2024-02-01T10:00:00.000Z\","
                        + "\"createdByAaid\":\"acc-1\"}");

        RestrictionState state = stub.getRestrictionState(GetRestrictionStateRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_FOLDER).setId("f1"))
                .build()).getRestrictionState();

        assertThat(state.getState()).isEqualTo(ContentRestrictionState.CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED);
        assertThat(state.getCreatedBy()).isEqualTo("acc-1");
        assertInvalid(() -> stub.getRestrictionState(GetRestrictionStateRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_ATTACHMENT)
                        .setId("a1")).build()));
    }

    @Test
    void listContentPermissionsForwardsOperationAndPrincipal() {
        fake.stub("/wiki/api/v2/blogposts/300/permissions", ConfluenceFixtures.listJson(null, "",
                "{\"operation\":\"read\",\"principal\":{\"principalType\":\"group\","
                        + "\"principalId\":\"g1\"},\"createdAt\":\"2024-02-01T10:00:00.000Z\","
                        + "\"createdBy\":\"acc-1\"}"));

        List<ContentPermission> permissions = new ArrayList<>();
        stub.listContentPermissions(ListContentPermissionsRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_BLOG_POST)
                        .setId("300"))
                .setOperation(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_READ)
                .setPrincipalType(ContentPermissionPrincipalType.CONTENT_PERMISSION_PRINCIPAL_TYPE_GROUP)
                .setPrincipalId("g1").build()).forEachRemaining(r -> permissions.add(r.getPermission()));

        assertThat(permissions).singleElement().satisfies(p -> {
            assertThat(p.getOperation()).isEqualTo(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_READ);
            assertThat(p.getPrincipal().getPrincipalType())
                    .isEqualTo(ContentPermissionPrincipalType.CONTENT_PERMISSION_PRINCIPAL_TYPE_GROUP);
            assertThat(p.getPrincipal().getPrincipalId()).isEqualTo("g1");
        });
        assertThat(fake.lastTo("/wiki/api/v2/blogposts/300/permissions").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100",
                        "operation", "read", "principal-type", "group", "principal-id", "g1"));
        assertInvalid(() -> stub.listContentPermissions(ListContentPermissionsRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE).setId("200"))
                .setPrincipalId("g1").build()).hasNext());
    }

    @Test
    void spacePermissionAssignmentsRolesAndRoleAssignments() {
        fake.stub("/wiki/api/v2/spaces/100/permissions", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"sp-1\",\"principal\":{\"type\":\"role\",\"id\":\"r1\"},"
                        + "\"operation\":{\"key\":\"administer\",\"targetType\":\"space\"}}"));
        fake.stub("/wiki/api/v2/space-roles", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"r1\",\"type\":\"SYSTEM\",\"name\":\"Admin\",\"description\":\"d\","
                        + "\"spacePermissions\":[\"sp-1\"]}"));
        fake.stub("/wiki/api/v2/spaces/100/role-assignments", ConfluenceFixtures.listJson(null, "",
                "{\"principal\":{\"principalType\":\"ACCESS_CLASS\",\"principalId\":"
                        + "\"authenticated-users\"},\"roleId\":\"r1\"}"));
        fake.stub("/wiki/api/v2/space-role-mode", "{\"mode\":\"ROLES_TRANSITION\"}");
        Principal user = Principal.newBuilder().setPrincipalType(PrincipalType.PRINCIPAL_TYPE_USER)
                .setPrincipalId("acc-1").build();

        List<SpacePermissionAssignment> assignments = new ArrayList<>();
        stub.listSpacePermissionAssignments(ListSpacePermissionAssignmentsRequest.newBuilder()
                .setSpaceId("100").build()).forEachRemaining(r -> assignments.add(r.getAssignment()));
        assertThat(assignments).singleElement().satisfies(a -> {
            assertThat(a.getPrincipal().getType())
                    .isEqualTo(SpacePermissionPrincipalType.SPACE_PERMISSION_PRINCIPAL_TYPE_ROLE);
            assertThat(a.getOperation().getKey())
                    .isEqualTo(SpacePermissionOperationKey.SPACE_PERMISSION_OPERATION_KEY_ADMINISTER);
        });

        List<SpaceRole> roles = new ArrayList<>();
        stub.listSpaceRoles(ListSpaceRolesRequest.newBuilder().setSpaceId("100")
                .setRoleType(RoleType.ROLE_TYPE_SYSTEM).setPrincipal(user).build())
                .forEachRemaining(r -> roles.add(r.getRole()));
        assertThat(roles).singleElement().satisfies(r -> {
            assertThat(r.getType()).isEqualTo(RoleType.ROLE_TYPE_SYSTEM);
            assertThat(r.getSpacePermissionsList()).containsExactly("sp-1");
        });
        assertThat(fake.lastTo("/wiki/api/v2/space-roles").params()).containsExactlyInAnyOrderEntriesOf(
                java.util.Map.of("limit", "100", "space-id", "100", "role-type", "SYSTEM",
                        "principal-type", "USER", "principal-id", "acc-1"));

        List<SpaceRoleAssignment> roleAssignments = new ArrayList<>();
        stub.listSpaceRoleAssignments(ListSpaceRoleAssignmentsRequest.newBuilder().setSpaceId("100")
                .setRoleId("r1").setRoleType(RoleType.ROLE_TYPE_CUSTOM).build())
                .forEachRemaining(r -> roleAssignments.add(r.getAssignment()));
        assertThat(roleAssignments).singleElement().satisfies(a -> {
            assertThat(a.getRoleId()).isEqualTo("r1");
            assertThat(a.getPrincipal().getPrincipalType()).isEqualTo(PrincipalType.PRINCIPAL_TYPE_ACCESS_CLASS);
        });
        assertThat(fake.lastTo("/wiki/api/v2/spaces/100/role-assignments").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100", "role-id", "r1",
                        "role-type", "CUSTOM"));
        assertInvalid(() -> stub.listSpaceRoleAssignments(ListSpaceRoleAssignmentsRequest.newBuilder()
                .setSpaceId("100").setPrincipal(Principal.newBuilder().setPrincipalId("x")).build())
                .hasNext());

        assertThat(stub.getSpaceRoleMode(GetSpaceRoleModeRequest.getDefaultInstance()).getMode())
                .isEqualTo(SpaceRoleMode.SPACE_ROLE_MODE_ROLES_TRANSITION);
    }

    @Test
    void resolveContentTypesChunksAndTypesTheResults() throws Exception {
        fake.stub("/wiki/api/v2/content/convert-ids-to-types", "{\"results\":{\"1\":\"page\","
                + "\"2\":\"inline-comment\",\"3\":\"ac:my-app:thing\"}}");
        ResolveContentTypesRequest.Builder request = ResolveContentTypesRequest.newBuilder();
        for (int i = 1; i <= 150; i++) {
            request.addIds(String.valueOf(i));
        }

        ResolveContentTypesResponse response = stub.resolveContentTypes(request.build());

        List<FakeConfluenceServer.RecordedRequest> calls =
                fake.requestsTo("/wiki/api/v2/content/convert-ids-to-types");
        assertThat(calls).hasSize(2);
        com.fasterxml.jackson.databind.ObjectMapper json = new com.fasterxml.jackson.databind.ObjectMapper();
        assertThat(json.readTree(calls.get(0).body()).path("contentIds")).hasSize(100);
        assertThat(json.readTree(calls.get(1).body()).path("contentIds")).hasSize(50);
        assertThat(response.getResultsList().subList(0, 3)).extracting(
                ResolvedContentType::getId, ResolvedContentType::getType, ResolvedContentType::getCustomType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("1", ContentType.CONTENT_TYPE_PAGE, ""),
                        org.assertj.core.groups.Tuple.tuple("2", ContentType.CONTENT_TYPE_INLINE_COMMENT, ""),
                        org.assertj.core.groups.Tuple.tuple("3", ContentType.CONTENT_TYPE_UNSPECIFIED,
                                "ac:my-app:thing"));
    }

    // ======================================================================
    // ROUTE COVERAGE: space custom content, attachment comment versions,
    // per-prefix label fan-out, thumbnails, database views, users-bulk links
    // ======================================================================

    @Test
    void listCustomContentOfASpaceRoutesThroughTheSpace() {
        fake.stub("/wiki/api/v2/spaces/100/custom-content",
                ConfluenceFixtures.listJson(null, "", CUSTOM_JSON));

        List<CustomContent> items = new ArrayList<>();
        stub.listCustomContent(ListCustomContentRequest.newBuilder().setType("ac:app:thing")
                .setSpaceId("100").setBodyFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML).build())
                .forEachRemaining(r -> items.add(r.getCustomContent()));

        assertThat(items).extracting(CustomContent::getId).containsExactly("cc1");
        assertThat(fake.lastTo("/wiki/api/v2/spaces/100/custom-content").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100",
                        "type", "ac:app:thing", "body-format", "storage"));
        assertThat(fake.requestsTo("/wiki/api/v2/custom-content")).isEmpty();

        // The space route takes no sort and no workspace filters, and the
        // space and content containers are mutually exclusive.
        assertInvalid(() -> stub.listCustomContent(ListCustomContentRequest.newBuilder()
                .setType("ac:app:thing").setSpaceId("100")
                .setSort(SortOrder.newBuilder().setField(SortField.SORT_FIELD_TITLE)).build()).hasNext());
        assertInvalid(() -> stub.listCustomContent(ListCustomContentRequest.newBuilder()
                .setType("ac:app:thing").setSpaceId("100").addSpaceIds("101").build()).hasNext());
        assertInvalid(() -> stub.listCustomContent(ListCustomContentRequest.newBuilder()
                .setType("ac:app:thing").setSpaceId("100")
                .setContainer(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE)
                        .setId("200")).build()).hasNext());
        assertThat(fake.requestsTo("/wiki/api/v2/spaces/100/custom-content")).hasSize(1);
    }

    @Test
    void listCommentsOfAnAttachmentForwardsTheVersion() {
        fake.stub("/wiki/api/v2/attachments/a1/footer-comments", ConfluenceFixtures.listJson(null, "",
                ConfluenceFixtures.footerCommentJson("c5", "200")));

        assertThat(comments(ListCommentsRequest.newBuilder()
                .setContainer(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_ATTACHMENT)
                        .setId("a1"))
                .setVersion(3).build())).extracting(Comment::getId).containsExactly("c5");
        assertThat(fake.lastTo("/wiki/api/v2/attachments/a1/footer-comments").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("limit", "100", "version", "3"));

        // Only the attachment route takes a version.
        assertInvalid(() -> comments(ListCommentsRequest.newBuilder()
                .setContainer(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE)
                        .setId("200"))
                .setVersion(3).build()));
        assertInvalid(() -> comments(ListCommentsRequest.newBuilder().setVersion(3).build()));
        assertInvalid(() -> comments(ListCommentsRequest.newBuilder().setParentCommentId("c1")
                .setVersion(3).build()));
    }

    @Test
    void listLabelsFansOutOnePrefixPerCallOnScopedRoutes() {
        fake.stub("/wiki/api/v2/spaces/100/labels?limit=100&prefix=team",
                ConfluenceFixtures.listJson(null, "", ConfluenceFixtures.labelJson("l2", "roadmap")));
        fake.stub("/wiki/api/v2/spaces/100/labels?limit=100&prefix=my",
                ConfluenceFixtures.listJson(null, "", ConfluenceFixtures.labelJson("l1", "mine")));

        List<Label> labels = new ArrayList<>();
        stub.listLabels(ListLabelsRequest.newBuilder().setSpaceId("100")
                .addPrefixes(LabelPrefix.LABEL_PREFIX_TEAM).addPrefixes(LabelPrefix.LABEL_PREFIX_MY)
                .addPrefixes(LabelPrefix.LABEL_PREFIX_TEAM) // duplicates collapse
                .build()).forEachRemaining(r -> labels.add(r.getLabel()));

        assertThat(labels).extracting(Label::getName).containsExactly("roadmap", "mine");
        assertThat(fake.requestsTo("/wiki/api/v2/spaces/100/labels"))
                .extracting(r -> r.params().get("prefix")).containsExactly("team", "my");
    }

    @Test
    void downloadAttachmentThumbnailFollowsTheRedirectAndStreamsChunks() {
        fake.stub("/wiki/api/v2/attachments/a1/thumbnail/download", new FakeConfluenceServer.Stub(302, "",
                java.util.Map.of("Location", "/media/thumbs/a1.png")));
        fake.stub("/media/thumbs/a1.png", "thumb-bytes");

        List<DownloadAttachmentThumbnailResponse> parts = new ArrayList<>();
        stub.downloadAttachmentThumbnail(DownloadAttachmentThumbnailRequest.newBuilder()
                .setId("a1").setVersion(2).setWidth(64).setHeight(48).setChunkBytes(5).build())
                .forEachRemaining(parts::add);

        assertThat(parts.get(0).getPartCase())
                .isEqualTo(DownloadAttachmentThumbnailResponse.PartCase.THUMBNAIL);
        assertThat(parts.get(0).getThumbnail().getAttachmentId()).isEqualTo("a1");
        assertThat(parts.get(0).getThumbnail().getSizeBytes()).isEqualTo(11);
        assertThat(parts.get(0).getThumbnail().getMediaType()).isNotEmpty();
        assertThat(parts.subList(1, parts.size()))
                .extracting(p -> p.getData().toString(StandardCharsets.UTF_8))
                .containsExactly("thumb", "-byte", "s");
        assertThat(fake.lastTo("/wiki/api/v2/attachments/a1/thumbnail/download").params())
                .containsExactlyInAnyOrderEntriesOf(java.util.Map.of("version", "2", "width", "64",
                        "height", "48"));

        // Without a chunk size the configured default applies: one chunk here.
        parts.clear();
        stub.downloadAttachmentThumbnail(DownloadAttachmentThumbnailRequest.newBuilder().setId("a1")
                .build()).forEachRemaining(parts::add);
        assertThat(parts).hasSize(2);
        assertThat(parts.get(1).getData().toString(StandardCharsets.UTF_8)).isEqualTo("thumb-bytes");
        assertThat(fake.lastTo("/wiki/api/v2/attachments/a1/thumbnail/download").params()).isEmpty();
    }

    @Test
    void downloadAttachmentThumbnailAboveMaxBytesFailsBeforeAnyMessage() {
        fake.stub("/wiki/api/v2/attachments/a1/thumbnail/download", new FakeConfluenceServer.Stub(302, "",
                java.util.Map.of("Location", "/media/thumbs/a1.png")));
        fake.stub("/media/thumbs/a1.png", "thumb-bytes");

        List<DownloadAttachmentThumbnailResponse> received = new ArrayList<>();
        assertThatThrownBy(() -> stub.downloadAttachmentThumbnail(DownloadAttachmentThumbnailRequest
                .newBuilder().setId("a1").setMaxBytes(10).build()).forEachRemaining(received::add))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(
                        e.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION));
        assertThat(received).isEmpty();

        // At the declared size it goes through; negative sizes are rejected.
        List<DownloadAttachmentThumbnailResponse> parts = new ArrayList<>();
        stub.downloadAttachmentThumbnail(DownloadAttachmentThumbnailRequest.newBuilder().setId("a1")
                .setMaxBytes(11).build()).forEachRemaining(parts::add);
        assertThat(parts).hasSize(2);
        assertInvalid(() -> stub.downloadAttachmentThumbnail(DownloadAttachmentThumbnailRequest
                .newBuilder().setId("a1").setWidth(-1).build()).hasNext());
    }

    @Test
    void downloadAttachmentThumbnailOfAnUnknownAttachmentIsNotFound() {
        assertThatThrownBy(() -> stub.downloadAttachmentThumbnail(DownloadAttachmentThumbnailRequest
                .newBuilder().setId("missing").build()).hasNext())
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(
                        e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
    }

    @Test
    void listDatabaseViewsStreamsTypedViews() {
        fake.stub("/wiki/api/v2/databases/d1/views", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"v1\",\"name\":\"All rows\",\"type\":\"table\"}",
                "{\"id\":\"v2\",\"name\":\"Board\",\"type\":\"board\"}"));

        List<DatabaseView> views = new ArrayList<>();
        stub.listDatabaseViews(ListDatabaseViewsRequest.newBuilder().setDatabaseId("d1").build())
                .forEachRemaining(r -> views.add(r.getView()));

        assertThat(views).extracting(DatabaseView::getId, DatabaseView::getName, DatabaseView::getType)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple("v1", "All rows",
                                DatabaseViewType.DATABASE_VIEW_TYPE_TABLE),
                        org.assertj.core.groups.Tuple.tuple("v2", "Board",
                                DatabaseViewType.DATABASE_VIEW_TYPE_BOARD));
        assertThat(fake.lastTo("/wiki/api/v2/databases/d1/views").params())
                .containsExactlyEntriesOf(java.util.Map.of("limit", "100"));
        assertInvalid(() -> stub.listDatabaseViews(ListDatabaseViewsRequest.getDefaultInstance())
                .hasNext());
    }

    @Test
    void batchGetUsersRefusesAnUndefinedNextLink() {
        fake.stub("/wiki/api/v2/users-bulk", "{\"results\":[{\"accountId\":\"acc-1\",\"accountType\":\"atlassian\",\"accountStatus\":\"active\",\"displayName\":\"Ada\",\"publicName\":\"ada\"}],"
                + "\"_links\":{\"base\":\"https://x.atlassian.net/wiki\","
                + "\"next\":\"/wiki/api/v2/users-bulk?cursor=abc\"}}");

        assertThatThrownBy(() -> stub.batchGetUsers(BatchGetUsersRequest.newBuilder()
                .addAccountIds("acc-1").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(
                        e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL));

        // `_links.base` alone (the documented shape) is fine.
        fake.stub("/wiki/api/v2/users-bulk", "{\"results\":[{\"accountId\":\"acc-1\",\"accountType\":\"atlassian\",\"accountStatus\":\"active\",\"displayName\":\"Ada\",\"publicName\":\"ada\"}],"
                + "\"_links\":{\"base\":\"https://x.atlassian.net/wiki\"}}");
        assertThat(stub.batchGetUsers(BatchGetUsersRequest.newBuilder().addAccountIds("acc-1")
                .build()).getUsersList()).extracting(User::getAccountId).containsExactly("acc-1");
    }

    // ======================================================================
    // FULL READ SURFACE: version details, likes, operations, properties by
    // id, app properties, space permission catalogue and transitions, admin
    // key, email access check
    // ======================================================================

    private static ContentRef ref(ContentType type, String id) {
        return ContentRef.newBuilder().setType(type).setId(id).build();
    }

    @Test
    void getVersionReadsTheDetailedVersionPerContentKind() {
        String detail = "{\"number\":3,\"authorId\":\"acc-1\",\"message\":\"m\","
                + "\"createdAt\":\"2024-03-01T00:00:00.000Z\",\"minorEdit\":true,"
                + "\"contentTypeModified\":false,\"collaborators\":[\"acc-1\",\"acc-2\"],"
                + "\"prevVersion\":2,\"nextVersion\":4}";
        fake.stub("/wiki/api/v2/pages/200/versions/3", detail);
        fake.stub("/wiki/api/v2/inline-comments/c2/versions/3", detail);

        DetailedVersion version = stub.getVersion(GetVersionRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_PAGE, "200")).setNumber(3).build()).getVersion();
        assertThat(version.getNumber()).isEqualTo(3);
        assertThat(version.getAuthorId()).isEqualTo("acc-1");
        assertThat(version.getMinorEdit()).isTrue();
        assertThat(version.getCollaboratorsList()).containsExactly("acc-1", "acc-2");
        assertThat(version.getPrevVersion()).isEqualTo(2);
        assertThat(version.getNextVersion()).isEqualTo(4);
        assertThat(version.getCreatedAt().getSeconds()).isEqualTo(T1.getEpochSecond());

        assertThat(stub.getVersion(GetVersionRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_INLINE_COMMENT, "c2")).setNumber(3).build())
                .getVersion().getNumber()).isEqualTo(3);
        assertInvalid(() -> stub.getVersion(GetVersionRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_WHITEBOARD, "w1")).setNumber(3).build()));
        assertInvalid(() -> stub.getVersion(GetVersionRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_PAGE, "200")).build()));
    }

    @Test
    void listLikesStreamsAcrossPagesAndGetLikeCountReadsTheCount() {
        fake.stub("/wiki/api/v2/footer-comments/c1/likes/users", ConfluenceFixtures.listJson("n2",
                "/wiki/api/v2/footer-comments/c1/likes/users", "{\"accountId\":\"acc-1\"}"));
        fake.stub("/wiki/api/v2/footer-comments/c1/likes/users?cursor=n2",
                ConfluenceFixtures.listJson(null, "", "{\"accountId\":\"acc-2\"}"));
        fake.stub("/wiki/api/v2/blogposts/300/likes/count", "{\"count\":42}");

        List<Like> likes = new ArrayList<>();
        stub.listLikes(ListLikesRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_FOOTER_COMMENT, "c1")).build())
                .forEachRemaining(r -> likes.add(r.getLike()));
        assertThat(likes).extracting(Like::getAccountId).containsExactly("acc-1", "acc-2");
        assertThat(fake.requestsTo("/wiki/api/v2/footer-comments/c1/likes/users").getFirst().params())
                .containsExactlyEntriesOf(java.util.Map.of("limit", "100"));

        assertThat(stub.getLikeCount(GetLikeCountRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_BLOG_POST, "300")).build()).getCount())
                .isEqualTo(42);
        assertInvalid(() -> stub.getLikeCount(GetLikeCountRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_ATTACHMENT, "a1")).build()));
        assertInvalid(() -> stub.listLikes(ListLikesRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_FOLDER, "f1")).build()).hasNext());
    }

    @Test
    void listOperationsReadsTheFullListForContentAndSpaces() {
        String ops = "{\"operations\":[{\"operation\":\"read\",\"targetType\":\"page\"},"
                + "{\"operation\":\"update\",\"targetType\":\"page\"}]}";
        fake.stub("/wiki/api/v2/embeds/e1/operations", ops);
        fake.stub("/wiki/api/v2/spaces/100/operations",
                "{\"operations\":[{\"operation\":\"administer\",\"targetType\":\"space\"}]}");

        assertThat(stub.listOperations(ListOperationsRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_SMART_LINK, "e1")).build()).getOperationsList())
                .extracting(Operation::getOperation, Operation::getTargetType).containsExactly(
                        org.assertj.core.groups.Tuple.tuple("read", "page"),
                        org.assertj.core.groups.Tuple.tuple("update", "page"));
        assertThat(stub.listOperations(ListOperationsRequest.newBuilder().setSpaceId("100").build())
                .getOperationsList()).extracting(Operation::getOperation).containsExactly("administer");
        assertInvalid(() -> stub.listOperations(ListOperationsRequest.getDefaultInstance()));
    }

    @Test
    void getContentAndSpacePropertyById() {
        fake.stub("/wiki/api/v2/comments/c1/properties/p1",
                ConfluenceFixtures.propertyJson("p1", "editor", "\"v2\""));
        fake.stub("/wiki/api/v2/databases/d1/properties/p2",
                ConfluenceFixtures.propertyJson("p2", "my-key", "7"));
        fake.stub("/wiki/api/v2/spaces/100/properties/sp1",
                ConfluenceFixtures.spacePropertyJson("sp1", "theme", "{\"a\":1}"));

        ContentProperty comment = stub.getContentProperty(GetContentPropertyRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_INLINE_COMMENT, "c1")).setPropertyId("p1")
                .build()).getProperty();
        assertThat(comment.getId()).isEqualTo("p1");
        assertThat(comment.getKey()).isEqualTo(PropertyKey.PROPERTY_KEY_EDITOR);
        ContentProperty database = stub.getContentProperty(GetContentPropertyRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_DATABASE, "d1")).setPropertyId("p2")
                .build()).getProperty();
        assertThat(database.getCustomKey()).isEqualTo("my-key");
        assertThat(database.getValue().getIntegerValue()).isEqualTo(7);

        SpaceProperty space = stub.getSpaceProperty(GetSpacePropertyRequest.newBuilder()
                .setSpaceId("100").setPropertyId("sp1").build()).getProperty();
        assertThat(space.getId()).isEqualTo("sp1");
        assertThat(space.getCustomKey()).isEqualTo("theme");

        assertInvalid(() -> stub.getContentProperty(GetContentPropertyRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_PAGE, "200")).build()));
        assertInvalid(() -> stub.getSpaceProperty(GetSpacePropertyRequest.newBuilder()
                .setSpaceId("100").build()));
    }

    @Test
    void appPropertiesListAndGetByKey() {
        fake.stub("/wiki/api/v2/app/properties", ConfluenceFixtures.listJson(null, "",
                "{\"key\":\"config\",\"value\":{\"enabled\":true}}",
                "{\"key\":\"count\",\"value\":3}"));
        fake.stub("/wiki/api/v2/app/properties/my key", "{\"key\":\"my key\",\"value\":\"x\"}");

        List<AppProperty> properties = new ArrayList<>();
        stub.listAppProperties(ListAppPropertiesRequest.getDefaultInstance())
                .forEachRemaining(r -> properties.add(r.getProperty()));
        assertThat(properties).extracting(AppProperty::getCustomKey).containsExactly("config", "count");
        assertThat(properties.get(1).getValue().getIntegerValue()).isEqualTo(3);

        AppProperty one = stub.getAppProperty(GetAppPropertyRequest.newBuilder().setKey("my key").build())
                .getProperty();
        assertThat(one.getCustomKey()).isEqualTo("my key");
        assertThat(one.getValue().getStringValue()).isEqualTo("x");
        assertInvalid(() -> stub.getAppProperty(GetAppPropertyRequest.getDefaultInstance()));
    }

    @Test
    void getSpaceRoleAndListSpacePermissions() {
        fake.stub("/wiki/api/v2/space-roles/r1", "{\"id\":\"r1\",\"type\":\"CUSTOM\",\"name\":\"Editor\","
                + "\"description\":\"d\",\"spacePermissions\":[\"read/space\"],\"_links\":{\"base\":\"b\"}}");
        fake.stub("/wiki/api/v2/space-permissions", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"read/space\",\"displayName\":\"View space\",\"description\":\"d\","
                        + "\"requiredPermissionIds\":[]}",
                "{\"id\":\"create/page\",\"displayName\":\"Add pages\",\"description\":\"d2\","
                        + "\"requiredPermissionIds\":[\"read/space\"]}"));

        SpaceRole role = stub.getSpaceRole(GetSpaceRoleRequest.newBuilder().setId("r1").build()).getRole();
        assertThat(role.getType()).isEqualTo(RoleType.ROLE_TYPE_CUSTOM);
        assertThat(role.getSpacePermissionsList()).containsExactly("read/space");

        List<SpacePermission> permissions = new ArrayList<>();
        stub.listSpacePermissions(ListSpacePermissionsRequest.getDefaultInstance())
                .forEachRemaining(r -> permissions.add(r.getPermission()));
        assertThat(permissions).extracting(SpacePermission::getId, SpacePermission::getDisplayName)
                .containsExactly(org.assertj.core.groups.Tuple.tuple("read/space", "View space"),
                        org.assertj.core.groups.Tuple.tuple("create/page", "Add pages"));
        assertThat(permissions.get(1).getRequiredPermissionIdsList()).containsExactly("read/space");
        assertThat(fake.lastTo("/wiki/api/v2/space-permissions").params())
                .containsExactlyEntriesOf(java.util.Map.of("limit", "100"));
    }

    @Test
    void transitionCombinationsFollowTheBodyCursorAndTypePrincipals() {
        fake.stub("/wiki/api/v2/space-permissions/transition/combinations?limit=100",
                "{\"results\":[{\"combinationId\":\"k1\",\"spaceCount\":3,\"principalCount\":9,"
                        + "\"permissions\":[{\"id\":\"VIEW_CONTENT\",\"displayName\":\"View\"}],"
                        + "\"principalTypes\":[\"USER\",\"ALL_LICENSED_USERS_USER_CLASS\",\"ROBOT\"]}],"
                        + "\"generatedAt\":\"2024-03-02T00:00:00Z\",\"cursor\":\"c2\"}");
        fake.stub("/wiki/api/v2/space-permissions/transition/combinations?cursor=c2&limit=100",
                "{\"results\":[{\"combinationId\":\"k2\",\"spaceCount\":1,\"principalCount\":1,"
                        + "\"permissions\":[],\"principalTypes\":[\"GROUP\"]}],"
                        + "\"generatedAt\":\"2024-03-02T00:00:00Z\",\"cursor\":null}");

        List<ListSpacePermissionTransitionCombinationsResponse> out = new ArrayList<>();
        stub.listSpacePermissionTransitionCombinations(
                ListSpacePermissionTransitionCombinationsRequest.getDefaultInstance())
                .forEachRemaining(out::add);

        assertThat(out).extracting(r -> r.getCombination().getCombinationId()).containsExactly("k1", "k2");
        BulkTransitionCombinationEntry first = out.get(0).getCombination();
        assertThat(first.getSpaceCount()).isEqualTo(3);
        assertThat(first.getPrincipalCount()).isEqualTo(9);
        assertThat(first.getPermissions(0).getDisplayName()).isEqualTo("View");
        assertThat(first.getPrincipalTypesList())
                .containsExactly("USER", "ALL_LICENSED_USERS_USER_CLASS", "ROBOT");
        assertThat(first.getTypedPrincipalTypesList()).containsExactly(
                BulkTransitionPrincipalType.BULK_TRANSITION_PRINCIPAL_TYPE_USER,
                BulkTransitionPrincipalType.BULK_TRANSITION_PRINCIPAL_TYPE_ALL_LICENSED_USERS_USER_CLASS,
                BulkTransitionPrincipalType.BULK_TRANSITION_PRINCIPAL_TYPE_UNSPECIFIED);
        assertThat(out).allSatisfy(r -> assertThat(r.getGeneratedAt().getSeconds())
                .isEqualTo(T2.getEpochSecond()));
        assertThat(fake.requestsTo("/wiki/api/v2/space-permissions/transition/combinations")).hasSize(2);
    }

    @Test
    void transitionCombinationsRefuseARepeatedCursor() {
        fake.stub("/wiki/api/v2/space-permissions/transition/combinations",
                "{\"results\":[],\"cursor\":\"same\"}");
        assertThatThrownBy(() -> stub.listSpacePermissionTransitionCombinations(
                ListSpacePermissionTransitionCombinationsRequest.getDefaultInstance()).forEachRemaining(r -> { }))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(
                        e.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL));
    }

    @Test
    void getSpacePermissionTransitionTaskTypesTheStatus() {
        fake.stub("/wiki/api/v2/space-permissions/transition/tasks/t1",
                "{\"taskId\":\"t1\",\"status\":\"FAILED\",\"errorMessage\":\"boom\"}");
        fake.stub("/wiki/api/v2/space-permissions/transition/tasks/t2",
                "{\"taskId\":\"t2\",\"status\":\"IN_PROGRESS\",\"errorMessage\":null}");

        BulkTransitionTaskStatusResponse failed = stub.getSpacePermissionTransitionTask(
                GetSpacePermissionTransitionTaskRequest.newBuilder().setTaskId("t1").build()).getTask();
        assertThat(failed.getStatus()).isEqualTo(BulkTransitionTaskStatus.BULK_TRANSITION_TASK_STATUS_FAILED);
        assertThat(failed.getErrorMessage()).isEqualTo("boom");
        BulkTransitionTaskStatusResponse running = stub.getSpacePermissionTransitionTask(
                GetSpacePermissionTransitionTaskRequest.newBuilder().setTaskId("t2").build()).getTask();
        assertThat(running.getStatus())
                .isEqualTo(BulkTransitionTaskStatus.BULK_TRANSITION_TASK_STATUS_IN_PROGRESS);
        assertThat(running.hasErrorMessage()).isFalse();
    }

    @Test
    void getAdminKeyReadsTheCallersKeyAndMapsAbsenceToNotFound() {
        fake.stub("/wiki/api/v2/admin-key",
                "{\"accountId\":\"acc-1\",\"expirationTime\":\"2024-03-03T00:00:00.000Z\"}");
        AdminKeyResponse key = stub.getAdminKey(GetAdminKeyRequest.getDefaultInstance()).getAdminKey();
        assertThat(key.getAccountId()).isEqualTo("acc-1");
        assertThat(key.getExpirationTime().getSeconds()).isEqualTo(T3.getEpochSecond());
        assertThat(fake.lastTo("/wiki/api/v2/admin-key").method()).isEqualTo("GET");

        fake.stub("/wiki/api/v2/admin-key", new FakeConfluenceServer.Stub(404, "{}", java.util.Map.of()));
        assertThatThrownBy(() -> stub.getAdminKey(GetAdminKeyRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> assertThat(
                        e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
    }

    @Test
    void checkAccessByEmailChunksAndMergesTheResults() throws Exception {
        fake.stub("/wiki/api/v2/user/access/check-access-by-email",
                "{\"emailsWithoutAccess\":[\"u1@x.io\"],\"invalidEmails\":[\"bad\"]}");
        CheckAccessByEmailRequest.Builder request = CheckAccessByEmailRequest.newBuilder();
        for (int i = 0; i < 150; i++) {
            request.addEmails("u" + i + "@x.io");
        }
        request.addEmails("u0@x.io").addEmails(" "); // duplicates and blanks drop

        CheckAccessByEmailResponse response = stub.checkAccessByEmail(request.build());

        assertThat(response.getEmailsWithoutAccessList()).containsExactly("u1@x.io", "u1@x.io");
        assertThat(response.getInvalidEmailsList()).containsExactly("bad", "bad");
        List<FakeConfluenceServer.RecordedRequest> calls =
                fake.requestsTo("/wiki/api/v2/user/access/check-access-by-email");
        assertThat(calls).hasSize(2).allSatisfy(c -> assertThat(c.method()).isEqualTo("POST"));
        com.fasterxml.jackson.databind.ObjectMapper json = new com.fasterxml.jackson.databind.ObjectMapper();
        assertThat(json.readTree(calls.get(0).body()).path("emails")).hasSize(100);
        assertThat(json.readTree(calls.get(1).body()).path("emails")).hasSize(50);
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
                .filter(SyncResponse::hasChange).map(SyncResponse::getChange)
                .filter(c -> c.getEntity().hasPage()).toList();
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
