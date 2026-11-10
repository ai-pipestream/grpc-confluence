package ai.pipestream.confluence;

import ai.pipestream.confluence.FakeConfluenceServer.RecordedRequest;
import ai.pipestream.confluence.v1.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import com.google.rpc.RetryInfo;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The write rpcs end to end against the fake REST server: the route, query
 * and JSON body each typed request becomes, the typed answer, and the error
 * mapping.
 */
class ConfluenceWriteRpcTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Path V2_SPEC = Path.of("../docs/upstream/atlassian/confluence-v2.json");
    /** The v2 POST routes that only read. */
    private static final Set<String> READ_POSTS = Set.of(
            "POST /content/convert-ids-to-types", "POST /user/access/check-access-by-email",
            "POST /users-bulk", "POST /space-permissions/transition/combinations");
    private static final String MODIFIED = "2024-03-01T00:00:00Z";

    private FakeConfluenceServer fake;
    private Server server;
    private ManagedChannel channel;
    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub;

    @BeforeEach
    void startStack() throws Exception {
        fake = FakeConfluenceServer.start();
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl()).email("bot@pipestream.ai").apiToken("token-123").build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ZERO);
        server = InProcessServerBuilder.forName("confluence-write-test")
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, client,
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        channel = InProcessChannelBuilder.forName("confluence-write-test").build();
        stub = ConfluenceServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void stopStack() {
        channel.shutdownNow();
        server.shutdownNow();
        fake.close();
    }

    // ======================================================================
    // COVERAGE OF THE V2 WRITE SURFACE
    // ======================================================================

    @Test
    void everyV2WriteOperationOfTheVendoredSpecHasAnRpc() throws Exception {
        Set<String> writes = new TreeSet<>();
        JSON.readTree(Files.readString(V2_SPEC)).path("paths").properties().forEach(path ->
                path.getValue().properties().forEach(operation -> {
                    String route = operation.getKey().toUpperCase() + " " + path.getKey();
                    if (Set.of("post", "put", "delete").contains(operation.getKey())
                            && !READ_POSTS.contains(route)) {
                        writes.add(route);
                    }
                }));
        Set<String> covered = new TreeSet<>();
        ConfluenceWriteRoutes.V2.values().forEach(covered::addAll);

        assertThat(writes).hasSize(100);
        assertThat(covered).isEqualTo(writes);
    }

    @Test
    void everyTabledRouteIsTheRouteItsRpcCalls() {
        Map<String, List<String>> routes = new HashMap<>(ConfluenceWriteRoutes.V2);
        routes.putAll(ConfluenceWriteRoutes.V1);
        routes.forEach((rpc, templates) -> {
            MethodDescriptor<?, ?> method = method(rpc);
            for (String template : templates) {
                String verb = template.substring(0, template.indexOf(' '));
                String route = template.substring(template.indexOf(' ') + 1);
                String path = "/wiki" + (ConfluenceWriteRoutes.V1.containsKey(rpc) ? "" : "/api/v2")
                        + concrete(route);
                fake.clearRequests();
                try {
                    call(method, filled(prototype(method).newBuilderForType(), route));
                } catch (StatusRuntimeException e) {
                    assertThat(e.getStatus().getCode()).as("%s via %s: %s", template, rpc, e)
                            .isEqualTo(Status.Code.NOT_FOUND);
                }
                assertThat(fake.requests()).as("%s via %s", template, rpc)
                        .extracting(r -> r.method() + " " + r.path()).containsExactly(verb + " " + path);
            }
        });
    }

    @Test
    void mutateCarriesEveryUnaryWrite() {
        Set<String> unary = new TreeSet<>(ConfluenceWriteRoutes.V2.keySet());
        unary.addAll(ConfluenceWriteRoutes.V1.keySet());
        Set<String> served = new TreeSet<>();
        ConfluenceWriter.OPS.keySet().forEach(type ->
                served.add(type.getSimpleName().replace("Request", "")));
        assertThat(served).isEqualTo(unary);

        Set<String> requestArms = new TreeSet<>();
        for (FieldDescriptor arm : MutateRequest.getDescriptor().getOneofs().get(0).getFields()) {
            String rpc = arm.getMessageType().getName().replace("Request", "");
            requestArms.add(rpc);
            assertThat(MutateResponse.getDescriptor().findFieldByNumber(arm.getNumber())
                    .getMessageType().getName()).isEqualTo(rpc + "Response");
        }
        assertThat(requestArms).isEqualTo(unary);
        assertThat(ConfluenceWriteRoutes.rpcs()).allSatisfy(rpc -> assertThat(method(rpc)).isNotNull());
    }

    // ======================================================================
    // PAGES, BLOG POSTS, COMMENTS
    // ======================================================================

    @Test
    void createPageSendsTheTypedBodyAsAdfAndReturnsThePage() throws Exception {
        fake.stub("POST", "/wiki/api/v2/pages", 200,
                ConfluenceFixtures.pageJson("501", "100", "Release notes", MODIFIED));
        AdfDocument adf = AdfDocument.newBuilder().addContent(AdfNode.newBuilder()
                .setType(AdfNodeType.ADF_NODE_TYPE_PARAGRAPH)
                .addContent(AdfNode.newBuilder().setType(AdfNodeType.ADF_NODE_TYPE_TEXT)
                        .setText("Shipped."))).build();

        CreatePageResponse response = stub.createPage(CreatePageRequest.newBuilder()
                .setSpaceId("100").setTitle("Release notes").setParentId("7")
                .setStatus(ContentStatus.CONTENT_STATUS_DRAFT)
                .setSubtype(PageSubtype.PAGE_SUBTYPE_LIVE)
                .setBody(ContentBody.newBuilder().setAdf(adf))
                .setPrivate(true).setRootLevel(true).build());

        assertThat(response.getPage().getId()).isEqualTo("501");
        RecordedRequest request = fake.lastTo("POST", "/wiki/api/v2/pages");
        assertThat(request.params()).containsOnly(Map.entry("private", "true"),
                Map.entry("root-level", "true"));
        assertThat(request.header("content-type")).isEqualTo("application/json");
        JsonNode body = JSON.readTree(request.body());
        assertThat(body.path("spaceId").asText()).isEqualTo("100");
        assertThat(body.path("status").asText()).isEqualTo("draft");
        assertThat(body.path("title").asText()).isEqualTo("Release notes");
        assertThat(body.path("parentId").asText()).isEqualTo("7");
        assertThat(body.path("subtype").asText()).isEqualTo("live");
        assertThat(body.path("body").path("representation").asText()).isEqualTo("atlas_doc_format");
        assertThat(JSON.readTree(body.path("body").path("value").asText())).isEqualTo(JSON.readTree("""
                {"version":1,"type":"doc","content":[
                  {"type":"paragraph","content":[{"type":"text","text":"Shipped."}]}]}"""));
    }

    @Test
    void updatePageSendsTheNextVersionNumber() throws Exception {
        fake.stub("PUT", "/wiki/api/v2/pages/501", 200,
                ConfluenceFixtures.pageJson("501", "100", "Renamed", MODIFIED));

        UpdatePageResponse response = stub.updatePage(UpdatePageRequest.newBuilder()
                .setId("501").setTitle("Renamed").setOwnerId("acc-9")
                .setBody(ContentBody.newBuilder()
                        .setStorage(StorageBody.newBuilder().setValue("<p>new</p>")))
                .setExpectedVersion(4).setVersionMessage("rename").build());

        assertThat(response.getPage().getTitle()).isEqualTo("Renamed");
        JsonNode body = JSON.readTree(fake.lastTo("PUT", "/wiki/api/v2/pages/501").body());
        assertThat(body).isEqualTo(JSON.readTree("""
                {"id":"501","status":"current","title":"Renamed","ownerId":"acc-9",
                 "body":{"representation":"storage","value":"<p>new</p>"},
                 "version":{"number":5,"message":"rename"}}"""));
    }

    @Test
    void aVersionConflictIsAborted() {
        fake.stub("PUT", "/wiki/api/v2/pages/501", 409, "{\"errors\":[{\"title\":\"stale\"}]}");
        assertThatThrownBy(() -> stub.updatePage(UpdatePageRequest.newBuilder()
                .setId("501").setTitle("t").setExpectedVersion(1).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.ABORTED));
    }

    @Test
    void throttlingIsResourceExhaustedWithRetryInfo() throws Exception {
        fake.stub("DELETE", "/wiki/api/v2/folders/9",
                new FakeConfluenceServer.Stub(429, "{}", Map.of("Retry-After", "0")));
        assertThatThrownBy(() -> stub.deleteFolder(DeleteFolderRequest.newBuilder().setId("9").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
                    assertThat(StatusProto.fromThrowable(e).getDetailsList())
                            .anySatisfy(detail -> assertThat(detail.is(RetryInfo.class)).isTrue());
                });

        StatusRuntimeException mapped = ConfluenceGrpcService.Failure.of(
                new ConfluenceClient.ConfluenceApiException(429, "slow down", 7)).toException();
        assertThat(StatusProto.fromThrowable(mapped).getDetails(0).unpack(RetryInfo.class)
                .getRetryDelay().getSeconds()).isEqualTo(7);
    }

    @Test
    void invalidRequestsAreRefusedBeforeAnyCall() {
        assertThatThrownBy(() -> stub.updatePage(UpdatePageRequest.newBuilder()
                .setId("5/../6").setExpectedVersion(1).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> stub.updatePage(UpdatePageRequest.newBuilder()
                .setId("5").setStatus(ContentStatus.CONTENT_STATUS_TRASHED).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> stub.createPage(CreatePageRequest.newBuilder().setSpaceId("1")
                .setBody(ContentBody.newBuilder().setRaw(RawBody.newBuilder().setValue("x"))).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> stub.createPage(CreatePageRequest.newBuilder().setSpaceId("1")
                .setBody(ContentBody.newBuilder().setAdf(AdfDocument.newBuilder()
                        .addContent(AdfNode.getDefaultInstance()))).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThat(fake.requests()).isEmpty();
    }

    @Test
    void deletePageCarriesPurgeAndDraft() {
        fake.stub("DELETE", "/wiki/api/v2/pages/501", 204, "");
        stub.deletePage(DeletePageRequest.newBuilder().setId("501").setPurge(true).build());
        assertThat(fake.lastTo("DELETE", "/wiki/api/v2/pages/501").params())
                .containsOnly(Map.entry("purge", "true"));
        stub.deletePage(DeletePageRequest.newBuilder().setId("501").setDraft(true).build());
        assertThat(fake.lastTo("DELETE", "/wiki/api/v2/pages/501").params())
                .containsOnly(Map.entry("draft", "true"));
    }

    @Test
    void blogPostWritesCarryCreatedAt() throws Exception {
        fake.stub("PUT", "/wiki/api/v2/blogposts/61", 200,
                ConfluenceFixtures.blogPostJson("61", "100", "News", MODIFIED));
        UpdateBlogPostResponse response = stub.updateBlogPost(UpdateBlogPostRequest.newBuilder()
                .setId("61").setTitle("News").setStatus(ContentStatus.CONTENT_STATUS_CURRENT)
                .setBody(ContentBody.newBuilder().setWiki(WikiBody.newBuilder().setValue("h1. News")))
                .setCreatedAt(Timestamp.newBuilder().setSeconds(1_700_000_000L))
                .setExpectedVersion(2).build());
        assertThat(response.getBlogPost().getId()).isEqualTo("61");
        assertThat(JSON.readTree(fake.lastTo("PUT", "/wiki/api/v2/blogposts/61").body()))
                .isEqualTo(JSON.readTree("""
                        {"id":"61","status":"current","title":"News",
                         "body":{"representation":"wiki","value":"h1. News"},
                         "version":{"number":3},"createdAt":"2023-11-14T22:13:20Z"}"""));
    }

    @Test
    void inlineCommentsCarryTheSelectionAndResolution() throws Exception {
        fake.stub("POST", "/wiki/api/v2/inline-comments", 201,
                ConfluenceFixtures.inlineCommentJson("c1", "501"));
        fake.stub("PUT", "/wiki/api/v2/inline-comments/c1", 200,
                ConfluenceFixtures.inlineCommentJson("c1", "501"));

        CreateInlineCommentResponse created = stub.createInlineComment(
                CreateInlineCommentRequest.newBuilder().setPageId("501")
                        .setBody(ContentBody.newBuilder()
                                .setStorage(StorageBody.newBuilder().setValue("<p>typo</p>")))
                        .setSelection(InlineCommentSelection.newBuilder().setTextSelection("teh")
                                .setTextSelectionMatchCount(3).setTextSelectionMatchIndex(1)).build());
        assertThat(created.getComment().getKind()).isEqualTo(CommentKind.COMMENT_KIND_INLINE);
        assertThat(JSON.readTree(fake.lastTo("POST", "/wiki/api/v2/inline-comments").body()))
                .isEqualTo(JSON.readTree("""
                        {"pageId":"501","body":{"representation":"storage","value":"<p>typo</p>"},
                         "inlineCommentProperties":{"textSelection":"teh",
                           "textSelectionMatchCount":3,"textSelectionMatchIndex":1}}"""));

        stub.updateInlineComment(UpdateInlineCommentRequest.newBuilder().setId("c1")
                .setExpectedVersion(1).setResolved(false).build());
        assertThat(JSON.readTree(fake.lastTo("PUT", "/wiki/api/v2/inline-comments/c1").body()))
                .isEqualTo(JSON.readTree("{\"version\":{\"number\":2},\"resolved\":false}"));
    }

    @Test
    void footerCommentRepliesNameTheParent() throws Exception {
        fake.stub("POST", "/wiki/api/v2/footer-comments", 201,
                ConfluenceFixtures.footerCommentJson("c2", "501"));
        CreateFooterCommentResponse created = stub.createFooterComment(
                CreateFooterCommentRequest.newBuilder().setParentCommentId("c1")
                        .setBody(ContentBody.newBuilder()
                                .setStorage(StorageBody.newBuilder().setValue("<p>+1</p>"))).build());
        assertThat(created.getComment().getKind()).isEqualTo(CommentKind.COMMENT_KIND_FOOTER);
        assertThat(JSON.readTree(fake.lastTo("POST", "/wiki/api/v2/footer-comments").body()))
                .isEqualTo(JSON.readTree("""
                        {"parentCommentId":"c1","body":{"representation":"storage","value":"<p>+1</p>"}}"""));
    }

    // ======================================================================
    // TREE NODES, SPACES, TASKS
    // ======================================================================

    @Test
    void createWhiteboardSendsTheTemplateAndLocaleWireValues() throws Exception {
        fake.stub("POST", "/wiki/api/v2/whiteboards", 200,
                "{\"id\":\"w1\",\"type\":\"whiteboard\",\"title\":\"Retro\",\"spaceId\":\"100\"}");
        CreateWhiteboardResponse response = stub.createWhiteboard(CreateWhiteboardRequest.newBuilder()
                .setSpaceId("100").setTitle("Retro")
                .setTemplateKey(WhiteboardTemplateKey.WHITEBOARD_TEMPLATE_KEY_4LS_RETRO)
                .setLocale(WhiteboardLocale.WHITEBOARD_LOCALE_PT_BR).setPrivate(true).build());
        assertThat(response.getWhiteboard().getId()).isEqualTo("w1");
        RecordedRequest request = fake.lastTo("POST", "/wiki/api/v2/whiteboards");
        assertThat(request.params()).containsOnly(Map.entry("private", "true"));
        assertThat(JSON.readTree(request.body())).isEqualTo(JSON.readTree("""
                {"spaceId":"100","title":"Retro","templateKey":"4ls-retro","locale":"pt-BR"}"""));
    }

    @Test
    void everyWhiteboardTemplateAndLocaleOfTheSpecHasAnEnumValue() throws Exception {
        JsonNode properties = JSON.readTree(Files.readString(V2_SPEC)).path("components")
                .path("requestBodies").path("WhiteboardCreateRequest").path("content")
                .path("application/json").path("schema").path("properties");
        assertThat(wireValues(WhiteboardTemplateKey.getDescriptor()))
                .isEqualTo(texts(properties.path("templateKey").path("enum")));
        Set<String> locales = new TreeSet<>();
        texts(properties.path("locale").path("enum")).forEach(l -> locales.add(l.toLowerCase()));
        assertThat(wireValues(WhiteboardLocale.getDescriptor())).isEqualTo(locales);
    }

    @Test
    void createSpaceSendsRoleAssignments() throws Exception {
        fake.stub("POST", "/wiki/api/v2/spaces", 201,
                ConfluenceFixtures.spaceJson("100", "ENG", "Engineering"));
        CreateSpaceResponse response = stub.createSpace(CreateSpaceRequest.newBuilder()
                .setName("Engineering").setKey("ENG").setDescription("All things eng")
                .addRoleAssignments(SpaceRoleAssignment.newBuilder().setRoleId("role-1")
                        .setPrincipal(Principal.newBuilder()
                                .setPrincipalType(PrincipalType.PRINCIPAL_TYPE_ACCESS_CLASS)
                                .setPrincipalId("authenticated-users")))
                .setCopySpaceAccessConfiguration(42).setCreatePrivateSpace(true).build());
        assertThat(response.getSpace().getKey()).isEqualTo("ENG");
        assertThat(JSON.readTree(fake.lastTo("POST", "/wiki/api/v2/spaces").body()))
                .isEqualTo(JSON.readTree("""
                        {"name":"Engineering","key":"ENG",
                         "description":{"value":"All things eng","representation":"plain"},
                         "roleAssignments":[{"principal":{"principalType":"ACCESS_CLASS",
                            "principalId":"authenticated-users"},"roleId":"role-1"}],
                         "copySpaceAccessConfiguration":42,"createPrivateSpace":true}"""));
    }

    @Test
    void updateTaskSendsTheStatusAndBodyFormat() throws Exception {
        fake.stub("PUT", "/wiki/api/v2/tasks/t1", 200,
                "{\"id\":\"t1\",\"status\":\"complete\",\"pageId\":\"501\"}");
        UpdateTaskResponse response = stub.updateTask(UpdateTaskRequest.newBuilder().setId("t1")
                .setStatus(TaskStatus.TASK_STATUS_COMPLETE)
                .setBodyFormat(BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT)
                .setDueAt(Timestamp.newBuilder().setSeconds(1_700_000_000L)).build());
        assertThat(response.getTask().getStatus()).isEqualTo(TaskStatus.TASK_STATUS_COMPLETE);
        RecordedRequest request = fake.lastTo("PUT", "/wiki/api/v2/tasks/t1");
        assertThat(request.params()).containsOnly(Map.entry("body-format", "atlas_doc_format"));
        assertThat(JSON.readTree(request.body())).isEqualTo(JSON.readTree(
                "{\"id\":\"t1\",\"status\":\"complete\",\"dueAt\":\"2023-11-14T22:13:20Z\"}"));
    }

    // ======================================================================
    // PROPERTIES
    // ======================================================================

    @Test
    void propertyValuesBecomeJsonOfTheSameShape() throws Exception {
        fake.stub("POST", "/wiki/api/v2/pages/501/properties", 200,
                ConfluenceFixtures.propertyJson("p1", "review", "{\"state\":\"open\"}"));
        PropertyValue value = PropertyValue.newBuilder().setObjectValue(PropertyObject.newBuilder()
                .addEntries(entry("state", PropertyValue.newBuilder().setStringValue("open")))
                .addEntries(entry("round", PropertyValue.newBuilder().setIntegerValue(2)))
                .addEntries(entry("score", PropertyValue.newBuilder().setDoubleValue(0.5)))
                .addEntries(entry("big", PropertyValue.newBuilder()
                        .setBigIntegerValue("123456789012345678901234567890")))
                .addEntries(entry("exact", PropertyValue.newBuilder()
                        .setDecimalValue("1.00000000000000000001")))
                .addEntries(entry("done", PropertyValue.newBuilder().setBoolValue(false)))
                .addEntries(entry("owner", PropertyValue.newBuilder()
                        .setNullValue(PropertyNull.getDefaultInstance())))
                .addEntries(entry("tags", PropertyValue.newBuilder().setListValue(
                        PropertyList.newBuilder()
                                .addValues(PropertyValue.newBuilder().setStringValue("a"))
                                .addValues(PropertyValue.newBuilder().setIntegerValue(1))))))
                .build();

        CreateContentPropertyResponse response = stub.createContentProperty(
                CreateContentPropertyRequest.newBuilder()
                        .setContent(ref(ContentType.CONTENT_TYPE_PAGE, "501"))
                        .setKey(PropertyKey.PROPERTY_KEY_CUSTOM).setCustomKey("review")
                        .setValue(value).build());

        assertThat(response.getProperty().getCustomKey()).isEqualTo("review");
        assertThat(fake.lastTo("POST", "/wiki/api/v2/pages/501/properties").body()).isEqualTo(
                "{\"key\":\"review\",\"value\":{\"state\":\"open\",\"round\":2,\"score\":0.5,"
                        + "\"big\":123456789012345678901234567890,\"exact\":1.00000000000000000001,"
                        + "\"done\":false,\"owner\":null,\"tags\":[\"a\",1]}}");
    }

    @Test
    void propertyUpdatesAndDeletesUseTheOwnersRoute() throws Exception {
        fake.stub("PUT", "/wiki/api/v2/comments/c1/properties/p1", 200,
                ConfluenceFixtures.propertyJson("p1", "editor", "\"v2\""));
        fake.stub("DELETE", "/wiki/api/v2/embeds/e1/properties/p2", 204, "");
        fake.stub("PUT", "/wiki/api/v2/spaces/100/properties/p3", 200,
                ConfluenceFixtures.spacePropertyJson("p3", "theme", "\"dark\""));

        stub.updateContentProperty(UpdateContentPropertyRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_INLINE_COMMENT, "c1")).setPropertyId("p1")
                .setKey(PropertyKey.PROPERTY_KEY_EDITOR)
                .setValue(PropertyValue.newBuilder().setStringValue("v2"))
                .setExpectedVersion(1).build());
        assertThat(JSON.readTree(fake.lastTo("PUT", "/wiki/api/v2/comments/c1/properties/p1").body()))
                .isEqualTo(JSON.readTree(
                        "{\"key\":\"editor\",\"value\":\"v2\",\"version\":{\"number\":2}}"));

        stub.deleteContentProperty(DeleteContentPropertyRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_SMART_LINK, "e1")).setPropertyId("p2").build());
        assertThat(fake.lastTo("DELETE", "/wiki/api/v2/embeds/e1/properties/p2")).isNotNull();

        UpdateSpacePropertyResponse space = stub.updateSpaceProperty(
                UpdateSpacePropertyRequest.newBuilder().setSpaceId("100").setPropertyId("p3")
                        .setKey(PropertyKey.PROPERTY_KEY_CUSTOM).setCustomKey("theme")
                        .setValue(PropertyValue.newBuilder().setStringValue("dark"))
                        .setExpectedVersion(6).setVersionMessage("night").build());
        assertThat(space.getProperty().getId()).isEqualTo("p3");
        assertThat(JSON.readTree(fake.lastTo("PUT", "/wiki/api/v2/spaces/100/properties/p3").body()))
                .isEqualTo(JSON.readTree("{\"key\":\"theme\",\"value\":\"dark\","
                        + "\"version\":{\"number\":7,\"message\":\"night\"}}"));
    }

    @Test
    void propertiesNeedAKeyAndAValue() {
        CreateContentPropertyRequest.Builder request = CreateContentPropertyRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_PAGE, "501"));
        assertThatThrownBy(() -> stub.createContentProperty(request.build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> stub.createContentProperty(request
                .setKey(PropertyKey.PROPERTY_KEY_EDITOR).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThat(fake.requests()).isEmpty();
    }

    @Test
    void appPropertyReportsWhetherItWasCreated() throws Exception {
        fake.stub("PUT", "/wiki/api/v2/app/properties/my key", 201, "");
        SetAppPropertyRequest request = SetAppPropertyRequest.newBuilder().setKey("my key")
                .setValue(PropertyValue.newBuilder().setObjectValue(PropertyObject.newBuilder()
                        .addEntries(entry("on", PropertyValue.newBuilder().setBoolValue(true)))))
                .build();
        assertThat(stub.setAppProperty(request).getCreated()).isTrue();
        assertThat(fake.requests().getLast().body()).isEqualTo("{\"on\":true}");
        fake.stub("PUT", "/wiki/api/v2/app/properties/my key", 200, "");
        assertThat(stub.setAppProperty(request).getCreated()).isFalse();
    }

    // ======================================================================
    // PERMISSIONS AND CLASSIFICATION
    // ======================================================================

    @Test
    void contentPermissionWritesUseTheOwnersRoute() throws Exception {
        String results = """
                {"results":[{"operation":"update","principal":{"principalType":"group",
                  "principalId":"g1"},"createdAt":"2024-03-01T00:00:00Z","createdBy":"acc-1"}],
                 "_links":{}}""";
        fake.stub("POST", "/wiki/api/v2/pages/501/permissions", 200, results);
        fake.stub("PUT", "/wiki/api/v2/folders/f1/permissions", 200, results);
        fake.stub("DELETE", "/wiki/api/v2/whiteboards/w1/permissions", 204, "");
        fake.stub("PUT", "/wiki/api/v2/databases/d1/restriction-state", 200,
                "{\"state\":\"EDIT_RESTRICTED\",\"createdBy\":\"acc-1\"}");
        ContentPermissionGrant grant = ContentPermissionGrant.newBuilder()
                .setOperation(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_UPDATE)
                .addPrincipals(principal("g1")).build();
        JsonNode expected = JSON.readTree("""
                {"permissions":[{"operation":"update",
                  "principals":[{"principalType":"group","principalId":"g1"}]}]}""");

        AddContentPermissionsResponse added = stub.addContentPermissions(
                AddContentPermissionsRequest.newBuilder()
                        .setContent(ref(ContentType.CONTENT_TYPE_PAGE, "501"))
                        .addPermissions(grant).setAllowSelfLockout(true).build());
        assertThat(added.getPermissions(0).getPrincipal().getPrincipalId()).isEqualTo("g1");
        RecordedRequest add = fake.lastTo("POST", "/wiki/api/v2/pages/501/permissions");
        assertThat(add.params()).containsOnly(Map.entry("allow-self-lockout", "true"));
        assertThat(JSON.readTree(add.body())).isEqualTo(expected);

        SetContentPermissionsResponse set = stub.setContentPermissions(
                SetContentPermissionsRequest.newBuilder()
                        .setContent(ref(ContentType.CONTENT_TYPE_FOLDER, "f1"))
                        .addPermissions(grant).build());
        assertThat(set.getPermissions(0).getOperation())
                .isEqualTo(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_UPDATE);
        assertThat(JSON.readTree(fake.lastTo("PUT", "/wiki/api/v2/folders/f1/permissions").body()))
                .isEqualTo(expected);

        stub.deleteContentPermissions(DeleteContentPermissionsRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_WHITEBOARD, "w1"))
                .setOperation(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_READ)
                .setPrincipal(principal("g1")).build());
        assertThat(fake.lastTo("DELETE", "/wiki/api/v2/whiteboards/w1/permissions").params())
                .containsOnly(Map.entry("operation", "read"), Map.entry("principal-type", "group"),
                        Map.entry("principal-id", "g1"));

        SetRestrictionStateResponse state = stub.setRestrictionState(
                SetRestrictionStateRequest.newBuilder()
                        .setContent(ref(ContentType.CONTENT_TYPE_DATABASE, "d1"))
                        .setState(ContentRestrictionState.CONTENT_RESTRICTION_STATE_EDIT_RESTRICTED)
                        .build());
        assertThat(state.getRestrictionState().getState())
                .isEqualTo(ContentRestrictionState.CONTENT_RESTRICTION_STATE_EDIT_RESTRICTED);
        assertThat(fake.lastTo("PUT", "/wiki/api/v2/databases/d1/restriction-state").body())
                .isEqualTo("{\"state\":\"EDIT_RESTRICTED\"}");
    }

    @Test
    void permissionWritesAreRefusedForContentWithoutTheRoute() {
        assertThatThrownBy(() -> stub.setRestrictionState(SetRestrictionStateRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_ATTACHMENT, "a1"))
                .setState(ContentRestrictionState.CONTENT_RESTRICTION_STATE_OPEN).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> stub.deleteContentPermissions(
                DeleteContentPermissionsRequest.newBuilder()
                        .setContent(ref(ContentType.CONTENT_TYPE_PAGE, "501"))
                        .setOperation(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_READ)
                        .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThat(fake.requests()).isEmpty();
    }

    @Test
    void classificationWritesSendTheLevelAndStatus() throws Exception {
        fake.stub("PUT", "/wiki/api/v2/pages/501/classification-level", 204, "");
        fake.stub("POST", "/wiki/api/v2/whiteboards/w1/classification-level/reset", 204, "");
        fake.stub("PUT", "/wiki/api/v2/spaces/100/classification-level/default", 204, "");
        fake.stub("DELETE", "/wiki/api/v2/spaces/100/classification-level/default", 204, "");

        stub.setContentClassificationLevel(SetContentClassificationLevelRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_PAGE, "501"))
                .setClassificationLevelId("lvl-2").setStatus(ContentStatus.CONTENT_STATUS_DRAFT).build());
        assertThat(JSON.readTree(fake.lastTo("PUT", "/wiki/api/v2/pages/501/classification-level").body()))
                .isEqualTo(JSON.readTree("{\"id\":\"lvl-2\",\"status\":\"draft\"}"));

        stub.resetContentClassificationLevel(ResetContentClassificationLevelRequest.newBuilder()
                .setContent(ref(ContentType.CONTENT_TYPE_WHITEBOARD, "w1")).build());
        assertThat(fake.lastTo("POST", "/wiki/api/v2/whiteboards/w1/classification-level/reset").body())
                .isEqualTo("{\"status\":\"current\"}");
        assertThatThrownBy(() -> stub.resetContentClassificationLevel(
                ResetContentClassificationLevelRequest.newBuilder()
                        .setContent(ref(ContentType.CONTENT_TYPE_WHITEBOARD, "w1"))
                        .setStatus(ContentStatus.CONTENT_STATUS_DRAFT).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

        stub.updateSpaceDefaultClassificationLevel(
                UpdateSpaceDefaultClassificationLevelRequest.newBuilder().setSpaceId("100")
                        .setClassificationLevelId("lvl-1").build());
        assertThat(fake.lastTo("PUT", "/wiki/api/v2/spaces/100/classification-level/default").body())
                .isEqualTo("{\"id\":\"lvl-1\"}");
        stub.deleteSpaceDefaultClassificationLevel(
                DeleteSpaceDefaultClassificationLevelRequest.newBuilder().setSpaceId("100").build());
        assertThat(fake.lastTo("DELETE", "/wiki/api/v2/spaces/100/classification-level/default"))
                .isNotNull();
    }

    // ======================================================================
    // SPACE ROLES, ACCESS, REDACTION, ADMIN KEY
    // ======================================================================

    @Test
    void spaceRoleWritesReturnTheRoleAndTask() throws Exception {
        fake.stub("POST", "/wiki/api/v2/space-roles", 201, """
                {"id":"r1","type":"CUSTOM","name":"Reviewer","description":"reads",
                 "spacePermissions":["read/space"]}""");
        fake.stub("PUT", "/wiki/api/v2/space-roles/r1", 202,
                "{\"id\":\"r1\",\"type\":\"CUSTOM\",\"name\":\"Reviewer\",\"taskId\":\"task-1\"}");
        fake.stub("DELETE", "/wiki/api/v2/space-roles/r1", 202, "{\"taskId\":\"task-2\"}");

        CreateSpaceRoleResponse created = stub.createSpaceRole(CreateSpaceRoleRequest.newBuilder()
                .setName("Reviewer").setDescription("reads").addSpacePermissions("read/space").build());
        assertThat(created.getRole().getType()).isEqualTo(RoleType.ROLE_TYPE_CUSTOM);
        assertThat(JSON.readTree(fake.lastTo("POST", "/wiki/api/v2/space-roles").body()))
                .isEqualTo(JSON.readTree("""
                        {"name":"Reviewer","description":"reads","spacePermissions":["read/space"]}"""));

        UpdateSpaceRoleResponse updated = stub.updateSpaceRole(UpdateSpaceRoleRequest.newBuilder()
                .setId("r1").setName("Reviewer").setDescription("reads")
                .setGuestReassignmentRoleId("r0").build());
        assertThat(updated.getTaskId()).isEqualTo("task-1");
        assertThat(updated.getRole().getId()).isEqualTo("r1");
        assertThat(JSON.readTree(fake.lastTo("PUT", "/wiki/api/v2/space-roles/r1").body()))
                .isEqualTo(JSON.readTree("""
                        {"name":"Reviewer","description":"reads","spacePermissions":[],
                         "guestReassignmentRoleId":"r0"}"""));

        assertThat(stub.deleteSpaceRole(DeleteSpaceRoleRequest.newBuilder().setId("r1").build())
                .getTaskId()).isEqualTo("task-2");
    }

    @Test
    void roleAssignmentsAndBulkTransitionsSendTheirSelections() throws Exception {
        fake.stub("POST", "/wiki/api/v2/spaces/100/role-assignments", 200, """
                {"results":[{"principal":{"principalType":"USER","principalId":"acc-1"},
                  "roleId":"r1"}],"_links":{}}""");
        fake.stub("POST", "/wiki/api/v2/space-permissions/transition/role-assignments", 202,
                "{\"taskId\":\"t9\",\"status\":\"IN_PROGRESS\",\"statusUrl\":\"/tasks/t9\"}");
        fake.stub("POST", "/wiki/api/v2/space-permissions/transition/access-removals", 202,
                "{\"taskId\":\"t10\",\"status\":\"COMPLETED\",\"statusUrl\":\"/tasks/t10\"}");
        fake.stub("POST", "/wiki/api/v2/user/access/invite-by-email", 200, "");

        SetSpaceRoleAssignmentsResponse set = stub.setSpaceRoleAssignments(
                SetSpaceRoleAssignmentsRequest.newBuilder().setSpaceId("100")
                        .addAssignments(assignment("acc-1", "r1"))
                        .addAssignments(assignment("acc-2", "")).build());
        assertThat(set.getAssignments(0).getRoleId()).isEqualTo("r1");
        assertThat(JSON.readTree(fake.lastTo("POST", "/wiki/api/v2/spaces/100/role-assignments").body()))
                .isEqualTo(JSON.readTree("""
                        [{"principal":{"principalType":"USER","principalId":"acc-1"},"roleId":"r1"},
                         {"principal":{"principalType":"USER","principalId":"acc-2"}}]"""));

        BulkTransitionSpaceSelection selection = BulkTransitionSpaceSelection.newBuilder()
                .setSpaceType(BulkTransitionSpaceType.BULK_TRANSITION_SPACE_TYPE_ALL_EXCEPT_SPECIFIC)
                .addSelectedSpaces(BulkTransitionSpaceTarget.newBuilder().setId("100").setKey("ENG"))
                .build();
        BulkAssignSpaceRolesResponse assigned = stub.bulkAssignSpaceRoles(
                BulkAssignSpaceRolesRequest.newBuilder().setSpaceSelection(selection)
                        .addAssignments(BulkTransitionRoleAssignment.newBuilder()
                                .setPermissionCombinationId("combo-1")
                                .addPrincipalTypeAssignments(
                                        BulkTransitionPrincipalTypeAssignment.newBuilder()
                                                .setPrincipalType(BulkTransitionPrincipalType
                                                        .BULK_TRANSITION_PRINCIPAL_TYPE_GROUP)
                                                .setRoleId("r1"))
                                .addPrincipalTypeAssignments(
                                        BulkTransitionPrincipalTypeAssignment.newBuilder()
                                                .setPrincipalType(BulkTransitionPrincipalType
                                                        .BULK_TRANSITION_PRINCIPAL_TYPE_ANONYMOUS)
                                                .setRemoveAccess(true))).build());
        assertThat(assigned.getTask().getStatus())
                .isEqualTo(BulkTransitionTaskStatus.BULK_TRANSITION_TASK_STATUS_IN_PROGRESS);
        assertThat(assigned.getTask().getStatusUrl()).isEqualTo("/tasks/t9");
        assertThat(JSON.readTree(fake.lastTo("POST",
                "/wiki/api/v2/space-permissions/transition/role-assignments").body()))
                .isEqualTo(JSON.readTree("""
                        {"assignments":[{"permissionCombinationId":"combo-1","principalTypeAssignments":[
                           {"principalType":"GROUP","removeAccess":false,"roleId":"r1"},
                           {"principalType":"ANONYMOUS","removeAccess":true}]}],
                         "spaceSelection":{"spaceType":"ALL_EXCEPT_SPECIFIC",
                           "selectedSpaces":[{"id":"100","key":"ENG"}]}}"""));

        BulkRemoveSpaceAccessResponse removed = stub.bulkRemoveSpaceAccess(
                BulkRemoveSpaceAccessRequest.newBuilder().addPermissionCombinationIds("combo-2")
                        .setSpaceSelection(BulkTransitionSpaceSelection.newBuilder().setSpaceType(
                                BulkTransitionSpaceType.BULK_TRANSITION_SPACE_TYPE_PERSONAL)).build());
        assertThat(removed.getTask().getTaskId()).isEqualTo("t10");
        assertThat(JSON.readTree(fake.lastTo("POST",
                "/wiki/api/v2/space-permissions/transition/access-removals").body()))
                .isEqualTo(JSON.readTree("""
                        {"permissionCombinationIds":["combo-2"],
                         "spaceSelection":{"spaceType":"PERSONAL"}}"""));

        stub.inviteByEmail(InviteByEmailRequest.newBuilder().addEmails("a@example.com").build());
        assertThat(fake.lastTo("POST", "/wiki/api/v2/user/access/invite-by-email").body())
                .isEqualTo("{\"emails\":[\"a@example.com\"]}");
    }

    @Test
    void redactionSendsThePointersAndReturnsTheRedactions() throws Exception {
        fake.stub("POST", "/wiki/api/v2/blogposts/61/redact", 202, """
                {"body":{"redactions":[{"pointer":"/content/0/content/0","from":2,"to":6,
                   "reason":"pii","redactionId":"11111111-2222-3333-4444-555555555555"}]},
                 "title":{"redactions":[]}}""");
        RedactBlogPostResponse response = stub.redactBlogPost(RedactBlogPostRequest.newBuilder()
                .setId("61").setCreatedAt(Timestamp.newBuilder().setSeconds(1_700_000_000L))
                .setCleanHistory(true).setVersionNumber(3)
                .addBodyRedactions(RedactionPointer.newBuilder().setPointer("/content/0/content/0")
                        .setFrom(2).setTo(6).setReason("pii"))
                .addTitleRedactions(RedactionPointer.newBuilder().setPointer("").setTo(4)).build());
        assertThat(response.getRedactions().getBody().getRedactions(0).getRedactionId())
                .isEqualTo("11111111-2222-3333-4444-555555555555");
        assertThat(JSON.readTree(fake.lastTo("POST", "/wiki/api/v2/blogposts/61/redact").body()))
                .isEqualTo(JSON.readTree("""
                        {"createdAt":"2023-11-14T22:13:20Z","cleanHistory":true,"versionNumber":3,
                         "body":{"redactions":[{"pointer":"/content/0/content/0","from":2,"to":6,
                            "reason":"pii"}]},
                         "title":{"redactions":[{"pointer":"","from":0,"to":4}]}}"""));
        assertThatThrownBy(() -> stub.redactPage(RedactPageRequest.newBuilder().setId("5").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void adminKeyIsEnabledAndDisabled() {
        fake.stub("POST", "/wiki/api/v2/admin-key", 200,
                "{\"accountId\":\"acc-1\",\"expirationTime\":\"2024-03-01T00:10:00.000Z\"}");
        fake.stub("DELETE", "/wiki/api/v2/admin-key", 204, "");
        EnableAdminKeyResponse enabled = stub.enableAdminKey(
                EnableAdminKeyRequest.newBuilder().setDurationInMinutes(10).build());
        assertThat(enabled.getAdminKey().getAccountId()).isEqualTo("acc-1");
        assertThat(fake.lastTo("POST", "/wiki/api/v2/admin-key").body())
                .isEqualTo("{\"durationInMinutes\":10}");
        stub.disableAdminKey(DisableAdminKeyRequest.getDefaultInstance());
        assertThat(fake.lastTo("DELETE", "/wiki/api/v2/admin-key")).isNotNull();
    }

    // ======================================================================
    // LABELS (REST API v1)
    // ======================================================================

    @Test
    void labelsAreAddedAndRemovedThroughTheV1Route() throws Exception {
        fake.stub("POST", "/wiki/rest/api/content/501/label", 200, """
                {"results":[{"prefix":"global","name":"release","id":"900","label":"release"},
                            {"prefix":"my","name":"todo","id":"901","label":"my:todo"}],"size":2}""");
        fake.stub("DELETE", "/wiki/rest/api/content/501/label", 204, "");

        AddLabelsResponse added = stub.addLabels(AddLabelsRequest.newBuilder().setContentId("501")
                .addLabels(LabelInput.newBuilder().setName("release"))
                .addLabels(LabelInput.newBuilder().setPrefix(LabelPrefix.LABEL_PREFIX_MY)
                        .setName("todo")).build());
        assertThat(added.getLabelsList()).extracting(Label::getName).containsExactly("release", "todo");
        assertThat(added.getLabels(1).getPrefix()).isEqualTo("my");
        assertThat(JSON.readTree(fake.lastTo("POST", "/wiki/rest/api/content/501/label").body()))
                .isEqualTo(JSON.readTree("""
                        [{"prefix":"global","name":"release"},{"prefix":"my","name":"todo"}]"""));

        stub.removeLabel(RemoveLabelRequest.newBuilder().setContentId("501").setName("a/b").build());
        assertThat(fake.lastTo("DELETE", "/wiki/rest/api/content/501/label").params())
                .containsOnly(Map.entry("name", "a/b"));
    }

    // ======================================================================
    // ATTACHMENT UPLOAD
    // ======================================================================

    @Test
    void uploadStreamsOneMultipartRequestAndAcknowledgesEachChunk() throws Exception {
        fake.stub("POST", "/wiki/rest/api/content/501/child/attachment", 200,
                "{\"results\":[{\"id\":\"att77\",\"title\":\"notes.txt\"}],\"size\":1}");
        fake.stub("/wiki/api/v2/attachments/att77", ConfluenceFixtures.attachmentJson("att77", "501"));
        String first = "a".repeat(70_000);
        String second = "b".repeat(30_000);

        List<UploadAttachmentResponse> events = upload(
                header("501", "notes \"v2\".txt").setMediaType("text/plain").setComment("draft")
                        .setMinorEdit(true).build(),
                List.of(first, second), true);

        assertThat(events).hasSize(3);
        assertThat(events.get(0).getProgress().getBytesReceived()).isEqualTo(70_000);
        assertThat(events.get(1).getProgress().getBytesReceived()).isEqualTo(100_000);
        assertThat(events.get(2).getAttachment().getId()).isEqualTo("att77");
        assertThat(events.get(2).getAttachment().getMediaType()).isEqualTo("image/png");

        RecordedRequest request = fake.lastTo("POST", "/wiki/rest/api/content/501/child/attachment");
        assertThat(request.header("x-atlassian-token")).isEqualTo("no-check");
        assertThat(request.header("transfer-encoding")).isEqualTo("chunked");
        assertThat(request.header("content-length")).isEmpty();
        String contentType = request.header("content-type");
        assertThat(contentType).startsWith("multipart/form-data; boundary=");
        String boundary = "--" + contentType.substring(contentType.indexOf('=') + 1);
        assertThat(request.body()).isEqualTo(boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"comment\"\r\n\r\ndraft\r\n"
                + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"minorEdit\"\r\n\r\ntrue\r\n"
                + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"file\"; filename=\"notes \\\"v2\\\".txt\"\r\n"
                + "Content-Type: text/plain\r\n\r\n"
                + first + second + "\r\n" + boundary + "--\r\n");
    }

    @Test
    void uploadReplacesAnExistingAttachmentWithPut() throws Exception {
        fake.stub("PUT", "/wiki/rest/api/content/501/child/attachment", 200,
                "{\"results\":[{\"id\":\"att78\",\"title\":\"a.bin\","
                        + "\"extensions\":{\"mediaType\":\"application/octet-stream\",\"fileSize\":3}}]}");

        List<UploadAttachmentResponse> events = upload(
                header("501", "a.bin").setReplaceExisting(true).build(), List.of("xyz"), true);

        Attachment stored = events.getLast().getAttachment();
        assertThat(stored.getId()).isEqualTo("att78");
        assertThat(stored.getFileSize()).isEqualTo(3);
        RecordedRequest request = fake.lastTo("PUT", "/wiki/rest/api/content/501/child/attachment");
        assertThat(request.body()).contains("Content-Type: application/octet-stream\r\n\r\nxyz\r\n");
        assertThat(fake.requests()).extracting(RecordedRequest::path)
                .contains("/wiki/api/v2/attachments/att78", "/wiki/api/v2/attachments/78");
    }

    @Test
    void uploadFailuresReachTheCaller() throws Exception {
        fake.stub("POST", "/wiki/rest/api/content/501/child/attachment", 400,
                "{\"message\":\"Cannot add a new attachment with same file name\"}");
        assertThat(uploadFailure(header("501", "dup.txt").build(), List.of("x"), true))
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(uploadFailure(null, List.of("x"), true)).isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(uploadFailure(header("501", "cut.txt").build(), List.of("x"), false))
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
        assertThat(uploadFailure(header("", "x.txt").build(), List.of(), true))
                .isEqualTo(Status.Code.INVALID_ARGUMENT);
    }

    // ======================================================================
    // MUTATE
    // ======================================================================

    @Test
    void mutateAnswersEveryOperationOnceWithItsTypedOutcome() throws Exception {
        fake.stub("POST", "/wiki/api/v2/pages", 200,
                ConfluenceFixtures.pageJson("501", "100", "New", MODIFIED));
        fake.stub("PUT", "/wiki/api/v2/pages/7", 409, "{\"message\":\"stale version\"}");
        fake.stub("POST", "/wiki/rest/api/content/501/label", 200,
                "{\"results\":[{\"prefix\":\"global\",\"name\":\"x\",\"id\":\"1\"}]}");
        fake.stub("DELETE", "/wiki/api/v2/pages/8/permissions", 204, "");
        List<MutateRequest> requests = new ArrayList<>();
        requests.add(MutateRequest.newBuilder().setOpId("create").setCreatePage(
                CreatePageRequest.newBuilder().setSpaceId("100").setTitle("New")).build());
        requests.add(MutateRequest.newBuilder().setOpId("stale").setUpdatePage(
                UpdatePageRequest.newBuilder().setId("7").setTitle("t").setExpectedVersion(1)).build());
        requests.add(MutateRequest.newBuilder().setOpId("label").setAddLabels(
                AddLabelsRequest.newBuilder().setContentId("501")
                        .addLabels(LabelInput.newBuilder().setName("x"))).build());
        requests.add(MutateRequest.newBuilder().setOpId("unshare").setDeleteContentPermissions(
                DeleteContentPermissionsRequest.newBuilder()
                        .setContent(ref(ContentType.CONTENT_TYPE_PAGE, "8"))).build());
        requests.add(MutateRequest.newBuilder().setOpId("empty").build());
        requests.add(MutateRequest.newBuilder().setOpId("missing").setDeleteFolder(
                DeleteFolderRequest.newBuilder().setId("404")).build());
        for (int i = 0; i < 30; i++) {
            requests.add(MutateRequest.newBuilder().setOpId("bulk-" + i).setDeleteContentPermissions(
                    DeleteContentPermissionsRequest.newBuilder()
                            .setContent(ref(ContentType.CONTENT_TYPE_PAGE, "8"))).build());
        }

        Map<String, MutateResponse> answers = new HashMap<>();
        for (MutateResponse response : mutate(requests)) {
            assertThat(answers.put(response.getOpId(), response)).as(response.getOpId()).isNull();
        }

        assertThat(answers).hasSize(requests.size());
        assertThat(answers.get("create").getCreatePage().getPage().getId()).isEqualTo("501");
        MutateError stale = answers.get("stale").getError();
        assertThat(stale.getCode()).isEqualTo(MutateErrorCode.MUTATE_ERROR_CODE_CONFLICT);
        assertThat(stale.getHttpStatus()).isEqualTo(409);
        assertThat(stale.getMessage()).contains("stale version");
        assertThat(answers.get("label").getAddLabels().getLabels(0).getName()).isEqualTo("x");
        assertThat(answers.get("unshare").getOutcomeCase())
                .isEqualTo(MutateResponse.OutcomeCase.DELETE_CONTENT_PERMISSIONS);
        assertThat(answers.get("empty").getError().getCode())
                .isEqualTo(MutateErrorCode.MUTATE_ERROR_CODE_INVALID_ARGUMENT);
        assertThat(answers.get("missing").getError().getCode())
                .isEqualTo(MutateErrorCode.MUTATE_ERROR_CODE_NOT_FOUND);
        assertThat(fake.requests()).filteredOn(r -> r.path().equals("/wiki/api/v2/pages/8/permissions"))
                .hasSize(31);
    }

    // ======================================================================
    // HELPERS
    // ======================================================================

    private static ContentRef ref(ContentType type, String id) {
        return ContentRef.newBuilder().setType(type).setId(id).build();
    }

    private static PropertyEntry entry(String key, PropertyValue.Builder value) {
        return PropertyEntry.newBuilder().setKey(key).setValue(value).build();
    }

    private static ContentPermissionPrincipal principal(String groupId) {
        return ContentPermissionPrincipal.newBuilder()
                .setPrincipalType(ContentPermissionPrincipalType.CONTENT_PERMISSION_PRINCIPAL_TYPE_GROUP)
                .setPrincipalId(groupId).build();
    }

    private static SpaceRoleAssignment assignment(String accountId, String roleId) {
        return SpaceRoleAssignment.newBuilder().setRoleId(roleId)
                .setPrincipal(Principal.newBuilder().setPrincipalType(PrincipalType.PRINCIPAL_TYPE_USER)
                        .setPrincipalId(accountId)).build();
    }

    private static UploadAttachmentHeader.Builder header(String contentId, String fileName) {
        return UploadAttachmentHeader.newBuilder().setContentId(contentId).setFileName(fileName);
    }

    private static Set<String> texts(JsonNode array) {
        Set<String> texts = new TreeSet<>();
        array.forEach(item -> texts.add(item.asText()));
        return texts;
    }

    /** The enum's non-zero value names without the prefix, lower-cased with hyphens. */
    private static Set<String> wireValues(EnumDescriptor type) {
        String prefix = type.getValues().get(0).getName().replace("UNSPECIFIED", "");
        Set<String> values = new TreeSet<>();
        type.getValues().stream().skip(1).forEach(value ->
                values.add(value.getName().substring(prefix.length()).toLowerCase().replace('_', '-')));
        return values;
    }

    /** Runs one upload; {@code header} null skips the header, {@code end} false skips the end frame. */
    private List<UploadAttachmentResponse> upload(UploadAttachmentHeader header, List<String> chunks,
            boolean end) throws Exception {
        List<UploadAttachmentResponse> events = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<UploadAttachmentRequest> frames = ConfluenceServiceGrpc.newStub(channel)
                .uploadAttachment(collector(events, failure, done));
        if (header != null) {
            frames.onNext(UploadAttachmentRequest.newBuilder().setHeader(header).build());
        }
        for (String chunk : chunks) {
            frames.onNext(UploadAttachmentRequest.newBuilder()
                    .setChunk(ByteString.copyFromUtf8(chunk)).build());
        }
        if (end) {
            frames.onNext(UploadAttachmentRequest.newBuilder()
                    .setEnd(UploadAttachmentEnd.getDefaultInstance()).build());
        }
        frames.onCompleted();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        if (failure.get() != null) {
            throw Status.fromThrowable(failure.get()).asRuntimeException();
        }
        return events;
    }

    private Status.Code uploadFailure(UploadAttachmentHeader header, List<String> chunks, boolean end)
            throws Exception {
        try {
            upload(header, chunks, end);
            return Status.Code.OK;
        } catch (StatusRuntimeException e) {
            return e.getStatus().getCode();
        }
    }

    private List<MutateResponse> mutate(List<MutateRequest> requests) throws Exception {
        List<MutateResponse> answers = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<MutateRequest> operations = ConfluenceServiceGrpc.newStub(channel)
                .mutate(collector(answers, failure, done));
        requests.forEach(operations::onNext);
        operations.onCompleted();
        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        assertThat(failure.get()).isNull();
        return answers;
    }

    private static <T> StreamObserver<T> collector(List<T> into, AtomicReference<Throwable> failure,
            CountDownLatch done) {
        return new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                synchronized (into) {
                    into.add(value);
                }
            }

            @Override
            public void onError(Throwable t) {
                failure.set(t);
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        };
    }

    private static MethodDescriptor<?, ?> method(String rpc) {
        return ConfluenceServiceGrpc.getServiceDescriptor().getMethods().stream()
                .filter(m -> rpc.equals(m.getBareMethodName())).findFirst()
                .orElseThrow(() -> new AssertionError("no rpc " + rpc));
    }

    private static Message prototype(MethodDescriptor<?, ?> method) {
        return (Message) ((MethodDescriptor.PrototypeMarshaller<?>) method.getRequestMarshaller())
                .getMessagePrototype();
    }

    @SuppressWarnings("unchecked")
    private <Q, R> void call(MethodDescriptor<Q, R> method, Message request) {
        ClientCalls.blockingUnaryCall(channel, method, CallOptions.DEFAULT, (Q) request);
    }

    /** A route template with every path parameter set to 77. */
    private static String concrete(String route) {
        StringBuilder path = new StringBuilder();
        for (String segment : route.split("/")) {
            if (!segment.isEmpty()) {
                path.append('/').append(segment.startsWith("{") ? "77" : segment);
            }
        }
        return path.toString();
    }

    /** A request of {@code type} with every field set, addressed at the content kind of {@code route}. */
    private static Message filled(Message.Builder builder, String route) {
        for (FieldDescriptor field : builder.getDescriptorForType().getFields()) {
            if (field.getName().equals("connection_id")) {
                continue;
            }
            Object value = value(builder, field, route);
            if (value == null) {
                continue;
            }
            if (field.isRepeated()) {
                builder.addRepeatedField(field, value);
            } else {
                builder.setField(field, value);
            }
        }
        return builder.build();
    }

    private static Object value(Message.Builder owner, FieldDescriptor field, String route) {
        return switch (field.getJavaType()) {
            case STRING -> "77";
            case ENUM -> {
                EnumDescriptor type = field.getEnumType();
                yield switch (type.getName()) {
                    case "ContentStatus" -> ContentStatus.CONTENT_STATUS_CURRENT.getValueDescriptor();
                    case "PropertyKey" -> PropertyKey.PROPERTY_KEY_CUSTOM.getValueDescriptor();
                    default -> type.getValues().get(1);
                };
            }
            case MESSAGE -> switch (field.getMessageType().getName()) {
                case "ContentRef" -> ref(contentType(route), "77");
                case "PropertyValue" -> PropertyValue.newBuilder().setStringValue("v").build();
                case "Timestamp" -> Timestamp.newBuilder().setSeconds(1).build();
                case "ContentBody" -> ContentBody.newBuilder()
                        .setStorage(StorageBody.newBuilder().setValue("<p/>")).build();
                default -> filled(owner.newBuilderForField(field), route);
            };
            default -> null;
        };
    }

    private static ContentType contentType(String route) {
        return switch (route.split("/")[1]) {
            case "pages" -> ContentType.CONTENT_TYPE_PAGE;
            case "blogposts" -> ContentType.CONTENT_TYPE_BLOG_POST;
            case "attachments" -> ContentType.CONTENT_TYPE_ATTACHMENT;
            case "custom-content" -> ContentType.CONTENT_TYPE_CUSTOM_CONTENT;
            case "comments" -> ContentType.CONTENT_TYPE_FOOTER_COMMENT;
            case "whiteboards" -> ContentType.CONTENT_TYPE_WHITEBOARD;
            case "databases" -> ContentType.CONTENT_TYPE_DATABASE;
            case "folders" -> ContentType.CONTENT_TYPE_FOLDER;
            case "embeds" -> ContentType.CONTENT_TYPE_SMART_LINK;
            default -> throw new AssertionError("no content kind for " + route);
        };
    }
}
