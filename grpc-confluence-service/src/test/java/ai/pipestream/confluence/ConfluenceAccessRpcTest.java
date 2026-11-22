package ai.pipestream.confluence;

import ai.pipestream.sync.v1.AclCompleteness;
import ai.pipestream.confluence.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The group rpcs and GetContentAccess against the fake Confluence server. */
class ConfluenceAccessRpcTest {

    private FakeConfluenceServer fake;
    private Server server;
    private ManagedChannel channel;
    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub;

    @BeforeEach
    void start() throws Exception {
        fake = FakeConfluenceServer.start();
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder().baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai").apiToken("token-123").pageSize(2).build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ZERO);
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, client,
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        stub = ConfluenceServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
        fake.close();
    }

    private static String user(String id) {
        return "{\"type\":\"known\",\"accountId\":\"" + id + "\",\"accountType\":\"atlassian\","
                + "\"email\":\"" + id + "@example.com\",\"publicName\":\"" + id + "\",\"displayName\":\""
                + id + "\"}";
    }

    private static String v1Page(String... results) {
        return "{\"results\":[" + String.join(",", results) + "],\"start\":0,\"limit\":2,\"size\":"
                + results.length + "}";
    }

    private static ContentRef page(String id) {
        return ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE).setId(id).build();
    }

    @Test
    void getGroupReadsTheV1GroupById() {
        fake.stub("/wiki/rest/api/group/by-id",
                "{\"type\":\"group\",\"name\":\"confluence-users\",\"id\":\"g-1\"}");

        Group group = stub.getGroup(GetGroupRequest.newBuilder().setGroupId("g-1").build()).getGroup();

        assertThat(group.getId()).isEqualTo("g-1");
        assertThat(group.getName()).isEqualTo("confluence-users");
        assertThat(group.getType()).isEqualTo("group");
        assertThat(fake.lastTo("/wiki/rest/api/group/by-id").params()).containsEntry("id", "g-1");
        assertCode(Status.Code.INVALID_ARGUMENT, () -> stub.getGroup(GetGroupRequest.getDefaultInstance()));
    }

    @Test
    void listGroupMembersWalksOffsetsUntilAShortPage() {
        String path = "/wiki/rest/api/group/g-1/membersByGroupId";
        fake.stubOnce(path, FakeConfluenceServer.Stub.json(v1Page(user("a"), user("b"))));
        fake.stubOnce(path, FakeConfluenceServer.Stub.json(v1Page(user("c"))));

        List<User> users = new ArrayList<>();
        stub.listGroupMembers(ListGroupMembersRequest.newBuilder().setGroupId("g-1").build())
                .forEachRemaining(r -> users.add(r.getUser()));

        assertThat(users).extracting(User::getAccountId).containsExactly("a", "b", "c");
        assertThat(users.getFirst().getEmail()).isEqualTo("a@example.com");
        assertThat(users.getFirst().getAccountType()).isEqualTo(AccountType.ACCOUNT_TYPE_ATLASSIAN);
        assertThat(fake.requestsTo(path)).extracting(r -> r.params().get("start")).containsExactly("0", "2");
        assertThat(fake.requestsTo(path)).allSatisfy(r -> assertThat(r.params()).containsEntry("limit", "2"));
    }

    @Test
    void listUserGroupsReadsMemberOfByAccountId() {
        fake.stub("/wiki/rest/api/user/memberof", v1Page(
                "{\"type\":\"group\",\"name\":\"hr\",\"id\":\"g-hr\"}"));

        List<Group> groups = new ArrayList<>();
        stub.listUserGroups(ListUserGroupsRequest.newBuilder().setAccountId("acc-1").build())
                .forEachRemaining(r -> groups.add(r.getGroup()));

        assertThat(groups).extracting(Group::getId).containsExactly("g-hr");
        assertThat(fake.lastTo("/wiki/rest/api/user/memberof").params())
                .containsEntry("accountId", "acc-1").containsEntry("start", "0");
        assertCode(Status.Code.INVALID_ARGUMENT, () -> stub.listUserGroups(
                ListUserGroupsRequest.getDefaultInstance()).hasNext());
    }

    @Test
    void getContentAccessReadsTheSpaceAndEveryRestrictedOwnerNearestFirst() {
        fake.stub("/wiki/api/v2/pages/300", "{\"id\":\"300\",\"spaceId\":\"100\",\"title\":\"Pay bands\"}");
        fake.stub("/wiki/api/v2/spaces/100/permissions", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"sp-1\",\"principal\":{\"type\":\"group\",\"id\":\"g-staff\"},"
                        + "\"operation\":{\"key\":\"read\",\"targetType\":\"space\"}}"));
        fake.stub("/wiki/api/v2/pages/300/ancestors", ConfluenceFixtures.listJson(null, "",
                "{\"id\":\"10\",\"type\":\"page\"}", "{\"id\":\"20\",\"type\":\"folder\"}",
                "{\"id\":\"30\",\"type\":\"embed\"}"));
        fake.stub("/wiki/api/v2/pages/300/restriction-state", "{\"state\":\"OPEN\"}");
        fake.stub("/wiki/api/v2/folders/20/restriction-state", "{\"state\":\"VIEW_RESTRICTED\"}");
        fake.stub("/wiki/api/v2/folders/20/permissions", ConfluenceFixtures.listJson(null, "",
                "{\"operation\":\"read\",\"principal\":{\"principalType\":\"group\",\"principalId\":\"g-hr\"}}"));
        fake.stub("/wiki/api/v2/pages/10/restriction-state", "{\"state\":\"EDIT_RESTRICTED\"}");
        fake.stub("/wiki/api/v2/pages/10/permissions", ConfluenceFixtures.listJson(null, "",
                "{\"operation\":\"update\",\"principal\":{\"principalType\":\"user\",\"principalId\":\"acc-1\"}}"));

        GetContentAccessResponse access = stub.getContentAccess(GetContentAccessRequest.newBuilder()
                .setContent(page("300")).build());

        assertThat(access.getSpaceId()).isEqualTo("100");
        assertThat(access.getSpacePermissionsList()).singleElement()
                .satisfies(p -> assertThat(p.getPrincipal().getId()).isEqualTo("g-staff"));
        assertThat(access.getSpaceRoleAssignmentsList()).as("role-assignments 404 = not served").isEmpty();
        assertThat(access.getOwnersList()).extracting(o -> o.getContent().getId())
                .containsExactly("300", "20", "10");
        assertThat(access.getOwners(1).getContent().getType()).isEqualTo(ContentType.CONTENT_TYPE_FOLDER);
        assertThat(access.getOwners(0).getPermissionsList()).isEmpty();
        assertThat(fake.requestsTo("/wiki/api/v2/pages/300/permissions")).as("OPEN owners skip the list").isEmpty();
        assertThat(access.getOwners(1).getRestrictionState().getState())
                .isEqualTo(ContentRestrictionState.CONTENT_RESTRICTION_STATE_VIEW_RESTRICTED);
        assertThat(access.getOwners(1).getPermissions(0).getPrincipal().getPrincipalId()).isEqualTo("g-hr");
        assertThat(access.getOwners(2).getPermissions(0).getOperation())
                .isEqualTo(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_UPDATE);
        assertThat(access.getCompleteness()).isEqualTo(AclCompleteness.ACL_COMPLETENESS_COMPLETE);
    }

    @Test
    void getContentAccessFailsInsteadOfAnsweringPartialAccess() {
        fake.stub("/wiki/api/v2/blogposts/5", "{\"id\":\"5\",\"spaceId\":\"100\"}");
        fake.stub("/wiki/api/v2/spaces/100/permissions", ConfluenceFixtures.listJson(null, ""));
        fake.stub("GET", "/wiki/api/v2/blogposts/5/restriction-state", 403,
                "{\"message\":\"not allowed\"}");

        assertThatThrownBy(() -> stub.getContentAccess(GetContentAccessRequest.newBuilder()
                .setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_BLOG_POST).setId("5"))
                .build())).isInstanceOfSatisfying(StatusRuntimeException.class,
                        e -> assertThat(e.getStatus().getCode()).isNotEqualTo(Status.Code.OK));
        assertThat(fake.requestsTo("/wiki/api/v2/blogposts/5/ancestors")).as("blog posts have no tree").isEmpty();
        assertCode(Status.Code.INVALID_ARGUMENT, () -> stub.getContentAccess(GetContentAccessRequest
                .newBuilder().setContent(ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_ATTACHMENT)
                        .setId("a1")).build()));
        assertCode(Status.Code.INTERNAL, () -> {
            fake.stub("/wiki/api/v2/pages/9", "{\"id\":\"9\"}");
            stub.getContentAccess(GetContentAccessRequest.newBuilder().setContent(page("9")).build());
        });
    }

    private static void assertCode(Status.Code code, org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(StatusRuntimeException.class,
                e -> assertThat(e.getStatus().getCode()).isEqualTo(code));
    }
}
