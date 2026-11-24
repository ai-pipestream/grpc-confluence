package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ChangeSource;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEvent;
import ai.pipestream.confluence.v1.ConfluenceEventAction;
import ai.pipestream.confluence.v1.ConfluenceEventObject;
import ai.pipestream.confluence.v1.GroupManagedBy;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.WatchChangesResponse;
import ai.pipestream.sync.v1.AclCompleteness;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Group events of the Forge queue resolved into group records, against the fake Confluence server. */
class ConfluenceChangeGroupEventTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String GROUP_ID = "e0e5b93d-f259-470c-83fd-e461f7fc1905";
    private static final String BY_ID = "/wiki/rest/api/group/by-id";
    private static final String MEMBERS = "/wiki/rest/api/group/" + GROUP_ID + "/membersByGroupId";

    private FakeConfluenceServer fake;
    private ConfluenceChangeGrpcService service;
    private final List<WatchChangesResponse> sent = new ArrayList<>();
    private ConfluenceChangeGrpcService.Emitter out;

    @BeforeEach
    void start() throws Exception {
        fake = FakeConfluenceServer.start();
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder().baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai").apiToken("token-123").pageSize(2).build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ZERO);
        service = new ConfluenceChangeGrpcService(new ConfluenceGrpcService(config, client,
                ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES), null, Clock.systemUTC());
        out = new ConfluenceChangeGrpcService.Emitter(new StreamObserver<>() {
            @Override
            public void onNext(WatchChangesResponse value) {
                sent.add(value);
            }

            @Override
            public void onError(Throwable t) {
                throw new AssertionError(t);
            }

            @Override
            public void onCompleted() {
                // Not ended by these tests.
            }
        }, "");
    }

    @AfterEach
    void stop() {
        fake.close();
    }

    /** A group event as Forge delivers it (shape of a live recording). */
    private static ConfluenceEvent groupEvent(String action) throws Exception {
        return ForgeEventQueue.event("evt:001795489375692:k", JSON.readTree("""
                {"eventCreatedDate":"2026-11-24T03:02:55.083Z",
                 "eventType":"avi:confluence:%s:group",
                 "group":{"id":"%s","name":"release-managers",
                          "resourceAri":"ari:cloud:identity::group/%s","type":"group"},
                 "selfGenerated":false}""".formatted(action, GROUP_ID, GROUP_ID)));
    }

    private static String member(String id) {
        return "{\"type\":\"known\",\"accountId\":\"" + id + "\",\"accountType\":\"atlassian\","
                + "\"publicName\":\"" + id + "\",\"displayName\":\"" + id + "\"}";
    }

    private List<ConfluenceChange> changes() {
        return sent.stream().filter(WatchChangesResponse::hasChange)
                .map(WatchChangesResponse::getChange).toList();
    }

    @Test
    void theQueueMapsAGroupEventToTheGroupIdAndName() throws Exception {
        ConfluenceEvent event = groupEvent("created");

        assertThat(event.getObject()).isEqualTo(ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_GROUP);
        assertThat(event.getAction()).isEqualTo(ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED);
        assertThat(event.getContentId()).isEqualTo(GROUP_ID);
        assertThat(event.getTitle()).isEqualTo("release-managers");
    }

    @Test
    void aCreatedGroupIsReReadWithItsMembers() throws Exception {
        fake.stub(BY_ID, """
                {"id":"%s","name":"release-managers","managedBy":"ADMINS",
                 "usageType":"USERBASE_GROUP","type":"group"}""".formatted(GROUP_ID));
        fake.stub(MEMBERS, "{\"results\":[" + member("acc-1") + "],\"start\":0,\"limit\":2,\"size\":1}");
        ConfluenceEvent event = groupEvent("created");

        service.resolve(SyncRequest.getDefaultInstance(), event, out);

        assertThat(sent).allMatch(WatchChangesResponse::hasChange);
        List<ConfluenceChange> changes = changes();
        assertThat(changes).extracting(c -> c.getEntity().getEntityId())
                .containsExactly("group:" + GROUP_ID + "/member/acc-1", GROUP_ID);
        assertThat(changes).allSatisfy(change -> {
            assertThat(change.getOperation()).isEqualTo(ChangeOperation.CHANGE_OPERATION_UPSERT);
            assertThat(change.getSource()).isEqualTo(ChangeSource.CHANGE_SOURCE_WEBHOOK);
            assertThat(change.getEvent()).isEqualTo(event);
            assertThat(change.getCursor()).isEqualTo(event.getEventId());
        });
        assertThat(changes.getFirst().getEntity().getGroupMember().getGroupId()).isEqualTo(GROUP_ID);
        assertThat(changes.getFirst().getEntity().getGroupMember().getUser().getAccountId())
                .isEqualTo("acc-1");
        assertThat(changes.getLast().getEntity().getGroup().getName()).isEqualTo("release-managers");
        assertThat(changes.getLast().getEntity().getGroup().getManagedBy())
                .isEqualTo(GroupManagedBy.GROUP_MANAGED_BY_ADMINS);
        assertThat(changes.getLast().getEntity().getSecurityCompleteness())
                .isEqualTo(AclCompleteness.ACL_COMPLETENESS_COMPLETE);
        assertThat(fake.lastTo(BY_ID).params()).containsEntry("id", GROUP_ID);
    }

    @Test
    void aGroupWhoseMembersAreRefusedIsEmittedUnavailable() throws Exception {
        fake.stub(BY_ID, "{\"id\":\"" + GROUP_ID + "\",\"name\":\"release-managers\",\"type\":\"group\"}");
        fake.stub(MEMBERS, new FakeConfluenceServer.Stub(403, "{\"message\":\"no\"}", java.util.Map.of()));

        service.resolve(SyncRequest.getDefaultInstance(), groupEvent("created"), out);

        assertThat(changes()).singleElement().satisfies(change -> {
            assertThat(change.getEntity().hasGroup()).isTrue();
            assertThat(change.getEntity().getSecurityCompleteness())
                    .isEqualTo(AclCompleteness.ACL_COMPLETENESS_UNAVAILABLE);
        });
    }

    @Test
    void aDeletedGroupIsADeleteWithoutAnyRead() throws Exception {
        ConfluenceEvent event = groupEvent("deleted");

        service.resolve(SyncRequest.getDefaultInstance(), event, out);

        assertThat(changes()).singleElement().satisfies(change -> {
            assertThat(change.getOperation()).isEqualTo(ChangeOperation.CHANGE_OPERATION_DELETE);
            assertThat(change.getEntity().getEntityId()).isEqualTo(GROUP_ID);
            assertThat(change.getEntity().getGroup().getId()).isEqualTo(GROUP_ID);
            assertThat(change.getEntity().getGroup().getName()).isEqualTo("release-managers");
            assertThat(change.getEvent()).isEqualTo(event);
            assertThat(change.getSource()).isEqualTo(ChangeSource.CHANGE_SOURCE_WEBHOOK);
        });
        assertThat(fake.requests()).isEmpty();
    }

    @Test
    void aCreatedGroupThatIsAlreadyGoneIsADelete() throws Exception {
        for (int status : List.of(400, 404)) {
            sent.clear();
            fake.stub(BY_ID, new FakeConfluenceServer.Stub(status, "{\"statusCode\":" + status + "}",
                    java.util.Map.of()));

            service.resolve(SyncRequest.getDefaultInstance(), groupEvent("created"), out);

            assertThat(changes()).as("HTTP %s", status).singleElement().satisfies(change -> {
                assertThat(change.getOperation()).isEqualTo(ChangeOperation.CHANGE_OPERATION_DELETE);
                assertThat(change.getEntity().getGroup().getId()).isEqualTo(GROUP_ID);
            });
        }
    }

    @Test
    void aRefusedOrFailedGroupReadLeavesTheEventUnresolved() throws Exception {
        for (int status : List.of(403, 500)) {
            fake.stub(BY_ID, new FakeConfluenceServer.Stub(status, "{}", java.util.Map.of()));
            ConfluenceEvent event = groupEvent("created");

            assertThatThrownBy(() -> service.resolve(SyncRequest.getDefaultInstance(), event, out))
                    .as("HTTP %s", status)
                    .isInstanceOfSatisfying(ConfluenceClient.ConfluenceApiException.class,
                            e -> assertThat(e.status()).isEqualTo(status));
        }
        assertThat(sent).isEmpty();
    }

    @Test
    void aGroupEventWithoutAnIdStaysUnresolved() throws Exception {
        ConfluenceEvent event = ForgeEventQueue.event("evt:1:k", JSON.readTree(
                "{\"eventType\":\"avi:confluence:created:group\",\"group\":{\"name\":\"x\"}}"));

        service.resolve(SyncRequest.getDefaultInstance(), event, out);

        assertThat(sent).singleElement().satisfies(message ->
                assertThat(message.getUnresolvedEvent()).isEqualTo(event));
    }
}
