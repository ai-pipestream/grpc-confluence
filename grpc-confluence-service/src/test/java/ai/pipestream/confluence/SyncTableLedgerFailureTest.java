package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ChangeSource;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import ai.pipestream.sync.AssetStore;
import ai.pipestream.sync.SyncTableGrpcService;
import ai.pipestream.sync.v1.Asset;
import ai.pipestream.sync.v1.AssetSyncStatus;
import ai.pipestream.sync.v1.GetAssetRequest;
import ai.pipestream.sync.v1.SyncTableServiceGrpc;
import ai.pipestream.sync.v1.UpsertAssetRequest;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A sync-table write that fails is the sink's failure, not a log line: the
 * Sync that made it ends with an error, hands out no resume cursor, and
 * does not reconcile the ledger against a run whose rows did not all land.
 */
class SyncTableLedgerFailureTest {

    /** The real ledger behind a switch that fails chosen RPCs and counts every call. */
    private static final class Ledger implements ServerInterceptor {
        final Set<String> failing = ConcurrentHashMap.newKeySet();
        final Map<String, Integer> calls = new ConcurrentHashMap<>();

        @Override
        public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call,
                Metadata headers, ServerCallHandler<Q, R> next) {
            String method = call.getMethodDescriptor().getBareMethodName();
            calls.merge(method, 1, Integer::sum);
            if (failing.contains(method)) {
                call.close(Status.UNAVAILABLE.withDescription("ledger restarting"), new Metadata());
                return new ServerCall.Listener<>() {
                };
            }
            return next.startCall(call, headers);
        }

        int calls(String method) {
            return calls.getOrDefault(method, 0);
        }
    }

    private final Ledger ledger = new Ledger();
    private Server syncServer;
    private ManagedChannel syncChannel;
    private FakeConfluenceServer fake;
    private Server confluenceServer;
    private ManagedChannel confluenceChannel;

    @BeforeEach
    void startLedger() throws Exception {
        String name = InProcessServerBuilder.generateName();
        syncServer = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(ServerInterceptors.intercept(
                        new SyncTableGrpcService(new AssetStore()), ledger))
                .build()
                .start();
        syncChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    @AfterEach
    void stop() {
        if (confluenceChannel != null) {
            confluenceChannel.shutdownNow();
        }
        if (confluenceServer != null) {
            confluenceServer.shutdownNow();
        }
        if (fake != null) {
            fake.close();
        }
        syncChannel.shutdownNow();
        syncServer.shutdownNow();
    }

    @Test
    void aFailedUpsertIsThrownAndNamesTheAsset() {
        ledger.failing.add("UpsertAsset");
        SyncTableChangeSink sink = new SyncTableChangeSink(syncChannel);

        assertThatThrownBy(() -> sink.emit(pageChange("200", ChangeOperation.CHANGE_OPERATION_UPSERT)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("sync-table")
                .hasMessageContaining("UpsertAsset")
                .hasMessageContaining("confluence:default:page:200")
                .hasMessageContaining("ledger restarting");
    }

    @Test
    void aFailedDeleteIsThrownAndNamesTheAsset() {
        ledger.failing.add("DeleteAsset");
        SyncTableChangeSink sink = new SyncTableChangeSink(syncChannel);

        assertThatThrownBy(() -> sink.emit(pageChange("200", ChangeOperation.CHANGE_OPERATION_DELETE)))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("sync-table")
                .hasMessageContaining("DeleteAsset")
                .hasMessageContaining("confluence:default:page:200");
    }

    @Test
    void aFailedCheckpointAndAFailedReconcileAreThrown() {
        ledger.failing.add("PutCheckpoint");
        ledger.failing.add("Reconcile");
        SyncTableChangeSink sink = new SyncTableChangeSink(syncChannel);

        assertThatThrownBy(() -> sink.snapshot(ConfluenceSnapshot.newBuilder()
                .setSpaceKey("ENG").setCursor("2024-03-02T00:00:00Z").build()))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("sync-table")
                .hasMessageContaining("PutCheckpoint")
                .hasMessageContaining("ENG");
        assertThatThrownBy(() -> sink.completeRun("run-1"))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("sync-table")
                .hasMessageContaining("Reconcile")
                .hasMessageContaining("run-1");
    }

    @Test
    void aFullSyncWhoseLedgerWritesFailEndsWithAnErrorAndDeletesNothing() throws Exception {
        // The page exists in Confluence and in the ledger from an earlier run.
        SyncTableServiceGrpc.SyncTableServiceBlockingStub rows =
                SyncTableServiceGrpc.newBlockingStub(syncChannel);
        rows.upsertAsset(UpsertAssetRequest.newBuilder()
                .setAsset(Asset.newBuilder()
                        .setAssetId("confluence:default:page:200")
                        .setSource("confluence")
                        .setConnectionId("default")
                        .setKind("page")
                        .setNativeId("200")
                        .setRunId("earlier-run")
                        .setTitle("Design Doc"))
                .build());
        AssetSyncStatus before = rows.getAsset(GetAssetRequest.newBuilder()
                .setAssetId("confluence:default:page:200").build()).getAsset().getStatus();
        startConfluenceWithOnePage();
        ledger.failing.add("UpsertAsset");

        List<SyncResponse> received = new ArrayList<>();
        Iterator<SyncResponse> sync = ConfluenceServiceGrpc.newBlockingStub(confluenceChannel)
                .sync(SyncRequest.getDefaultInstance());
        assertThatThrownBy(() -> sync.forEachRemaining(received::add))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.UNAVAILABLE);
                    assertThat(e.getStatus().getDescription())
                            .contains("sync-table").contains("UpsertAsset");
                });

        // No cursor is handed out for a pass the ledger did not record.
        assertThat(received).noneMatch(
                r -> r.getEventCase() == SyncResponse.EventCase.RESUME_CURSOR);
        // The ledger is not reconciled against the run: the page whose
        // upsert failed still exists and must not be marked deleted.
        assertThat(ledger.calls("Reconcile")).isZero();
        assertThat(rows.getAsset(GetAssetRequest.newBuilder()
                .setAssetId("confluence:default:page:200").build()).getAsset().getStatus())
                .isEqualTo(before)
                .isNotEqualTo(AssetSyncStatus.ASSET_SYNC_STATUS_DELETED);
    }

    @Test
    void aFailedReconcileFailsTheSync() throws Exception {
        startConfluenceWithOnePage();
        ledger.failing.add("Reconcile");

        List<SyncResponse> received = new ArrayList<>();
        Iterator<SyncResponse> sync = ConfluenceServiceGrpc.newBlockingStub(confluenceChannel)
                .sync(SyncRequest.getDefaultInstance());
        assertThatThrownBy(() -> sync.forEachRemaining(received::add))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getDescription())
                                .contains("sync-table").contains("Reconcile"));
        assertThat(received).noneMatch(
                r -> r.getEventCase() == SyncResponse.EventCase.RESUME_CURSOR);
    }

    private void startConfluenceWithOnePage() throws Exception {
        fake = FakeConfluenceServer.start();
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson("200", "100", "Design Doc",
                                "2024-03-02T00:00:00Z")));
        fake.stub("/wiki/api/v2/pages/200/footer-comments", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/200/inline-comments", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/200/attachments", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/200/labels", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/200/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());

        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("token-123")
                .build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ZERO);
        String name = InProcessServerBuilder.generateName();
        confluenceServer = InProcessServerBuilder.forName(name)
                // The service blocks on outbound flow control, as in production:
                // a direct executor would run it on the client thread and stall.
                .executor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, client,
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES,
                        new SyncTableChangeSink(syncChannel)))
                .build()
                .start();
        confluenceChannel = InProcessChannelBuilder.forName(name).directExecutor().build();
    }

    private static ConfluenceChange pageChange(String id, ChangeOperation operation) {
        return ConfluenceChange.newBuilder()
                .setChangeId("c-" + id)
                .setOperation(operation)
                .setSource(ChangeSource.CHANGE_SOURCE_CRAWL)
                .setCursor("run-1")
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId(id)
                        .setPage(Page.newBuilder().setId(id).setSpaceId("100").setTitle("Design Doc")))
                .build();
    }
}
