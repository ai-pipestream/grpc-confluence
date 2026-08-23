package ai.pipestream.sync;

import ai.pipestream.sync.v1.Asset;
import ai.pipestream.sync.v1.AssetPhase;
import ai.pipestream.sync.v1.AssetSyncStatus;
import ai.pipestream.sync.v1.GetAssetRequest;
import ai.pipestream.sync.v1.ListAssetsRequest;
import ai.pipestream.sync.v1.ListAssetsResponse;
import ai.pipestream.sync.v1.ReconcileRequest;
import ai.pipestream.sync.v1.SyncTableServiceGrpc;
import ai.pipestream.sync.v1.UpsertAssetRequest;
import ai.pipestream.sync.v1.WatchRequest;
import ai.pipestream.sync.v1.WatchResponse;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class SyncTableGrpcServiceTest {

    private Server server;
    private ManagedChannel channel;
    private SyncTableServiceGrpc.SyncTableServiceBlockingStub stub;
    private SyncTableServiceGrpc.SyncTableServiceStub async;

    @BeforeEach
    void start() throws Exception {
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new SyncTableGrpcService(new AssetStore()))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = SyncTableServiceGrpc.newBlockingStub(channel);
        async = SyncTableServiceGrpc.newStub(channel);
    }

    @AfterEach
    void stop() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    @Test
    void upsertUpdateDeleteAndReconcile() {
        Asset first = stub.upsertAsset(UpsertAssetRequest.newBuilder()
                .setAsset(Asset.newBuilder()
                        .setAssetId("confluence:page:1")
                        .setSource("confluence")
                        .setNativeId("1")
                        .setKind("page")
                        .setTitle("Design")
                        .setRunId("run-a")
                        .setAttachment(false))
                .build()).getAsset();
        assertThat(first.getPhase()).isEqualTo(AssetPhase.ASSET_PHASE_INITIAL_CRAWL);
        assertThat(first.getStatus()).isEqualTo(AssetSyncStatus.ASSET_SYNC_STATUS_SYNCED);

        Asset updated = stub.upsertAsset(UpsertAssetRequest.newBuilder()
                .setAsset(first.toBuilder().setTitle("Design v2").setRunId("run-b"))
                .build()).getAsset();
        assertThat(updated.getPhase()).isEqualTo(AssetPhase.ASSET_PHASE_UPDATE);
        assertThat(updated.getFirstSeenAt()).isEqualTo(first.getFirstSeenAt());

        stub.upsertAsset(UpsertAssetRequest.newBuilder()
                .setAsset(Asset.newBuilder()
                        .setAssetId("confluence:attachment:a1")
                        .setSource("confluence")
                        .setKind("attachment")
                        .setNativeId("a1")
                        .setAttachment(true)
                        .setParentAssetId("confluence:page:1")
                        .setRunId("run-b")
                        .setTitle("notes.txt"))
                .build());

        List<ListAssetsResponse> attachments = new ArrayList<>();
        stub.listAssets(ListAssetsRequest.newBuilder()
                .setSource("confluence")
                .setAttachmentsOnly(true)
                .build()).forEachRemaining(attachments::add);
        assertThat(attachments).extracting(r -> r.getAsset().getAssetId())
                .containsExactly("confluence:attachment:a1");

        int deleted = stub.reconcile(ReconcileRequest.newBuilder()
                .setSource("confluence")
                .setRunId("run-b")
                .build()).getDeleted();
        assertThat(deleted).isZero();

        stub.upsertAsset(UpsertAssetRequest.newBuilder()
                .setAsset(Asset.newBuilder()
                        .setAssetId("confluence:page:gone")
                        .setSource("confluence")
                        .setKind("page")
                        .setNativeId("gone")
                        .setRunId("run-old"))
                .build());
        assertThat(stub.reconcile(ReconcileRequest.newBuilder()
                .setSource("confluence")
                .setRunId("run-b")
                .build()).getDeleted()).isEqualTo(1);
        assertThat(stub.getAsset(GetAssetRequest.newBuilder()
                .setAssetId("confluence:page:gone").build()).getAsset().getStatus())
                .isEqualTo(AssetSyncStatus.ASSET_SYNC_STATUS_DELETED);
    }

    @Test
    void watchStreamsMutations() throws Exception {
        CopyOnWriteArrayList<Asset> seen = new CopyOnWriteArrayList<>();
        CountDownLatch first = new CountDownLatch(1);
        async.watch(WatchRequest.newBuilder().setIncludeSnapshot(true).build(),
                new io.grpc.stub.StreamObserver<>() {
                    @Override
                    public void onNext(WatchResponse value) {
                        seen.add(value.getAsset());
                        first.countDown();
                    }

                    @Override
                    public void onError(Throwable t) {
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
        stub.upsertAsset(UpsertAssetRequest.newBuilder()
                .setAsset(Asset.newBuilder()
                        .setAssetId("microsoft:drive_item:f1")
                        .setSource("microsoft")
                        .setKind("drive_item")
                        .setNativeId("f1")
                        .setAttachment(true)
                        .setTitle("file.txt"))
                .build());
        assertThat(first.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(seen).extracting(Asset::getAssetId).contains("microsoft:drive_item:f1");
    }
}
