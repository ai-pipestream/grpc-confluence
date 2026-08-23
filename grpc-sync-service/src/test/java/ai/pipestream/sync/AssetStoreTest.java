package ai.pipestream.sync;

import ai.pipestream.sync.v1.Asset;
import ai.pipestream.sync.v1.AssetPhase;
import ai.pipestream.sync.v1.AssetSyncStatus;
import ai.pipestream.sync.v1.Checkpoint;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AssetStoreTest {

    @Test
    void reupsertIsUpdateAndKeepsFirstSeen() {
        AssetStore store = new AssetStore();
        Asset first = store.upsert(Asset.newBuilder()
                .setAssetId("confluence:page:1")
                .setSource("confluence")
                .setKind("page")
                .setNativeId("1")
                .setTitle("Design")
                .setPhase(AssetPhase.ASSET_PHASE_INITIAL_CRAWL)
                .build());
        Asset second = store.upsert(first.toBuilder()
                .setTitle("Design v2")
                .setPhase(AssetPhase.ASSET_PHASE_INITIAL_CRAWL)
                .build());
        assertThat(second.getPhase()).isEqualTo(AssetPhase.ASSET_PHASE_UPDATE);
        assertThat(second.getFirstSeenAt()).isEqualTo(first.getFirstSeenAt());
        assertThat(second.getTitle()).isEqualTo("Design v2");
    }

    @Test
    void filtersAttachmentsParentAndStatus() {
        AssetStore store = new AssetStore();
        store.upsert(Asset.newBuilder()
                .setAssetId("confluence:page:1")
                .setSource("confluence")
                .setKind("page")
                .setNativeId("1")
                .build());
        store.upsert(Asset.newBuilder()
                .setAssetId("confluence:attachment:a1")
                .setSource("confluence")
                .setKind("attachment")
                .setNativeId("a1")
                .setAttachment(true)
                .setParentAssetId("confluence:page:1")
                .build());
        store.upsert(Asset.newBuilder()
                .setAssetId("microsoft:drive_item:f1")
                .setSource("microsoft")
                .setKind("drive_item")
                .setNativeId("f1")
                .setAttachment(true)
                .build());

        assertThat(store.list("confluence", "", "", true,
                AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED, 0))
                .extracting(Asset::getAssetId)
                .containsExactly("confluence:attachment:a1");
        assertThat(store.list("", "", "confluence:page:1", false,
                AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED, 0))
                .extracting(Asset::getAssetId)
                .containsExactly("confluence:attachment:a1");
        assertThat(store.list("microsoft", "drive_item", "", false,
                AssetSyncStatus.ASSET_SYNC_STATUS_SYNCED, 1))
                .hasSize(1);
    }

    @Test
    void deleteIsSoftAndReconcileSkipsDeletedAndSnapshots() {
        AssetStore store = new AssetStore();
        store.upsert(Asset.newBuilder()
                .setAssetId("confluence:page:keep")
                .setSource("confluence")
                .setKind("page")
                .setNativeId("keep")
                .setRunId("run-b")
                .build());
        store.upsert(Asset.newBuilder()
                .setAssetId("confluence:page:old")
                .setSource("confluence")
                .setKind("page")
                .setNativeId("old")
                .setRunId("run-a")
                .build());
        store.upsert(Asset.newBuilder()
                .setAssetId("confluence:snapshot:run-b")
                .setSource("confluence")
                .setKind("marker")
                .setNativeId("run-b")
                .setPhase(AssetPhase.ASSET_PHASE_SNAPSHOT)
                .setRunId("run-a")
                .build());
        store.delete("confluence:page:already", "run-b", "c");
        assertThat(store.reconcile("confluence", "run-b", "page")).isEqualTo(1);
        assertThat(store.get("confluence:page:old").orElseThrow().getStatus())
                .isEqualTo(AssetSyncStatus.ASSET_SYNC_STATUS_DELETED);
        assertThat(store.get("confluence:snapshot:run-b").orElseThrow().getPhase())
                .isEqualTo(AssetPhase.ASSET_PHASE_SNAPSHOT);
        assertThat(store.get("confluence:page:keep").orElseThrow().getStatus())
                .isEqualTo(AssetSyncStatus.ASSET_SYNC_STATUS_SYNCED);
    }

    @Test
    void checkpointsRoundTripPerScope() {
        AssetStore store = new AssetStore();
        store.putCheckpoint(Checkpoint.newBuilder()
                .setSource("confluence")
                .setScope("ENG")
                .setCursor("2024-03-02T00:00:00Z")
                .build());
        store.putCheckpoint(Checkpoint.newBuilder()
                .setSource("confluence")
                .setScope("DOCS")
                .setCursor("later")
                .build());
        assertThat(store.getCheckpoint("confluence", "ENG").orElseThrow().getCursor())
                .isEqualTo("2024-03-02T00:00:00Z");
        assertThat(store.getCheckpoint("confluence", "DOCS").orElseThrow().getCursor())
                .isEqualTo("later");
        assertThat(store.getCheckpoint("microsoft", "ENG")).isEmpty();
    }

    @Test
    void rejectsBlankIds() {
        AssetStore store = new AssetStore();
        assertThatThrownBy(() -> store.upsert(Asset.getDefaultInstance()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("asset_id");
        assertThatThrownBy(() -> store.reconcile("", "run", ""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.putCheckpoint(Checkpoint.getDefaultInstance()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void virtualThreadsCanUpsertConcurrently() throws Exception {
        AssetStore store = new AssetStore();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 80; i++) {
                int n = i;
                futures.add(executor.submit(() -> store.upsert(Asset.newBuilder()
                        .setAssetId("microsoft:drive_item:" + n)
                        .setSource("microsoft")
                        .setKind("drive_item")
                        .setNativeId(String.valueOf(n))
                        .setAttachment(n % 2 == 0)
                        .build())));
            }
            for (Future<?> future : futures) {
                future.get();
            }
        }
        assertThat(store.list("microsoft", "", "", false,
                AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED, 0)).hasSize(80);
        assertThat(store.list("microsoft", "", "", true,
                AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED, 0)).hasSize(40);
    }
}
