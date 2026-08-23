package ai.pipestream.sync;

import ai.pipestream.sync.v1.Asset;
import ai.pipestream.sync.v1.AssetPhase;
import ai.pipestream.sync.v1.AssetSyncStatus;
import ai.pipestream.sync.v1.Checkpoint;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * In-memory ledger. Thread-safe for virtual-thread crawlers. This
 * <em>is</em> the current database of source assets.
 */
public final class AssetStore {

    public interface Watcher extends Consumer<Asset> {
    }

    private final ConcurrentHashMap<String, Asset> assets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Watcher> watchers = new CopyOnWriteArrayList<>();

    public Asset upsert(Asset incoming) {
        Objects.requireNonNull(incoming, "asset");
        if (incoming.getAssetId().isBlank()) {
            throw new IllegalArgumentException("asset_id is required");
        }
        Instant now = Instant.now();
        Asset merged = assets.compute(incoming.getAssetId(), (id, existing) -> {
            Asset.Builder next = incoming.toBuilder();
            if (existing == null) {
                if (next.getPhase() == AssetPhase.ASSET_PHASE_UNSPECIFIED) {
                    next.setPhase(AssetPhase.ASSET_PHASE_INITIAL_CRAWL);
                }
                if (next.getStatus() == AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED) {
                    next.setStatus(AssetSyncStatus.ASSET_SYNC_STATUS_SYNCED);
                }
                if (!next.hasFirstSeenAt()) {
                    next.setFirstSeenAt(timestamp(now));
                }
            } else {
                if (!existing.hasFirstSeenAt()) {
                    next.setFirstSeenAt(timestamp(now));
                } else {
                    next.setFirstSeenAt(existing.getFirstSeenAt());
                }
                // The ledger owns phase: a re-upsert is UPDATE even when a
                // crawler still labels the write INITIAL_CRAWL. DELETE and
                // SNAPSHOT stay caller-controlled.
                if (next.getPhase() != AssetPhase.ASSET_PHASE_DELETE
                        && next.getPhase() != AssetPhase.ASSET_PHASE_SNAPSHOT) {
                    next.setPhase(AssetPhase.ASSET_PHASE_UPDATE);
                }
                if (next.getStatus() == AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED) {
                    next.setStatus(AssetSyncStatus.ASSET_SYNC_STATUS_SYNCED);
                }
                if (next.getTitle().isEmpty()) {
                    next.setTitle(existing.getTitle());
                }
                if (next.getSourceUri().isEmpty()) {
                    next.setSourceUri(existing.getSourceUri());
                }
                if (next.getParentAssetId().isEmpty()) {
                    next.setParentAssetId(existing.getParentAssetId());
                }
                existing.getAttributesMap().forEach(next::putAttributes);
                incoming.getAttributesMap().forEach(next::putAttributes);
            }
            next.setLastSeenAt(timestamp(now));
            return next.build();
        });
        notify(merged);
        return merged;
    }

    public Optional<Asset> get(String assetId) {
        return Optional.ofNullable(assets.get(assetId));
    }

    public List<Asset> list(String source, String kind, String parentAssetId,
            boolean attachmentsOnly, AssetSyncStatus status, int limit) {
        List<Asset> out = new ArrayList<>();
        for (Asset asset : assets.values()) {
            if (!source.isEmpty() && !source.equals(asset.getSource())) {
                continue;
            }
            if (!kind.isEmpty() && !kind.equals(asset.getKind())) {
                continue;
            }
            if (!parentAssetId.isEmpty() && !parentAssetId.equals(asset.getParentAssetId())) {
                continue;
            }
            if (attachmentsOnly && !asset.getAttachment()) {
                continue;
            }
            if (status != AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED
                    && asset.getStatus() != status) {
                continue;
            }
            out.add(asset);
            if (limit > 0 && out.size() >= limit) {
                break;
            }
        }
        return out;
    }

    public Asset delete(String assetId, String runId, String cursor) {
        Instant now = Instant.now();
        Asset deleted = assets.compute(assetId, (id, existing) -> {
            Asset.Builder next = existing == null
                    ? Asset.newBuilder().setAssetId(assetId)
                    : existing.toBuilder();
            next.setPhase(AssetPhase.ASSET_PHASE_DELETE)
                    .setStatus(AssetSyncStatus.ASSET_SYNC_STATUS_DELETED)
                    .setDeletedAt(timestamp(now))
                    .setLastSeenAt(timestamp(now));
            if (!runId.isEmpty()) {
                next.setRunId(runId);
            }
            if (!cursor.isEmpty()) {
                next.setCursor(cursor);
            }
            return next.build();
        });
        notify(deleted);
        return deleted;
    }

    public int reconcile(String source, String runId, String kind) {
        if (source.isBlank() || runId.isBlank()) {
            throw new IllegalArgumentException("source and run_id are required");
        }
        int deleted = 0;
        for (Asset asset : List.copyOf(assets.values())) {
            if (!source.equals(asset.getSource())) {
                continue;
            }
            if (!kind.isEmpty() && !kind.equals(asset.getKind())) {
                continue;
            }
            if (asset.getStatus() == AssetSyncStatus.ASSET_SYNC_STATUS_DELETED) {
                continue;
            }
            if (asset.getPhase() == AssetPhase.ASSET_PHASE_SNAPSHOT) {
                continue;
            }
            if (runId.equals(asset.getRunId())) {
                continue;
            }
            delete(asset.getAssetId(), runId, asset.getCursor());
            deleted++;
        }
        return deleted;
    }

    public Checkpoint putCheckpoint(Checkpoint checkpoint) {
        if (checkpoint.getSource().isBlank()) {
            throw new IllegalArgumentException("checkpoint.source is required");
        }
        Checkpoint stored = checkpoint.toBuilder()
                .setUpdatedAt(timestamp(Instant.now()))
                .build();
        checkpoints.put(key(stored.getSource(), stored.getScope()), stored);
        return stored;
    }

    public Optional<Checkpoint> getCheckpoint(String source, String scope) {
        return Optional.ofNullable(checkpoints.get(key(source, scope)));
    }

    public AutoCloseable watch(Watcher watcher) {
        watchers.add(watcher);
        return () -> watchers.remove(watcher);
    }

    private void notify(Asset asset) {
        for (Watcher watcher : watchers) {
            watcher.accept(asset);
        }
    }

    private static String key(String source, String scope) {
        return source + "\0" + (scope == null ? "" : scope);
    }

    static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
