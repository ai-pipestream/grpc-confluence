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

    /**
     * Observer notified after each successful {@link #upsert(Asset)} or
     * {@link #delete(String, String, String)}.
     */
    public interface Watcher extends Consumer<Asset> {
    }

    private final ConcurrentHashMap<String, Asset> assets = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Checkpoint> checkpoints = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<Watcher> watchers = new CopyOnWriteArrayList<>();

    /** Creates an empty in-memory ledger. */
    public AssetStore() {
    }

    /**
     * Inserts {@code incoming} or merges it into the existing row with the same
     * {@code asset_id}.
     *
     * <p>The ledger owns {@link AssetPhase}: a first insert with unspecified phase
     * becomes {@code INITIAL_CRAWL}; a re-upsert becomes {@code UPDATE} even when
     * the crawler still labels the write {@code INITIAL_CRAWL}. {@code DELETE} and
     * {@code SNAPSHOT} stay caller-controlled. Unspecified status becomes
     * {@code SYNCED}. {@code first_seen_at} is set on insert and preserved on
     * update; {@code last_seen_at} is always now. Empty title, source URI, and
     * parent id are filled from the existing row; attributes are the existing map
     * overlaid with the incoming map.</p>
     *
     * @param incoming asset to store; must have a non-blank {@code asset_id}
     * @return the stored row after merge
     */
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

    /**
     * Looks up one asset by id.
     *
     * @param assetId ledger key
     * @return the row, or empty when unknown
     */
    public Optional<Asset> get(String assetId) {
        return Optional.ofNullable(assets.get(assetId));
    }

    /**
     * Returns assets matching the given filters, in map iteration order.
     *
     * @param source source name; empty matches any
     * @param kind asset kind; empty matches any
     * @param parentAssetId parent id; empty matches any
     * @param attachmentsOnly when {@code true}, only rows with {@code attachment} set
     * @param status required sync status; {@code UNSPECIFIED} matches any
     * @param limit max rows to return; {@code 0} or negative means no cap
     * @return a new list of matching assets
     */
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

    /**
     * Soft-deletes {@code assetId}: phase {@code DELETE}, status {@code DELETED},
     * with {@code deleted_at} and {@code last_seen_at} set to now. A missing id
     * becomes a stub row. Non-empty {@code runId} and {@code cursor} overwrite
     * those fields.
     *
     * @param assetId ledger key
     * @param runId crawl run to record; empty leaves the field unchanged
     * @param cursor source cursor to record; empty leaves the field unchanged
     * @return the deleted row
     */
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

    /**
     * Marks rows of {@code source} as deleted when they were not seen in
     * {@code runId}.
     *
     * <p>Skips rows already {@code DELETED}, rows whose phase is {@code SNAPSHOT},
     * and rows whose {@code run_id} already equals {@code runId}. Optional
     * {@code kind} restricts the scan.</p>
     *
     * @param source source name; required
     * @param runId current crawl run; required
     * @param kind asset kind; empty matches any kind
     * @return number of rows newly marked deleted
     */
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

    /**
     * Stores {@code checkpoint}, overwriting any previous value for the same
     * source and scope, and sets {@code updated_at} to now.
     *
     * @param checkpoint checkpoint to persist; {@code source} must be non-blank
     * @return the stored checkpoint including {@code updated_at}
     */
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

    /**
     * Looks up the checkpoint for {@code source} and {@code scope}.
     *
     * @param source source name
     * @param scope checkpoint scope; {@code null} is treated as empty
     * @return the checkpoint, or empty when unknown
     */
    public Optional<Checkpoint> getCheckpoint(String source, String scope) {
        return Optional.ofNullable(checkpoints.get(key(source, scope)));
    }

    /**
     * Registers {@code watcher} for subsequent upsert and delete notifications.
     *
     * @param watcher callback invoked with the stored row after each mutation
     * @return a handle whose {@link AutoCloseable#close()} unregisters the watcher
     */
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
