package ai.pipestream.sync;

import ai.pipestream.sync.v1.Asset;
import ai.pipestream.sync.v1.AssetSyncStatus;
import ai.pipestream.sync.v1.Checkpoint;
import ai.pipestream.sync.v1.Connection;
import ai.pipestream.sync.v1.ConnectionKind;
import ai.pipestream.sync.v1.RuntimeSettings;

import java.util.List;
import java.util.Optional;

/**
 * Durable (or in-memory) catalog + asset ledger. {@link AssetStore} is the
 * in-process implementation; {@link JdbcLedger} is the SQLite one a
 * frontend will sit on. Secrets stay in the store; gRPC redacts them.
 */
public interface Ledger extends AutoCloseable {

    /**
     * Inserts or merges one asset row. Same phase rules as
     * {@link AssetStore#upsert(Asset)}.
     *
     * @param incoming row to store
     * @return the stored row
     */
    Asset upsert(Asset incoming);

    /**
     * Looks up one asset.
     *
     * @param assetId ledger key
     * @return the row, or empty
     */
    Optional<Asset> get(String assetId);

    /**
     * Lists assets matching the filters.
     *
     * @param source source family; empty matches any
     * @param kind entity kind; empty matches any
     * @param parentAssetId parent id; empty matches any
     * @param attachmentsOnly when true, only attachment rows
     * @param status required status; {@code UNSPECIFIED} matches any
     * @param limit max rows; {@code 0} means no cap
     * @param connectionId catalog connection; empty matches any
     * @return matching rows
     */
    List<Asset> list(String source, String kind, String parentAssetId, boolean attachmentsOnly,
            AssetSyncStatus status, int limit, String connectionId);

    /**
     * Soft-deletes one asset.
     *
     * @param assetId ledger key
     * @param runId crawl run; empty leaves the field
     * @param cursor source cursor; empty leaves the field
     * @return the deleted row
     */
    Asset delete(String assetId, String runId, String cursor);

    /**
     * Marks unseen rows deleted for a source (and optional connection).
     *
     * @param source source family; required
     * @param runId current run; required
     * @param kind optional kind filter
     * @param connectionId optional connection scope
     * @return rows newly marked deleted
     */
    int reconcile(String source, String runId, String kind, String connectionId);

    /**
     * Stores a checkpoint.
     *
     * @param checkpoint row to store
     * @return the stored checkpoint
     */
    Checkpoint putCheckpoint(Checkpoint checkpoint);

    /**
     * Reads a checkpoint.
     *
     * @param source source family
     * @param scope scope within the source
     * @param connectionId catalog connection; empty is the default slot
     * @return the checkpoint, or empty
     */
    Optional<Checkpoint> getCheckpoint(String source, String scope, String connectionId);

    /**
     * Registers a mutation watcher.
     *
     * @param watcher callback
     * @return handle that unregisters on close
     */
    AutoCloseable watch(AssetStore.Watcher watcher);

    /**
     * Creates or replaces a catalog row. Empty {@code token} /
     * {@code client_secret} on an update leave the stored secrets in place.
     *
     * @param incoming row to store
     * @param creating when true, reject a duplicate id
     * @return the stored row including secrets
     */
    Connection putConnection(Connection incoming, boolean creating);

    /**
     * Looks up one connection, including secrets.
     *
     * @param connectionId catalog key
     * @return the row, or empty
     */
    Optional<Connection> getConnection(String connectionId);

    /**
     * Lists catalog rows, including secrets (callers redact).
     *
     * @param kind kind filter; {@code UNSPECIFIED} matches any
     * @return matching rows in insertion order
     */
    List<Connection> listConnections(ConnectionKind kind);

    /**
     * Deletes a catalog row. Asset rows stay.
     *
     * @param connectionId catalog key
     * @return true when a row was removed
     */
    boolean deleteConnection(String connectionId);

    /**
     * Records a probe against an existing connection.
     *
     * @param connectionId catalog key
     * @param ok whether the probe succeeded
     * @param errorMessage detail when {@code ok} is false
     * @return the updated row
     */
    Connection recordProbe(String connectionId, boolean ok, String errorMessage);

    /**
     * Reads process settings. Never empty: missing rows are defaults.
     *
     * @return the stored settings
     */
    RuntimeSettings getSettings();

    /**
     * Merges process settings. Empty fields leave stored values.
     *
     * @param incoming patch
     * @return the stored settings
     */
    RuntimeSettings putSettings(RuntimeSettings incoming);

    /**
     * Releases store resources. Memory stores are a no-op.
     */
    @Override
    default void close() {
    }
}
