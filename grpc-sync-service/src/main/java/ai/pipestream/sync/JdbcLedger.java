package ai.pipestream.sync;

import ai.pipestream.sync.v1.Asset;
import ai.pipestream.sync.v1.AssetPhase;
import ai.pipestream.sync.v1.AssetSyncStatus;
import ai.pipestream.sync.v1.Checkpoint;
import ai.pipestream.sync.v1.Connection;
import ai.pipestream.sync.v1.ConnectionKind;
import ai.pipestream.sync.v1.ConnectionStatus;
import com.google.protobuf.InvalidProtocolBufferException;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SQLite-backed {@link Ledger}. Callers (Confluence, MCP, a future UI) never
 * touch this class; they call {@code ConnectionService} / {@code SyncTableService}
 * over gRPC. Only the sync-table process opens the file.
 */
public final class JdbcLedger implements Ledger {

    private final java.sql.Connection db;
    private final ReentrantLock lock = new ReentrantLock();
    private final CopyOnWriteArrayList<AssetStore.Watcher> watchers = new CopyOnWriteArrayList<>();

    private JdbcLedger(java.sql.Connection db) {
        this.db = db;
    }

    /**
     * Opens {@code jdbcUrl} (SQLite) and creates tables if needed.
     *
     * @param jdbcUrl {@code jdbc:sqlite:...}
     * @return an open ledger
     * @throws SQLException if the driver cannot open the file
     */
    public static JdbcLedger open(String jdbcUrl) throws SQLException {
        Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        java.sql.Connection db = java.sql.DriverManager.getConnection(jdbcUrl);
        db.setAutoCommit(true);
        try (Statement statement = db.createStatement()) {
            statement.execute("PRAGMA journal_mode=WAL");
            statement.execute("PRAGMA busy_timeout=5000");
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS assets (
                      asset_id TEXT PRIMARY KEY,
                      connection_id TEXT NOT NULL DEFAULT '',
                      source TEXT NOT NULL DEFAULT '',
                      kind TEXT NOT NULL DEFAULT '',
                      parent_asset_id TEXT NOT NULL DEFAULT '',
                      attachment INTEGER NOT NULL DEFAULT 0,
                      status INTEGER NOT NULL DEFAULT 0,
                      run_id TEXT NOT NULL DEFAULT '',
                      payload BLOB NOT NULL
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS checkpoints (
                      source TEXT NOT NULL,
                      connection_id TEXT NOT NULL DEFAULT '',
                      scope TEXT NOT NULL DEFAULT '',
                      payload BLOB NOT NULL,
                      PRIMARY KEY (source, connection_id, scope)
                    )
                    """);
            statement.execute("""
                    CREATE TABLE IF NOT EXISTS connections (
                      connection_id TEXT PRIMARY KEY,
                      kind INTEGER NOT NULL DEFAULT 0,
                      payload BLOB NOT NULL
                    )
                    """);
        }
        return new JdbcLedger(db);
    }

    @Override
    public Asset upsert(Asset incoming) {
        Objects.requireNonNull(incoming, "asset");
        if (incoming.getAssetId().isBlank()) {
            throw new IllegalArgumentException("asset_id is required");
        }
        lock.lock();
        try {
            Asset existing = loadAsset(incoming.getAssetId()).orElse(null);
            Asset merged = mergeAsset(incoming, existing, Instant.now());
            saveAsset(merged);
            notify(merged);
            return merged;
        } catch (SQLException e) {
            throw new IllegalStateException("ledger upsert failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Asset> get(String assetId) {
        lock.lock();
        try {
            return loadAsset(assetId);
        } catch (SQLException e) {
            throw new IllegalStateException("ledger get failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<Asset> list(String source, String kind, String parentAssetId, boolean attachmentsOnly,
            AssetSyncStatus status, int limit, String connectionId) {
        lock.lock();
        try {
            StringBuilder sql = new StringBuilder("SELECT payload FROM assets WHERE 1=1");
            List<Object> params = new ArrayList<>();
            if (source != null && !source.isEmpty()) {
                sql.append(" AND source = ?");
                params.add(source);
            }
            if (kind != null && !kind.isEmpty()) {
                sql.append(" AND kind = ?");
                params.add(kind);
            }
            if (parentAssetId != null && !parentAssetId.isEmpty()) {
                sql.append(" AND parent_asset_id = ?");
                params.add(parentAssetId);
            }
            if (connectionId != null && !connectionId.isEmpty()) {
                sql.append(" AND connection_id = ?");
                params.add(connectionId);
            }
            if (attachmentsOnly) {
                sql.append(" AND attachment = 1");
            }
            if (status != null && status != AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED) {
                sql.append(" AND status = ?");
                params.add(status.getNumber());
            }
            if (limit > 0) {
                sql.append(" LIMIT ?");
                params.add(limit);
            }
            List<Asset> out = new ArrayList<>();
            try (PreparedStatement statement = db.prepareStatement(sql.toString())) {
                bind(statement, params);
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        out.add(Asset.parseFrom(rows.getBytes(1)));
                    }
                }
            }
            return out;
        } catch (SQLException | InvalidProtocolBufferException e) {
            throw new IllegalStateException("ledger list failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Asset delete(String assetId, String runId, String cursor) {
        lock.lock();
        try {
            Instant now = Instant.now();
            Asset existing = loadAsset(assetId).orElse(null);
            Asset.Builder next = existing == null
                    ? Asset.newBuilder().setAssetId(assetId)
                    : existing.toBuilder();
            next.setPhase(AssetPhase.ASSET_PHASE_DELETE)
                    .setStatus(AssetSyncStatus.ASSET_SYNC_STATUS_DELETED)
                    .setDeletedAt(AssetStore.timestamp(now))
                    .setLastSeenAt(AssetStore.timestamp(now));
            if (runId != null && !runId.isEmpty()) {
                next.setRunId(runId);
            }
            if (cursor != null && !cursor.isEmpty()) {
                next.setCursor(cursor);
            }
            Asset deleted = next.build();
            saveAsset(deleted);
            notify(deleted);
            return deleted;
        } catch (SQLException e) {
            throw new IllegalStateException("ledger delete failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int reconcile(String source, String runId, String kind, String connectionId) {
        if (source == null || source.isBlank() || runId == null || runId.isBlank()) {
            throw new IllegalArgumentException("source and run_id are required");
        }
        List<Asset> candidates = list(source, kind == null ? "" : kind, "", false,
                AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED, 0,
                connectionId == null ? "" : connectionId);
        int deleted = 0;
        for (Asset asset : candidates) {
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

    @Override
    public Checkpoint putCheckpoint(Checkpoint checkpoint) {
        if (checkpoint.getSource().isBlank()) {
            throw new IllegalArgumentException("checkpoint.source is required");
        }
        lock.lock();
        try {
            Checkpoint stored = checkpoint.toBuilder()
                    .setUpdatedAt(AssetStore.timestamp(Instant.now()))
                    .build();
            try (PreparedStatement statement = db.prepareStatement("""
                    INSERT INTO checkpoints(source, connection_id, scope, payload)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(source, connection_id, scope) DO UPDATE SET payload = excluded.payload
                    """)) {
                statement.setString(1, stored.getSource());
                statement.setString(2, stored.getConnectionId());
                statement.setString(3, stored.getScope());
                statement.setBytes(4, stored.toByteArray());
                statement.executeUpdate();
            }
            return stored;
        } catch (SQLException e) {
            throw new IllegalStateException("ledger checkpoint put failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Checkpoint> getCheckpoint(String source, String scope, String connectionId) {
        lock.lock();
        try {
            try (PreparedStatement statement = db.prepareStatement("""
                    SELECT payload FROM checkpoints
                    WHERE source = ? AND connection_id = ? AND scope = ?
                    """)) {
                statement.setString(1, source);
                statement.setString(2, connectionId == null ? "" : connectionId);
                statement.setString(3, scope == null ? "" : scope);
                try (ResultSet rows = statement.executeQuery()) {
                    if (!rows.next()) {
                        return Optional.empty();
                    }
                    return Optional.of(Checkpoint.parseFrom(rows.getBytes(1)));
                }
            }
        } catch (SQLException | InvalidProtocolBufferException e) {
            throw new IllegalStateException("ledger checkpoint get failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public AutoCloseable watch(AssetStore.Watcher watcher) {
        watchers.add(watcher);
        return () -> watchers.remove(watcher);
    }

    @Override
    public Connection putConnection(Connection incoming, boolean creating) {
        Objects.requireNonNull(incoming, "connection");
        if (incoming.getConnectionId().isBlank()) {
            throw new IllegalArgumentException("connection_id is required");
        }
        if (incoming.getKind() == ConnectionKind.CONNECTION_KIND_UNSPECIFIED) {
            throw new IllegalArgumentException("connection.kind is required");
        }
        lock.lock();
        try {
            Optional<Connection> existing = loadConnection(incoming.getConnectionId());
            if (creating && existing.isPresent()) {
                throw new IllegalStateException("connection already exists: "
                        + incoming.getConnectionId());
            }
            if (!creating && existing.isEmpty()) {
                throw new IllegalStateException("connection not found: "
                        + incoming.getConnectionId());
            }
            Connection stored = mergeConnection(incoming, existing.orElse(null), Instant.now());
            saveConnection(stored);
            return stored;
        } catch (SQLException e) {
            throw new IllegalStateException("ledger connection put failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Connection> getConnection(String connectionId) {
        lock.lock();
        try {
            return loadConnection(connectionId);
        } catch (SQLException e) {
            throw new IllegalStateException("ledger connection get failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<Connection> listConnections(ConnectionKind kind) {
        lock.lock();
        try {
            String sql = kind == null || kind == ConnectionKind.CONNECTION_KIND_UNSPECIFIED
                    ? "SELECT payload FROM connections ORDER BY connection_id"
                    : "SELECT payload FROM connections WHERE kind = ? ORDER BY connection_id";
            List<Connection> out = new ArrayList<>();
            try (PreparedStatement statement = db.prepareStatement(sql)) {
                if (kind != null && kind != ConnectionKind.CONNECTION_KIND_UNSPECIFIED) {
                    statement.setInt(1, kind.getNumber());
                }
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        out.add(Connection.parseFrom(rows.getBytes(1)));
                    }
                }
            }
            return out;
        } catch (SQLException | InvalidProtocolBufferException e) {
            throw new IllegalStateException("ledger connection list failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean deleteConnection(String connectionId) {
        lock.lock();
        try {
            try (PreparedStatement statement = db.prepareStatement(
                    "DELETE FROM connections WHERE connection_id = ?")) {
                statement.setString(1, connectionId);
                return statement.executeUpdate() > 0;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("ledger connection delete failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Connection recordProbe(String connectionId, boolean ok, String errorMessage) {
        lock.lock();
        try {
            Connection existing = loadConnection(connectionId).orElseThrow(
                    () -> new IllegalStateException("connection not found: " + connectionId));
            Instant now = Instant.now();
            Connection stored = existing.toBuilder()
                    .setStatus(ok ? ConnectionStatus.CONNECTION_STATUS_READY
                            : ConnectionStatus.CONNECTION_STATUS_ERROR)
                    .setLastError(ok ? "" : Objects.toString(errorMessage, ""))
                    .setLastTestedAt(AssetStore.timestamp(now))
                    .setUpdatedAt(AssetStore.timestamp(now))
                    .build();
            saveConnection(stored);
            return stored;
        } catch (SQLException e) {
            throw new IllegalStateException("ledger probe failed", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            db.close();
        } catch (SQLException e) {
            throw new IllegalStateException("ledger close failed", e);
        } finally {
            lock.unlock();
        }
    }

    private Optional<Asset> loadAsset(String assetId) throws SQLException {
        try (PreparedStatement statement = db.prepareStatement(
                "SELECT payload FROM assets WHERE asset_id = ?")) {
            statement.setString(1, assetId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return Optional.empty();
                }
                return Optional.of(Asset.parseFrom(rows.getBytes(1)));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException("corrupt asset row " + assetId, e);
            }
        }
    }

    private void saveAsset(Asset asset) throws SQLException {
        try (PreparedStatement statement = db.prepareStatement("""
                INSERT INTO assets(asset_id, connection_id, source, kind, parent_asset_id,
                  attachment, status, run_id, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(asset_id) DO UPDATE SET
                  connection_id = excluded.connection_id,
                  source = excluded.source,
                  kind = excluded.kind,
                  parent_asset_id = excluded.parent_asset_id,
                  attachment = excluded.attachment,
                  status = excluded.status,
                  run_id = excluded.run_id,
                  payload = excluded.payload
                """)) {
            statement.setString(1, asset.getAssetId());
            statement.setString(2, asset.getConnectionId());
            statement.setString(3, asset.getSource());
            statement.setString(4, asset.getKind());
            statement.setString(5, asset.getParentAssetId());
            statement.setInt(6, asset.getAttachment() ? 1 : 0);
            statement.setInt(7, asset.getStatusValue());
            statement.setString(8, asset.getRunId());
            statement.setBytes(9, asset.toByteArray());
            statement.executeUpdate();
        }
    }

    private Optional<Connection> loadConnection(String connectionId) throws SQLException {
        try (PreparedStatement statement = db.prepareStatement(
                "SELECT payload FROM connections WHERE connection_id = ?")) {
            statement.setString(1, connectionId);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    return Optional.empty();
                }
                return Optional.of(Connection.parseFrom(rows.getBytes(1)));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException("corrupt connection row " + connectionId, e);
            }
        }
    }

    private void saveConnection(Connection connection) throws SQLException {
        try (PreparedStatement statement = db.prepareStatement("""
                INSERT INTO connections(connection_id, kind, payload)
                VALUES (?, ?, ?)
                ON CONFLICT(connection_id) DO UPDATE SET
                  kind = excluded.kind,
                  payload = excluded.payload
                """)) {
            statement.setString(1, connection.getConnectionId());
            statement.setInt(2, connection.getKindValue());
            statement.setBytes(3, connection.toByteArray());
            statement.executeUpdate();
        }
    }

    private void notify(Asset asset) {
        for (AssetStore.Watcher watcher : watchers) {
            watcher.accept(asset);
        }
    }

    private static void bind(PreparedStatement statement, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            Object value = params.get(i);
            if (value instanceof Integer n) {
                statement.setInt(i + 1, n);
            } else {
                statement.setString(i + 1, String.valueOf(value));
            }
        }
    }

    static Asset mergeAsset(Asset incoming, Asset existing, Instant now) {
        Asset.Builder next = incoming.toBuilder();
        if (existing == null) {
            if (next.getPhase() == AssetPhase.ASSET_PHASE_UNSPECIFIED) {
                next.setPhase(AssetPhase.ASSET_PHASE_INITIAL_CRAWL);
            }
            if (next.getStatus() == AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED) {
                next.setStatus(AssetSyncStatus.ASSET_SYNC_STATUS_SYNCED);
            }
            if (!next.hasFirstSeenAt()) {
                next.setFirstSeenAt(AssetStore.timestamp(now));
            }
        } else {
            if (!existing.hasFirstSeenAt()) {
                next.setFirstSeenAt(AssetStore.timestamp(now));
            } else {
                next.setFirstSeenAt(existing.getFirstSeenAt());
            }
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
            if (next.getConnectionId().isEmpty()) {
                next.setConnectionId(existing.getConnectionId());
            }
            existing.getAttributesMap().forEach(next::putAttributes);
            incoming.getAttributesMap().forEach(next::putAttributes);
        }
        next.setLastSeenAt(AssetStore.timestamp(now));
        return next.build();
    }

    static Connection mergeConnection(Connection incoming, Connection existing, Instant now) {
        Connection.Builder next = incoming.toBuilder();
        if (existing != null) {
            if (next.getToken().isEmpty()) {
                next.setToken(existing.getToken());
            }
            if (next.getClientSecret().isEmpty()) {
                next.setClientSecret(existing.getClientSecret());
            }
            if (next.getDisplayName().isEmpty()) {
                next.setDisplayName(existing.getDisplayName());
            }
            if (next.getBaseUrl().isEmpty()) {
                next.setBaseUrl(existing.getBaseUrl());
            }
            if (next.getEmail().isEmpty()) {
                next.setEmail(existing.getEmail());
            }
            if (next.getTenantId().isEmpty()) {
                next.setTenantId(existing.getTenantId());
            }
            if (next.getClientId().isEmpty()) {
                next.setClientId(existing.getClientId());
            }
            if (next.getSiteId().isEmpty()) {
                next.setSiteId(existing.getSiteId());
            }
            if (next.getSpaceKeysList().isEmpty()) {
                next.addAllSpaceKeys(existing.getSpaceKeysList());
            }
            if (next.getDriveIdsList().isEmpty()) {
                next.addAllDriveIds(existing.getDriveIdsList());
            }
            if (!next.hasOutput() || (next.getOutput().getStore().isEmpty()
                    && next.getOutput().getFormatsList().isEmpty()
                    && next.getOutput().getDirectory().isEmpty()
                    && next.getOutput().getS3Bucket().isEmpty())) {
                next.setOutput(existing.getOutput());
            }
            if (existing.hasCreatedAt()) {
                next.setCreatedAt(existing.getCreatedAt());
            }
            if (!next.hasLastTestedAt() && existing.hasLastTestedAt()) {
                next.setLastTestedAt(existing.getLastTestedAt());
            }
            if (next.getStatus() == ConnectionStatus.CONNECTION_STATUS_UNSPECIFIED) {
                next.setStatus(existing.getStatus());
            }
            if (next.getLastError().isEmpty()) {
                next.setLastError(existing.getLastError());
            }
        } else if (!next.hasCreatedAt()) {
            next.setCreatedAt(AssetStore.timestamp(now));
        }
        if (next.getStatus() == ConnectionStatus.CONNECTION_STATUS_UNSPECIFIED) {
            next.setStatus(ConnectionStatus.CONNECTION_STATUS_PENDING);
        }
        next.setHasToken(!next.getToken().isEmpty());
        next.setHasClientSecret(!next.getClientSecret().isEmpty());
        next.setUpdatedAt(AssetStore.timestamp(now));
        return next.build();
    }
}
