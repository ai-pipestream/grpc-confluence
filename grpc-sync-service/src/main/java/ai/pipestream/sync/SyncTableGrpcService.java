package ai.pipestream.sync;

import ai.pipestream.sync.v1.Asset;
import ai.pipestream.sync.v1.DeleteAssetRequest;
import ai.pipestream.sync.v1.DeleteAssetResponse;
import ai.pipestream.sync.v1.GetAssetRequest;
import ai.pipestream.sync.v1.GetAssetResponse;
import ai.pipestream.sync.v1.GetCheckpointRequest;
import ai.pipestream.sync.v1.GetCheckpointResponse;
import ai.pipestream.sync.v1.ListAssetsRequest;
import ai.pipestream.sync.v1.ListAssetsResponse;
import ai.pipestream.sync.v1.PutCheckpointRequest;
import ai.pipestream.sync.v1.PutCheckpointResponse;
import ai.pipestream.sync.v1.ReconcileRequest;
import ai.pipestream.sync.v1.ReconcileResponse;
import ai.pipestream.sync.v1.SyncTableServiceGrpc;
import ai.pipestream.sync.v1.UpsertAssetRequest;
import ai.pipestream.sync.v1.UpsertAssetResponse;
import ai.pipestream.sync.v1.WatchRequest;
import ai.pipestream.sync.v1.WatchResponse;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.util.Objects;

/**
 * {@code SyncTableService} over a {@link Ledger}. List and Watch are
 * server-streaming; handlers run on the server's virtual-thread executor.
 */
public final class SyncTableGrpcService extends SyncTableServiceGrpc.SyncTableServiceImplBase {

    private final Ledger store;

    /**
     * Serves {@code SyncTableService} from {@code store}.
     *
     * @param store ledger (memory or JDBC); must not be {@code null}
     */
    public SyncTableGrpcService(Ledger store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    /**
     * Returns the ledger this service reads and writes.
     *
     * @return the {@link Ledger} passed to the constructor
     */
    public Ledger store() {
        return store;
    }

    @Override
    public void upsertAsset(UpsertAssetRequest request, StreamObserver<UpsertAssetResponse> observer) {
        try {
            Asset asset = store.upsert(request.getAsset());
            observer.onNext(UpsertAssetResponse.newBuilder().setAsset(asset).build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void getAsset(GetAssetRequest request, StreamObserver<GetAssetResponse> observer) {
        try {
            store.get(request.getAssetId()).ifPresentOrElse(asset -> {
                observer.onNext(GetAssetResponse.newBuilder().setAsset(asset).build());
                observer.onCompleted();
            }, () -> observer.onError(Status.NOT_FOUND
                    .withDescription("asset not found: " + request.getAssetId())
                    .asRuntimeException()));
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void listAssets(ListAssetsRequest request, StreamObserver<ListAssetsResponse> observer) {
        try {
            for (Asset asset : store.list(request.getSource(), request.getKind(),
                    request.getParentAssetId(), request.getAttachmentsOnly(), request.getStatus(),
                    request.getLimit(), request.getConnectionId())) {
                observer.onNext(ListAssetsResponse.newBuilder().setAsset(asset).build());
            }
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void watch(WatchRequest request, StreamObserver<WatchResponse> observer) {
        ServerCallStreamObserver<WatchResponse> call =
                (ServerCallStreamObserver<WatchResponse>) observer;
        AutoCloseable subscription = store.watch(asset -> {
            if (!request.getSource().isEmpty() && !request.getSource().equals(asset.getSource())) {
                return;
            }
            if (!request.getConnectionId().isEmpty()
                    && !request.getConnectionId().equals(asset.getConnectionId())) {
                return;
            }
            try {
                observer.onNext(WatchResponse.newBuilder().setAsset(asset).build());
            } catch (Throwable ignored) {
                // client gone
            }
        });
        call.setOnCancelHandler(() -> closeQuietly(subscription));
        try {
            if (request.getIncludeSnapshot()) {
                for (Asset asset : store.list(request.getSource(), "", "", false,
                        ai.pipestream.sync.v1.AssetSyncStatus.ASSET_SYNC_STATUS_UNSPECIFIED, 0,
                        request.getConnectionId())) {
                    observer.onNext(WatchResponse.newBuilder().setAsset(asset).build());
                }
            }
        } catch (Throwable t) {
            closeQuietly(subscription);
            fail(observer, t);
        }
    }

    @Override
    public void deleteAsset(DeleteAssetRequest request, StreamObserver<DeleteAssetResponse> observer) {
        try {
            if (request.getAssetId().isBlank()) {
                throw new IllegalArgumentException("asset_id is required");
            }
            Asset asset = store.delete(request.getAssetId(), request.getRunId(), request.getCursor());
            observer.onNext(DeleteAssetResponse.newBuilder().setAsset(asset).build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void reconcile(ReconcileRequest request, StreamObserver<ReconcileResponse> observer) {
        try {
            int deleted = store.reconcile(request.getSource(), request.getRunId(), request.getKind(),
                    request.getConnectionId());
            observer.onNext(ReconcileResponse.newBuilder().setDeleted(deleted).build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void getCheckpoint(GetCheckpointRequest request,
            StreamObserver<GetCheckpointResponse> observer) {
        try {
            store.getCheckpoint(request.getSource(), request.getScope(), request.getConnectionId())
                    .ifPresentOrElse(cp -> {
                observer.onNext(GetCheckpointResponse.newBuilder().setCheckpoint(cp).build());
                observer.onCompleted();
            }, () -> observer.onError(Status.NOT_FOUND
                    .withDescription("checkpoint not found")
                    .asRuntimeException()));
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void putCheckpoint(PutCheckpointRequest request,
            StreamObserver<PutCheckpointResponse> observer) {
        try {
            observer.onNext(PutCheckpointResponse.newBuilder()
                    .setCheckpoint(store.putCheckpoint(request.getCheckpoint()))
                    .build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }

    private static void fail(StreamObserver<?> observer, Throwable t) {
        if (t instanceof io.grpc.StatusRuntimeException e) {
            observer.onError(e);
            return;
        }
        if (t instanceof IllegalArgumentException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage())
                    .asRuntimeException());
            return;
        }
        observer.onError(Status.INTERNAL.withDescription(String.valueOf(t.getMessage()))
                .asRuntimeException());
    }
}
