package ai.pipestream.sync;

import ai.pipestream.sync.v1.Connection;
import ai.pipestream.sync.v1.ConnectionServiceGrpc;
import ai.pipestream.sync.v1.CreateConnectionRequest;
import ai.pipestream.sync.v1.CreateConnectionResponse;
import ai.pipestream.sync.v1.DeleteConnectionRequest;
import ai.pipestream.sync.v1.DeleteConnectionResponse;
import ai.pipestream.sync.v1.GetConnectionRequest;
import ai.pipestream.sync.v1.GetConnectionResponse;
import ai.pipestream.sync.v1.ListConnectionsRequest;
import ai.pipestream.sync.v1.ListConnectionsResponse;
import ai.pipestream.sync.v1.RecordProbeRequest;
import ai.pipestream.sync.v1.RecordProbeResponse;
import ai.pipestream.sync.v1.UpdateConnectionRequest;
import ai.pipestream.sync.v1.UpdateConnectionResponse;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * {@code ConnectionService} gRPC implementation. This <em>is</em> the
 * connection service: Confluence, Microsoft, MCP, and a future UI call the
 * generated stub. They do not reach the {@link Ledger} themselves.
 */
public final class ConnectionGrpcService extends ConnectionServiceGrpc.ConnectionServiceImplBase {

    private final Ledger ledger;

    /**
     * Serves {@code ConnectionService} from {@code ledger}.
     *
     * @param ledger store used only inside this process
     */
    public ConnectionGrpcService(Ledger ledger) {
        this.ledger = Objects.requireNonNull(ledger, "ledger");
    }

    @Override
    public void createConnection(CreateConnectionRequest request,
            StreamObserver<CreateConnectionResponse> observer) {
        try {
            Connection incoming = request.getConnection();
            Set<String> taken = new LinkedHashSet<>();
            ledger.listConnections(ai.pipestream.sync.v1.ConnectionKind.CONNECTION_KIND_UNSPECIFIED)
                    .forEach(row -> taken.add(row.getConnectionId()));
            String id = ConnectionIds.allocate(incoming.getConnectionId(),
                    incoming.getDisplayName(), taken);
            Connection stored = ledger.putConnection(incoming.toBuilder()
                    .setConnectionId(id)
                    .build(), true);
            observer.onNext(CreateConnectionResponse.newBuilder()
                    .setConnection(ConnectionViews.redact(stored))
                    .build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void getConnection(GetConnectionRequest request,
            StreamObserver<GetConnectionResponse> observer) {
        try {
            if (request.getConnectionId().isBlank()) {
                throw new IllegalArgumentException("connection_id is required");
            }
            ledger.getConnection(request.getConnectionId()).ifPresentOrElse(row -> {
                Connection view = request.getIncludeSecret() ? row : ConnectionViews.redact(row);
                observer.onNext(GetConnectionResponse.newBuilder().setConnection(view).build());
                observer.onCompleted();
            }, () -> observer.onError(Status.NOT_FOUND
                    .withDescription("connection not found: " + request.getConnectionId())
                    .asRuntimeException()));
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void listConnections(ListConnectionsRequest request,
            StreamObserver<ListConnectionsResponse> observer) {
        try {
            ListConnectionsResponse.Builder response = ListConnectionsResponse.newBuilder();
            for (Connection row : ledger.listConnections(request.getKind())) {
                response.addConnections(ConnectionViews.redact(row));
            }
            observer.onNext(response.build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void updateConnection(UpdateConnectionRequest request,
            StreamObserver<UpdateConnectionResponse> observer) {
        try {
            if (request.getConnection().getConnectionId().isBlank()) {
                throw new IllegalArgumentException("connection_id is required");
            }
            Connection stored = ledger.putConnection(request.getConnection(), false);
            observer.onNext(UpdateConnectionResponse.newBuilder()
                    .setConnection(ConnectionViews.redact(stored))
                    .build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void deleteConnection(DeleteConnectionRequest request,
            StreamObserver<DeleteConnectionResponse> observer) {
        try {
            if (request.getConnectionId().isBlank()) {
                throw new IllegalArgumentException("connection_id is required");
            }
            if (!ledger.deleteConnection(request.getConnectionId())) {
                observer.onError(Status.NOT_FOUND
                        .withDescription("connection not found: " + request.getConnectionId())
                        .asRuntimeException());
                return;
            }
            observer.onNext(DeleteConnectionResponse.newBuilder()
                    .setConnectionId(request.getConnectionId())
                    .build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
        }
    }

    @Override
    public void recordProbe(RecordProbeRequest request,
            StreamObserver<RecordProbeResponse> observer) {
        try {
            if (request.getConnectionId().isBlank()) {
                throw new IllegalArgumentException("connection_id is required");
            }
            Connection stored = ledger.recordProbe(request.getConnectionId(), request.getOk(),
                    request.getErrorMessage());
            observer.onNext(RecordProbeResponse.newBuilder()
                    .setConnection(ConnectionViews.redact(stored))
                    .build());
            observer.onCompleted();
        } catch (Throwable t) {
            fail(observer, t);
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
        if (t instanceof IllegalStateException e) {
            String message = String.valueOf(e.getMessage());
            Status status = message.contains("already exists")
                    ? Status.ALREADY_EXISTS
                    : message.contains("not found")
                            ? Status.NOT_FOUND
                            : Status.FAILED_PRECONDITION;
            observer.onError(status.withDescription(message).asRuntimeException());
            return;
        }
        observer.onError(Status.INTERNAL.withDescription(String.valueOf(t.getMessage()))
                .asRuntimeException());
    }
}
