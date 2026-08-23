package ai.pipestream.sync;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.services.HealthStatusManager;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Standalone SyncTable + Connection gRPC process. Handlers run on virtual
 * threads. Callers reach {@code SyncTableService} and
 * {@code ConnectionService} on this port; they do not open the store.
 *
 * <p>{@code SYNC_TABLE_GRPC_PORT}, default 9097. Durable SQLite when
 * {@code SYNC_TABLE_JDBC_URL} or {@code SYNC_TABLE_DB} is set.</p>
 */
public final class SyncTableServer {

    /** Environment variable for the gRPC listen port. */
    public static final String ENV_GRPC_PORT = "SYNC_TABLE_GRPC_PORT";
    /** Fallback listen port when {@link #ENV_GRPC_PORT} is unset or not a number. */
    public static final int DEFAULT_GRPC_PORT = 9097;

    private static final System.Logger LOG = System.getLogger(SyncTableServer.class.getName());

    private SyncTableServer() {
    }

    /**
     * Starts {@link SyncTableGrpcService} on {@link #ENV_GRPC_PORT} (default
     * {@link #DEFAULT_GRPC_PORT}) and blocks until shutdown.
     *
     * @param args unused
     * @throws Exception if the Netty server fails to start or wait is interrupted
     */
    public static void main(String[] args) throws Exception {
        Ledger ledger = Ledgers.open();
        int port = parseInt(System.getenv(ENV_GRPC_PORT), DEFAULT_GRPC_PORT);
        Server server = startNetty(port, new SyncTableGrpcService(ledger),
                new ConnectionGrpcService(ledger));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            try {
                if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
                ledger.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "ledger close failed: {0}", e.toString());
            }
        }, "sync-table-shutdown"));
        LOG.log(System.Logger.Level.INFO, "sync-table listening on port {0} store={1}",
                server.getPort(), Ledgers.durable(System.getenv()) ? "jdbc" : "memory");
        server.awaitTermination();
    }

    /**
     * Binds {@code service} on {@code port} with health, reflection, and a
     * virtual-thread executor.
     *
     * @param service gRPC bindable service, typically {@link SyncTableGrpcService}
     * @param port listen port; {@code 0} selects an ephemeral port
     * @return the started server
     * @throws IOException if the port cannot be bound
     */
    public static Server startNetty(BindableService service, int port) throws IOException {
        return startNetty(port, service);
    }

    /**
     * Binds {@code services} on {@code port} with health, reflection, and a
     * virtual-thread executor.
     *
     * @param port listen port; {@code 0} selects an ephemeral port
     * @param services gRPC services to expose ({@link SyncTableGrpcService},
     *        {@link ConnectionGrpcService})
     * @return the started server
     * @throws IOException if the port cannot be bound
     */
    public static Server startNetty(int port, BindableService... services) throws IOException {
        HealthStatusManager health = new HealthStatusManager();
        NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor());
        for (BindableService service : services) {
            builder.addService(service);
        }
        Server server = builder
                .addService(health.getHealthService())
                .addService(ProtoReflectionService.newInstance())
                .addService(ProtoReflectionServiceV1.newInstance())
                .build()
                .start();
        health.setStatus("", HealthCheckResponse.ServingStatus.SERVING);
        return server;
    }

    private static int parseInt(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
