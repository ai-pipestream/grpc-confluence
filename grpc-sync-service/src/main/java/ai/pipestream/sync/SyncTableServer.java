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
 * Standalone SyncTable gRPC process. Handlers run on virtual threads.
 *
 * <p>{@code SYNC_TABLE_GRPC_PORT}, default 9097.</p>
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
        Server server = startNetty(new SyncTableGrpcService(new AssetStore()),
                parseInt(System.getenv(ENV_GRPC_PORT), DEFAULT_GRPC_PORT));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            try {
                if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            }
        }, "sync-table-shutdown"));
        LOG.log(System.Logger.Level.INFO, "sync-table listening on port {0}", server.getPort());
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
        HealthStatusManager health = new HealthStatusManager();
        Server server = NettyServerBuilder.forPort(port)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(service)
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
