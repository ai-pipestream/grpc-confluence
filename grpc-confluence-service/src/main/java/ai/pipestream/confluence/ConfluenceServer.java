package ai.pipestream.confluence;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.services.HealthStatusManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Standalone Confluence gRPC proxy. Config comes from the environment; the
 * facade sits on Netty with reflection and health on, handlers on virtual
 * threads.
 *
 * <p>Environment:</p>
 * <ul>
 *   <li>{@code CONFLUENCE_BASE_URL}, {@code CONFLUENCE_EMAIL} (or
 *   {@code CONFLUENCE_USER}), {@code CONFLUENCE_API_TOKEN} (or
 *   {@code CONFLUENCE_TOKEN}) and the rest of the crawler config, per
 *   {@link ConfluenceConnectorConfig#fromEnvironment()}</li>
 *   <li>{@code CONFLUENCE_GRPC_PORT}: listen port, default 9095</li>
 *   <li>{@code CONFLUENCE_ATTACHMENT_MAX_BYTES}: inline attachment cap,
 *   default 25 MiB</li>
 *   <li>{@code CONFLUENCE_KAFKA_BOOTSTRAP_SERVERS} (plus optional
 *   {@code CONFLUENCE_KAFKA_TOPIC} / {@code CONFLUENCE_KAFKA_SNAPSHOTS_TOPIC}):
 *   every change a sync emits also publishes through {@link KafkaChangeSink}</li>
 *   <li>{@code SYNC_TABLE_TARGET} (plus optional {@code SYNC_TABLE_PLAINTEXT}):
   *   every change also upserts into the generic {@code SyncTableService}</li>
   *   <li>{@code OKF_DIR} (plus optional {@code OKF_ZIP} / {@code OKF_WARC}):
   *   every completed Sync writes an OKF v0.2 directory, zip, and sibling
   *   WARC 1.1 file</li>
   * </ul>
 */
public final class ConfluenceServer {

    /** Environment variable for the gRPC listen port. */
    public static final String ENV_GRPC_PORT = "CONFLUENCE_GRPC_PORT";
    /** Environment variable for the inline attachment byte cap. */
    public static final String ENV_ATTACHMENT_MAX_BYTES = "CONFLUENCE_ATTACHMENT_MAX_BYTES";
    /** Default gRPC listen port when {@link #ENV_GRPC_PORT} is unset. */
    public static final int DEFAULT_GRPC_PORT = 9095;

    private static final System.Logger LOG = System.getLogger(ConfluenceServer.class.getName());

    private ConfluenceServer() {
    }

    /**
     * Starts the proxy from the process environment and blocks until shutdown.
     *
     * @param args unused
     * @throws Exception if the server cannot start or is interrupted
     */
    public static void main(String[] args) throws Exception {
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.fromEnvironment();
        List<AutoCloseable> closables = new ArrayList<>();
        ChangeSink downstream = null;
        List<ChangeSink> sinks = new ArrayList<>();
        if (KafkaChangeSink.enabled()) {
            KafkaChangeSink kafka = KafkaChangeSink.fromEnvironment();
            sinks.add(kafka);
            closables.add(kafka);
            LOG.log(System.Logger.Level.INFO, "confluence-proxy kafka sink active on {0}",
                    System.getenv(KafkaChangeSink.ENV_BOOTSTRAP_SERVERS));
        }
        if (SyncTableChangeSink.enabled()) {
            SyncTableChangeSink syncTable = SyncTableChangeSink.fromEnvironment();
            sinks.add(syncTable);
            closables.add(syncTable);
            LOG.log(System.Logger.Level.INFO, "confluence-proxy sync-table sink active on {0}",
                    System.getenv(SyncTableChangeSink.ENV_TARGET));
        }
        if (OkfChangeSink.enabled()) {
            sinks.add(OkfChangeSink.fromEnvironment());
            LOG.log(System.Logger.Level.INFO, "confluence-proxy OKF sink active dir={0} zip={1} warc={2}",
                    System.getenv(ai.pipestream.okf.OkfOutput.ENV_DIR),
                    System.getenv(ai.pipestream.okf.OkfOutput.ENV_ZIP),
                    System.getenv(ai.pipestream.okf.OkfOutput.ENV_WARC));
        }
        if (!sinks.isEmpty()) {
            downstream = sinks.size() == 1 ? sinks.get(0) : new CompositeChangeSink(sinks);
        }
        ConfluenceGrpcService service = new ConfluenceGrpcService(config,
                new ConfluenceClient(config),
                parseLong(System.getenv(ENV_ATTACHMENT_MAX_BYTES),
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES),
                downstream);
        Server server = startNetty(service, parseInt(System.getenv(ENV_GRPC_PORT),
                DEFAULT_GRPC_PORT));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.shutdown();
            try {
                if (!server.awaitTermination(10, TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
                for (AutoCloseable closable : closables) {
                    closable.close();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                server.shutdownNow();
            } catch (Exception e) {
                LOG.log(System.Logger.Level.WARNING, "sink close failed: {0}", e.toString());
            }
        }, "confluence-proxy-shutdown"));
        LOG.log(System.Logger.Level.INFO,
                "confluence-proxy listening on port {0} (base url {1}, spaces {2})",
                server.getPort(), config.baseUrl(),
                config.hasSpaceAllowlist() ? config.spaces() : "all");
        server.awaitTermination();
    }

    /**
     * Binds {@code service} on Netty with reflection, health, and a virtual-thread executor.
     *
     * @param service the gRPC service to serve
     * @param port the listen port
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
                .build().start();
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

    private static long parseLong(String value, long fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
