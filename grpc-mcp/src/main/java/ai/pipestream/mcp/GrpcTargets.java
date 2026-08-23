package ai.pipestream.mcp;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

/** Plaintext gRPC channels to the other jars in this repo. */
public final class GrpcTargets {

    /** Environment variable for the Confluence gRPC {@code host:port}. */
    public static final String ENV_CONFLUENCE = "CONFLUENCE_GRPC_TARGET";
    /** Environment variable for the Microsoft gRPC {@code host:port}. */
    public static final String ENV_MICROSOFT = "MICROSOFT_GRPC_TARGET";
    /** Environment variable for the SyncTable gRPC {@code host:port}. */
    public static final String ENV_SYNC = "SYNC_TABLE_TARGET";
    /** Fallback Confluence target when {@link #ENV_CONFLUENCE} is unset. */
    public static final String DEFAULT_CONFLUENCE = "localhost:9095";
    /** Fallback Microsoft target when {@link #ENV_MICROSOFT} is unset. */
    public static final String DEFAULT_MICROSOFT = "localhost:9096";
    /** Fallback SyncTable target when {@link #ENV_SYNC} is unset. */
    public static final String DEFAULT_SYNC = "localhost:9097";

    private GrpcTargets() {
    }

    /**
     * Opens a plaintext {@link ManagedChannel} to {@code envName}, or {@code fallback}
     * when that variable is unset or blank.
     *
     * @param envName environment variable holding a {@code host:port}
     * @param fallback target used when {@code envName} is missing or blank
     * @return a new plaintext channel to the resolved target
     */
    public static ManagedChannel channel(String envName, String fallback) {
        String target = System.getenv(envName);
        if (target == null || target.isBlank()) {
            target = fallback;
        }
        return ManagedChannelBuilder.forTarget(target.trim()).usePlaintext().build();
    }

    /**
     * Shuts down {@code channel}, waiting up to five seconds before forcing
     * {@code shutdownNow}. A {@code null} channel is ignored. Interrupted waits
     * restore the interrupt flag and force shutdown.
     *
     * @param channel channel to close; may be {@code null}
     */
    public static void shutdown(ManagedChannel channel) {
        if (channel == null) {
            return;
        }
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
