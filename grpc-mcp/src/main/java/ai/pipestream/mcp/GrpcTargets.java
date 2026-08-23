package ai.pipestream.mcp;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

/** Plaintext gRPC channels to the other jars in this repo. */
public final class GrpcTargets {

    public static final String ENV_CONFLUENCE = "CONFLUENCE_GRPC_TARGET";
    public static final String ENV_MICROSOFT = "MICROSOFT_GRPC_TARGET";
    public static final String ENV_SYNC = "SYNC_TABLE_TARGET";
    public static final String DEFAULT_CONFLUENCE = "localhost:9095";
    public static final String DEFAULT_MICROSOFT = "localhost:9096";
    public static final String DEFAULT_SYNC = "localhost:9097";

    private GrpcTargets() {
    }

    public static ManagedChannel channel(String envName, String fallback) {
        String target = System.getenv(envName);
        if (target == null || target.isBlank()) {
            target = fallback;
        }
        return ManagedChannelBuilder.forTarget(target.trim()).usePlaintext().build();
    }

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
