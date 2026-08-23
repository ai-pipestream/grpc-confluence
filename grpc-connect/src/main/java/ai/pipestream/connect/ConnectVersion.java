package ai.pipestream.connect;

/**
 * Kafka Connect plugin version shared by the Confluence and Microsoft
 * source connectors and their tasks.
 */
public final class ConnectVersion {

    /** Version string returned by each connector and task {@code version()} method. */
    public static final String VALUE = "0.1.0-SNAPSHOT";

    private ConnectVersion() {
    }
}
