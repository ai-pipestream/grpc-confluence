package ai.pipestream.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.List;
import java.util.Map;

/**
 * Kafka Connect source for Confluence Cloud. One task, one crawl stream.
 * Values are {@code ConfluenceChange} protobuf bytes.
 */
public final class ConfluenceSourceConnector extends SourceConnector {

    public static final String TOPIC = "topic";
    public static final String GRPC_TARGET = "grpc.target";
    public static final String GRPC_PLAINTEXT = "grpc.plaintext";
    public static final String INCLUDE_BODIES = "include.bodies";
    public static final String BASE_URL = "confluence.base.url";
    public static final String EMAIL = "confluence.email";
    public static final String API_TOKEN = "confluence.api.token";
    public static final String SPACES = "confluence.spaces";

    private Map<String, String> props;

    static ConfigDef definition() {
        return new ConfigDef()
                .define(TOPIC, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH, "Topic for ConfluenceChange bytes")
                .define(GRPC_TARGET, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                        "host:port of ConfluenceService; empty = in-process crawler")
                .define(GRPC_PLAINTEXT, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                        "Use plaintext when grpc.target is set")
                .define(INCLUDE_BODIES, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                        "Include page/blog/comment bodies")
                .define(BASE_URL, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                        "Confluence Cloud base URL with /wiki (direct mode)")
                .define(EMAIL, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                        "Atlassian account email (direct mode)")
                .define(API_TOKEN, ConfigDef.Type.PASSWORD, "", ConfigDef.Importance.HIGH,
                        "Atlassian API token (direct mode)")
                .define(SPACES, ConfigDef.Type.STRING, "", ConfigDef.Importance.MEDIUM,
                        "Comma-separated space keys; empty = all");
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = Map.copyOf(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ConfluenceSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return List.of(props);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return definition();
    }

    @Override
    public String version() {
        return ConnectVersion.VALUE;
    }
}
