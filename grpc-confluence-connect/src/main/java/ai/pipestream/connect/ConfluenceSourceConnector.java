package ai.pipestream.connect;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;

/**
 * Kafka Connect source for Confluence Cloud. One task, one crawl stream.
 * Values are {@code ConfluenceChange} protobuf bytes.
 */
public final class ConfluenceSourceConnector extends ChangeSourceConnector {

    /** Config key: include page, blog, and comment bodies in the crawl. */
    public static final String INCLUDE_BODIES = "include.bodies";
    /** Config key for the Confluence Cloud base URL with {@code /wiki} (direct mode). */
    public static final String BASE_URL = "confluence.base.url";
    /** Config key for the Atlassian account email (direct mode). */
    public static final String EMAIL = "confluence.email";
    /** Config key for the Atlassian API token (direct mode). */
    public static final String API_TOKEN = "confluence.api.token";
    /** Config key for comma-separated space keys; empty means all spaces. */
    public static final String SPACES = "confluence.spaces";

    /** Creates a Confluence Cloud Kafka Connect source connector. */
    public ConfluenceSourceConnector() {
    }

    static ConfigDef definition() {
        return baseDefinition("ConfluenceChange", "ConfluenceService")
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
    public Class<? extends Task> taskClass() {
        return ConfluenceSourceTask.class;
    }

    @Override
    public ConfigDef config() {
        return definition();
    }
}
