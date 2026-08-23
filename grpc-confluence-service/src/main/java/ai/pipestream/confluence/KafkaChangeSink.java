package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Publishes crawler output to Kafka as raw protobuf bytes. Changes land on
 * the changes topic keyed by {@code change_id}; snapshots land on the
 * snapshots topic keyed by {@code snapshot_id}. {@link ConfluenceValidator}
 * runs before each send so an invalid record never reaches the topic.
 *
 * <p>Sends are async and failures are logged, never thrown: a Kafka outage
 * must not abort a multi-hour crawl. Delivery is at-most-once per record.</p>
 */
public final class KafkaChangeSink implements ChangeSink, AutoCloseable {

    /** Environment variable for the Kafka bootstrap servers. */
    public static final String ENV_BOOTSTRAP_SERVERS = "CONFLUENCE_KAFKA_BOOTSTRAP_SERVERS";
    /** Environment variable for the changes topic. */
    public static final String ENV_TOPIC = "CONFLUENCE_KAFKA_TOPIC";
    /** Environment variable for the snapshots topic. */
    public static final String ENV_SNAPSHOTS_TOPIC = "CONFLUENCE_KAFKA_SNAPSHOTS_TOPIC";
    /** Default changes topic when {@link #ENV_TOPIC} is unset. */
    public static final String DEFAULT_TOPIC = "confluence-events";
    /** Default snapshots topic when {@link #ENV_SNAPSHOTS_TOPIC} is unset. */
    public static final String DEFAULT_SNAPSHOTS_TOPIC = "confluence-snapshots";

    private static final System.Logger LOG = System.getLogger(KafkaChangeSink.class.getName());
    private static final ConfluenceValidator VALIDATOR = ConfluenceValidator.create();

    private final KafkaProducer<String, byte[]> producer;
    private final String topic;
    private final String snapshotsTopic;

    /**
     * Creates a sink that publishes through {@code producer}.
     *
     * @param producer the Kafka producer; not closed by this constructor
     * @param topic the changes topic
     * @param snapshotsTopic the snapshots topic
     */
    public KafkaChangeSink(KafkaProducer<String, byte[]> producer, String topic,
            String snapshotsTopic) {
        this.producer = Objects.requireNonNull(producer, "producer");
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic cannot be null or blank");
        }
        if (snapshotsTopic == null || snapshotsTopic.isBlank()) {
            throw new IllegalArgumentException("snapshotsTopic cannot be null or blank");
        }
        this.topic = topic;
        this.snapshotsTopic = snapshotsTopic;
    }

    /**
     * Whether Kafka publishing is configured in the process environment.
     *
     * @return {@code true} when {@link #ENV_BOOTSTRAP_SERVERS} is set
     */
    public static boolean enabled() {
        String servers = System.getenv(ENV_BOOTSTRAP_SERVERS);
        return servers != null && !servers.isBlank();
    }

    /**
     * Builds a sink from {@code CONFLUENCE_KAFKA_*} environment variables.
     *
     * @return a sink connected to the configured bootstrap servers
     */
    public static KafkaChangeSink fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    static KafkaChangeSink fromEnvironment(Map<String, String> env) {
        String servers = env.get(ENV_BOOTSTRAP_SERVERS);
        if (servers == null || servers.isBlank()) {
            throw new IllegalStateException(ENV_BOOTSTRAP_SERVERS + " is required");
        }
        String topic = firstNonBlank(env.get(ENV_TOPIC), DEFAULT_TOPIC);
        String snapshots = firstNonBlank(env.get(ENV_SNAPSHOTS_TOPIC), DEFAULT_SNAPSHOTS_TOPIC);
        return new KafkaChangeSink(newProducer(servers), topic, snapshots);
    }

    /**
     * Builds a producer with idempotent {@code acks=all} and byte-array values.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @return a new producer
     */
    public static KafkaProducer<String, byte[]> newProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    @Override
    public void emit(ConfluenceChange change) {
        VALIDATOR.requireValid(change);
        send(topic, change.getChangeId(), change);
    }

    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        VALIDATOR.requireValid(snapshot);
        send(snapshotsTopic, snapshot.getSnapshotId(), snapshot);
    }

    private void send(String topic, String key, Message value) {
        try {
            producer.send(new ProducerRecord<>(topic, key, value.toByteArray()),
                    (metadata, exception) -> {
                        if (exception != null) {
                            LOG.log(System.Logger.Level.WARNING,
                                    "confluence kafka sink: record key={0} to {1} failed: {2}",
                                    key, topic, exception.toString());
                        }
                    });
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING,
                    "confluence kafka sink: record key={0} to {1} rejected: {2}",
                    key, topic, e.toString());
        }
    }

    /** Closes the underlying producer. */
    @Override
    public void close() {
        producer.close();
    }

    private static String firstNonBlank(String value, String fallback) {
        return value == null || value.isBlank() ? fallback : value;
    }
}
