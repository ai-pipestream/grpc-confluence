package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import com.google.protobuf.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publishes crawler output to Kafka as raw protobuf bytes. Changes land on
 * the changes topic keyed by {@code change_id}; snapshots land on the
 * snapshots topic keyed by {@code snapshot_id}. {@link ConfluenceValidator}
 * runs before each send so an invalid record never reaches the topic.
 *
 * <p>Sends are async, and every record of a pass has to reach its topic. A
 * Sync that ends OK hands out a resume cursor, and the next pass never emits
 * again what lies before it, so a record the producer gave up on would be
 * lost for good. A failed send therefore fails the pass with
 * {@link DeliveryFailedException}: from the next {@link #emit} or
 * {@link #snapshot} once the producer has reported it, and at the latest
 * from {@link #completeRun(String, boolean)}, which flushes the producer and
 * waits for every outstanding record. The producer's own retries
 * ({@code delivery.timeout.ms}) come first; what fails here has run out of
 * them.</p>
 *
 * <p>Failures are counted per pass. {@link #forConnection} gives each Sync its
 * own count, so one Sync's failed record does not fail another's pass.</p>
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

    private final Producer<String, byte[]> producer;
    private final String topic;
    private final String snapshotsTopic;
    private final Pass own = new Pass();

    /**
     * Creates a sink that publishes through {@code producer}.
     *
     * @param producer the Kafka producer; not closed by this constructor
     * @param topic the changes topic
     * @param snapshotsTopic the snapshots topic
     */
    public KafkaChangeSink(Producer<String, byte[]> producer, String topic,
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
        own.emit(change);
    }

    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        own.snapshot(snapshot);
    }

    @Override
    public void completeRun(String runId) {
        own.completeRun(runId, true);
    }

    @Override
    public void completeRun(String runId, boolean fullSweep) {
        own.completeRun(runId, fullSweep);
    }

    @Override
    public ChangeSink forConnection(String connectionId) {
        return new Pass();
    }

    /**
     * A record of a pass did not reach its topic. The pass must not hand out
     * a resume cursor.
     */
    public static final class DeliveryFailedException extends RuntimeException {
        /** Serialization identifier. */
        @java.io.Serial
        private static final long serialVersionUID = 1L;

        /** How many records of the pass failed so far. */
        private final int failed;
        /** Whether the first failure is one Kafka calls retriable. */
        private final boolean retriable;

        DeliveryFailedException(int failed, String topic, String key, Exception first) {
            super("kafka delivery failed for " + failed + (failed == 1 ? " record" : " records")
                    + " of this pass; first: key " + key + " to " + topic + ": " + first, first);
            this.failed = failed;
            this.retriable = first instanceof RetriableException;
        }

        /**
         * How many records of the pass had failed when this was thrown.
         *
         * @return the count, at least 1
         */
        public int failed() {
            return failed;
        }

        /**
         * Whether running the pass again can help: the first failure is one
         * Kafka calls retriable (a timeout, an unreachable broker), not a
         * refusal such as a record that is too large or a missing grant.
         *
         * @return true when the first failure is retriable
         */
        public boolean retriable() {
            return retriable;
        }
    }

    /** The first record of a pass that failed. */
    private record Failed(String topic, String key, Exception cause) {
    }

    /** The sends of one crawl pass and what became of them. */
    private final class Pass implements ChangeSink {

        private final AtomicInteger failed = new AtomicInteger();
        private final AtomicReference<Failed> first = new AtomicReference<>();

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

        @Override
        public void completeRun(String runId) {
            completeRun(runId, true);
        }

        /** Waits for every record of the pass and fails when one did not arrive. */
        @Override
        public void completeRun(String runId, boolean fullSweep) {
            producer.flush();
            Failed failure = first.getAndSet(null);
            int count = failed.getAndSet(0);
            if (failure != null) {
                throw new DeliveryFailedException(count, failure.topic(), failure.key(),
                        failure.cause());
            }
        }

        private void send(String to, String key, Message value) {
            failIfAnyFailed();
            try {
                producer.send(new ProducerRecord<>(to, key, value.toByteArray()),
                        (metadata, exception) -> {
                            if (exception != null) {
                                record(to, key, exception);
                            }
                        });
            } catch (RuntimeException e) {
                record(to, key, e);
            }
            // A send that failed before it was queued has reported by now.
            failIfAnyFailed();
        }

        private void record(String to, String key, Exception cause) {
            failed.incrementAndGet();
            first.compareAndSet(null, new Failed(to, key, cause));
            LOG.log(System.Logger.Level.WARNING,
                    "confluence kafka sink: record key={0} to {1} failed: {2}",
                    key, to, cause.toString());
        }

        private void failIfAnyFailed() {
            Failed failure = first.get();
            if (failure != null) {
                throw new DeliveryFailedException(failed.get(), failure.topic(), failure.key(),
                        failure.cause());
            }
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
