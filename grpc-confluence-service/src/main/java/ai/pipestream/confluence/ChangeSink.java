package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;

/**
 * Where the crawler's output goes. Implementations must be thread-safe: the
 * crawler emits from virtual threads concurrently. The shipped
 * implementations: {@link KafkaChangeSink} (raw protobuf bytes on Kafka,
 * activated by {@code CONFLUENCE_KAFKA_BOOTSTRAP_SERVERS}),
 * {@link CompositeChangeSink} (fan-out when several are active), plus
 * {@link LoggingChangeSink} and {@link InMemoryChangeSink} for tests.
 */
public interface ChangeSink {

    /**
     * One upsert or delete against the Confluence mirror.
     *
     * @param change the change to emit
     */
    void emit(ConfluenceChange change);

    /**
     * The full-sync marker for one completed space crawl.
     *
     * @param snapshot the snapshot to emit
     */
    void snapshot(ConfluenceSnapshot snapshot);

    /**
     * A full crawl run finished. Sync-table sinks reconcile rows not seen
     * in {@code runId}; others ignore.
     *
     * @param runId the crawl run identifier
     */
    default void completeRun(String runId) {
    }
}
