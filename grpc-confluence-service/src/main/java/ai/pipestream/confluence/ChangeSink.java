package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;

/**
 * Where the crawler's output goes. Implementations must be thread-safe: the
 * crawler emits from virtual threads concurrently. The shipped
 * implementations: {@link KafkaChangeSink} (raw protobuf bytes on Kafka,
 * activated by {@code CONFLUENCE_KAFKA_BOOTSTRAP_SERVERS}),
 * {@link OkfChangeSink} (OKF v0.2 directory + zip + sibling WARC),
 * {@link CompositeChangeSink} (fan-out when several are active), plus
 * {@link LoggingChangeSink} and {@link InMemoryChangeSink} for tests.
 *
 * <p>Every pass ends with {@link #completeRun(String, boolean)}. Only a full
 * sweep (a full crawl of everything the connection covers) lets a sink treat
 * what it holds but the pass did not emit as gone. After an incremental pass,
 * or a crawl of a requested subset of spaces, what was not emitted is
 * unchanged or out of scope and must be kept.</p>
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

    /**
     * A pass finished. End-of-run outputs (OKF tree, zip, WARC) write here
     * for every pass. The default forwards full sweeps to
     * {@link #completeRun(String)} and ignores the rest, so a sink that
     * reconciles by run id never deletes what a narrower pass did not visit.
     *
     * @param runId the crawl run identifier; for an incremental pass, the
     *        cursor it resumed from
     * @param fullSweep whether the pass enumerated everything the connection
     *        covers
     */
    default void completeRun(String runId, boolean fullSweep) {
        if (fullSweep) {
            completeRun(runId);
        }
    }

    /**
     * This sink for one crawl pass of one catalog connection. A sink that
     * keeps state between {@link #emit} and {@link #completeRun(String,
     * boolean)} returns a view with its own pass state, so that concurrent
     * Syncs do not mix their records and a full sweep of one connection does
     * not drop what the sink holds for another.
     *
     * @param connectionId the catalog connection id
     * @return a sink for that connection; this sink when it keeps no state
     *         per pass
     */
    default ChangeSink forConnection(String connectionId) {
        return this;
    }
}
