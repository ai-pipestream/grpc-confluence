package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link ChangeSink} that collects everything in memory, for tests and for
 * callers that want to batch-drain a crawl. Thread-safe by construction.
 */
public final class InMemoryChangeSink implements ChangeSink {

    private final List<ConfluenceChange> changes = new CopyOnWriteArrayList<>();
    private final List<ConfluenceSnapshot> snapshots = new CopyOnWriteArrayList<>();
    private final List<String> completedRuns = new CopyOnWriteArrayList<>();

    /** Creates an empty collector. */
    public InMemoryChangeSink() {}

    @Override
    public void emit(ConfluenceChange change) {
        changes.add(change);
    }

    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        snapshots.add(snapshot);
    }

    @Override
    public void completeRun(String runId) {
        completedRuns.add(runId);
    }

    /**
     * The changes collected so far, in emission order.
     *
     * @return a snapshot copy of the collected changes
     */
    public List<ConfluenceChange> changes() {
        return List.copyOf(changes);
    }

    /**
     * The snapshots collected so far, in emission order.
     *
     * @return a snapshot copy of the collected snapshots
     */
    public List<ConfluenceSnapshot> snapshots() {
        return List.copyOf(snapshots);
    }

    /**
     * The run ids passed to {@link #completeRun(String)}, in call order.
     *
     * @return a snapshot copy of the completed run ids
     */
    public List<String> completedRuns() {
        return List.copyOf(completedRuns);
    }

    /** Drops every collected change, snapshot, and completed run id. */
    public void clear() {
        changes.clear();
        snapshots.clear();
        completedRuns.clear();
    }
}
