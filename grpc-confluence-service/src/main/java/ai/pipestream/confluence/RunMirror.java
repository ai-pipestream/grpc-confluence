package ai.pipestream.confluence;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What an end-of-run output writes: the mirror of every record the full
 * sweeps and the passes since left standing, keyed by where the record lives
 * in the output. Each {@link Pass} collects the records of one crawl pass of
 * one connection and folds them into the mirror when it completes.
 *
 * <p>A full sweep of a connection replaces that connection's records in the
 * mirror with exactly what the sweep emitted; the records of other
 * connections stay. A pass that was not a full sweep (an incremental pass, or
 * a crawl of a requested subset of spaces) cannot prove that anything it did
 * not emit is gone, so it merges into the mirror. Such a pass, for a
 * connection with no full sweep in this mirror yet, has nothing to merge
 * into: its records are an increment, to be written apart from the full
 * bundle and never over it.</p>
 *
 * @param <V> the record type
 */
final class RunMirror<V> {

    /**
     * The records to write for one completed pass.
     *
     * @param values the records, in first-emission order
     * @param increment whether these are only the records of the pass, to be
     *        written apart from the full bundle
     * @param changed whether the pass can have changed what is written: false
     *        only for a pass that was not a full sweep and emitted nothing
     * @param <V> the record type
     */
    record Completed<V>(List<V> values, boolean increment, boolean changed) {
    }

    private record Owned<V>(String owner, V value) {
    }

    private final Map<String, Owned<V>> mirror = new LinkedHashMap<>();
    private final Set<String> baselines = new HashSet<>();

    /**
     * Starts collecting for {@code connectionId}. One pass object may serve
     * several crawl passes in turn, never two at once.
     *
     * @param connectionId the connection whose records the pass owns; empty
     *        for a sink that serves a single connection
     * @return the collector
     */
    Pass pass(String connectionId) {
        return new Pass(connectionId == null ? "" : connectionId);
    }

    /** The records of the crawl pass in progress for one connection. */
    final class Pass {

        private final String owner;
        private final Map<String, V> run = new LinkedHashMap<>();

        private Pass(String owner) {
            this.owner = owner;
        }

        /**
         * The connection this pass collects for.
         *
         * @return the connection id; empty for a single-connection sink
         */
        String connectionId() {
            return owner;
        }

        /**
         * Records {@code value}; the latest value for a key wins.
         *
         * @param key where the record lives in the output
         * @param value the record
         */
        void put(String key, V value) {
            synchronized (run) {
                run.put(key, value);
            }
        }

        /**
         * Ends the crawl pass in progress and folds it into the mirror.
         *
         * @param fullSweep whether the pass enumerated everything the
         *        connection covers
         * @return what to write
         */
        Completed<V> complete(boolean fullSweep) {
            Map<String, V> emitted;
            synchronized (run) {
                emitted = new LinkedHashMap<>(run);
                run.clear();
            }
            boolean changed = fullSweep || !emitted.isEmpty();
            synchronized (RunMirror.this) {
                if (fullSweep) {
                    mirror.values().removeIf(owned -> owned.owner().equals(owner));
                    baselines.add(owner);
                } else if (!baselines.contains(owner)) {
                    return new Completed<>(new ArrayList<>(emitted.values()), true, changed);
                }
                emitted.forEach((key, value) -> mirror.put(key, new Owned<>(owner, value)));
                List<V> values = new ArrayList<>(mirror.size());
                mirror.values().forEach(owned -> values.add(owned.value()));
                return new Completed<>(values, false, changed);
            }
        }
    }
}
