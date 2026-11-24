package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.confluence.output.ConfluenceObjectKeys;
import ai.pipestream.output.ObjectKeys;
import ai.pipestream.output.OutputEnv;
import ai.pipestream.output.OutputFormat;
import ai.pipestream.output.OutputFormats;
import ai.pipestream.output.OutputStore;
import ai.pipestream.output.OutputStores;
import ai.pipestream.sync.v1.ConnectionOutput;
import com.google.protobuf.Message;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Writes crawl artifacts through the output SPI: protobuf, JSON, OKF, and
 * any other loaded {@link OutputFormat}, into the selected
 * {@link OutputStore} (filesystem by default, S3 when that jar is loaded
 * and {@code OUTPUT_STORE=s3}).
 *
 * <p>Per-record formats write on {@link #emit}. End-of-run formats (OKF) are
 * written when a pass completes, from a mirror: the latest record per object
 * key. A full sweep of a connection replaces that connection's records in
 * the mirror with exactly what it emitted. A pass that was not a full sweep
 * merges its records into the mirror, so the bundle keeps what the pass did
 * not visit. When this sink has completed no full sweep of the connection
 * yet there is nothing to merge into, and the pass writes its records under
 * {@code {prefix}/increments/{run-id}} (with the connection id in between
 * when the sink serves several) instead of replacing the full bundle with a
 * partial one. A pass that was not a full sweep and emitted nothing writes
 * nothing.</p>
 */
public final class OutputChangeSink implements ChangeSink, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(OutputChangeSink.class.getName());

    private final OutputStore store;
    private final List<OutputFormat> formats;
    private final String prefix;
    private final RunMirror<Message> mirror = new RunMirror<>();
    private final RunMirror<Message>.Pass own = mirror.pass("");

    /**
     * Creates a sink.
     *
     * @param store opened store
     * @param formats selected formats
     * @param prefix store prefix
     */
    public OutputChangeSink(OutputStore store, List<OutputFormat> formats, String prefix) {
        this.store = Objects.requireNonNull(store, "store");
        this.formats = List.copyOf(formats);
        this.prefix = prefix == null ? "" : prefix;
    }

    /**
     * Whether a store destination is configured.
     *
     * @return true when {@link OutputEnv#destinationSet(Map)}
     */
    public static boolean enabled() {
        return OutputEnv.destinationSet(OutputEnv.process());
    }

    /**
     * Builds a sink from the process environment and ServiceLoader jars.
     *
     * @return the sink
     */
    public static OutputChangeSink fromEnvironment() {
        return from(OutputEnv.process(), OutputStores.load(), OutputFormats.load());
    }

    /**
     * Whether {@code output} names a filesystem directory or S3 bucket.
     *
     * @param output catalog or process output; {@code null} is unset
     * @return true when a destination is present
     */
    public static boolean configured(ConnectionOutput output) {
        return output != null
                && (!output.getDirectory().isEmpty() || !output.getS3Bucket().isEmpty());
    }

    /**
     * Opens a sink from a catalog or process {@link ConnectionOutput}.
     *
     * @param output destination binding
     * @return the sink
     */
    public static OutputChangeSink from(ConnectionOutput output) {
        Objects.requireNonNull(output, "output");
        if (!configured(output)) {
            throw new IllegalArgumentException("output directory or s3_bucket is required");
        }
        Map<String, String> env = new LinkedHashMap<>();
        if (!output.getStore().isEmpty()) {
            env.put(OutputEnv.STORE, output.getStore());
        }
        if (!output.getDirectory().isEmpty()) {
            env.put(OutputEnv.DIR, output.getDirectory());
        }
        if (!output.getPrefix().isEmpty()) {
            env.put(OutputEnv.PREFIX, output.getPrefix());
        }
        if (!output.getFormatsList().isEmpty()) {
            env.put(OutputEnv.FORMATS, String.join(",", output.getFormatsList()));
        }
        if (!output.getS3Bucket().isEmpty()) {
            env.put(OutputEnv.S3_BUCKET, output.getS3Bucket());
        }
        if (!output.getS3Prefix().isEmpty()) {
            env.put(OutputEnv.S3_PREFIX, output.getS3Prefix());
        }
        if (!output.getS3Region().isEmpty()) {
            env.put(OutputEnv.S3_REGION, output.getS3Region());
        }
        return from(env, OutputStores.load(), OutputFormats.load());
    }

    /**
     * Builds a sink from {@code env} and already-loaded catalogs.
     *
     * @param env environment
     * @param stores store catalog
     * @param formats format catalog
     * @return the sink
     */
    public static OutputChangeSink from(Map<String, String> env, OutputStores stores,
            OutputFormats formats) {
        try {
            OutputStore store = stores.select(env);
            List<OutputFormat> selected = formats.select(env);
            if (selected.isEmpty()) {
                throw new IllegalStateException("no output formats loaded for "
                        + OutputEnv.formats(env) + "; loaded=" + formats.ids());
            }
            LOG.log(System.Logger.Level.INFO,
                    "confluence-proxy output store={0} loadedStores={1} formats={2} prefix={3}",
                    store.id(), stores.ids(),
                    selected.stream().map(OutputFormat::id).toList(), OutputEnv.prefix(env));
            return new OutputChangeSink(store, selected, OutputEnv.prefix(env));
        } catch (IOException e) {
            throw new UncheckedIOException("output store failed to open", e);
        }
    }

    @Override
    public void emit(ConfluenceChange change) {
        emit(own, change);
    }

    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        // Snapshots are crawl markers; artifacts come from emit/complete.
    }

    @Override
    public void completeRun(String runId) {
        completeRun(runId, true);
    }

    @Override
    public void completeRun(String runId, boolean fullSweep) {
        complete(own, runId, fullSweep);
    }

    @Override
    public ChangeSink forConnection(String connectionId) {
        RunMirror<Message>.Pass pass = mirror.pass(connectionId);
        return new ChangeSink() {
            @Override
            public void emit(ConfluenceChange change) {
                OutputChangeSink.this.emit(pass, change);
            }

            @Override
            public void snapshot(ConfluenceSnapshot snapshot) {
                OutputChangeSink.this.snapshot(snapshot);
            }

            @Override
            public void completeRun(String runId) {
                complete(pass, runId, true);
            }

            @Override
            public void completeRun(String runId, boolean fullSweep) {
                complete(pass, runId, fullSweep);
            }
        };
    }

    private void emit(RunMirror<Message>.Pass pass, ConfluenceChange change) {
        String relative = ConfluenceObjectKeys.of(change);
        pass.put(relative, change);
        String key = ObjectKeys.under(prefix, relative);
        try {
            for (OutputFormat format : formats) {
                if (format.supports(change)) {
                    format.emit(store, change, key);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("output emit failed for " + key, e);
        }
    }

    private void complete(RunMirror<Message>.Pass pass, String runId, boolean fullSweep) {
        // One pass at a time writes the end-of-run artifacts.
        synchronized (mirror) {
            RunMirror.Completed<Message> completed = pass.complete(fullSweep);
            if (!completed.changed()) {
                return;
            }
            String target = completed.increment()
                    ? ObjectKeys.under(prefix, ObjectKeys.join("increments",
                            pass.connectionId().isEmpty()
                                    ? "" : ObjectKeys.segment(pass.connectionId()),
                            ObjectKeys.segment(runId)))
                    : prefix;
            try {
                for (OutputFormat format : formats) {
                    format.complete(store, completed.values(), target);
                }
                LOG.log(System.Logger.Level.INFO,
                        "confluence-proxy output wrote {0} records store={1} formats={2}"
                                + " prefix={3} fullSweep={4}",
                        completed.values().size(), store.id(),
                        formats.stream().map(OutputFormat::id).toList(), target, fullSweep);
            } catch (IOException e) {
                throw new UncheckedIOException("output complete failed for run " + runId, e);
            }
        }
    }

    /**
     * Selected store (tests).
     *
     * @return store
     */
    public OutputStore store() {
        return store;
    }

    /**
     * Selected formats (tests).
     *
     * @return formats
     */
    public List<OutputFormat> formats() {
        return formats;
    }

    /**
     * Closes the store.
     */
    @Override
    public void close() {
        store.close();
    }
}
