package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Writes crawl artifacts through the output SPI: protobuf, JSON, OKF, and
 * any other loaded {@link OutputFormat}, into the selected
 * {@link OutputStore} (filesystem by default, S3 when that jar is loaded
 * and {@code OUTPUT_STORE=s3}).
 */
public final class OutputChangeSink implements ChangeSink, AutoCloseable {

    private static final System.Logger LOG = System.getLogger(OutputChangeSink.class.getName());

    private final OutputStore store;
    private final List<OutputFormat> formats;
    private final String prefix;
    private final List<Message> records = new CopyOnWriteArrayList<>();

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
                    store.id(), stores.ids(), formats.ids(), OutputEnv.prefix(env));
            return new OutputChangeSink(store, selected, OutputEnv.prefix(env));
        } catch (IOException e) {
            throw new UncheckedIOException("output store failed to open", e);
        }
    }

    @Override
    public void emit(ConfluenceChange change) {
        records.add(change);
        String key = ObjectKeys.under(prefix, ObjectKeys.of(change));
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

    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        // Snapshots are crawl markers; artifacts come from emit/complete.
    }

    @Override
    public void completeRun(String runId) {
        try {
            for (OutputFormat format : formats) {
                format.complete(store, new ArrayList<>(records), prefix);
            }
            LOG.log(System.Logger.Level.INFO,
                    "confluence-proxy output wrote {0} records store={1} formats={2}",
                    records.size(), store.id(),
                    formats.stream().map(OutputFormat::id).toList());
        } catch (IOException e) {
            throw new UncheckedIOException("output complete failed for run " + runId, e);
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
