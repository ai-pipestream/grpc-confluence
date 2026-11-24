package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.okf.CatalogEntry;
import ai.pipestream.okf.KnowledgeBundle;
import ai.pipestream.okf.OkfOutput;
import ai.pipestream.okf.OkfPaths;
import ai.pipestream.confluence.output.ConfluenceCatalog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

/**
 * Accumulates Confluence changes into an OKF v0.2 directory tree, zip, and
 * sibling WARC 1.1 file when {@code OKF_DIR} / {@code OKF_ZIP} /
 * {@code OKF_WARC} are set. Materializes when a pass completes, from a
 * mirror: the latest entry per concept path.
 *
 * <p>A full sweep of a connection replaces that connection's entries in the
 * mirror with exactly what it emitted. A pass that was not a full sweep
 * merges its entries into the mirror, so the bundle keeps what the pass did
 * not visit. When this sink has completed no full sweep of the connection
 * yet there is nothing to merge into, and the pass writes its entries beside
 * the configured destinations, each renamed with
 * {@code -increments-{run-id}} (with the connection id before the run id
 * when the sink serves several), instead of replacing the full bundle with a
 * partial one. A pass that was not a full sweep and emitted nothing writes
 * nothing.</p>
 */
public final class OkfChangeSink implements ChangeSink {

    private static final System.Logger LOG = System.getLogger(OkfChangeSink.class.getName());

    private final OkfOutput output;
    private final RunMirror<CatalogEntry> mirror = new RunMirror<>();
    private final RunMirror<CatalogEntry>.Pass own = mirror.pass("");

    /**
     * Creates a sink that writes {@code output} when a pass completes.
     *
     * @param output destinations
     */
    public OkfChangeSink(OkfOutput output) {
        this.output = output;
    }

    /**
     * Whether any OKF destination is configured on the process environment.
     *
     * @return true when the sink should be wired
     */
    public static boolean enabled() {
        return OkfOutput.fromEnvironment().enabled();
    }

    /**
     * Builds a sink from {@code OKF_DIR} / {@code OKF_ZIP} / {@code OKF_WARC}.
     *
     * @return the sink
     */
    public static OkfChangeSink fromEnvironment() {
        return new OkfChangeSink(OkfOutput.fromEnvironment());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void emit(ConfluenceChange change) {
        emit(own, change);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        // Snapshots are crawl markers; concepts come from emit().
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void completeRun(String runId) {
        completeRun(runId, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void completeRun(String runId, boolean fullSweep) {
        complete(own, runId, fullSweep);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ChangeSink forConnection(String connectionId) {
        RunMirror<CatalogEntry>.Pass pass = mirror.pass(connectionId);
        return new ChangeSink() {
            @Override
            public void emit(ConfluenceChange change) {
                OkfChangeSink.emit(pass, change);
            }

            @Override
            public void snapshot(ConfluenceSnapshot snapshot) {
                OkfChangeSink.this.snapshot(snapshot);
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

    private static void emit(RunMirror<CatalogEntry>.Pass pass, ConfluenceChange change) {
        ConfluenceCatalog.from(change).ifPresent(entry -> pass.put(entry.path(), entry));
    }

    private void complete(RunMirror<CatalogEntry>.Pass pass, String runId, boolean fullSweep) {
        // One pass at a time writes the bundle.
        synchronized (mirror) {
            RunMirror.Completed<CatalogEntry> completed = pass.complete(fullSweep);
            if (!completed.changed()) {
                return;
            }
            List<CatalogEntry> captured = completed.values();
            String suffix = "increments-"
                    + (pass.connectionId().isEmpty()
                            ? "" : OkfPaths.segment(pass.connectionId()) + "-")
                    + OkfPaths.segment(runId);
            OkfOutput dest = completed.increment() ? beside(output, suffix) : output;
            try {
                KnowledgeBundle.assemble(
                        "Confluence crawl",
                        "OKF v0.2 capture of a ConfluenceService.Sync run.",
                        ConfluenceCatalog.ACTOR,
                        Instant.now(),
                        "grpc-confluence/okf-producer",
                        captured).write(dest);
                LOG.log(System.Logger.Level.INFO,
                        "confluence-proxy OKF wrote {0} concepts dir={1} zip={2} warc={3}"
                                + " fullSweep={4}",
                        captured.size(), dest.directory(), dest.zip(), dest.warc(), fullSweep);
            } catch (IOException e) {
                throw new UncheckedIOException("OKF write failed for run " + runId, e);
            }
        }
    }

    /** The configured destinations renamed with {@code suffix}, beside the originals. */
    static OkfOutput beside(OkfOutput output, String suffix) {
        return new OkfOutput(
                output.directory() == null ? null
                        : output.directory().resolveSibling(name(output.directory()) + "-" + suffix),
                output.zip() == null ? null : output.zip().resolveSibling(
                        stem(name(output.zip()), ".zip") + "-" + suffix + ".zip"),
                output.warc() == null ? null : output.warc().resolveSibling(
                        stem(name(output.warc()), ".warc.gz") + "-" + suffix + ".warc.gz"));
    }

    private static String name(Path path) {
        return path.getFileName() == null ? "bundle" : path.getFileName().toString();
    }

    private static String stem(String name, String extension) {
        return name.endsWith(extension)
                ? name.substring(0, name.length() - extension.length()) : name;
    }
}
