package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.okf.CatalogEntry;
import ai.pipestream.okf.KnowledgeBundle;
import ai.pipestream.okf.OkfOutput;
import ai.pipestream.okf.confluence.ConfluenceCatalog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Accumulates Confluence changes into an OKF v0.2 directory tree, zip, and
 * sibling WARC 1.1 file when {@code OKF_DIR} / {@code OKF_ZIP} /
 * {@code OKF_WARC} are set. Materializes on {@link #completeRun(String)}.
 */
public final class OkfChangeSink implements ChangeSink {

    private static final System.Logger LOG = System.getLogger(OkfChangeSink.class.getName());

    private final OkfOutput output;
    private final Map<String, CatalogEntry> entries = new ConcurrentHashMap<>();

    /**
     * Creates a sink that writes {@code output} on {@link #completeRun(String)}.
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
        ConfluenceCatalog.from(change).ifPresent(entry -> entries.put(entry.path(), entry));
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
        List<CatalogEntry> captured = new ArrayList<>(entries.values());
        try {
            KnowledgeBundle.assemble(
                    "Confluence crawl",
                    "OKF v0.2 capture of a ConfluenceService.Sync run.",
                    ConfluenceCatalog.ACTOR,
                    Instant.now(),
                    "grpc-confluence/okf-producer",
                    captured).write(output);
            LOG.log(System.Logger.Level.INFO,
                    "confluence-proxy OKF wrote {0} concepts dir={1} zip={2} warc={3}",
                    captured.size(), output.directory(), output.zip(), output.warc());
        } catch (IOException e) {
            throw new UncheckedIOException("OKF write failed for run " + runId, e);
        }
    }
}
