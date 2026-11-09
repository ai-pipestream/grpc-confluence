package ai.pipestream.okf;

import ai.pipestream.output.OutputFormat;
import ai.pipestream.output.OutputStore;
import com.google.protobuf.Message;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * OKF v0.2 + sibling WARC written through an {@link OutputStore} at
 * end of run. Per-record {@link #emit} is a no-op; {@link #complete}
 * assembles one bundle per {@link OkfCatalogSource} that saw records.
 * Sources are discovered with {@link ServiceLoader}.
 */
public final class OkfOutputFormat implements OutputFormat {

    /** Format id. */
    public static final String ID = "okf";

    private final List<OkfCatalogSource> sources;

    /**
     * Creates the format with every {@link OkfCatalogSource} on the
     * classpath (ServiceLoader).
     */
    public OkfOutputFormat() {
        this(load());
    }

    /**
     * Creates the format with a fixed source list.
     *
     * @param sources catalog sources, in bundle-writing order
     */
    public OkfOutputFormat(List<OkfCatalogSource> sources) {
        this.sources = List.copyOf(sources);
    }

    private static List<OkfCatalogSource> load() {
        List<OkfCatalogSource> found = new ArrayList<>();
        for (OkfCatalogSource source : ServiceLoader.load(OkfCatalogSource.class,
                OkfCatalogSource.class.getClassLoader())) {
            found.add(source);
        }
        return found;
    }

    /**
     * Loaded catalog sources.
     *
     * @return the sources
     */
    public List<OkfCatalogSource> sources() {
        return sources;
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public boolean supports(Message record) {
        return source(record).isPresent();
    }

    @Override
    public void complete(OutputStore store, List<Message> records, String prefix)
            throws IOException {
        Map<OkfCatalogSource, List<CatalogEntry>> bySource = new LinkedHashMap<>();
        for (OkfCatalogSource source : sources) {
            bySource.put(source, new ArrayList<>());
        }
        for (Message record : records) {
            Optional<OkfCatalogSource> source = source(record);
            if (source.isPresent()) {
                source.get().entry(record).ifPresent(bySource.get(source.get())::add);
            }
        }
        for (Map.Entry<OkfCatalogSource, List<CatalogEntry>> run : bySource.entrySet()) {
            if (run.getValue().isEmpty()) {
                continue;
            }
            OkfCatalogSource source = run.getKey();
            KnowledgeBundle.assemble(
                    source.title(),
                    source.description(),
                    source.actor(),
                    Instant.now(),
                    source.software(),
                    run.getValue()).write(store, prefix);
        }
    }

    private Optional<OkfCatalogSource> source(Message record) {
        for (OkfCatalogSource source : sources) {
            if (source.supports(record)) {
                return Optional.of(source);
            }
        }
        return Optional.empty();
    }
}
