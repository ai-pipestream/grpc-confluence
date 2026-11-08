package ai.pipestream.confluence.output;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.okf.CatalogEntry;
import ai.pipestream.okf.OkfCatalogSource;
import com.google.protobuf.Message;

import java.util.Optional;

/**
 * Registers {@link ConfluenceCatalog} with the {@code okf} output format.
 */
public final class ConfluenceOkfSource implements OkfCatalogSource {

    /** Source id. */
    public static final String ID = "confluence";

    /**
     * Creates the source (ServiceLoader).
     */
    public ConfluenceOkfSource() {
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public boolean supports(Message record) {
        return record instanceof ConfluenceChange;
    }

    @Override
    public Optional<CatalogEntry> entry(Message record) {
        return record instanceof ConfluenceChange change
                ? ConfluenceCatalog.from(change)
                : Optional.empty();
    }

    @Override
    public String title() {
        return "Confluence crawl";
    }

    @Override
    public String description() {
        return "OKF v0.2 capture of a ConfluenceService.Sync run.";
    }

    @Override
    public String actor() {
        return ConfluenceCatalog.ACTOR;
    }

    @Override
    public String software() {
        return "grpc-confluence/okf-producer";
    }
}
