package ai.pipestream.okf;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.microsoft.v1.MicrosoftChange;
import ai.pipestream.okf.confluence.ConfluenceCatalog;
import ai.pipestream.okf.microsoft.MicrosoftCatalog;
import ai.pipestream.output.OutputFormat;
import ai.pipestream.output.OutputStore;
import com.google.protobuf.Message;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * OKF v0.2 + sibling WARC written through an {@link OutputStore} at
 * end of run. Per-record {@link #emit} is a no-op; {@link #complete}
 * assembles the bundle.
 */
public final class OkfOutputFormat implements OutputFormat {

    /** Format id. */
    public static final String ID = "okf";

    /**
     * Creates the format (ServiceLoader).
     */
    public OkfOutputFormat() {
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public boolean supports(Message record) {
        return record instanceof ConfluenceChange || record instanceof MicrosoftChange;
    }

    @Override
    public void complete(OutputStore store, List<Message> records, String prefix)
            throws IOException {
        List<CatalogEntry> confluence = new ArrayList<>();
        List<CatalogEntry> microsoft = new ArrayList<>();
        for (Message record : records) {
            if (record instanceof ConfluenceChange change) {
                ConfluenceCatalog.from(change).ifPresent(confluence::add);
            } else if (record instanceof MicrosoftChange change) {
                MicrosoftCatalog.from(change).ifPresent(microsoft::add);
            }
        }
        if (!confluence.isEmpty()) {
            KnowledgeBundle.assemble(
                    "Confluence crawl",
                    "OKF v0.2 capture of a ConfluenceService.Sync run.",
                    ConfluenceCatalog.ACTOR,
                    Instant.now(),
                    "grpc-confluence/okf-producer",
                    confluence).write(store, prefix);
        }
        if (!microsoft.isEmpty()) {
            KnowledgeBundle.assemble(
                    "Microsoft Graph crawl",
                    "OKF v0.2 capture of a MicrosoftService.Sync run.",
                    MicrosoftCatalog.ACTOR,
                    Instant.now(),
                    "grpc-microsoft/okf-producer",
                    microsoft).write(store, prefix);
        }
    }
}
