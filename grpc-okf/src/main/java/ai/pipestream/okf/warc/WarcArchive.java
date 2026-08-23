package ai.pipestream.okf.warc;

import ai.pipestream.okf.CatalogEntry;
import ai.pipestream.okf.CollectionPage;
import ai.pipestream.okf.OkfVersion;
import ai.pipestream.okf.OkfYaml;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Builds a WARC 1.1 record list for a catalog: {@code warcinfo}, one
 * {@code resource} per live URI, one {@code conversion} per OKF markdown
 * (WARC-Refers-To the resource), and the HTML collection page as its own
 * {@code resource}. The OKF zip is not a record.
 */
public final class WarcArchive {

    /** Collection-page WARC-Target-URI. */
    public static final String COLLECTION_URI = "urn:okf:" + OkfVersion.VALUE + ":collection";

    private WarcArchive() {
    }

    /**
     * Conversion-record URI for a bundle-relative markdown path.
     *
     * @param path markdown path
     * @return {@code urn:okf:0.2:…}
     */
    public static String conversionUri(String path) {
        return "urn:okf:" + OkfVersion.VALUE + ":" + path.replace('\\', '/');
    }

    /**
     * Builds records.
     *
     * @param date WARC-Date for every record in this file
     * @param software warcinfo software token
     * @param title collection page title
     * @param description collection page intro
     * @param entries catalog entries
     * @param collectionHtml already-rendered HTML (hrefs must be live URIs)
     * @return records, warcinfo first
     */
    public static List<WarcRecord> records(Instant date, String software, String title,
            String description, List<CatalogEntry> entries, String collectionHtml) {
        Objects.requireNonNull(date, "date");
        Objects.requireNonNull(entries, "entries");
        List<WarcRecord> records = new ArrayList<>();
        records.add(WarcWriter.warcinfo(date, software));
        List<CollectionPage.Link> links = new ArrayList<>();
        for (CatalogEntry entry : entries) {
            String resourceId = WarcRecord.newRecordId();
            records.add(new WarcRecord(
                    WarcRecord.TYPE_RESOURCE,
                    resourceId,
                    date,
                    entry.targetUri(),
                    null,
                    entry.mediaType(),
                    entry.resourceBody(),
                    Map.of()));
            byte[] markdown = OkfYaml.render(entry.concept()).getBytes(StandardCharsets.UTF_8);
            records.add(new WarcRecord(
                    WarcRecord.TYPE_CONVERSION,
                    WarcRecord.newRecordId(),
                    date,
                    conversionUri(entry.path()),
                    resourceId,
                    "text/markdown",
                    markdown,
                    Map.of("WARC-Identified-Payload-Type", "text/markdown")));
            links.add(new CollectionPage.Link(
                    entry.targetUri(), entry.title(), entry.kind(), entry.path()));
        }
        String html = collectionHtml != null ? collectionHtml
                : CollectionPage.render(title, description, links);
        records.add(new WarcRecord(
                WarcRecord.TYPE_RESOURCE,
                WarcRecord.newRecordId(),
                date,
                COLLECTION_URI,
                null,
                "text/html",
                html.getBytes(StandardCharsets.UTF_8),
                Map.of()));
        return List.copyOf(records);
    }
}
