package ai.pipestream.okf;

import ai.pipestream.okf.warc.WarcArchive;
import ai.pipestream.okf.warc.WarcRecord;
import ai.pipestream.okf.warc.WarcWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Assembles catalog entries into a conformant OKF bundle plus a sibling WARC
 * 1.1 file. Writes the directory tree, zip, and {@code .warc.gz} independently
 * according to {@link OkfOutput}.
 */
public final class KnowledgeBundle {

    private final OkfBundle bundle;
    private final List<WarcRecord> warcRecords;
    private final String collectionHtml;

    private KnowledgeBundle(OkfBundle bundle, List<WarcRecord> warcRecords, String collectionHtml) {
        this.bundle = bundle;
        this.warcRecords = List.copyOf(warcRecords);
        this.collectionHtml = collectionHtml;
    }

    /**
     * Builds a bundle from catalog entries.
     *
     * @param title root index title
     * @param description root index and collection intro
     * @param generatedBy actor string
     * @param at generation instant
     * @param software warcinfo software token
     * @param entries captured entities
     * @return the assembled bundle (already §11-checked)
     */
    public static KnowledgeBundle assemble(String title, String description, String generatedBy,
            Instant at, String software, List<CatalogEntry> entries) {
        Objects.requireNonNull(title, "title");
        Objects.requireNonNull(generatedBy, "generatedBy");
        Objects.requireNonNull(at, "at");
        Objects.requireNonNull(software, "software");
        List<CatalogEntry> copy = List.copyOf(Objects.requireNonNull(entries, "entries"));
        OkfBundle bundle = new OkfBundle();
        Map<String, String> titles = new LinkedHashMap<>();
        Map<String, String> descriptions = new LinkedHashMap<>();
        List<String> markdownPaths = new ArrayList<>();
        List<CollectionPage.Link> links = new ArrayList<>();
        for (CatalogEntry entry : copy) {
            bundle.putConcept(entry.path(), entry.concept());
            markdownPaths.add(entry.path());
            titles.put(entry.path(), entry.title());
            descriptions.put(entry.path(), entry.concept().description().orElse(""));
            links.add(new CollectionPage.Link(
                    entry.targetUri(), entry.title(), entry.kind(), entry.path()));
        }
        bundle.putConcept(OkfSkills.COMPUTATION_PATH, OkfSkills.computation(generatedBy, at));
        bundle.putConcept(OkfSkills.EXECUTOR_PATH, OkfSkills.executorSkill(generatedBy, at));
        bundle.putConcept(OkfSkills.ATTESTER_PATH, OkfSkills.attester(generatedBy, at));
        markdownPaths.add(OkfSkills.COMPUTATION_PATH);
        titles.put(OkfSkills.COMPUTATION_PATH, "Run gRPC Sync");
        descriptions.put(OkfSkills.COMPUTATION_PATH, "Attested Computation contract");
        Map<String, List<OkfNav.IndexEntry>> sections =
                OkfNav.groupByDirectory(markdownPaths, titles, descriptions);
        bundle.putText("index.md", OkfNav.rootIndex(title, description, sections));
        putNestedIndexes(bundle, copy);
        bundle.putText("log.md", OkfNav.log(title, at, List.of(
                "* **Creation**: Generated " + copy.size()
                        + " concepts from a gRPC Sync run by " + generatedBy + ".")));
        String collectionHtml = CollectionPage.render(title, description, links);
        bundle.putBytes("collection.html", collectionHtml.getBytes(StandardCharsets.UTF_8));
        OkfConformance.require(bundle);
        List<WarcRecord> warc = WarcArchive.records(at, software, title, description, copy,
                collectionHtml);
        return new KnowledgeBundle(bundle, warc, collectionHtml);
    }

    /**
     * The OKF files.
     *
     * @return the bundle
     */
    public OkfBundle okf() {
        return bundle;
    }

    /**
     * WARC records (warcinfo first).
     *
     * @return records
     */
    public List<WarcRecord> warcRecords() {
        return warcRecords;
    }

    /**
     * Collection HTML (same bytes as {@code collection.html} and the WARC
     * collection resource).
     *
     * @return HTML
     */
    public String collectionHtml() {
        return collectionHtml;
    }

    /**
     * Writes the configured destinations. Missing paths are skipped.
     *
     * @param output destinations
     * @return this bundle
     * @throws IOException if a write fails
     */
    public KnowledgeBundle write(OkfOutput output) throws IOException {
        Objects.requireNonNull(output, "output");
        if (output.directory() != null) {
            OkfWriter.write(bundle, output.directory());
        }
        if (output.zip() != null) {
            OkfZip.write(bundle, output.zip());
        }
        if (output.warc() != null) {
            WarcWriter.writeGzip(output.warc(), warcRecords);
        }
        return this;
    }

    private static void putNestedIndexes(OkfBundle bundle, List<CatalogEntry> entries) {
        Map<String, List<OkfNav.IndexEntry>> byDir = new TreeMap<>();
        for (CatalogEntry entry : entries) {
            int slash = entry.path().indexOf('/');
            if (slash < 0) {
                continue;
            }
            String dir = entry.path().substring(0, slash);
            String href = entry.path().substring(slash + 1);
            byDir.computeIfAbsent(dir, k -> new ArrayList<>())
                    .add(new OkfNav.IndexEntry(href, entry.title(),
                            entry.concept().description().orElse("")));
        }
        for (Map.Entry<String, List<OkfNav.IndexEntry>> dir : byDir.entrySet()) {
            String indexPath = dir.getKey() + "/index.md";
            if (!bundle.contains(indexPath)) {
                bundle.putText(indexPath, OkfNav.nestedIndex(dir.getKey(),
                        Map.of(dir.getKey(), dir.getValue())));
            }
        }
    }
}
