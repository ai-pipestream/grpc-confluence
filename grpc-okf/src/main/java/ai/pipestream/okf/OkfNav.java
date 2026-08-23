package ai.pipestream.okf;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Builds {@code index.md} (§8) and {@code log.md} (§9) for an OKF bundle.
 */
public final class OkfNav {

    private static final DateTimeFormatter DAY = DateTimeFormatter.ISO_LOCAL_DATE;

    private OkfNav() {
    }

    /**
     * Bundle-root index with {@code okf_version} frontmatter.
     *
     * @param title bundle title
     * @param description one-line description
     * @param sections heading → list of {@code relative-url | title | description}
     * @return markdown
     */
    public static String rootIndex(String title, String description, Map<String, List<IndexEntry>> sections) {
        StringBuilder out = new StringBuilder();
        out.append("---\n");
        out.append("okf_version: \"").append(OkfVersion.VALUE).append("\"\n");
        out.append("---\n\n");
        out.append("# ").append(title).append("\n\n");
        if (description != null && !description.isBlank()) {
            out.append(description.trim()).append("\n\n");
        }
        appendSections(out, sections);
        return out.toString();
    }

    /**
     * Nested directory index: no frontmatter.
     *
     * @param heading directory heading
     * @param sections sections
     * @return markdown
     */
    public static String nestedIndex(String heading, Map<String, List<IndexEntry>> sections) {
        StringBuilder out = new StringBuilder();
        out.append("# ").append(heading).append("\n\n");
        appendSections(out, sections);
        return out.toString();
    }

    /**
     * A log with one Creation entry for {@code at}, newest-first date heading.
     *
     * @param scopeTitle heading
     * @param at when the run happened
     * @param lines log bullets (already include the leading {@code * **Creation**: …})
     * @return markdown
     */
    public static String log(String scopeTitle, Instant at, List<String> lines) {
        Objects.requireNonNull(at, "at");
        String day = DAY.format(at.atOffset(ZoneOffset.UTC));
        StringBuilder out = new StringBuilder();
        out.append("# ").append(scopeTitle).append(" Update Log\n\n");
        out.append("## ").append(day).append('\n');
        if (lines == null || lines.isEmpty()) {
            out.append("* **Initialization**: Created foundational directory structure.\n");
        } else {
            for (String line : lines) {
                out.append(line);
                if (!line.endsWith("\n")) {
                    out.append('\n');
                }
            }
        }
        return out.toString();
    }

    /**
     * One {@code index.md} bullet.
     *
     * @param href relative url
     * @param title link text
     * @param description short description
     */
    public record IndexEntry(String href, String title, String description) {
        /**
         * Creates an entry.
         *
         * @param href relative url
         * @param title link text
         * @param description short description
         */
        public IndexEntry {
            Objects.requireNonNull(href, "href");
            Objects.requireNonNull(title, "title");
        }
    }

    /**
     * Groups files under first path segment for automatic indexes.
     *
     * @param markdownPaths concept markdown paths
     * @param titles path → title
     * @param descriptions path → description
     * @return section map
     */
    public static Map<String, List<IndexEntry>> groupByDirectory(Iterable<String> markdownPaths,
            Map<String, String> titles, Map<String, String> descriptions) {
        Map<String, List<IndexEntry>> sections = new TreeMap<>();
        for (String path : markdownPaths) {
            if (!path.endsWith(".md") || OkfPaths.reserved(filename(path))) {
                continue;
            }
            int slash = path.indexOf('/');
            String section = slash < 0 ? "Concepts" : path.substring(0, slash);
            String href = slash < 0 ? path : path.substring(slash + 1);
            if (slash >= 0 && path.indexOf('/', slash + 1) >= 0) {
                href = path.substring(slash + 1);
            }
            String title = titles.getOrDefault(path, path);
            String description = descriptions.getOrDefault(path, "");
            sections.computeIfAbsent(section, k -> new ArrayList<>())
                    .add(new IndexEntry(href, title, description));
        }
        return sections;
    }

    private static void appendSections(StringBuilder out, Map<String, List<IndexEntry>> sections) {
        if (sections == null) {
            return;
        }
        for (Map.Entry<String, List<IndexEntry>> section : sections.entrySet()) {
            out.append("# ").append(section.getKey()).append("\n\n");
            for (IndexEntry entry : section.getValue()) {
                out.append("* [").append(entry.title()).append("](").append(entry.href()).append(")");
                if (entry.description() != null && !entry.description().isBlank()) {
                    out.append(" - ").append(entry.description());
                }
                out.append('\n');
            }
            out.append('\n');
        }
    }

    private static String filename(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? path : path.substring(slash + 1);
    }
}
