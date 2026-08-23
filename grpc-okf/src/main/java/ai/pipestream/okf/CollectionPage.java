package ai.pipestream.okf;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * HTML collection page that hyperlinks every archived entity at the same
 * URI used as {@code WARC-Target-URI}. This is a real web page for the
 * WARC {@code resource} record — not a ZIP stuffed into WARC.
 */
public final class CollectionPage {

    /**
     * One link on the collection page.
     *
     * @param href live or archive URI (same as WARC-Target-URI)
     * @param title link text
     * @param kind short type label
     * @param okfPath optional bundle-relative markdown path for humans opening the zip
     */
    public record Link(String href, String title, String kind, String okfPath) {
        /**
         * Creates a link.
         *
         * @param href URI
         * @param title title
         * @param kind kind
         * @param okfPath optional OKF path
         */
        public Link {
            Objects.requireNonNull(href, "href");
            Objects.requireNonNull(title, "title");
            Objects.requireNonNull(kind, "kind");
        }
    }

    private CollectionPage() {
    }

    /**
     * Renders the collection HTML.
     *
     * @param title page title
     * @param description intro paragraph
     * @param links entities
     * @return HTML 5 document
     */
    public static String render(String title, String description, List<Link> links) {
        List<Link> copy = new ArrayList<>(Objects.requireNonNull(links, "links"));
        StringBuilder out = new StringBuilder();
        out.append("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
        out.append("<meta charset=\"utf-8\">\n");
        out.append("<title>").append(escape(title)).append("</title>\n");
        out.append("</head>\n<body>\n");
        out.append("<h1>").append(escape(title)).append("</h1>\n");
        if (description != null && !description.isBlank()) {
            out.append("<p>").append(escape(description)).append("</p>\n");
        }
        out.append("<p>Each heading links the live resource URI captured as ");
        out.append("<code>WARC-Target-URI</code>. The Open Knowledge Format ");
        out.append("markdown for the same entity lives in the companion ZIP, not inside WARC.</p>\n");
        out.append("<ul>\n");
        for (Link link : copy) {
            out.append("<li>");
            out.append("<a href=\"").append(escape(link.href())).append("\">")
                    .append(escape(link.title())).append("</a>");
            out.append(" — ").append(escape(link.kind()));
            if (link.okfPath() != null && !link.okfPath().isBlank()) {
                out.append(" (<code>").append(escape(link.okfPath())).append("</code>)");
            }
            out.append("</li>\n");
        }
        out.append("</ul>\n</body>\n</html>\n");
        return out.toString();
    }

    private static String escape(String value) {
        return value.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
                .replace("\"", "&quot;");
    }
}
