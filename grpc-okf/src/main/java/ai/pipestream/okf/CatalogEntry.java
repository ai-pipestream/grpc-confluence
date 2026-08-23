package ai.pipestream.okf;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * One captured entity ready for an OKF concept and a WARC {@code resource}
 * record. {@code targetUri} is the live URI used as {@code WARC-Target-URI}
 * and as the collection-page {@code href}.
 *
 * @param path bundle-relative markdown path
 * @param concept OKF concept
 * @param targetUri live resource URI
 * @param mediaType MIME type of {@code resourceBody}
 * @param resourceBody bytes stored in the WARC resource record (not the ZIP)
 * @param kind short type label for the collection page
 */
public record CatalogEntry(
        String path,
        OkfConcept concept,
        String targetUri,
        String mediaType,
        byte[] resourceBody,
        String kind) {

    /**
     * Copies the resource body.
     *
     * @param path markdown path
     * @param concept concept
     * @param targetUri live URI
     * @param mediaType MIME type
     * @param resourceBody WARC resource payload
     * @param kind collection-page kind
     */
    public CatalogEntry {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(concept, "concept");
        Objects.requireNonNull(targetUri, "targetUri");
        Objects.requireNonNull(mediaType, "mediaType");
        Objects.requireNonNull(resourceBody, "resourceBody");
        Objects.requireNonNull(kind, "kind");
        if (targetUri.isBlank()) {
            throw new IllegalArgumentException("targetUri is required");
        }
        resourceBody = resourceBody.clone();
    }

    /**
     * Display title for indexes and the collection page.
     *
     * @return title
     */
    public String title() {
        return concept.title().orElse(path);
    }

    /**
     * UTF-8 helper when the resource body is text.
     *
     * @param path markdown path
     * @param concept concept
     * @param targetUri live URI
     * @param mediaType MIME type
     * @param text resource text
     * @param kind kind
     * @return the entry
     */
    public static CatalogEntry text(String path, OkfConcept concept, String targetUri,
            String mediaType, String text, String kind) {
        byte[] bytes = text == null ? new byte[0] : text.getBytes(StandardCharsets.UTF_8);
        return new CatalogEntry(path, concept, targetUri, mediaType, bytes, kind);
    }
}
