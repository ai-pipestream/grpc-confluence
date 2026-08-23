package ai.pipestream.okf;

import java.util.Locale;
import java.util.Objects;

/**
 * Path helpers for OKF concept IDs and zip/directory entries. A concept ID
 * is the bundle-relative path with the {@code .md} suffix removed (§2).
 */
public final class OkfPaths {

    private OkfPaths() {
    }

    /**
     * Sanitizes one path segment so it is a safe filename. Letters, digits,
     * {@code .}, {@code -}, and {@code _} are kept; everything else becomes
     * {@code _}. Empty input becomes {@code _}.
     *
     * @param raw an id, key, or name
     * @return a filesystem-safe segment
     */
    public static String segment(String raw) {
        if (raw == null || raw.isBlank()) {
            return "_";
        }
        StringBuilder out = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '.' || c == '-' || c == '_') {
                out.append(c);
            } else {
                out.append('_');
            }
        }
        String value = out.toString();
        return value.isEmpty() ? "_" : value;
    }

    /**
     * Joins segments with {@code /}, skipping blanks. Result has no leading slash.
     *
     * @param segments path pieces
     * @return a relative path
     */
    public static String join(String... segments) {
        StringBuilder out = new StringBuilder();
        for (String segment : segments) {
            if (segment == null || segment.isBlank()) {
                continue;
            }
            String cleaned = segment.replace('\\', '/');
            while (cleaned.startsWith("/")) {
                cleaned = cleaned.substring(1);
            }
            while (cleaned.endsWith("/")) {
                cleaned = cleaned.substring(0, cleaned.length() - 1);
            }
            if (cleaned.isEmpty()) {
                continue;
            }
            if (out.length() > 0) {
                out.append('/');
            }
            out.append(cleaned);
        }
        return out.toString();
    }

    /**
     * Concept ID for a markdown file path ({@code pages/200.md} → {@code pages/200}).
     *
     * @param markdownPath bundle-relative path ending in {@code .md}
     * @return the concept ID
     */
    public static String conceptId(String markdownPath) {
        Objects.requireNonNull(markdownPath, "markdownPath");
        String path = markdownPath.replace('\\', '/');
        if (path.toLowerCase(Locale.ROOT).endsWith(".md")) {
            return path.substring(0, path.length() - 3);
        }
        return path;
    }

    /**
     * Bundle-relative markdown href beginning with {@code /} (§6.1 recommended form).
     *
     * @param markdownPath path such as {@code pages/200.md}
     * @return {@code /pages/200.md}
     */
    public static String href(String markdownPath) {
        String path = markdownPath.replace('\\', '/');
        return path.startsWith("/") ? path : "/" + path;
    }

    /**
     * Whether {@code filename} is reserved ({@code index.md} or {@code log.md}).
     *
     * @param filename a file name, not a path
     * @return true when the name is reserved
     */
    public static boolean reserved(String filename) {
        return "index.md".equals(filename) || "log.md".equals(filename);
    }
}
