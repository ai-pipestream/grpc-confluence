package ai.pipestream.output;

/**
 * Source-neutral store-key helpers. Each source's output module builds its
 * own hierarchy (for example {@code ConfluenceObjectKeys}) from these.
 * Segments are filesystem-safe; results have no leading slash.
 */
public final class ObjectKeys {

    private ObjectKeys() {
    }

    /**
     * Joins prefix and key with {@code /}, skipping blanks.
     *
     * @param prefix optional prefix
     * @param key relative key
     * @return combined key
     */
    public static String under(String prefix, String key) {
        if (prefix == null || prefix.isBlank()) {
            return key;
        }
        return join(prefix, key);
    }

    /**
     * Joins segments with {@code /}.
     *
     * @param parts path pieces
     * @return relative path
     */
    public static String join(String... parts) {
        StringBuilder out = new StringBuilder();
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String cleaned = part.replace('\\', '/');
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
        return out.isEmpty() ? "_" : out.toString();
    }

    /**
     * Sanitizes one path segment.
     *
     * @param raw raw id
     * @return safe segment
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
        return out.isEmpty() ? "_" : out.toString();
    }
}
