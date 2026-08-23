package ai.pipestream.sync;

import java.util.Locale;
import java.util.Set;
import java.util.UUID;

/**
 * Allocates stable {@code connection_id} values for the catalog.
 */
public final class ConnectionIds {

    private ConnectionIds() {
    }

    /**
     * Returns {@code requested} when it is already a usable id, otherwise a
     * slug of {@code displayName}, uniquified against {@code taken}.
     *
     * @param requested caller-supplied id; may be blank
     * @param displayName human name to slug
     * @param taken ids already in the catalog
     * @return a non-blank id
     */
    public static String allocate(String requested, String displayName, Set<String> taken) {
        String base = slug(requested);
        if (base.isEmpty()) {
            base = slug(displayName);
        }
        if (base.isEmpty()) {
            base = "conn-" + UUID.randomUUID().toString().substring(0, 8);
        }
        if (!taken.contains(base)) {
            return base;
        }
        for (int i = 2; i < 10_000; i++) {
            String candidate = base + "-" + i;
            if (!taken.contains(candidate)) {
                return candidate;
            }
        }
        return base + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Lowercase slug: letters, digits, and dashes.
     *
     * @param raw raw text
     * @return slug, possibly empty
     */
    public static String slug(String raw) {
        if (raw == null || raw.isBlank()) {
            return "";
        }
        StringBuilder out = new StringBuilder();
        boolean dash = false;
        for (int i = 0; i < raw.length(); i++) {
            char c = Character.toLowerCase(raw.charAt(i));
            if (Character.isLetterOrDigit(c)) {
                out.append(c);
                dash = false;
            } else if (!dash && out.length() > 0) {
                out.append('-');
                dash = true;
            }
        }
        if (out.length() > 0 && out.charAt(out.length() - 1) == '-') {
            out.setLength(out.length() - 1);
        }
        return out.toString().toLowerCase(Locale.ROOT);
    }
}
