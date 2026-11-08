package ai.pipestream.confluence;

import java.util.ArrayList;
import java.util.List;

/**
 * Plain character-loop string helpers, so production code never reaches for
 * the regex engine behind {@code String.split}, {@code replaceAll} or
 * {@code matches}.
 */
final class Strings {

    private Strings() {
    }

    /** {@code value} without any trailing {@code '/'} characters. */
    static String stripTrailingSlashes(String value) {
        int end = value.length();
        while (end > 0 && value.charAt(end - 1) == '/') {
            end--;
        }
        return value.substring(0, end);
    }

    /**
     * Splits on a single character with {@code String.split} semantics for a
     * literal separator: trailing empty parts are dropped, and a value
     * without the separator is one part.
     */
    static List<String> split(String value, char separator) {
        List<String> parts = new ArrayList<>();
        int from = 0;
        while (true) {
            int at = value.indexOf(separator, from);
            if (at < 0) {
                parts.add(value.substring(from));
                break;
            }
            parts.add(value.substring(from, at));
            from = at + 1;
        }
        int size = parts.size();
        while (size > 1 && parts.get(size - 1).isEmpty()) {
            parts.remove(--size);
        }
        if (size == 1 && parts.getFirst().isEmpty() && !value.isEmpty()) {
            parts.clear(); // only separators: String.split answers no parts
        }
        return parts;
    }

    /** Whether {@code c} is an ASCII hexadecimal digit. */
    static boolean isHex(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }

    /** Whether {@code value} is a canonical 8-4-4-4-12 hexadecimal UUID. */
    static boolean isUuid(String value) {
        if (value.length() != 36) {
            return false;
        }
        for (int i = 0; i < 36; i++) {
            char c = value.charAt(i);
            boolean dash = i == 8 || i == 13 || i == 18 || i == 23;
            if (dash ? c != '-' : !isHex(c)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Whether {@code value} looks like {@code local@domain.tld}: one
     * {@code '@'} with a non-empty local part, no spaces anywhere, and a
     * domain holding a {@code '.'} that is neither its first nor its last
     * character.
     */
    static boolean isEmailShaped(String value) {
        if (value.indexOf(' ') >= 0) {
            return false;
        }
        int at = value.indexOf('@');
        if (at <= 0 || value.indexOf('@', at + 1) >= 0) {
            return false;
        }
        String domain = value.substring(at + 1);
        int dot = domain.indexOf('.', 1);
        return dot > 0 && dot < domain.length() - 1;
    }
}
