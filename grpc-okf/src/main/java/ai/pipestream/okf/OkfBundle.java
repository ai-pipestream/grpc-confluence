package ai.pipestream.okf;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * In-memory OKF bundle: markdown concepts plus any {@code references/}
 * binaries. Keys are bundle-relative paths using {@code /}.
 */
public final class OkfBundle {

    private final Map<String, byte[]> files = new TreeMap<>();

    /**
     * Creates an empty bundle.
     */
    public OkfBundle() {
    }

    /**
     * Puts a UTF-8 text file.
     *
     * @param path relative path
     * @param utf8 contents
     */
    public void putText(String path, String utf8) {
        putBytes(path, Objects.requireNonNull(utf8, "utf8").getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Puts a concept document at {@code path} (must end in {@code .md} and
     * not be a reserved name).
     *
     * @param path relative markdown path
     * @param concept the concept
     */
    public void putConcept(String path, OkfConcept concept) {
        String normalized = normalize(path);
        String filename = filename(normalized);
        if (OkfPaths.reserved(filename)) {
            throw new IllegalArgumentException(path + " is reserved; use putText for index/log");
        }
        if (!normalized.endsWith(".md")) {
            throw new IllegalArgumentException(path + " must end with .md");
        }
        putText(normalized, OkfYaml.render(concept));
    }

    /**
     * Puts raw bytes (binaries under {@code references/}, HTML collection, …).
     *
     * @param path relative path
     * @param bytes contents
     */
    public void putBytes(String path, byte[] bytes) {
        files.put(normalize(path), bytes.clone());
    }

    /**
     * All files in path order.
     *
     * @return unmodifiable map
     */
    public Map<String, byte[]> files() {
        return Collections.unmodifiableMap(files);
    }

    /**
     * Whether a path is present.
     *
     * @param path relative path
     * @return true when present
     */
    public boolean contains(String path) {
        return files.containsKey(normalize(path));
    }

    static String normalize(String path) {
        String cleaned = Objects.requireNonNull(path, "path").replace('\\', '/');
        while (cleaned.startsWith("/")) {
            cleaned = cleaned.substring(1);
        }
        if (cleaned.isBlank() || cleaned.contains("..")) {
            throw new IllegalArgumentException("illegal bundle path: " + path);
        }
        return cleaned;
    }

    private static String filename(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? path : path.substring(slash + 1);
    }
}
