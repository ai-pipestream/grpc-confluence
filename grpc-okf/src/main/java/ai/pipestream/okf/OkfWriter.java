package ai.pipestream.okf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/**
 * Writes an {@link OkfBundle} as a directory tree.
 */
public final class OkfWriter {

    private OkfWriter() {
    }

    /**
     * Writes every file under {@code directory}, creating parents as needed.
     *
     * @param bundle the bundle
     * @param directory destination root
     * @return {@code directory}
     * @throws IOException if a file cannot be written
     */
    public static Path write(OkfBundle bundle, Path directory) throws IOException {
        Objects.requireNonNull(bundle, "bundle");
        Objects.requireNonNull(directory, "directory");
        Files.createDirectories(directory);
        for (Map.Entry<String, byte[]> entry : bundle.files().entrySet()) {
            Path target = directory.resolve(entry.getKey()).normalize();
            if (!target.startsWith(directory.normalize())) {
                throw new IOException("refusing to write outside bundle root: " + entry.getKey());
            }
            Files.createDirectories(target.getParent());
            Files.write(target, entry.getValue());
        }
        return directory;
    }
}
