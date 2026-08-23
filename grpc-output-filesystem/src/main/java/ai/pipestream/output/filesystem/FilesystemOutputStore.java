package ai.pipestream.output.filesystem;

import ai.pipestream.output.OutputEnv;
import ai.pipestream.output.OutputObject;
import ai.pipestream.output.OutputStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/**
 * Writes artifacts under a local directory. ServiceLoader id
 * {@code filesystem}.
 */
public final class FilesystemOutputStore implements OutputStore {

    /** Store id. */
    public static final String ID = "filesystem";

    private Path root;

    /**
     * Creates an unopened store (ServiceLoader).
     */
    public FilesystemOutputStore() {
    }

    /**
     * Creates a store already bound to {@code root} (tests).
     *
     * @param root destination directory
     */
    public FilesystemOutputStore(Path root) {
        this.root = Objects.requireNonNull(root, "root");
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public boolean available(Map<String, String> env) {
        return OutputEnv.directory(env) != null || root != null;
    }

    @Override
    public void open(Map<String, String> env) throws IOException {
        if (root == null) {
            String dir = OutputEnv.directory(env);
            if (dir == null) {
                throw new IOException("filesystem store needs OUTPUT_DIR or OKF_DIR");
            }
            root = Path.of(dir);
        }
        Files.createDirectories(root);
    }

    @Override
    public void put(OutputObject object) throws IOException {
        if (root == null) {
            throw new IOException("filesystem store is not open");
        }
        Path target = root.resolve(object.key()).normalize();
        if (!target.startsWith(root.normalize())) {
            throw new IOException("refusing to write outside store root: " + object.key());
        }
        if (target.getParent() != null) {
            Files.createDirectories(target.getParent());
        }
        Files.write(target, object.content());
    }

    /**
     * Bound root, or {@code null} before {@link #open}.
     *
     * @return root
     */
    public Path root() {
        return root;
    }
}
