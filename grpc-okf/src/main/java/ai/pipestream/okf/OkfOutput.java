package ai.pipestream.okf;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;

/**
 * Destination paths for an OKF directory tree, zip, and sibling WARC file.
 * Set {@code OKF_DIR} and the zip/warc default beside that directory;
 * {@code OKF_ZIP} / {@code OKF_WARC} override those defaults.
 *
 * @param directory OKF tree root, or {@code null}
 * @param zip zip path, or {@code null}
 * @param warc gzip WARC path, or {@code null}
 */
public record OkfOutput(Path directory, Path zip, Path warc) {

    /** Environment variable for the OKF directory tree. */
    public static final String ENV_DIR = "OKF_DIR";
    /** Environment variable for the OKF zip. */
    public static final String ENV_ZIP = "OKF_ZIP";
    /** Environment variable for the sibling {@code .warc.gz}. */
    public static final String ENV_WARC = "OKF_WARC";

    /**
     * Reads destinations from {@code env}.
     *
     * @param env environment map
     * @return destinations; all-null when nothing is set
     */
    public static OkfOutput from(Map<String, String> env) {
        Objects.requireNonNull(env, "env");
        String dir = blankToNull(env.get(ENV_DIR));
        String zip = blankToNull(env.get(ENV_ZIP));
        String warc = blankToNull(env.get(ENV_WARC));
        Path directory = dir == null ? null : Path.of(dir);
        Path zipPath = zip == null ? null : Path.of(zip);
        Path warcPath = warc == null ? null : Path.of(warc);
        if (directory != null) {
            String baseName = directory.getFileName() == null
                    ? "bundle" : directory.getFileName().toString();
            Path parent = directory.getParent() == null ? Path.of(".") : directory.getParent();
            if (zipPath == null) {
                zipPath = parent.resolve(baseName + ".zip");
            }
            if (warcPath == null) {
                warcPath = parent.resolve(baseName + ".warc.gz");
            }
        }
        return new OkfOutput(directory, zipPath, warcPath);
    }

    /**
     * Reads destinations from the process environment.
     *
     * @return destinations
     */
    public static OkfOutput fromEnvironment() {
        return from(System.getenv());
    }

    /**
     * Whether any destination is set.
     *
     * @return true when at least one path is present
     */
    public boolean enabled() {
        return directory != null || zip != null || warc != null;
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
