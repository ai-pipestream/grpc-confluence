package ai.pipestream.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Discovers {@link OutputFormat} implementations. Built-in formats live in
 * this jar; OKF registers from {@code grpc-okf}.
 */
public final class OutputFormats {

    private final List<OutputFormat> formats;

    private OutputFormats(List<OutputFormat> formats) {
        this.formats = List.copyOf(formats);
    }

    /**
     * Loads formats from the SPI class loader.
     *
     * @return discovered formats
     */
    public static OutputFormats load() {
        return load(OutputFormat.class.getClassLoader());
    }

    /**
     * Loads formats from {@code classLoader}.
     *
     * @param classLoader loader
     * @return discovered formats
     */
    public static OutputFormats load(ClassLoader classLoader) {
        List<OutputFormat> found = new ArrayList<>();
        for (OutputFormat format : ServiceLoader.load(OutputFormat.class, classLoader)) {
            found.add(format);
        }
        return new OutputFormats(found);
    }

    /**
     * Fixed list (tests).
     *
     * @param formats formats
     * @return wrapper
     */
    public static OutputFormats of(OutputFormat... formats) {
        return new OutputFormats(List.of(formats));
    }

    /**
     * Every format on the classpath.
     *
     * @return loaded formats
     */
    public List<OutputFormat> loaded() {
        return formats;
    }

    /**
     * Whether a format id is on the classpath.
     *
     * @param id format id
     * @return true when loaded
     */
    public boolean has(String id) {
        return find(id).isPresent();
    }

    /**
     * Looks up a loaded format.
     *
     * @param id format id
     * @return the format, or empty
     */
    public Optional<OutputFormat> find(String id) {
        if (id == null || id.isBlank()) {
            return Optional.empty();
        }
        String want = id.trim().toLowerCase(Locale.ROOT);
        for (OutputFormat format : formats) {
            if (want.equals(format.id())) {
                return Optional.of(format);
            }
        }
        return Optional.empty();
    }

    /**
     * Formats named by {@code OUTPUT_FORMATS} (default {@code okf}).
     * Unknown ids are skipped so a missing OKF jar does not abort protobuf.
     *
     * @param env environment
     * @return selected formats
     */
    public List<OutputFormat> select(Map<String, String> env) {
        Objects.requireNonNull(env, "env");
        List<OutputFormat> selected = new ArrayList<>();
        for (String id : OutputEnv.formats(env)) {
            find(id).ifPresent(selected::add);
        }
        return List.copyOf(selected);
    }

    /**
     * Loaded format ids.
     *
     * @return ids
     */
    public List<String> ids() {
        List<String> ids = new ArrayList<>();
        for (OutputFormat format : formats) {
            ids.add(format.id());
        }
        return List.copyOf(ids);
    }
}
