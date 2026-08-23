package ai.pipestream.output;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Discovers {@link OutputStore} implementations on the classpath.
 * Filesystem is the default; use {@link #has(String)} to see whether S3
 * (or another jar) was loaded.
 */
public final class OutputStores {

    private final List<OutputStore> stores;

    private OutputStores(List<OutputStore> stores) {
        this.stores = List.copyOf(stores);
    }

    /**
     * Loads stores from the SPI class loader.
     *
     * @return discovered stores
     */
    public static OutputStores load() {
        return load(OutputStore.class.getClassLoader());
    }

    /**
     * Loads stores from {@code classLoader}.
     *
     * @param classLoader loader
     * @return discovered stores
     */
    public static OutputStores load(ClassLoader classLoader) {
        List<OutputStore> found = new ArrayList<>();
        for (OutputStore store : ServiceLoader.load(OutputStore.class, classLoader)) {
            found.add(store);
        }
        return new OutputStores(found);
    }

    /**
     * Fixed list (tests).
     *
     * @param stores stores
     * @return wrapper
     */
    public static OutputStores of(OutputStore... stores) {
        return new OutputStores(List.of(stores));
    }

    /**
     * Every store jar that registered itself, configured or not.
     *
     * @return loaded stores
     */
    public List<OutputStore> loaded() {
        return stores;
    }

    /**
     * Whether a store id is on the classpath.
     *
     * @param id store id
     * @return true when loaded
     */
    public boolean has(String id) {
        return find(id).isPresent();
    }

    /**
     * Looks up a loaded store by id.
     *
     * @param id store id
     * @return the store, or empty
     */
    public Optional<OutputStore> find(String id) {
        if (id == null || id.isBlank()) {
            return Optional.empty();
        }
        String want = id.trim().toLowerCase(Locale.ROOT);
        for (OutputStore store : stores) {
            if (want.equals(store.id())) {
                return Optional.of(store);
            }
        }
        return Optional.empty();
    }

    /**
     * Loaded stores whose {@link OutputStore#available(Map)} is true.
     *
     * @param env environment
     * @return available stores
     */
    public List<OutputStore> available(Map<String, String> env) {
        Objects.requireNonNull(env, "env");
        List<OutputStore> out = new ArrayList<>();
        for (OutputStore store : stores) {
            if (store.available(env)) {
                out.add(store);
            }
        }
        return List.copyOf(out);
    }

    /**
     * Selects {@code OUTPUT_STORE} (default filesystem), opens it, and
     * returns it. Fails when the id is not loaded or not configured.
     *
     * @param env environment
     * @return opened store
     * @throws IOException if {@link OutputStore#open(Map)} fails
     */
    public OutputStore select(Map<String, String> env) throws IOException {
        Objects.requireNonNull(env, "env");
        String id = OutputEnv.storeId(env);
        OutputStore store = find(id).orElseThrow(() -> new IllegalStateException(
                "output store '" + id + "' is not loaded; loaded=" + ids()));
        if (!store.available(env)) {
            throw new IllegalStateException("output store '" + id + "' is loaded but not configured");
        }
        store.open(env);
        return store;
    }

    /**
     * Loaded store ids, for logs and errors.
     *
     * @return ids
     */
    public List<String> ids() {
        List<String> ids = new ArrayList<>();
        for (OutputStore store : stores) {
            ids.add(store.id());
        }
        return List.copyOf(ids);
    }
}
