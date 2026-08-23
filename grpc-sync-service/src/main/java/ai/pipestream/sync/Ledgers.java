package ai.pipestream.sync;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Opens the process ledger: SQLite when {@code SYNC_TABLE_JDBC_URL} or
 * {@code SYNC_TABLE_DB} is set, otherwise an in-memory {@link AssetStore}.
 */
public final class Ledgers {

    /** JDBC URL. {@code jdbc:sqlite:...} is the supported dialect. */
    public static final String ENV_JDBC_URL = "SYNC_TABLE_JDBC_URL";
    /** SQLite file path when {@link #ENV_JDBC_URL} is unset. */
    public static final String ENV_DB = "SYNC_TABLE_DB";
    /** {@code memory} forces {@link AssetStore} even when a path is set. */
    public static final String ENV_STORE = "SYNC_TABLE_STORE";

    private Ledgers() {
    }

    /**
     * Opens a ledger from the process environment.
     *
     * @return memory store or SQLite ledger
     */
    public static Ledger open() {
        return open(System.getenv());
    }

    /**
     * Opens a ledger from {@code env}.
     *
     * @param env environment
     * @return memory store or SQLite ledger
     */
    public static Ledger open(Map<String, String> env) {
        Objects.requireNonNull(env, "env");
        String store = value(env, ENV_STORE);
        if ("memory".equalsIgnoreCase(store)) {
            return new AssetStore();
        }
        String jdbc = value(env, ENV_JDBC_URL);
        if (jdbc == null) {
            String file = value(env, ENV_DB);
            if (file != null) {
                jdbc = "jdbc:sqlite:" + Path.of(file).toAbsolutePath();
            }
        }
        if (jdbc == null) {
            return new AssetStore();
        }
        try {
            return JdbcLedger.open(jdbc);
        } catch (SQLException e) {
            throw new IllegalStateException("ledger JDBC open failed: " + jdbc, e);
        }
    }

    /**
     * Whether {@code env} selected a durable store.
     *
     * @param env environment
     * @return true when a JDBC URL or db path is set and store is not memory
     */
    public static boolean durable(Map<String, String> env) {
        Objects.requireNonNull(env, "env");
        if ("memory".equalsIgnoreCase(value(env, ENV_STORE))) {
            return false;
        }
        return value(env, ENV_JDBC_URL) != null || value(env, ENV_DB) != null;
    }

    private static String value(Map<String, String> env, String key) {
        String raw = env.get(key);
        if (raw == null || raw.isBlank()) {
            return null;
        }
        return raw.trim();
    }

    /**
     * Normalizes a JDBC URL for logs (no query secrets expected).
     *
     * @param jdbc URL
     * @return lowercase scheme + rest
     */
    public static String describe(String jdbc) {
        if (jdbc == null) {
            return "memory";
        }
        return jdbc.toLowerCase(Locale.ROOT);
    }
}
