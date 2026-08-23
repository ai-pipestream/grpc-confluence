package ai.pipestream.output;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Environment keys for the output SPI.
 */
public final class OutputEnv {

    /** Selected store id; default {@link #DEFAULT_STORE}. */
    public static final String STORE = "OUTPUT_STORE";
    /** Filesystem root (alias of {@code OKF_DIR}). */
    public static final String DIR = "OUTPUT_DIR";
    /** Compat filesystem / OKF tree root. */
    public static final String OKF_DIR = "OKF_DIR";
    /** Run prefix under the store (optional). */
    public static final String PREFIX = "OUTPUT_PREFIX";
    /** Comma-separated format ids. Default {@code okf}. */
    public static final String FORMATS = "OUTPUT_FORMATS";
    /** S3 bucket. */
    public static final String S3_BUCKET = "OUTPUT_S3_BUCKET";
    /** S3 key prefix. */
    public static final String S3_PREFIX = "OUTPUT_S3_PREFIX";
    /** AWS region for the S3 client. */
    public static final String S3_REGION = "OUTPUT_S3_REGION";
    /** Default store id. */
    public static final String DEFAULT_STORE = "filesystem";
    /** Default format when {@link #FORMATS} is unset. */
    public static final String DEFAULT_FORMAT = "okf";

    private OutputEnv() {
    }

    /**
     * Requested store id, defaulting to filesystem.
     *
     * @param env environment
     * @return store id
     */
    public static String storeId(Map<String, String> env) {
        String value = blankToNull(env.get(STORE));
        return value == null ? DEFAULT_STORE : value.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * Filesystem directory, {@code OUTPUT_DIR} then {@code OKF_DIR}.
     *
     * @param env environment
     * @return path string, or {@code null}
     */
    public static String directory(Map<String, String> env) {
        String dir = blankToNull(env.get(DIR));
        return dir != null ? dir : blankToNull(env.get(OKF_DIR));
    }

    /**
     * Run prefix (no leading/trailing slash).
     *
     * @param env environment
     * @return prefix, possibly empty
     */
    public static String prefix(Map<String, String> env) {
        String value = blankToNull(env.get(PREFIX));
        if (value == null) {
            return "";
        }
        while (value.startsWith("/")) {
            value = value.substring(1);
        }
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    /**
     * Selected format ids. Default is {@code okf} so existing {@code OKF_DIR}
     * runs stay markdown-only.
     *
     * @param env environment
     * @return format ids, lowercased
     */
    public static List<String> formats(Map<String, String> env) {
        String raw = blankToNull(env.get(FORMATS));
        List<String> ids = new ArrayList<>();
        if (raw == null) {
            ids.add(DEFAULT_FORMAT);
            return ids;
        }
        for (String part : raw.split(",")) {
            String id = part.trim().toLowerCase(Locale.ROOT);
            if (!id.isEmpty()) {
                ids.add(id);
            }
        }
        if (ids.isEmpty()) {
            ids.add(DEFAULT_FORMAT);
        }
        return ids;
    }

    /**
     * Whether any store destination is configured.
     *
     * @param env environment
     * @return true when a directory or S3 bucket is set
     */
    public static boolean destinationSet(Map<String, String> env) {
        Objects.requireNonNull(env, "env");
        return directory(env) != null || blankToNull(env.get(S3_BUCKET)) != null;
    }

    /**
     * Process environment as a map.
     *
     * @return {@link System#getenv()}
     */
    public static Map<String, String> process() {
        return System.getenv();
    }

    /**
     * Trims blank strings to {@code null}.
     *
     * @param value raw value
     * @return trimmed value, or {@code null}
     */
    public static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
