package ai.pipestream.output;

import java.io.IOException;
import java.util.Map;

/**
 * A destination for crawl artifacts. Implementations are loaded with
 * {@link java.util.ServiceLoader} from their own jars ({@code filesystem},
 * {@code s3}, …).
 */
public interface OutputStore extends AutoCloseable {

    /**
     * Stable store id ({@code filesystem}, {@code s3}).
     *
     * @return the id
     */
    String id();

    /**
     * Whether this store can run with {@code env} (credentials, bucket, dir).
     *
     * @param env environment map
     * @return true when {@link #open(Map)} would succeed
     */
    boolean available(Map<String, String> env);

    /**
     * Binds configuration. Called once before {@link #put}.
     *
     * @param env environment map
     * @throws IOException if the destination cannot be prepared
     */
    void open(Map<String, String> env) throws IOException;

    /**
     * Writes one object. Keys use Confluence / Graph hierarchy
     * ({@link ObjectKeys}).
     *
     * @param object the object
     * @throws IOException if the write fails
     */
    void put(OutputObject object) throws IOException;

    /**
     * Releases resources. Default is a no-op.
     */
    @Override
    default void close() {
    }
}
