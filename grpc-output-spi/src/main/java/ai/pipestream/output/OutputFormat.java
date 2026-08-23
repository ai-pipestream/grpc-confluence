package ai.pipestream.output;

import com.google.protobuf.Message;

import java.io.IOException;
import java.util.List;

/**
 * A crawl-output encoding written through an {@link OutputStore}.
 * Implementations are loaded with {@link java.util.ServiceLoader}.
 * Built-in ids: {@code protobuf}, {@code json}, {@code okf},
 * {@code microsoft-connector}.
 */
public interface OutputFormat {

    /**
     * Stable format id.
     *
     * @return the id
     */
    String id();

    /**
     * Whether this format can encode {@code record}.
     *
     * @param record a change or snapshot message
     * @return true when {@link #emit} will write something
     */
    boolean supports(Message record);

    /**
     * Writes one record immediately (protobuf / JSON / connector).
     *
     * @param store destination
     * @param record change message
     * @param key hierarchy key without extension
     * @throws IOException if the write fails
     */
    default void emit(OutputStore store, Message record, String key) throws IOException {
    }

    /**
     * Writes end-of-run artifacts (OKF tree, zip, WARC).
     *
     * @param store destination
     * @param records accumulated changes
     * @param prefix store prefix for the run
     * @throws IOException if the write fails
     */
    default void complete(OutputStore store, List<Message> records, String prefix)
            throws IOException {
    }
}
