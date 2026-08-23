package ai.pipestream.output;

import com.google.protobuf.Message;

import java.io.IOException;

/**
 * Binary protobuf of the change message — the same bytes Kafka Connect
 * already publishes. Extension {@code .pb}.
 */
public final class ProtobufOutputFormat implements OutputFormat {

    /** Format id. */
    public static final String ID = "protobuf";
    /** Media type. */
    public static final String MEDIA_TYPE = "application/x-protobuf";

    /**
     * Creates the format (ServiceLoader).
     */
    public ProtobufOutputFormat() {
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public boolean supports(Message record) {
        return record != null;
    }

    @Override
    public void emit(OutputStore store, Message record, String key) throws IOException {
        store.put(OutputObject.of(key + ".pb", record.toByteArray(), MEDIA_TYPE));
    }
}
