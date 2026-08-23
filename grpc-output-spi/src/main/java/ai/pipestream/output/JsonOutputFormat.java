package ai.pipestream.output;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Protobuf JSON export of the change message. This is a file format, not
 * the gRPC wire (the wire stays typed protobuf).
 */
public final class JsonOutputFormat implements OutputFormat {

    /** Format id. */
    public static final String ID = "json";
    /** Media type. */
    public static final String MEDIA_TYPE = "application/json; charset=utf-8";

    private static final JsonFormat.Printer PRINTER = JsonFormat.printer()
            .omittingInsignificantWhitespace();

    /**
     * Creates the format (ServiceLoader).
     */
    public JsonOutputFormat() {
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
        try {
            byte[] json = PRINTER.print(record).getBytes(StandardCharsets.UTF_8);
            store.put(OutputObject.of(key + ".json", json, MEDIA_TYPE));
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("JSON export failed for " + key, e);
        }
    }
}
