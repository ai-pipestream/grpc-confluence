package ai.pipestream.okf.warc;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * One WARC 1.1 record ready to serialize. {@code warcinfo} must omit
 * {@code targetUri}; {@code conversion} should set {@code refersTo} to the
 * {@code recordId} of the resource it was derived from.
 *
 * @param type WARC-Type ({@code warcinfo}, {@code resource}, {@code conversion})
 * @param recordId WARC-Record-ID including the {@code <urn:uuid:…>} brackets
 * @param date WARC-Date
 * @param targetUri WARC-Target-URI, or {@code null} for warcinfo
 * @param refersTo WARC-Refers-To, or {@code null} when unused
 * @param contentType Content-Type of the block
 * @param block record block bytes
 * @param extra additional WARC headers (name → value), insertion order kept
 */
public record WarcRecord(
        String type,
        String recordId,
        Instant date,
        String targetUri,
        String refersTo,
        String contentType,
        byte[] block,
        Map<String, String> extra) {

    /**
     * Canonical {@code warcinfo} type.
     */
    public static final String TYPE_WARCINFO = "warcinfo";
    /**
     * Canonical {@code resource} type.
     */
    public static final String TYPE_RESOURCE = "resource";
    /**
     * Canonical {@code conversion} type.
     */
    public static final String TYPE_CONVERSION = "conversion";

    /**
     * Validates required fields and copies the block and extra headers.
     *
     * @param type WARC-Type
     * @param recordId WARC-Record-ID
     * @param date WARC-Date
     * @param targetUri WARC-Target-URI or {@code null}
     * @param refersTo WARC-Refers-To or {@code null}
     * @param contentType Content-Type
     * @param block payload
     * @param extra extra headers
     */
    public WarcRecord {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(recordId, "recordId");
        Objects.requireNonNull(date, "date");
        Objects.requireNonNull(contentType, "contentType");
        Objects.requireNonNull(block, "block");
        if (TYPE_WARCINFO.equals(type) && targetUri != null && !targetUri.isBlank()) {
            throw new IllegalArgumentException("warcinfo must not carry WARC-Target-URI");
        }
        if (!TYPE_WARCINFO.equals(type) && (targetUri == null || targetUri.isBlank())) {
            throw new IllegalArgumentException(type + " requires WARC-Target-URI");
        }
        block = block.clone();
        extra = extra == null ? Map.of() : Map.copyOf(new LinkedHashMap<>(extra));
    }

    /**
     * A new {@code <urn:uuid:…>} record id.
     *
     * @return WARC-Record-ID value
     */
    public static String newRecordId() {
        return "<urn:uuid:" + UUID.randomUUID() + ">";
    }
}
