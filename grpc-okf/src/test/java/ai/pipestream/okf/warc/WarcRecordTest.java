package ai.pipestream.okf.warc;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WarcRecordTest {

    private static final Instant AT = Instant.parse("2026-08-23T12:00:00Z");

    @Test
    void warcinfoRejectsTargetUri() {
        assertThatThrownBy(() -> new WarcRecord(WarcRecord.TYPE_WARCINFO, WarcRecord.newRecordId(),
                AT, "https://example/", null, "application/warc-fields", new byte[0], Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("warcinfo must not carry WARC-Target-URI");
    }

    @Test
    void resourceRequiresTargetUri() {
        assertThatThrownBy(() -> new WarcRecord(WarcRecord.TYPE_RESOURCE, WarcRecord.newRecordId(),
                AT, null, null, "text/html", new byte[0], Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires WARC-Target-URI");
        assertThatThrownBy(() -> new WarcRecord(WarcRecord.TYPE_CONVERSION, WarcRecord.newRecordId(),
                AT, "  ", null, "text/markdown", new byte[0], Map.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void recordIdIsBracketedUuid() {
        assertThat(WarcRecord.newRecordId()).matches("<urn:uuid:[0-9a-fA-F\\-]{36}>");
    }

    @Test
    void blockIsCopied() {
        byte[] payload = {1, 2, 3};
        WarcRecord record = new WarcRecord(WarcRecord.TYPE_RESOURCE, WarcRecord.newRecordId(),
                AT, "https://example/page", null, "text/plain", payload, Map.of());
        payload[0] = 9;
        assertThat(record.block()[0]).isEqualTo((byte) 1);
    }

    @Test
    void extraHeadersAreCopied() {
        java.util.Map<String, String> extra = new java.util.LinkedHashMap<>();
        extra.put("WARC-Identified-Payload-Type", "text/markdown");
        WarcRecord record = new WarcRecord(WarcRecord.TYPE_CONVERSION, WarcRecord.newRecordId(),
                AT, "urn:okf:0.2:pages/200.md", "<urn:uuid:1>", "text/markdown", new byte[0], extra);
        extra.put("X-Mutate", "no");
        assertThat(record.extra()).containsEntry("WARC-Identified-Payload-Type", "text/markdown");
        assertThat(record.extra()).doesNotContainKey("X-Mutate");
        assertThat(record.refersTo()).isEqualTo("<urn:uuid:1>");
    }
}
