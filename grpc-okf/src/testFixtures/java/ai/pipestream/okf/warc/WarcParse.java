package ai.pipestream.okf.warc;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal WARC 1.1 reader for tests: CRLF headers, Content-Length block,
 * trailing CRLF CRLF. Not a production parser.
 */
public final class WarcParse {

    private static final byte CR = '\r';
    private static final byte LF = '\n';

    public record Record(String version, Map<String, String> headers, byte[] block) {
        public String type() {
            return headers.get("WARC-Type");
        }

        public String targetUri() {
            return headers.get("WARC-Target-URI");
        }

        public String recordId() {
            return headers.get("WARC-Record-ID");
        }

        public String refersTo() {
            return headers.get("WARC-Refers-To");
        }

        public String blockDigest() {
            return headers.get("WARC-Block-Digest");
        }

        public int contentLength() {
            return Integer.parseInt(headers.get("Content-Length"));
        }
    }

    private WarcParse() {
    }

    public static List<Record> parse(byte[] warc) {
        List<Record> records = new ArrayList<>();
        int i = 0;
        while (i < warc.length) {
            int headerEnd = indexOf(warc, i, CR, LF, CR, LF);
            if (headerEnd < 0) {
                throw new AssertionError("truncated WARC headers at offset " + i);
            }
            String headerText = new String(warc, i, headerEnd - i, StandardCharsets.US_ASCII);
            String[] lines = headerText.split("\r\n");
            if (lines.length == 0 || !lines[0].startsWith("WARC/")) {
                throw new AssertionError("expected WARC version line, got: " + headerText);
            }
            Map<String, String> headers = new LinkedHashMap<>();
            for (int n = 1; n < lines.length; n++) {
                int colon = lines[n].indexOf(':');
                if (colon < 0) {
                    throw new AssertionError("malformed header: " + lines[n]);
                }
                headers.put(lines[n].substring(0, colon), lines[n].substring(colon + 1).trim());
            }
            int length = Integer.parseInt(headers.get("Content-Length"));
            int blockStart = headerEnd + 4;
            if (blockStart + length + 4 > warc.length) {
                throw new AssertionError("truncated WARC block");
            }
            byte[] block = new byte[length];
            System.arraycopy(warc, blockStart, block, 0, length);
            if (warc[blockStart + length] != CR || warc[blockStart + length + 1] != LF
                    || warc[blockStart + length + 2] != CR || warc[blockStart + length + 3] != LF) {
                throw new AssertionError("record not terminated by CRLF CRLF");
            }
            records.add(new Record(lines[0], headers, block));
            i = blockStart + length + 4;
        }
        return records;
    }

    private static int indexOf(byte[] data, int from, byte a, byte b, byte c, byte d) {
        for (int i = from; i + 3 < data.length; i++) {
            if (data[i] == a && data[i + 1] == b && data[i + 2] == c && data[i + 3] == d) {
                return i;
            }
        }
        return -1;
    }
}
