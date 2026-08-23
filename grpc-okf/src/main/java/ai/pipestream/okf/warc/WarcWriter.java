package ai.pipestream.okf.warc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.zip.GZIPOutputStream;

/**
 * WARC 1.1 serializer: CRLF headers, {@code Content-Length}, a blank line,
 * the block, then CRLF CRLF. {@code WARC-Block-Digest} is {@code sha1:} plus
 * RFC 4648 base32 (Internet Archive convention).
 */
public final class WarcWriter {

    /** WARC version line. */
    public static final String VERSION = "WARC/1.1";

    private static final byte[] CRLF = {'\r', '\n'};
    private static final DateTimeFormatter DATE =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);

    private WarcWriter() {
    }

    /**
     * Writes records to {@code out} (uncompressed WARC).
     *
     * @param out destination
     * @param records records in file order
     * @throws IOException if writing fails
     */
    public static void write(OutputStream out, List<WarcRecord> records) throws IOException {
        Objects.requireNonNull(out, "out");
        for (WarcRecord record : Objects.requireNonNull(records, "records")) {
            writeRecord(out, record);
        }
        out.flush();
    }

    /**
     * Writes a gzip-compressed {@code .warc.gz} file.
     *
     * @param warcGz destination path
     * @param records records in file order
     * @return {@code warcGz}
     * @throws IOException if writing fails
     */
    public static Path writeGzip(Path warcGz, List<WarcRecord> records) throws IOException {
        Objects.requireNonNull(warcGz, "warcGz");
        if (warcGz.getParent() != null) {
            Files.createDirectories(warcGz.getParent());
        }
        try (OutputStream file = Files.newOutputStream(warcGz);
                GZIPOutputStream gzip = new GZIPOutputStream(file)) {
            write(gzip, records);
        }
        return warcGz;
    }

    /**
     * Serializes records to gzip bytes.
     *
     * @param records records
     * @return {@code .warc.gz} bytes
     */
    public static byte[] toGzipBytes(List<WarcRecord> records) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            write(gzip, records);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return out.toByteArray();
    }

    /**
     * Serializes records to uncompressed bytes (tests).
     *
     * @param records records
     * @return WARC bytes
     */
    public static byte[] toBytes(List<WarcRecord> records) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            write(out, records);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return out.toByteArray();
    }

    /**
     * A {@code warcinfo} record describing this producer. No
     * {@code WARC-Target-URI}.
     *
     * @param date record date
     * @param software software token
     * @return the record
     */
    public static WarcRecord warcinfo(Instant date, String software) {
        String fields = "software: " + software + "\r\n"
                + "format: WARC File Format 1.1\r\n"
                + "conformsTo: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/\r\n"
                + "okf-version: 0.2\r\n";
        return new WarcRecord(
                WarcRecord.TYPE_WARCINFO,
                WarcRecord.newRecordId(),
                date,
                null,
                null,
                "application/warc-fields",
                fields.getBytes(StandardCharsets.UTF_8),
                Map.of());
    }

    private static void writeRecord(OutputStream out, WarcRecord record) throws IOException {
        byte[] block = record.block();
        List<String> headers = new ArrayList<>();
        headers.add(VERSION);
        headers.add("WARC-Type: " + record.type());
        headers.add("WARC-Date: " + DATE.format(record.date()));
        headers.add("WARC-Record-ID: " + record.recordId());
        if (record.targetUri() != null) {
            headers.add("WARC-Target-URI: " + record.targetUri());
        }
        if (record.refersTo() != null && !record.refersTo().isBlank()) {
            headers.add("WARC-Refers-To: " + record.refersTo());
        }
        headers.add("Content-Type: " + record.contentType());
        headers.add("Content-Length: " + block.length);
        headers.add("WARC-Block-Digest: " + WarcDigest.sha1Base32(block));
        headers.add("WARC-Payload-Digest: " + WarcDigest.sha1Base32(block));
        for (Map.Entry<String, String> extra : record.extra().entrySet()) {
            headers.add(extra.getKey() + ": " + extra.getValue());
        }
        for (String header : headers) {
            out.write(header.getBytes(StandardCharsets.US_ASCII));
            out.write(CRLF);
        }
        out.write(CRLF);
        out.write(block);
        out.write(CRLF);
        out.write(CRLF);
    }
}
