package ai.pipestream.okf.warc;

import ai.pipestream.okf.CatalogEntry;
import ai.pipestream.okf.OkfConcept;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static org.assertj.core.api.Assertions.assertThat;

class WarcWriterTest {

    private static final Instant AT = Instant.parse("2026-08-23T12:00:00Z");

    @Test
    void parsedRecordsMatchLengthsDigestsAndRefersTo() {
        byte[] html = "<p>hello</p>".getBytes(StandardCharsets.UTF_8);
        CatalogEntry entry = new CatalogEntry(
                "pages/200.md",
                OkfConcept.of("Page").title("Doc")
                        .resource("https://wiki.example/pages/200")
                        .generated(new OkfConcept.Generated("process:test", AT))
                        .body("hello")
                        .build(),
                "https://wiki.example/pages/200",
                "text/html",
                html,
                "page");
        byte[] warc = WarcWriter.toBytes(WarcArchive.records(AT, "grpc-okf-test", "Crawl", "intro",
                List.of(entry), null));
        List<WarcParse.Record> records = WarcParse.parse(warc);

        assertThat(records).hasSize(4);
        assertThat(records.get(0).type()).isEqualTo("warcinfo");
        assertThat(records.get(0).targetUri()).isNull();
        assertThat(records.get(0).headers()).doesNotContainKey("WARC-Target-URI");
        assertThat(records.get(0).version()).isEqualTo("WARC/1.1");

        WarcParse.Record resource = records.get(1);
        assertThat(resource.type()).isEqualTo("resource");
        assertThat(resource.targetUri()).isEqualTo("https://wiki.example/pages/200");
        assertThat(resource.block()).isEqualTo(html);
        assertThat(resource.contentLength()).isEqualTo(html.length);
        assertThat(resource.blockDigest()).isEqualTo(WarcDigest.sha1Base32(html));

        WarcParse.Record conversion = records.get(2);
        assertThat(conversion.type()).isEqualTo("conversion");
        assertThat(conversion.targetUri()).isEqualTo("urn:okf:0.2:pages/200.md");
        assertThat(conversion.refersTo()).isEqualTo(resource.recordId());
        assertThat(new String(conversion.block(), StandardCharsets.UTF_8)).contains("type: Page");
        assertThat(conversion.blockDigest()).isEqualTo(WarcDigest.sha1Base32(conversion.block()));

        WarcParse.Record collection = records.get(3);
        assertThat(collection.type()).isEqualTo("resource");
        assertThat(collection.targetUri()).isEqualTo(WarcArchive.COLLECTION_URI);
        String collectionHtml = new String(collection.block(), StandardCharsets.UTF_8);
        assertThat(collectionHtml).contains("href=\"https://wiki.example/pages/200\"");
        assertThat(collectionHtml).doesNotContain("href=\"pages/200.md\"");

        String text = new String(warc, StandardCharsets.ISO_8859_1);
        assertThat(text).doesNotContain("PK\u0003\u0004");
        assertThat(text).startsWith("WARC/1.1\r\n");
    }

    @Test
    void extraHeadersAndWarcDateAreEmitted() {
        byte[] html = "<p>x</p>".getBytes(StandardCharsets.UTF_8);
        CatalogEntry entry = CatalogEntry.text(
                "pages/200.md",
                OkfConcept.of("Page").title("Doc").body("hello").build(),
                "https://wiki.example/pages/200",
                "text/html",
                "<p>x</p>",
                "page");
        byte[] warc = WarcWriter.toBytes(WarcArchive.records(AT, "grpc-okf-test", "Crawl", "intro",
                List.of(entry), null));
        String text = new String(warc, StandardCharsets.ISO_8859_1);
        assertThat(text).contains("WARC-Date: 2026-08-23T12:00:00Z");
        assertThat(text).contains("WARC-Identified-Payload-Type: text/markdown");
        assertThat(text).contains("WARC-Payload-Digest: ");
        assertThat(text).contains("Content-Length: " + html.length);
        assertThat(WarcArchive.conversionUri("pages/200.md")).isEqualTo("urn:okf:0.2:pages/200.md");
        assertThat(WarcArchive.COLLECTION_URI).isEqualTo("urn:okf:0.2:collection");
    }

    @Test
    void emptyBlockStillHasTrailingCrlfCrlf() {
        List<WarcParse.Record> parsed = WarcParse.parse(WarcWriter.toBytes(
                List.of(WarcWriter.warcinfo(AT, "test"))));
        assertThat(parsed.get(0).contentLength()).isEqualTo(parsed.get(0).block().length);
        assertThat(parsed.get(0).blockDigest()).isEqualTo(WarcDigest.sha1Base32(parsed.get(0).block()));
    }

    @Test
    void writeGzipRoundTrips(@TempDir Path dir) throws Exception {
        List<WarcRecord> records = List.of(WarcWriter.warcinfo(AT, "test"));
        Path gz = dir.resolve("bundle.warc.gz");
        WarcWriter.writeGzip(gz, records);
        byte[] uncompressed;
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(Files.readAllBytes(gz)))) {
            uncompressed = in.readAllBytes();
        }
        List<WarcParse.Record> parsed = WarcParse.parse(uncompressed);
        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).type()).isEqualTo("warcinfo");
        assertThat(new String(parsed.get(0).block(), StandardCharsets.UTF_8))
                .contains("format: WARC File Format 1.1");
    }
}
