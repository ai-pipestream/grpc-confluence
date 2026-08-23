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

    @Test
    void warcinfoOmitsTargetUriAndRecordsUseCrlf() {
        Instant at = Instant.parse("2026-08-23T12:00:00Z");
        CatalogEntry entry = CatalogEntry.text(
                "pages/200.md",
                OkfConcept.of("Page").title("Doc")
                        .resource("https://wiki.example/pages/200")
                        .generated(new OkfConcept.Generated("process:test", at))
                        .body("hello")
                        .build(),
                "https://wiki.example/pages/200",
                "text/html",
                "<p>hello</p>",
                "page");
        List<WarcRecord> records = WarcArchive.records(at, "grpc-okf-test", "Crawl", "intro",
                List.of(entry), null);
        byte[] warc = WarcWriter.toBytes(records);
        String text = new String(warc, StandardCharsets.ISO_8859_1);

        assertThat(text).startsWith("WARC/1.1\r\n");
        assertThat(text).contains("WARC-Type: warcinfo\r\n");
        assertThat(text).contains("WARC-Type: resource\r\n");
        assertThat(text).contains("WARC-Type: conversion\r\n");
        assertThat(text).contains("WARC-Target-URI: https://wiki.example/pages/200\r\n");
        assertThat(text).contains("WARC-Target-URI: urn:okf:0.2:pages/200.md\r\n");
        assertThat(text).contains("WARC-Target-URI: " + WarcArchive.COLLECTION_URI + "\r\n");
        assertThat(text).contains("WARC-Block-Digest: sha1:");
        assertThat(text).contains("href=\"https://wiki.example/pages/200\"");
        assertThat(text).doesNotContain("PK\u0003\u0004");
        assertThat(text.indexOf("WARC-Type: warcinfo"))
                .isLessThan(text.indexOf("WARC-Target-URI:"));
        int warcinfoEnd = text.indexOf("\r\n\r\n", text.indexOf("WARC-Type: warcinfo"));
        String warcinfoHeaders = text.substring(0, warcinfoEnd);
        assertThat(warcinfoHeaders).doesNotContain("WARC-Target-URI:");
        assertThat(text).contains("WARC-Refers-To: ");
    }

    @Test
    void writeGzipRoundTrips(@TempDir Path dir) throws Exception {
        Instant at = Instant.parse("2026-08-23T12:00:00Z");
        List<WarcRecord> records = List.of(WarcWriter.warcinfo(at, "test"));
        Path gz = dir.resolve("bundle.warc.gz");
        WarcWriter.writeGzip(gz, records);
        byte[] uncompressed;
        try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(Files.readAllBytes(gz)))) {
            uncompressed = in.readAllBytes();
        }
        assertThat(new String(uncompressed, StandardCharsets.ISO_8859_1)).startsWith("WARC/1.1\r\n");
    }
}
