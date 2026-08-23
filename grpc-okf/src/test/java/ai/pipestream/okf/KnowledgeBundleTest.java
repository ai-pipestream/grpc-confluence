package ai.pipestream.okf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

class KnowledgeBundleTest {

    @Test
    void writesDirectoryZipAndWarcAsSiblings(@TempDir Path dir) throws Exception {
        CatalogEntry entry = CatalogEntry.text(
                "pages/200.md",
                OkfConcept.of("Page").title("Doc")
                        .resource("https://wiki.example/wiki/pages/200")
                        .generated(new OkfConcept.Generated("process:test", Instant.parse("2026-01-01T00:00:00Z")))
                        .body("hello")
                        .build(),
                "https://wiki.example/wiki/pages/200",
                "text/html",
                "<p>hello</p>",
                "page");
        Path tree = dir.resolve("run");
        Path zip = dir.resolve("run.zip");
        Path warc = dir.resolve("run.warc.gz");
        KnowledgeBundle bundle = KnowledgeBundle.assemble(
                "Confluence crawl",
                "test bundle",
                "process:test",
                Instant.parse("2026-01-01T00:00:00Z"),
                "grpc-okf-test",
                List.of(entry));
        bundle.write(new OkfOutput(tree, zip, warc));

        assertThat(OkfConformance.check(bundle.okf())).isEmpty();
        assertThat(tree.resolve("index.md")).exists();
        assertThat(tree.resolve("pages/200.md")).exists();
        assertThat(tree.resolve("collection.html")).exists();
        assertThat(Files.readString(tree.resolve("index.md"))).contains("okf_version: \"0.2\"");
        assertThat(Files.readString(tree.resolve("collection.html")))
                .contains("href=\"https://wiki.example/wiki/pages/200\"")
                .doesNotContain("href=\"pages/200.md\"");
        try (ZipFile zipFile = new ZipFile(zip.toFile())) {
            assertThat(zipFile.getEntry("pages/200.md")).isNotNull();
            assertThat(zipFile.getEntry("collection.html")).isNotNull();
        }
        assertThat(Files.size(warc)).isPositive();
        assertThat(bundle.warcRecords()).extracting(r -> r.type())
                .contains("warcinfo", "resource", "conversion");
        assertThat(bundle.okf().files().values().stream()
                .noneMatch(bytes -> new String(bytes, StandardCharsets.UTF_8).contains("WARC/1.1")))
                .isTrue();
    }

    @Test
    void outputDefaultsZipAndWarcBesideDirectory() {
        OkfOutput output = OkfOutput.from(Map.of("OKF_DIR", "/tmp/okf-run"));
        assertThat(output.directory()).isEqualTo(Path.of("/tmp/okf-run"));
        assertThat(output.zip()).isEqualTo(Path.of("/tmp/okf-run.zip"));
        assertThat(output.warc()).isEqualTo(Path.of("/tmp/okf-run.warc.gz"));
        assertThat(output.enabled()).isTrue();
    }
}
