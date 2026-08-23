package ai.pipestream.okf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OkfBundleIoTest {

    @Test
    void rejectsReservedConceptNamesAndTraversal() {
        OkfBundle bundle = new OkfBundle();
        OkfConcept page = OkfConcept.of("Page").title("Doc").build();
        assertThatThrownBy(() -> bundle.putConcept("index.md", page))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved");
        assertThatThrownBy(() -> bundle.putConcept("pages/log.md", page))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> bundle.putConcept("pages/200.txt", page))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(".md");
        assertThatThrownBy(() -> bundle.putBytes("../escape.md", new byte[] {1}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("illegal");
        assertThatThrownBy(() -> bundle.putBytes("", new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> bundle.putBytes(null, new byte[0]))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void leadingSlashNormalizeAndContains() {
        OkfBundle bundle = new OkfBundle();
        byte[] payload = {1, 2, 3};
        bundle.putBytes("/references/a.bin", payload);
        payload[0] = 9;
        assertThat(bundle.contains("references/a.bin")).isTrue();
        assertThat(bundle.contains("/references/a.bin")).isTrue();
        assertThat(bundle.files().get("references/a.bin")[0]).isEqualTo((byte) 1);
    }

    @Test
    void writeTreeAndZipRoundTrip(@TempDir Path dir) throws Exception {
        OkfBundle bundle = new OkfBundle();
        bundle.putConcept("pages/200.md", OkfConcept.of("Page").title("Doc").body("hi").build());
        bundle.putText("index.md", OkfNav.rootIndex("Crawl", "intro",
                OkfNav.groupByDirectory(bundle.files().keySet(),
                        java.util.Map.of("pages/200.md", "Doc"),
                        java.util.Map.of("pages/200.md", "a page"))));
        Path tree = dir.resolve("tree");
        OkfWriter.write(bundle, tree);
        assertThat(tree.resolve("pages/200.md")).exists();
        assertThat(Files.readString(tree.resolve("index.md"))).contains("okf_version: \"0.2\"");

        Path zip = dir.resolve("bundle.zip");
        OkfZip.write(bundle, zip);
        try (ZipFile zipFile = new ZipFile(zip.toFile())) {
            assertThat(zipFile.getEntry("pages/200.md")).isNotNull();
            assertThat(zipFile.getEntry("index.md")).isNotNull();
            byte[] bytes = zipFile.getInputStream(zipFile.getEntry("pages/200.md")).readAllBytes();
            assertThat(new String(bytes, StandardCharsets.UTF_8)).contains("type: Page");
        }
    }
}
