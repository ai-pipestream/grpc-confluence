package ai.pipestream.okf;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OkfConformanceAndNavTest {

    @Test
    void missingTypeAndNestedIndexFrontmatterAreViolations() {
        OkfBundle bundle = new OkfBundle();
        bundle.putText("pages/200.md", "no frontmatter\n");
        bundle.putText("pages/index.md", "---\nokf_version: \"0.2\"\n---\n# Pages\n");
        bundle.putText("log.md", "# Log\nwithout a date heading\n");
        List<String> violations = OkfConformance.check(bundle);
        assertThat(violations).anyMatch(v -> v.contains("missing YAML frontmatter"));
        assertThat(violations).anyMatch(v -> v.contains("nested index.md must not carry frontmatter"));
        assertThat(violations).anyMatch(v -> v.contains("log.md should contain ISO date headings"));
        assertThatThrownBy(() -> OkfConformance.require(bundle))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void blankTypeAndWrongVersion() {
        OkfBundle bundle = new OkfBundle();
        bundle.putText("pages/200.md", "---\ntype: \"\"\n---\n\nbody\n");
        bundle.putText("index.md", "---\nokf_version: \"0.1\"\n---\n# Crawl\n");
        List<String> violations = OkfConformance.check(bundle);
        assertThat(violations).anyMatch(v -> v.contains("type is required"));
        assertThat(violations).anyMatch(v -> v.contains("okf_version should be \"0.2\""));
    }

    @Test
    void unparseableYaml() {
        OkfBundle bundle = new OkfBundle();
        bundle.putText("pages/200.md", "---\n{not: [valid\n---\n");
        assertThat(OkfConformance.check(bundle)).anyMatch(v -> v.contains("unparseable YAML"));
    }

    @Test
    void navRootAndNestedAndGroupByDirectory() {
        Instant at = Instant.parse("2026-08-23T00:00:00Z");
        Map<String, List<OkfNav.IndexEntry>> sections = OkfNav.groupByDirectory(
                List.of("pages/200.md", "pages/201.md", "index.md", "log.md"),
                Map.of("pages/200.md", "Doc", "pages/201.md", "Other"),
                Map.of("pages/200.md", "first"));
        assertThat(sections).containsOnlyKeys("pages");
        String root = OkfNav.rootIndex("Crawl", "intro", sections);
        assertThat(root).startsWith("---\nokf_version: \"0.2\"\n---\n");
        assertThat(root).contains("# Crawl");
        assertThat(root).contains("[Doc](200.md) - first");
        String nested = OkfNav.nestedIndex("pages", sections);
        assertThat(nested).doesNotStartWith("---");
        String log = OkfNav.log("Crawl", at, List.of("* **Creation**: ran."));
        assertThat(log).contains("## 2026-08-23");
        assertThat(log).contains("* **Creation**: ran.");
    }

    @Test
    void outputEnvParsing() {
        assertThat(OkfOutput.from(Map.of()).enabled()).isFalse();
        OkfOutput onlyZip = OkfOutput.from(Map.of("OKF_ZIP", "/tmp/out.zip"));
        assertThat(onlyZip.zip()).isEqualTo(java.nio.file.Path.of("/tmp/out.zip"));
        assertThat(onlyZip.directory()).isNull();
        assertThat(onlyZip.warc()).isNull();
        OkfOutput overrides = OkfOutput.from(Map.of(
                "OKF_DIR", "/data/run",
                "OKF_ZIP", "/data/custom.zip",
                "OKF_WARC", "/data/custom.warc.gz"));
        assertThat(overrides.zip()).isEqualTo(java.nio.file.Path.of("/data/custom.zip"));
        assertThat(overrides.warc()).isEqualTo(java.nio.file.Path.of("/data/custom.warc.gz"));
    }

    @Test
    void crlfFrontmatterAndRootIndexHeading() {
        OkfBundle ok = new OkfBundle();
        ok.putText("pages/200.md", "---\r\ntype: Page\r\n---\r\n\r\nbody\n");
        assertThat(OkfConformance.check(ok)).isEmpty();

        OkfBundle noHeading = new OkfBundle();
        noHeading.putText("index.md", "---\nokf_version: \"0.2\"\n---\nplain text\n");
        assertThat(OkfConformance.check(noHeading))
                .anyMatch(v -> v.contains("at least one markdown heading"));
    }

    @Test
    void groupByDirectoryKeepsNestedHrefsAndLogDefault() {
        Map<String, List<OkfNav.IndexEntry>> sections = OkfNav.groupByDirectory(
                List.of("items/drive-1/file-1.md", "sites/site-1.md"),
                Map.of("items/drive-1/file-1.md", "notes.txt", "sites/site-1.md", "ENG"),
                Map.of());
        assertThat(sections).containsOnlyKeys("items", "sites");
        assertThat(sections.get("items")).extracting(OkfNav.IndexEntry::href)
                .containsExactly("drive-1/file-1.md");
        String log = OkfNav.log("Crawl", Instant.parse("2026-08-23T00:00:00Z"), List.of());
        assertThat(log).contains("* **Initialization**: Created foundational directory structure.");
    }

    @Test
    void blankEnvValuesDisableOutput() {
        assertThat(OkfOutput.from(Map.of("OKF_DIR", "  ", "OKF_ZIP", "")).enabled()).isFalse();
    }
}
