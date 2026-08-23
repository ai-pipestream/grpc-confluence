package ai.pipestream.okf;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OkfPathsTest {

    @Test
    void segmentKeepsSafeCharsAndReplacesTheRest() {
        assertThat(OkfPaths.segment("Page-200_v1.md")).isEqualTo("Page-200_v1.md");
        assertThat(OkfPaths.segment("a/b c")).isEqualTo("a_b_c");
        assertThat(OkfPaths.segment(null)).isEqualTo("_");
        assertThat(OkfPaths.segment("")).isEqualTo("_");
        assertThat(OkfPaths.segment("   ")).isEqualTo("_");
    }

    @Test
    void joinSkipsBlanksAndStripsSlashes() {
        assertThat(OkfPaths.join("pages", "200.md")).isEqualTo("pages/200.md");
        assertThat(OkfPaths.join("/pages/", null, "", "200.md")).isEqualTo("pages/200.md");
        assertThat(OkfPaths.join("a\\b", "c")).isEqualTo("a/b/c");
    }

    @Test
    void conceptIdAndHref() {
        assertThat(OkfPaths.conceptId("pages/200.md")).isEqualTo("pages/200");
        assertThat(OkfPaths.conceptId("pages/200.MD")).isEqualTo("pages/200");
        assertThat(OkfPaths.href("pages/200.md")).isEqualTo("/pages/200.md");
        assertThat(OkfPaths.href("/pages/200.md")).isEqualTo("/pages/200.md");
    }

    @Test
    void reservedNames() {
        assertThat(OkfPaths.reserved("index.md")).isTrue();
        assertThat(OkfPaths.reserved("log.md")).isTrue();
        assertThat(OkfPaths.reserved("page.md")).isFalse();
        assertThat(OkfPaths.reserved("pages/index.md")).isFalse();
        assertThat(OkfPaths.reserved("INDEX.md")).isFalse();
    }
}
