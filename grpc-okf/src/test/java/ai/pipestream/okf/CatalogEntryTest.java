package ai.pipestream.okf;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CatalogEntryTest {

    @Test
    void copiesResourceBodyAndRejectsBlankTargetUri() {
        byte[] payload = {1, 2, 3};
        CatalogEntry entry = new CatalogEntry(
                "pages/200.md",
                OkfConcept.of("Page").title("Doc").build(),
                "https://wiki.example/pages/200",
                "text/plain",
                payload,
                "page");
        payload[0] = 9;
        assertThat(entry.resourceBody()[0]).isEqualTo((byte) 1);
        assertThat(entry.title()).isEqualTo("Doc");
        assertThatThrownBy(() -> new CatalogEntry(
                "pages/200.md",
                OkfConcept.of("Page").build(),
                "  ",
                "text/plain",
                new byte[0],
                "page"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("targetUri");
    }
}
