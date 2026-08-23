package ai.pipestream.okf;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CollectionPageTest {

    @Test
    void linksLiveUrisAndShowsOkfPathAsCode() {
        String html = CollectionPage.render("Crawl", "intro", List.of(
                new CollectionPage.Link("https://wiki.example/pages/200", "Doc", "page",
                        "pages/200.md")));
        assertThat(html).contains("href=\"https://wiki.example/pages/200\"");
        assertThat(html).contains("<code>pages/200.md</code>");
        assertThat(html).doesNotContain("href=\"pages/200.md\"");
    }
}
