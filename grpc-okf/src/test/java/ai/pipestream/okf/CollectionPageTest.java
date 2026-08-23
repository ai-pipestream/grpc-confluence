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
        assertThat(html).contains("<!DOCTYPE html>");
        assertThat(html).contains("WARC-Target-URI");
    }

    @Test
    void escapesHtmlInTitlesAndHrefs() {
        String html = CollectionPage.render("<script>", "a & b", List.of(
                new CollectionPage.Link("https://ex.test/?q=\"x\"", "<img>", "page", "a\"b.md")));
        assertThat(html).contains("&lt;script&gt;");
        assertThat(html).contains("a &amp; b");
        assertThat(html).contains("href=\"https://ex.test/?q=&quot;x&quot;\"");
        assertThat(html).contains("&lt;img&gt;");
        assertThat(html).contains("<code>a&quot;b.md</code>");
        assertThat(html).doesNotContain("<script>");
        assertThat(html).doesNotContain("<img>");
    }

    @Test
    void emptyListStillDocumentsTheContract() {
        String html = CollectionPage.render("Empty", "", List.of());
        assertThat(html).contains("<ul>");
        assertThat(html).contains("</ul>");
        assertThat(html).contains("companion ZIP, not inside WARC");
        assertThat(html).doesNotContain("<a href");
    }
}
