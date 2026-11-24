package ai.pipestream.confluence.output;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.BlogPost;
import ai.pipestream.confluence.v1.Body;
import ai.pipestream.confluence.v1.BodyType;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.okf.CatalogEntry;
import ai.pipestream.okf.KnowledgeBundle;
import ai.pipestream.okf.OkfConformance;
import ai.pipestream.okf.OkfYaml;
import ai.pipestream.okf.warc.WarcParse;
import ai.pipestream.okf.warc.WarcWriter;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The archive holds a record for a live URL only when bytes read from
 * Confluence stand behind it. A Sync emits attachment metadata without the
 * binary, and with {@code include_bodies} off it emits pages, blog posts
 * and comments without their bodies: those entities keep their OKF markdown
 * and their link, and get no resource record and no stand-in text.
 */
class NotFetchedContentTest {

    private static final String DOWNLOAD = "https://example.atlassian.net/wiki/download/a1";
    private static final String PAGE_URL = "https://example.atlassian.net/wiki/spaces/ENG/pages/200";
    private static final String BLOG_URL = "https://example.atlassian.net/wiki/spaces/ENG/blog/201";
    private static final String COMMENT_URL = PAGE_URL + "?focusedCommentId=9";

    @Test
    void anAttachmentWhoseBinaryWasNotFetchedHasNoResourceRecord() {
        CatalogEntry entry = entry(ConfluenceEntity.newBuilder()
                .setEntityId("a1")
                .setAttachment(Attachment.newBuilder().setId("a1").setTitle("report.pdf")
                        .setPageId("200")
                        .setMediaType("application/pdf")
                        .setFileSize(1_048_576)
                        .setDownloadUrl(DOWNLOAD)));

        List<WarcParse.Record> records = warc(entry);

        // Nothing in the archive passes for the PDF: not under its download
        // URL, and not its file name stored as a PDF.
        assertThat(records).extracting(WarcParse.Record::targetUri).doesNotContain(DOWNLOAD);
        assertThat(records)
                .noneMatch(r -> "application/pdf".equals(r.headers().get("Content-Type")));
        assertThat(records).extracting(r -> new String(r.block(), StandardCharsets.UTF_8))
                .doesNotContain("report.pdf");
        // The attachment is still described and linked.
        WarcParse.Record markdown = conversion(records, "attachments/a1.md");
        assertThat(new String(markdown.block(), StandardCharsets.UTF_8))
                .contains("type: Attachment", "capture: not_fetched", "media_type: application/pdf",
                        "File size: 1048576 bytes");
        assertThat(markdown.refersTo()).isNull();
        assertThat(collection(records)).contains("href=\"" + DOWNLOAD + "\"");
    }

    @Test
    void anAttachmentWhoseBinaryWasFetchedKeepsItsResourceRecord() {
        byte[] pdf = "%PDF-1.7".getBytes(StandardCharsets.US_ASCII);
        CatalogEntry entry = entry(ConfluenceEntity.newBuilder()
                .setEntityId("a1")
                .setAttachment(Attachment.newBuilder().setId("a1").setTitle("report.pdf")
                        .setMediaType("application/pdf")
                        .setDownloadUrl(DOWNLOAD)
                        .setContent(ByteString.copyFrom(pdf))));

        List<WarcParse.Record> records = warc(entry);

        WarcParse.Record resource = resource(records, DOWNLOAD);
        assertThat(resource.block()).isEqualTo(pdf);
        assertThat(resource.headers().get("Content-Type")).isEqualTo("application/pdf");
        WarcParse.Record markdown = conversion(records, "attachments/a1.md");
        assertThat(markdown.refersTo()).isEqualTo(resource.recordId());
        assertThat(new String(markdown.block(), StandardCharsets.UTF_8))
                .doesNotContain("capture:");
    }

    @Test
    void aPageWithoutItsBodyHasNoResourceRecordAndNoStandInText() {
        CatalogEntry entry = entry(ConfluenceEntity.newBuilder()
                .setEntityId("200")
                .setPage(Page.newBuilder().setId("200").setTitle("Runbook").setWebUrl(PAGE_URL)));

        List<WarcParse.Record> records = warc(entry);

        assertThat(records).extracting(WarcParse.Record::targetUri).doesNotContain(PAGE_URL);
        String markdown = new String(conversion(records, "pages/200.md").block(),
                StandardCharsets.UTF_8);
        assertThat(markdown).contains("type: Page", "title: Runbook", "capture: not_fetched")
                .doesNotContain("No body fetched");
        assertThat(collection(records)).contains("href=\"" + PAGE_URL + "\"");
    }

    @Test
    void aBlogPostAndACommentWithoutTheirBodiesHaveNoResourceRecords() {
        CatalogEntry blog = entry(ConfluenceEntity.newBuilder()
                .setEntityId("201")
                .setBlogPost(BlogPost.newBuilder().setId("201").setTitle("Release notes")
                        .setWebUrl(BLOG_URL)));
        CatalogEntry comment = entry(ConfluenceEntity.newBuilder()
                .setEntityId("9")
                .setComment(Comment.newBuilder().setId("9").setPageId("200")
                        .setWebUrl(COMMENT_URL)));

        List<WarcParse.Record> records = warc(blog, comment);

        assertThat(records).extracting(WarcParse.Record::targetUri)
                .doesNotContain(BLOG_URL, COMMENT_URL);
        assertThat(new String(conversion(records, "blogs/201.md").block(), StandardCharsets.UTF_8))
                .contains("capture: not_fetched").doesNotContain("No body fetched");
        assertThat(new String(conversion(records, "comments/9.md").block(),
                StandardCharsets.UTF_8))
                .contains("capture: not_fetched").doesNotContain("No body fetched");
    }

    @Test
    void aPageWithItsBodyKeepsItsResourceRecord() {
        CatalogEntry entry = entry(ConfluenceEntity.newBuilder()
                .setEntityId("200")
                .setPage(Page.newBuilder().setId("200").setTitle("Runbook").setWebUrl(PAGE_URL)
                        .setBody(Body.newBuilder().setStorage(
                                BodyType.newBuilder().setValue("<h1>Runbook</h1>")))));

        List<WarcParse.Record> records = warc(entry);

        WarcParse.Record resource = resource(records, PAGE_URL);
        assertThat(new String(resource.block(), StandardCharsets.UTF_8))
                .isEqualTo("<h1>Runbook</h1>");
        assertThat(resource.headers().get("Content-Type")).isEqualTo("text/html");
        assertThat(OkfYaml.render(entry.concept())).doesNotContain("capture:");
    }

    private static CatalogEntry entry(ConfluenceEntity.Builder entity) {
        return ConfluenceCatalog.from(ConfluenceChange.newBuilder()
                .setChangeId("c")
                .setEntity(entity.setIngestedAt(Timestamp.newBuilder().setSeconds(1_700_000_000)))
                .build()).orElseThrow();
    }

    private static List<WarcParse.Record> warc(CatalogEntry... entries) {
        KnowledgeBundle bundle = KnowledgeBundle.assemble(
                "Engineering space",
                "OKF v0.2 capture of a ConfluenceService.Sync run.",
                ConfluenceCatalog.ACTOR,
                Instant.parse("2026-08-23T12:00:00Z"),
                "grpc-confluence/okf-producer",
                List.of(entries));
        assertThat(OkfConformance.check(bundle.okf())).isEmpty();
        return WarcParse.parse(WarcWriter.toBytes(bundle.warcRecords()));
    }

    private static WarcParse.Record resource(List<WarcParse.Record> records, String uri) {
        return records.stream()
                .filter(r -> "resource".equals(r.type()) && uri.equals(r.targetUri()))
                .findFirst().orElseThrow();
    }

    private static WarcParse.Record conversion(List<WarcParse.Record> records, String path) {
        return records.stream()
                .filter(r -> "conversion".equals(r.type())
                        && ("urn:okf:0.2:" + path).equals(r.targetUri()))
                .findFirst().orElseThrow();
    }

    private static String collection(List<WarcParse.Record> records) {
        return new String(resource(records, ai.pipestream.okf.warc.WarcArchive.COLLECTION_URI)
                .block(), StandardCharsets.UTF_8);
    }
}
