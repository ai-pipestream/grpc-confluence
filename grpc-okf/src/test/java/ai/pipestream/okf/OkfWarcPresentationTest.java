package ai.pipestream.okf;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.BlogPost;
import ai.pipestream.confluence.v1.Body;
import ai.pipestream.confluence.v1.BodyType;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Label;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.okf.confluence.ConfluenceCatalog;
import ai.pipestream.okf.warc.WarcParse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end OKF + WARC shape a Confluence crawl should present: one live URI
 * per resource, conversion markdown, collection HTML linking those URIs, zip
 * as a sibling file never nested in WARC.
 */
class OkfWarcPresentationTest {

    private static Timestamp ts() {
        return Timestamp.newBuilder().setSeconds(1_700_000_000).build();
    }

    @Test
    void confluenceBundleHasSiblingWarcNotZipInsideWarc(@TempDir Path dir) throws Exception {
        List<CatalogEntry> entries = new ArrayList<>();
        entries.add(ConfluenceCatalog.from(entity(ConfluenceEntity.newBuilder()
                .setEntityId("ENG")
                .setSpace(Space.newBuilder().setId("1").setKey("ENG").setName("Engineering")
                        .setWebUrl("https://example.atlassian.net/wiki/spaces/ENG")))).orElseThrow());
        entries.add(ConfluenceCatalog.from(entity(ConfluenceEntity.newBuilder()
                .setEntityId("200")
                .setPage(Page.newBuilder().setId("200").setTitle("Runbook")
                        .setSpaceId("1")
                        .setWebUrl("https://example.atlassian.net/wiki/spaces/ENG/pages/200")
                        .addLabels(Label.newBuilder().setName("ops"))
                        .setBody(Body.newBuilder().setStorage(
                                BodyType.newBuilder().setValue("<h1>Runbook</h1>")))))).orElseThrow());
        entries.add(ConfluenceCatalog.from(entity(ConfluenceEntity.newBuilder()
                .setEntityId("201")
                .setBlogPost(BlogPost.newBuilder().setId("201").setTitle("Release notes")
                        .setWebUrl("https://example.atlassian.net/wiki/spaces/ENG/blog/201")
                        .setBody(Body.newBuilder().setView(
                                BodyType.newBuilder().setValue("<p>shipped</p>")))))).orElseThrow());
        entries.add(ConfluenceCatalog.from(entity(ConfluenceEntity.newBuilder()
                .setEntityId("9")
                .setComment(Comment.newBuilder().setId("9").setPageId("200")
                        .setWebUrl("https://example.atlassian.net/wiki/spaces/ENG/pages/200?focusedCommentId=9")
                        .setBody(Body.newBuilder().setStorage(
                                BodyType.newBuilder().setValue("<p>lgtm</p>")))))).orElseThrow());
        byte[] png = new byte[] {(byte) 0x89, 0x50, 0x4e, 0x47};
        entries.add(ConfluenceCatalog.from(entity(ConfluenceEntity.newBuilder()
                .setEntityId("a1")
                .setAttachment(Attachment.newBuilder().setId("a1").setTitle("diagram.png")
                        .setPageId("200")
                        .setMediaType("image/png")
                        .setFileSize(png.length)
                        .setWebUrl("https://example.atlassian.net/wiki/spaces/ENG/pages/200")
                        .setDownloadUrl("https://example.atlassian.net/wiki/download/a1")
                        .setContent(ByteString.copyFrom(png))))).orElseThrow());

        KnowledgeBundle bundle = KnowledgeBundle.assemble(
                "Engineering space",
                "OKF v0.2 capture of a ConfluenceService.Sync run.",
                ConfluenceCatalog.ACTOR,
                Instant.parse("2026-08-23T12:00:00Z"),
                "grpc-confluence/okf-producer",
                entries);
        Path tree = dir.resolve("eng");
        Path zip = dir.resolve("eng.zip");
        Path warcGz = dir.resolve("eng.warc.gz");
        bundle.write(new OkfOutput(tree, zip, warcGz));

        assertThat(OkfConformance.check(bundle.okf())).isEmpty();
        assertThat(Files.readString(tree.resolve("index.md"))).contains("okf_version: \"0.2\"");
        assertThat(tree.resolve("pages/200.md")).exists();
        assertThat(tree.resolve("blogs/201.md")).exists();
        assertThat(tree.resolve("comments/9.md")).exists();
        assertThat(tree.resolve("attachments/a1.md")).exists();
        assertThat(tree.resolve("spaces/ENG.md")).exists();
        assertThat(tree.resolve("attested-computations/run-grpc-sync.md")).exists();
        String collection = Files.readString(tree.resolve("collection.html"));
        assertThat(collection).contains("href=\"https://example.atlassian.net/wiki/spaces/ENG/pages/200\"");
        assertThat(collection).contains("href=\"https://example.atlassian.net/wiki/download/a1\"");
        assertThat(collection).doesNotContain("href=\"pages/200.md\"");

        try (ZipFile zipFile = new ZipFile(zip.toFile())) {
            assertThat(zipFile.getEntry("pages/200.md")).isNotNull();
            assertThat(zipFile.getEntry("collection.html")).isNotNull();
        }

        byte[] warcBytes;
        try (GZIPInputStream in = new GZIPInputStream(Files.newInputStream(warcGz))) {
            warcBytes = in.readAllBytes();
        }
        List<WarcParse.Record> records = WarcParse.parse(warcBytes);
        assertThat(records.get(0).type()).isEqualTo("warcinfo");
        assertThat(records.get(0).headers()).doesNotContainKey("WARC-Target-URI");

        List<WarcParse.Record> resources = records.stream()
                .filter(r -> "resource".equals(r.type())).toList();
        List<WarcParse.Record> conversions = records.stream()
                .filter(r -> "conversion".equals(r.type())).toList();
        assertThat(conversions).hasSize(entries.size());
        assertThat(resources).hasSize(entries.size() + 1);

        List<String> liveUris = entries.stream().map(CatalogEntry::targetUri).toList();
        assertThat(resources).extracting(WarcParse.Record::targetUri)
                .containsAll(liveUris)
                .contains(ai.pipestream.okf.warc.WarcArchive.COLLECTION_URI);

        for (int i = 0; i < entries.size(); i++) {
            WarcParse.Record resource = records.get(1 + i * 2);
            WarcParse.Record conversion = records.get(2 + i * 2);
            assertThat(resource.type()).isEqualTo("resource");
            assertThat(conversion.type()).isEqualTo("conversion");
            assertThat(conversion.refersTo()).isEqualTo(resource.recordId());
            assertThat(conversion.targetUri()).startsWith("urn:okf:0.2:");
        }

        WarcParse.Record attachment = records.stream()
                .filter(r -> "https://example.atlassian.net/wiki/download/a1".equals(r.targetUri()))
                .findFirst().orElseThrow();
        assertThat(attachment.block()).isEqualTo(png);

        String warcText = new String(warcBytes, StandardCharsets.ISO_8859_1);
        assertThat(warcText).doesNotContain("PK\u0003\u0004");
        assertThat(Files.size(zip)).isPositive();
        assertThat(Files.size(warcGz)).isPositive();
        assertThat(warcBytes.length).isNotEqualTo(Files.size(zip));
    }

    @Test
    void changeWithoutEntityIsSkipped() {
        assertThat(ConfluenceCatalog.from(ConfluenceChange.newBuilder().setChangeId("x").build()))
                .isEmpty();
    }

    private static ConfluenceChange entity(ConfluenceEntity.Builder entity) {
        return ConfluenceChange.newBuilder()
                .setChangeId("c")
                .setEntity(entity.setIngestedAt(ts()))
                .build();
    }
}
