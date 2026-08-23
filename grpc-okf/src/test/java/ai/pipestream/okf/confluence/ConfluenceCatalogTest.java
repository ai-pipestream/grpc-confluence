package ai.pipestream.okf.confluence;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.Body;
import ai.pipestream.confluence.v1.BodyType;
import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Label;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.User;
import ai.pipestream.okf.CatalogEntry;
import ai.pipestream.okf.OkfConcept;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConfluenceCatalogTest {

    private static Timestamp ts() {
        return Timestamp.newBuilder().setSeconds(1).build();
    }

    @Test
    void pageUsesLiveWebUrlAsResource() {
        ConfluenceChange change = ConfluenceChange.newBuilder()
                .setChangeId("c1")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setIngestedAt(ts())
                        .setPage(Page.newBuilder()
                                .setId("200")
                                .setTitle("Doc")
                                .setWebUrl("https://example.atlassian.net/wiki/pages/200")
                                .addLabels(Label.newBuilder().setName("eng"))
                                .setBody(Body.newBuilder().setStorage(
                                        BodyType.newBuilder().setValue("<p>hi</p>")))))
                .build();
        CatalogEntry entry = ConfluenceCatalog.from(change).orElseThrow();
        assertThat(entry.path()).isEqualTo("pages/200.md");
        assertThat(entry.targetUri()).isEqualTo("https://example.atlassian.net/wiki/pages/200");
        assertThat(entry.kind()).isEqualTo("page");
        assertThat(entry.concept().type()).isEqualTo("Page");
        assertThat(entry.concept().resource()).contains(entry.targetUri());
        assertThat(entry.concept().tags()).contains("eng");
        assertThat(new String(entry.resourceBody())).contains("<p>hi</p>");
    }

    @Test
    void deleteMarksDeprecated() {
        CatalogEntry entry = ConfluenceCatalog.from(ConfluenceChange.newBuilder()
                .setChangeId("c2")
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setIngestedAt(ts())
                        .setPage(Page.newBuilder()
                                .setId("200")
                                .setTitle("Doc")
                                .setWebUrl("https://example/wiki/pages/200")))
                .build()).orElseThrow();
        assertThat(entry.concept().status()).contains(OkfConcept.Status.DEPRECATED);
    }

    @Test
    void missingWebUrlBecomesUrn() {
        CatalogEntry entry = ConfluenceCatalog.from(ConfluenceEntity.newBuilder()
                .setEntityId("200")
                .setIngestedAt(ts())
                .setPage(Page.newBuilder().setId("200").setTitle("Doc"))
                .build());
        assertThat(entry.targetUri()).isEqualTo("urn:okf:0.2:confluence:page:200");
        assertThat(entry.concept().resource()).contains(entry.targetUri());
    }

    @Test
    void spaceCommentAttachmentAndFallback() {
        CatalogEntry space = ConfluenceCatalog.from(ConfluenceEntity.newBuilder()
                .setEntityId("1")
                .setIngestedAt(ts())
                .setSpace(Space.newBuilder().setId("1").setKey("ENG").setName("Engineering")
                        .setWebUrl("https://example/wiki/spaces/ENG"))
                .build());
        assertThat(space.path()).isEqualTo("spaces/ENG.md");
        assertThat(space.kind()).isEqualTo("space");

        CatalogEntry comment = ConfluenceCatalog.from(ConfluenceEntity.newBuilder()
                .setEntityId("9")
                .setIngestedAt(ts())
                .setComment(Comment.newBuilder().setId("9").setPageId("200")
                        .setBody(Body.newBuilder().setStorage(BodyType.newBuilder().setValue("hi"))))
                .build());
        assertThat(comment.path()).isEqualTo("comments/9.md");
        assertThat(comment.targetUri()).startsWith("urn:okf:0.2:confluence:comment:");

        byte[] bytes = "file".getBytes();
        CatalogEntry attachment = ConfluenceCatalog.from(ConfluenceEntity.newBuilder()
                .setEntityId("a1")
                .setIngestedAt(ts())
                .setAttachment(Attachment.newBuilder().setId("a1").setTitle("notes.txt")
                        .setMediaType("text/plain")
                        .setDownloadUrl("https://example/wiki/download/a1")
                        .setContent(ByteString.copyFrom(bytes)))
                .build());
        assertThat(attachment.targetUri()).isEqualTo("https://example/wiki/download/a1");
        assertThat(attachment.resourceBody()).isEqualTo(bytes);
        assertThat(attachment.concept().extra()).containsKey("sha256");

        CatalogEntry user = ConfluenceCatalog.from(ConfluenceEntity.newBuilder()
                .setEntityId("u1")
                .setIngestedAt(ts())
                .setUser(User.newBuilder().setAccountId("u1").setDisplayName("Ada"))
                .build());
        assertThat(user.path()).startsWith("entities/");
        assertThat(user.kind()).isEqualTo("user");
        assertThat(user.concept().type()).isEqualTo("User");
    }

    @Test
    void labelAndPropertiesUseUrns() {
        CatalogEntry label = ConfluenceCatalog.from(ConfluenceEntity.newBuilder()
                .setEntityId("l1")
                .setIngestedAt(ts())
                .setLabel(Label.newBuilder().setId("l1").setName("ops").setPrefix("global"))
                .build());
        assertThat(label.path()).isEqualTo("labels/l1.md");
        assertThat(label.targetUri()).isEqualTo("urn:okf:0.2:confluence:label:l1");
        assertThat(label.concept().tags()).contains("ops");

        CatalogEntry contentProperty = ConfluenceCatalog.from(ConfluenceEntity.newBuilder()
                .setEntityId("p1")
                .setIngestedAt(ts())
                .setContentProperty(ai.pipestream.confluence.v1.ContentProperty.newBuilder()
                        .setId("p1")
                        .setCustomKey("score"))
                .build());
        assertThat(contentProperty.path()).isEqualTo("properties/content/p1.md");
        assertThat(contentProperty.kind()).isEqualTo("content-property");

        CatalogEntry spaceProperty = ConfluenceCatalog.from(ConfluenceEntity.newBuilder()
                .setEntityId("sp1")
                .setIngestedAt(ts())
                .setSpaceProperty(ai.pipestream.confluence.v1.SpaceProperty.newBuilder()
                        .setId("sp1"))
                .build());
        assertThat(spaceProperty.path()).isEqualTo("properties/space/sp1.md");
        assertThat(spaceProperty.kind()).isEqualTo("space-property");
    }

    @Test
    void atlasDocBodyIsFencedJson() {
        CatalogEntry entry = ConfluenceCatalog.from(ConfluenceEntity.newBuilder()
                .setEntityId("200")
                .setIngestedAt(ts())
                .setPage(Page.newBuilder().setId("200").setTitle("Doc")
                        .setWebUrl("https://example/wiki/pages/200")
                        .setBody(Body.newBuilder().setAtlasDocFormat(
                                BodyType.newBuilder().setValue("{\"type\":\"doc\"}"))))
                .build());
        assertThat(new String(entry.resourceBody())).contains("```json")
                .contains("{\"type\":\"doc\"}");
    }

    @Test
    void emptyChangeIsSkipped() {
        assertThat(ConfluenceCatalog.from(ConfluenceChange.newBuilder().setChangeId("x").build()))
                .isEmpty();
    }
}
