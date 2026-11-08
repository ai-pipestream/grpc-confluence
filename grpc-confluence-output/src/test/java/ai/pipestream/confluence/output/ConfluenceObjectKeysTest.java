package ai.pipestream.confluence.output;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConfluenceObjectKeysTest {

    @Test
    void confluenceHierarchyUsesSpaceThenPageThenChildren() {
        ConfluenceEntity page = ConfluenceEntity.newBuilder()
                .setEntityId("200")
                .setPage(Page.newBuilder().setId("200").setSpaceId("ENG").setTitle("Doc"))
                .build();
        assertThat(ConfluenceObjectKeys.of(page)).isEqualTo("ENG/pages/200");

        assertThat(ConfluenceObjectKeys.of(ConfluenceEntity.newBuilder()
                .setEntityId("9")
                .setComment(Comment.newBuilder().setId("9").setPageId("200"))
                .build())).isEqualTo("pages/200/comments/9");

        assertThat(ConfluenceObjectKeys.of(ConfluenceEntity.newBuilder()
                .setEntityId("a1")
                .setAttachment(Attachment.newBuilder().setId("a1").setPageId("200"))
                .build())).isEqualTo("pages/200/attachments/a1");

        assertThat(ConfluenceObjectKeys.of(ConfluenceEntity.newBuilder()
                .setEntityId("1")
                .setSpace(Space.newBuilder().setId("1").setKey("ENG"))
                .build())).isEqualTo("ENG/spaces/ENG");
    }

    @Test
    void ofUsesChangeEntity() {
        String key = ConfluenceObjectKeys.of(ConfluenceChange.newBuilder()
                .setChangeId("c")
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setPage(Page.newBuilder().setId("200").setSpaceId("100")))
                .build());
        assertThat(key).isEqualTo("100/pages/200");
    }
}
