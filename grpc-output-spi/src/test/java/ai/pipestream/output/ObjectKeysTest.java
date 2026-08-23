package ai.pipestream.output;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.microsoft.v1.Drive;
import ai.pipestream.microsoft.v1.DriveItem;
import ai.pipestream.microsoft.v1.MicrosoftChange;
import ai.pipestream.microsoft.v1.MicrosoftEntity;
import ai.pipestream.microsoft.v1.Site;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectKeysTest {

    @Test
    void confluenceHierarchyUsesSpaceThenPageThenChildren() {
        ConfluenceEntity page = ConfluenceEntity.newBuilder()
                .setEntityId("200")
                .setPage(Page.newBuilder().setId("200").setSpaceId("ENG").setTitle("Doc"))
                .build();
        assertThat(ObjectKeys.confluence(page)).isEqualTo("ENG/pages/200");

        assertThat(ObjectKeys.confluence(ConfluenceEntity.newBuilder()
                .setEntityId("9")
                .setComment(Comment.newBuilder().setId("9").setPageId("200"))
                .build())).isEqualTo("pages/200/comments/9");

        assertThat(ObjectKeys.confluence(ConfluenceEntity.newBuilder()
                .setEntityId("a1")
                .setAttachment(Attachment.newBuilder().setId("a1").setPageId("200"))
                .build())).isEqualTo("pages/200/attachments/a1");

        assertThat(ObjectKeys.confluence(ConfluenceEntity.newBuilder()
                .setEntityId("1")
                .setSpace(Space.newBuilder().setId("1").setKey("ENG"))
                .build())).isEqualTo("ENG/spaces/ENG");
    }

    @Test
    void microsoftHierarchyUsesSiteDriveItem() {
        assertThat(ObjectKeys.microsoft(MicrosoftEntity.newBuilder()
                .setEntityId("site-1")
                .setSite(Site.newBuilder().setId("site-1"))
                .build())).isEqualTo("sites/site-1");
        assertThat(ObjectKeys.microsoft(MicrosoftEntity.newBuilder()
                .setEntityId("drive-1")
                .setDrive(Drive.newBuilder().setId("drive-1").setSiteId("site-1"))
                .build())).isEqualTo("sites/site-1/drives/drive-1");
        assertThat(ObjectKeys.microsoft(MicrosoftEntity.newBuilder()
                .setEntityId("file-1")
                .setDriveItem(DriveItem.newBuilder().setId("file-1").setDriveId("drive-1"))
                .build())).isEqualTo("drives/drive-1/items/file-1");
    }

    @Test
    void ofUsesChangeEntity() {
        String key = ObjectKeys.of(ConfluenceChange.newBuilder()
                .setChangeId("c")
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setPage(Page.newBuilder().setId("200").setSpaceId("100")))
                .build());
        assertThat(key).isEqualTo("100/pages/200");
        assertThat(ObjectKeys.of(MicrosoftChange.newBuilder()
                .setChangeId("c")
                .setEntity(MicrosoftEntity.newBuilder()
                        .setEntityId("file-1")
                        .setDriveItem(DriveItem.newBuilder().setId("file-1").setDriveId("d")))
                .build())).isEqualTo("drives/d/items/file-1");
    }

    @Test
    void underAndSegment() {
        assertThat(ObjectKeys.under("run-1", "ENG/pages/200")).isEqualTo("run-1/ENG/pages/200");
        assertThat(ObjectKeys.under("", "ENG/pages/200")).isEqualTo("ENG/pages/200");
        assertThat(ObjectKeys.segment("a/b c")).isEqualTo("a_b_c");
    }
}
