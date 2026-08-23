package ai.pipestream.okf.microsoft;

import ai.pipestream.microsoft.v1.DriveItem;
import ai.pipestream.microsoft.v1.ListColumn;
import ai.pipestream.microsoft.v1.MicrosoftChange;
import ai.pipestream.microsoft.v1.MicrosoftEntity;
import ai.pipestream.okf.CatalogEntry;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MicrosoftCatalogTest {

    @Test
    void fileUsesDownloadUrlAndRendersColumns() {
        DriveItem item = DriveItem.newBuilder()
                .setId("file-1")
                .setName("notes.txt")
                .setDriveId("drive-1")
                .setWebUrl("https://contoso.sharepoint.com/notes.txt")
                .setDownloadUrl("https://contoso.sharepoint.com/download/notes.txt")
                .setMimeType("text/plain")
                .setDescription("a note")
                .addListColumns(ListColumn.newBuilder().setName("Title").setStringValue("Notes"))
                .addListColumns(ListColumn.newBuilder().setName("Count").setIntValue(3))
                .build();
        CatalogEntry entry = MicrosoftCatalog.from(MicrosoftChange.newBuilder()
                .setChangeId("c1")
                .setEntity(MicrosoftEntity.newBuilder()
                        .setEntityId("file-1")
                        .setIngestedAt(Timestamp.newBuilder().setSeconds(1))
                        .setDriveItem(item))
                .build()).orElseThrow();
        assertThat(entry.path()).isEqualTo("items/drive-1/file-1.md");
        assertThat(entry.targetUri()).isEqualTo("https://contoso.sharepoint.com/download/notes.txt");
        assertThat(entry.concept().body()).contains("| Title | Notes |").contains("| Count | 3 |");
    }
}
