package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ChangeSource;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.sync.v1.Asset;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SyncTableChangeSinkTest {

    @Test
    void mapsPageAndAttachment() {
        ConfluenceChange page = ConfluenceChange.newBuilder()
                .setChangeId("c1")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setSource(ChangeSource.CHANGE_SOURCE_CRAWL)
                .setCursor("run-1")
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setIngestedAt(Timestamp.newBuilder().setSeconds(1))
                        .setPage(Page.newBuilder().setId("200").setSpaceId("100").setTitle("Doc")))
                .build();
        Asset pageAsset = SyncTableChangeSink.toAsset(page);
        assertThat(pageAsset.getAssetId()).isEqualTo("confluence:page:200");
        assertThat(pageAsset.getPhase()).isEqualTo(ai.pipestream.sync.v1.AssetPhase.ASSET_PHASE_INITIAL_CRAWL);
        assertThat(pageAsset.getAttachment()).isFalse();

        ConfluenceChange attachment = ConfluenceChange.newBuilder()
                .setChangeId("c2")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setSource(ChangeSource.CHANGE_SOURCE_CQL_INCREMENTAL)
                .setCursor("run-1")
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("a1")
                        .setIngestedAt(Timestamp.newBuilder().setSeconds(1))
                        .setAttachment(Attachment.newBuilder()
                                .setId("a1")
                                .setTitle("notes.txt")
                                .setPageId("200")
                                .setFileSize(12)
                                .setMediaType("text/plain")))
                .build();
        Asset file = SyncTableChangeSink.toAsset(attachment);
        assertThat(file.getAttachment()).isTrue();
        assertThat(file.getParentAssetId()).isEqualTo("confluence:page:200");
        assertThat(file.getPhase()).isEqualTo(ai.pipestream.sync.v1.AssetPhase.ASSET_PHASE_UPDATE);
        assertThat(file.getContentBytes()).isEqualTo(12);
    }
}
