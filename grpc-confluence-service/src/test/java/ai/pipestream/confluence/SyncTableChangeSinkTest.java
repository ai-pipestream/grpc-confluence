package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ChangeSource;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.BlogPost;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
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

    @Test
    void mapsSpaceBlogAndDeletePhase() {
        Asset space = SyncTableChangeSink.toAsset(ConfluenceChange.newBuilder()
                .setChangeId("c3")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setSource(ChangeSource.CHANGE_SOURCE_CRAWL)
                .setCursor("run-1")
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("100")
                        .setSpace(Space.newBuilder().setId("100").setKey("ENG").setName("Engineering")
                                .setWebUrl("https://example/wiki/spaces/ENG")))
                .build());
        assertThat(space.getAssetId()).isEqualTo("confluence:space:100");
        assertThat(space.getTitle()).isEqualTo("Engineering");

        Asset blog = SyncTableChangeSink.toAsset(ConfluenceChange.newBuilder()
                .setChangeId("c4")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setSource(ChangeSource.CHANGE_SOURCE_CQL_INCREMENTAL)
                .setCursor("run-1")
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("300")
                        .setBlogPost(BlogPost.newBuilder()
                                .setId("300")
                                .setSpaceId("100")
                                .setTitle("Release notes")))
                .build());
        assertThat(blog.getKind()).isEqualTo("blog_post");
        assertThat(blog.getPhase()).isEqualTo(ai.pipestream.sync.v1.AssetPhase.ASSET_PHASE_UPDATE);

        Asset deleted = SyncTableChangeSink.toAsset(ConfluenceChange.newBuilder()
                .setChangeId("c5")
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .setSource(ChangeSource.CHANGE_SOURCE_CQL_INCREMENTAL)
                .setCursor("run-1")
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setPage(Page.newBuilder().setId("200").setSpaceId("100")))
                .build());
        assertThat(deleted.getPhase()).isEqualTo(ai.pipestream.sync.v1.AssetPhase.ASSET_PHASE_DELETE);
    }
}
