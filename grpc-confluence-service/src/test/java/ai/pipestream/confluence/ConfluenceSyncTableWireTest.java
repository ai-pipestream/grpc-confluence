package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.sync.AssetStore;
import ai.pipestream.sync.SyncTableGrpcService;
import ai.pipestream.sync.v1.Asset;
import ai.pipestream.sync.v1.AssetSyncStatus;
import ai.pipestream.sync.v1.GetAssetRequest;
import ai.pipestream.sync.v1.ListAssetsRequest;
import ai.pipestream.sync.v1.SyncTableServiceGrpc;
import ai.pipestream.sync.v1.UpsertAssetRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full Confluence Sync writes the generic ledger, including attachments
 * and a reconcile of rows not seen in the run.
 */
class ConfluenceSyncTableWireTest {

    private FakeConfluenceServer fake;
    private Server confluenceServer;
    private Server syncServer;
    private ManagedChannel confluenceChannel;
    private ManagedChannel syncChannel;

    @AfterEach
    void stop() {
        if (confluenceChannel != null) {
            confluenceChannel.shutdownNow();
        }
        if (syncChannel != null) {
            syncChannel.shutdownNow();
        }
        if (confluenceServer != null) {
            confluenceServer.shutdownNow();
        }
        if (syncServer != null) {
            syncServer.shutdownNow();
        }
        if (fake != null) {
            fake.close();
        }
    }

    @Test
    void syncWritesAttachmentsAndReconcilesStaleRows() throws Exception {
        fake = FakeConfluenceServer.start();
        Instant modified = Instant.parse("2024-03-02T00:00:00Z");
        fake.stub("/wiki/api/v2/spaces",
                ConfluenceFixtures.spaceListJson(null,
                        ConfluenceFixtures.spaceJson("100", "ENG", "Engineering")));
        fake.stub("/wiki/api/v2/spaces/100/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages",
                ConfluenceFixtures.pageListJson(null,
                        ConfluenceFixtures.pageJson("200", "100", "Design Doc", modified.toString())));
        fake.stub("/wiki/api/v2/pages/200/footer-comments", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/200/inline-comments", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/200/attachments",
                ConfluenceFixtures.listJson(null, "",
                        ConfluenceFixtures.attachmentJson("a1", "200")));
        fake.stub("/wiki/api/v2/pages/200/labels", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/pages/200/properties", ConfluenceFixtures.emptyListJson());
        fake.stub("/wiki/api/v2/blogposts", ConfluenceFixtures.emptyListJson());

        String syncName = InProcessServerBuilder.generateName();
        String confluenceName = InProcessServerBuilder.generateName();
        syncServer = InProcessServerBuilder.forName(syncName)
                .directExecutor()
                .addService(new SyncTableGrpcService(new AssetStore()))
                .build()
                .start();
        syncChannel = InProcessChannelBuilder.forName(syncName).directExecutor().build();
        SyncTableServiceGrpc.SyncTableServiceBlockingStub ledger =
                SyncTableServiceGrpc.newBlockingStub(syncChannel);
        ledger.upsertAsset(UpsertAssetRequest.newBuilder()
                .setAsset(Asset.newBuilder()
                        .setAssetId("confluence:page:gone")
                        .setSource("confluence")
                        .setKind("page")
                        .setNativeId("gone")
                        .setRunId("stale-run")
                        .setTitle("Removed"))
                .build());

        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("token-123")
                .build();
        ConfluenceClient client = new ConfluenceClient(config.baseUrl(), config.email(),
                config.apiToken(), Duration.ZERO);
        confluenceServer = InProcessServerBuilder.forName(confluenceName)
                .directExecutor()
                .addService(new ConfluenceGrpcService(config, client,
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES,
                        new SyncTableChangeSink(syncChannel)))
                .build()
                .start();
        confluenceChannel = InProcessChannelBuilder.forName(confluenceName).directExecutor().build();
        ConfluenceServiceGrpc.newBlockingStub(confluenceChannel)
                .sync(SyncRequest.getDefaultInstance())
                .forEachRemaining(ignored -> {
                });

        Asset page = ledger.getAsset(GetAssetRequest.newBuilder()
                .setAssetId("confluence:page:200").build()).getAsset();
        assertThat(page.getTitle()).isEqualTo("Design Doc");
        assertThat(page.getSource()).isEqualTo("confluence");

        Asset attachment = ledger.getAsset(GetAssetRequest.newBuilder()
                .setAssetId("confluence:attachment:a1").build()).getAsset();
        assertThat(attachment.getAttachment()).isTrue();
        assertThat(attachment.getParentAssetId()).isEqualTo("confluence:page:200");

        List<String> attachmentIds = new ArrayList<>();
        ledger.listAssets(ListAssetsRequest.newBuilder()
                .setSource("confluence")
                .setAttachmentsOnly(true)
                .build()).forEachRemaining(r -> attachmentIds.add(r.getAsset().getAssetId()));
        assertThat(attachmentIds).containsExactly("confluence:attachment:a1");

        assertThat(ledger.getAsset(GetAssetRequest.newBuilder()
                .setAssetId("confluence:page:gone").build()).getAsset().getStatus())
                .isEqualTo(AssetSyncStatus.ASSET_SYNC_STATUS_DELETED);
    }
}
