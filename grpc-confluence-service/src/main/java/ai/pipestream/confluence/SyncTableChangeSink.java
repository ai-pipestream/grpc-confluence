package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ChangeSource;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.sync.v1.Asset;
import ai.pipestream.sync.v1.AssetPhase;
import ai.pipestream.sync.v1.AssetSyncStatus;
import ai.pipestream.sync.v1.Checkpoint;
import ai.pipestream.sync.v1.DeleteAssetRequest;
import ai.pipestream.sync.v1.PutCheckpointRequest;
import ai.pipestream.sync.v1.ReconcileRequest;
import ai.pipestream.sync.v1.SyncTableServiceGrpc;
import ai.pipestream.sync.v1.UpsertAssetRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Writes every Confluence change into {@code SyncTableService}. Attachments
 * carry {@code attachment=true} and a parent page/blog asset id.
 */
public final class SyncTableChangeSink implements ChangeSink, AutoCloseable {

    /** Asset {@code source} written for every Confluence row. */
    public static final String SOURCE = "confluence";
    /** Environment variable for the {@code SyncTableService} target. */
    public static final String ENV_TARGET = "SYNC_TABLE_TARGET";
    /** Environment variable; {@code false} disables plaintext on the channel. */
    public static final String ENV_PLAINTEXT = "SYNC_TABLE_PLAINTEXT";

    private static final System.Logger LOG = System.getLogger(SyncTableChangeSink.class.getName());

    private final ManagedChannel channel;
    private final SyncTableServiceGrpc.SyncTableServiceBlockingStub stub;

    /**
     * Creates a sink over an existing channel (takes ownership for {@link #close()}).
     *
     * @param channel the gRPC channel to {@code SyncTableService}
     */
    public SyncTableChangeSink(ManagedChannel channel) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.stub = SyncTableServiceGrpc.newBlockingStub(channel);
    }

    /**
     * Whether sync-table publishing is configured in the process environment.
     *
     * @return {@code true} when {@link #ENV_TARGET} is set
     */
    public static boolean enabled() {
        String target = System.getenv(ENV_TARGET);
        return target != null && !target.isBlank();
    }

    /**
     * Builds a sink from {@code SYNC_TABLE_*} environment variables.
     *
     * @return a sink connected to the configured target
     */
    public static SyncTableChangeSink fromEnvironment() {
        String target = System.getenv(ENV_TARGET);
        if (target == null || target.isBlank()) {
            throw new IllegalStateException(ENV_TARGET + " is required");
        }
        boolean plaintext = !"false".equalsIgnoreCase(
                Objects.toString(System.getenv(ENV_PLAINTEXT), "true"));
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target.trim());
        if (plaintext) {
            builder.usePlaintext();
        }
        return new SyncTableChangeSink(builder.build());
    }

    @Override
    public void emit(ConfluenceChange change) {
        try {
            if (change.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE) {
                stub.deleteAsset(DeleteAssetRequest.newBuilder()
                        .setAssetId(assetId(change.getEntity()))
                        .setRunId(change.getCursor())
                        .setCursor(change.getCursor())
                        .build());
                return;
            }
            stub.upsertAsset(UpsertAssetRequest.newBuilder()
                    .setAsset(toAsset(change))
                    .build());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "sync-table emit failed: {0}", e.toString());
        }
    }

    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        try {
            stub.putCheckpoint(PutCheckpointRequest.newBuilder()
                    .setCheckpoint(Checkpoint.newBuilder()
                            .setSource(SOURCE)
                            .setScope(snapshot.getSpaceKey())
                            .setCursor(snapshot.getCursor()))
                    .build());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "sync-table snapshot failed: {0}", e.toString());
        }
    }

    /** Mark source rows not seen in this full-crawl run as deleted. */
    @Override
    public void completeRun(String runId) {
        if (runId == null || runId.isBlank()) {
            return;
        }
        try {
            stub.reconcile(ReconcileRequest.newBuilder()
                    .setSource(SOURCE)
                    .setRunId(runId)
                    .build());
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "sync-table reconcile failed: {0}", e.toString());
        }
    }

    static Asset toAsset(ConfluenceChange change) {
        ConfluenceEntity entity = change.getEntity();
        String kind = kind(entity);
        Asset.Builder asset = Asset.newBuilder()
                .setAssetId(assetId(entity))
                .setSource(SOURCE)
                .setNativeId(entity.getEntityId())
                .setKind(kind)
                .setPhase(phase(change))
                .setStatus(AssetSyncStatus.ASSET_SYNC_STATUS_SYNCED)
                .setRunId(change.getCursor())
                .setCursor(change.getCursor());
        if (entity.hasIngestedAt()) {
            asset.setLastModifiedAt(entity.getIngestedAt());
        }
        if (entity.getEntityCase() == ConfluenceEntity.EntityCase.ATTACHMENT) {
            Attachment attachment = entity.getAttachment();
            asset.setAttachment(true)
                    .setTitle(attachment.getTitle())
                    .setSourceUri(first(attachment.getWebUrl(), attachment.getDownloadUrl()))
                    .setContentBytes(attachment.getFileSize())
                    .setMediaType(attachment.getMediaType());
            String parent = first(attachment.getPageId(), attachment.getBlogPostId(),
                    attachment.getCustomContentId());
            if (!parent.isEmpty()) {
                String parentKind = !attachment.getPageId().isEmpty() ? "page"
                        : !attachment.getBlogPostId().isEmpty() ? "blog_post" : "custom_content";
                asset.setParentAssetId(SOURCE + ":" + parentKind + ":" + parent);
            }
        } else {
            asset.setTitle(title(entity)).setSourceUri(uri(entity));
        }
        asset.putAttributes("change_id", change.getChangeId());
        asset.putAttributes("operation", change.getOperation().name());
        return asset.build();
    }

    static String assetId(ConfluenceEntity entity) {
        return SOURCE + ":" + kind(entity) + ":" + entity.getEntityId();
    }

    private static String kind(ConfluenceEntity entity) {
        return entity.getEntityCase().name().toLowerCase(Locale.ROOT);
    }

    private static AssetPhase phase(ConfluenceChange change) {
        if (change.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE) {
            return AssetPhase.ASSET_PHASE_DELETE;
        }
        return change.getSource() == ChangeSource.CHANGE_SOURCE_CRAWL
                ? AssetPhase.ASSET_PHASE_INITIAL_CRAWL
                : AssetPhase.ASSET_PHASE_UPDATE;
    }

    private static String title(ConfluenceEntity entity) {
        return switch (entity.getEntityCase()) {
            case PAGE -> entity.getPage().getTitle();
            case BLOG_POST -> entity.getBlogPost().getTitle();
            case SPACE -> entity.getSpace().getName();
            case ATTACHMENT -> entity.getAttachment().getTitle();
            default -> entity.getEntityId();
        };
    }

    private static String uri(ConfluenceEntity entity) {
        return switch (entity.getEntityCase()) {
            case PAGE -> entity.getPage().getWebUrl();
            case BLOG_POST -> entity.getBlogPost().getWebUrl();
            case SPACE -> entity.getSpace().getWebUrl();
            case ATTACHMENT -> entity.getAttachment().getWebUrl();
            default -> "";
        };
    }

    private static String first(String... values) {
        for (String value : values) {
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return "";
    }

    /** Shuts down the underlying channel. */
    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
