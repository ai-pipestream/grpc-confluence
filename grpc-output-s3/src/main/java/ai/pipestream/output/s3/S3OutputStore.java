package ai.pipestream.output.s3;

import ai.pipestream.output.ObjectKeys;
import ai.pipestream.output.OutputEnv;
import ai.pipestream.output.OutputObject;
import ai.pipestream.output.OutputStore;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Writes artifacts to an S3 bucket. ServiceLoader id {@code s3}. Available
 * when {@code OUTPUT_S3_BUCKET} is set. Object keys follow Confluence /
 * Graph hierarchy under {@code OUTPUT_S3_PREFIX}.
 */
public final class S3OutputStore implements OutputStore {

    /** Store id. */
    public static final String ID = "s3";
    /** Default region when {@code OUTPUT_S3_REGION} is unset. */
    public static final String DEFAULT_REGION = "us-east-1";

    private S3ObjectPutter putter;
    private S3Client ownedClient;
    private String bucket;
    private String prefix;

    /**
     * Creates an unopened store (ServiceLoader).
     */
    public S3OutputStore() {
    }

    /**
     * Creates a store with an injected putter (tests).
     *
     * @param putter destination
     * @param bucket bucket
     * @param prefix key prefix, or blank
     */
    public S3OutputStore(S3ObjectPutter putter, String bucket, String prefix) {
        this.putter = Objects.requireNonNull(putter, "putter");
        this.bucket = Objects.requireNonNull(bucket, "bucket");
        this.prefix = prefix == null ? "" : prefix;
    }

    @Override
    public String id() {
        return ID;
    }

    @Override
    public boolean available(Map<String, String> env) {
        return putter != null || OutputEnv.blankToNull(env.get(OutputEnv.S3_BUCKET)) != null;
    }

    @Override
    public void open(Map<String, String> env) throws IOException {
        if (bucket == null) {
            bucket = OutputEnv.blankToNull(env.get(OutputEnv.S3_BUCKET));
        }
        if (bucket == null) {
            throw new IOException("s3 store needs OUTPUT_S3_BUCKET");
        }
        if (prefix == null || prefix.isBlank()) {
            String configured = OutputEnv.blankToNull(env.get(OutputEnv.S3_PREFIX));
            prefix = configured == null ? "" : configured;
        }
        if (putter == null) {
            String region = OutputEnv.blankToNull(env.get(OutputEnv.S3_REGION));
            ownedClient = S3Client.builder()
                    .region(Region.of(region == null ? DEFAULT_REGION : region))
                    .build();
            S3Client client = ownedClient;
            putter = (bkt, key, body, contentType) -> client.putObject(
                    PutObjectRequest.builder()
                            .bucket(bkt)
                            .key(key)
                            .contentType(contentType)
                            .build(),
                    RequestBody.fromBytes(body));
        }
    }

    @Override
    public void put(OutputObject object) throws IOException {
        if (putter == null || bucket == null) {
            throw new IOException("s3 store is not open");
        }
        String key = ObjectKeys.under(prefix, object.key());
        putter.put(bucket, key, object.content(), object.mediaType());
    }

    @Override
    public void close() {
        if (ownedClient != null) {
            ownedClient.close();
            ownedClient = null;
        }
    }

    /**
     * Configured bucket, or {@code null} before {@link #open}.
     *
     * @return bucket
     */
    public String bucket() {
        return bucket;
    }
}
