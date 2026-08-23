package ai.pipestream.output.s3;

import java.io.IOException;

/**
 * Puts one object into a bucket. Production wraps the AWS S3 client; tests
 * record calls.
 */
@FunctionalInterface
public interface S3ObjectPutter {

    /**
     * Writes {@code body} at {@code bucket}/{@code key}.
     *
     * @param bucket bucket name
     * @param key object key
     * @param body payload
     * @param contentType MIME type
     * @throws IOException if the put fails
     */
    void put(String bucket, String key, byte[] body, String contentType) throws IOException;
}
