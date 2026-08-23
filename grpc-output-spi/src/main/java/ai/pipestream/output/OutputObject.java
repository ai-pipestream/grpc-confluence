package ai.pipestream.output;

import java.util.Objects;

/**
 * One object to persist: a hierarchy key, bytes, and a media type.
 *
 * @param key store-relative path using {@code /} (no leading slash)
 * @param content payload
 * @param mediaType MIME type
 */
public record OutputObject(String key, byte[] content, String mediaType) {

    /**
     * Copies the payload.
     *
     * @param key store-relative path
     * @param content payload
     * @param mediaType MIME type
     */
    public OutputObject {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(content, "content");
        Objects.requireNonNull(mediaType, "mediaType");
        if (key.isBlank() || key.startsWith("/") || key.contains("..")) {
            throw new IllegalArgumentException("illegal output key: " + key);
        }
        content = content.clone();
    }

    /**
     * Creates an object.
     *
     * @param key store-relative path
     * @param content payload
     * @param mediaType MIME type
     * @return the object
     */
    public static OutputObject of(String key, byte[] content, String mediaType) {
        return new OutputObject(key, content, mediaType);
    }
}
