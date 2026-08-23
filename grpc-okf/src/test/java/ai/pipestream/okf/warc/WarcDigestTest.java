package ai.pipestream.okf.warc;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class WarcDigestTest {

    @Test
    void sha1Base32IsRfc4648UppercaseNoPadding() {
        String digest = WarcDigest.sha1Base32("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(digest).startsWith("sha1:");
        assertThat(digest.substring(5)).isUpperCase().doesNotContain("=");
        assertThat(WarcDigest.sha1Base32("hello".getBytes(StandardCharsets.UTF_8)))
                .isEqualTo(digest);
    }

    @Test
    void sha256HexIsPrefixed() {
        assertThat(WarcDigest.sha256Hex(new byte[0])).startsWith("sha256:");
    }
}
