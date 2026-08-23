package ai.pipestream.okf.warc;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;

class WarcDigestTest {

    @Test
    void sha1OfHelloMatchesKnownDigest() {
        byte[] sha1 = WarcDigest.sha1("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(HexFormat.of().formatHex(sha1)).isEqualTo("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d");
        String token = WarcDigest.sha1Base32("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(token).startsWith("sha1:");
        assertThat(token.substring(5)).isEqualTo(WarcDigest.base32NoPad(sha1));
        assertThat(token.substring(5)).doesNotContain("=").isUpperCase();
        // SHA-1("hello") as RFC 4648 base32, no padding (IA WARC-Block-Digest).
        assertThat(token).isEqualTo("sha1:VL2MMHO4YXUKFWV63YHTWSBM3GXKSQ2N");
    }

    @Test
    void emptySha1AndRfc4648Base32OfHelloString() {
        assertThat(HexFormat.of().formatHex(WarcDigest.sha1(new byte[0])))
                .isEqualTo("da39a3ee5e6b4b0d3255bfef95601890afd80709");
        assertThat(WarcDigest.base32NoPad("hello".getBytes(StandardCharsets.US_ASCII)))
                .isEqualTo("NBSWY3DP");
    }

    @Test
    void sha256HexIsPrefixedLowercase() {
        String hex = WarcDigest.sha256Hex("hello".getBytes(StandardCharsets.UTF_8));
        assertThat(hex).startsWith("sha256:");
        assertThat(hex.substring(7)).isLowerCase().hasSize(64);
    }
}
