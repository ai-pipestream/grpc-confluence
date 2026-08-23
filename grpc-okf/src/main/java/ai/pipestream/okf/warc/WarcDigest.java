package ai.pipestream.okf.warc;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;

/**
 * WARC digest helpers. IIPC/IA use {@code WARC-Block-Digest: sha1:} plus RFC 4648
 * base32 without padding. We also emit hex SHA-256 in OKF extras.
 */
public final class WarcDigest {

    private WarcDigest() {
    }

    /**
     * SHA-1 of {@code payload}.
     *
     * @param payload bytes
     * @return 20-byte digest
     */
    public static byte[] sha1(byte[] payload) {
        return digest("SHA-1", payload);
    }

    /**
     * SHA-256 of {@code payload}.
     *
     * @param payload bytes
     * @return 32-byte digest
     */
    public static byte[] sha256(byte[] payload) {
        return digest("SHA-256", payload);
    }

    /**
     * {@code sha1:} plus RFC 4648 base32, no padding, uppercase.
     *
     * @param payload bytes
     * @return WARC digest token
     */
    public static String sha1Base32(byte[] payload) {
        return "sha1:" + base32NoPad(sha1(payload));
    }

    /**
     * {@code sha256:} plus lowercase hex.
     *
     * @param payload bytes
     * @return digest token
     */
    public static String sha256Hex(byte[] payload) {
        return "sha256:" + HexFormat.of().formatHex(sha256(payload));
    }

    /**
     * RFC 4648 base32, no padding, uppercase.
     *
     * @param data bytes
     * @return encoded string
     */
    public static String base32NoPad(byte[] data) {
        String encoded = new Base32().encodeToString(data).replace("=", "");
        return encoded.toUpperCase(Locale.ROOT);
    }

    private static byte[] digest(String algorithm, byte[] payload) {
        try {
            return MessageDigest.getInstance(algorithm).digest(payload);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(algorithm + " is required", e);
        }
    }

    /** Minimal RFC 4648 base32 encoder. */
    private static final class Base32 {
        private static final char[] ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".toCharArray();

        String encodeToString(byte[] data) {
            StringBuilder out = new StringBuilder((data.length * 8 + 4) / 5);
            int buffer = 0;
            int bitsLeft = 0;
            for (byte b : data) {
                buffer = (buffer << 8) | (b & 0xff);
                bitsLeft += 8;
                while (bitsLeft >= 5) {
                    out.append(ALPHABET[(buffer >> (bitsLeft - 5)) & 0x1f]);
                    bitsLeft -= 5;
                }
            }
            if (bitsLeft > 0) {
                out.append(ALPHABET[(buffer << (5 - bitsLeft)) & 0x1f]);
            }
            while (out.length() % 8 != 0) {
                out.append('=');
            }
            return out.toString();
        }
    }
}
