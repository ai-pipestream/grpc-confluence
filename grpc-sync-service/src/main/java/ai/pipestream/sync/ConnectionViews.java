package ai.pipestream.sync;

import ai.pipestream.sync.v1.Connection;

/**
 * Public (frontend / MCP) views of a {@link Connection}. Secrets never leave.
 */
public final class ConnectionViews {

    private ConnectionViews() {
    }

    /**
     * Returns a copy with {@code token} and {@code client_secret} cleared and
     * the {@code has_*} flags set from whether those fields were non-empty.
     *
     * @param connection stored row, possibly with secrets
     * @return redacted row
     */
    public static Connection redact(Connection connection) {
        boolean hasToken = !connection.getToken().isEmpty() || connection.getHasToken();
        boolean hasSecret = !connection.getClientSecret().isEmpty()
                || connection.getHasClientSecret();
        return connection.toBuilder()
                .clearToken()
                .clearClientSecret()
                .setHasToken(hasToken)
                .setHasClientSecret(hasSecret)
                .build();
    }
}
