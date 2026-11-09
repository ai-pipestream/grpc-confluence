package ai.pipestream.confluence;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A thin authorized client for the Confluence Cloud REST API v2: JSON in, JSON
 * out, basic auth from the account email plus API token, and the service's
 * throttling contract honored - 429 responses are retried after the
 * server-directed {@code Retry-After} with jitter, and a politeness limiter
 * keeps a minimum gap between requests. No Atlassian SDK: the crawler needs a
 * handful of read routes, and the whole client stays inspectable.
 *
 * <p>All calls are blocking and virtual-thread friendly: run them on virtual
 * threads and the {@code Thread.sleep} backoffs park instead of pinning.</p>
 */
public final class ConfluenceClient {

    /** A Confluence API error, carrying the service's status and raw body. */
    public static final class ConfluenceApiException extends IOException {
        /** Serialization identifier. */
        @java.io.Serial
        private static final long serialVersionUID = 1L;

        /** HTTP status of the failing response. */
        private final int status;
        /** Raw error response body. */
        private final String body;

        ConfluenceApiException(int status, String body) {
            super("Confluence " + status + ": " + abbreviate(body));
            this.status = status;
            this.body = body == null ? "" : body;
        }

        /**
         * The HTTP status of the failing response.
         *
         * @return the status code
         */
        public int status() {
            return status;
        }

        /**
         * The raw error response body.
         *
         * @return the body, empty when the service sent none
         */
        public String body() {
            return body;
        }

        private static String abbreviate(String body) {
            if (body == null) {
                return "";
            }
            String oneLine = body.replace('\n', ' ').trim();
            return oneLine.length() <= 200 ? oneLine : oneLine.substring(0, 200) + "...";
        }
    }

    /**
     * Response parser. Floating-point numbers parse as exact
     * {@link java.math.BigDecimal}s with their scale kept, and integers
     * beyond int64 as {@link java.math.BigInteger}s, so no numeric value is
     * rounded before the mapper decides between a double and an exact
     * decimal. {@code doubleValue()} and integral accessors on these nodes
     * return the same values the default double parse gave.
     */
    private static final ObjectMapper JSON = JsonMapper.builder()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .disable(JsonNodeFeature.STRIP_TRAILING_BIGDECIMAL_ZEROES)
            .build();
    private static final int MAX_THROTTLE_RETRIES = 5;
    /** Redirect hops allowed on an attachment download (302 to the media CDN). */
    private static final int MAX_DOWNLOAD_REDIRECTS = 5;
    private static final Duration DEFAULT_MIN_REQUEST_INTERVAL = Duration.ofMillis(100);

    /**
     * The mapper responses are parsed with, so tests build exactly the
     * trees the client hands the mapper.
     *
     * @return the shared, fully configured response mapper
     */
    static ObjectMapper responseJson() {
        return JSON;
    }

    private final String baseUrl;
    private final String origin;
    private final String basePath;
    private final String authHeader;
    private final HttpClient http;
    private final Duration minRequestInterval;

    private long lastRequestNanos;

    /**
     * Creates a client from connector config (default politeness interval).
     *
     * @param config the crawler config supplying base URL and credentials
     */
    public ConfluenceClient(ConfluenceConnectorConfig config) {
        this(config.baseUrl(), config.email(), config.apiToken());
    }

    /**
     * Creates a client with the default politeness interval.
     *
     * @param baseUrl the Confluence Cloud base URL including {@code /wiki}
     * @param email the Atlassian account email for basic auth
     * @param apiToken the Atlassian API token for basic auth
     */
    public ConfluenceClient(String baseUrl, String email, String apiToken) {
        this(baseUrl, email, apiToken, DEFAULT_MIN_REQUEST_INTERVAL);
    }

    /**
     * Creates a client. {@code minRequestInterval} is overridable for tests
     * (zero disables politeness).
     *
     * @param baseUrl the Confluence Cloud base URL including {@code /wiki}
     * @param email the Atlassian account email for basic auth
     * @param apiToken the Atlassian API token for basic auth
     * @param minRequestInterval minimum gap between requests; {@link Duration#ZERO} disables
     */
    public ConfluenceClient(String baseUrl, String email, String apiToken,
            Duration minRequestInterval) {
        String normalized = Strings.stripTrailingSlashes(Objects.requireNonNull(baseUrl, "baseUrl"));
        this.baseUrl = normalized;
        URI uri = URI.create(normalized);
        this.origin = uri.getScheme() + "://" + uri.getAuthority();
        this.basePath = uri.getPath() == null ? "" : uri.getPath();
        String credentials = Objects.requireNonNull(email, "email") + ":"
                + Objects.requireNonNull(apiToken, "apiToken");
        this.authHeader = "Basic "
                + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
        this.minRequestInterval = Objects.requireNonNull(minRequestInterval);
    }

    /**
     * One page of a list response: the body plus the resolved next-page URL, if any.
     *
     * @param body the JSON list body
     * @param nextUrl the absolute next-page URL, or {@code null} when this page is last
     */
    public record ResultPage(JsonNode body, String nextUrl) {
    }

    /**
     * GET a v2 route (e.g. {@code /api/v2/pages}) or an absolute URL taken
     * from a {@code _links.next} cursor, with query parameters.
     *
     * @param pathOrUrl a v2 path or an absolute URL
     * @param query query parameters; empty when the URL already carries them
     * @return the parsed JSON body
     * @throws IOException if the request fails or the service returns an error status
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public JsonNode get(String pathOrUrl, Map<String, String> query)
            throws IOException, InterruptedException {
        return json(send(request(url(pathOrUrl, query)).GET().build(),
                HttpResponse.BodyHandlers.ofString()));
    }

    /**
     * GET with no query parameters.
     *
     * @param pathOrUrl a v2 path or an absolute URL
     * @return the parsed JSON body
     * @throws IOException if the request fails or the service returns an error status
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public JsonNode get(String pathOrUrl) throws IOException, InterruptedException {
        return get(pathOrUrl, Map.of());
    }

    /**
     * GET one page of a cursor-paginated list endpoint. The next-page URL
     * comes from the body's {@code _links.next} (relative against the tenant
     * origin) or, failing that, a {@code Link: <...>; rel="next"} header.
     *
     * @param pathOrUrl a v2 path or an absolute URL
     * @param query query parameters; empty when the URL already carries them
     * @return the page body and resolved next-page URL
     * @throws IOException if the request fails or the service returns an error status
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public ResultPage getPage(String pathOrUrl, Map<String, String> query)
            throws IOException, InterruptedException {
        HttpResponse<String> response = send(request(url(pathOrUrl, query)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        JsonNode body = json(response);
        String next = nextPageUrl(body)
                .or(() -> nextFromLinkHeader(response).map(this::resolveRelative))
                .orElse(null);
        return new ResultPage(body, next);
    }

    /**
     * Download an attachment binary. {@code downloadUrl} may be the relative
     * {@code downloadLink} of an attachment; it is resolved against the
     * tenant origin.
     *
     * <p>Confluence Cloud answers the download route with a 302 to a
     * pre-signed {@code api.media.atlassian.com} URL. Redirects are followed
     * here by hand (the shared client never follows) and the basic-auth
     * header is only sent to the tenant origin - the media URL carries its
     * own token and must not see the API credential.</p>
     *
     * @param downloadUrl a relative or absolute attachment download URL
     * @return the response body bytes
     * @throws IOException if the request fails, redirects too many times, or
     *         the service returns an error status
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public byte[] downloadAttachmentBytes(String downloadUrl)
            throws IOException, InterruptedException {
        String url = resolveRelative(downloadUrl);
        for (int hop = 0; hop <= MAX_DOWNLOAD_REDIRECTS; hop++) {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(60))
                    .header("accept", "*/*");
            if (onOrigin(url)) {
                builder.header("authorization", authHeader);
            }
            HttpResponse<byte[]> response = send(builder.GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            int status = response.statusCode();
            if (status >= 300 && status < 400) {
                String location = response.headers().firstValue("location").orElse(null);
                if (location == null || location.isBlank()) {
                    throw new ConfluenceApiException(status,
                            "redirect without a Location header from " + url);
                }
                url = URI.create(url).resolve(location).toString();
                continue;
            }
            if (status >= 400) {
                throw new ConfluenceApiException(status,
                        new String(response.body(), StandardCharsets.UTF_8));
            }
            return response.body();
        }
        throw new IOException("attachment download exceeded " + MAX_DOWNLOAD_REDIRECTS
                + " redirects: " + downloadUrl);
    }

    /**
     * POST a JSON body to a v2 route that is a read in all but verb
     * ({@code /users-bulk}, {@code /content/convert-ids-to-types}).
     *
     * @param path a v2 path
     * @param body the JSON request body
     * @return the parsed JSON response body
     * @throws IOException if the request fails or the service returns an error status
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public JsonNode post(String path, JsonNode body) throws IOException, InterruptedException {
        HttpRequest req = request(url(path, Map.of()))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                .build();
        return json(send(req, HttpResponse.BodyHandlers.ofString()));
    }

    /**
     * Open an attachment binary as a stream, following the media redirect the
     * same way {@link #downloadAttachmentBytes} does (credentials only to the
     * tenant origin). The caller must close the stream.
     *
     * @param downloadUrl a relative or absolute attachment download URL
     * @return the response body stream
     * @throws IOException if the request fails, redirects too many times, or
     *         the service returns an error status
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public InputStream openAttachmentStream(String downloadUrl)
            throws IOException, InterruptedException {
        return openFollowingRedirects(resolveRelative(downloadUrl), downloadUrl).body();
    }

    /**
     * A streamed binary response: the body plus what the final response
     * declared about it.
     *
     * @param body the response body stream; the caller must close it
     * @param contentType the final response's {@code Content-Type}, empty when absent
     * @param contentLength the final response's {@code Content-Length}, -1 when absent
     */
    public record StreamedBinary(InputStream body, String contentType, long contentLength) {
    }

    /**
     * GET a v2 route that answers with a redirect to a binary (for example
     * {@code /api/v2/attachments/{id}/thumbnail/download}) and open the
     * final response as a stream. Redirects are followed exactly as in
     * {@link #openAttachmentStream} (credentials only to the tenant origin).
     *
     * @param path a v2 path
     * @param query query parameters
     * @return the body stream and the final response's declared type and length
     * @throws IOException if the request fails, redirects too many times, or
     *         the service returns an error status
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public StreamedBinary openBinaryStream(String path, Map<String, String> query)
            throws IOException, InterruptedException {
        HttpResponse<InputStream> response = openFollowingRedirects(url(path, query), path);
        return new StreamedBinary(response.body(),
                response.headers().firstValue("content-type").orElse(""),
                response.headers().firstValueAsLong("content-length").orElse(-1L));
    }

    /** GETs {@code url}, following redirects by hand; the 2xx response, body unread. */
    private HttpResponse<InputStream> openFollowingRedirects(String url, String requested)
            throws IOException, InterruptedException {
        for (int hop = 0; hop <= MAX_DOWNLOAD_REDIRECTS; hop++) {
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(60))
                    .header("accept", "*/*");
            if (onOrigin(url)) {
                builder.header("authorization", authHeader);
            }
            HttpResponse<InputStream> response = send(builder.GET().build(),
                    HttpResponse.BodyHandlers.ofInputStream());
            int status = response.statusCode();
            if (status >= 300 && status < 400) {
                response.body().close();
                String location = response.headers().firstValue("location").orElse(null);
                if (location == null || location.isBlank()) {
                    throw new ConfluenceApiException(status,
                            "redirect without a Location header from " + url);
                }
                url = URI.create(url).resolve(location).toString();
                continue;
            }
            if (status >= 400) {
                try (InputStream error = response.body()) {
                    throw new ConfluenceApiException(status,
                            new String(error.readAllBytes(), StandardCharsets.UTF_8));
                }
            }
            return response;
        }
        throw new IOException("download exceeded " + MAX_DOWNLOAD_REDIRECTS
                + " redirects: " + requested);
    }

    /**
     * The absolute URL of the next result page, from the body's
     * {@code _links.next} (relative against the tenant origin).
     *
     * @param body a list-endpoint JSON body
     * @return the absolute next-page URL, or empty when the page is the last
     */
    public Optional<String> nextPageUrl(JsonNode body) {
        JsonNode next = body.path("_links").path("next");
        if (next.isTextual() && !next.asText().isBlank()) {
            return Optional.of(resolveRelative(next.asText()));
        }
        return Optional.empty();
    }

    /**
     * The next-page URL a {@code Link} response header points at, when the
     * service paginates through headers instead of the body.
     */
    static Optional<String> nextFromLinkHeader(HttpResponse<?> response) {
        return response.headers().firstValue("Link").flatMap(header -> {
            for (String part : Strings.split(header, ',')) {
                java.util.List<String> segments = Strings.split(part, ';');
                if (segments.size() == 2 && segments.get(1).trim().equals("rel=\"next\"")) {
                    String url = segments.get(0).trim();
                    if (url.startsWith("<") && url.endsWith(">")) {
                        return Optional.of(url.substring(1, url.length() - 1));
                    }
                }
            }
            return Optional.empty();
        });
    }

    private String url(String pathOrUrl, Map<String, String> query) {
        String url;
        if (pathOrUrl.startsWith("http")) {
            url = pathOrUrl;
        } else {
            requireSafePath(pathOrUrl);
            url = baseUrl + pathOrUrl;
        }
        if (query == null || query.isEmpty()) {
            return url;
        }
        StringBuilder sb = new StringBuilder(url);
        sb.append(url.contains("?") ? '&' : '?');
        boolean first = true;
        for (Map.Entry<String, String> entry : query.entrySet()) {
            if (!first) {
                sb.append('&');
            }
            sb.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)).append('=')
                    .append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
            first = false;
        }
        return sb.toString();
    }

    /**
     * A list route handed back a next-page URL it had already returned in
     * the same walk: following it would loop forever.
     */
    public static final class RepeatedCursorException extends IOException {
        /** Serialization identifier. */
        @java.io.Serial
        private static final long serialVersionUID = 1L;

        RepeatedCursorException(String message) {
            super(message);
        }
    }

    /**
     * One pagination walk's memory of the next-page URLs it followed, so a
     * service that repeats a cursor fails the walk instead of spinning.
     * Not thread-safe: one guard per walk.
     */
    public static final class CursorGuard {
        private final String route;
        private final java.util.Set<String> seen = new java.util.HashSet<>();

        /**
         * Creates a guard for one walk.
         *
         * @param route the route being walked, for the error message
         */
        public CursorGuard(String route) {
            this.route = route;
        }

        /**
         * Admits the next-page URL of the page just read.
         *
         * @param next the next-page URL, or {@code null} at the last page
         * @return {@code next}, unchanged
         * @throws RepeatedCursorException when {@code next} was already followed
         */
        public String check(String next) throws RepeatedCursorException {
            if (next != null && !seen.add(next)) {
                throw new RepeatedCursorException(route + " repeated the next-page URL " + next
                        + "; refusing to loop");
            }
            return next;
        }
    }

    /**
     * A route path that a request id spliced into is not a single literal
     * route: it carries a query, fragment, backslash, whitespace, a dot
     * segment, or an escape of a dot, slash or backslash. Ids reach paths verbatim, so such a value could point
     * the tenant credential at a different route than the rpc names.
     */
    public static final class UnsafePathException extends IllegalArgumentException {
        /** Serialization identifier. */
        @java.io.Serial
        private static final long serialVersionUID = 1L;

        UnsafePathException(String message) {
            super(message);
        }
    }

    /**
     * Refuses a relative route path that is not one literal route (see
     * {@link UnsafePathException}). Percent-escapes are allowed (a route may
     * encode a free-form key) except those of {@code .}, {@code /} and
     * {@code \}, which a server may decode back into a dot segment or a
     * path separator.
     */
    static void requireSafePath(String path) {
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '%') {
                String escape = i + 2 < path.length() ? path.substring(i + 1, i + 3) : "";
                if (escape.length() != 2 || Character.digit(escape.charAt(0), 16) < 0
                        || Character.digit(escape.charAt(1), 16) < 0
                        || escape.equalsIgnoreCase("2e") || escape.equalsIgnoreCase("2f")
                        || escape.equalsIgnoreCase("5c")) {
                    throw new UnsafePathException("an id in the route carries the escape %"
                            + escape + ", which could change the route");
                }
                continue;
            }
            if (c == '?' || c == '#' || c == '\\' || c <= ' ' || c == 0x7f) {
                throw new UnsafePathException("an id in the route contains '" + c
                        + "', which a Confluence id never does");
            }
        }
        for (String segment : Strings.split(path, '/')) {
            if (segment.equals(".") || segment.equals("..")) {
                throw new UnsafePathException("an id in the route is a dot segment");
            }
        }
    }

    /**
     * Whether {@code url} is on the tenant origin: same scheme, host and
     * port, no user-info. A string-prefix test is not enough - it accepts
     * {@code https://tenant.atlassian.net.example.com} and
     * {@code https://tenant.atlassian.net@example.com}.
     */
    boolean onOrigin(String url) {
        URI target;
        try {
            target = new URI(url);
        } catch (java.net.URISyntaxException e) {
            return false;
        }
        URI base = URI.create(origin);
        return target.isAbsolute() && target.getRawUserInfo() == null && target.getHost() != null
                && base.getScheme().equalsIgnoreCase(target.getScheme())
                && base.getHost().equalsIgnoreCase(target.getHost())
                && port(base) == port(target);
    }

    private static int port(URI uri) {
        if (uri.getPort() >= 0) {
            return uri.getPort();
        }
        return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
    }

    /**
     * Resolve a server-relative URL. Paths already carrying the base path
     * ({@code /wiki/api/v2/...}, as {@code _links.next} returns) resolve
     * against the origin; bare paths ({@code /download/...}) against the full
     * base URL - the same rule the mapper applies to webui links.
     */
    private String resolveRelative(String url) {
        if (url.startsWith("http")) {
            return url;
        }
        String path = url.startsWith("/") ? url : "/" + url;
        if (!basePath.isEmpty() && path.startsWith(basePath + "/")) {
            return origin + path;
        }
        return baseUrl + path;
    }

    private HttpRequest.Builder request(String url) throws IOException {
        if (!onOrigin(url)) {
            throw new IOException("refusing to send the Confluence credential to a URL outside "
                    + origin);
        }
        return HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(60))
                .header("authorization", authHeader)
                .header("accept", "application/json");
    }

    /**
     * Sends with Confluence's throttling contract: the politeness gap first,
     * then 429 retried after {@code Retry-After} seconds plus jitter.
     */
    private <T> HttpResponse<T> send(HttpRequest req, HttpResponse.BodyHandler<T> handler)
            throws IOException, InterruptedException {
        HttpResponse<T> response = null;
        for (int attempt = 0; attempt <= MAX_THROTTLE_RETRIES; attempt++) {
            throttle();
            response = http.send(req, handler);
            if (response.statusCode() != 429 || attempt == MAX_THROTTLE_RETRIES) {
                return response;
            }
            if (response.body() instanceof java.io.Closeable streamed) {
                streamed.close();
            }
            long waitSeconds = response.headers().firstValueAsLong("Retry-After").orElse(2L);
            long jitterMillis = ThreadLocalRandom.current().nextLong(500);
            Thread.sleep(Duration.ofSeconds(Math.min(waitSeconds, 60)).plusMillis(jitterMillis));
        }
        return response;
    }

    /** Keeps at least {@code minRequestInterval} between requests, sleeping on the caller. */
    private synchronized void throttle() throws InterruptedException {
        if (minRequestInterval.isZero() || minRequestInterval.isNegative()) {
            return;
        }
        long now = System.nanoTime();
        long wait = lastRequestNanos + minRequestInterval.toNanos() - now;
        if (wait > 0) {
            Thread.sleep(Duration.ofNanos(wait));
        }
        lastRequestNanos = System.nanoTime();
    }

    private static JsonNode json(HttpResponse<String> response) throws IOException {
        String body = response.body();
        if (response.statusCode() >= 400) {
            throw new ConfluenceApiException(response.statusCode(), body);
        }
        if (body == null || body.isBlank()) {
            return JSON.createObjectNode();
        }
        return JSON.readTree(body);
    }
}
