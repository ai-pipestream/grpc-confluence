package ai.pipestream.confluence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.stream.Stream;

import javax.net.ssl.SSLSession;

/**
 * Answers every request from a recording, without a network. A request must
 * equal the next recorded request of the scenario (method, host, path,
 * query, kept headers, whether it is authorized, body); within a group
 * recorded as unordered any request of the group may come next. Anything
 * else is a violation and fails the request.
 */
final class ReplayTape extends ExchangeTape {

    private final Path root;
    private final List<String> violations = new ArrayList<>();
    /** Uploaded file parts by SHA-256; a recorded download of the same bytes is served from here. */
    private final Map<String, byte[]> payloads = new HashMap<>();
    private List<Recorded> exchanges = List.of();
    private String scenario = "";

    /** One recorded exchange and whether it was served. */
    private static final class Recorded {
        private final Path file;
        private final JsonNode exchange;
        private boolean served;

        Recorded(Path file, JsonNode exchange) {
            this.file = file;
            this.exchange = exchange;
        }
    }

    ReplayTape(Path root) {
        this.root = root;
    }

    /** Whether a recording exists under {@code root}. */
    static boolean recorded(Path root) {
        return Files.isRegularFile(root.resolve(RecordingTape.RUN_FILE));
    }

    /** The recording's {@code run.json}. */
    JsonNode run() throws IOException {
        return RecordedExchange.JSON.readTree(root.resolve(RecordingTape.RUN_FILE).toFile());
    }

    /** Whether the recording holds the named scenario. */
    boolean has(String name) {
        return Files.isDirectory(root.resolve(name));
    }

    /** Requests that matched no recorded exchange, and recorded exchanges that were never requested. */
    synchronized List<String> violations() {
        return List.copyOf(violations);
    }

    @Override
    synchronized void scenario(String name) {
        finish();
        scenario = name;
        List<Recorded> loaded = new ArrayList<>();
        Path directory = root.resolve(name);
        if (Files.isDirectory(directory)) {
            try (Stream<Path> files = Files.list(directory)) {
                for (Path file : files.filter(f -> f.getFileName().toString().endsWith(".json"))
                        .sorted().toList()) {
                    loaded.add(new Recorded(file, RecordedExchange.JSON.readTree(file.toFile())));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        exchanges = loaded;
    }

    /** Ends the current scenario: every exchange left unserved is a violation. */
    synchronized void finish() {
        exchanges.stream().filter(recorded -> !recorded.served).forEach(recorded -> violations.add(
                scenario + ": recorded but never requested: " + recorded.file.getFileName()));
        exchanges = List.of();
    }

    @Override
    void unordered(boolean on) {
        // The groups come from the recording.
    }

    @Override
    void pause(Duration time) {
        // The recording has already waited.
    }

    @Override
    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler)
            throws IOException, InterruptedException {
        RecordedExchange.Described actual = RecordedExchange.describe(request, drain(request));
        JsonNode response;
        Path file;
        synchronized (this) {
            payloads.putAll(actual.payloads());
            Recorded match = match(actual.request());
            match.served = true;
            response = match.exchange.path("response");
            file = match.file;
        }
        int status = response.path("status").asInt();
        Map<String, List<String>> headerValues = new HashMap<>();
        response.path("headers").properties().forEach(header -> headerValues.put(header.getKey(),
                // A recorded wait is not waited again.
                List.of(header.getKey().equals("retry-after") ? "0" : header.getValue().asText())));
        HttpHeaders headers = HttpHeaders.of(headerValues, (name, value) -> true);
        byte[] body = body(response.path("body"), file);
        HttpResponse.BodySubscriber<T> subscriber = handler.apply(new HttpResponse.ResponseInfo() {
            @Override
            public int statusCode() {
                return status;
            }

            @Override
            public HttpHeaders headers() {
                return headers;
            }

            @Override
            public HttpClient.Version version() {
                return HttpClient.Version.HTTP_1_1;
            }
        });
        subscriber.onSubscribe(new Flow.Subscription() {
            private boolean delivered;

            @Override
            public void request(long n) {
                if (!delivered) {
                    delivered = true;
                    if (body.length > 0) {
                        subscriber.onNext(List.of(ByteBuffer.wrap(body)));
                    }
                    subscriber.onComplete();
                }
            }

            @Override
            public void cancel() {
                delivered = true;
            }
        });
        T served;
        try {
            served = subscriber.getBody().toCompletableFuture().get();
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
        return new Served<>(request, status, headers, served);
    }

    /** The next recorded exchange if {@code actual} equals its request; a violation otherwise. */
    private Recorded match(ObjectNode actual) throws IOException {
        Recorded next = exchanges.stream().filter(recorded -> !recorded.served).findFirst()
                .orElse(null);
        String sent = actual.path("method").asText() + " " + actual.path("host").asText()
                + actual.path("path").asText();
        if (next == null) {
            throw violation(scenario + ": " + sent + " was sent after the last recorded exchange");
        }
        int group = next.exchange.path("unordered").asInt(0);
        for (Recorded candidate : exchanges) {
            boolean eligible = candidate == next
                    || group > 0 && candidate.exchange.path("unordered").asInt(0) == group;
            if (eligible && !candidate.served && candidate.exchange.path("request").equals(actual)) {
                return candidate;
            }
        }
        JsonNode expected = next.exchange.path("request");
        List<String> differing = new ArrayList<>();
        for (String field : List.of("method", "host", "path", "query", "headers", "authorized", "body")) {
            if (!expected.path(field).equals(actual.path(field))) {
                differing.add(field + ": recorded " + brief(expected.path(field)) + ", sent "
                        + brief(actual.path(field)));
            }
        }
        throw violation(scenario + ": " + sent + " does not match " + next.file.getFileName()
                + (group > 0 ? " or another exchange of its unordered group" : "") + "; " + differing);
    }

    private IOException violation(String message) {
        violations.add(message);
        return new IOException("replay: " + message);
    }

    private static String brief(JsonNode node) {
        String text = node.isMissingNode() ? "nothing" : node.toString();
        return text.length() > 400 ? text.substring(0, 400) + "..." : text;
    }

    /** The response body bytes: the recorded JSON or text, or the bytes a SHA-256 names. */
    private byte[] body(JsonNode body, Path file) throws IOException {
        if (body.has("json")) {
            return RecordedExchange.JSON.writeValueAsBytes(body.get("json"));
        }
        if (body.has("text")) {
            return body.get("text").asText().getBytes(StandardCharsets.UTF_8);
        }
        if (body.has("file")) {
            return Files.readAllBytes(file.resolveSibling(body.get("file").asText()));
        }
        if (body.has("sha256")) {
            byte[] bytes;
            synchronized (this) {
                bytes = payloads.get(body.get("sha256").asText());
            }
            if (bytes == null) {
                throw violation(scenario + ": " + file.getFileName()
                        + " answers with bytes that were never uploaded in this replay");
            }
            return bytes;
        }
        return new byte[0];
    }

    /** Reads a request body to its end. */
    private static byte[] drain(HttpRequest request) throws IOException, InterruptedException {
        Optional<HttpRequest.BodyPublisher> publisher = request.bodyPublisher();
        if (publisher.isEmpty()) {
            return new byte[0];
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        CompletableFuture<Void> done = new CompletableFuture<>();
        publisher.get().subscribe(new Flow.Subscriber<ByteBuffer>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(ByteBuffer item) {
                byte[] chunk = new byte[item.remaining()];
                item.get(chunk);
                synchronized (bytes) {
                    bytes.writeBytes(chunk);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                done.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                done.complete(null);
            }
        });
        try {
            done.get();
        } catch (ExecutionException e) {
            throw new IOException(e.getCause());
        }
        synchronized (bytes) {
            return bytes.toByteArray();
        }
    }

    /** A response served from a recording. */
    private record Served<T>(HttpRequest request, int statusCode, HttpHeaders headers, T body)
            implements HttpResponse<T> {

        @Override
        public Optional<HttpResponse<T>> previousResponse() {
            return Optional.empty();
        }

        @Override
        public Optional<SSLSession> sslSession() {
            return Optional.empty();
        }

        @Override
        public URI uri() {
            return request.uri();
        }

        @Override
        public HttpClient.Version version() {
            return HttpClient.Version.HTTP_1_1;
        }
    }
}
