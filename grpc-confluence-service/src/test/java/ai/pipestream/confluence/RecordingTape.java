package ai.pipestream.confluence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;
import java.util.stream.Stream;

/**
 * Sends every request with a real HTTP client and saves each exchange,
 * scrubbed, as one file under {@code <root>/<scenario>/}. {@link #finish}
 * ends the recording and writes {@code run.json}.
 */
final class RecordingTape extends ExchangeTape {

    /** The file of a recording that describes its run. */
    static final String RUN_FILE = "run.json";

    private final HttpClient delegate;
    private final Path root;
    private final RecordingScrubber scrubber;
    private final List<Path> written = new ArrayList<>();
    private final List<Pending> open = new ArrayList<>();
    private final List<String> scenarios = new ArrayList<>();
    /** SHA-256 of every file part uploaded so far. */
    private final Set<String> uploaded = new HashSet<>();
    private String scenario;
    private int sequence;
    private int groups;
    private boolean unordered;

    /** Starts a recording under {@code root}, removing the recording that was there. */
    RecordingTape(HttpClient delegate, Path root, RecordingScrubber scrubber) throws IOException {
        this.delegate = delegate;
        this.root = root;
        this.scrubber = scrubber;
        clear();
        Files.createDirectories(root);
    }

    @Override
    synchronized void scenario(String name) {
        scenario = name;
        sequence = 0;
        scenarios.add(name);
    }

    @Override
    synchronized void unordered(boolean on) {
        if (on && !unordered) {
            groups++;
        }
        unordered = on;
    }

    @Override
    void pause(Duration time) throws InterruptedException {
        Thread.sleep(time);
    }

    @Override
    public <T> HttpResponse<T> send(HttpRequest request, HttpResponse.BodyHandler<T> handler)
            throws IOException, InterruptedException {
        Pending pending;
        synchronized (this) {
            if (scenario == null) {
                throw new IllegalStateException("no scenario was started");
            }
            pending = new Pending(request, scenario, ++sequence, unordered ? groups : 0);
            open.add(pending);
        }
        HttpRequest outgoing = request.bodyPublisher()
                .map(body -> HttpRequest.newBuilder(request, (name, value) -> true)
                        .method(request.method(), new TeePublisher(body, pending)).build())
                .orElse(request);
        return delegate.send(outgoing, info -> {
            pending.status = info.statusCode();
            pending.headers = info.headers();
            return new TeeSubscriber<>(handler.apply(info), pending);
        });
    }

    /**
     * Ends the recording: saves the exchanges whose body was not read to
     * its end, scrubs every file once more and writes {@code run.json}
     * from {@code run} plus the scenario names. If a file still holds
     * something the scrubber must remove, the whole recording is deleted.
     *
     * @throws AssertionError if the recording was deleted
     */
    synchronized void finish(ObjectNode run) throws IOException {
        for (Pending pending : List.copyOf(open)) {
            pending.save(true);
        }
        ObjectNode described = run.deepCopy();
        scenarios.forEach(described.putArray("scenarios")::add);
        Path runFile = root.resolve(RUN_FILE);
        write(runFile, scrubber.scrub(described));
        written.add(runFile);
        List<String> leaking = new ArrayList<>();
        for (Path file : written) {
            // A value that got its placeholder late is replaced in the earlier files here.
            JsonNode scrubbed = scrubber.scrub(RecordedExchange.JSON.readTree(file.toFile()));
            write(file, scrubbed);
            List<String> leaks = RecordingScrubber.leaks(Files.readString(file, StandardCharsets.UTF_8));
            if (!leaks.isEmpty()) {
                leaking.add(root.relativize(file) + " " + leaks);
            }
        }
        if (!leaking.isEmpty()) {
            clear();
            throw new AssertionError("the recording was deleted, the scrubber left: " + leaking);
        }
    }

    private void clear() throws IOException {
        if (!Files.isDirectory(root)) {
            return;
        }
        try (Stream<Path> files = Files.walk(root)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                if (!file.equals(root)) {
                    Files.delete(file);
                }
            }
        }
    }

    private static void write(Path file, JsonNode content) throws IOException {
        Files.createDirectories(file.getParent());
        Files.writeString(file, RecordedExchange.JSON.writerWithDefaultPrettyPrinter()
                .writeValueAsString(content) + "\n", StandardCharsets.UTF_8);
    }

    /** One exchange between its request and the end of its response body. */
    private final class Pending {
        private final HttpRequest request;
        private final String scenario;
        private final int sequence;
        private final int group;
        private final ByteArrayOutputStream requestBody = new ByteArrayOutputStream();
        private final ByteArrayOutputStream responseBody = new ByteArrayOutputStream();
        private volatile int status;
        private volatile HttpHeaders headers;

        Pending(HttpRequest request, String scenario, int sequence, int group) {
            this.request = request;
            this.scenario = scenario;
            this.sequence = sequence;
            this.group = group;
        }

        /** Writes the exchange file, once; an exchange without a response is dropped. */
        void save(boolean truncated) {
            synchronized (RecordingTape.this) {
                if (!open.remove(this) || headers == null) {
                    return;
                }
                RecordedExchange.Described described;
                byte[] body;
                synchronized (this) {
                    described = RecordedExchange.describe(request, requestBody.toByteArray());
                    body = responseBody.toByteArray();
                }
                uploaded.addAll(described.payloads().keySet());
                ObjectNode exchange = RecordedExchange.JSON.createObjectNode();
                exchange.put("scenario", scenario);
                exchange.put("sequence", sequence);
                if (group > 0) {
                    exchange.put("unordered", group);
                }
                exchange.set("request", described.request());
                ObjectNode response = RecordedExchange.describe(status, headers, body);
                if (truncated) {
                    response.put("truncated", true);
                }
                exchange.set("response", response);
                JsonNode scrubbed = scrubber.scrub(exchange);
                Path file = root.resolve(scenario)
                        .resolve(RecordedExchange.fileName(sequence, scrubbed.path("request")));
                try {
                    String digest = response.path("body").path("sha256").asText("");
                    if (!digest.isEmpty() && !uploaded.contains(digest)) {
                        // Bytes the run did not upload itself are kept next to the exchange.
                        String name = file.getFileName().toString().replace(".json", ".body.bin");
                        Files.createDirectories(file.getParent());
                        Files.write(file.resolveSibling(name), body);
                        ((ObjectNode) scrubbed.path("response").path("body")).put("file", name);
                    }
                    write(file, scrubbed);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                written.add(file);
            }
        }

        synchronized void request(ByteBuffer buffer) {
            copy(buffer, requestBody);
        }

        synchronized void response(List<ByteBuffer> buffers) {
            buffers.forEach(buffer -> copy(buffer, responseBody));
        }

        private static void copy(ByteBuffer buffer, ByteArrayOutputStream into) {
            ByteBuffer view = buffer.duplicate();
            byte[] bytes = new byte[view.remaining()];
            view.get(bytes);
            into.writeBytes(bytes);
        }
    }

    /** Publishes a request body unchanged and copies it into the exchange. */
    private static final class TeePublisher implements HttpRequest.BodyPublisher {
        private final HttpRequest.BodyPublisher body;
        private final Pending pending;

        TeePublisher(HttpRequest.BodyPublisher body, Pending pending) {
            this.body = body;
            this.pending = pending;
        }

        @Override
        public long contentLength() {
            return body.contentLength();
        }

        @Override
        public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
            body.subscribe(new Flow.Subscriber<ByteBuffer>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    subscriber.onSubscribe(subscription);
                }

                @Override
                public void onNext(ByteBuffer item) {
                    pending.request(item);
                    subscriber.onNext(item);
                }

                @Override
                public void onError(Throwable throwable) {
                    subscriber.onError(throwable);
                }

                @Override
                public void onComplete() {
                    subscriber.onComplete();
                }
            });
        }
    }

    /** Hands a response body on unchanged, copies it, and saves the exchange at its end. */
    private static final class TeeSubscriber<T> implements HttpResponse.BodySubscriber<T> {
        private final HttpResponse.BodySubscriber<T> body;
        private final Pending pending;

        TeeSubscriber(HttpResponse.BodySubscriber<T> body, Pending pending) {
            this.body = body;
            this.pending = pending;
        }

        @Override
        public CompletionStage<T> getBody() {
            return body.getBody();
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            body.onSubscribe(subscription);
        }

        @Override
        public void onNext(List<ByteBuffer> item) {
            pending.response(item);
            body.onNext(item);
        }

        @Override
        public void onError(Throwable throwable) {
            pending.save(true);
            body.onError(throwable);
        }

        @Override
        public void onComplete() {
            pending.save(false);
            body.onComplete();
        }
    }
}
