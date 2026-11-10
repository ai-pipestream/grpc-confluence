package ai.pipestream.confluence;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An in-process fake of the Confluence Cloud REST API over
 * {@link HttpServer}, same idiom as the schema-registry stub tests: stubs are
 * keyed by request path (optionally {@code path?query} or {@code METHOD path}), requests are
 * recorded for assertions, and one-shot stubs model a 429-then-success
 * exchange. No live Atlassian calls anywhere.
 */
final class FakeConfluenceServer implements AutoCloseable {

    /** One canned response. */
    record Stub(int status, String body, Map<String, String> headers) {
        static Stub json(String body) {
            return new Stub(200, body, Map.of());
        }
    }

    /** One request the fake received. */
    record RecordedRequest(String method, String path, String query, String authorization,
            String body, Map<String, String> headers) {

        RecordedRequest(String method, String path, String query, String authorization) {
            this(method, path, query, authorization, "", Map.of());
        }

        RecordedRequest(String method, String path, String query, String authorization,
                String body) {
            this(method, path, query, authorization, body, Map.of());
        }

        /** The first value of a request header, by lower-case name; empty when absent. */
        String header(String name) {
            return headers.getOrDefault(name.toLowerCase(java.util.Locale.ROOT), "");
        }

        /** The query decoded into name/value pairs, in request order. */
        Map<String, String> params() {
            Map<String, String> params = new java.util.LinkedHashMap<>();
            if (query.isEmpty()) {
                return params;
            }
            for (String pair : query.split("&")) {
                int eq = pair.indexOf('=');
                String name = eq < 0 ? pair : pair.substring(0, eq);
                String value = eq < 0 ? "" : pair.substring(eq + 1);
                params.put(java.net.URLDecoder.decode(name, StandardCharsets.UTF_8),
                        java.net.URLDecoder.decode(value, StandardCharsets.UTF_8));
            }
            return params;
        }
    }

    private final HttpServer server;
    private final Map<String, Stub> stubs = new ConcurrentHashMap<>();
    private final Map<String, Queue<Stub>> onceStubs = new ConcurrentHashMap<>();
    private final List<RecordedRequest> requests = new CopyOnWriteArrayList<>();

    private FakeConfluenceServer(HttpServer server) {
        this.server = server;
    }

    static FakeConfluenceServer start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        FakeConfluenceServer fake = new FakeConfluenceServer(server);
        server.createContext("/", exchange -> {
            try {
                fake.handle(exchange);
            } finally {
                exchange.close();
            }
        });
        server.start();
        return fake;
    }

    /** The base URL the crawler config takes, with the Cloud {@code /wiki} suffix. */
    String baseUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + "/wiki";
    }

    /** Stubs a JSON 200 for a path (or {@code path?query} when the query matters). */
    void stub(String pathOrPathQuery, String json) {
        stubs.put(pathOrPathQuery, Stub.json(json));
    }

    void stub(String pathOrPathQuery, Stub stub) {
        stubs.put(pathOrPathQuery, stub);
    }

    /** Stubs a response for one HTTP method on a path; it wins over a method-less stub. */
    void stub(String method, String path, Stub stub) {
        stubs.put(method + " " + path, stub);
    }

    /** Stubs a JSON response with the given status for one HTTP method on a path. */
    void stub(String method, String path, int status, String json) {
        stub(method, path, new Stub(status, json, Map.of()));
    }

    /** The last request with {@code method} to {@code path}; fails when there was none. */
    RecordedRequest lastTo(String method, String path) {
        List<RecordedRequest> matching = requests.stream()
                .filter(r -> r.method().equals(method) && r.path().equals(path)).toList();
        if (matching.isEmpty()) {
            throw new AssertionError("no " + method + " to " + path + "; saw " + requests);
        }
        return matching.getLast();
    }

    /** Forgets the recorded requests. */
    void clearRequests() {
        requests.clear();
    }

    /** A response used exactly once for the path, before the permanent stub applies. */
    void stubOnce(String path, Stub stub) {
        onceStubs.computeIfAbsent(path, k -> new ConcurrentLinkedQueue<>()).add(stub);
    }

    List<RecordedRequest> requests() {
        return List.copyOf(requests);
    }

    /** The last request to {@code path}; fails when there was none. */
    RecordedRequest lastTo(String path) {
        List<RecordedRequest> matching = requestsTo(path);
        if (matching.isEmpty()) {
            throw new AssertionError("no request to " + path + "; saw " + requests);
        }
        return matching.getLast();
    }

    List<RecordedRequest> requestsTo(String path) {
        return requests.stream().filter(r -> r.path().equals(path)).toList();
    }

    private void handle(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String query = exchange.getRequestURI().getRawQuery();
        String requestBody;
        try (java.io.InputStream in = exchange.getRequestBody()) {
            requestBody = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        Map<String, String> headers = new java.util.HashMap<>();
        exchange.getRequestHeaders().forEach((name, values) ->
                headers.put(name.toLowerCase(java.util.Locale.ROOT), values.getFirst()));
        requests.add(new RecordedRequest(exchange.getRequestMethod(), path,
                query == null ? "" : query,
                exchange.getRequestHeaders().getFirst("authorization"), requestBody, headers));

        Stub stub = null;
        Queue<Stub> once = onceStubs.get(path);
        if (once != null) {
            stub = once.poll();
        }
        if (stub == null) {
            stub = stubs.get(exchange.getRequestMethod() + " " + path);
        }
        if (stub == null && query != null) {
            stub = stubs.get(path + "?" + query);
        }
        if (stub == null) {
            stub = stubs.get(path);
        }
        if (stub == null) {
            stub = new Stub(404, "{\"message\":\"not stubbed: " + path + "\"}", Map.of());
        }

        byte[] body = stub.body().getBytes(StandardCharsets.UTF_8);
        stub.headers().forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
        if (stub.status() == 200) {
            exchange.getResponseHeaders().add("content-type", "application/json");
        }
        if (stub.status() == 204) {
            exchange.sendResponseHeaders(204, -1);
            return;
        }
        exchange.sendResponseHeaders(stub.status(), body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
