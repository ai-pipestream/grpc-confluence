package ai.pipestream.confluence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URI;
import java.net.URLDecoder;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The JSON form of one recorded HTTP exchange: how a request and a response
 * are described in a recording file, and how the file is named.
 */
final class RecordedExchange {

    /** The mapper recordings are read and written with; numbers keep their exact value. */
    static final ObjectMapper JSON = ConfluenceClient.responseJson();
    /** The request headers a recording keeps and a replay compares. */
    static final List<String> REQUEST_HEADERS = List.of("accept", "content-type", "x-atlassian-token");
    /** The response headers a recording keeps and a replay serves. */
    static final List<String> RESPONSE_HEADERS = List.of("content-type", "retry-after", "location");

    /**
     * A described request.
     *
     * @param request the recording form of the request
     * @param payloads the bytes of each uploaded file part, by SHA-256
     */
    record Described(ObjectNode request, Map<String, byte[]> payloads) {
    }

    private RecordedExchange() {
    }

    /**
     * Describes a request: method, host, decoded path, query pairs sorted by
     * name, the kept headers, whether it carried an Authorization header,
     * and the body (JSON, text, or the parts of a multipart form with each
     * file part as SHA-256 and length).
     */
    static Described describe(HttpRequest request, byte[] body) {
        ObjectNode node = JSON.createObjectNode();
        Map<String, byte[]> payloads = new HashMap<>();
        URI uri = request.uri();
        node.put("method", request.method());
        node.put("host", uri.getPort() < 0 ? uri.getHost() : uri.getHost() + ":" + uri.getPort());
        node.put("path", uri.getPath());
        node.set("query", query(uri.getRawQuery()));
        String contentType = request.headers().firstValue("content-type").orElse("");
        boolean multipart = contentType.toLowerCase(Locale.ROOT).startsWith("multipart/");
        ObjectNode headers = node.putObject("headers");
        for (String name : REQUEST_HEADERS) {
            request.headers().firstValue(name).ifPresent(value -> headers.put(name,
                    multipart && name.equals("content-type") ? mediaType(value) : value));
        }
        node.put("authorized", request.headers().firstValue("authorization").isPresent());
        if (body.length > 0) {
            ObjectNode described = node.putObject("body");
            if (multipart) {
                described.set("multipart", parts(body, boundary(contentType), payloads));
            } else {
                text(described, body, contentType);
            }
        }
        return new Described(node, payloads);
    }

    /**
     * Describes a response: status, the kept headers and the body. A body
     * that is neither JSON nor text is SHA-256 and length, plus its
     * {@code Content-Length}.
     */
    static ObjectNode describe(int status, HttpHeaders responseHeaders, byte[] body) {
        ObjectNode node = JSON.createObjectNode();
        node.put("status", status);
        ObjectNode headers = node.putObject("headers");
        for (String name : RESPONSE_HEADERS) {
            responseHeaders.firstValue(name).ifPresent(value -> headers.put(name, value));
        }
        if (body.length > 0) {
            ObjectNode described = node.putObject("body");
            String contentType = responseHeaders.firstValue("content-type").orElse("");
            if (!text(described, body, contentType)) {
                described.put("sha256", sha256(body));
                described.put("length", body.length);
                headers.put("content-length", String.valueOf(body.length));
            }
        }
        return node;
    }

    /** Sets {@code json} or {@code text} when the body is JSON or text; false for a binary body. */
    private static boolean text(ObjectNode described, byte[] body, String contentType) {
        String type = mediaType(contentType);
        boolean json = type.endsWith("json");
        if (!json && !type.isEmpty() && !type.startsWith("text/") && !type.endsWith("xml")
                && !type.equals("application/x-www-form-urlencoded")) {
            return false;
        }
        String decoded;
        try {
            decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(body)).toString();
        } catch (CharacterCodingException e) {
            return false;
        }
        if (json) {
            try {
                described.set("json", JSON.readTree(decoded));
                return true;
            } catch (JsonProcessingException e) {
                // Not JSON after all: kept as text.
            }
        }
        described.put("text", decoded);
        return true;
    }

    /** The query as name/value pairs, decoded and sorted by name, then value. */
    private static ArrayNode query(String rawQuery) {
        List<String[]> pairs = new ArrayList<>();
        if (rawQuery != null && !rawQuery.isEmpty()) {
            for (String pair : rawQuery.split("&")) {
                int eq = pair.indexOf('=');
                pairs.add(new String[] {
                        URLDecoder.decode(eq < 0 ? pair : pair.substring(0, eq), StandardCharsets.UTF_8),
                        URLDecoder.decode(eq < 0 ? "" : pair.substring(eq + 1), StandardCharsets.UTF_8)});
            }
        }
        pairs.sort(Comparator.<String[], String>comparing(pair -> pair[0])
                .thenComparing(pair -> pair[1]));
        ArrayNode query = JSON.createArrayNode();
        pairs.forEach(pair -> query.addObject().put("name", pair[0]).put("value", pair[1]));
        return query;
    }

    /** The media type of a {@code Content-Type} value, lower-case and without parameters. */
    private static String mediaType(String contentType) {
        int semicolon = contentType.indexOf(';');
        return (semicolon < 0 ? contentType : contentType.substring(0, semicolon)).trim()
                .toLowerCase(Locale.ROOT);
    }

    private static String boundary(String contentType) {
        int at = contentType.indexOf("boundary=");
        String boundary = at < 0 ? "" : contentType.substring(at + "boundary=".length()).trim();
        return boundary.startsWith("\"") && boundary.endsWith("\"") && boundary.length() > 1
                ? boundary.substring(1, boundary.length() - 1) : boundary;
    }

    /** The parts of a multipart body: a text field with its value, a file part as SHA-256 and length. */
    private static ArrayNode parts(byte[] body, String boundary, Map<String, byte[]> payloads) {
        ArrayNode parts = JSON.createArrayNode();
        byte[] delimiter = ("--" + boundary).getBytes(StandardCharsets.UTF_8);
        byte[] blankLine = "\r\n\r\n".getBytes(StandardCharsets.UTF_8);
        int at = indexOf(body, delimiter, 0);
        while (at >= 0) {
            int start = at + delimiter.length + 2;
            int next = indexOf(body, delimiter, start);
            if (next < 0) {
                break;
            }
            int headerEnd = indexOf(body, blankLine, start);
            if (headerEnd < 0 || headerEnd > next) {
                break;
            }
            String headers = new String(body, start, headerEnd - start, StandardCharsets.UTF_8);
            byte[] content = Arrays.copyOfRange(body, headerEnd + blankLine.length, next - 2);
            ObjectNode part = parts.addObject();
            part.put("name", parameter(headers, "name"));
            String fileName = parameter(headers, "filename");
            if (fileName == null) {
                part.put("value", new String(content, StandardCharsets.UTF_8));
            } else {
                String digest = sha256(content);
                part.put("filename", fileName);
                for (String line : headers.split("\r\n")) {
                    if (line.toLowerCase(Locale.ROOT).startsWith("content-type:")) {
                        part.put("contentType", line.substring("content-type:".length()).trim());
                    }
                }
                part.put("sha256", digest);
                part.put("length", content.length);
                payloads.put(digest, content);
            }
            at = next;
        }
        return parts;
    }

    /** The quoted value of a {@code Content-Disposition} parameter, or null. */
    private static String parameter(String headers, String name) {
        String marker = "; " + name + "=\"";
        int at = headers.indexOf(marker);
        if (at < 0) {
            return null;
        }
        int end = headers.indexOf('"', at + marker.length());
        return end < 0 ? null : headers.substring(at + marker.length(), end);
    }

    private static int indexOf(byte[] in, byte[] what, int from) {
        outer:
        for (int i = Math.max(from, 0); i <= in.length - what.length; i++) {
            for (int j = 0; j < what.length; j++) {
                if (in[i + j] != what[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The file name of an exchange: {@code NNN-METHOD-short-route.json}. */
    static String fileName(int sequence, JsonNode request) {
        StringBuilder route = new StringBuilder();
        for (String segment : request.path("path").asText().split("/")) {
            if (segment.isEmpty() || segment.equals("wiki") || route.length() > 60) {
                continue;
            }
            boolean version = segment.equals("v1") || segment.equals("v2");
            boolean id = !version && segment.chars().anyMatch(c -> c >= '0' && c <= '9');
            boolean plain = segment.chars().allMatch(c -> c == '-' || c == '_'
                    || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9');
            route.append(route.isEmpty() ? "" : "-").append(id || !plain ? "id" : segment);
        }
        return String.format(Locale.ROOT, "%03d-%s-%s.json", sequence,
                request.path("method").asText(), route.isEmpty() ? "root" : route);
    }
}
