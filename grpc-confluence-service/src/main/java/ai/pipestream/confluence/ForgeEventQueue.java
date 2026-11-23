package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceEvent;
import ai.pipestream.confluence.v1.ConfluenceEventAction;
import ai.pipestream.confluence.v1.ConfluenceEventObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Timestamp;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Client for the event queue of the companion Forge app
 * ({@code grpc-confluence-forge}): takes queued Confluence product events and
 * acknowledges them once handled. Calls go out from this server to the app's
 * web trigger; nothing calls in.
 */
final class ForgeEventQueue {

    /** Environment variable for the queue web trigger URL; unset = no queue. */
    static final String ENV_URL = "CONFLUENCE_FORGE_QUEUE_URL";
    /** Environment variable for the shared secret the web trigger checks. */
    static final String ENV_SECRET = "CONFLUENCE_FORGE_QUEUE_SECRET";
    /** Header that carries the shared secret. */
    static final String SECRET_HEADER = "x-pipestream-queue-secret";
    /** Most events taken per call. */
    static final int DEFAULT_TAKE = 100;

    /** Most attempts per call when the web trigger answers busy. */
    static final int ATTEMPTS = 6;
    /** Longest wait between attempts. */
    static final Duration MAX_WAIT = Duration.ofSeconds(60);

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Waits between attempts. */
    interface Sleeper {
        /** Waits for {@code time}. */
        void sleep(Duration time) throws InterruptedException;
    }

    /** The web trigger still answered busy (HTTP 429 or 503) after every attempt. */
    static final class Busy extends IOException {
        private static final long serialVersionUID = 1L;

        Busy(String message) {
            super(message);
        }
    }

    /**
     * One queued event.
     *
     * @param key the queue key (acknowledge with it)
     * @param event the typed event
     * @param expiresAt when the queue drops the event uncollected; {@code null} if not answered
     */
    record Queued(String key, ConfluenceEvent event, Instant expiresAt) {
    }

    private final URI url;
    private final String secret;
    private final HttpClient http;
    private final Sleeper sleeper;

    ForgeEventQueue(String url, String secret, HttpClient http) {
        this(url, secret, http, time -> Thread.sleep(time.toMillis()));
    }

    ForgeEventQueue(String url, String secret, HttpClient http, Sleeper sleeper) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException(ENV_URL + " is required");
        }
        if (secret == null || secret.isBlank()) {
            throw new IllegalArgumentException(ENV_SECRET + " is required with " + ENV_URL);
        }
        this.url = URI.create(url.trim());
        if (!"https".equalsIgnoreCase(this.url.getScheme())
                && !"localhost".equalsIgnoreCase(this.url.getHost())
                && !"127.0.0.1".equals(this.url.getHost())) {
            throw new IllegalArgumentException(ENV_URL + " must be https");
        }
        this.secret = secret;
        this.http = http;
        this.sleeper = sleeper;
    }

    /** The queue from the environment, or {@code null} when none is configured. */
    static ForgeEventQueue fromEnvironment(HttpClient http) {
        String url = System.getenv(ENV_URL);
        if (url == null || url.isBlank()) {
            return null;
        }
        return new ForgeEventQueue(url, System.getenv(ENV_SECRET), http);
    }

    /** Takes up to {@code max} of the oldest queued events, without removing them. */
    List<Queued> take(int max) throws IOException, InterruptedException {
        ObjectNode body = JSON.createObjectNode().put("action", "take").put("max", max);
        JsonNode answer = post(body);
        List<Queued> out = new ArrayList<>();
        for (JsonNode item : answer.path("events")) {
            String key = item.path("key").asText("");
            if (!key.isBlank()) {
                out.add(new Queued(key, event(key, item.path("event")),
                        instant(item.path("expireTime").asText(""))));
            }
        }
        return out;
    }

    /** Removes handled events from the queue. */
    void ack(List<String> keys) throws IOException, InterruptedException {
        if (keys.isEmpty()) {
            return;
        }
        ObjectNode body = JSON.createObjectNode().put("action", "ack");
        ArrayNode array = body.putArray("keys");
        keys.forEach(array::add);
        post(body);
    }

    private JsonNode post(ObjectNode body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(url).timeout(Duration.ofSeconds(30))
                .header("content-type", "application/json")
                .header(SECRET_HEADER, secret)
                .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        for (int attempt = 1; busy(response.statusCode()) && attempt < ATTEMPTS; attempt++) {
            sleeper.sleep(wait(response, attempt));
            response = http.send(request, HttpResponse.BodyHandlers.ofString());
        }
        if (response.statusCode() / 100 != 2) {
            String text = response.body() == null ? "" : response.body().strip();
            String message = "the Forge event queue answered HTTP " + response.statusCode()
                    + (text.isEmpty() ? "" : ": " + text.substring(0, Math.min(500, text.length())));
            throw busy(response.statusCode()) ? new Busy(message) : new IOException(message);
        }
        String text = response.body();
        return text == null || text.isBlank() ? JSON.createObjectNode() : JSON.readTree(text);
    }

    private static boolean busy(int status) {
        return status == 429 || status == 503;
    }

    /** The wait before the next attempt: Retry-After seconds when given, else doubling from 2 s. */
    static Duration wait(HttpResponse<?> response, int attempt) {
        Duration wait = Duration.ofSeconds(1L << Math.min(attempt, 5));
        String after = response.headers().firstValue("retry-after").orElse("").strip();
        if (!after.isEmpty() && after.chars().allMatch(Character::isDigit) && after.length() < 6) {
            wait = Duration.ofSeconds(Long.parseLong(after));
        }
        return wait.compareTo(MAX_WAIT) > 0 ? MAX_WAIT : wait;
    }

    /** Maps one Forge product event to the typed contract. */
    static ConfluenceEvent event(String key, JsonNode event) {
        String type = event.path("eventType").asText("");
        String[] parts = type.split(":", -1);
        String action = parts.length > 2 ? parts[2] : "";
        String object = parts.length > 3 ? parts[3] : "";
        JsonNode subject = subject(event);
        ConfluenceEvent.Builder out = ConfluenceEvent.newBuilder()
                .setEventId(key)
                .setEventType(type)
                .setAction(action(action))
                .setObject(object(object))
                .setContentId(text(subject.path("id")))
                .setContentType(subject.path("type").asText(""))
                .setTitle(subject.path("title").asText(subject.path("name").asText("")))
                .setVersionNumber(subject.path("version").path("number").asInt(0))
                .setActorAccountId(event.path("atlassianId").asText(""))
                .setSelfGenerated(event.path("selfGenerated").asBoolean(false))
                .setContainerId(text(subject.path("container").path("id")))
                .setContainerType(subject.path("container").path("type").asText(""))
                .setLabelName(event.path("label").path("name").asText(""))
                .setOriginContentId(text(event.path("originContentId")))
                .setPreviousParentId(text(event.path("prevContent").path("parentId")));
        JsonNode space = subject.has("space") ? subject.path("space") : event.path("space");
        if (out.getObject() == ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_SPACE) {
            space = subject;
        }
        out.setSpaceId(text(space.path("id"))).setSpaceKey(space.path("key").asText(""));
        Timestamp at = timestamp(event.path("eventCreatedDate").asText(""));
        if (at != null) {
            out.setOccurredAt(at);
        }
        return out.build();
    }

    /** The object an event is about: its content, attachment, comment or space node. */
    private static JsonNode subject(JsonNode event) {
        for (String field : List.of("content", "attachment", "comment", "space", "customContent",
                "user", "group", "task")) {
            if (event.path(field).isObject()) {
                return event.path(field);
            }
        }
        return JSON.createObjectNode();
    }

    private static String text(JsonNode node) {
        return node.isMissingNode() || node.isNull() ? "" : node.asText("");
    }

    static ConfluenceEventAction action(String name) {
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "created" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED;
            case "updated" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_UPDATED;
            case "trashed" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_TRASHED;
            case "restored" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_RESTORED;
            case "deleted" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_DELETED;
            case "moved" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_MOVED;
            case "copied" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_COPIED;
            case "permissions_updated" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_PERMISSIONS_UPDATED;
            case "added" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_ADDED;
            case "removed" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_REMOVED;
            case "archived" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_ARCHIVED;
            case "unarchived" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_UNARCHIVED;
            case "published" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_PUBLISHED;
            case "children_reordered" -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CHILDREN_REORDERED;
            default -> ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_UNSPECIFIED;
        };
    }

    static ConfluenceEventObject object(String name) {
        return switch (name.toLowerCase(Locale.ROOT)) {
            case "page" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_PAGE;
            case "blogpost" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_BLOG_POST;
            case "attachment" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_ATTACHMENT;
            case "comment" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_COMMENT;
            case "label" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_LABEL;
            case "space" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_SPACE;
            case "custom_content" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_CUSTOM_CONTENT;
            case "whiteboard" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_WHITEBOARD;
            case "database" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_DATABASE;
            case "folder" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_FOLDER;
            case "embed" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_EMBED;
            case "task" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_TASK;
            case "user" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_USER;
            case "group" -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_GROUP;
            default -> ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_UNSPECIFIED;
        };
    }

    private static Timestamp timestamp(String text) {
        Instant at = instant(text);
        return at == null ? null
                : Timestamp.newBuilder().setSeconds(at.getEpochSecond()).setNanos(at.getNano()).build();
    }

    private static Instant instant(String text) {
        if (text.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(text);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
