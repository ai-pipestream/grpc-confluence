package ai.pipestream.connect;

import ai.pipestream.confluence.ChangeSink;
import ai.pipestream.confluence.ConfluenceClient;
import ai.pipestream.confluence.ConfluenceConnectorConfig;
import ai.pipestream.confluence.ConfluenceCrawler;
import ai.pipestream.confluence.InMemoryChangeSink;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * One poll is one Sync pass (gRPC) or one crawler pass (direct). The
 * resume cursor is stored as the Connect offset.
 */
public final class ConfluenceSourceTask extends SourceTask {

    static final Map<String, String> PARTITION = Map.of("source", "confluence");

    /** Kafka Connect task that pulls ConfluenceChange protobuf bytes. */
    public ConfluenceSourceTask() {
    }

    private String topic;
    private String grpcTarget;
    private boolean plaintext;
    private boolean includeBodies;
    private List<String> spaceKeys = List.of();
    private ConfluenceConnectorConfig direct;
    private String lastCursor;

    @Override
    public String version() {
        return ConnectVersion.VALUE;
    }

    @Override
    public void start(Map<String, String> props) {
        topic = required(props, ConfluenceSourceConnector.TOPIC);
        grpcTarget = props.getOrDefault(ConfluenceSourceConnector.GRPC_TARGET, "").trim();
        plaintext = Boolean.parseBoolean(
                props.getOrDefault(ConfluenceSourceConnector.GRPC_PLAINTEXT, "true"));
        includeBodies = Boolean.parseBoolean(
                props.getOrDefault(ConfluenceSourceConnector.INCLUDE_BODIES, "false"));
        spaceKeys = splitCsv(props.getOrDefault(ConfluenceSourceConnector.SPACES, ""));
        if (grpcTarget.isEmpty()) {
            List<String> spaces = spaceKeys;
            direct = ConfluenceConnectorConfig.builder()
                    .baseUrl(required(props, ConfluenceSourceConnector.BASE_URL))
                    .email(required(props, ConfluenceSourceConnector.EMAIL))
                    .apiToken(required(props, ConfluenceSourceConnector.API_TOKEN))
                    .spaces(spaces)
                    .build();
        }
        Map<String, Object> offset = context.offsetStorageReader().offset(PARTITION);
        if (offset != null && offset.get("cursor") instanceof String cursor) {
            lastCursor = cursor;
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        try {
            List<ConfluenceChange> changes = new ArrayList<>();
            String cursor;
            if (!grpcTarget.isEmpty()) {
                cursor = pollGrpc(changes);
            } else {
                cursor = pollDirect(changes);
            }
            lastCursor = cursor;
            Map<String, String> offset = Map.of("cursor", cursor);
            List<SourceRecord> records = new ArrayList<>(changes.size());
            for (ConfluenceChange change : changes) {
                records.add(new SourceRecord(PARTITION, offset, topic,
                        Schema.STRING_SCHEMA, change.getEntity().getEntityId(),
                        Schema.BYTES_SCHEMA, change.toByteArray()));
            }
            return records;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("confluence source poll failed", e);
        }
    }

    private String pollGrpc(List<ConfluenceChange> out) {
        ManagedChannel channel = channel();
        try {
            ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub =
                    ConfluenceServiceGrpc.newBlockingStub(channel);
            SyncRequest.Builder request = SyncRequest.newBuilder()
                    .setIncludeBodies(includeBodies);
            if (lastCursor != null && !lastCursor.isBlank()) {
                request.setSinceCursor(lastCursor);
            }
            if (!spaceKeys.isEmpty()) {
                request.addAllSpaceKeys(spaceKeys);
            }
            Iterator<SyncResponse> events = stub.sync(request.build());
            String cursor = lastCursor == null ? Instant.now().toString() : lastCursor;
            while (events.hasNext()) {
                SyncResponse event = events.next();
                if (event.hasChange()) {
                    out.add(event.getChange());
                } else if (event.getEventCase() == SyncResponse.EventCase.RESUME_CURSOR) {
                    cursor = event.getResumeCursor();
                }
            }
            return cursor;
        } finally {
            channel.shutdownNow();
        }
    }

    private String pollDirect(List<ConfluenceChange> out) throws Exception {
        InMemoryChangeSink sink = new InMemoryChangeSink();
        ConfluenceCrawler crawler = new ConfluenceCrawler(direct,
                new ConfluenceClient(direct), (ChangeSink) sink);
        if (lastCursor == null || lastCursor.isBlank()) {
            crawler.crawl();
            out.addAll(sink.changes());
            return Instant.now().toString();
        }
        String next = crawler.crawlIncremental(lastCursor);
        out.addAll(sink.changes());
        return next;
    }

    private ManagedChannel channel() {
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(grpcTarget);
        if (plaintext) {
            builder.usePlaintext();
        }
        return builder.build();
    }

    @Override
    public void stop() {
    }

    private static List<String> splitCsv(String raw) {
        if (raw == null || raw.isBlank()) {
            return List.of();
        }
        return java.util.Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private static String required(Map<String, String> props, String key) {
        String value = props.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " is required");
        }
        return value;
    }
}
