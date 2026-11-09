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
import io.grpc.Channel;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * One poll is one Sync pass (gRPC) or one crawler pass (direct). The
 * resume cursor is stored as the Connect offset.
 */
public final class ConfluenceSourceTask extends ChangeSourceTask<ConfluenceChange> {

    /** Kafka Connect task that pulls ConfluenceChange protobuf bytes. */
    public ConfluenceSourceTask() {
        super("confluence");
    }

    private boolean includeBodies;
    private List<String> spaceKeys = List.of();
    private ConfluenceConnectorConfig direct;

    @Override
    protected void configure(Map<String, String> props, boolean directMode) {
        includeBodies = Boolean.parseBoolean(
                props.getOrDefault(ConfluenceSourceConnector.INCLUDE_BODIES, "false"));
        spaceKeys = splitCsv(props.getOrDefault(ConfluenceSourceConnector.SPACES, ""));
        if (directMode) {
            direct = ConfluenceConnectorConfig.builder()
                    .baseUrl(required(props, ConfluenceSourceConnector.BASE_URL))
                    .email(required(props, ConfluenceSourceConnector.EMAIL))
                    .apiToken(required(props, ConfluenceSourceConnector.API_TOKEN))
                    .spaces(spaceKeys)
                    .build();
        }
    }

    @Override
    protected String pollGrpc(Channel channel, String lastCursor, List<ConfluenceChange> out) {
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
        String cursor = fallbackCursor(lastCursor);
        while (events.hasNext()) {
            SyncResponse event = events.next();
            if (event.hasChange()) {
                out.add(event.getChange());
            } else if (event.getEventCase() == SyncResponse.EventCase.RESUME_CURSOR) {
                cursor = event.getResumeCursor();
            }
        }
        return cursor;
    }

    @Override
    protected String pollDirect(String lastCursor, List<ConfluenceChange> out) throws Exception {
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

    @Override
    protected String recordKey(ConfluenceChange change) {
        return change.getEntity().getEntityId();
    }
}
