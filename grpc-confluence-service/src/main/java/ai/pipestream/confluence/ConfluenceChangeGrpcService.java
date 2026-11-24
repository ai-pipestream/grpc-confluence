package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ChangeSource;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceChangeServiceGrpc;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ConfluenceEvent;
import ai.pipestream.confluence.v1.ConfluenceEventAction;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.confluence.v1.PassCompleted;
import ai.pipestream.confluence.v1.PassTrigger;
import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.BlogPost;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.Group;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import ai.pipestream.confluence.v1.WatchChangesRequest;
import ai.pipestream.confluence.v1.WatchChangesResponse;
import com.google.protobuf.Timestamp;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The continuous Confluence change feed: Sync passes on a timer, and between
 * them the companion Forge app's queued events, each resolved by re-reading
 * the object it names.
 */
public final class ConfluenceChangeGrpcService
        implements ConfluenceChangeServiceGrpc.AsyncService, io.grpc.BindableService {

    /** Wait between Sync passes when a request leaves it unset. */
    static final Duration DEFAULT_POLL_INTERVAL = Duration.ofMinutes(15);
    /** Wait between queue drains when a request leaves it unset. */
    static final Duration DEFAULT_EVENT_INTERVAL = Duration.ofSeconds(15);
    /** How often a waiting stream checks whether its client has gone. */
    private static final Duration CANCEL_CHECK = Duration.ofSeconds(1);

    private static final System.Logger LOG =
            System.getLogger(ConfluenceChangeGrpcService.class.getName());

    private final ConfluenceGrpcService confluence;
    private final ForgeEventQueue queue;
    private final Clock clock;
    private final AtomicLong drainRequests = new AtomicLong();
    private volatile java.util.function.Consumer<Drain> drainListener = drain -> { };

    /**
     * Creates the service.
     *
     * @param confluence the read service whose Sync each pass runs
     * @param queue the Forge event queue; {@code null} = timer passes only
     * @param clock time source
     */
    ConfluenceChangeGrpcService(ConfluenceGrpcService confluence, ForgeEventQueue queue,
            Clock clock) {
        this.confluence = confluence;
        this.queue = queue;
        this.clock = clock;
    }

    /**
     * Binds the service behind {@link FlowControlInterceptor}.
     *
     * @return the intercepted service definition
     */
    @Override
    public io.grpc.ServerServiceDefinition bindService() {
        return io.grpc.ServerInterceptors.intercept(ConfluenceChangeServiceGrpc.bindService(this),
                new FlowControlInterceptor());
    }

    @Override
    public void watchChanges(WatchChangesRequest request,
            StreamObserver<WatchChangesResponse> observer) {
        Duration poll = request.hasPollInterval() ? duration(request.getPollInterval())
                : DEFAULT_POLL_INTERVAL;
        Duration drain = request.hasEventInterval() ? duration(request.getEventInterval())
                : DEFAULT_EVENT_INTERVAL;
        if (!poll.isPositive() || !drain.isPositive()) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("poll_interval and event_interval must be positive")
                    .asRuntimeException());
            return;
        }
        Context context = Context.current();
        Emitter out = new Emitter(observer, request.getResumeCursor().isBlank()
                ? request.getSync().getSinceCursor() : request.getResumeCursor());
        try {
            Instant nextPass = request.getSkipInitialPass() ? clock.instant().plus(poll)
                    : clock.instant();
            PassTrigger trigger = PassTrigger.PASS_TRIGGER_START;
            long pass = 0;
            Instant nextDrain = clock.instant();
            long drained = drainRequests.get();
            while (!context.isCancelled()) {
                Instant now = clock.instant();
                if (!now.isBefore(nextPass)) {
                    runPass(request.getSync(), ++pass, trigger, out);
                    trigger = PassTrigger.PASS_TRIGGER_TIMER;
                    nextPass = clock.instant().plus(poll);
                    continue;
                }
                long wanted = drainRequests.get();
                if (queue != null && (!now.isBefore(nextDrain) || wanted != drained)) {
                    drained = wanted;
                    drain(request.getSync(), out);
                    nextDrain = clock.instant().plus(drain);
                    continue;
                }
                Instant wake = queue == null || nextPass.isBefore(nextDrain) ? nextPass : nextDrain;
                long millis = Math.max(1, Math.min(CANCEL_CHECK.toMillis(),
                        Duration.between(now, wake).toMillis()));
                Thread.sleep(millis);
            }
            observer.onCompleted();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            observer.onError(Status.CANCELLED.withDescription("watch interrupted").asRuntimeException());
        } catch (Throwable t) {
            if (!context.isCancelled()) {
                observer.onError(ConfluenceGrpcService.Failure.of(t).toException());
            }
        }
    }

    /**
     * One drain of one stream, as told to {@link #onDrain} when it ends.
     *
     * @param taken the events the drain took
     * @param lastSequence the {@code sequence} of the last message the stream
     *        had sent when the drain ended, 0 when it had sent none. The
     *        listener is told from the stream's own thread, ahead of the
     *        messages still on their way to the client: a caller that reads
     *        the stream after a drain waits for this message first.
     */
    record Drain(List<ForgeEventQueue.Queued> taken, long lastSequence) {
    }

    /** Sets what is told each drain, after it ends. */
    void onDrain(java.util.function.Consumer<Drain> listener) {
        this.drainListener = listener;
    }

    /** Makes every open stream drain the event queue once, now. */
    void drainNow() {
        drainRequests.incrementAndGet();
    }

    /** Runs one Sync pass and forwards its events. */
    private void runPass(SyncRequest scope, long pass, PassTrigger trigger, Emitter out)
            throws Throwable {
        Instant started = clock.instant();
        AtomicLong changes = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicReference<String> next = new AtomicReference<>();
        String resume = out.cursor();
        confluence.sync(scope.toBuilder().setSinceCursor(resume).build(), new StreamObserver<>() {
            @Override
            public void onNext(SyncResponse value) {
                switch (value.getEventCase()) {
                    case CHANGE -> {
                        out.change(value.getChange());
                        changes.incrementAndGet();
                    }
                    case SNAPSHOT -> out.snapshot(value.getSnapshot());
                    case RESUME_CURSOR -> next.set(value.getResumeCursor());
                    default -> {
                        // Nothing else is streamed.
                    }
                }
            }

            @Override
            public void onError(Throwable t) {
                failure.set(t);
            }

            @Override
            public void onCompleted() {
                // The resume cursor arrived in onNext.
            }
        });
        if (failure.get() != null) {
            throw failure.get();
        }
        if (next.get() != null && !next.get().isBlank()) {
            out.cursor(next.get());
        }
        out.passCompleted(PassCompleted.newBuilder().setPass(pass).setTrigger(trigger)
                .setChangeCount(changes.get()).setStartedAt(timestamp(started))
                .setCompletedAt(timestamp(clock.instant())).build());
    }

    /**
     * Takes queued events, resolves them, and acknowledges those handled. An
     * event followed in the same batch by one of the same type about the same
     * object is acknowledged without its own re-read, since the later re-read
     * answers the same state. A failed read leaves the event and every later
     * one in the queue, so the next drain retries them in order.
     */
    private void drain(SyncRequest scope, Emitter out) throws Exception {
        List<ForgeEventQueue.Queued> batch;
        try {
            batch = queue.take(ForgeEventQueue.DEFAULT_TAKE);
        } catch (ForgeEventQueue.Busy e) {
            LOG.log(System.Logger.Level.WARNING, "event queue busy, retried next drain: {0}",
                    e.getMessage());
            return;
        }
        java.util.Map<String, Integer> last = new java.util.HashMap<>();
        for (int i = 0; i < batch.size(); i++) {
            String key = coalesceKey(batch.get(i).event());
            if (key != null) {
                last.put(key, i);
            }
        }
        List<String> handled = new ArrayList<>();
        try {
            for (int i = 0; i < batch.size(); i++) {
                ForgeEventQueue.Queued queued = batch.get(i);
                String key = coalesceKey(queued.event());
                if (key == null || last.get(key) == i) {
                    resolve(scope, queued.event(), out);
                }
                handled.add(queued.key());
            }
        } catch (Exception e) {
            LOG.log(System.Logger.Level.WARNING, "event {0} not resolved, retried next drain: {1}",
                    batch.get(handled.size()).event().getEventType(), e.toString());
        } finally {
            try {
                queue.ack(handled);
            } catch (ForgeEventQueue.Busy e) {
                LOG.log(System.Logger.Level.WARNING,
                        "event queue busy, {0} handled events are taken again next drain: {1}",
                        handled.size(), e.getMessage());
            }
            drainListener.accept(new Drain(List.copyOf(batch), out.sequence()));
        }
    }

    /** The key under which events of one type about one object coalesce; {@code null} = never. */
    static String coalesceKey(ConfluenceEvent event) {
        String target = event.getObject()
                == ai.pipestream.confluence.v1.ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_SPACE
                && !event.getSpaceId().isBlank() ? event.getSpaceId() : event.getContentId();
        if (target.isBlank() || event.getEventType().isBlank()) {
            return null;
        }
        return event.getEventType() + "|" + target;
    }

    /** Re-reads what one event names and emits it, stamped with the event; skips other spaces. */
    void resolve(SyncRequest scope, ConfluenceEvent event, Emitter out) throws Exception {
        ChangeSink sink = new ChangeSink() {
            @Override
            public void emit(ConfluenceChange change) {
                ConfluenceChange stamped = change.toBuilder().setEvent(event).build();
                out.change(scope.getIncludeBodies() ? stamped
                        : ConfluenceGrpcService.stripBodies(stamped));
            }

            @Override
            public void snapshot(ConfluenceSnapshot snapshot) {
                out.snapshot(snapshot);
            }
        };
        List<String> spaces = confluence.spaceScope(scope);
        if (!spaces.isEmpty() && !event.getSpaceKey().isBlank()
                && !spaces.contains(event.getSpaceKey())) {
            return;
        }
        String id = event.getContentId();
        boolean gone = event.getAction() == ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_TRASHED
                || event.getAction() == ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_DELETED;
        ConfluenceCrawler crawler = confluence.crawler(scope.getConnectionId(), sink);
        String cursor = event.getEventId();
        ChangeSource source = ChangeSource.CHANGE_SOURCE_WEBHOOK;
        switch (event.getObject()) {
            case CONFLUENCE_EVENT_OBJECT_PAGE, CONFLUENCE_EVENT_OBJECT_BLOG_POST -> {
                boolean blog = event.getObject()
                        == ai.pipestream.confluence.v1.ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_BLOG_POST;
                if (gone) {
                    sink.emit(delete(blog ? ConfluenceEntity.newBuilder().setEntityId(id)
                            .setBlogPost(BlogPost.newBuilder().setId(id))
                            : ConfluenceEntity.newBuilder().setEntityId(id)
                                    .setPage(Page.newBuilder().setId(id)), cursor));
                } else {
                    rereadOrDelete(() -> crawler.reread(blog, id, source, cursor), sink,
                            blog ? ConfluenceEntity.newBuilder().setEntityId(id)
                                    .setBlogPost(BlogPost.newBuilder().setId(id))
                                    : ConfluenceEntity.newBuilder().setEntityId(id)
                                            .setPage(Page.newBuilder().setId(id)), cursor);
                }
            }
            case CONFLUENCE_EVENT_OBJECT_ATTACHMENT, CONFLUENCE_EVENT_OBJECT_COMMENT,
                    CONFLUENCE_EVENT_OBJECT_LABEL -> {
                boolean attachment = event.getObject()
                        == ai.pipestream.confluence.v1.ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_ATTACHMENT;
                boolean comment = event.getObject()
                        == ai.pipestream.confluence.v1.ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_COMMENT;
                if (gone && attachment) {
                    sink.emit(delete(ConfluenceEntity.newBuilder().setEntityId(id)
                            .setAttachment(Attachment.newBuilder().setId(id)), cursor));
                } else if (gone && comment) {
                    sink.emit(delete(ConfluenceEntity.newBuilder().setEntityId(id)
                            .setComment(Comment.newBuilder().setId(id)), cursor));
                }
                String container = container(event);
                if (container.isBlank()) {
                    out.unresolved(event);
                } else {
                    boolean blog = event.getContentType().equals("blogpost")
                            || containerType(event).equals("blogpost");
                    rereadOrDelete(() -> crawler.reread(blog, container, source, cursor), sink,
                            null, cursor);
                }
            }
            case CONFLUENCE_EVENT_OBJECT_SPACE -> {
                String spaceId = event.getSpaceId().isBlank() ? id : event.getSpaceId();
                if (gone) {
                    sink.emit(delete(ConfluenceEntity.newBuilder().setEntityId(spaceId)
                            .setSpace(Space.newBuilder().setId(spaceId)), cursor));
                } else {
                    rereadOrDelete(() -> crawler.rereadSpace(spaceId, source, cursor), sink,
                            ConfluenceEntity.newBuilder().setEntityId(spaceId)
                                    .setSpace(Space.newBuilder().setId(spaceId)), cursor);
                }
            }
            case CONFLUENCE_EVENT_OBJECT_TASK -> {
                String container = container(event);
                if (container.isBlank()) {
                    out.unresolved(event);
                } else {
                    boolean blog = containerType(event).equals("blogpost");
                    rereadOrDelete(() -> crawler.reread(blog, container, source, cursor), sink,
                            null, cursor);
                }
            }
            case CONFLUENCE_EVENT_OBJECT_WHITEBOARD, CONFLUENCE_EVENT_OBJECT_DATABASE,
                    CONFLUENCE_EVENT_OBJECT_FOLDER, CONFLUENCE_EVENT_OBJECT_EMBED -> {
                String type = switch (event.getObject()) {
                    case CONFLUENCE_EVENT_OBJECT_WHITEBOARD -> "whiteboard";
                    case CONFLUENCE_EVENT_OBJECT_DATABASE -> "database";
                    case CONFLUENCE_EVENT_OBJECT_FOLDER -> "folder";
                    default -> "embed";
                };
                ConfluenceEntity.Builder item = treeItem(type, id);
                if (id.isBlank()) {
                    out.unresolved(event);
                } else if (gone) {
                    sink.emit(delete(item, cursor));
                } else {
                    rereadOrDelete(() -> crawler.rereadTreeItem(type, id, source, cursor), sink,
                            item, cursor);
                }
            }
            case CONFLUENCE_EVENT_OBJECT_GROUP -> {
                ConfluenceEntity.Builder group = ConfluenceEntity.newBuilder().setEntityId(id)
                        .setGroup(Group.newBuilder().setId(id).setName(event.getTitle()));
                if (id.isBlank()) {
                    out.unresolved(event);
                } else if (gone) {
                    sink.emit(delete(group, cursor));
                } else {
                    try {
                        crawler.rereadGroup(id, source, cursor);
                    } catch (ConfluenceClient.ConfluenceApiException e) {
                        if (e.status() != 400 && e.status() != 404) {
                            throw e;
                        }
                        sink.emit(delete(group, cursor));
                    }
                }
            }
            default -> out.unresolved(event);
        }
    }

    /** An entity naming one whiteboard, database, folder or Smart Link by id. */
    private static ConfluenceEntity.Builder treeItem(String type, String id) {
        ConfluenceEntity.Builder entity = ConfluenceEntity.newBuilder().setEntityId(id);
        return switch (type) {
            case "whiteboard" -> entity.setWhiteboard(
                    ai.pipestream.confluence.v1.Whiteboard.newBuilder().setId(id));
            case "database" -> entity.setDatabase(
                    ai.pipestream.confluence.v1.Database.newBuilder().setId(id));
            case "folder" -> entity.setFolder(ai.pipestream.confluence.v1.Folder.newBuilder().setId(id));
            default -> entity.setSmartLink(ai.pipestream.confluence.v1.SmartLink.newBuilder().setId(id));
        };
    }

    /** The page or blog post an attachment, comment or label event belongs to. */
    private static String container(ConfluenceEvent event) {
        if (!event.getContainerId().isBlank()) {
            return event.getContainerId();
        }
        String type = event.getContentType();
        return type.equals("page") || type.equals("blogpost") ? event.getContentId() : "";
    }

    private static String containerType(ConfluenceEvent event) {
        return event.getContainerId().isBlank() ? event.getContentType() : event.getContainerType();
    }

    @FunctionalInterface
    private interface Read {
        void run() throws Exception;
    }

    /** Runs a re-read; a 404 means the object is gone, so emits a delete instead. */
    private static void rereadOrDelete(Read read, ChangeSink sink, ConfluenceEntity.Builder gone,
            String cursor) throws Exception {
        try {
            read.run();
        } catch (ConfluenceClient.ConfluenceApiException e) {
            if (e.status() != 404 || gone == null) {
                throw e;
            }
            sink.emit(delete(gone, cursor));
        }
    }

    private static ConfluenceChange delete(ConfluenceEntity.Builder entity, String cursor) {
        return ConfluenceChange.newBuilder().setChangeId(UUID.randomUUID().toString())
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .setEntity(entity.setIngestedAt(timestamp(Instant.now())))
                .setCursor(cursor).setSource(ChangeSource.CHANGE_SOURCE_WEBHOOK)
                .setOccurredAt(timestamp(Instant.now())).build();
    }

    /** Numbers and sends the messages of one stream. */
    static final class Emitter {
        private final StreamObserver<WatchChangesResponse> observer;
        private final AtomicLong sequence = new AtomicLong();
        private volatile String cursor;

        Emitter(StreamObserver<WatchChangesResponse> observer, String cursor) {
            this.observer = observer;
            this.cursor = cursor;
        }

        String cursor() {
            return cursor;
        }

        void cursor(String next) {
            this.cursor = next;
        }

        void change(ConfluenceChange change) {
            send(WatchChangesResponse.newBuilder().setChange(change));
        }

        void snapshot(ConfluenceSnapshot snapshot) {
            send(WatchChangesResponse.newBuilder().setSnapshot(snapshot));
        }

        void passCompleted(PassCompleted completed) {
            send(WatchChangesResponse.newBuilder().setPassCompleted(completed));
        }

        void unresolved(ConfluenceEvent event) {
            send(WatchChangesResponse.newBuilder().setUnresolvedEvent(event));
        }

        /** The sequence of the last message sent, 0 before the first. */
        long sequence() {
            return sequence.get();
        }

        private synchronized void send(WatchChangesResponse.Builder message) {
            observer.onNext(message.setSequence(sequence.incrementAndGet()).setResumeCursor(cursor)
                    .build());
        }
    }

    private static Duration duration(com.google.protobuf.Duration value) {
        return Duration.ofSeconds(value.getSeconds(), value.getNanos());
    }

    private static Timestamp timestamp(Instant instant) {
        return Timestamp.newBuilder().setSeconds(instant.getEpochSecond()).setNanos(instant.getNano())
                .build();
    }
}
