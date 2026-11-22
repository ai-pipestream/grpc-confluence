package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ChangeSource;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceChangeServiceGrpc;
import ai.pipestream.confluence.v1.ConfluenceEvent;
import ai.pipestream.confluence.v1.ConfluenceEventAction;
import ai.pipestream.confluence.v1.ConfluenceEventObject;
import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.ContentBody;
import ai.pipestream.confluence.v1.CreateFooterCommentRequest;
import ai.pipestream.confluence.v1.CreatePageRequest;
import ai.pipestream.confluence.v1.DeletePageRequest;
import ai.pipestream.confluence.v1.GetSpaceRequest;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.StorageBody;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.UpdatePageRequest;
import ai.pipestream.confluence.v1.WatchChangesRequest;
import ai.pipestream.confluence.v1.WatchChangesResponse;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Every step of a {@code ConfluenceChangeService.WatchChanges} run fed by the
 * Forge event queue, shared by the live recording and the replay. Each step's
 * REST and queue exchanges are saved under its id.
 */
final class ChangeScenarios {

    /** How long a live step waits for Forge to queue an event. */
    static final Duration EVENT_WAIT = Duration.ofMinutes(6);
    /** How long a step waits for a drain or a streamed change. */
    static final Duration STREAM_WAIT = Duration.ofMinutes(2);
    private static final long CALL_DEADLINE_SECONDS = 120;

    /** Where queued events come from: live waits for Forge, replay does nothing. */
    interface Events {
        /** Live: waits until an event matching {@code match} is queued. */
        void await(Predicate<ConfluenceEvent> match) throws Exception;
    }

    /** One step body; answers the detail of a PASS. */
    @FunctionalInterface
    interface Body {
        String run() throws Exception;
    }

    /** One step. */
    record Scenario(String id, String label, Body body) {
    }

    /** One report row. */
    record Row(String id, String label, String outcome, String detail) {
    }

    private final ConfluenceServiceGrpc.ConfluenceServiceBlockingStub confluence;
    private final ConfluenceChangeServiceGrpc.ConfluenceChangeServiceStub changes;
    private final ConfluenceChangeGrpcService service;
    private final String spaceKey;
    private final String run;
    private final Events events;
    private final BlockingQueue<Integer> drains = new LinkedBlockingQueue<>();
    private final BlockingQueue<WatchChangesResponse> watched = new LinkedBlockingQueue<>();
    private final List<Row> rows = new ArrayList<>();

    private Context.CancellableContext watch;
    private volatile Throwable watchError;
    private Space space;
    private Page page;
    private boolean purged;

    ChangeScenarios(ManagedChannel channel, ConfluenceChangeGrpcService service, String spaceKey,
            String run, Events events) {
        this.confluence = ConfluenceServiceGrpc.newBlockingStub(channel);
        this.changes = ConfluenceChangeServiceGrpc.newStub(channel);
        this.service = service;
        this.spaceKey = spaceKey;
        this.run = run;
        this.events = events;
        service.onDrain(drains::add);
    }

    /** Every step, in run order. */
    List<Scenario> all() {
        return List.of(
                new Scenario("01-watch-start", "WatchChanges (open, first drain)", this::watchStart),
                new Scenario("02-create-page", "CreatePage", this::createPage),
                new Scenario("03-created-event", "page created event resolved", () -> pageEvent(
                        ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED, 1)),
                new Scenario("04-update-page", "UpdatePage", this::updatePage),
                new Scenario("05-updated-event", "page updated event resolved", () -> pageEvent(
                        ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_UPDATED, 2)),
                new Scenario("06-create-comment", "CreateFooterComment", this::createComment),
                new Scenario("07-comment-event", "comment created event resolved",
                        this::commentEvent),
                new Scenario("08-trash-page", "DeletePage (trash)", this::trashPage),
                new Scenario("09-trashed-event", "page trashed event resolved",
                        () -> goneEvent(ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_TRASHED)),
                new Scenario("10-purge-page", "DeletePage (purge)", this::purgePage),
                new Scenario("11-deleted-event", "page deleted event resolved",
                        () -> goneEvent(ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_DELETED)),
                new Scenario("12-watch-stop", "WatchChanges (cancel)", this::watchStop));
    }

    /** Runs one step and records its row; a failure is a FAIL row, not an exception. */
    void run(Scenario scenario) {
        try {
            rows.add(new Row(scenario.id(), scenario.label(), "PASS", scenario.body().run()));
        } catch (Throwable t) {
            rows.add(new Row(scenario.id(), scenario.label(), "FAIL", String.valueOf(t)));
        }
    }

    List<Row> rows() {
        return rows;
    }

    /** Purges the page if a step left it, and stops the watch. */
    void cleanUp() {
        if (watch != null) {
            watch.cancel(null);
        }
        if (page != null && !purged) {
            try {
                stub().deletePage(DeletePageRequest.newBuilder().setId(page.getId()).build());
            } catch (RuntimeException e) {
                // Already trashed.
            }
            try {
                stub().deletePage(DeletePageRequest.newBuilder().setId(page.getId()).setPurge(true)
                        .build());
            } catch (RuntimeException e) {
                rows.add(new Row("cleanup", "purge the page", "FAIL", String.valueOf(e)));
            }
        }
    }

    private String watchStart() throws Exception {
        space = stub().getSpace(GetSpaceRequest.newBuilder().setKey(spaceKey).build()).getSpace();
        WatchChangesRequest request = WatchChangesRequest.newBuilder()
                .setSync(SyncRequest.newBuilder().addSpaceKeys(spaceKey))
                .setSkipInitialPass(true)
                .setPollInterval(com.google.protobuf.Duration.newBuilder().setSeconds(86_400))
                .setEventInterval(com.google.protobuf.Duration.newBuilder().setSeconds(86_400))
                .build();
        watch = Context.current().withCancellation();
        watch.run(() -> changes.watchChanges(request, new StreamObserver<>() {
            @Override
            public void onNext(WatchChangesResponse value) {
                watched.add(value);
            }

            @Override
            public void onError(Throwable t) {
                watchError = t;
            }

            @Override
            public void onCompleted() {
                // Ends on cancel.
            }
        }));
        return "first drain took " + awaitDrain() + " event(s)";
    }

    private String createPage() {
        page = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                .setTitle(run).setBody(storage("<p>Change feed run " + run + ".</p>")).build())
                .getPage();
        return "page " + page.getId();
    }

    private String updatePage() {
        Page updated = stub().updatePage(UpdatePageRequest.newBuilder().setId(page.getId())
                .setTitle(run).setBody(storage("<p>Change feed run " + run + ", edited.</p>"))
                .setExpectedVersion(page.getVersion().getNumber()).build()).getPage();
        check(updated.getVersion().getNumber() == page.getVersion().getNumber() + 1,
                "version is " + updated.getVersion().getNumber());
        page = updated;
        return "version " + updated.getVersion().getNumber();
    }

    private String createComment() {
        Comment comment = stub().createFooterComment(CreateFooterCommentRequest.newBuilder()
                .setPageId(page.getId()).setBody(storage("<p>comment " + run + "</p>")).build())
                .getComment();
        return "comment " + comment.getId();
    }

    private String trashPage() {
        stub().deletePage(DeletePageRequest.newBuilder().setId(page.getId()).build());
        return "trashed";
    }

    private String purgePage() {
        stub().deletePage(DeletePageRequest.newBuilder().setId(page.getId()).setPurge(true).build());
        purged = true;
        return "purged";
    }

    /** An event about the page with {@code action} arrives and is streamed as a re-read upsert. */
    private String pageEvent(ConfluenceEventAction action, int minVersion) throws Exception {
        Predicate<ConfluenceEvent> match = event -> event.getContentId().equals(page.getId())
                && event.getObject() == ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_PAGE
                && event.getAction() == action;
        ConfluenceChange change = drainUntil(match, c -> c.getEntity().hasPage()
                && c.getEntity().getEntityId().equals(page.getId()));
        check(change.getOperation() == ChangeOperation.CHANGE_OPERATION_UPSERT,
                "operation " + change.getOperation());
        check(change.getEntity().getPage().getVersion().getNumber() >= minVersion,
                "re-read version " + change.getEntity().getPage().getVersion().getNumber());
        check(change.getEntity().getPage().getTitle().equals(run), "re-read title differs");
        return event(change) + " -> page upsert, version "
                + change.getEntity().getPage().getVersion().getNumber();
    }

    /** The comment event re-reads the page it is on, comments included. */
    private String commentEvent() throws Exception {
        Predicate<ConfluenceEvent> match = event ->
                event.getObject() == ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_COMMENT
                        && event.getAction() == ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED
                        && (event.getContainerId().equals(page.getId())
                                || event.getContentId().equals(page.getId()));
        ConfluenceChange change = drainUntil(match, c -> c.getEntity().hasComment()
                && c.getEntity().getComment().getPageId().equals(page.getId()));
        return event(change) + " -> comment upsert from the page re-read";
    }

    /** A trashed or deleted page is streamed as a delete. */
    private String goneEvent(ConfluenceEventAction action) throws Exception {
        Predicate<ConfluenceEvent> match = event -> event.getContentId().equals(page.getId())
                && event.getObject() == ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_PAGE
                && event.getAction() == action;
        ConfluenceChange change = drainUntil(match, c -> c.getEntity().getEntityId()
                .equals(page.getId()) && c.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE);
        return event(change) + " -> delete";
    }

    private String watchStop() throws Exception {
        watch.cancel(null);
        Instant until = Instant.now().plus(Duration.ofSeconds(10));
        while (watchError == null && Instant.now().isBefore(until)) {
            TimeUnit.MILLISECONDS.sleep(50);
        }
        check(watchError != null, "the stream did not end on cancel");
        return "stream ended: " + io.grpc.Status.fromThrowable(watchError).getCode();
    }

    /**
     * Waits for the event, drains once, and answers the first streamed change
     * matching {@code wanted} that carries a matching event and source WEBHOOK.
     */
    private ConfluenceChange drainUntil(Predicate<ConfluenceEvent> event,
            Predicate<ConfluenceChange> wanted) throws Exception {
        events.await(event);
        service.drainNow();
        int taken = awaitDrain();
        List<String> seen = new ArrayList<>();
        WatchChangesResponse message;
        while ((message = watched.poll(10, TimeUnit.SECONDS)) != null) {
            if (message.hasChange()) {
                ConfluenceChange change = message.getChange();
                seen.add(change.getEntity().getEntityId() + " " + change.getEvent().getEventType());
                if (wanted.test(change) && event.test(change.getEvent())) {
                    check(change.getSource() == ChangeSource.CHANGE_SOURCE_WEBHOOK,
                            "source " + change.getSource());
                    return change;
                }
            } else if (message.hasUnresolvedEvent()) {
                seen.add("unresolved " + message.getUnresolvedEvent().getEventType());
            }
        }
        throw new AssertionError("the drain took " + taken + " event(s) but streamed no matching"
                + " change; streamed: " + seen);
    }

    private int awaitDrain() throws Exception {
        Integer taken = drains.poll(STREAM_WAIT.toSeconds(), TimeUnit.SECONDS);
        if (watchError != null) {
            throw new AssertionError("the stream failed: " + watchError);
        }
        check(taken != null, "no drain completed");
        return taken;
    }

    private static String event(ConfluenceChange change) {
        return change.getEvent().getEventType();
    }

    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub() {
        return confluence.withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS);
    }

    private static ContentBody storage(String xhtml) {
        return ContentBody.newBuilder().setStorage(StorageBody.newBuilder().setValue(xhtml)).build();
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
