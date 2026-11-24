package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.*;
import ai.pipestream.sync.v1.AclCompleteness;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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
    /** Page updates queued at once by the backlog step; more than one take answers. */
    static final int BACKLOG = 105;
    /** How long the Forge app keeps an uncollected event. */
    static final Duration EVENT_TTL = Duration.ofDays(7);
    /** Set to {@code 1} for the live step that adds the caller to the throwaway group and removes it. */
    static final String ENV_GROUP_MEMBER = "CONFLUENCE_LIVE_CHANGES_GROUP_MEMBER";
    private static final long CALL_DEADLINE_SECONDS = 120;
    private static final int MAX_BACKLOG_DRAINS = 40;
    private static final String TREE_COPY_PREFIX = "Copy of ";
    private static final int GROUP_GONE_READS = 16;

    /** Where queued events come from: live waits for Forge, replay does nothing. */
    interface Events {
        /** Live: waits until an event matching {@code match} is queued. */
        void await(Predicate<ConfluenceEvent> match) throws Exception;

        /** Live: waits until at least {@code count} events are queued. */
        void awaitQueued(int count) throws Exception;

        /** Live: sleeps; replay: returns at once. */
        void pause(Duration time) throws InterruptedException;
    }

    /** One step body; answers the detail of a PASS. */
    @FunctionalInterface
    interface Body {
        String run() throws Exception;
    }

    /** One step. */
    record Scenario(String id, String label, Body body) {
    }

    /** Ends a step as LOST: Forge never delivered the event the step waits for. */
    static final class Lost extends Exception {
        private static final long serialVersionUID = 1L;

        Lost(String reason) {
            super(reason);
        }
    }

    /** One report row. */
    record Row(String id, String label, String outcome, String detail) {
    }

    /** Ends a step as SKIPPED with a reason. */
    static final class Skip extends Exception {
        private static final long serialVersionUID = 1L;

        Skip(String reason) {
            super(reason);
        }
    }

    /** What drains took and streamed. */
    private record Drained(List<ForgeEventQueue.Queued> taken, List<WatchChangesResponse> streamed) {
    }

    /** A content tree item whose restrictions the run changes. */
    private enum Item {
        FOLDER("folder", ContentType.CONTENT_TYPE_FOLDER,
                ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_FOLDER),
        WHITEBOARD("whiteboard", ContentType.CONTENT_TYPE_WHITEBOARD,
                ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_WHITEBOARD),
        DATABASE("database", ContentType.CONTENT_TYPE_DATABASE,
                ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_DATABASE);

        private final String kind;
        private final ContentType type;
        private final ConfluenceEventObject object;

        Item(String kind, ContentType type, ConfluenceEventObject object) {
            this.kind = kind;
            this.type = type;
            this.object = object;
        }
    }

    private final ManagedChannel channel;
    private final ConfluenceServiceGrpc.ConfluenceServiceBlockingStub confluence;
    private final ConfluenceChangeServiceGrpc.ConfluenceChangeServiceStub changes;
    private final ConfluenceChangeGrpcService service;
    private final String spaceKey;
    private final String ownSpaceKey;
    private final String run;
    private final Events events;
    private final WatchedChanges watched;
    private final List<Row> rows = new ArrayList<>();
    private final List<Runnable> cleanups = new ArrayList<>();
    private final Map<String, ForgeEventQueue.Queued> allTaken = new LinkedHashMap<>();
    private final Map<Item, String> items = new EnumMap<>(Item.class);
    private final Set<Item> restricted = EnumSet.noneOf(Item.class);
    private final Set<Item> unrestricted = EnumSet.noneOf(Item.class);
    private final boolean memberSteps;

    private Context.CancellableContext watch;
    private Space space;
    private Page page;
    private Page parent;
    private Page backlogPage;
    private Comment comment;
    private Attachment attachment;
    private BlogPost blog;
    private Space ownSpace;
    private Drained createdDrain;
    private boolean restored;
    private String copyId;
    private boolean archived;
    private String treeCopyId;
    private String groupId;
    private boolean groupDeleted;
    private final String groupName;

    /**
     * The steps of one run.
     *
     * @param channel the channel to the service under test
     * @param service the change service behind the channel
     * @param spaceKey the space the run writes in
     * @param run the name of the run, unique to it
     * @param memberSteps whether the run adds the caller to its throwaway group
     * @param events where queued events come from
     */
    ChangeScenarios(ManagedChannel channel, ConfluenceChangeGrpcService service, String spaceKey,
            String run, boolean memberSteps, Events events) {
        this.channel = channel;
        this.memberSteps = memberSteps;
        this.groupName = run + "-group";
        this.confluence = ConfluenceServiceGrpc.newBlockingStub(channel);
        this.changes = ConfluenceChangeServiceGrpc.newStub(channel);
        this.service = service;
        this.spaceKey = spaceKey;
        this.ownSpaceKey = "LC" + run.substring(run.length() - 8).toUpperCase(Locale.ROOT);
        this.run = run;
        this.events = events;
        this.watched = new WatchedChanges(service);
    }

    /** Every step, in run order. */
    List<Scenario> all() {
        ConfluenceEventObject pageObject = ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_PAGE;
        ConfluenceEventObject blogObject = ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_BLOG_POST;
        return List.of(
                new Scenario("01-watch-start", "WatchChanges (open, first drain)", this::watchStart),
                new Scenario("02-create-pages", "CreatePage (with a task, and a parent)",
                        this::createPages),
                new Scenario("03-created-event", "page created event resolved", () -> contentEvent(
                        pageObject, ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED, 1)),
                new Scenario("04-event-expiry", "queued event expires 7 days after it happened",
                        this::eventExpiry),
                new Scenario("05-update-page", "UpdatePage", this::updatePage),
                new Scenario("06-updated-event", "page updated event resolved", () -> contentEvent(
                        pageObject, ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_UPDATED, 2)),
                new Scenario("07-move-page", "UpdatePage (new parent)", this::movePage),
                new Scenario("08-moved-event", "page moved event resolved", this::movedEvent),
                new Scenario("08a-copy-page", "CopyPage", this::copyPage),
                new Scenario("08b-copied-event", "page copied event resolved", this::copiedEvent),
                new Scenario("08c-archive-copy", "ArchivePages", this::archiveCopy),
                new Scenario("08d-archived-event", "page archived event resolved", this::archivedEvent),
                new Scenario("08e-update-archived", "UpdatePage (archived page) is refused",
                        this::updateArchived),
                new Scenario("08f-copy-hierarchy", "CopyPageHierarchy", this::copyHierarchy),
                new Scenario("08g-hierarchy-copied-event", "copied page tree event resolved",
                        this::hierarchyCopiedEvent),
                new Scenario("09-create-comment", "CreateFooterComment", this::createComment),
                new Scenario("10-comment-created-event", "comment created event resolved",
                        () -> commentEvent(ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED, 1)),
                new Scenario("11-update-comment", "UpdateFooterComment", this::updateComment),
                new Scenario("12-comment-updated-event", "comment updated event resolved",
                        () -> commentEvent(ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_UPDATED, 2)),
                new Scenario("13-delete-comment", "DeleteFooterComment", this::deleteComment),
                new Scenario("14-comment-deleted-event", "comment deleted event resolved",
                        this::commentDeletedEvent),
                new Scenario("15-upload-attachment", "UploadAttachment", this::uploadAttachment),
                new Scenario("16-attachment-created-event", "attachment created event resolved",
                        this::attachmentCreatedEvent),
                new Scenario("17-trash-attachment", "DeleteAttachment (trash)",
                        () -> deleteAttachment(false)),
                new Scenario("18-attachment-trashed-event", "attachment trashed event resolved",
                        () -> attachmentGoneEvent(ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_TRASHED)),
                new Scenario("19-purge-attachment", "DeleteAttachment (purge)",
                        () -> deleteAttachment(true)),
                new Scenario("20-attachment-deleted-event", "attachment deleted event resolved",
                        () -> attachmentGoneEvent(ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_DELETED)),
                new Scenario("21-restrict-page", "AddContentPermissions (page)", this::restrictPage),
                new Scenario("22-restricted-event", "page permissions event carries the permission",
                        this::restrictedEvent),
                new Scenario("23-unrestrict-page", "DeleteContentPermissions (page)",
                        this::unrestrictPage),
                new Scenario("24-unrestricted-event", "page permissions event drops the permission",
                        this::unrestrictedEvent),
                new Scenario("24a-restrict-folder", "CreateFolder, AddContentPermissions (folder)",
                        () -> restrictItem(Item.FOLDER)),
                new Scenario("24b-folder-restricted-event", "folder permissions event carries the permission",
                        () -> itemRestrictedEvent(Item.FOLDER)),
                new Scenario("24c-unrestrict-folder", "DeleteContentPermissions (folder)",
                        () -> unrestrictItem(Item.FOLDER)),
                new Scenario("24d-folder-unrestricted-event",
                        "folder permissions event drops the permission",
                        () -> itemUnrestrictedEvent(Item.FOLDER)),
                new Scenario("24e-restrict-whiteboard",
                        "CreateWhiteboard, AddContentPermissions (whiteboard)",
                        () -> restrictItem(Item.WHITEBOARD)),
                new Scenario("24f-whiteboard-restricted-event",
                        "whiteboard permissions event carries the permission",
                        () -> itemRestrictedEvent(Item.WHITEBOARD)),
                new Scenario("24g-unrestrict-whiteboard", "DeleteContentPermissions (whiteboard)",
                        () -> unrestrictItem(Item.WHITEBOARD)),
                new Scenario("24h-whiteboard-unrestricted-event",
                        "whiteboard permissions event drops the permission",
                        () -> itemUnrestrictedEvent(Item.WHITEBOARD)),
                new Scenario("24i-restrict-database", "CreateDatabase, AddContentPermissions (database)",
                        () -> restrictItem(Item.DATABASE)),
                new Scenario("24j-database-restricted-event",
                        "database permissions event carries the permission",
                        () -> itemRestrictedEvent(Item.DATABASE)),
                new Scenario("24k-unrestrict-database", "DeleteContentPermissions (database)",
                        () -> unrestrictItem(Item.DATABASE)),
                new Scenario("24l-database-unrestricted-event",
                        "database permissions event drops the permission",
                        () -> itemUnrestrictedEvent(Item.DATABASE)),
                new Scenario("25-complete-task", "UpdateTask (complete)", this::completeTask),
                new Scenario("26-task-event", "task updated event resolved", this::taskEvent),
                new Scenario("27-create-blog", "CreateBlogPost", this::createBlog),
                new Scenario("28-blog-created-event", "blog post created event resolved",
                        () -> contentEvent(blogObject, ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED, 1)),
                new Scenario("29-update-blog", "UpdateBlogPost", this::updateBlog),
                new Scenario("30-blog-updated-event", "blog post updated event resolved",
                        () -> contentEvent(blogObject, ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_UPDATED, 2)),
                new Scenario("31-blog-comment", "CreateFooterComment (blog post)", this::blogComment),
                new Scenario("32-blog-comment-event", "blog comment event re-reads the blog post",
                        this::blogCommentEvent),
                new Scenario("33-trash-blog", "DeleteBlogPost (trash)", () -> deleteBlog(false)),
                new Scenario("34-blog-trashed-event", "blog post trashed event resolved",
                        () -> goneEvent(blogObject, ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_TRASHED)),
                new Scenario("35-purge-blog", "DeleteBlogPost (purge)", () -> deleteBlog(true)),
                new Scenario("36-blog-deleted-event", "blog post deleted event resolved",
                        () -> goneEvent(blogObject, ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_DELETED)),
                new Scenario("37-backlog", "backlog of " + BACKLOG + " page updates", this::backlog),
                new Scenario("38-trash-page", "DeletePage (trash)", this::trashPage),
                new Scenario("39-trashed-event", "page trashed event resolved",
                        () -> goneEvent(pageObject, ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_TRASHED)),
                new Scenario("40-restore-page", "UpdatePage (restore from trash)", this::restorePage),
                new Scenario("41-restored-event", "page restored event resolved", this::restoredEvent),
                new Scenario("42-purge-page", "DeletePage (purge)", this::purgePage),
                new Scenario("43-deleted-event", "page deleted event resolved",
                        () -> goneEvent(pageObject, ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_DELETED)),
                new Scenario("44-create-space", "CreateSpace (own throwaway space)", this::createSpace),
                new Scenario("45-space-created-event", "space created event resolved",
                        this::spaceCreatedEvent),
                new Scenario("46-delete-space", "DeleteSpace (own throwaway space)", this::deleteSpace),
                new Scenario("47-space-deleted-event", "space deleted event resolved",
                        this::spaceDeletedEvent),
                new Scenario("47a-create-group", "CreateGroup (throwaway), GetGroup, ListGroups",
                        this::createGroup),
                new Scenario("47b-group-created-event", "group created event resolved",
                        this::groupCreatedEvent),
                new Scenario("47c-group-members", "AddGroupMember, RemoveGroupMember (the caller)",
                        this::groupMembers),
                new Scenario("47d-delete-group", "DeleteGroup (throwaway)", this::deleteGroup),
                new Scenario("47e-group-deleted-event", "group deleted event resolved",
                        this::groupDeletedEvent),
                new Scenario("48-watch-stop", "WatchChanges (cancel)", this::watchStop));
    }

    /** Runs one step and records its row; a failure is a FAIL row, not an exception. */
    void run(Scenario scenario) {
        try {
            rows.add(new Row(scenario.id(), scenario.label(), "PASS", scenario.body().run()));
        } catch (Skip s) {
            rows.add(new Row(scenario.id(), scenario.label(), "SKIPPED", s.getMessage()));
        } catch (Lost l) {
            rows.add(new Row(scenario.id(), scenario.label(), "LOST", l.getMessage()));
        } catch (Throwable t) {
            rows.add(new Row(scenario.id(), scenario.label(), "FAIL", String.valueOf(t)));
        }
    }

    List<Row> rows() {
        return rows;
    }

    /** Removes whatever the steps left behind, newest first, and stops the watch. */
    void cleanUp() {
        if (watch != null) {
            watch.cancel(null);
        }
        for (Runnable cleanup : cleanups.reversed()) {
            try {
                cleanup.run();
            } catch (RuntimeException e) {
                rows.add(new Row("cleanup", "clean up", "FAIL", String.valueOf(e)));
            }
        }
    }

    // --- steps -----------------------------------------------------------------------------

    private String watchStart() throws Exception {
        space = stub().getSpace(GetSpaceRequest.newBuilder().setKey(spaceKey).build()).getSpace();
        WatchChangesRequest request = WatchChangesRequest.newBuilder()
                .setSync(SyncRequest.newBuilder().addSpaceKeys(spaceKey).addSpaceKeys(ownSpaceKey))
                .setSkipInitialPass(true)
                .setPollInterval(com.google.protobuf.Duration.newBuilder().setSeconds(86_400))
                .setEventInterval(com.google.protobuf.Duration.newBuilder().setSeconds(86_400))
                .build();
        watch = Context.current().withCancellation();
        watch.run(() -> changes.watchChanges(request, watched));
        return "first drain took " + awaitDrain().size() + " event(s)";
    }

    private String createPages() {
        parent = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                .setTitle(run + " parent").setBody(storage("<p>Parent for " + run + ".</p>")).build())
                .getPage();
        String parentId = parent.getId();
        cleanups.add(() -> purgePage(parentId));
        page = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                .setTitle(run).setBody(storage(pageBody(""))).build()).getPage();
        String pageId = page.getId();
        cleanups.add(() -> purgePage(pageId));
        return "page " + page.getId() + ", parent " + parent.getId();
    }

    private String eventExpiry() {
        check(createdDrain != null, "the created event was not drained");
        for (ForgeEventQueue.Queued queued : createdDrain.taken()) {
            ConfluenceEvent event = queued.event();
            if (!event.getContentId().equals(page.getId())
                    || event.getAction() != ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED) {
                continue;
            }
            check(queued.expiresAt() != null, "the queue answered no expiry time");
            check(event.hasOccurredAt(), "the event has no time");
            Instant occurred = Instant.ofEpochSecond(event.getOccurredAt().getSeconds());
            Duration kept = Duration.between(occurred, queued.expiresAt());
            check(kept.minus(EVENT_TTL).abs().compareTo(Duration.ofMinutes(15)) <= 0,
                    "kept for " + kept);
            return "expires " + kept.toHours() + " h after the event";
        }
        throw new AssertionError("the created event was not in its drain");
    }

    private String updatePage() throws InterruptedException {
        page = update(page, UpdatePageRequest.newBuilder().setTitle(run).setBody(storage(pageBody(
                ", edited"))));
        check(page.getVersion().getNumber() == 2, "version is " + page.getVersion().getNumber());
        return "version " + page.getVersion().getNumber();
    }

    private String movePage() throws InterruptedException {
        page = update(page, UpdatePageRequest.newBuilder().setTitle(run).setParentId(parent.getId())
                .setBody(storage(pageBody(", edited"))));
        return "parent " + parent.getId();
    }

    /** A move through the API arrives as a moved or an updated page event. */
    private String movedEvent() throws Exception {
        ConfluenceChange change = expect(event -> (isAbout(event,
                ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_PAGE,
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_MOVED, page.getId())
                || isAbout(event, ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_PAGE,
                        ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_UPDATED, page.getId())),
                c -> c.getEntity().hasPage() && c.getEntity().getEntityId().equals(page.getId())
                        && c.getEntity().getPage().getParentId().equals(parent.getId()));
        return event(change) + " -> page upsert under the new parent";
    }

    private String createComment() {
        comment = stub().createFooterComment(CreateFooterCommentRequest.newBuilder()
                .setPageId(page.getId()).setBody(storage("<p>comment " + run + "</p>")).build())
                .getComment();
        return "comment " + comment.getId();
    }

    private String updateComment() {
        stub().updateFooterComment(UpdateFooterCommentRequest.newBuilder().setId(comment.getId())
                .setBody(storage("<p>comment " + run + " edited</p>"))
                .setExpectedVersion(comment.getVersion().getNumber()).build());
        return "comment " + comment.getId() + " edited";
    }

    private String deleteComment() {
        stub().deleteFooterComment(DeleteFooterCommentRequest.newBuilder().setId(comment.getId())
                .build());
        return "comment " + comment.getId() + " deleted";
    }

    /** A comment event re-reads the page it is on; the comment comes back at {@code version}. */
    private String commentEvent(ConfluenceEventAction action, int version) throws Exception {
        ConfluenceChange change = expect(event -> isAbout(event,
                ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_COMMENT, action, null)
                && onContainer(event, page.getId(), comment.getId()),
                c -> c.getEntity().hasComment() && c.getEntity().getEntityId().equals(comment.getId()));
        int read = change.getEntity().getComment().getVersion().getNumber();
        check(read >= version, "re-read comment version " + read);
        return event(change) + " -> comment upsert from the page re-read, version " + read;
    }

    private String commentDeletedEvent() throws Exception {
        ConfluenceChange change = expect(event -> isAbout(event,
                ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_COMMENT,
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_DELETED, null)
                && onContainer(event, page.getId(), comment.getId()),
                c -> c.getEntity().getEntityId().equals(comment.getId())
                        && c.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE);
        return event(change) + " -> comment delete";
    }

    private String uploadAttachment() throws Exception {
        byte[] bytes = ("change feed attachment " + run + "\n").getBytes(StandardCharsets.UTF_8);
        attachment = upload(page.getId(), run + ".txt", bytes);
        return "attachment " + attachment.getId();
    }

    private String attachmentCreatedEvent() throws Exception {
        ConfluenceChange change = expect(event -> isAbout(event,
                ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_ATTACHMENT,
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED, null)
                && onContainer(event, page.getId(), attachment.getId()),
                c -> c.getEntity().hasAttachment()
                        && c.getEntity().getEntityId().equals(attachment.getId()));
        return event(change) + " -> attachment upsert from the page re-read";
    }

    private String deleteAttachment(boolean purge) {
        stub().deleteAttachment(DeleteAttachmentRequest.newBuilder().setId(attachment.getId())
                .setPurge(purge).build());
        return purge ? "purged" : "trashed";
    }

    private String attachmentGoneEvent(ConfluenceEventAction action) throws Exception {
        ConfluenceChange change = expect(event -> isAbout(event,
                ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_ATTACHMENT, action, attachment.getId()),
                c -> c.getEntity().getEntityId().equals(attachment.getId())
                        && c.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE);
        return event(change) + " -> attachment delete";
    }

    private ContentRef pageRef() {
        return ContentRef.newBuilder().setType(ContentType.CONTENT_TYPE_PAGE).setId(page.getId())
                .build();
    }

    private String restrictPage() {
        stub().addContentPermissions(AddContentPermissionsRequest.newBuilder().setContent(pageRef())
                .addPermissions(ContentPermissionGrant.newBuilder()
                        .setOperation(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_UPDATE)
                        .addPrincipals(self()))
                .build());
        stub().setRestrictionState(SetRestrictionStateRequest.newBuilder().setContent(pageRef())
                .setState(ContentRestrictionState.CONTENT_RESTRICTION_STATE_EDIT_RESTRICTED).build());
        return "update permission for the page author; edit restricted";
    }

    private Predicate<ConfluenceEvent> permissionsEvent() {
        return event -> isAbout(event, ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_PAGE,
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_PERMISSIONS_UPDATED, page.getId());
    }

    private String restrictedEvent() throws Exception {
        String owner = "page:" + page.getId() + "/permission/";
        ConfluenceChange change = expect(permissionsEvent(), c -> c.getEntity().hasContentPermission()
                && c.getEntity().getEntityId().startsWith(owner)
                && c.getEntity().getContentPermission().getPrincipal().getPrincipalId()
                        .equals(page.getAuthorId()));
        return event(change) + " -> " + change.getEntity().getContentPermission().getOperation()
                + " permission for the author";
    }

    private String unrestrictPage() {
        stub().setRestrictionState(SetRestrictionStateRequest.newBuilder().setContent(pageRef())
                .setState(ContentRestrictionState.CONTENT_RESTRICTION_STATE_OPEN).build());
        stub().deleteContentPermissions(DeleteContentPermissionsRequest.newBuilder()
                .setContent(pageRef()).setAllowSelfLockout(true).build());
        return "open; direct permissions removed";
    }

    private String unrestrictedEvent() throws Exception {
        String state = "page:" + page.getId() + "/restriction-state";
        String owner = "page:" + page.getId() + "/permission/";
        Predicate<ConfluenceChange> isState = c -> c.getEntity().getEntityId().equals(state);
        Drained drained = drainUntil(permissionsEvent(), isState);
        ConfluenceChange restriction = find(drained, permissionsEvent(), isState);
        String eventId = restriction.getEvent().getEventId();
        for (WatchChangesResponse message : drained.streamed()) {
            if (message.hasChange() && message.getChange().getEvent().getEventId().equals(eventId)) {
                check(!message.getChange().getEntity().getEntityId().startsWith(owner),
                        "a direct permission is still streamed: "
                                + message.getChange().getEntity().getEntityId());
            }
        }
        return event(restriction) + " -> restriction state "
                + restriction.getEntity().getRestrictionState().getState() + ", no direct permission";
    }

    private String completeTask() throws Exception {
        Task task = null;
        for (int attempt = 0; attempt < 15 && task == null; attempt++) {
            Iterator<ListTasksResponse> tasks = stub().listTasks(ListTasksRequest.newBuilder()
                    .addPageIds(page.getId()).build());
            if (tasks.hasNext()) {
                task = tasks.next().getTask();
            } else {
                events.pause(Duration.ofSeconds(2));
            }
        }
        check(task != null, "the page has no task");
        stub().updateTask(UpdateTaskRequest.newBuilder().setId(task.getId())
                .setStatus(TaskStatus.TASK_STATUS_COMPLETE).build());
        return "task " + task.getId() + " complete";
    }

    private String taskEvent() throws Exception {
        Predicate<ConfluenceEvent> match = event ->
                event.getObject() == ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_TASK
                        && event.getAction() == ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_UPDATED;
        ConfluenceChange change = expect(match, c -> c.getEntity().hasPage()
                && c.getEntity().getEntityId().equals(page.getId()));
        return event(change) + " -> page upsert";
    }

    private String createBlog() {
        blog = stub().createBlogPost(CreateBlogPostRequest.newBuilder().setSpaceId(space.getId())
                .setTitle(run).setBody(storage("<p>Change feed blog " + run + ".</p>")).build())
                .getBlogPost();
        String blogId = blog.getId();
        cleanups.add(() -> purgeBlog(blogId));
        return "blog post " + blog.getId();
    }

    private String updateBlog() {
        blog = stub().updateBlogPost(UpdateBlogPostRequest.newBuilder().setId(blog.getId())
                .setTitle(run).setBody(storage("<p>Change feed blog " + run + ", edited.</p>"))
                .setExpectedVersion(blog.getVersion().getNumber()).build()).getBlogPost();
        return "version " + blog.getVersion().getNumber();
    }

    private String blogComment() {
        comment = stub().createFooterComment(CreateFooterCommentRequest.newBuilder()
                .setBlogPostId(blog.getId()).setBody(storage("<p>blog comment " + run + "</p>"))
                .build()).getComment();
        return "comment " + comment.getId();
    }

    private String blogCommentEvent() throws Exception {
        ConfluenceChange change = expect(event -> isAbout(event,
                ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_COMMENT,
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED, null)
                && onContainer(event, blog.getId(), comment.getId()),
                c -> c.getEntity().hasComment() && c.getEntity().getEntityId().equals(comment.getId()));
        return event(change) + " -> comment upsert from the blog post re-read";
    }

    private String deleteBlog(boolean purge) {
        stub().deleteBlogPost(DeleteBlogPostRequest.newBuilder().setId(blog.getId()).setPurge(purge)
                .build());
        return purge ? "purged" : "trashed";
    }

    /**
     * Queues {@link #BACKLOG} updates of one page, then drains until the
     * last is streamed: the first take answers the 100-event cap, later takes
     * the rest, and each drain re-reads the page once. Forge may send fewer
     * events than edits.
     */
    private String backlog() throws Exception {
        backlogPage = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                .setTitle(run + " backlog").setBody(storage("<p>backlog 0</p>")).build()).getPage();
        String id = backlogPage.getId();
        cleanups.add(() -> purgePage(id));
        for (int i = 1; i <= BACKLOG; i++) {
            backlogPage = update(backlogPage, UpdatePageRequest.newBuilder().setTitle(run + " backlog")
                    .setBody(storage("<p>backlog " + i + "</p>")));
        }
        int last = backlogPage.getVersion().getNumber();
        events.awaitQueued(ForgeEventQueue.DEFAULT_TAKE);
        List<Integer> sizes = new ArrayList<>();
        int updates = 0;
        int upserts = 0;
        boolean done = false;
        boolean drained = false;
        for (int round = 0; round < MAX_BACKLOG_DRAINS && !drained; round++) {
            service.drainNow();
            List<ForgeEventQueue.Queued> taken = awaitDrain();
            sizes.add(taken.size());
            int here = 0;
            for (ForgeEventQueue.Queued queued : taken) {
                ConfluenceEvent event = queued.event();
                if (event.getContentId().equals(id)
                        && event.getAction() == ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_UPDATED) {
                    here++;
                    done |= event.getVersionNumber() == last;
                }
            }
            updates += here;
            List<WatchChangesResponse> streamed = new ArrayList<>();
            watched.drainTo(streamed);
            long pageUpserts = streamed.stream().filter(m -> m.hasChange()
                    && m.getChange().getEntity().hasPage()
                    && m.getChange().getEntity().getEntityId().equals(id)
                    && m.getChange().getEvent().getAction()
                            == ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_UPDATED).count();
            check(pageUpserts == (here > 0 ? 1 : 0), "drain " + (round + 1) + " took " + here
                    + " update event(s) of the page but streamed " + pageUpserts + " page upsert(s)");
            upserts += (int) pageUpserts;
            drained = done && here == 0;
            if (!drained && taken.isEmpty()) {
                events.pause(Duration.ofSeconds(5));
            }
        }
        check(done, "the last update (version " + last + ") was not drained; takes " + sizes);
        check(sizes.getFirst() == ForgeEventQueue.DEFAULT_TAKE, "the first take answered "
                + sizes.getFirst() + " events");
        check(updates > ForgeEventQueue.DEFAULT_TAKE, "only " + updates
                + " update events were queued for " + BACKLOG + " edits; takes " + sizes);
        check(upserts >= 2, "the backlog was read in " + upserts + " drain(s); takes " + sizes);
        return updates + " update events for " + BACKLOG + " edits over takes " + sizes + ", "
                + upserts + " page re-read(s)";
    }

    private String trashPage() {
        stub().deletePage(DeletePageRequest.newBuilder().setId(page.getId()).build());
        return "trashed";
    }

    private String restorePage() throws Skip, InterruptedException {
        try {
            page = stub().getPage(GetPageRequest.newBuilder().setId(page.getId()).build()).getPage();
            page = update(page, UpdatePageRequest.newBuilder().setTitle(run)
                    .setStatus(ContentStatus.CONTENT_STATUS_CURRENT)
                    .setBody(storage(pageBody(", restored"))));
        } catch (StatusRuntimeException e) {
            throw new Skip("the site refused the restore: " + e.getStatus().getCode() + " "
                    + e.getStatus().getDescription());
        }
        restored = true;
        return "restored at version " + page.getVersion().getNumber();
    }

    private String restoredEvent() throws Exception {
        if (!restored) {
            throw new Skip("the page was not restored");
        }
        return contentEvent(ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_PAGE,
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_RESTORED, 1);
    }

    private String purgePage() {
        if (restored) {
            stub().deletePage(DeletePageRequest.newBuilder().setId(page.getId()).build());
        }
        stub().deletePage(DeletePageRequest.newBuilder().setId(page.getId()).setPurge(true).build());
        return restored ? "trashed and purged" : "purged";
    }

    private String createSpace() {
        ownSpace = stub().createSpace(CreateSpaceRequest.newBuilder().setName(run).setKey(ownSpaceKey)
                .setDescription("Created by the live change run " + run).build()).getSpace();
        cleanups.add(() -> {
            if (ownSpace != null) {
                try {
                    stub().deleteSpace(DeleteSpaceRequest.newBuilder().setKey(ownSpaceKey).build());
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                        throw e;
                    }
                }
            }
        });
        return "space " + ownSpace.getKey();
    }

    private String spaceCreatedEvent() throws Exception {
        ConfluenceChange change = expect(event -> isSpace(event,
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED),
                c -> c.getEntity().hasSpace() && c.getEntity().getEntityId().equals(ownSpace.getId())
                        && c.getOperation() == ChangeOperation.CHANGE_OPERATION_UPSERT);
        check(change.getEntity().getSpace().getKey().equals(ownSpaceKey),
                "re-read key " + change.getEntity().getSpace().getKey());
        return event(change) + " -> space upsert";
    }

    private String deleteSpace() {
        String task = stub().deleteSpace(DeleteSpaceRequest.newBuilder().setKey(ownSpaceKey).build())
                .getTaskId();
        return task.isEmpty() ? "deleted" : "delete task answered";
    }

    private String spaceDeletedEvent() throws Exception {
        ConfluenceChange change = expect(event -> isSpace(event,
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_DELETED),
                c -> c.getEntity().getEntityId().equals(ownSpace.getId())
                        && c.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE);
        ownSpace = null;
        return event(change) + " -> space delete";
    }

    private String watchStop() throws Exception {
        watch.cancel(null);
        Instant until = Instant.now().plus(Duration.ofSeconds(10));
        while (watched.error() == null && Instant.now().isBefore(until)) {
            TimeUnit.MILLISECONDS.sleep(50);
        }
        check(watched.error() != null, "the stream did not end on cancel");
        return "stream ended: " + Status.fromThrowable(watched.error()).getCode();
    }

    // --- copies and the archive ------------------------------------------------------------

    private String copyPage() {
        Page copy = stub().copyPage(CopyPageRequest.newBuilder().setId(page.getId())
                .setDestination(CopyPageDestination.newBuilder().setParentPageId(parent.getId()))
                .setPageTitle(run + " copy").build()).getPage();
        check(!copy.getId().isEmpty(), "the copy answered no id");
        copyId = copy.getId();
        String id = copyId;
        cleanups.add(() -> purgePage(id));
        check(!copyId.equals(page.getId()), "the copy has the id of its source");
        Page read = stub().getPage(GetPageRequest.newBuilder().setId(copyId).build()).getPage();
        check(read.getTitle().equals(run + " copy"), "the title read back is " + read.getTitle());
        check(read.getParentId().equals(parent.getId()), "the parent read back is " + read.getParentId());
        check(copy.getTitle().equals(read.getTitle()) && copy.getParentId().equals(read.getParentId())
                && copy.getSpaceId().equals(space.getId()), "the answer differs from the page read back");
        return "copy " + copyId + " under the parent";
    }

    private String copiedEvent() throws Exception {
        if (copyId == null) {
            throw new Skip("the page was not copied");
        }
        ConfluenceChange change = expect(event -> event.getObject()
                == ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_PAGE
                && event.getAction() == ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_COPIED
                && (event.getContentId().equals(copyId) || event.getOriginContentId().equals(page.getId())),
                c -> c.getEntity().hasPage() && c.getEntity().getEntityId().equals(copyId)
                        && c.getOperation() == ChangeOperation.CHANGE_OPERATION_UPSERT);
        return event(change) + " -> page upsert of the copy, origin "
                + (change.getEvent().getOriginContentId().equals(page.getId()) ? "the page" : "not named");
    }

    private String archiveCopy() throws Exception {
        if (copyId == null) {
            throw new Skip("the page was not copied");
        }
        String task;
        try {
            task = stub().archivePages(ArchivePagesRequest.newBuilder().addPageIds(copyId).build())
                    .getTaskId();
        } catch (StatusRuntimeException e) {
            throw new Skip("the site refused the archive: " + e.getStatus().getCode() + " "
                    + e.getStatus().getDescription());
        }
        awaitTask(task);
        archived = true;
        ContentStatus status = stub().getPage(GetPageRequest.newBuilder().setId(copyId).build())
                .getPage().getStatus();
        check(status == ContentStatus.CONTENT_STATUS_ARCHIVED, "the status read back is " + status);
        return "archive task finished; the copy reads back archived";
    }

    private String archivedEvent() throws Exception {
        if (!archived) {
            throw new Skip("the copy was not archived");
        }
        ConfluenceChange change = expect(event -> isAbout(event,
                ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_PAGE,
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_ARCHIVED, copyId),
                c -> c.getEntity().getEntityId().equals(copyId)
                        && (c.getEntity().hasPage() || c.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE));
        String state = change.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE ? "delete"
                : "page upsert, status " + change.getEntity().getPage().getStatus();
        return event(change) + " -> " + state;
    }

    /** An archived page reads back, but an update of it answers not found. */
    private String updateArchived() throws Exception {
        if (!archived) {
            throw new Skip("the copy was not archived");
        }
        Page copy = stub().getPage(GetPageRequest.newBuilder().setId(copyId).build()).getPage();
        try {
            stub().updatePage(UpdatePageRequest.newBuilder().setId(copyId).setTitle(run + " copy")
                    .setStatus(ContentStatus.CONTENT_STATUS_CURRENT)
                    .setBody(storage(pageBody(", out of the archive")))
                    .setExpectedVersion(copy.getVersion().getNumber()).build());
        } catch (StatusRuntimeException e) {
            check(e.getStatus().getCode() == Status.Code.NOT_FOUND,
                    "the update answered " + e.getStatus().getCode());
            return "NOT_FOUND";
        }
        throw new AssertionError("the update of an archived page was accepted");
    }

    private String copyHierarchy() throws Exception {
        Page root = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                .setParentId(parent.getId()).setTitle(run + " tree")
                .setBody(storage("<p>Tree of " + run + ".</p>")).build()).getPage();
        String rootId = root.getId();
        cleanups.add(() -> purgePage(rootId));
        Page leaf = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                .setParentId(rootId).setTitle(run + " tree leaf")
                .setBody(storage("<p>Leaf of " + run + ".</p>")).build()).getPage();
        String leafId = leaf.getId();
        cleanups.add(() -> purgePage(leafId));
        String task = stub().copyPageHierarchy(CopyPageHierarchyRequest.newBuilder().setId(rootId)
                .setDestinationPageId(parent.getId())
                .setTitleOptions(CopyPageTitleOptions.newBuilder().setPrefix(TREE_COPY_PREFIX)).build())
                .getTaskId();
        awaitTask(task);
        String leafCopy = null;
        for (int attempt = 0; attempt < 15 && (treeCopyId == null || leafCopy == null); attempt++) {
            if (attempt > 0) {
                events.pause(Duration.ofSeconds(2));
            }
            Iterator<ListDescendantsResponse> descendants = stub().listDescendants(
                    ListDescendantsRequest.newBuilder().setRoot(ContentRef.newBuilder()
                            .setType(ContentType.CONTENT_TYPE_PAGE).setId(parent.getId())).build());
            while (descendants.hasNext()) {
                ContentTreeEntry entry = descendants.next().getEntry();
                if (entry.getTitle().equals(TREE_COPY_PREFIX + run + " tree")) {
                    treeCopyId = entry.getId();
                } else if (entry.getTitle().equals(TREE_COPY_PREFIX + run + " tree leaf")) {
                    leafCopy = entry.getId();
                    check(treeCopyId == null || entry.getParentId().equals(treeCopyId),
                            "the copied leaf is under " + entry.getParentId());
                }
            }
        }
        for (String id : new String[] {treeCopyId, leafCopy}) {
            if (id != null) {
                cleanups.add(() -> purgePage(id));
            }
        }
        check(treeCopyId != null, "no copy of the tree is under the parent");
        check(leafCopy != null, "the leaf was not copied with the tree");
        return "copy task finished; tree copy " + treeCopyId + " with its leaf " + leafCopy;
    }

    /** A page copied with its tree arrives as a copied or a created page event. */
    private String hierarchyCopiedEvent() throws Exception {
        if (treeCopyId == null) {
            throw new Skip("the tree was not copied");
        }
        ConfluenceChange change = expect(event -> event.getObject()
                == ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_PAGE
                && event.getContentId().equals(treeCopyId)
                && (event.getAction() == ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_COPIED
                        || event.getAction() == ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED),
                c -> c.getEntity().hasPage() && c.getEntity().getEntityId().equals(treeCopyId)
                        && c.getOperation() == ChangeOperation.CHANGE_OPERATION_UPSERT);
        check(change.getEntity().getPage().getTitle().equals(TREE_COPY_PREFIX + run + " tree"),
                "re-read title differs");
        return event(change) + " -> page upsert of the tree copy";
    }

    /** Polls a long-running task until it ends without an error. */
    private void awaitTask(String taskId) throws InterruptedException {
        check(!taskId.isEmpty(), "no task id was answered");
        for (int attempt = 0; attempt < 60; attempt++) {
            LongRunningTask task = stub().getLongRunningTask(GetLongRunningTaskRequest.newBuilder()
                    .setTaskId(taskId).build()).getTask();
            if (task.getFinished()) {
                check(task.getSuccessful(), "the task ended with errors " + task.getErrorsList());
                return;
            }
            events.pause(Duration.ofSeconds(2));
        }
        throw new AssertionError("the task did not finish in two minutes");
    }

    // --- restrictions on folders, whiteboards and databases --------------------------------

    private ContentRef ref(Item item) {
        return ContentRef.newBuilder().setType(item.type).setId(items.get(item)).build();
    }

    private String restrictItem(Item item) {
        String title = run + " " + item.kind;
        String id = switch (item) {
            case FOLDER -> stub().createFolder(CreateFolderRequest.newBuilder().setSpaceId(space.getId())
                    .setParentId(parent.getId()).setTitle(title).build()).getFolder().getId();
            case WHITEBOARD -> stub().createWhiteboard(CreateWhiteboardRequest.newBuilder()
                    .setSpaceId(space.getId()).setParentId(parent.getId()).setTitle(title).build())
                    .getWhiteboard().getId();
            case DATABASE -> stub().createDatabase(CreateDatabaseRequest.newBuilder()
                    .setSpaceId(space.getId()).setParentId(parent.getId()).setTitle(title).build())
                    .getDatabase().getId();
        };
        items.put(item, id);
        cleanups.add(() -> {
            try {
                switch (item) {
                    case FOLDER -> stub().deleteFolder(DeleteFolderRequest.newBuilder().setId(id).build());
                    case WHITEBOARD -> stub().deleteWhiteboard(DeleteWhiteboardRequest.newBuilder()
                            .setId(id).build());
                    case DATABASE -> stub().deleteDatabase(DeleteDatabaseRequest.newBuilder().setId(id)
                            .build());
                }
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                    throw e;
                }
            }
        });
        stub().addContentPermissions(AddContentPermissionsRequest.newBuilder().setContent(ref(item))
                .addPermissions(ContentPermissionGrant.newBuilder()
                        .setOperation(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_UPDATE)
                        .addPrincipals(self()))
                .build());
        stub().setRestrictionState(SetRestrictionStateRequest.newBuilder().setContent(ref(item))
                .setState(ContentRestrictionState.CONTENT_RESTRICTION_STATE_EDIT_RESTRICTED).build());
        restricted.add(item);
        return item.kind + " " + id + ", update permission for the author; edit restricted";
    }

    private Predicate<ConfluenceEvent> itemEvent(Item item) {
        return event -> isAbout(event, item.object,
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_PERMISSIONS_UPDATED, items.get(item));
    }

    private String itemRestrictedEvent(Item item) throws Exception {
        if (!restricted.contains(item)) {
            throw new Skip("the " + item.kind + " was not restricted");
        }
        String owner = item.kind + ":" + items.get(item) + "/permission/";
        ConfluenceChange change = expect(itemEvent(item), c -> c.getEntity().hasContentPermission()
                && c.getEntity().getEntityId().startsWith(owner)
                && c.getEntity().getContentPermission().getPrincipal().getPrincipalId()
                        .equals(page.getAuthorId()));
        return event(change) + " -> " + change.getEntity().getContentPermission().getOperation()
                + " permission for the author";
    }

    private String unrestrictItem(Item item) throws Exception {
        if (!restricted.contains(item)) {
            throw new Skip("the " + item.kind + " was not restricted");
        }
        stub().setRestrictionState(SetRestrictionStateRequest.newBuilder().setContent(ref(item))
                .setState(ContentRestrictionState.CONTENT_RESTRICTION_STATE_OPEN).build());
        stub().deleteContentPermissions(DeleteContentPermissionsRequest.newBuilder()
                .setContent(ref(item)).setAllowSelfLockout(true).build());
        unrestricted.add(item);
        return "open; direct permissions removed";
    }

    private String itemUnrestrictedEvent(Item item) throws Exception {
        if (!unrestricted.contains(item)) {
            throw new Skip("the " + item.kind + " restrictions were not removed");
        }
        String state = item.kind + ":" + items.get(item) + "/restriction-state";
        String owner = item.kind + ":" + items.get(item) + "/permission/";
        Predicate<ConfluenceChange> isState = c -> c.getEntity().getEntityId().equals(state);
        Drained drained = drainUntil(itemEvent(item), isState);
        ConfluenceChange restriction = find(drained, itemEvent(item), isState);
        String eventId = restriction.getEvent().getEventId();
        for (WatchChangesResponse message : drained.streamed()) {
            if (message.hasChange() && message.getChange().getEvent().getEventId().equals(eventId)) {
                check(!message.getChange().getEntity().getEntityId().startsWith(owner),
                        "a direct permission is still streamed: "
                                + message.getChange().getEntity().getEntityId());
            }
        }
        return event(restriction) + " -> restriction state "
                + restriction.getEntity().getRestrictionState().getState() + ", no direct permission";
    }

    // --- groups ----------------------------------------------------------------------------

    private String createGroup() {
        Group group = stub().createGroup(CreateGroupRequest.newBuilder().setName(groupName).build())
                .getGroup();
        check(!group.getId().isEmpty(), "the group answered no id");
        groupId = group.getId();
        String id = groupId;
        cleanups.add(() -> {
            if (!groupDeleted) {
                try {
                    stub().deleteGroup(DeleteGroupRequest.newBuilder().setGroupId(id).build());
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() != Status.Code.NOT_FOUND
                            && e.getStatus().getCode() != Status.Code.INVALID_ARGUMENT) {
                        throw e;
                    }
                }
            }
        });
        check(group.getName().equals(groupName), "the group is named " + group.getName());
        Group read = stub().getGroup(GetGroupRequest.newBuilder().setGroupId(groupId).build()).getGroup();
        check(read.getName().equals(groupName), "the name read back is " + read.getName());
        check(read.getManagedBy() == group.getManagedBy() && read.getUsageType() == group.getUsageType(),
                "the answer differs from the group read back");
        int listed = 0;
        boolean found = false;
        Iterator<ListGroupsResponse> groups = stub().listGroups(ListGroupsRequest.getDefaultInstance());
        while (groups.hasNext()) {
            listed++;
            found |= groups.next().getGroup().getId().equals(groupId);
        }
        check(found, "ListGroups answered " + listed + " group(s) without the new one");
        return "group created, managed by " + read.getManagedBy() + "; read back and listed among "
                + listed + " group(s)";
    }

    private Predicate<ConfluenceEvent> groupEvent(ConfluenceEventAction action) {
        return event -> event.getObject() == ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_GROUP
                && event.getAction() == action && event.getContentId().equals(groupId);
    }

    /** A created group is re-read; Forge does not always deliver the event. */
    private String groupCreatedEvent() throws Exception {
        Predicate<ConfluenceEvent> match = groupEvent(
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED);
        try {
            events.await(match);
        } catch (AssertionError e) {
            throw new Lost("Forge delivered no group created event: " + e.getMessage());
        }
        Predicate<ConfluenceChange> wanted = c -> c.getEntity().hasGroup()
                && c.getEntity().getEntityId().equals(groupId);
        ConfluenceChange change = find(drain(match, wanted), match, wanted);
        check(change.getOperation() == ChangeOperation.CHANGE_OPERATION_UPSERT,
                "operation " + change.getOperation());
        check(change.getEntity().getGroup().getName().equals(groupName), "re-read name differs");
        check(change.getEntity().getSecurityCompleteness() == AclCompleteness.ACL_COMPLETENESS_COMPLETE,
                "members are " + change.getEntity().getSecurityCompleteness());
        return event(change) + " -> group upsert with its members read";
    }

    private List<String> members() {
        List<String> ids = new ArrayList<>();
        stub().listGroupMembers(ListGroupMembersRequest.newBuilder().setGroupId(groupId).build())
                .forEachRemaining(member -> ids.add(member.getUser().getAccountId()));
        return ids;
    }

    private String groupMembers() throws Exception {
        if (!memberSteps) {
            throw new Skip("not enabled; set " + ENV_GROUP_MEMBER + "=1");
        }
        String account = page.getAuthorId();
        stub().addGroupMember(AddGroupMemberRequest.newBuilder().setGroupId(groupId)
                .setAccountId(account).build());
        List<String> added = members();
        check(added.equals(List.of(account)), "after the add the group has " + added.size()
                + " member(s)");
        stub().removeGroupMember(RemoveGroupMemberRequest.newBuilder().setGroupId(groupId)
                .setAccountId(account).build());
        List<String> left = members();
        check(left.isEmpty(), "after the remove the group has " + left.size() + " member(s)");
        return "the caller added, listed, removed and gone from the list";
    }

    /** Deletes the group; it can still read back for a while, so the read is retried for 30 seconds. */
    private String deleteGroup() throws InterruptedException {
        stub().deleteGroup(DeleteGroupRequest.newBuilder().setGroupId(groupId).build());
        groupDeleted = true;
        for (int attempt = 0; attempt < GROUP_GONE_READS; attempt++) {
            if (attempt > 0) {
                events.pause(Duration.ofSeconds(2));
            }
            try {
                stub().getGroup(GetGroupRequest.newBuilder().setGroupId(groupId).build());
            } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();
                check(code == Status.Code.NOT_FOUND || code == Status.Code.INVALID_ARGUMENT,
                        "GetGroup of the deleted group answered " + code);
                return "group deleted; GetGroup answers " + code + " after " + (attempt + 1)
                        + " read(s)";
            }
        }
        throw new AssertionError("the deleted group still reads back after " + GROUP_GONE_READS
                + " reads");
    }

    private String groupDeletedEvent() throws Exception {
        Predicate<ConfluenceEvent> match = groupEvent(
                ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_DELETED);
        Predicate<ConfluenceChange> wanted = c -> c.getEntity().hasGroup()
                && c.getEntity().getEntityId().equals(groupId)
                && c.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE;
        Drained drained = drainUntil(match, wanted);
        ConfluenceChange change = find(drained, match, wanted);
        check(change.getEntity().getGroup().getName().equals(groupName), "the delete names "
                + change.getEntity().getGroup().getName());
        String late = "";
        for (ForgeEventQueue.Queued queued : drained.taken()) {
            if (groupEvent(ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED).test(queued.event())) {
                late = "; the created event was queued with it, " + lag(queued) + " after it happened";
            }
        }
        return event(change) + " -> group delete" + late;
    }

    // --- event checks ----------------------------------------------------------------------

    /** A page or blog post event streamed as a re-read upsert at {@code minVersion} or later. */
    private String contentEvent(ConfluenceEventObject object, ConfluenceEventAction action,
            int minVersion) throws Exception {
        boolean isBlog = object == ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_BLOG_POST;
        String id = isBlog ? blog.getId() : page.getId();
        Predicate<ConfluenceEvent> match = event -> isAbout(event, object, action, id);
        Predicate<ConfluenceChange> wanted = c -> (isBlog ? c.getEntity().hasBlogPost()
                : c.getEntity().hasPage()) && c.getEntity().getEntityId().equals(id);
        Drained drained = drainUntil(match, wanted);
        if (!isBlog && action == ConfluenceEventAction.CONFLUENCE_EVENT_ACTION_CREATED) {
            createdDrain = drained;
        }
        ConfluenceChange change = find(drained, match, wanted);
        check(change.getOperation() == ChangeOperation.CHANGE_OPERATION_UPSERT,
                "operation " + change.getOperation());
        int version = isBlog ? change.getEntity().getBlogPost().getVersion().getNumber()
                : change.getEntity().getPage().getVersion().getNumber();
        String title = isBlog ? change.getEntity().getBlogPost().getTitle()
                : change.getEntity().getPage().getTitle();
        check(version >= minVersion, "re-read version " + version);
        check(title.equals(run), "re-read title differs");
        return event(change) + " -> " + (isBlog ? "blog post" : "page") + " upsert, version " + version;
    }

    /** A trashed or deleted page or blog post is streamed as a delete. */
    private String goneEvent(ConfluenceEventObject object, ConfluenceEventAction action)
            throws Exception {
        String id = object == ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_BLOG_POST
                ? blog.getId() : page.getId();
        ConfluenceChange change = expect(event -> isAbout(event, object, action, id),
                c -> c.getEntity().getEntityId().equals(id)
                        && c.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE);
        return event(change) + " -> delete";
    }

    private static boolean isAbout(ConfluenceEvent event, ConfluenceEventObject object,
            ConfluenceEventAction action, String id) {
        return event.getObject() == object && event.getAction() == action
                && (id == null || event.getContentId().equals(id));
    }

    private static boolean onContainer(ConfluenceEvent event, String container, String id) {
        return event.getContainerId().equals(container) || event.getContentId().equals(id);
    }

    private boolean isSpace(ConfluenceEvent event, ConfluenceEventAction action) {
        return event.getObject() == ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_SPACE
                && event.getAction() == action && event.getSpaceKey().equals(ownSpaceKey);
    }

    /** Waits for the event, drains, and answers the first matching change streamed. */
    private ConfluenceChange expect(Predicate<ConfluenceEvent> event,
            Predicate<ConfluenceChange> wanted) throws Exception {
        return find(drainUntil(event, wanted), event, wanted);
    }

    /**
     * Waits for the event, then drains until a change matching {@code wanted}
     * and stamped with a matching event is streamed, at most three drains.
     */
    private Drained drainUntil(Predicate<ConfluenceEvent> event, Predicate<ConfluenceChange> wanted)
            throws Exception {
        events.await(event);
        return drain(event, wanted);
    }

    /**
     * Drains until a change matching {@code wanted} and stamped with a
     * matching event is streamed, at most three drains.
     */
    private Drained drain(Predicate<ConfluenceEvent> event, Predicate<ConfluenceChange> wanted)
            throws Exception {
        List<ForgeEventQueue.Queued> taken = new ArrayList<>();
        List<WatchChangesResponse> streamed = new ArrayList<>();
        for (int round = 0; round < 3; round++) {
            service.drainNow();
            taken.addAll(awaitDrain());
            watched.drainTo(streamed);
            Drained drained = new Drained(taken, streamed);
            if (findOrNull(drained, event, wanted) != null) {
                return drained;
            }
        }
        List<String> seen = new ArrayList<>();
        for (WatchChangesResponse message : streamed) {
            if (message.hasChange()) {
                seen.add(message.getChange().getEntity().getEntityId() + " "
                        + message.getChange().getEvent().getEventType());
            } else if (message.hasUnresolvedEvent()) {
                ConfluenceEvent unresolved = message.getUnresolvedEvent();
                seen.add("unresolved " + unresolved.getEventType() + " content="
                        + unresolved.getContentId() + " type=" + unresolved.getContentType()
                        + " container=" + unresolved.getContainerId() + "/"
                        + unresolved.getContainerType());
            }
        }
        throw new AssertionError("the drains took " + taken.size() + " event(s) but streamed no"
                + " matching change; streamed: " + seen);
    }

    private static ConfluenceChange find(Drained drained, Predicate<ConfluenceEvent> event,
            Predicate<ConfluenceChange> wanted) {
        ConfluenceChange change = findOrNull(drained, event, wanted);
        check(change != null, "no matching change was streamed");
        check(change.getSource() == ChangeSource.CHANGE_SOURCE_WEBHOOK, "source " + change.getSource());
        return change;
    }

    private static ConfluenceChange findOrNull(Drained drained, Predicate<ConfluenceEvent> event,
            Predicate<ConfluenceChange> wanted) {
        for (WatchChangesResponse message : drained.streamed()) {
            if (message.hasChange() && wanted.test(message.getChange())
                    && event.test(message.getChange().getEvent())) {
                return message.getChange();
            }
        }
        return null;
    }

    private List<ForgeEventQueue.Queued> awaitDrain() throws Exception {
        List<ForgeEventQueue.Queued> drained = watched.awaitDrain(STREAM_WAIT);
        drained.forEach(queued -> allTaken.putIfAbsent(queued.key(), queued));
        return drained;
    }

    // --- delivery --------------------------------------------------------------------------

    /**
     * How long Forge took to queue the events the run drained, one line per
     * event type: the count and the shortest, median and longest delay.
     */
    List<String> deliveries() {
        Map<String, List<Duration>> byType = new TreeMap<>();
        for (ForgeEventQueue.Queued queued : allTaken.values()) {
            Duration delay = delay(queued);
            if (delay != null) {
                byType.computeIfAbsent(queued.event().getEventType(), type -> new ArrayList<>())
                        .add(delay);
            }
        }
        List<String> lines = new ArrayList<>();
        byType.forEach((type, delays) -> {
            delays.sort(null);
            lines.add(type + ": " + delays.size() + " event(s), queued " + seconds(delays.getFirst())
                    + " / " + seconds(delays.get(delays.size() / 2)) + " / " + seconds(delays.getLast())
                    + " after they happened (shortest / median / longest)");
        });
        return lines;
    }

    /** The time between an event and the moment the Forge app queued it; null when either is unknown. */
    static Duration delay(ForgeEventQueue.Queued queued) {
        String key = queued.key();
        int start = key.indexOf(':');
        int end = key.indexOf(':', start + 1);
        if (start < 0 || end < 0 || !queued.event().hasOccurredAt()) {
            return null;
        }
        try {
            Instant stored = Instant.ofEpochMilli(Long.parseLong(key.substring(start + 1, end)));
            Instant occurred = Instant.ofEpochSecond(queued.event().getOccurredAt().getSeconds(),
                    queued.event().getOccurredAt().getNanos());
            return Duration.between(occurred, stored);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String lag(ForgeEventQueue.Queued queued) {
        Duration delay = delay(queued);
        return delay == null ? "an unknown time" : seconds(delay);
    }

    private static String seconds(Duration delay) {
        return String.format(Locale.ROOT, "%.1f s", delay.toMillis() / 1000.0);
    }

    // --- helpers ---------------------------------------------------------------------------

    /** Updates a page; a conflict re-reads the version and retries, at most five times. */
    private Page update(Page current, UpdatePageRequest.Builder request) throws InterruptedException {
        int version = current.getVersion().getNumber();
        for (int attempt = 1; ; attempt++) {
            try {
                return stub().updatePage(request.setId(current.getId()).setExpectedVersion(version)
                        .build()).getPage();
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() != Status.Code.ABORTED || attempt == 5) {
                    throw e;
                }
                events.pause(Duration.ofSeconds(attempt));
                version = stub().getPage(GetPageRequest.newBuilder().setId(current.getId()).build())
                        .getPage().getVersion().getNumber();
            }
        }
    }

    private ContentPermissionPrincipal self() {
        return ContentPermissionPrincipal.newBuilder()
                .setPrincipalType(ContentPermissionPrincipalType.CONTENT_PERMISSION_PRINCIPAL_TYPE_USER)
                .setPrincipalId(page.getAuthorId()).build();
    }

    private String pageBody(String suffix) {
        return "<p>Change feed run " + run + suffix + ".</p><ac:task-list><ac:task><ac:task-id>1"
                + "</ac:task-id><ac:task-status>incomplete</ac:task-status><ac:task-body>check the"
                + " change feed</ac:task-body></ac:task></ac:task-list>";
    }

    private void purgePage(String id) {
        try {
            stub().deletePage(DeletePageRequest.newBuilder().setId(id).build());
        } catch (StatusRuntimeException e) {
            // Already trashed or gone.
        }
        try {
            stub().deletePage(DeletePageRequest.newBuilder().setId(id).setPurge(true).build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                throw e;
            }
        }
    }

    private void purgeBlog(String id) {
        try {
            stub().deleteBlogPost(DeleteBlogPostRequest.newBuilder().setId(id).build());
        } catch (StatusRuntimeException e) {
            // Already trashed or gone.
        }
        try {
            stub().deleteBlogPost(DeleteBlogPostRequest.newBuilder().setId(id).setPurge(true).build());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                throw e;
            }
        }
    }

    private Attachment upload(String contentId, String fileName, byte[] bytes) throws Exception {
        List<UploadAttachmentResponse> answers = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<UploadAttachmentRequest> frames = ConfluenceServiceGrpc.newStub(channel)
                .uploadAttachment(new StreamObserver<>() {
                    @Override
                    public void onNext(UploadAttachmentResponse value) {
                        synchronized (answers) {
                            answers.add(value);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        failure.set(t);
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        done.countDown();
                    }
                });
        frames.onNext(UploadAttachmentRequest.newBuilder().setHeader(UploadAttachmentHeader.newBuilder()
                .setContentId(contentId).setFileName(fileName).setMediaType("text/plain")
                .setComment("live change run").setMinorEdit(true)).build());
        frames.onNext(UploadAttachmentRequest.newBuilder().setChunk(ByteString.copyFrom(bytes)).build());
        frames.onNext(UploadAttachmentRequest.newBuilder()
                .setEnd(UploadAttachmentEnd.getDefaultInstance()).build());
        frames.onCompleted();
        check(done.await(2, TimeUnit.MINUTES), "the upload did not finish in time");
        if (failure.get() != null) {
            throw Status.fromThrowable(failure.get()).asRuntimeException();
        }
        synchronized (answers) {
            check(!answers.isEmpty() && answers.getLast().hasAttachment(),
                    "the upload did not end with the attachment");
            return answers.getLast().getAttachment();
        }
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
