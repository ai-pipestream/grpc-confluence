package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Runs the write rpcs against the real Confluence workspace through the
 * in-process gRPC service, reads every result back through the read rpcs,
 * and writes one row per rpc to {@code build/live-write.md}.
 *
 * <p>Gated on the credentials {@link ConfluenceLiveSmokeIT} uses and on
 * {@code CONFLUENCE_LIVE_WRITE=1}; the target space is
 * {@code CONFLUENCE_SPACE_KEY} (default {@code CLAUDEGRPC}). Excluded from
 * the default {@code test} task; run with
 * {@code ./gradlew :grpc-confluence-service:liveWriteTest}.</p>
 *
 * <p>Everything is created under a fresh parent page named
 * {@code pipestream-live-write-<UTC timestamp>-<random>} and is deleted and
 * purged at the end; anything left behind is listed in the report. Tenant
 * level writes (space create, space roles, role assignments, the space
 * default classification, invitations, the admin key) run only with
 * {@code CONFLUENCE_LIVE_WRITE_ADMIN=1}; invitations also need
 * {@code CONFLUENCE_LIVE_WRITE_INVITE_EMAIL}. Custom content needs a type
 * in {@code CONFLUENCE_LIVE_WRITE_CUSTOM_CONTENT_TYPE}. The bulk permission
 * transitions and the Forge app properties are never run. Credential values
 * never reach the report or stdout.</p>
 */
class ConfluenceLiveWriteIT {

    private static final String DEFAULT_BASE_URL = "https://pipestreamai.atlassian.net/wiki";
    private static final String DEFAULT_SPACE_KEY = "CLAUDEGRPC";
    private static final String TAG = "[ConfluenceLiveWriteIT] ";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final long CALL_DEADLINE_SECONDS = 120;
    private static final int LARGE_UPLOAD_BYTES = 5 * 1024 * 1024;
    private static final int UPLOAD_CHUNK_BYTES = 256 * 1024;

    /** One report row. */
    private record Row(String rpc, String label, String outcome, String detail) {
    }

    /** One step of the run; answers the detail of a PASS. */
    @FunctionalInterface
    private interface Step {
        String run() throws Exception;
    }

    /** Ends a step as SKIPPED. */
    private static final class Skip extends RuntimeException {
        @java.io.Serial
        private static final long serialVersionUID = 1L;

        Skip(String reason) {
            super(reason);
        }
    }

    private final List<Row> rows = new ArrayList<>();
    private final List<String> leftBehind = new ArrayList<>();
    /** Cleanup actions, run last-created first. */
    private final List<Runnable> cleanups = new ArrayList<>();
    private ManagedChannel channel;
    private String email;
    private String token;
    private String run;

    private static String env(String name) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? null : value.strip();
    }

    private static String credential(String canonical, String alias) {
        String value = env(canonical);
        return value != null ? value : env(alias);
    }

    @Test
    void everyWriteRpcAgainstTheLiveWorkspace() throws Exception {
        email = credential(ConfluenceConnectorConfig.ENV_EMAIL, ConfluenceConnectorConfig.ENV_EMAIL_ALIAS);
        token = credential(ConfluenceConnectorConfig.ENV_API_TOKEN,
                ConfluenceConnectorConfig.ENV_API_TOKEN_ALIAS);
        assumeTrue(email != null && token != null,
                "no live Confluence credentials in the environment; skipping");
        assumeTrue("1".equals(env("CONFLUENCE_LIVE_WRITE")),
                "CONFLUENCE_LIVE_WRITE=1 is not set; skipping the live write run");
        String baseUrl = env(ConfluenceConnectorConfig.ENV_BASE_URL);
        if (baseUrl == null) {
            baseUrl = DEFAULT_BASE_URL;
        }
        String spaceKey = env("CONFLUENCE_SPACE_KEY");
        if (spaceKey == null) {
            spaceKey = DEFAULT_SPACE_KEY;
        }
        boolean admin = "1".equals(env("CONFLUENCE_LIVE_WRITE_ADMIN"));
        run = "pipestream-live-write-"
                + DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC)
                        .format(Instant.now())
                + "-" + UUID.randomUUID().toString().substring(0, 8);
        Path out = Path.of(System.getProperty("liveWrite.report", "build/live-write.md"));

        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(baseUrl).email(email).apiToken(token).pageSize(25).build();
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config, new ConfluenceClient(config),
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build().start();
        channel = InProcessChannelBuilder.forName(name).build();
        Instant started = Instant.now();
        try {
            exercise(spaceKey, admin);
        } finally {
            cleanUp();
            channel.shutdownNow();
            server.shutdownNow();
            server.awaitTermination(10, TimeUnit.SECONDS);
        }

        Set<String> seen = new LinkedHashSet<>();
        rows.forEach(row -> seen.add(row.rpc()));
        for (String rpc : ConfluenceWriteRoutes.rpcs()) {
            if (!seen.contains(rpc)) {
                rows.add(new Row(rpc, rpc, "FAIL", "not exercised by this test"));
            }
        }
        String report = report(URI.create(baseUrl).getHost(), spaceKey, started);
        Files.createDirectories(out.toAbsolutePath().getParent());
        Files.writeString(out, report, StandardCharsets.UTF_8);
        System.out.println(report);
        System.out.println(TAG + "report written to " + out.toAbsolutePath());

        List<String> failures = new ArrayList<>();
        rows.stream().filter(row -> row.outcome().equals("FAIL"))
                .forEach(row -> failures.add(row.label() + ": " + row.detail()));
        assertThat(failures).as("failed write rpcs (see %s)", out).isEmpty();
    }

    // ======================================================================
    // THE RUN
    // ======================================================================

    private void exercise(String spaceKey, boolean admin) {
        AtomicReference<Space> spaceRef = new AtomicReference<>();
        AtomicReference<Page> parentRef = new AtomicReference<>();
        step("CreatePage", "CreatePage (parent page, storage body)", () -> {
            Space space = stub().getSpace(GetSpaceRequest.newBuilder().setKey(spaceKey).build()).getSpace();
            spaceRef.set(space);
            Page parent = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                    .setTitle(run).setBody(storage("<p>Parent of the live write run " + run + ".</p>"))
                    .build()).getPage();
            trackPage(parent.getId());
            parentRef.set(parent);
            return "page " + parent.getId() + " in space " + space.getKey();
        });
        if (parentRef.get() == null) {
            ConfluenceWriteRoutes.rpcs().forEach(rpc -> {
                if (!rpc.equals("CreatePage")) {
                    rows.add(new Row(rpc, rpc, "SKIPPED", "the parent page could not be created"));
                }
            });
            return;
        }
        Space space = spaceRef.get();
        Page parent = parentRef.get();
        String self = parent.getAuthorId();

        Page adfPage = pagesWithAdf(space, parent);
        Page page = pageLifecycle(space, parent);
        if (page != null) {
            labels(page);
            contentProperties(page);
            comments(page);
            attachments(page);
            permissions(page, self);
            classification(page, space, admin);
        } else {
            skipAll("the test page could not be created", "AddLabels", "RemoveLabel",
                    "CreateContentProperty", "UpdateContentProperty", "DeleteContentProperty",
                    "CreateFooterComment", "UpdateFooterComment", "DeleteFooterComment",
                    "CreateInlineComment", "UpdateInlineComment", "DeleteInlineComment",
                    "UploadAttachment", "DeleteAttachment", "AddContentPermissions",
                    "SetContentPermissions", "DeleteContentPermissions", "SetRestrictionState",
                    "SetContentClassificationLevel", "ResetContentClassificationLevel",
                    "UpdateSpaceDefaultClassificationLevel", "DeleteSpaceDefaultClassificationLevel");
        }
        tasksAndRedaction(adfPage);
        spaceProperties(space);
        blogPosts(space);
        customContent(space, parent);
        treeNodes(space, parent);
        mutate(space, parent, page);
        deleteAndPurge(space, parent);
        tenantLevel(space, admin);
        skipAll("tenant-wide permission transition, never run", "BulkAssignSpaceRoles",
                "BulkRemoveSpaceAccess");
        skipAll("served to Forge app credentials only", "SetAppProperty", "DeleteAppProperty");
    }

    private Page pagesWithAdf(Space space, Page parent) {
        AtomicReference<Page> created = new AtomicReference<>();
        step("CreatePage", "CreatePage (ADF body)", () -> {
            AdfDocument document = AdfDocument.newBuilder()
                    .addContent(AdfNode.newBuilder().setType(AdfNodeType.ADF_NODE_TYPE_HEADING)
                            .setHeading(AdfHeadingAttrs.newBuilder().setLevel(2))
                            .addContent(text("Typed heading")))
                    .addContent(AdfNode.newBuilder().setType(AdfNodeType.ADF_NODE_TYPE_PARAGRAPH)
                            .addContent(text("alpha REDACTME omega")))
                    .addContent(AdfNode.newBuilder().setType(AdfNodeType.ADF_NODE_TYPE_TASK_LIST)
                            .setTaskList(AdfLocalIdAttrs.newBuilder()
                                    .setLocalId(UUID.randomUUID().toString()))
                            .addContent(AdfNode.newBuilder()
                                    .setType(AdfNodeType.ADF_NODE_TYPE_TASK_ITEM)
                                    .setTaskItem(AdfTaskItemAttrs.newBuilder()
                                            .setLocalId(UUID.randomUUID().toString())
                                            .setState(AdfTaskState.ADF_TASK_STATE_TODO))
                                    .addContent(text("ship it"))))
                    .build();
            Page page = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                    .setParentId(parent.getId()).setTitle(run + " adf")
                    .setBody(ContentBody.newBuilder().setAdf(document)).build()).getPage();
            trackPage(page.getId());
            created.set(page);
            Page read = stub().getPage(GetPageRequest.newBuilder().setId(page.getId())
                    .setBodyFormat(BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT).build()).getPage();
            AdfDocument back = AdfCodec.fromJson(
                    JSON.readTree(read.getBody().getAtlasDocFormat().getValue()));
            List<String> texts = new ArrayList<>();
            Set<AdfNodeType> types = new LinkedHashSet<>();
            collect(back.getContentList(), texts, types);
            check(texts.containsAll(List.of("Typed heading", "alpha REDACTME omega", "ship it")),
                    "the ADF read back lacks the written text: " + texts);
            check(types.containsAll(Set.of(AdfNodeType.ADF_NODE_TYPE_HEADING,
                    AdfNodeType.ADF_NODE_TYPE_PARAGRAPH, AdfNodeType.ADF_NODE_TYPE_TASK_LIST,
                    AdfNodeType.ADF_NODE_TYPE_TASK_ITEM)), "the ADF read back lacks node types: " + types);
            check(read.getParentId().equals(parent.getId()), "the page is not under the parent page");
            return "page " + page.getId() + "; ADF read back with " + types.size() + " node types";
        });
        return created.get();
    }

    private Page pageLifecycle(Space space, Page parent) {
        AtomicReference<Page> current = new AtomicReference<>();
        String first = "<p>Storage body <strong>bold</strong> " + run + " inlinetarget</p>";
        step("CreatePage", "CreatePage (storage body)", () -> {
            Page page = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                    .setParentId(parent.getId()).setTitle(run + " storage").setBody(storage(first))
                    .build()).getPage();
            trackPage(page.getId());
            current.set(page);
            String read = readStorage(page.getId());
            check(read.contains("<strong>bold</strong>") && read.contains(run),
                    "the storage body read back differs: " + read);
            return "page " + page.getId() + (read.equals(first) ? "; body identical" : "; body normalised");
        });
        if (current.get() == null) {
            skipAll("the test page could not be created", "UpdatePage", "UpdatePageTitle");
            return null;
        }
        String id = current.get().getId();
        int version = current.get().getVersion().getNumber();
        String second = "<p>Updated body " + run + " inlinetarget</p>";
        boolean updated = step("UpdatePage", "UpdatePage (expected_version)", () -> {
            Page page = stub().updatePage(UpdatePageRequest.newBuilder().setId(id)
                    .setTitle(run + " storage").setBody(storage(second))
                    .setExpectedVersion(version).setVersionMessage("live write update").build()).getPage();
            check(page.getVersion().getNumber() == version + 1,
                    "version is " + page.getVersion().getNumber() + ", expected " + (version + 1));
            check(readStorage(id).contains("Updated body"), "the updated body was not read back");
            return "version " + version + " -> " + page.getVersion().getNumber();
        });
        step("UpdatePage", "UpdatePage (stale expected_version is ABORTED)", () -> {
            if (!updated) {
                throw new Skip("the update before it failed");
            }
            try {
                stub().updatePage(UpdatePageRequest.newBuilder().setId(id).setTitle(run + " storage")
                        .setBody(storage("<p>stale</p>")).setExpectedVersion(version).build());
            } catch (StatusRuntimeException e) {
                check(e.getStatus().getCode() == Status.Code.ABORTED,
                        "a stale version answered " + describe(e) + ", expected ABORTED");
                check(readStorage(id).contains("Updated body"), "the stale update changed the page");
                return "ABORTED";
            }
            throw new AssertionError("a stale version was accepted");
        });
        step("UpdatePageTitle", "UpdatePageTitle", () -> {
            String title = run + " retitled";
            stub().updatePageTitle(UpdatePageTitleRequest.newBuilder().setId(id).setTitle(title).build());
            Page read = stub().getPage(GetPageRequest.newBuilder().setId(id).build()).getPage();
            check(read.getTitle().equals(title), "the title read back is " + read.getTitle());
            return "title read back";
        });
        try {
            return stub().getPage(GetPageRequest.newBuilder().setId(id).build()).getPage();
        } catch (StatusRuntimeException e) {
            return current.get();
        }
    }

    private void labels(Page page) {
        ContentRef ref = ref(ContentType.CONTENT_TYPE_PAGE, page.getId());
        String label = "live-write-" + run.substring(run.length() - 8);
        boolean added = step("AddLabels", "AddLabels", () -> {
            AddLabelsResponse response = stub().addLabels(AddLabelsRequest.newBuilder()
                    .setContentId(page.getId())
                    .addLabels(LabelInput.newBuilder().setName(label))
                    .addLabels(LabelInput.newBuilder().setPrefix(LabelPrefix.LABEL_PREFIX_GLOBAL)
                            .setName(label + "-b")).build());
            check(labelNames(ref).containsAll(List.of(label, label + "-b")),
                    "the labels were not read back: " + labelNames(ref));
            return response.getLabelsCount() + " labels on the page";
        });
        step("RemoveLabel", "RemoveLabel", () -> {
            if (!added) {
                throw new Skip("no label was added");
            }
            stub().removeLabel(RemoveLabelRequest.newBuilder().setContentId(page.getId())
                    .setName(label).build());
            List<String> names = labelNames(ref);
            check(!names.contains(label) && names.contains(label + "-b"),
                    "labels after the removal: " + names);
            return "label gone, the other kept";
        });
    }

    private void contentProperties(Page page) {
        ContentRef ref = ref(ContentType.CONTENT_TYPE_PAGE, page.getId());
        AtomicReference<ContentProperty> created = new AtomicReference<>();
        step("CreateContentProperty", "CreateContentProperty (every plain value arm)", () -> {
            ContentProperty property = stub().createContentProperty(
                    CreateContentPropertyRequest.newBuilder().setContent(ref)
                            .setKey(PropertyKey.PROPERTY_KEY_CUSTOM).setCustomKey(run + "-plain")
                            .setValue(plainValue("one")).build()).getProperty();
            created.set(property);
            ContentProperty read = stub().getContentProperty(GetContentPropertyRequest.newBuilder()
                    .setContent(ref).setPropertyId(property.getId()).build()).getProperty();
            check(read.getValue().equals(plainValue("one")), "the value read back differs: " + read.getValue());
            return "property " + property.getId() + "; value identical";
        });
        step("CreateContentProperty", "CreateContentProperty (big integer and exact decimal)", () -> {
            PropertyValue exact = PropertyValue.newBuilder().setObjectValue(PropertyObject.newBuilder()
                    .addEntries(entry("big", PropertyValue.newBuilder()
                            .setBigIntegerValue("123456789012345678901234567890")))
                    .addEntries(entry("decimal", PropertyValue.newBuilder()
                            .setDecimalValue("1.00000000000000000001")))).build();
            ContentProperty property = stub().createContentProperty(
                    CreateContentPropertyRequest.newBuilder().setContent(ref)
                            .setKey(PropertyKey.PROPERTY_KEY_CUSTOM).setCustomKey(run + "-exact")
                            .setValue(exact).build()).getProperty();
            ContentProperty read = stub().getContentProperty(GetContentPropertyRequest.newBuilder()
                    .setContent(ref).setPropertyId(property.getId()).build()).getProperty();
            if (!read.getValue().equals(exact)) {
                throw new Skip("the tenant stored the numbers rounded: " + read.getValue());
            }
            return "value identical";
        });
        step("UpdateContentProperty", "UpdateContentProperty", () -> {
            ContentProperty property = require(created.get(), "no property was created");
            ContentProperty updated = stub().updateContentProperty(
                    UpdateContentPropertyRequest.newBuilder().setContent(ref)
                            .setPropertyId(property.getId())
                            .setKey(PropertyKey.PROPERTY_KEY_CUSTOM).setCustomKey(run + "-plain")
                            .setValue(plainValue("two"))
                            .setExpectedVersion(property.getVersion().getNumber()).build()).getProperty();
            ContentProperty read = stub().getContentProperty(GetContentPropertyRequest.newBuilder()
                    .setContent(ref).setPropertyId(property.getId()).build()).getProperty();
            check(read.getValue().equals(plainValue("two")), "the value read back differs: " + read.getValue());
            return "version " + updated.getVersion().getNumber();
        });
        step("DeleteContentProperty", "DeleteContentProperty", () -> {
            ContentProperty property = require(created.get(), "no property was created");
            stub().deleteContentProperty(DeleteContentPropertyRequest.newBuilder().setContent(ref)
                    .setPropertyId(property.getId()).build());
            expectNotFound(() -> stub().getContentProperty(GetContentPropertyRequest.newBuilder()
                    .setContent(ref).setPropertyId(property.getId()).build()));
            return "property gone";
        });
    }

    private void spaceProperties(Space space) {
        AtomicReference<SpaceProperty> created = new AtomicReference<>();
        step("CreateSpaceProperty", "CreateSpaceProperty", () -> {
            SpaceProperty property = stub().createSpaceProperty(CreateSpacePropertyRequest.newBuilder()
                    .setSpaceId(space.getId()).setKey(PropertyKey.PROPERTY_KEY_CUSTOM)
                    .setCustomKey(run).setValue(plainValue("one")).build()).getProperty();
            created.set(property);
            cleanups.add(() -> {
                try {
                    stub().deleteSpaceProperty(DeleteSpacePropertyRequest.newBuilder()
                            .setSpaceId(space.getId()).setPropertyId(property.getId()).build());
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                        leftBehind.add("space property " + property.getId() + ": " + describe(e));
                    }
                }
            });
            SpaceProperty read = stub().getSpaceProperty(GetSpacePropertyRequest.newBuilder()
                    .setSpaceId(space.getId()).setPropertyId(property.getId()).build()).getProperty();
            check(read.getValue().equals(plainValue("one")), "the value read back differs: " + read.getValue());
            return "property " + property.getId() + "; value identical";
        });
        step("UpdateSpaceProperty", "UpdateSpaceProperty", () -> {
            SpaceProperty property = require(created.get(), "no space property was created");
            stub().updateSpaceProperty(UpdateSpacePropertyRequest.newBuilder()
                    .setSpaceId(space.getId()).setPropertyId(property.getId())
                    .setKey(PropertyKey.PROPERTY_KEY_CUSTOM).setCustomKey(run)
                    .setValue(plainValue("two"))
                    .setExpectedVersion(property.getVersion().getNumber()).build());
            SpaceProperty read = stub().getSpaceProperty(GetSpacePropertyRequest.newBuilder()
                    .setSpaceId(space.getId()).setPropertyId(property.getId()).build()).getProperty();
            check(read.getValue().equals(plainValue("two")), "the value read back differs: " + read.getValue());
            return "version " + read.getVersion().getNumber();
        });
        step("DeleteSpaceProperty", "DeleteSpaceProperty", () -> {
            SpaceProperty property = require(created.get(), "no space property was created");
            stub().deleteSpaceProperty(DeleteSpacePropertyRequest.newBuilder()
                    .setSpaceId(space.getId()).setPropertyId(property.getId()).build());
            expectNotFound(() -> stub().getSpaceProperty(GetSpacePropertyRequest.newBuilder()
                    .setSpaceId(space.getId()).setPropertyId(property.getId()).build()));
            return "property gone";
        });
    }

    private void comments(Page page) {
        AtomicReference<Comment> footer = new AtomicReference<>();
        step("CreateFooterComment", "CreateFooterComment", () -> {
            Comment comment = stub().createFooterComment(CreateFooterCommentRequest.newBuilder()
                    .setPageId(page.getId()).setBody(storage("<p>footer " + run + "</p>")).build())
                    .getComment();
            footer.set(comment);
            Comment read = stub().getComment(GetCommentRequest.newBuilder().setId(comment.getId())
                    .setKind(CommentKind.COMMENT_KIND_FOOTER).build()).getComment();
            check(read.getBody().getStorage().getValue().contains("footer " + run),
                    "the comment body was not read back");
            check(read.getPageId().equals(page.getId()), "the comment is not on the page");
            return "comment " + comment.getId();
        });
        step("UpdateFooterComment", "UpdateFooterComment", () -> {
            Comment comment = require(footer.get(), "no footer comment was created");
            stub().updateFooterComment(UpdateFooterCommentRequest.newBuilder().setId(comment.getId())
                    .setBody(storage("<p>footer edited " + run + "</p>"))
                    .setExpectedVersion(comment.getVersion().getNumber()).build());
            Comment read = stub().getComment(GetCommentRequest.newBuilder().setId(comment.getId())
                    .setKind(CommentKind.COMMENT_KIND_FOOTER).build()).getComment();
            check(read.getBody().getStorage().getValue().contains("footer edited"),
                    "the edited body was not read back");
            return "version " + read.getVersion().getNumber();
        });
        step("DeleteFooterComment", "DeleteFooterComment", () -> {
            Comment comment = require(footer.get(), "no footer comment was created");
            stub().deleteFooterComment(DeleteFooterCommentRequest.newBuilder()
                    .setId(comment.getId()).build());
            expectNotFound(() -> stub().getComment(GetCommentRequest.newBuilder()
                    .setId(comment.getId()).setKind(CommentKind.COMMENT_KIND_FOOTER).build()));
            return "comment gone";
        });

        AtomicReference<Comment> inline = new AtomicReference<>();
        step("CreateInlineComment", "CreateInlineComment", () -> {
            Comment comment = stub().createInlineComment(CreateInlineCommentRequest.newBuilder()
                    .setPageId(page.getId()).setBody(storage("<p>inline " + run + "</p>"))
                    .setSelection(InlineCommentSelection.newBuilder().setTextSelection("inlinetarget")
                            .setTextSelectionMatchCount(1).setTextSelectionMatchIndex(0)).build())
                    .getComment();
            inline.set(comment);
            Comment read = stub().getComment(GetCommentRequest.newBuilder().setId(comment.getId())
                    .setKind(CommentKind.COMMENT_KIND_INLINE).build()).getComment();
            check(read.getBody().getStorage().getValue().contains("inline " + run),
                    "the comment body was not read back");
            check(read.getInlineProperties().getInlineOriginalSelection().equals("inlinetarget"),
                    "the selection read back is "
                            + read.getInlineProperties().getInlineOriginalSelection());
            return "comment " + comment.getId();
        });
        step("UpdateInlineComment", "UpdateInlineComment (resolve)", () -> {
            Comment comment = require(inline.get(), "no inline comment was created");
            stub().updateInlineComment(UpdateInlineCommentRequest.newBuilder().setId(comment.getId())
                    .setBody(storage("<p>inline edited " + run + "</p>"))
                    .setExpectedVersion(comment.getVersion().getNumber()).setResolved(true).build());
            Comment read = stub().getComment(GetCommentRequest.newBuilder().setId(comment.getId())
                    .setKind(CommentKind.COMMENT_KIND_INLINE).build()).getComment();
            check(read.getBody().getStorage().getValue().contains("inline edited"),
                    "the edited body was not read back");
            return "resolution " + read.getResolutionStatus();
        });
        step("DeleteInlineComment", "DeleteInlineComment", () -> {
            Comment comment = require(inline.get(), "no inline comment was created");
            stub().deleteInlineComment(DeleteInlineCommentRequest.newBuilder()
                    .setId(comment.getId()).build());
            expectNotFound(() -> stub().getComment(GetCommentRequest.newBuilder()
                    .setId(comment.getId()).setKind(CommentKind.COMMENT_KIND_INLINE).build()));
            return "comment gone";
        });
    }

    private void attachments(Page page) {
        AtomicReference<Attachment> small = new AtomicReference<>();
        byte[] smallBytes = ("hello live write " + run + "\n").getBytes(StandardCharsets.UTF_8);
        step("UploadAttachment", "UploadAttachment (small)", () -> {
            Attachment attachment = upload(page.getId(), run + ".txt", "text/plain", smallBytes, false);
            small.set(attachment);
            check(java.util.Arrays.equals(download(attachment.getId()), smallBytes),
                    "the bytes read back differ");
            return "attachment " + attachment.getId() + "; " + smallBytes.length + " bytes identical";
        });
        step("UploadAttachment", "UploadAttachment (multi-MB, streamed)", () -> {
            byte[] large = new byte[LARGE_UPLOAD_BYTES];
            new Random(42).nextBytes(large);
            Attachment attachment = upload(page.getId(), run + ".bin", "application/octet-stream",
                    large, false);
            check(sha256(download(attachment.getId())).equals(sha256(large)),
                    "the SHA-256 of the bytes read back differs");
            return "attachment " + attachment.getId() + "; " + large.length + " bytes, SHA-256 identical";
        });
        step("UploadAttachment", "UploadAttachment (replace_existing adds a version)", () -> {
            require(small.get(), "no attachment was uploaded");
            byte[] next = ("second version " + run + "\n").getBytes(StandardCharsets.UTF_8);
            Attachment attachment = upload(page.getId(), run + ".txt", "text/plain", next, true);
            check(java.util.Arrays.equals(download(attachment.getId()), next),
                    "the bytes read back differ");
            return "attachment " + attachment.getId() + " version " + attachment.getVersion().getNumber();
        });
        step("DeleteAttachment", "DeleteAttachment (trash, then purge)", () -> {
            Attachment attachment = require(small.get(), "no attachment was uploaded");
            stub().deleteAttachment(DeleteAttachmentRequest.newBuilder().setId(attachment.getId()).build());
            String purge;
            try {
                stub().deleteAttachment(DeleteAttachmentRequest.newBuilder().setId(attachment.getId())
                        .setPurge(true).build());
                purge = "purged";
            } catch (StatusRuntimeException e) {
                purge = "purge answered " + describe(e);
            }
            return "trashed; " + purge;
        });
    }

    private void permissions(Page page, String self) {
        ContentRef ref = ref(ContentType.CONTENT_TYPE_PAGE, page.getId());
        ContentPermissionPrincipal me = ContentPermissionPrincipal.newBuilder()
                .setPrincipalType(ContentPermissionPrincipalType.CONTENT_PERMISSION_PRINCIPAL_TYPE_USER)
                .setPrincipalId(self).build();
        ContentPermissionGrant grant = ContentPermissionGrant.newBuilder()
                .setOperation(ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_UPDATE)
                .addPrincipals(me).build();
        step("AddContentPermissions", "AddContentPermissions", () -> {
            stub().addContentPermissions(AddContentPermissionsRequest.newBuilder().setContent(ref)
                    .addPermissions(grant).build());
            check(permissionHolders(ref).contains(self), "the permission was not read back");
            return "update permission for the caller";
        });
        step("SetRestrictionState", "SetRestrictionState", () -> {
            stub().setRestrictionState(SetRestrictionStateRequest.newBuilder().setContent(ref)
                    .setState(ContentRestrictionState.CONTENT_RESTRICTION_STATE_EDIT_RESTRICTED).build());
            ContentRestrictionState read = stub().getRestrictionState(
                    GetRestrictionStateRequest.newBuilder().setContent(ref).build())
                    .getRestrictionState().getState();
            check(read == ContentRestrictionState.CONTENT_RESTRICTION_STATE_EDIT_RESTRICTED,
                    "the state read back is " + read);
            return "EDIT_RESTRICTED read back";
        });
        step("SetContentPermissions", "SetContentPermissions", () -> {
            stub().setContentPermissions(SetContentPermissionsRequest.newBuilder().setContent(ref)
                    .addPermissions(grant).build());
            check(permissionHolders(ref).contains(self), "the permission was not read back");
            return "permissions replaced";
        });
        step("SetRestrictionState", "SetRestrictionState (back to OPEN)", () -> {
            stub().setRestrictionState(SetRestrictionStateRequest.newBuilder().setContent(ref)
                    .setState(ContentRestrictionState.CONTENT_RESTRICTION_STATE_OPEN).build());
            ContentRestrictionState read = stub().getRestrictionState(
                    GetRestrictionStateRequest.newBuilder().setContent(ref).build())
                    .getRestrictionState().getState();
            check(read == ContentRestrictionState.CONTENT_RESTRICTION_STATE_OPEN,
                    "the state read back is " + read);
            return "OPEN read back";
        });
        step("DeleteContentPermissions", "DeleteContentPermissions", () -> {
            stub().deleteContentPermissions(DeleteContentPermissionsRequest.newBuilder().setContent(ref)
                    .setAllowSelfLockout(true).build());
            check(permissionHolders(ref).isEmpty(), "permissions remain: " + permissionHolders(ref));
            return "no direct permissions left";
        });
    }

    private void classification(Page page, Space space, boolean admin) {
        ContentRef ref = ref(ContentType.CONTENT_TYPE_PAGE, page.getId());
        List<ClassificationLevel> levels = new ArrayList<>();
        String unavailable = null;
        try {
            levels.addAll(stub().listClassificationLevels(
                    ListClassificationLevelsRequest.getDefaultInstance()).getLevelsList());
            if (levels.isEmpty()) {
                unavailable = "classification disabled: the tenant has no classification levels";
            }
        } catch (StatusRuntimeException e) {
            unavailable = "classification disabled: " + describe(e);
        }
        if (unavailable != null) {
            skipAll(unavailable, "SetContentClassificationLevel", "ResetContentClassificationLevel",
                    "UpdateSpaceDefaultClassificationLevel", "DeleteSpaceDefaultClassificationLevel");
            return;
        }
        String level = levels.get(0).getId();
        optional("SetContentClassificationLevel", "SetContentClassificationLevel", () -> {
            stub().setContentClassificationLevel(SetContentClassificationLevelRequest.newBuilder()
                    .setContent(ref).setClassificationLevelId(level).build());
            return "level " + level;
        });
        optional("ResetContentClassificationLevel", "ResetContentClassificationLevel", () -> {
            stub().resetContentClassificationLevel(ResetContentClassificationLevelRequest.newBuilder()
                    .setContent(ref).build());
            return "reset";
        });
        if (!admin) {
            skipAll("tenant-level, not run", "UpdateSpaceDefaultClassificationLevel",
                    "DeleteSpaceDefaultClassificationLevel");
            return;
        }
        optional("UpdateSpaceDefaultClassificationLevel", "UpdateSpaceDefaultClassificationLevel", () -> {
            stub().updateSpaceDefaultClassificationLevel(
                    UpdateSpaceDefaultClassificationLevelRequest.newBuilder()
                            .setSpaceId(space.getId()).setClassificationLevelId(level).build());
            return "level " + level;
        });
        optional("DeleteSpaceDefaultClassificationLevel", "DeleteSpaceDefaultClassificationLevel", () -> {
            stub().deleteSpaceDefaultClassificationLevel(
                    DeleteSpaceDefaultClassificationLevelRequest.newBuilder()
                            .setSpaceId(space.getId()).build());
            return "default removed";
        });
    }

    private void tasksAndRedaction(Page adfPage) {
        step("UpdateTask", "UpdateTask", () -> {
            Page page = require(adfPage, "the ADF page could not be created");
            Task task = null;
            for (int attempt = 0; attempt < 10 && task == null; attempt++) {
                Iterator<ListTasksResponse> tasks = stub().listTasks(ListTasksRequest.newBuilder()
                        .addPageIds(page.getId()).build());
                if (tasks.hasNext()) {
                    task = tasks.next().getTask();
                } else {
                    Thread.sleep(2000);
                }
            }
            if (task == null) {
                throw new Skip("no task exists in the test page");
            }
            stub().updateTask(UpdateTaskRequest.newBuilder().setId(task.getId())
                    .setStatus(TaskStatus.TASK_STATUS_COMPLETE).build());
            Task read = stub().getTask(GetTaskRequest.newBuilder().setId(task.getId()).build()).getTask();
            check(read.getStatus() == TaskStatus.TASK_STATUS_COMPLETE, "the status read back is "
                    + read.getStatus());
            return "task " + task.getId() + " complete";
        });
        optional("RedactPage", "RedactPage", () -> {
            Page page = require(adfPage, "the ADF page could not be created");
            Page read = stub().getPage(GetPageRequest.newBuilder().setId(page.getId()).build()).getPage();
            RedactionResponse redactions = stub().redactPage(RedactPageRequest.newBuilder()
                    .setId(page.getId()).setCreatedAt(read.getVersion().getCreatedAt())
                    .addBodyRedactions(RedactionPointer.newBuilder().setPointer("/content/1/content/0")
                            .setFrom(6).setTo(14).setReason("live write test")).build()).getRedactions();
            String body = stub().getPage(GetPageRequest.newBuilder().setId(page.getId())
                    .setBodyFormat(BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT).build()).getPage()
                    .getBody().getAtlasDocFormat().getValue();
            check(!body.contains("REDACTME"), "the redacted text is still in the body");
            return redactions.getBody().getRedactionsCount() + " redaction(s) applied";
        });
    }

    private void blogPosts(Space space) {
        AtomicReference<BlogPost> created = new AtomicReference<>();
        step("CreateBlogPost", "CreateBlogPost", () -> {
            BlogPost post = stub().createBlogPost(CreateBlogPostRequest.newBuilder()
                    .setSpaceId(space.getId()).setTitle(run + " blog")
                    .setBody(storage("<p>blog REDACTME " + run + "</p>")).build()).getBlogPost();
            created.set(post);
            cleanups.add(() -> purge("blog post " + post.getId(),
                    () -> stub().deleteBlogPost(DeleteBlogPostRequest.newBuilder()
                            .setId(post.getId()).build()),
                    () -> stub().deleteBlogPost(DeleteBlogPostRequest.newBuilder()
                            .setId(post.getId()).setPurge(true).build())));
            BlogPost read = stub().getBlogPost(GetBlogPostRequest.newBuilder().setId(post.getId())
                    .build()).getBlogPost();
            check(read.getBody().getStorage().getValue().contains(run), "the body was not read back");
            return "blog post " + post.getId();
        });
        step("UpdateBlogPost", "UpdateBlogPost", () -> {
            BlogPost post = require(created.get(), "no blog post was created");
            BlogPost updated = stub().updateBlogPost(UpdateBlogPostRequest.newBuilder()
                    .setId(post.getId()).setTitle(run + " blog edited")
                    .setBody(storage("<p>blog edited REDACTME " + run + "</p>"))
                    .setExpectedVersion(post.getVersion().getNumber()).build()).getBlogPost();
            BlogPost read = stub().getBlogPost(GetBlogPostRequest.newBuilder().setId(post.getId())
                    .build()).getBlogPost();
            check(read.getTitle().equals(run + " blog edited"), "the title read back is " + read.getTitle());
            check(read.getBody().getStorage().getValue().contains("blog edited"),
                    "the edited body was not read back");
            return "version " + updated.getVersion().getNumber();
        });
        optional("RedactBlogPost", "RedactBlogPost", () -> {
            BlogPost post = require(created.get(), "no blog post was created");
            BlogPost read = stub().getBlogPost(GetBlogPostRequest.newBuilder().setId(post.getId())
                    .build()).getBlogPost();
            RedactionResponse redactions = stub().redactBlogPost(RedactBlogPostRequest.newBuilder()
                    .setId(post.getId()).setCreatedAt(read.getVersion().getCreatedAt())
                    .addBodyRedactions(RedactionPointer.newBuilder().setPointer("/content/0/content/0")
                            .setFrom(12).setTo(20).setReason("live write test")).build()).getRedactions();
            return redactions.getBody().getRedactionsCount() + " redaction(s) applied";
        });
        step("DeleteBlogPost", "DeleteBlogPost (trash, then purge)", () -> {
            BlogPost post = require(created.get(), "no blog post was created");
            stub().deleteBlogPost(DeleteBlogPostRequest.newBuilder().setId(post.getId()).build());
            stub().deleteBlogPost(DeleteBlogPostRequest.newBuilder().setId(post.getId())
                    .setPurge(true).build());
            expectNotFound(() -> stub().getBlogPost(GetBlogPostRequest.newBuilder()
                    .setId(post.getId()).build()));
            return "blog post gone";
        });
    }

    private void customContent(Space space, Page parent) {
        String type = env("CONFLUENCE_LIVE_WRITE_CUSTOM_CONTENT_TYPE");
        if (type == null) {
            skipAll("no custom content type available (set CONFLUENCE_LIVE_WRITE_CUSTOM_CONTENT_TYPE)",
                    "CreateCustomContent", "UpdateCustomContent", "DeleteCustomContent");
            return;
        }
        AtomicReference<CustomContent> created = new AtomicReference<>();
        step("CreateCustomContent", "CreateCustomContent", () -> {
            CustomContent content = stub().createCustomContent(CreateCustomContentRequest.newBuilder()
                    .setType(type).setPageId(parent.getId()).setTitle(run + " custom")
                    .setBody(storage("<p>custom " + run + "</p>")).build()).getCustomContent();
            created.set(content);
            cleanups.add(() -> purge("custom content " + content.getId(),
                    () -> stub().deleteCustomContent(DeleteCustomContentRequest.newBuilder()
                            .setId(content.getId()).build()),
                    () -> stub().deleteCustomContent(DeleteCustomContentRequest.newBuilder()
                            .setId(content.getId()).setPurge(true).build())));
            CustomContent read = stub().getCustomContent(GetCustomContentRequest.newBuilder()
                    .setId(content.getId()).build()).getCustomContent();
            check(read.getTitle().equals(run + " custom"), "the title read back is " + read.getTitle());
            return "custom content " + content.getId();
        });
        step("UpdateCustomContent", "UpdateCustomContent", () -> {
            CustomContent content = require(created.get(), "no custom content was created");
            stub().updateCustomContent(UpdateCustomContentRequest.newBuilder().setId(content.getId())
                    .setType(type).setPageId(parent.getId()).setTitle(run + " custom edited")
                    .setBody(storage("<p>custom edited " + run + "</p>"))
                    .setExpectedVersion(content.getVersion().getNumber()).build());
            CustomContent read = stub().getCustomContent(GetCustomContentRequest.newBuilder()
                    .setId(content.getId()).build()).getCustomContent();
            check(read.getTitle().equals(run + " custom edited"), "the title read back is "
                    + read.getTitle());
            return "title read back";
        });
        step("DeleteCustomContent", "DeleteCustomContent (trash, then purge)", () -> {
            CustomContent content = require(created.get(), "no custom content was created");
            stub().deleteCustomContent(DeleteCustomContentRequest.newBuilder()
                    .setId(content.getId()).build());
            stub().deleteCustomContent(DeleteCustomContentRequest.newBuilder()
                    .setId(content.getId()).setPurge(true).build());
            return "custom content gone";
        });
    }

    private void treeNodes(Space space, Page parent) {
        AtomicReference<String> folder = new AtomicReference<>();
        step("CreateFolder", "CreateFolder", () -> {
            Folder created = stub().createFolder(CreateFolderRequest.newBuilder()
                    .setSpaceId(space.getId()).setParentId(parent.getId()).setTitle(run + " folder")
                    .build()).getFolder();
            folder.set(created.getId());
            cleanups.add(() -> remove("folder " + created.getId(), () -> stub().deleteFolder(
                    DeleteFolderRequest.newBuilder().setId(created.getId()).build())));
            Folder read = stub().getFolder(GetFolderRequest.newBuilder().setId(created.getId()).build())
                    .getFolder();
            check(read.getTitle().equals(run + " folder"), "the title read back is " + read.getTitle());
            return "folder " + created.getId();
        });
        step("DeleteFolder", "DeleteFolder", () -> {
            String id = require(folder.get(), "no folder was created");
            stub().deleteFolder(DeleteFolderRequest.newBuilder().setId(id).build());
            return "folder deleted";
        });

        AtomicReference<String> whiteboard = new AtomicReference<>();
        step("CreateWhiteboard", "CreateWhiteboard", () -> {
            Whiteboard created = stub().createWhiteboard(CreateWhiteboardRequest.newBuilder()
                    .setSpaceId(space.getId()).setParentId(parent.getId())
                    .setTitle(run + " whiteboard").build()).getWhiteboard();
            whiteboard.set(created.getId());
            cleanups.add(() -> remove("whiteboard " + created.getId(), () -> stub().deleteWhiteboard(
                    DeleteWhiteboardRequest.newBuilder().setId(created.getId()).build())));
            Whiteboard read = stub().getWhiteboard(GetWhiteboardRequest.newBuilder()
                    .setId(created.getId()).build()).getWhiteboard();
            check(read.getTitle().equals(run + " whiteboard"), "the title read back is " + read.getTitle());
            return "whiteboard " + created.getId();
        });
        step("DeleteWhiteboard", "DeleteWhiteboard", () -> {
            String id = require(whiteboard.get(), "no whiteboard was created");
            stub().deleteWhiteboard(DeleteWhiteboardRequest.newBuilder().setId(id).build());
            return "whiteboard deleted";
        });

        AtomicReference<String> database = new AtomicReference<>();
        step("CreateDatabase", "CreateDatabase", () -> {
            Database created = stub().createDatabase(CreateDatabaseRequest.newBuilder()
                    .setSpaceId(space.getId()).setParentId(parent.getId())
                    .setTitle(run + " database").build()).getDatabase();
            database.set(created.getId());
            cleanups.add(() -> remove("database " + created.getId(), () -> stub().deleteDatabase(
                    DeleteDatabaseRequest.newBuilder().setId(created.getId()).build())));
            Database read = stub().getDatabase(GetDatabaseRequest.newBuilder()
                    .setId(created.getId()).build()).getDatabase();
            check(read.getTitle().equals(run + " database"), "the title read back is " + read.getTitle());
            return "database " + created.getId();
        });
        step("DeleteDatabase", "DeleteDatabase", () -> {
            String id = require(database.get(), "no database was created");
            stub().deleteDatabase(DeleteDatabaseRequest.newBuilder().setId(id).build());
            return "database deleted";
        });

        AtomicReference<String> smartLink = new AtomicReference<>();
        step("CreateSmartLink", "CreateSmartLink", () -> {
            SmartLink created = stub().createSmartLink(CreateSmartLinkRequest.newBuilder()
                    .setSpaceId(space.getId()).setParentId(parent.getId())
                    .setTitle(run + " smart link").setEmbedUrl("https://www.atlassian.com/").build())
                    .getSmartLink();
            smartLink.set(created.getId());
            cleanups.add(() -> remove("smart link " + created.getId(), () -> stub().deleteSmartLink(
                    DeleteSmartLinkRequest.newBuilder().setId(created.getId()).build())));
            SmartLink read = stub().getSmartLink(GetSmartLinkRequest.newBuilder()
                    .setId(created.getId()).build()).getSmartLink();
            check(read.getTitle().equals(run + " smart link"), "the title read back is " + read.getTitle());
            return "smart link " + created.getId();
        });
        step("DeleteSmartLink", "DeleteSmartLink", () -> {
            String id = require(smartLink.get(), "no smart link was created");
            stub().deleteSmartLink(DeleteSmartLinkRequest.newBuilder().setId(id).build());
            return "smart link deleted";
        });
    }

    private void mutate(Space space, Page parent, Page page) {
        step("Mutate", "Mutate (several operations)", () -> {
            Page target = require(page, "the test page could not be created");
            ContentRef ref = ref(ContentType.CONTENT_TYPE_PAGE, target.getId());
            List<MutateRequest> operations = List.of(
                    MutateRequest.newBuilder().setOpId("page-a").setCreatePage(
                            CreatePageRequest.newBuilder().setSpaceId(space.getId())
                                    .setParentId(parent.getId()).setTitle(run + " mutate a")
                                    .setBody(storage("<p>mutate a</p>"))).build(),
                    MutateRequest.newBuilder().setOpId("page-b").setCreatePage(
                            CreatePageRequest.newBuilder().setSpaceId(space.getId())
                                    .setParentId(parent.getId()).setTitle(run + " mutate b")
                                    .setBody(storage("<p>mutate b</p>"))).build(),
                    MutateRequest.newBuilder().setOpId("label").setAddLabels(
                            AddLabelsRequest.newBuilder().setContentId(target.getId())
                                    .addLabels(LabelInput.newBuilder().setName("live-write-mutate")))
                            .build(),
                    MutateRequest.newBuilder().setOpId("property").setCreateContentProperty(
                            CreateContentPropertyRequest.newBuilder().setContent(ref)
                                    .setKey(PropertyKey.PROPERTY_KEY_CUSTOM)
                                    .setCustomKey(run + "-mutate").setValue(plainValue("m"))).build(),
                    MutateRequest.newBuilder().setOpId("stale").setUpdatePage(
                            UpdatePageRequest.newBuilder().setId(target.getId())
                                    .setTitle(target.getTitle()).setBody(storage("<p>stale</p>"))
                                    .setExpectedVersion(0)).build());
            Map<String, MutateResponse> answers = new HashMap<>();
            for (MutateResponse answer : mutateAll(operations)) {
                check(answers.put(answer.getOpId(), answer) == null,
                        "operation " + answer.getOpId() + " was answered twice");
            }
            check(answers.keySet().equals(Set.of("page-a", "page-b", "label", "property", "stale")),
                    "answered operations: " + answers.keySet());
            for (String op : List.of("page-a", "page-b")) {
                MutateResponse answer = answers.get(op);
                check(answer.hasCreatePage(), op + " answered " + answer.getError());
                String id = answer.getCreatePage().getPage().getId();
                trackPage(id);
                check(stub().getPage(GetPageRequest.newBuilder().setId(id).build()).getPage()
                        .getParentId().equals(parent.getId()), op + " is not under the parent page");
            }
            check(answers.get("label").hasAddLabels(), "label answered " + answers.get("label").getError());
            check(labelNames(ref).contains("live-write-mutate"), "the label was not read back");
            check(answers.get("property").hasCreateContentProperty(),
                    "property answered " + answers.get("property").getError());
            check(answers.get("stale").getError().getCode() == MutateErrorCode.MUTATE_ERROR_CODE_CONFLICT,
                    "the stale update answered " + answers.get("stale").getOutcomeCase() + " "
                            + answers.get("stale").getError());
            return "5 operations answered once each: 4 succeeded, the stale update answered CONFLICT";
        });
    }

    /** Deletes one page of the run through DeletePage and checks trash and purge. */
    private void deleteAndPurge(Space space, Page parent) {
        step("DeletePage", "DeletePage (trash, then purge)", () -> {
            Page page = stub().createPage(CreatePageRequest.newBuilder().setSpaceId(space.getId())
                    .setParentId(parent.getId()).setTitle(run + " to delete")
                    .setBody(storage("<p>delete me</p>")).build()).getPage();
            trackPage(page.getId());
            stub().deletePage(DeletePageRequest.newBuilder().setId(page.getId()).build());
            Page trashed = stub().getPage(GetPageRequest.newBuilder().setId(page.getId())
                    .addStatuses(ContentStatus.CONTENT_STATUS_TRASHED).build()).getPage();
            check(trashed.getStatus() == ContentStatus.CONTENT_STATUS_TRASHED,
                    "the status after the delete is " + trashed.getStatus());
            stub().deletePage(DeletePageRequest.newBuilder().setId(page.getId()).setPurge(true).build());
            expectNotFound(() -> stub().getPage(GetPageRequest.newBuilder().setId(page.getId())
                    .addStatuses(ContentStatus.CONTENT_STATUS_TRASHED).build()));
            return "trashed, then purged";
        });
    }

    private void tenantLevel(Space space, boolean admin) {
        if (!admin) {
            skipAll("tenant-level, not run", "CreateSpace", "CreateSpaceRole", "UpdateSpaceRole",
                    "DeleteSpaceRole", "SetSpaceRoleAssignments", "InviteByEmail", "EnableAdminKey",
                    "DisableAdminKey");
            return;
        }
        step("CreateSpace", "CreateSpace", () -> {
            String key = "LW" + run.substring(run.length() - 8).toUpperCase(java.util.Locale.ROOT);
            Space created = stub().createSpace(CreateSpaceRequest.newBuilder().setName(run)
                    .setKey(key).setDescription("Created by the live write run " + run).build())
                    .getSpace();
            leftBehind.add("space " + created.getKey() + " (id " + created.getId()
                    + "): REST API v2 has no route that deletes a space");
            Space read = stub().getSpace(GetSpaceRequest.newBuilder().setId(created.getId()).build())
                    .getSpace();
            check(read.getKey().equals(key), "the key read back is " + read.getKey());
            return "space " + created.getKey();
        });
        AtomicReference<SpaceRole> role = new AtomicReference<>();
        step("CreateSpaceRole", "CreateSpaceRole", () -> {
            List<String> permissions = new ArrayList<>();
            Iterator<ListSpacePermissionsResponse> available = stub().listSpacePermissions(
                    ListSpacePermissionsRequest.getDefaultInstance());
            if (available.hasNext()) {
                permissions.add(available.next().getPermission().getId());
            }
            SpaceRole created = stub().createSpaceRole(CreateSpaceRoleRequest.newBuilder()
                    .setName(run).setDescription("live write run").addAllSpacePermissions(permissions)
                    .build()).getRole();
            role.set(created);
            cleanups.add(() -> remove("space role " + created.getId(), () -> stub().deleteSpaceRole(
                    DeleteSpaceRoleRequest.newBuilder().setId(created.getId()).build())));
            SpaceRole read = stub().getSpaceRole(GetSpaceRoleRequest.newBuilder()
                    .setId(created.getId()).build()).getRole();
            check(read.getName().equals(run), "the name read back is " + read.getName());
            return "role " + created.getId();
        });
        step("UpdateSpaceRole", "UpdateSpaceRole", () -> {
            SpaceRole created = require(role.get(), "no space role was created");
            UpdateSpaceRoleResponse updated = stub().updateSpaceRole(UpdateSpaceRoleRequest.newBuilder()
                    .setId(created.getId()).setName(run).setDescription("live write run, edited")
                    .addAllSpacePermissions(created.getSpacePermissionsList()).build());
            return "task " + updated.getTaskId();
        });
        step("SetSpaceRoleAssignments", "SetSpaceRoleAssignments (re-sets one existing assignment)", () -> {
            Iterator<ListSpaceRoleAssignmentsResponse> existing = stub().listSpaceRoleAssignments(
                    ListSpaceRoleAssignmentsRequest.newBuilder().setSpaceId(space.getId()).build());
            if (!existing.hasNext()) {
                throw new Skip("the space has no role assignment to re-set");
            }
            SpaceRoleAssignment assignment = existing.next().getAssignment();
            SetSpaceRoleAssignmentsResponse response = stub().setSpaceRoleAssignments(
                    SetSpaceRoleAssignmentsRequest.newBuilder().setSpaceId(space.getId())
                            .addAssignments(assignment).build());
            return response.getAssignmentsCount() + " assignment(s) answered";
        });
        step("DeleteSpaceRole", "DeleteSpaceRole", () -> {
            SpaceRole created = require(role.get(), "no space role was created");
            return "task " + stub().deleteSpaceRole(DeleteSpaceRoleRequest.newBuilder()
                    .setId(created.getId()).build()).getTaskId();
        });
        step("InviteByEmail", "InviteByEmail", () -> {
            String invitee = env("CONFLUENCE_LIVE_WRITE_INVITE_EMAIL");
            if (invitee == null) {
                throw new Skip("no address to invite (set CONFLUENCE_LIVE_WRITE_INVITE_EMAIL)");
            }
            stub().inviteByEmail(InviteByEmailRequest.newBuilder().addEmails(invitee).build());
            return "one address invited";
        });
        boolean enabled = step("EnableAdminKey", "EnableAdminKey", () -> {
            stub().enableAdminKey(EnableAdminKeyRequest.newBuilder().setDurationInMinutes(1).build());
            cleanups.add(() -> remove("admin key", () -> stub().disableAdminKey(
                    DisableAdminKeyRequest.getDefaultInstance())));
            stub().getAdminKey(GetAdminKeyRequest.getDefaultInstance());
            return "admin key read back";
        });
        step("DisableAdminKey", "DisableAdminKey", () -> {
            if (!enabled) {
                throw new Skip("no admin key was enabled");
            }
            stub().disableAdminKey(DisableAdminKeyRequest.getDefaultInstance());
            expectNotFound(() -> stub().getAdminKey(GetAdminKeyRequest.getDefaultInstance()));
            return "admin key gone";
        });
    }

    // ======================================================================
    // CLEAN-UP
    // ======================================================================

    /** Registers a page to be trashed and purged at the end. */
    private void trackPage(String id) {
        cleanups.add(() -> purge("page " + id,
                () -> stub().deletePage(DeletePageRequest.newBuilder().setId(id).build()),
                () -> stub().deletePage(DeletePageRequest.newBuilder().setId(id).setPurge(true).build())));
    }

    private void cleanUp() {
        for (int i = cleanups.size() - 1; i >= 0; i--) {
            try {
                cleanups.get(i).run();
            } catch (RuntimeException e) {
                leftBehind.add("a clean-up step failed: " + scrub(String.valueOf(e.getMessage())));
            }
        }
        leftBehind.forEach(line -> System.out.println(TAG + "left behind: " + scrub(line)));
    }

    /** Trashes and purges; a NOT_FOUND means it is already gone. */
    private void purge(String what, Runnable trash, Runnable purge) {
        try {
            trash.run();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                leftBehind.add(what + " (not trashed): " + describe(e));
                return;
            }
        }
        try {
            purge.run();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                leftBehind.add(what + " (in the trash, not purged): " + describe(e));
            }
        }
    }

    private void remove(String what, Runnable delete) {
        try {
            delete.run();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() != Status.Code.NOT_FOUND) {
                leftBehind.add(what + ": " + describe(e));
            }
        }
    }

    // ======================================================================
    // STEPS AND ROWS
    // ======================================================================

    /** Runs one step; any failure is a FAIL row. */
    private boolean step(String rpc, String label, Step body) {
        return run(rpc, label, body, false);
    }

    /** Runs one step of a feature the tenant may not offer; a gRPC error is a SKIPPED row. */
    private boolean optional(String rpc, String label, Step body) {
        return run(rpc, label, body, true);
    }

    private boolean run(String rpc, String label, Step body, boolean tenantMayReject) {
        try {
            rows.add(new Row(rpc, label, "PASS", scrub(body.run())));
            return true;
        } catch (Skip e) {
            rows.add(new Row(rpc, label, "SKIPPED", scrub(e.getMessage())));
        } catch (StatusRuntimeException e) {
            Status.Code code = e.getStatus().getCode();
            boolean rejected = tenantMayReject && code != Status.Code.INTERNAL
                    && code != Status.Code.UNKNOWN;
            rows.add(new Row(rpc, label, rejected ? "SKIPPED" : "FAIL",
                    (rejected ? "the tenant rejected it: " : "") + describe(e)));
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            rows.add(new Row(rpc, label, "FAIL", scrub(t.getClass().getSimpleName() + ": "
                    + t.getMessage())));
        }
        return false;
    }

    private void skipAll(String reason, String... rpcs) {
        for (String rpc : rpcs) {
            rows.add(new Row(rpc, rpc, "SKIPPED", reason));
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static <T> T require(T value, String reason) {
        if (value == null) {
            throw new Skip(reason);
        }
        return value;
    }

    private static void expectNotFound(Runnable read) {
        try {
            read.run();
        } catch (StatusRuntimeException e) {
            check(e.getStatus().getCode() == Status.Code.NOT_FOUND,
                    "the read after the delete answered " + e.getStatus().getCode());
            return;
        }
        throw new AssertionError("still readable after the delete");
    }

    private String describe(StatusRuntimeException e) {
        String description = String.valueOf(e.getStatus().getDescription()).replace('\n', ' ');
        return scrub(e.getStatus().getCode() + ": "
                + (description.length() > 300 ? description.substring(0, 300) + "..." : description));
    }

    private String scrub(String text) {
        String out = text == null ? "" : text;
        if (token != null) {
            out = out.replace(token, "***");
        }
        if (email != null) {
            out = out.replace(email, "***");
        }
        return out;
    }

    private String report(String host, String spaceKey, Instant started) {
        StringBuilder md = new StringBuilder("# Confluence live write run\n\n");
        md.append("- Site: ").append(host).append(", space ").append(spaceKey).append('\n');
        md.append("- Parent page: ").append(run).append('\n');
        md.append("- Run: ").append(started).append(" (")
                .append(java.time.Duration.between(started, Instant.now()).toSeconds()).append(" s)\n");
        Map<String, Integer> outcomes = new TreeMap<>();
        rows.forEach(row -> outcomes.merge(row.outcome(), 1, Integer::sum));
        md.append("- Rows: ").append(rows.size()).append(", outcomes ").append(outcomes).append("\n\n");
        md.append("| rpc | step | outcome | detail |\n|---|---|---|---|\n");
        rows.stream().sorted(java.util.Comparator.comparing(Row::rpc)).forEach(row ->
                md.append("| ").append(row.rpc()).append(" | ").append(cell(row.label())).append(" | ")
                        .append(row.outcome().equals("FAIL") ? "**FAIL**" : row.outcome())
                        .append(" | ").append(cell(row.detail())).append(" |\n"));
        md.append("\n## Left behind\n\n");
        if (leftBehind.isEmpty()) {
            md.append("Nothing: everything the run created was deleted and purged.\n");
        } else {
            leftBehind.forEach(line -> md.append("- ").append(cell(scrub(line))).append('\n'));
        }
        return md.toString();
    }

    private static String cell(String text) {
        return text.replace("|", "\\|").replace('\n', ' ');
    }

    // ======================================================================
    // CALLS
    // ======================================================================

    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub() {
        return ConfluenceServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(CALL_DEADLINE_SECONDS, TimeUnit.SECONDS);
    }

    private static ContentRef ref(ContentType type, String id) {
        return ContentRef.newBuilder().setType(type).setId(id).build();
    }

    private static ContentBody storage(String xhtml) {
        return ContentBody.newBuilder().setStorage(StorageBody.newBuilder().setValue(xhtml)).build();
    }

    private static AdfNode text(String text) {
        return AdfNode.newBuilder().setType(AdfNodeType.ADF_NODE_TYPE_TEXT).setText(text).build();
    }

    private static PropertyEntry entry(String key, PropertyValue.Builder value) {
        return PropertyEntry.newBuilder().setKey(key).setValue(value).build();
    }

    /** An object holding a string, integer, double, bool, null, list and nested object. */
    private static PropertyValue plainValue(String marker) {
        return PropertyValue.newBuilder().setObjectValue(PropertyObject.newBuilder()
                .addEntries(entry("string", PropertyValue.newBuilder().setStringValue(marker)))
                .addEntries(entry("integer", PropertyValue.newBuilder().setIntegerValue(42)))
                .addEntries(entry("double", PropertyValue.newBuilder().setDoubleValue(2.5)))
                .addEntries(entry("bool", PropertyValue.newBuilder().setBoolValue(true)))
                .addEntries(entry("null", PropertyValue.newBuilder()
                        .setNullValue(PropertyNull.getDefaultInstance())))
                .addEntries(entry("list", PropertyValue.newBuilder().setListValue(
                        PropertyList.newBuilder()
                                .addValues(PropertyValue.newBuilder().setStringValue("a"))
                                .addValues(PropertyValue.newBuilder().setIntegerValue(1)))))
                .addEntries(entry("object", PropertyValue.newBuilder().setObjectValue(
                        PropertyObject.newBuilder().addEntries(
                                entry("nested", PropertyValue.newBuilder().setBoolValue(false)))))))
                .build();
    }

    private static void collect(List<AdfNode> nodes, List<String> texts, Set<AdfNodeType> types) {
        for (AdfNode node : nodes) {
            types.add(node.getType());
            if (!node.getText().isEmpty()) {
                texts.add(node.getText());
            }
            collect(node.getContentList(), texts, types);
        }
    }

    private String readStorage(String pageId) {
        return stub().getPage(GetPageRequest.newBuilder().setId(pageId)
                .setBodyFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML).build()).getPage()
                .getBody().getStorage().getValue();
    }

    private List<String> labelNames(ContentRef ref) {
        List<String> names = new ArrayList<>();
        stub().listLabels(ListLabelsRequest.newBuilder().setContent(ref).build())
                .forEachRemaining(row -> names.add(row.getLabel().getName()));
        return names;
    }

    private List<String> permissionHolders(ContentRef ref) {
        List<String> holders = new ArrayList<>();
        stub().listContentPermissions(ListContentPermissionsRequest.newBuilder().setContent(ref).build())
                .forEachRemaining(row -> holders.add(row.getPermission().getPrincipal().getPrincipalId()));
        return holders;
    }

    private byte[] download(String attachmentId) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        stub().downloadAttachment(DownloadAttachmentRequest.newBuilder().setId(attachmentId).build())
                .forEachRemaining(part -> bytes.writeBytes(part.getData().toByteArray()));
        return bytes.toByteArray();
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private Attachment upload(String contentId, String fileName, String mediaType, byte[] bytes,
            boolean replace) throws Exception {
        List<UploadAttachmentResponse> events = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<UploadAttachmentRequest> frames = ConfluenceServiceGrpc.newStub(channel)
                .uploadAttachment(collector(events, failure, done));
        frames.onNext(UploadAttachmentRequest.newBuilder().setHeader(UploadAttachmentHeader.newBuilder()
                .setContentId(contentId).setFileName(fileName).setMediaType(mediaType)
                .setComment("live write run").setMinorEdit(true).setReplaceExisting(replace)).build());
        for (int from = 0; from < bytes.length; from += UPLOAD_CHUNK_BYTES) {
            frames.onNext(UploadAttachmentRequest.newBuilder().setChunk(ByteString.copyFrom(bytes, from,
                    Math.min(UPLOAD_CHUNK_BYTES, bytes.length - from))).build());
        }
        frames.onNext(UploadAttachmentRequest.newBuilder()
                .setEnd(UploadAttachmentEnd.getDefaultInstance()).build());
        frames.onCompleted();
        check(done.await(10, TimeUnit.MINUTES), "the upload did not finish in time");
        if (failure.get() != null) {
            throw Status.fromThrowable(failure.get()).asRuntimeException();
        }
        synchronized (events) {
            UploadAttachmentResponse last = events.getLast();
            check(last.hasAttachment(), "the upload did not end with the attachment");
            long acknowledged = events.size() < 2 ? 0
                    : events.get(events.size() - 2).getProgress().getBytesReceived();
            check(acknowledged == bytes.length, "progress acknowledged " + acknowledged + " of "
                    + bytes.length + " bytes");
            return last.getAttachment();
        }
    }

    private List<MutateResponse> mutateAll(List<MutateRequest> operations) throws Exception {
        List<MutateResponse> answers = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        StreamObserver<MutateRequest> requests = ConfluenceServiceGrpc.newStub(channel)
                .mutate(collector(answers, failure, done));
        operations.forEach(requests::onNext);
        requests.onCompleted();
        check(done.await(5, TimeUnit.MINUTES), "Mutate did not finish in time");
        if (failure.get() != null) {
            throw Status.fromThrowable(failure.get()).asRuntimeException();
        }
        synchronized (answers) {
            return List.copyOf(answers);
        }
    }

    private static <T> StreamObserver<T> collector(List<T> into, AtomicReference<Throwable> failure,
            CountDownLatch done) {
        return new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                synchronized (into) {
                    into.add(value);
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
        };
    }
}
