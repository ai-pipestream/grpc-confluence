package ai.pipestream.confluence;

import ai.pipestream.confluence.ConfluenceGrpcService.Session;
import ai.pipestream.confluence.ConfluenceRoutes.Query;
import ai.pipestream.confluence.v1.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.ProtocolMessageEnum;
import com.google.protobuf.Timestamp;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static ai.pipestream.confluence.ConfluenceRoutes.invalid;
import static ai.pipestream.confluence.ConfluenceRoutes.requireId;
import static ai.pipestream.confluence.ConfluenceRoutes.values;

/**
 * The write rpcs: each method turns one typed request into its REST call
 * and maps the answer back.
 */
final class ConfluenceWriter {

    /** One write rpc body. */
    @FunctionalInterface
    interface Op<Q, R> {
        /**
         * Runs the write.
         *
         * @param session the connection to write through
         * @param request the typed request
         * @return the typed response
         * @throws IOException if the REST call fails
         * @throws InterruptedException if the calling thread is interrupted
         */
        R apply(Session session, Q request) throws IOException, InterruptedException;
    }

    private static final JsonNodeFactory NODES = JsonNodeFactory.instance;
    private static final ConfluenceValidator VALIDATOR = ConfluenceValidator.create();
    private static final Map<String, String> NO_QUERY = Map.of();

    private static final Set<String> CURRENT_OR_DRAFT = values("current", "draft");
    private static final Set<String> CURRENT = values("current");
    /** Body formats pages, blog posts and comments accept. */
    private static final Set<ContentBody.FormatCase> TEXT_BODY = EnumSet.of(
            ContentBody.FormatCase.ADF, ContentBody.FormatCase.STORAGE, ContentBody.FormatCase.WIKI);
    /** Body formats custom content accepts. */
    private static final Set<ContentBody.FormatCase> CUSTOM_BODY = EnumSet.of(
            ContentBody.FormatCase.ADF, ContentBody.FormatCase.STORAGE, ContentBody.FormatCase.RAW);
    /** Content kinds with their own {@code /properties} route. */
    private static final Set<ContentType> PROPERTY_HOLDERS = EnumSet.of(
            ContentType.CONTENT_TYPE_PAGE, ContentType.CONTENT_TYPE_BLOG_POST,
            ContentType.CONTENT_TYPE_ATTACHMENT, ContentType.CONTENT_TYPE_CUSTOM_CONTENT,
            ContentType.CONTENT_TYPE_WHITEBOARD, ContentType.CONTENT_TYPE_DATABASE,
            ContentType.CONTENT_TYPE_FOLDER, ContentType.CONTENT_TYPE_SMART_LINK);
    /** Content kinds with a classification-level write route. */
    private static final Set<ContentType> CLASSIFIED = EnumSet.of(ContentType.CONTENT_TYPE_PAGE,
            ContentType.CONTENT_TYPE_BLOG_POST, ContentType.CONTENT_TYPE_DATABASE,
            ContentType.CONTENT_TYPE_WHITEBOARD);
    /** {@code POST /user/access/invite-by-email} takes at most this many emails per call. */
    private static final int INVITE_MAX_EMAILS = ConfluenceGrpcService.CHECK_ACCESS_MAX_EMAILS;

    private ConfluenceWriter() {
    }

    /** Every unary write, keyed by its request type. */
    static final Map<Class<? extends Message>, Op<Message, Message>> OPS = ops();

    private static Map<Class<? extends Message>, Op<Message, Message>> ops() {
        Map<Class<? extends Message>, Op<Message, Message>> ops = new java.util.HashMap<>();
        register(ops, CreatePageRequest.class, ConfluenceWriter::createPage);
        register(ops, UpdatePageRequest.class, ConfluenceWriter::updatePage);
        register(ops, DeletePageRequest.class, ConfluenceWriter::deletePage);
        register(ops, UpdatePageTitleRequest.class, ConfluenceWriter::updatePageTitle);
        register(ops, CreateBlogPostRequest.class, ConfluenceWriter::createBlogPost);
        register(ops, UpdateBlogPostRequest.class, ConfluenceWriter::updateBlogPost);
        register(ops, DeleteBlogPostRequest.class, ConfluenceWriter::deleteBlogPost);
        register(ops, CreateFooterCommentRequest.class, ConfluenceWriter::createFooterComment);
        register(ops, UpdateFooterCommentRequest.class, ConfluenceWriter::updateFooterComment);
        register(ops, DeleteFooterCommentRequest.class, ConfluenceWriter::deleteFooterComment);
        register(ops, CreateInlineCommentRequest.class, ConfluenceWriter::createInlineComment);
        register(ops, UpdateInlineCommentRequest.class, ConfluenceWriter::updateInlineComment);
        register(ops, DeleteInlineCommentRequest.class, ConfluenceWriter::deleteInlineComment);
        register(ops, CreateCustomContentRequest.class, ConfluenceWriter::createCustomContent);
        register(ops, UpdateCustomContentRequest.class, ConfluenceWriter::updateCustomContent);
        register(ops, DeleteCustomContentRequest.class, ConfluenceWriter::deleteCustomContent);
        register(ops, CreateDatabaseRequest.class, ConfluenceWriter::createDatabase);
        register(ops, DeleteDatabaseRequest.class, ConfluenceWriter::deleteDatabase);
        register(ops, CreateFolderRequest.class, ConfluenceWriter::createFolder);
        register(ops, DeleteFolderRequest.class, ConfluenceWriter::deleteFolder);
        register(ops, CreateWhiteboardRequest.class, ConfluenceWriter::createWhiteboard);
        register(ops, DeleteWhiteboardRequest.class, ConfluenceWriter::deleteWhiteboard);
        register(ops, CreateSmartLinkRequest.class, ConfluenceWriter::createSmartLink);
        register(ops, DeleteSmartLinkRequest.class, ConfluenceWriter::deleteSmartLink);
        register(ops, DeleteAttachmentRequest.class, ConfluenceWriter::deleteAttachment);
        register(ops, CreateSpaceRequest.class, ConfluenceWriter::createSpace);
        register(ops, UpdateTaskRequest.class, ConfluenceWriter::updateTask);
        register(ops, CreateContentPropertyRequest.class, ConfluenceWriter::createContentProperty);
        register(ops, UpdateContentPropertyRequest.class, ConfluenceWriter::updateContentProperty);
        register(ops, DeleteContentPropertyRequest.class, ConfluenceWriter::deleteContentProperty);
        register(ops, CreateSpacePropertyRequest.class, ConfluenceWriter::createSpaceProperty);
        register(ops, UpdateSpacePropertyRequest.class, ConfluenceWriter::updateSpaceProperty);
        register(ops, DeleteSpacePropertyRequest.class, ConfluenceWriter::deleteSpaceProperty);
        register(ops, SetAppPropertyRequest.class, ConfluenceWriter::setAppProperty);
        register(ops, DeleteAppPropertyRequest.class, ConfluenceWriter::deleteAppProperty);
        register(ops, AddLabelsRequest.class, ConfluenceWriter::addLabels);
        register(ops, RemoveLabelRequest.class, ConfluenceWriter::removeLabel);
        register(ops, AddContentPermissionsRequest.class, ConfluenceWriter::addContentPermissions);
        register(ops, SetContentPermissionsRequest.class, ConfluenceWriter::setContentPermissions);
        register(ops, DeleteContentPermissionsRequest.class, ConfluenceWriter::deleteContentPermissions);
        register(ops, SetRestrictionStateRequest.class, ConfluenceWriter::setRestrictionState);
        register(ops, SetContentClassificationLevelRequest.class, ConfluenceWriter::setContentClassificationLevel);
        register(ops, ResetContentClassificationLevelRequest.class, ConfluenceWriter::resetContentClassificationLevel);
        register(ops, UpdateSpaceDefaultClassificationLevelRequest.class, ConfluenceWriter::updateSpaceDefaultClassificationLevel);
        register(ops, DeleteSpaceDefaultClassificationLevelRequest.class, ConfluenceWriter::deleteSpaceDefaultClassificationLevel);
        register(ops, CreateSpaceRoleRequest.class, ConfluenceWriter::createSpaceRole);
        register(ops, UpdateSpaceRoleRequest.class, ConfluenceWriter::updateSpaceRole);
        register(ops, DeleteSpaceRoleRequest.class, ConfluenceWriter::deleteSpaceRole);
        register(ops, SetSpaceRoleAssignmentsRequest.class, ConfluenceWriter::setSpaceRoleAssignments);
        register(ops, BulkAssignSpaceRolesRequest.class, ConfluenceWriter::bulkAssignSpaceRoles);
        register(ops, BulkRemoveSpaceAccessRequest.class, ConfluenceWriter::bulkRemoveSpaceAccess);
        register(ops, InviteByEmailRequest.class, ConfluenceWriter::inviteByEmail);
        register(ops, RedactPageRequest.class, ConfluenceWriter::redactPage);
        register(ops, RedactBlogPostRequest.class, ConfluenceWriter::redactBlogPost);
        register(ops, EnableAdminKeyRequest.class, ConfluenceWriter::enableAdminKey);
        register(ops, DisableAdminKeyRequest.class, ConfluenceWriter::disableAdminKey);
        return Map.copyOf(ops);
    }

    private static <Q extends Message, R extends Message> void register(
            Map<Class<? extends Message>, Op<Message, Message>> ops, Class<Q> type, Op<Q, R> op) {
        ops.put(type, (session, request) -> op.apply(session, type.cast(request)));
    }

    // ======================================================================
    // PAGES AND BLOG POSTS
    // ======================================================================

    static CreatePageResponse createPage(Session session, CreatePageRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        put(body, "spaceId", requireId(request.getSpaceId(), "space_id"));
        put(body, "status", optionalStatus(request.getStatus(), CURRENT_OR_DRAFT, "CreatePage"));
        put(body, "title", request.getTitle());
        put(body, "parentId", request.getParentId());
        putBody(body, request.getBody(), TEXT_BODY, "CreatePage");
        if (request.getSubtype() == PageSubtype.PAGE_SUBTYPE_LIVE) {
            body.put("subtype", "live");
        }
        Query query = new Query().flag("embedded", request.getEmbedded())
                .flag("private", request.getPrivate()).flag("root-level", request.getRootLevel());
        JsonNode node = session.client().post("/api/v2/pages", query.map(), body);
        return CreatePageResponse.newBuilder().setPage(valid(session.mapper().toPage(node))).build();
    }

    static UpdatePageResponse updatePage(Session session, UpdatePageRequest request)
            throws IOException, InterruptedException {
        String id = id(request.getId(), "id");
        ObjectNode body = NODES.objectNode();
        body.put("id", id);
        body.put("status", status(request.getStatus(), CURRENT_OR_DRAFT, "UpdatePage"));
        body.put("title", request.getTitle());
        put(body, "spaceId", request.getSpaceId());
        put(body, "parentId", request.getParentId());
        put(body, "ownerId", request.getOwnerId());
        putBody(body, request.getBody(), TEXT_BODY, "UpdatePage");
        body.set("version", version(request.getExpectedVersion(), request.getVersionMessage()));
        JsonNode node = session.client().put("/api/v2/pages/" + id, NO_QUERY, body);
        return UpdatePageResponse.newBuilder().setPage(valid(session.mapper().toPage(node))).build();
    }

    static DeletePageResponse deletePage(Session session, DeletePageRequest request)
            throws IOException, InterruptedException {
        session.client().delete("/api/v2/pages/" + id(request.getId(), "id"),
                new Query().flag("purge", request.getPurge()).flag("draft", request.getDraft()).map());
        return DeletePageResponse.getDefaultInstance();
    }

    static UpdatePageTitleResponse updatePageTitle(Session session, UpdatePageTitleRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        body.put("status", status(request.getStatus(), CURRENT_OR_DRAFT, "UpdatePageTitle"));
        body.put("title", request.getTitle());
        JsonNode node = session.client().put("/api/v2/pages/" + id(request.getId(), "id") + "/title",
                NO_QUERY, body);
        return UpdatePageTitleResponse.newBuilder().setPage(valid(session.mapper().toPage(node)))
                .build();
    }

    static CreateBlogPostResponse createBlogPost(Session session, CreateBlogPostRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        put(body, "spaceId", requireId(request.getSpaceId(), "space_id"));
        put(body, "status", optionalStatus(request.getStatus(), CURRENT_OR_DRAFT, "CreateBlogPost"));
        put(body, "title", request.getTitle());
        putBody(body, request.getBody(), TEXT_BODY, "CreateBlogPost");
        putTime(body, "createdAt", request.hasCreatedAt(), request.getCreatedAt());
        JsonNode node = session.client().post("/api/v2/blogposts",
                new Query().flag("private", request.getPrivate()).map(), body);
        return CreateBlogPostResponse.newBuilder()
                .setBlogPost(valid(session.mapper().toBlogPost(node))).build();
    }

    static UpdateBlogPostResponse updateBlogPost(Session session, UpdateBlogPostRequest request)
            throws IOException, InterruptedException {
        String id = id(request.getId(), "id");
        ObjectNode body = NODES.objectNode();
        body.put("id", id);
        body.put("status", status(request.getStatus(), CURRENT_OR_DRAFT, "UpdateBlogPost"));
        body.put("title", request.getTitle());
        put(body, "spaceId", request.getSpaceId());
        putBody(body, request.getBody(), TEXT_BODY, "UpdateBlogPost");
        body.set("version", version(request.getExpectedVersion(), request.getVersionMessage()));
        putTime(body, "createdAt", request.hasCreatedAt(), request.getCreatedAt());
        JsonNode node = session.client().put("/api/v2/blogposts/" + id, NO_QUERY, body);
        return UpdateBlogPostResponse.newBuilder()
                .setBlogPost(valid(session.mapper().toBlogPost(node))).build();
    }

    static DeleteBlogPostResponse deleteBlogPost(Session session, DeleteBlogPostRequest request)
            throws IOException, InterruptedException {
        session.client().delete("/api/v2/blogposts/" + id(request.getId(), "id"),
                new Query().flag("purge", request.getPurge()).flag("draft", request.getDraft()).map());
        return DeleteBlogPostResponse.getDefaultInstance();
    }

    // ======================================================================
    // COMMENTS
    // ======================================================================

    static CreateFooterCommentResponse createFooterComment(Session session,
            CreateFooterCommentRequest request) throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        put(body, "blogPostId", request.getBlogPostId());
        put(body, "pageId", request.getPageId());
        put(body, "parentCommentId", request.getParentCommentId());
        put(body, "attachmentId", request.getAttachmentId());
        put(body, "customContentId", request.getCustomContentId());
        putBody(body, request.getBody(), TEXT_BODY, "CreateFooterComment");
        JsonNode node = session.client().post("/api/v2/footer-comments", NO_QUERY, body);
        return CreateFooterCommentResponse.newBuilder().setComment(valid(
                session.mapper().toComment(node, CommentKind.COMMENT_KIND_FOOTER))).build();
    }

    static UpdateFooterCommentResponse updateFooterComment(Session session,
            UpdateFooterCommentRequest request) throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        body.set("version", version(request.getExpectedVersion(), request.getVersionMessage()));
        putBody(body, request.getBody(), TEXT_BODY, "UpdateFooterComment");
        JsonNode node = session.client().put(
                "/api/v2/footer-comments/" + id(request.getId(), "id"), NO_QUERY, body);
        return UpdateFooterCommentResponse.newBuilder().setComment(valid(
                session.mapper().toComment(node, CommentKind.COMMENT_KIND_FOOTER))).build();
    }

    static DeleteFooterCommentResponse deleteFooterComment(Session session,
            DeleteFooterCommentRequest request) throws IOException, InterruptedException {
        session.client().delete("/api/v2/footer-comments/" + id(request.getId(), "id"), NO_QUERY);
        return DeleteFooterCommentResponse.getDefaultInstance();
    }

    static CreateInlineCommentResponse createInlineComment(Session session,
            CreateInlineCommentRequest request) throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        put(body, "blogPostId", request.getBlogPostId());
        put(body, "pageId", request.getPageId());
        put(body, "parentCommentId", request.getParentCommentId());
        putBody(body, request.getBody(), TEXT_BODY, "CreateInlineComment");
        if (request.hasSelection()) {
            InlineCommentSelection selection = request.getSelection();
            body.putObject("inlineCommentProperties")
                    .put("textSelection", selection.getTextSelection())
                    .put("textSelectionMatchCount", selection.getTextSelectionMatchCount())
                    .put("textSelectionMatchIndex", selection.getTextSelectionMatchIndex());
        }
        JsonNode node = session.client().post("/api/v2/inline-comments", NO_QUERY, body);
        return CreateInlineCommentResponse.newBuilder().setComment(valid(
                session.mapper().toComment(node, CommentKind.COMMENT_KIND_INLINE))).build();
    }

    static UpdateInlineCommentResponse updateInlineComment(Session session,
            UpdateInlineCommentRequest request) throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        body.set("version", version(request.getExpectedVersion(), request.getVersionMessage()));
        putBody(body, request.getBody(), TEXT_BODY, "UpdateInlineComment");
        if (request.hasResolved()) {
            body.put("resolved", request.getResolved());
        }
        JsonNode node = session.client().put(
                "/api/v2/inline-comments/" + id(request.getId(), "id"), NO_QUERY, body);
        return UpdateInlineCommentResponse.newBuilder().setComment(valid(
                session.mapper().toComment(node, CommentKind.COMMENT_KIND_INLINE))).build();
    }

    static DeleteInlineCommentResponse deleteInlineComment(Session session,
            DeleteInlineCommentRequest request) throws IOException, InterruptedException {
        session.client().delete("/api/v2/inline-comments/" + id(request.getId(), "id"), NO_QUERY);
        return DeleteInlineCommentResponse.getDefaultInstance();
    }

    // ======================================================================
    // CUSTOM CONTENT
    // ======================================================================

    static CreateCustomContentResponse createCustomContent(Session session,
            CreateCustomContentRequest request) throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        body.put("type", request.getType());
        put(body, "status", optionalStatus(request.getStatus(), CURRENT_OR_DRAFT, "CreateCustomContent"));
        put(body, "spaceId", request.getSpaceId());
        put(body, "pageId", request.getPageId());
        put(body, "blogPostId", request.getBlogPostId());
        put(body, "customContentId", request.getCustomContentId());
        body.put("title", request.getTitle());
        putBody(body, request.getBody(), CUSTOM_BODY, "CreateCustomContent");
        JsonNode node = session.client().post("/api/v2/custom-content", NO_QUERY, body);
        return CreateCustomContentResponse.newBuilder()
                .setCustomContent(valid(session.mapper().toCustomContent(node))).build();
    }

    static UpdateCustomContentResponse updateCustomContent(Session session,
            UpdateCustomContentRequest request) throws IOException, InterruptedException {
        String id = id(request.getId(), "id");
        ObjectNode body = NODES.objectNode();
        body.put("id", id);
        body.put("type", request.getType());
        body.put("status", status(request.getStatus(), CURRENT, "UpdateCustomContent"));
        put(body, "spaceId", request.getSpaceId());
        put(body, "pageId", request.getPageId());
        put(body, "blogPostId", request.getBlogPostId());
        put(body, "customContentId", request.getCustomContentId());
        body.put("title", request.getTitle());
        putBody(body, request.getBody(), CUSTOM_BODY, "UpdateCustomContent");
        body.set("version", version(request.getExpectedVersion(), request.getVersionMessage()));
        JsonNode node = session.client().put("/api/v2/custom-content/" + id, NO_QUERY, body);
        return UpdateCustomContentResponse.newBuilder()
                .setCustomContent(valid(session.mapper().toCustomContent(node))).build();
    }

    static DeleteCustomContentResponse deleteCustomContent(Session session,
            DeleteCustomContentRequest request) throws IOException, InterruptedException {
        session.client().delete("/api/v2/custom-content/" + id(request.getId(), "id"),
                new Query().flag("purge", request.getPurge()).map());
        return DeleteCustomContentResponse.getDefaultInstance();
    }

    // ======================================================================
    // DATABASES, FOLDERS, WHITEBOARDS, SMART LINKS, ATTACHMENTS
    // ======================================================================

    static CreateDatabaseResponse createDatabase(Session session, CreateDatabaseRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = treeNode(request.getSpaceId(), request.getTitle(), request.getParentId());
        JsonNode node = session.client().post("/api/v2/databases",
                new Query().flag("private", request.getPrivate()).map(), body);
        return CreateDatabaseResponse.newBuilder()
                .setDatabase(valid(session.mapper().toDatabase(node))).build();
    }

    static DeleteDatabaseResponse deleteDatabase(Session session, DeleteDatabaseRequest request)
            throws IOException, InterruptedException {
        session.client().delete("/api/v2/databases/" + id(request.getId(), "id"), NO_QUERY);
        return DeleteDatabaseResponse.getDefaultInstance();
    }

    static CreateFolderResponse createFolder(Session session, CreateFolderRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = treeNode(request.getSpaceId(), request.getTitle(), request.getParentId());
        JsonNode node = session.client().post("/api/v2/folders", NO_QUERY, body);
        return CreateFolderResponse.newBuilder().setFolder(valid(session.mapper().toFolder(node)))
                .build();
    }

    static DeleteFolderResponse deleteFolder(Session session, DeleteFolderRequest request)
            throws IOException, InterruptedException {
        session.client().delete("/api/v2/folders/" + id(request.getId(), "id"), NO_QUERY);
        return DeleteFolderResponse.getDefaultInstance();
    }

    static CreateWhiteboardResponse createWhiteboard(Session session, CreateWhiteboardRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = treeNode(request.getSpaceId(), request.getTitle(), request.getParentId());
        if (request.getTemplateKey() != WhiteboardTemplateKey.WHITEBOARD_TEMPLATE_KEY_UNSPECIFIED) {
            body.put("templateKey", kebab(request.getTemplateKey(), "WHITEBOARD_TEMPLATE_KEY_",
                    "template_key"));
        }
        if (request.getLocale() != WhiteboardLocale.WHITEBOARD_LOCALE_UNSPECIFIED) {
            String locale = upper(request.getLocale(), "WHITEBOARD_LOCALE_", "locale");
            int split = locale.indexOf('_');
            body.put("locale", locale.substring(0, split).toLowerCase(Locale.ROOT) + "-"
                    + locale.substring(split + 1));
        }
        JsonNode node = session.client().post("/api/v2/whiteboards",
                new Query().flag("private", request.getPrivate()).map(), body);
        return CreateWhiteboardResponse.newBuilder()
                .setWhiteboard(valid(session.mapper().toWhiteboard(node))).build();
    }

    static DeleteWhiteboardResponse deleteWhiteboard(Session session, DeleteWhiteboardRequest request)
            throws IOException, InterruptedException {
        session.client().delete("/api/v2/whiteboards/" + id(request.getId(), "id"), NO_QUERY);
        return DeleteWhiteboardResponse.getDefaultInstance();
    }

    static CreateSmartLinkResponse createSmartLink(Session session, CreateSmartLinkRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = treeNode(request.getSpaceId(), request.getTitle(), request.getParentId());
        put(body, "embedUrl", request.getEmbedUrl());
        JsonNode node = session.client().post("/api/v2/embeds", NO_QUERY, body);
        return CreateSmartLinkResponse.newBuilder()
                .setSmartLink(valid(session.mapper().toSmartLink(node))).build();
    }

    static DeleteSmartLinkResponse deleteSmartLink(Session session, DeleteSmartLinkRequest request)
            throws IOException, InterruptedException {
        session.client().delete("/api/v2/embeds/" + id(request.getId(), "id"), NO_QUERY);
        return DeleteSmartLinkResponse.getDefaultInstance();
    }

    static DeleteAttachmentResponse deleteAttachment(Session session, DeleteAttachmentRequest request)
            throws IOException, InterruptedException {
        session.client().delete("/api/v2/attachments/" + id(request.getId(), "id"),
                new Query().flag("purge", request.getPurge()).map());
        return DeleteAttachmentResponse.getDefaultInstance();
    }

    // ======================================================================
    // SPACES AND TASKS
    // ======================================================================

    static CreateSpaceResponse createSpace(Session session, CreateSpaceRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        body.put("name", request.getName());
        put(body, "key", request.getKey());
        put(body, "alias", request.getAlias());
        if (!request.getDescription().isEmpty()) {
            body.putObject("description").put("value", request.getDescription())
                    .put("representation", "plain");
        }
        if (request.getRoleAssignmentsCount() > 0) {
            body.set("roleAssignments", roleAssignments(request.getRoleAssignmentsList()));
        }
        if (request.hasCopySpaceAccessConfiguration()) {
            body.put("copySpaceAccessConfiguration", request.getCopySpaceAccessConfiguration());
        }
        if (request.getCreatePrivateSpace()) {
            body.put("createPrivateSpace", true);
        }
        put(body, "templateKey", request.getTemplateKey());
        JsonNode node = session.client().post("/api/v2/spaces", NO_QUERY, body);
        return CreateSpaceResponse.newBuilder().setSpace(valid(session.mapper().toSpace(node)))
                .build();
    }

    static UpdateTaskResponse updateTask(Session session, UpdateTaskRequest request)
            throws IOException, InterruptedException {
        String id = id(request.getId(), "id");
        ObjectNode body = NODES.objectNode();
        body.put("id", id);
        put(body, "localId", request.getLocalId());
        put(body, "spaceId", request.getSpaceId());
        put(body, "pageId", request.getPageId());
        put(body, "blogPostId", request.getBlogPostId());
        body.put("status", lower(request.getStatus(), "TASK_STATUS_", "status"));
        put(body, "createdBy", request.getCreatedBy());
        put(body, "assignedTo", request.getAssignedTo());
        put(body, "completedBy", request.getCompletedBy());
        putTime(body, "createdAt", request.hasCreatedAt(), request.getCreatedAt());
        putTime(body, "updatedAt", request.hasUpdatedAt(), request.getUpdatedAt());
        putTime(body, "dueAt", request.hasDueAt(), request.getDueAt());
        putTime(body, "completedAt", request.hasCompletedAt(), request.getCompletedAt());
        Query query = new Query();
        if (request.getBodyFormat() != BodyFormat.BODY_FORMAT_UNSPECIFIED) {
            query.put("body-format", ConfluenceRoutes.bodyFormat(request.getBodyFormat(),
                    ConfluenceRoutes.PRIMARY_BODY, "UpdateTask"));
        }
        JsonNode node = session.client().put("/api/v2/tasks/" + id, query.map(), body);
        return UpdateTaskResponse.newBuilder().setTask(valid(session.mapper().toTask(node))).build();
    }

    // ======================================================================
    // PROPERTIES
    // ======================================================================

    static CreateContentPropertyResponse createContentProperty(Session session,
            CreateContentPropertyRequest request) throws IOException, InterruptedException {
        ObjectNode body = property(request.getKey(), request.getCustomKey(), request.getValue());
        JsonNode node = session.client().post(
                propertyOwner(request.getContent(), "CreateContentProperty") + "/properties",
                NO_QUERY, body);
        return CreateContentPropertyResponse.newBuilder()
                .setProperty(valid(session.mapper().toContentProperty(node))).build();
    }

    static UpdateContentPropertyResponse updateContentProperty(Session session,
            UpdateContentPropertyRequest request) throws IOException, InterruptedException {
        ObjectNode body = property(request.getKey(), request.getCustomKey(), request.getValue());
        body.set("version", version(request.getExpectedVersion(), request.getVersionMessage()));
        JsonNode node = session.client().put(
                propertyOwner(request.getContent(), "UpdateContentProperty") + "/properties/"
                        + id(request.getPropertyId(), "property_id"), NO_QUERY, body);
        return UpdateContentPropertyResponse.newBuilder()
                .setProperty(valid(session.mapper().toContentProperty(node))).build();
    }

    static DeleteContentPropertyResponse deleteContentProperty(Session session,
            DeleteContentPropertyRequest request) throws IOException, InterruptedException {
        session.client().delete(
                propertyOwner(request.getContent(), "DeleteContentProperty") + "/properties/"
                        + id(request.getPropertyId(), "property_id"), NO_QUERY);
        return DeleteContentPropertyResponse.getDefaultInstance();
    }

    static CreateSpacePropertyResponse createSpaceProperty(Session session,
            CreateSpacePropertyRequest request) throws IOException, InterruptedException {
        ObjectNode body = property(request.getKey(), request.getCustomKey(), request.getValue());
        JsonNode node = session.client().post(
                "/api/v2/spaces/" + id(request.getSpaceId(), "space_id") + "/properties", NO_QUERY, body);
        return CreateSpacePropertyResponse.newBuilder()
                .setProperty(valid(session.mapper().toSpaceProperty(node))).build();
    }

    static UpdateSpacePropertyResponse updateSpaceProperty(Session session,
            UpdateSpacePropertyRequest request) throws IOException, InterruptedException {
        ObjectNode body = property(request.getKey(), request.getCustomKey(), request.getValue());
        body.set("version", version(request.getExpectedVersion(), request.getVersionMessage()));
        JsonNode node = session.client().put(
                "/api/v2/spaces/" + id(request.getSpaceId(), "space_id") + "/properties/"
                        + id(request.getPropertyId(), "property_id"), NO_QUERY, body);
        return UpdateSpacePropertyResponse.newBuilder()
                .setProperty(valid(session.mapper().toSpaceProperty(node))).build();
    }

    static DeleteSpacePropertyResponse deleteSpaceProperty(Session session,
            DeleteSpacePropertyRequest request) throws IOException, InterruptedException {
        session.client().delete(
                "/api/v2/spaces/" + id(request.getSpaceId(), "space_id") + "/properties/"
                        + id(request.getPropertyId(), "property_id"), NO_QUERY);
        return DeleteSpacePropertyResponse.getDefaultInstance();
    }

    static SetAppPropertyResponse setAppProperty(Session session, SetAppPropertyRequest request)
            throws IOException, InterruptedException {
        ConfluenceClient.JsonResponse response = session.client().exchange("PUT",
                appProperty(request.getKey()), NO_QUERY,
                PropertyJson.toJson(request.getValue(), "value"));
        return SetAppPropertyResponse.newBuilder().setCreated(response.status() == 201).build();
    }

    static DeleteAppPropertyResponse deleteAppProperty(Session session,
            DeleteAppPropertyRequest request) throws IOException, InterruptedException {
        session.client().delete(appProperty(request.getKey()), NO_QUERY);
        return DeleteAppPropertyResponse.getDefaultInstance();
    }

    // ======================================================================
    // LABELS (REST API v1)
    // ======================================================================

    static AddLabelsResponse addLabels(Session session, AddLabelsRequest request)
            throws IOException, InterruptedException {
        if (request.getLabelsCount() == 0) {
            throw invalid("labels is required");
        }
        ArrayNode body = NODES.arrayNode();
        for (LabelInput label : request.getLabelsList()) {
            String prefix = label.getPrefix() == LabelPrefix.LABEL_PREFIX_UNSPECIFIED ? "global"
                    : lower(label.getPrefix(), "LABEL_PREFIX_", "prefix");
            body.addObject().put("prefix", prefix).put("name", requireId(label.getName(), "name"));
        }
        JsonNode node = session.client().post(labelPath(request.getContentId()), NO_QUERY, body);
        AddLabelsResponse.Builder response = AddLabelsResponse.newBuilder();
        node.path("results").forEach(label ->
                response.addLabels(valid(session.mapper().toLabel(label))));
        return response.build();
    }

    static RemoveLabelResponse removeLabel(Session session, RemoveLabelRequest request)
            throws IOException, InterruptedException {
        session.client().delete(labelPath(request.getContentId()),
                new Query().put("name", requireId(request.getName(), "name")).map());
        return RemoveLabelResponse.getDefaultInstance();
    }

    /** The v1 label route of a piece of content. */
    private static String labelPath(String contentId) {
        return "/rest/api/content/" + id(contentId, "content_id") + "/label";
    }

    /** The v1 attachment upload route of a piece of content. */
    static String attachmentUploadPath(String contentId) {
        return "/rest/api/content/" + id(contentId, "content_id") + "/child/attachment";
    }

    // ======================================================================
    // CONTENT PERMISSIONS
    // ======================================================================

    static AddContentPermissionsResponse addContentPermissions(Session session,
            AddContentPermissionsRequest request) throws IOException, InterruptedException {
        JsonNode node = session.client().post(
                permissionOwner(request.getContent(), "AddContentPermissions") + "/permissions",
                selfLockout(request.getAllowSelfLockout()), grants(request.getPermissionsList()));
        AddContentPermissionsResponse.Builder response = AddContentPermissionsResponse.newBuilder();
        node.path("results").forEach(entry ->
                response.addPermissions(session.mapper().toContentPermission(entry)));
        return response.build();
    }

    static SetContentPermissionsResponse setContentPermissions(Session session,
            SetContentPermissionsRequest request) throws IOException, InterruptedException {
        JsonNode node = session.client().put(
                permissionOwner(request.getContent(), "SetContentPermissions") + "/permissions",
                selfLockout(request.getAllowSelfLockout()), grants(request.getPermissionsList()));
        SetContentPermissionsResponse.Builder response = SetContentPermissionsResponse.newBuilder();
        node.path("results").forEach(entry ->
                response.addPermissions(session.mapper().toContentPermission(entry)));
        return response.build();
    }

    static DeleteContentPermissionsResponse deleteContentPermissions(Session session,
            DeleteContentPermissionsRequest request) throws IOException, InterruptedException {
        Query query = new Query().flag("allow-self-lockout", request.getAllowSelfLockout());
        boolean byOperation = request.getOperation()
                != ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_UNSPECIFIED;
        if (byOperation && !request.hasPrincipal()) {
            throw invalid("operation needs principal");
        }
        if (request.hasPrincipal()) {
            ContentPermissionPrincipal principal = request.getPrincipal();
            query.put("principal-type", lower(principal.getPrincipalType(),
                    "CONTENT_PERMISSION_PRINCIPAL_TYPE_", "principal.principal_type"));
            query.put("principal-id", requireId(principal.getPrincipalId(), "principal.principal_id"));
        }
        if (byOperation) {
            query.put("operation", lower(request.getOperation(), "CONTENT_PERMISSION_OPERATION_",
                    "operation"));
        }
        session.client().delete(
                permissionOwner(request.getContent(), "DeleteContentPermissions") + "/permissions",
                query.map());
        return DeleteContentPermissionsResponse.getDefaultInstance();
    }

    static SetRestrictionStateResponse setRestrictionState(Session session,
            SetRestrictionStateRequest request) throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        body.put("state", upper(request.getState(), "CONTENT_RESTRICTION_STATE_", "state"));
        JsonNode node = session.client().put(
                permissionOwner(request.getContent(), "SetRestrictionState") + "/restriction-state",
                selfLockout(request.getAllowSelfLockout()), body);
        return SetRestrictionStateResponse.newBuilder()
                .setRestrictionState(session.mapper().toRestrictionState(node)).build();
    }

    // ======================================================================
    // CLASSIFICATION
    // ======================================================================

    static SetContentClassificationLevelResponse setContentClassificationLevel(Session session,
            SetContentClassificationLevelRequest request) throws IOException, InterruptedException {
        String route = "SetContentClassificationLevel";
        ObjectNode body = NODES.objectNode();
        body.put("id", requireId(request.getClassificationLevelId(), "classification_level_id"));
        body.put("status", classificationStatus(request.getContent(), request.getStatus(), route));
        session.client().put(ConfluenceRoutes.contentPath(request.getContent(), CLASSIFIED, route)
                + "/classification-level", NO_QUERY, body);
        return SetContentClassificationLevelResponse.getDefaultInstance();
    }

    static ResetContentClassificationLevelResponse resetContentClassificationLevel(Session session,
            ResetContentClassificationLevelRequest request) throws IOException, InterruptedException {
        String route = "ResetContentClassificationLevel";
        ObjectNode body = NODES.objectNode();
        body.put("status", classificationStatus(request.getContent(), request.getStatus(), route));
        session.client().post(ConfluenceRoutes.contentPath(request.getContent(), CLASSIFIED, route)
                + "/classification-level/reset", NO_QUERY, body);
        return ResetContentClassificationLevelResponse.getDefaultInstance();
    }

    static UpdateSpaceDefaultClassificationLevelResponse updateSpaceDefaultClassificationLevel(
            Session session, UpdateSpaceDefaultClassificationLevelRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        body.put("id", requireId(request.getClassificationLevelId(), "classification_level_id"));
        session.client().put("/api/v2/spaces/" + id(request.getSpaceId(), "space_id")
                + "/classification-level/default", NO_QUERY, body);
        return UpdateSpaceDefaultClassificationLevelResponse.getDefaultInstance();
    }

    static DeleteSpaceDefaultClassificationLevelResponse deleteSpaceDefaultClassificationLevel(
            Session session, DeleteSpaceDefaultClassificationLevelRequest request)
            throws IOException, InterruptedException {
        session.client().delete("/api/v2/spaces/" + id(request.getSpaceId(), "space_id")
                + "/classification-level/default", NO_QUERY);
        return DeleteSpaceDefaultClassificationLevelResponse.getDefaultInstance();
    }

    // ======================================================================
    // SPACE ROLES AND ACCESS
    // ======================================================================

    static CreateSpaceRoleResponse createSpaceRole(Session session, CreateSpaceRoleRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = role(request.getName(), request.getDescription(),
                request.getSpacePermissionsList());
        JsonNode node = session.client().post("/api/v2/space-roles", NO_QUERY, body);
        return CreateSpaceRoleResponse.newBuilder().setRole(session.mapper().toSpaceRole(node))
                .build();
    }

    static UpdateSpaceRoleResponse updateSpaceRole(Session session, UpdateSpaceRoleRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = role(request.getName(), request.getDescription(),
                request.getSpacePermissionsList());
        put(body, "anonymousReassignmentRoleId", request.getAnonymousReassignmentRoleId());
        put(body, "guestReassignmentRoleId", request.getGuestReassignmentRoleId());
        JsonNode node = session.client().put("/api/v2/space-roles/" + id(request.getId(), "id"),
                NO_QUERY, body);
        return UpdateSpaceRoleResponse.newBuilder().setRole(session.mapper().toSpaceRole(node))
                .setTaskId(node.path("taskId").asText("")).build();
    }

    static DeleteSpaceRoleResponse deleteSpaceRole(Session session, DeleteSpaceRoleRequest request)
            throws IOException, InterruptedException {
        JsonNode node = session.client().delete("/api/v2/space-roles/" + id(request.getId(), "id"),
                NO_QUERY);
        return DeleteSpaceRoleResponse.newBuilder().setTaskId(node.path("taskId").asText("")).build();
    }

    static SetSpaceRoleAssignmentsResponse setSpaceRoleAssignments(Session session,
            SetSpaceRoleAssignmentsRequest request) throws IOException, InterruptedException {
        JsonNode node = session.client().post("/api/v2/spaces/" + id(request.getSpaceId(), "space_id")
                + "/role-assignments", NO_QUERY, roleAssignments(request.getAssignmentsList()));
        SetSpaceRoleAssignmentsResponse.Builder response = SetSpaceRoleAssignmentsResponse.newBuilder();
        node.path("results").forEach(entry ->
                response.addAssignments(session.mapper().toSpaceRoleAssignment(entry)));
        return response.build();
    }

    static BulkAssignSpaceRolesResponse bulkAssignSpaceRoles(Session session,
            BulkAssignSpaceRolesRequest request) throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        ArrayNode assignments = body.putArray("assignments");
        for (BulkTransitionRoleAssignment assignment : request.getAssignmentsList()) {
            ObjectNode entry = assignments.addObject();
            entry.put("permissionCombinationId", assignment.getPermissionCombinationId());
            ArrayNode types = entry.putArray("principalTypeAssignments");
            for (BulkTransitionPrincipalTypeAssignment type
                    : assignment.getPrincipalTypeAssignmentsList()) {
                ObjectNode item = types.addObject();
                item.put("principalType", upper(type.getPrincipalType(),
                        "BULK_TRANSITION_PRINCIPAL_TYPE_", "principal_type"));
                item.put("removeAccess", type.getRemoveAccess());
                if (type.hasRoleId()) {
                    item.put("roleId", type.getRoleId());
                }
            }
        }
        body.set("spaceSelection", spaceSelection(request.getSpaceSelection()));
        JsonNode node = session.client().post("/api/v2/space-permissions/transition/role-assignments",
                NO_QUERY, body);
        return BulkAssignSpaceRolesResponse.newBuilder().setTask(transitionTask(node)).build();
    }

    static BulkRemoveSpaceAccessResponse bulkRemoveSpaceAccess(Session session,
            BulkRemoveSpaceAccessRequest request) throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        ArrayNode ids = body.putArray("permissionCombinationIds");
        request.getPermissionCombinationIdsList().forEach(ids::add);
        body.set("spaceSelection", spaceSelection(request.getSpaceSelection()));
        JsonNode node = session.client().post("/api/v2/space-permissions/transition/access-removals",
                NO_QUERY, body);
        return BulkRemoveSpaceAccessResponse.newBuilder().setTask(transitionTask(node)).build();
    }

    static InviteByEmailResponse inviteByEmail(Session session, InviteByEmailRequest request)
            throws IOException, InterruptedException {
        List<String> emails = request.getEmailsList();
        if (emails.isEmpty()) {
            throw invalid("emails is required");
        }
        for (int from = 0; from < emails.size(); from += INVITE_MAX_EMAILS) {
            ObjectNode body = NODES.objectNode();
            ArrayNode chunk = body.putArray("emails");
            emails.subList(from, Math.min(emails.size(), from + INVITE_MAX_EMAILS)).forEach(chunk::add);
            session.client().post("/api/v2/user/access/invite-by-email", NO_QUERY, body);
        }
        return InviteByEmailResponse.getDefaultInstance();
    }

    // ======================================================================
    // REDACTION AND ADMIN KEY
    // ======================================================================

    static RedactPageResponse redactPage(Session session, RedactPageRequest request)
            throws IOException, InterruptedException {
        JsonNode node = session.client().post("/api/v2/pages/" + id(request.getId(), "id") + "/redact",
                NO_QUERY, redaction(request.hasCreatedAt(), request.getCreatedAt(),
                        request.hasCleanHistory(), request.getCleanHistory(),
                        request.hasVersionNumber(), request.getVersionNumber(),
                        request.getBodyRedactionsList(), request.getTitleRedactionsList()));
        return RedactPageResponse.newBuilder().setRedactions(valid(redactions(node))).build();
    }

    static RedactBlogPostResponse redactBlogPost(Session session, RedactBlogPostRequest request)
            throws IOException, InterruptedException {
        JsonNode node = session.client().post(
                "/api/v2/blogposts/" + id(request.getId(), "id") + "/redact",
                NO_QUERY, redaction(request.hasCreatedAt(), request.getCreatedAt(),
                        request.hasCleanHistory(), request.getCleanHistory(),
                        request.hasVersionNumber(), request.getVersionNumber(),
                        request.getBodyRedactionsList(), request.getTitleRedactionsList()));
        return RedactBlogPostResponse.newBuilder().setRedactions(valid(redactions(node))).build();
    }

    static EnableAdminKeyResponse enableAdminKey(Session session, EnableAdminKeyRequest request)
            throws IOException, InterruptedException {
        ObjectNode body = NODES.objectNode();
        if (request.hasDurationInMinutes()) {
            body.put("durationInMinutes", request.getDurationInMinutes());
        }
        JsonNode node = session.client().post("/api/v2/admin-key", NO_QUERY, body);
        return EnableAdminKeyResponse.newBuilder()
                .setAdminKey(valid(session.mapper().toAdminKey(node))).build();
    }

    static DisableAdminKeyResponse disableAdminKey(Session session, DisableAdminKeyRequest request)
            throws IOException, InterruptedException {
        session.client().delete("/api/v2/admin-key", NO_QUERY);
        return DisableAdminKeyResponse.getDefaultInstance();
    }

    // ======================================================================
    // REQUEST JSON
    // ======================================================================

    /** A path id: required, and a single path segment. */
    private static String id(String id, String field) {
        if (requireId(id, field).indexOf('/') >= 0) {
            throw invalid(field + " must not contain '/'");
        }
        return id;
    }

    /** Sets a string member unless the value is empty. */
    private static void put(ObjectNode object, String name, String value) {
        if (value != null && !value.isEmpty()) {
            object.put(name, value);
        }
    }

    /** Sets a date-time member when the request carries one. */
    private static void putTime(ObjectNode object, String name, boolean set, Timestamp time) {
        if (set) {
            object.put(name, Instant.ofEpochSecond(time.getSeconds(), time.getNanos()).toString());
        }
    }

    /** Sets the {@code body} member when the request carries a body. */
    private static void putBody(ObjectNode object, ContentBody body,
            Set<ContentBody.FormatCase> accepted, String route) {
        ContentBody.FormatCase format = body.getFormatCase();
        if (format == ContentBody.FormatCase.FORMAT_NOT_SET) {
            return;
        }
        if (!accepted.contains(format)) {
            throw invalid(route + " does not accept a " + format + " body; accepted: " + accepted);
        }
        ObjectNode json = object.putObject("body");
        switch (format) {
            case ADF -> json.put("representation", "atlas_doc_format")
                    .put("value", AdfCodec.toJson(body.getAdf()).toString());
            case STORAGE -> json.put("representation", "storage")
                    .put("value", body.getStorage().getValue());
            case WIKI -> json.put("representation", "wiki").put("value", body.getWiki().getValue());
            case RAW -> json.put("representation", "raw").put("value", body.getRaw().getValue());
            case FORMAT_NOT_SET -> throw invalid("body is required");
        }
    }

    /** The {@code version} member of an update: the expected version plus one. */
    private static ObjectNode version(int expectedVersion, String message) {
        if (expectedVersion < 0) {
            throw invalid("expected_version must not be negative");
        }
        ObjectNode version = NODES.objectNode();
        version.put("number", Math.addExact(expectedVersion, 1));
        put(version, "message", message);
        return version;
    }

    /** A status wire value, defaulting to {@code current}. */
    private static String status(ContentStatus status, Set<String> allowed, String route) {
        String wire = optionalStatus(status, allowed, route);
        return wire == null ? "current" : wire;
    }

    /** A status wire value, or {@code null} when the request names none. */
    private static String optionalStatus(ContentStatus status, Set<String> allowed, String route) {
        if (status == ContentStatus.CONTENT_STATUS_UNSPECIFIED) {
            return null;
        }
        if (status == ContentStatus.UNRECOGNIZED) {
            throw invalid("unknown status");
        }
        return ConfluenceRoutes.status(status.getValueDescriptor().getName(), "CONTENT_STATUS_",
                allowed, route);
    }

    private static String classificationStatus(ContentRef content, ContentStatus status,
            String route) {
        boolean drafts = content.getType() == ContentType.CONTENT_TYPE_PAGE
                || content.getType() == ContentType.CONTENT_TYPE_BLOG_POST;
        return status(status, drafts ? CURRENT_OR_DRAFT : CURRENT, route);
    }

    /** The create body shared by databases, folders, whiteboards and Smart Links. */
    private static ObjectNode treeNode(String spaceId, String title, String parentId) {
        ObjectNode body = NODES.objectNode();
        body.put("spaceId", requireId(spaceId, "space_id"));
        put(body, "title", title);
        put(body, "parentId", parentId);
        return body;
    }

    private static ObjectNode property(PropertyKey key, String customKey, PropertyValue value) {
        ObjectNode body = NODES.objectNode();
        body.put("key", propertyKey(key, customKey));
        body.set("value", PropertyJson.toJson(value, "value"));
        return body;
    }

    /** The API key string of a typed property key. */
    static String propertyKey(PropertyKey key, String customKey) {
        return switch (key) {
            case PROPERTY_KEY_EDITOR -> "editor";
            case PROPERTY_KEY_CONTENT_APPEARANCE_PUBLISHED -> "content-appearance-published";
            case PROPERTY_KEY_CONTENT_APPEARANCE_DRAFT -> "content-appearance-draft";
            case PROPERTY_KEY_CUSTOM -> requireId(customKey, "custom_key");
            case PROPERTY_KEY_UNSPECIFIED, UNRECOGNIZED -> throw invalid("key is required");
        };
    }

    /** {@code /api/v2/{content-type}/{id}} of a property owner; comments share one route. */
    private static String propertyOwner(ContentRef content, String route) {
        if (content.getType() == ContentType.CONTENT_TYPE_FOOTER_COMMENT
                || content.getType() == ContentType.CONTENT_TYPE_INLINE_COMMENT) {
            return "/api/v2/comments/" + id(content.getId(), "content id");
        }
        id(content.getId(), "content id");
        return ConfluenceRoutes.contentPath(content, PROPERTY_HOLDERS, route);
    }

    private static String permissionOwner(ContentRef content, String route) {
        id(content.getId(), "content id");
        return ConfluenceRoutes.contentPath(content, ConfluenceRoutes.PERMISSION_TARGETS, route);
    }

    private static String appProperty(String key) {
        return "/api/v2/app/properties/" + URLEncoder.encode(requireId(key, "key"),
                StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static Map<String, String> selfLockout(boolean allow) {
        return new Query().flag("allow-self-lockout", allow).map();
    }

    private static ObjectNode grants(List<ContentPermissionGrant> grants) {
        ObjectNode body = NODES.objectNode();
        ArrayNode permissions = body.putArray("permissions");
        for (ContentPermissionGrant grant : grants) {
            ObjectNode entry = permissions.addObject();
            entry.put("operation", lower(grant.getOperation(), "CONTENT_PERMISSION_OPERATION_",
                    "operation"));
            ArrayNode principals = entry.putArray("principals");
            for (ContentPermissionPrincipal principal : grant.getPrincipalsList()) {
                principals.addObject()
                        .put("principalType", lower(principal.getPrincipalType(),
                                "CONTENT_PERMISSION_PRINCIPAL_TYPE_", "principal_type"))
                        .put("principalId", requireId(principal.getPrincipalId(), "principal_id"));
            }
        }
        return body;
    }

    private static ObjectNode role(String name, String description, List<String> permissions) {
        ObjectNode body = NODES.objectNode();
        body.put("name", name);
        body.put("description", description);
        ArrayNode ids = body.putArray("spacePermissions");
        permissions.forEach(ids::add);
        return body;
    }

    private static ArrayNode roleAssignments(List<SpaceRoleAssignment> assignments) {
        ArrayNode array = NODES.arrayNode();
        for (SpaceRoleAssignment assignment : assignments) {
            ObjectNode entry = array.addObject();
            entry.putObject("principal")
                    .put("principalType", upper(assignment.getPrincipal().getPrincipalType(),
                            "PRINCIPAL_TYPE_", "principal.principal_type"))
                    .put("principalId", requireId(assignment.getPrincipal().getPrincipalId(),
                            "principal.principal_id"));
            put(entry, "roleId", assignment.getRoleId());
        }
        return array;
    }

    private static ObjectNode spaceSelection(BulkTransitionSpaceSelection selection) {
        ObjectNode json = NODES.objectNode();
        json.put("spaceType", upper(selection.getSpaceType(), "BULK_TRANSITION_SPACE_TYPE_",
                "space_selection.space_type"));
        if (selection.getSelectedSpacesCount() > 0) {
            ArrayNode spaces = json.putArray("selectedSpaces");
            for (BulkTransitionSpaceTarget space : selection.getSelectedSpacesList()) {
                spaces.addObject().put("id", space.getId()).put("key", space.getKey());
            }
        }
        return json;
    }

    private static ObjectNode redaction(boolean hasCreatedAt, Timestamp createdAt,
            boolean hasCleanHistory, boolean cleanHistory, boolean hasVersion, int version,
            List<RedactionPointer> body, List<RedactionPointer> title) {
        if (!hasCreatedAt) {
            throw invalid("created_at is required");
        }
        ObjectNode json = NODES.objectNode();
        putTime(json, "createdAt", true, createdAt);
        if (hasCleanHistory) {
            json.put("cleanHistory", cleanHistory);
        }
        if (hasVersion) {
            json.put("versionNumber", version);
        }
        if (!body.isEmpty()) {
            json.putObject("body").set("redactions", pointers(body));
        }
        if (!title.isEmpty()) {
            json.putObject("title").set("redactions", pointers(title));
        }
        return json;
    }

    private static ArrayNode pointers(List<RedactionPointer> pointers) {
        ArrayNode array = NODES.arrayNode();
        for (RedactionPointer pointer : pointers) {
            ObjectNode entry = array.addObject();
            entry.put("pointer", pointer.getPointer());
            entry.put("from", pointer.getFrom());
            entry.put("to", pointer.getTo());
            if (pointer.hasReason()) {
                entry.put("reason", pointer.getReason());
            }
        }
        return array;
    }

    // ======================================================================
    // RESPONSE MAPPING
    // ======================================================================

    private static RedactionResponse redactions(JsonNode node) {
        RedactionResponse.Builder response = RedactionResponse.newBuilder();
        if (node.path("body").isObject()) {
            response.setBody(redactionSection(node.path("body")));
        }
        if (node.path("title").isObject()) {
            response.setTitle(redactionSection(node.path("title")));
        }
        return response.build();
    }

    private static RedactionSectionResponse redactionSection(JsonNode node) {
        RedactionSectionResponse.Builder section = RedactionSectionResponse.newBuilder();
        for (JsonNode entry : node.path("redactions")) {
            section.addRedactions(Redaction.newBuilder()
                    .setPointer(entry.path("pointer").asText(""))
                    .setFrom(entry.path("from").asInt(0))
                    .setTo(entry.path("to").asInt(0))
                    .setReason(entry.path("reason").asText(""))
                    .setRedactionId(entry.path("redactionId").asText("")));
        }
        return section.build();
    }

    private static BulkTransitionTask transitionTask(JsonNode node) {
        return BulkTransitionTask.newBuilder()
                .setTaskId(node.path("taskId").asText(""))
                .setStatus(ConfluenceMapper.wireEnum(BulkTransitionTaskStatus.class,
                        "BULK_TRANSITION_TASK_STATUS_", node.path("status").asText(""),
                        BulkTransitionTaskStatus.BULK_TRANSITION_TASK_STATUS_UNSPECIFIED))
                .setStatusUrl(node.path("statusUrl").asText(""))
                .build();
    }

    private static <T extends Message> T valid(T message) {
        VALIDATOR.requireValid(message);
        return message;
    }

    // ======================================================================
    // ENUM WIRE VALUES
    // ======================================================================

    /** The enum name without its prefix; INVALID_ARGUMENT when the value is unset or unknown. */
    private static String upper(ProtocolMessageEnum value, String prefix, String field) {
        if (value.toString().equals("UNRECOGNIZED") || value.getNumber() == 0) {
            throw invalid(field + " is required");
        }
        return value.getValueDescriptor().getName().substring(prefix.length());
    }

    private static String lower(ProtocolMessageEnum value, String prefix, String field) {
        return upper(value, prefix, field).toLowerCase(Locale.ROOT);
    }

    private static String kebab(ProtocolMessageEnum value, String prefix, String field) {
        return lower(value, prefix, field).replace('_', '-');
    }
}
