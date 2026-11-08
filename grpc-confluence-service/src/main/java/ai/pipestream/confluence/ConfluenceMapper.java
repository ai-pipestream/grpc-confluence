package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.AccountStatus;
import ai.pipestream.confluence.v1.AccountType;
import ai.pipestream.confluence.v1.Ancestor;
import ai.pipestream.confluence.v1.AncestorType;
import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.BlogPost;
import ai.pipestream.confluence.v1.BlogPostContentStatus;
import ai.pipestream.confluence.v1.Body;
import ai.pipestream.confluence.v1.BodyFormat;
import ai.pipestream.confluence.v1.BodyType;
import ai.pipestream.confluence.v1.ClassificationLevel;
import ai.pipestream.confluence.v1.ClassificationLevelColor;
import ai.pipestream.confluence.v1.ClassificationLevelStatus;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.CommentKind;
import ai.pipestream.confluence.v1.ContentPermission;
import ai.pipestream.confluence.v1.ContentPermissionOperation;
import ai.pipestream.confluence.v1.ContentPermissionPrincipal;
import ai.pipestream.confluence.v1.ContentPermissionPrincipalType;
import ai.pipestream.confluence.v1.ContentProperty;
import ai.pipestream.confluence.v1.ContentRestrictionState;
import ai.pipestream.confluence.v1.ContentStatus;
import ai.pipestream.confluence.v1.ContentTreeEntry;
import ai.pipestream.confluence.v1.ContentType;
import ai.pipestream.confluence.v1.CustomContent;
import ai.pipestream.confluence.v1.DataPolicy;
import ai.pipestream.confluence.v1.DataPolicyMetadata;
import ai.pipestream.confluence.v1.Database;
import ai.pipestream.confluence.v1.DatabaseAiSummaryFieldSettings;
import ai.pipestream.confluence.v1.DatabaseAiSummaryUpdater;
import ai.pipestream.confluence.v1.DatabaseAiSummaryValue;
import ai.pipestream.confluence.v1.DatabaseAiSummaryValueMetadata;
import ai.pipestream.confluence.v1.DatabaseAttachmentValue;
import ai.pipestream.confluence.v1.DatabaseBacklinkFieldSettings;
import ai.pipestream.confluence.v1.DatabaseDateFieldSettings;
import ai.pipestream.confluence.v1.DatabaseEntry;
import ai.pipestream.confluence.v1.DatabaseEntryDetailsFieldSettings;
import ai.pipestream.confluence.v1.DatabaseEntryLinkValue;
import ai.pipestream.confluence.v1.DatabaseEntryRef;
import ai.pipestream.confluence.v1.DatabaseField;
import ai.pipestream.confluence.v1.DatabaseFieldType;
import ai.pipestream.confluence.v1.DatabaseFieldValue;
import ai.pipestream.confluence.v1.DatabaseHyperlinkItem;
import ai.pipestream.confluence.v1.DatabaseHyperlinkValue;
import ai.pipestream.confluence.v1.DatabaseJiraIssueDetailsFieldSettings;
import ai.pipestream.confluence.v1.DatabaseJiraIssueFieldSettings;
import ai.pipestream.confluence.v1.DatabaseJiraIssueRef;
import ai.pipestream.confluence.v1.DatabaseJiraIssueValue;
import ai.pipestream.confluence.v1.DatabaseNumberFieldSettings;
import ai.pipestream.confluence.v1.DatabaseNumberFormat;
import ai.pipestream.confluence.v1.DatabaseNumberFormatType;
import ai.pipestream.confluence.v1.DatabasePageDetailsFieldSettings;
import ai.pipestream.confluence.v1.DatabasePageLinkValue;
import ai.pipestream.confluence.v1.DatabaseSelectFieldSettings;
import ai.pipestream.confluence.v1.DatabaseSelectOption;
import ai.pipestream.confluence.v1.DatabaseSelectValue;
import ai.pipestream.confluence.v1.DatabaseSourceFieldSettings;
import ai.pipestream.confluence.v1.DatabaseTextFieldSettings;
import ai.pipestream.confluence.v1.DatabaseTextValue;
import ai.pipestream.confluence.v1.DatabaseUserValue;
import ai.pipestream.confluence.v1.DatabaseView;
import ai.pipestream.confluence.v1.DatabaseViewType;
import ai.pipestream.confluence.v1.Folder;
import ai.pipestream.confluence.v1.Icon;
import ai.pipestream.confluence.v1.IncludeExpansion;
import ai.pipestream.confluence.v1.InlineCommentProperties;
import ai.pipestream.confluence.v1.InlineCommentResolutionStatus;
import ai.pipestream.confluence.v1.Label;
import ai.pipestream.confluence.v1.Like;
import ai.pipestream.confluence.v1.OnlyArchivedAndCurrentContentStatus;
import ai.pipestream.confluence.v1.Operation;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.ParentContentType;
import ai.pipestream.confluence.v1.Principal;
import ai.pipestream.confluence.v1.PrincipalType;
import ai.pipestream.confluence.v1.PropertyKey;
import ai.pipestream.confluence.v1.PropertyValue;
import ai.pipestream.confluence.v1.ResolvedContentType;
import ai.pipestream.confluence.v1.RestrictionState;
import ai.pipestream.confluence.v1.RoleType;
import ai.pipestream.confluence.v1.SmartLink;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.SpaceDescription;
import ai.pipestream.confluence.v1.SpaceIcon;
import ai.pipestream.confluence.v1.SpacePermissionAssignment;
import ai.pipestream.confluence.v1.SpacePermissionOperation;
import ai.pipestream.confluence.v1.SpacePermissionOperationKey;
import ai.pipestream.confluence.v1.SpacePermissionOperationTargetType;
import ai.pipestream.confluence.v1.SpacePermissionPrincipal;
import ai.pipestream.confluence.v1.SpacePermissionPrincipalType;
import ai.pipestream.confluence.v1.SpaceProperty;
import ai.pipestream.confluence.v1.SpacePropertyVersion;
import ai.pipestream.confluence.v1.SpaceRole;
import ai.pipestream.confluence.v1.SpaceRoleAssignment;
import ai.pipestream.confluence.v1.SpaceRoleMode;
import ai.pipestream.confluence.v1.SpaceStatus;
import ai.pipestream.confluence.v1.SpaceType;
import ai.pipestream.confluence.v1.Task;
import ai.pipestream.confluence.v1.TaskStatus;
import ai.pipestream.confluence.v1.User;
import ai.pipestream.confluence.v1.Version;
import ai.pipestream.confluence.v1.VersionedEntity;
import ai.pipestream.confluence.v1.WebResourceBatch;
import ai.pipestream.confluence.v1.WebResources;
import ai.pipestream.confluence.v1.Whiteboard;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;

import java.net.URI;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Jackson {@link JsonNode} to domain proto translation for the Confluence
 * Cloud REST API v2 shapes. One method per crawled entity kind; every method
 * is null-tolerant (missing fields stay unset) and never throws on unknown
 * enum wire values - those map to the {@code UNSPECIFIED} member, per the
 * domain model's field-level fidelity rules.
 *
 * <p>REST response decoration is dropped here: {@code _links} blocks collapse
 * to the entity's {@code web_url} (composed absolute from the tenant base URL
 * when the API returns a server-relative path), and pagination transport
 * never reaches the protos. Properties translate the API's string keys into
 * {@link PropertyKey} ({@code CUSTOM} plus {@code custom_key} as the escape
 * hatch) and the JSON value into the matching {@link PropertyValue} arm.</p>
 */
public final class ConfluenceMapper {

    private final String baseUrl;
    private final String basePath;
    private final String origin;

    /**
     * Creates a mapper bound to one tenant origin.
     *
     * @param baseUrl the tenant base URL including {@code /wiki} (e.g.
     *        {@code https://pipestreamai.atlassian.net/wiki}); used to compose
     *        absolute {@code web_url} / {@code download_url} values
     */
    public ConfluenceMapper(String baseUrl) {
        this.baseUrl = Strings.stripTrailingSlashes(Objects.requireNonNull(baseUrl, "baseUrl"));
        URI uri = URI.create(this.baseUrl);
        this.basePath = uri.getPath() == null ? "" : uri.getPath();
        this.origin = uri.getScheme() + "://" + uri.getAuthority();
    }

    // ======================================================================
    // SPACES
    // ======================================================================

    /**
     * Maps a REST v2 {@code Space} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public Space toSpace(JsonNode node) {
        Space.Builder b = Space.newBuilder()
                .setId(text(node, "id"))
                .setKey(text(node, "key"))
                .setName(text(node, "name"))
                .setType(wireEnum(SpaceType.class, "SPACE_TYPE_", text(node, "type"),
                        SpaceType.SPACE_TYPE_UNSPECIFIED))
                .setStatus(wireEnum(SpaceStatus.class, "SPACE_STATUS_", text(node, "status"),
                        SpaceStatus.SPACE_STATUS_UNSPECIFIED))
                .setAuthorId(text(node, "authorId"))
                .setSpaceOwnerId(text(node, "spaceOwnerId"))
                .setCurrentActiveAlias(text(node, "currentActiveAlias"))
                .setHomepageId(text(node, "homepageId"));
        setTimestamp(node, "createdAt", b::setCreatedAt);
        JsonNode description = node.path("description");
        if (description.isObject()) {
            b.setDescription(toSpaceDescription(description));
        }
        JsonNode icon = node.path("icon");
        if (icon.isObject()) {
            b.setIcon(SpaceIcon.newBuilder()
                    .setPath(text(icon, "path"))
                    .setApiDownloadUrl(absolute(text(icon, "apiDownloadLink")))
                    .build());
        }
        b.setWebUrl(absolute(webui(node)));
        applyIncludes(b, node);
        return b.build();
    }

    private SpaceDescription toSpaceDescription(JsonNode node) {
        SpaceDescription.Builder b = SpaceDescription.newBuilder();
        JsonNode plain = node.path("plain");
        if (plain.isObject()) {
            b.setPlain(toBodyType(plain, BodyFormat.BODY_FORMAT_PLAIN_TEXT));
        }
        JsonNode view = node.path("view");
        if (view.isObject()) {
            b.setView(toBodyType(view, BodyFormat.BODY_FORMAT_RENDERED_XHTML));
        }
        return b.build();
    }

    // ======================================================================
    // PAGES AND BLOG POSTS
    // ======================================================================

    /**
     * Maps a REST v2 {@code Page} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public Page toPage(JsonNode node) {
        Page.Builder b = Page.newBuilder()
                .setId(text(node, "id"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setSpaceId(text(node, "spaceId"))
                .setParentId(text(node, "parentId"))
                .setParentType(wireEnum(ParentContentType.class, "PARENT_CONTENT_TYPE_",
                        text(node, "parentType"), ParentContentType.PARENT_CONTENT_TYPE_UNSPECIFIED))
                .setAuthorId(text(node, "authorId"));
        if (hasText(node, "subtype")) {
            b.setSubtype(text(node, "subtype"));
        }
        if (hasText(node, "ownerId")) {
            b.setOwnerId(text(node, "ownerId"));
        }
        if (hasText(node, "lastOwnerId")) {
            b.setLastOwnerId(text(node, "lastOwnerId"));
        }
        if (node.path("position").isIntegralNumber()) {
            b.setPosition(node.path("position").intValue());
        }
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        setBody(node, b::setBody);
        b.setWebUrl(absolute(webui(node)));
        b.addAllCollaboratorAccountIds(collaboratorIds(node));
        if (node.path("webResources").isObject()) {
            b.setWebResources(toWebResources(node.path("webResources")));
        }
        applyIncludes(b, node);
        return b.build();
    }

    /**
     * Maps a REST v2 {@code BlogPost} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public BlogPost toBlogPost(JsonNode node) {
        BlogPost.Builder b = BlogPost.newBuilder()
                .setId(text(node, "id"))
                .setStatus(wireEnum(BlogPostContentStatus.class, "BLOG_POST_CONTENT_STATUS_",
                        text(node, "status"), BlogPostContentStatus.BLOG_POST_CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setSpaceId(text(node, "spaceId"))
                .setAuthorId(text(node, "authorId"));
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        setBody(node, b::setBody);
        b.setWebUrl(absolute(webui(node)));
        b.addAllCollaboratorAccountIds(collaboratorIds(node));
        if (node.path("webResources").isObject()) {
            b.setWebResources(toWebResources(node.path("webResources")));
        }
        applyIncludes(b, node);
        return b.build();
    }

    // ======================================================================
    // COMMENTS
    // ======================================================================

    /**
     * Maps a REST v2 {@code Comment} JSON object to the domain proto.
     * One proto covers footer and inline comments; inline-only fields stay
     * unset on footer comments.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public Comment toComment(JsonNode node) {
        return toComment(node, CommentKind.COMMENT_KIND_UNSPECIFIED);
    }

    /**
     * Maps a REST v2 {@code Comment} JSON object read from a footer or inline
     * route. The API does not echo the kind, so the caller passes it.
     *
     * @param node the API object
     * @param kind the kind of the route the comment was read from
     * @return the proto; missing fields stay unset
     */
    public Comment toComment(JsonNode node, CommentKind kind) {
        Comment.Builder b = Comment.newBuilder()
                .setId(text(node, "id"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setBlogPostId(text(node, "blogPostId"))
                .setPageId(text(node, "pageId"))
                .setAttachmentId(text(node, "attachmentId"))
                .setCustomContentId(text(node, "customContentId"))
                .setParentCommentId(text(node, "parentCommentId"))
                .setResolutionStatus(wireEnum(InlineCommentResolutionStatus.class,
                        "INLINE_COMMENT_RESOLUTION_STATUS_", text(node, "resolutionStatus"),
                        InlineCommentResolutionStatus.INLINE_COMMENT_RESOLUTION_STATUS_UNSPECIFIED))
                .setKind(kind);
        setVersion(node, b::setVersion);
        setBody(node, b::setBody);
        if (hasText(node, "resolutionLastModifierId")) {
            b.setResolutionLastModifierId(text(node, "resolutionLastModifierId"));
        }
        if (hasText(node, "resolutionLastModifiedAt")) {
            Timestamp at = timestamp(text(node, "resolutionLastModifiedAt"));
            if (at != null) {
                b.setResolutionLastModifiedAt(at);
            }
        }
        // Inline list responses carry the highlighted-text properties under
        // "properties" (single reads reuse that name for content properties).
        JsonNode inline = node.path("properties");
        if (!inline.has("inlineMarkerRef") && !inline.has("inlineOriginalSelection")) {
            inline = node.path("inlineProperties");
        }
        if (inline.isObject() && (inline.has("inlineMarkerRef")
                || inline.has("inlineOriginalSelection"))) {
            b.setInlineProperties(InlineCommentProperties.newBuilder()
                    .setInlineMarkerRef(text(inline, "inlineMarkerRef"))
                    .setInlineOriginalSelection(text(inline, "inlineOriginalSelection"))
                    .build());
        }
        b.setWebUrl(absolute(webui(node)));
        applyIncludes(b, node);
        return b.build();
    }

    // ======================================================================
    // ATTACHMENTS
    // ======================================================================

    /**
     * Maps a REST v2 {@code Attachment} JSON object to the domain proto.
     * Bytes are not part of list responses; the crawler fills {@code content}
     * from a separate {@link ConfluenceClient#downloadAttachmentBytes} call
     * when it wants the binary.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public Attachment toAttachment(JsonNode node) {
        Attachment.Builder b = Attachment.newBuilder()
                .setId(text(node, "id"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setPageId(text(node, "pageId"))
                .setBlogPostId(text(node, "blogPostId"))
                .setCustomContentId(text(node, "customContentId"))
                .setMediaType(text(node, "mediaType"))
                .setMediaTypeDescription(text(node, "mediaTypeDescription"))
                .setComment(text(node, "comment"))
                .setFileId(text(node, "fileId"))
                .setFileSize(node.path("fileSize").isIntegralNumber()
                        ? node.path("fileSize").longValue() : 0L);
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        // Attachments carry their links as direct fields as well as _links;
        // the direct fields win.
        String webui = hasText(node, "webuiLink") ? text(node, "webuiLink") : webui(node);
        b.setWebUrl(absolute(webui));
        String download = hasText(node, "downloadLink") ? text(node, "downloadLink")
                : text(node.path("_links"), "download");
        b.setDownloadUrl(absolute(download));
        b.addAllCollaboratorAccountIds(collaboratorIds(node));
        applyIncludes(b, node);
        return b.build();
    }

    // ======================================================================
    // LABELS, TASKS, USERS
    // ======================================================================

    /**
     * Maps a REST v2 {@code Label} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public Label toLabel(JsonNode node) {
        return Label.newBuilder()
                .setId(text(node, "id"))
                .setName(text(node, "name"))
                .setPrefix(text(node, "prefix"))
                .build();
    }

    /**
     * Maps a REST v2 {@code Task} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public Task toTask(JsonNode node) {
        Task.Builder b = Task.newBuilder()
                .setId(text(node, "id"))
                .setLocalId(text(node, "localId"))
                .setSpaceId(text(node, "spaceId"))
                .setPageId(text(node, "pageId"))
                .setBlogPostId(text(node, "blogPostId"))
                .setStatus(wireEnum(TaskStatus.class, "TASK_STATUS_", text(node, "status"),
                        TaskStatus.TASK_STATUS_UNSPECIFIED))
                .setCreatedBy(text(node, "createdBy"))
                .setAssignedTo(text(node, "assignedTo"))
                .setCompletedBy(text(node, "completedBy"));
        JsonNode body = node.path("body");
        if (body.isObject()) {
            Body.Builder bb = Body.newBuilder();
            JsonNode view = body.path("view");
            if (view.isObject()) {
                bb.setView(toBodyType(view, BodyFormat.BODY_FORMAT_RENDERED_XHTML));
            }
            JsonNode storage = body.path("storage");
            if (storage.isObject()) {
                bb.setStorage(toBodyType(storage, BodyFormat.BODY_FORMAT_STORAGE_XHTML));
            }
            JsonNode adf = body.path("atlas_doc_format");
            if (adf.isObject()) {
                bb.setAtlasDocFormat(toBodyType(adf, BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT));
            }
            b.setBody(bb.build());
        }
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setTimestamp(node, "updatedAt", b::setUpdatedAt);
        setTimestamp(node, "dueAt", b::setDueAt);
        setTimestamp(node, "completedAt", b::setCompletedAt);
        return b.build();
    }

    /**
     * Maps a REST v2 {@code User} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public User toUser(JsonNode node) {
        User.Builder b = User.newBuilder()
                .setAccountId(text(node, "accountId"))
                .setAccountType(wireEnum(AccountType.class, "ACCOUNT_TYPE_",
                        text(node, "accountType"), AccountType.ACCOUNT_TYPE_UNSPECIFIED))
                .setAccountStatus(wireEnum(AccountStatus.class, "ACCOUNT_STATUS_",
                        text(node, "accountStatus"), AccountStatus.ACCOUNT_STATUS_UNSPECIFIED))
                .setDisplayName(text(node, "displayName"))
                .setPublicName(text(node, "publicName"))
                .setEmail(text(node, "email"))
                .setTimeZone(text(node, "timeZone"))
                .setIsExternalCollaborator(node.path("isExternalCollaborator").asBoolean(false));
        if (hasText(node, "personalSpaceId")) {
            b.setPersonalSpaceId(text(node, "personalSpaceId"));
        }
        JsonNode picture = node.path("profilePicture");
        if (picture.isObject()) {
            b.setProfilePicture(Icon.newBuilder()
                    .setPath(text(picture, "path"))
                    .setIsDefault(picture.path("isDefault").asBoolean(false))
                    .build());
        }
        return b.build();
    }

    // ======================================================================
    // HIERARCHICAL CONTENT
    // ======================================================================

    /**
     * Maps a REST v2 {@code Whiteboard} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public Whiteboard toWhiteboard(JsonNode node) {
        Whiteboard.Builder b = Whiteboard.newBuilder()
                .setId(text(node, "id"))
                .setType(text(node, "type"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setParentId(text(node, "parentId"))
                .setParentType(wireEnum(ParentContentType.class, "PARENT_CONTENT_TYPE_",
                        text(node, "parentType"), ParentContentType.PARENT_CONTENT_TYPE_UNSPECIFIED))
                .setAuthorId(text(node, "authorId"))
                .setOwnerId(text(node, "ownerId"))
                .setSpaceId(text(node, "spaceId"));
        if (node.path("position").isIntegralNumber()) {
            b.setPosition(node.path("position").intValue());
        }
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        b.setWebUrl(absolute(webui(node)));
        b.addAllCollaboratorAccountIds(collaboratorIds(node));
        applyIncludes(b, node);
        return b.build();
    }

    /**
     * Maps a REST v2 {@code Database} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public Database toDatabase(JsonNode node) {
        Database.Builder b = Database.newBuilder()
                .setId(text(node, "id"))
                .setType(text(node, "type"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setParentId(text(node, "parentId"))
                .setParentType(wireEnum(ParentContentType.class, "PARENT_CONTENT_TYPE_",
                        text(node, "parentType"), ParentContentType.PARENT_CONTENT_TYPE_UNSPECIFIED))
                .setAuthorId(text(node, "authorId"))
                .setOwnerId(text(node, "ownerId"))
                .setSpaceId(text(node, "spaceId"));
        if (node.path("position").isIntegralNumber()) {
            b.setPosition(node.path("position").intValue());
        }
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        b.setWebUrl(absolute(webui(node)));
        b.addAllCollaboratorAccountIds(collaboratorIds(node));
        applyIncludes(b, node);
        return b.build();
    }

    /**
     * Maps a REST v2 {@code Folder} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public Folder toFolder(JsonNode node) {
        Folder.Builder b = Folder.newBuilder()
                .setId(text(node, "id"))
                .setType(text(node, "type"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setParentId(text(node, "parentId"))
                .setParentType(wireEnum(ParentContentType.class, "PARENT_CONTENT_TYPE_",
                        text(node, "parentType"), ParentContentType.PARENT_CONTENT_TYPE_UNSPECIFIED))
                .setAuthorId(text(node, "authorId"))
                .setOwnerId(text(node, "ownerId"))
                .setSpaceId(text(node, "spaceId"));
        if (node.path("position").isIntegralNumber()) {
            b.setPosition(node.path("position").intValue());
        }
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        b.setWebUrl(absolute(webui(node)));
        b.addAllCollaboratorAccountIds(collaboratorIds(node));
        applyIncludes(b, node);
        return b.build();
    }

    /**
     * Maps a REST v2 {@code CustomContent} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public CustomContent toCustomContent(JsonNode node) {
        CustomContent.Builder b = CustomContent.newBuilder()
                .setId(text(node, "id"))
                .setType(text(node, "type"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setSpaceId(text(node, "spaceId"))
                .setPageId(text(node, "pageId"))
                .setBlogPostId(text(node, "blogPostId"))
                .setCustomContentId(text(node, "customContentId"))
                .setAuthorId(text(node, "authorId"));
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        setBody(node, b::setBody);
        b.setWebUrl(absolute(webui(node)));
        b.addAllCollaboratorAccountIds(collaboratorIds(node));
        applyIncludes(b, node);
        return b.build();
    }

    // ======================================================================
    // PROPERTIES
    // ======================================================================

    /**
     * Maps a REST v2 {@code ContentProperty} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public ContentProperty toContentProperty(JsonNode node) {
        ContentProperty.Builder b = ContentProperty.newBuilder()
                .setId(text(node, "id"));
        applyPropertyKey(node, b::setKey, b::setCustomKey);
        setPropertyValue(node, b::setValue);
        setVersion(node, b::setVersion);
        return b.build();
    }

    /**
     * Maps a REST v2 {@code SpaceProperty} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public SpaceProperty toSpaceProperty(JsonNode node) {
        SpaceProperty.Builder b = SpaceProperty.newBuilder()
                .setId(text(node, "id"))
                .setCreatedBy(text(node, "createdBy"));
        applyPropertyKey(node, b::setKey, b::setCustomKey);
        setPropertyValue(node, b::setValue);
        setTimestamp(node, "createdAt", b::setCreatedAt);
        JsonNode version = node.path("version");
        if (version.isObject()) {
            SpacePropertyVersion.Builder vb = SpacePropertyVersion.newBuilder()
                    .setCreatedBy(text(version, "createdBy"))
                    .setMessage(text(version, "message"))
                    .setNumber(version.path("number").isIntegralNumber()
                            ? version.path("number").intValue() : 0);
            Timestamp at = timestamp(text(version, "createdAt"));
            if (at != null) {
                vb.setCreatedAt(at);
            }
            b.setVersion(vb.build());
        }
        return b.build();
    }

    /** The well-known keys map by name; anything else is CUSTOM plus the raw key. */
    static void applyPropertyKey(JsonNode node,
            java.util.function.Consumer<PropertyKey> keySetter,
            java.util.function.Consumer<String> customKeySetter) {
        String key = text(node, "key");
        switch (key) {
            case "editor" -> keySetter.accept(PropertyKey.PROPERTY_KEY_EDITOR);
            case "content-appearance-published" ->
                    keySetter.accept(PropertyKey.PROPERTY_KEY_CONTENT_APPEARANCE_PUBLISHED);
            case "content-appearance-draft" ->
                    keySetter.accept(PropertyKey.PROPERTY_KEY_CONTENT_APPEARANCE_DRAFT);
            case "" -> keySetter.accept(PropertyKey.PROPERTY_KEY_UNSPECIFIED);
            default -> {
                keySetter.accept(PropertyKey.PROPERTY_KEY_CUSTOM);
                customKeySetter.accept(key);
            }
        }
    }

    /** The oneof arm matches the JSON value's actual type; objects go to json_value, arrays to json_list_value. */
    static void setPropertyValue(JsonNode node,
            java.util.function.Consumer<PropertyValue> valueSetter) {
        JsonNode value = node.path("value");
        if (value.isMissingNode() || value.isNull()) {
            return;
        }
        PropertyValue.Builder b = PropertyValue.newBuilder();
        if (value.isTextual()) {
            b.setStringValue(value.asText());
        } else if (value.isBoolean()) {
            b.setBoolValue(value.asBoolean());
        } else if (value.isIntegralNumber()) {
            b.setIntegerValue(value.longValue());
        } else if (value.isNumber()) {
            b.setDoubleValue(value.doubleValue());
        } else if (value.isObject()) {
            b.setJsonValue(toStruct(value));
        } else if (value.isArray()) {
            b.setJsonListValue(toListValue(value));
        } else {
            return;
        }
        valueSetter.accept(b.build());
    }

    // ======================================================================
    // VERSIONS AND CLASSIFICATION LEVELS
    // ======================================================================

    /**
     * The account ids of {@code collaborators} ({@code include-collaborators}):
     * an array of {@code {accountId}} objects; empty when absent.
     *
     * @param node the content object
     * @return the account ids, in API order
     */
    static List<String> collaboratorIds(JsonNode node) {
        List<String> ids = new ArrayList<>();
        node.path("collaborators").forEach(c -> {
            if (hasText(c, "accountId")) {
                ids.add(text(c, "accountId"));
            }
        });
        return ids;
    }

    /**
     * Maps the {@code webResources} object ({@code include-webresources}).
     *
     * @param node the {@code webResources} object
     * @return the proto
     */
    static WebResources toWebResources(JsonNode node) {
        WebResources.Builder b = WebResources.newBuilder().setTags(text(node, "tags"));
        if (node.path("superbatch").isObject()) {
            b.setSuperbatch(WebResourceBatch.newBuilder()
                    .setTags(text(node.path("superbatch"), "tags")));
        }
        return b.build();
    }

    /**
     * Maps a REST v2 {@code Version} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public Version toVersion(JsonNode node) {
        Version.Builder b = Version.newBuilder()
                .setMessage(text(node, "message"))
                .setNumber(node.path("number").isIntegralNumber()
                        ? node.path("number").intValue() : 0)
                .setMinorEdit(node.path("minorEdit").asBoolean(false))
                .setAuthorId(text(node, "authorId"));
        setTimestamp(node, "createdAt", b::setCreatedAt);
        // Version-history entries name their snapshot after the content kind.
        for (String kind : VERSIONED_ENTITY_KEYS) {
            JsonNode entity = node.path(kind);
            if (entity.isObject()) {
                VersionedEntity.Builder eb = VersionedEntity.newBuilder()
                        .setTitle(text(entity, "title"))
                        .setId(text(entity, "id"));
                setBody(entity, eb::setBody);
                b.setEntity(eb);
                break;
            }
        }
        return b.build();
    }

    /** The snapshot key of each version-history shape (PageVersion, BlogPostVersion, ...). */
    private static final List<String> VERSIONED_ENTITY_KEYS =
            List.of("page", "blogpost", "attachment", "custom", "comment");

    /**
     * Maps a REST v2 {@code ClassificationLevel} JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public ClassificationLevel toClassificationLevel(JsonNode node) {
        ClassificationLevel.Builder b = ClassificationLevel.newBuilder()
                .setId(text(node, "id"))
                .setStatus(wireEnum(ClassificationLevelStatus.class, "CLASSIFICATION_LEVEL_STATUS_",
                        text(node, "status"),
                        ClassificationLevelStatus.CLASSIFICATION_LEVEL_STATUS_UNSPECIFIED))
                .setOrder(node.path("order").isNumber() ? node.path("order").doubleValue() : 0d)
                .setName(text(node, "name"))
                .setDescription(text(node, "description"))
                .setGuideline(text(node, "guideline"))
                .setColor(wireEnum(ClassificationLevelColor.class, "CLASSIFICATION_LEVEL_COLOR_",
                        text(node, "color"),
                        ClassificationLevelColor.CLASSIFICATION_LEVEL_COLOR_UNSPECIFIED));
        return b.build();
    }

    // ======================================================================
    // SMART LINKS AND THE CONTENT TREE
    // ======================================================================

    /**
     * Maps a REST v2 {@code SmartLink} ({@code /embeds/{id}}) JSON object to the domain proto.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public SmartLink toSmartLink(JsonNode node) {
        SmartLink.Builder b = SmartLink.newBuilder()
                .setId(text(node, "id"))
                .setType(text(node, "type"))
                .setStatus(wireEnum(ContentStatus.class, "CONTENT_STATUS_", text(node, "status"),
                        ContentStatus.CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setParentId(text(node, "parentId"))
                .setParentType(wireEnum(ParentContentType.class, "PARENT_CONTENT_TYPE_",
                        text(node, "parentType"), ParentContentType.PARENT_CONTENT_TYPE_UNSPECIFIED))
                .setAuthorId(text(node, "authorId"))
                .setOwnerId(text(node, "ownerId"))
                .setEmbedUrl(text(node, "embedUrl"))
                .setSpaceId(text(node, "spaceId"));
        if (node.path("position").isIntegralNumber()) {
            b.setPosition(node.path("position").intValue());
        }
        setTimestamp(node, "createdAt", b::setCreatedAt);
        setVersion(node, b::setVersion);
        b.setWebUrl(absolute(webui(node)));
        b.addAllCollaboratorAccountIds(collaboratorIds(node));
        applyIncludes(b, node);
        return b.build();
    }

    /**
     * Maps a direct-children, descendants or custom-content child entry.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public ContentTreeEntry toTreeEntry(JsonNode node) {
        ContentTreeEntry.Builder b = ContentTreeEntry.newBuilder()
                .setId(text(node, "id"))
                .setStatus(wireEnum(OnlyArchivedAndCurrentContentStatus.class,
                        "ONLY_ARCHIVED_AND_CURRENT_CONTENT_STATUS_", text(node, "status"),
                        OnlyArchivedAndCurrentContentStatus.ONLY_ARCHIVED_AND_CURRENT_CONTENT_STATUS_UNSPECIFIED))
                .setTitle(text(node, "title"))
                .setType(text(node, "type"))
                .setSpaceId(text(node, "spaceId"))
                .setParentId(text(node, "parentId"));
        if (node.path("depth").isIntegralNumber()) {
            b.setDepth(node.path("depth").intValue());
        }
        if (node.path("childPosition").isIntegralNumber()) {
            b.setChildPosition(node.path("childPosition").intValue());
        }
        return b.build();
    }

    /**
     * Maps an ancestor entry.
     *
     * @param node the API object
     * @return the proto
     */
    public Ancestor toAncestor(JsonNode node) {
        return Ancestor.newBuilder()
                .setId(text(node, "id"))
                .setType(wireEnum(AncestorType.class, "ANCESTOR_TYPE_", text(node, "type"),
                        AncestorType.ANCESTOR_TYPE_UNSPECIFIED))
                .build();
    }

    // ======================================================================
    // PERMISSIONS
    // ======================================================================

    /**
     * Maps a {@code restriction-state} response.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public RestrictionState toRestrictionState(JsonNode node) {
        RestrictionState.Builder b = RestrictionState.newBuilder()
                .setState(wireEnum(ContentRestrictionState.class, "CONTENT_RESTRICTION_STATE_",
                        text(node, "state"), ContentRestrictionState.CONTENT_RESTRICTION_STATE_UNSPECIFIED));
        setTimestamp(node, "createdAt", b::setCreatedAt);
        String createdBy = createdBy(node);
        if (!createdBy.isEmpty()) {
            b.setCreatedBy(createdBy);
        }
        return b.build();
    }

    /** {@code createdBy}, falling back to its deprecated alias {@code createdByAaid}. */
    private static String createdBy(JsonNode node) {
        return hasText(node, "createdBy") ? text(node, "createdBy") : text(node, "createdByAaid");
    }

    /**
     * Maps one direct content permission entry ({@code /{content-type}/{id}/permissions}).
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public ContentPermission toContentPermission(JsonNode node) {
        JsonNode principal = node.path("principal");
        ContentPermission.Builder b = ContentPermission.newBuilder()
                .setOperation(wireEnum(ContentPermissionOperation.class, "CONTENT_PERMISSION_OPERATION_",
                        text(node, "operation"), ContentPermissionOperation.CONTENT_PERMISSION_OPERATION_UNSPECIFIED))
                .setPrincipal(ContentPermissionPrincipal.newBuilder()
                        .setPrincipalType(wireEnum(ContentPermissionPrincipalType.class,
                                "CONTENT_PERMISSION_PRINCIPAL_TYPE_", text(principal, "principalType"),
                                ContentPermissionPrincipalType.CONTENT_PERMISSION_PRINCIPAL_TYPE_UNSPECIFIED))
                        .setPrincipalId(text(principal, "principalId")))
                .setCreatedBy(createdBy(node));
        setTimestamp(node, "createdAt", b::setCreatedAt);
        return b.build();
    }

    /**
     * Maps one space permission assignment ({@code /spaces/{id}/permissions}).
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public SpacePermissionAssignment toSpacePermissionAssignment(JsonNode node) {
        JsonNode principal = node.path("principal");
        JsonNode operation = node.path("operation");
        return SpacePermissionAssignment.newBuilder()
                .setId(text(node, "id"))
                .setPrincipal(SpacePermissionPrincipal.newBuilder()
                        .setType(wireEnum(SpacePermissionPrincipalType.class,
                                "SPACE_PERMISSION_PRINCIPAL_TYPE_", text(principal, "type"),
                                SpacePermissionPrincipalType.SPACE_PERMISSION_PRINCIPAL_TYPE_UNSPECIFIED))
                        .setId(text(principal, "id")))
                .setOperation(SpacePermissionOperation.newBuilder()
                        .setKey(wireEnum(SpacePermissionOperationKey.class,
                                "SPACE_PERMISSION_OPERATION_KEY_", text(operation, "key"),
                                SpacePermissionOperationKey.SPACE_PERMISSION_OPERATION_KEY_UNSPECIFIED))
                        .setTargetType(wireEnum(SpacePermissionOperationTargetType.class,
                                "SPACE_PERMISSION_OPERATION_TARGET_TYPE_", snake(text(operation, "targetType")),
                                SpacePermissionOperationTargetType.SPACE_PERMISSION_OPERATION_TARGET_TYPE_UNSPECIFIED)))
                .build();
    }

    /**
     * Maps one space role ({@code /space-roles}).
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public SpaceRole toSpaceRole(JsonNode node) {
        SpaceRole.Builder b = SpaceRole.newBuilder()
                .setId(text(node, "id"))
                .setType(wireEnum(RoleType.class, "ROLE_TYPE_", text(node, "type"),
                        RoleType.ROLE_TYPE_UNSPECIFIED))
                .setName(text(node, "name"))
                .setDescription(text(node, "description"));
        node.path("spacePermissions").forEach(permission -> {
            if (permission.isTextual()) {
                b.addSpacePermissions(permission.asText());
            }
        });
        return b.build();
    }

    /**
     * Maps one space role assignment ({@code /spaces/{id}/role-assignments}).
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public SpaceRoleAssignment toSpaceRoleAssignment(JsonNode node) {
        return SpaceRoleAssignment.newBuilder()
                .setPrincipal(toPrincipal(node.path("principal")))
                .setRoleId(text(node, "roleId"))
                .build();
    }

    private static Principal toPrincipal(JsonNode node) {
        return Principal.newBuilder()
                .setPrincipalType(wireEnum(PrincipalType.class, "PRINCIPAL_TYPE_",
                        text(node, "principalType"), PrincipalType.PRINCIPAL_TYPE_UNSPECIFIED))
                .setPrincipalId(text(node, "principalId"))
                .build();
    }

    /**
     * Maps the {@code /space-role-mode} response.
     *
     * @param node the API object
     * @return the mode; UNSPECIFIED when unknown
     */
    public static SpaceRoleMode toSpaceRoleMode(JsonNode node) {
        return wireEnum(SpaceRoleMode.class, "SPACE_ROLE_MODE_", text(node, "mode"),
                SpaceRoleMode.SPACE_ROLE_MODE_UNSPECIFIED);
    }

    // ======================================================================
    // CONTENT TYPE RESOLUTION
    // ======================================================================

    /**
     * Maps the {@code POST /content/convert-ids-to-types} response: one entry
     * per key of {@code results}. Built-in kinds become {@link ContentType};
     * any other value is a custom content type and lands in
     * {@code custom_type} verbatim.
     *
     * @param node the API object
     * @return the entries, in API order
     */
    public List<ResolvedContentType> toResolvedContentTypes(JsonNode node) {
        List<ResolvedContentType> out = new ArrayList<>();
        for (Map.Entry<String, JsonNode> entry : node.path("results").properties()) {
            if (!entry.getValue().isTextual()) {
                continue;
            }
            String wire = entry.getValue().asText();
            ContentType type = contentType(wire);
            ResolvedContentType.Builder b = ResolvedContentType.newBuilder()
                    .setId(entry.getKey())
                    .setType(type);
            if (type == ContentType.CONTENT_TYPE_UNSPECIFIED) {
                b.setCustomType(wire);
            }
            out.add(b.build());
        }
        return out;
    }

    /**
     * The built-in content-type strings of the v2 API (singular, as
     * {@code convert-ids-to-types} returns them) to {@link ContentType};
     * anything else is {@code UNSPECIFIED}.
     *
     * @param wire the API string, or {@code null}
     * @return the matching kind, {@code UNSPECIFIED} for custom content types
     */
    public static ContentType contentType(String wire) {
        return switch (wire == null ? "" : wire) {
            case "page" -> ContentType.CONTENT_TYPE_PAGE;
            case "blogpost" -> ContentType.CONTENT_TYPE_BLOG_POST;
            case "attachment" -> ContentType.CONTENT_TYPE_ATTACHMENT;
            case "footer-comment" -> ContentType.CONTENT_TYPE_FOOTER_COMMENT;
            case "inline-comment" -> ContentType.CONTENT_TYPE_INLINE_COMMENT;
            case "whiteboard" -> ContentType.CONTENT_TYPE_WHITEBOARD;
            case "database" -> ContentType.CONTENT_TYPE_DATABASE;
            case "folder" -> ContentType.CONTENT_TYPE_FOLDER;
            case "embed" -> ContentType.CONTENT_TYPE_SMART_LINK;
            default -> ContentType.CONTENT_TYPE_UNSPECIFIED;
        };
    }

    // ======================================================================
    // DATA POLICIES
    // ======================================================================

    /**
     * Maps one {@code /data-policies/spaces} entry.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public DataPolicy toDataPolicy(JsonNode node) {
        DataPolicy.Builder b = DataPolicy.newBuilder()
                .setId(text(node, "id"))
                .setKey(text(node, "key"))
                .setName(text(node, "name"))
                .setAnyContentBlocked(node.path("dataPolicy").path("anyContentBlocked").asBoolean(false));
        if (node.path("description").isObject()) {
            b.setDescription(toSpaceDescription(node.path("description")));
        }
        JsonNode icon = node.path("icon");
        if (icon.isObject()) {
            b.setIcon(SpaceIcon.newBuilder()
                    .setPath(text(icon, "path"))
                    .setApiDownloadUrl(absolute(text(icon, "apiDownloadLink"))));
        }
        b.setWebUrl(absolute(webui(node)));
        return b.build();
    }

    /**
     * Maps the {@code /data-policies/metadata} response.
     *
     * @param node the API object
     * @return the proto
     */
    public DataPolicyMetadata toDataPolicyMetadata(JsonNode node) {
        return DataPolicyMetadata.newBuilder()
                .setAnyContentBlocked(node.path("anyContentBlocked").asBoolean(false))
                .build();
    }

    // ======================================================================
    // DATABASE DATA (experimental upstream)
    // ======================================================================

    /**
     * Maps one database field definition; the settings arm follows {@code type}.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public DatabaseField toDatabaseField(JsonNode node) {
        DatabaseFieldType type = databaseFieldType(text(node, "type"));
        DatabaseField.Builder b = DatabaseField.newBuilder()
                .setId(text(node, "id"))
                .setName(text(node, "name"))
                .setDescription(text(node, "description"))
                .setType(type);
        JsonNode settings = node.path("settings");
        switch (type) {
            case DATABASE_FIELD_TYPE_TEXT -> b.setText(DatabaseTextFieldSettings.newBuilder()
                    .setRichText(settings.path("richText").asBoolean(false)));
            case DATABASE_FIELD_TYPE_AI_SUMMARY -> b.setAiSummary(DatabaseAiSummaryFieldSettings.newBuilder()
                    .setLastAiRunUserAccountId(text(settings.path("aiRunMetadata"), "lastAiRunUserAccountId")));
            case DATABASE_FIELD_TYPE_NUMBER -> {
                DatabaseNumberFieldSettings.Builder number = DatabaseNumberFieldSettings.newBuilder();
                JsonNode format = settings.path("format");
                if (format.isObject()) {
                    DatabaseNumberFormat.Builder fb = DatabaseNumberFormat.newBuilder()
                            .setType(wireEnum(DatabaseNumberFormatType.class, "DATABASE_NUMBER_FORMAT_TYPE_",
                                    text(format, "type"), DatabaseNumberFormatType.DATABASE_NUMBER_FORMAT_TYPE_UNSPECIFIED))
                            .setPrefix(text(format, "prefix"))
                            .setSuffix(text(format, "suffix"));
                    if (format.path("decimalPlaces").isIntegralNumber()) {
                        fb.setDecimalPlaces(format.path("decimalPlaces").intValue());
                    }
                    number.setFormat(fb);
                }
                b.setNumber(number);
            }
            case DATABASE_FIELD_TYPE_SELECT -> {
                DatabaseSelectFieldSettings.Builder select = DatabaseSelectFieldSettings.newBuilder();
                settings.path("options").forEach(option -> select.addOptions(toSelectOption(option)));
                b.setSelect(select);
            }
            case DATABASE_FIELD_TYPE_DATE -> b.setDate(DatabaseDateFieldSettings.newBuilder()
                    .setIncludesTime(settings.path("includesTime").asBoolean(false)));
            case DATABASE_FIELD_TYPE_JIRA_ISSUE -> b.setJiraIssue(DatabaseJiraIssueFieldSettings.newBuilder()
                    .setCloudId(text(settings, "cloudId"))
                    .setServerId(text(settings, "serverId")));
            case DATABASE_FIELD_TYPE_ENTRY_DETAILS -> b.setEntryDetails(DatabaseEntryDetailsFieldSettings.newBuilder()
                    .setSourceFieldId(text(settings, "sourceFieldId"))
                    .setLookupFieldId(text(settings, "lookupFieldId")));
            case DATABASE_FIELD_TYPE_JIRA_ISSUE_DETAILS -> b.setJiraIssueDetails(
                    DatabaseJiraIssueDetailsFieldSettings.newBuilder()
                            .setSourceFieldId(text(settings, "sourceFieldId"))
                            .setJiraFieldKey(text(settings, "jiraFieldKey")));
            case DATABASE_FIELD_TYPE_PAGE_DETAILS -> b.setPageDetails(DatabasePageDetailsFieldSettings.newBuilder()
                    .setSourceFieldId(text(settings, "sourceFieldId"))
                    .setPagePropertyKey(text(settings, "pagePropertyKey")));
            case DATABASE_FIELD_TYPE_PAGE_LABELS, DATABASE_FIELD_TYPE_PAGE_STATUS -> b.setSource(
                    DatabaseSourceFieldSettings.newBuilder().setSourceFieldId(text(settings, "sourceFieldId")));
            case DATABASE_FIELD_TYPE_BACKLINK -> b.setBacklink(DatabaseBacklinkFieldSettings.newBuilder()
                    .setSourceDatabaseContentId(text(settings, "sourceDatabaseContentId")));
            default -> {
                // checkbox, hyperlink, user, attachment, pageLink, entryLink: no settings
            }
        }
        return b.build();
    }

    /**
     * Maps one database entry (row); every field value is typed by its {@code type}.
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public DatabaseEntry toDatabaseEntry(JsonNode node) {
        DatabaseEntry.Builder b = DatabaseEntry.newBuilder()
                .setId(text(node, "id"))
                .setDatabaseId(text(node, "databaseId"));
        for (Map.Entry<String, JsonNode> value : node.path("values").properties()) {
            b.putValues(value.getKey(), toDatabaseFieldValue(value.getValue()));
        }
        return b.build();
    }

    private DatabaseFieldValue toDatabaseFieldValue(JsonNode node) {
        DatabaseFieldType type = databaseFieldType(text(node, "type"));
        DatabaseFieldValue.Builder b = DatabaseFieldValue.newBuilder().setType(type);
        JsonNode value = node.path("value");
        switch (type) {
            case DATABASE_FIELD_TYPE_TEXT -> {
                if (!node.path("plain").isNull() || !node.path("rich").isNull()) {
                    DatabaseTextValue.Builder text = DatabaseTextValue.newBuilder()
                            .setPlain(text(node, "plain"));
                    if (node.path("rich").isObject()) {
                        text.setRich(adf(node.path("rich")));
                    }
                    b.setText(text);
                }
            }
            case DATABASE_FIELD_TYPE_AI_SUMMARY -> {
                DatabaseAiSummaryValue.Builder summary = DatabaseAiSummaryValue.newBuilder()
                        .setPlain(text(node, "plain"));
                if (node.path("rich").isObject()) {
                    summary.setRich(adf(node.path("rich")));
                }
                JsonNode metadata = node.path("metadata");
                if (metadata.isObject()) {
                    DatabaseAiSummaryValueMetadata.Builder mb = DatabaseAiSummaryValueMetadata.newBuilder()
                            .setLastUpdatedBy(wireEnum(DatabaseAiSummaryUpdater.class,
                                    "DATABASE_AI_SUMMARY_UPDATER_", text(metadata, "lastUpdatedBy"),
                                    DatabaseAiSummaryUpdater.DATABASE_AI_SUMMARY_UPDATER_UNSPECIFIED))
                            .setLastUpdatedUserAccountId(text(metadata, "lastUpdatedUserAccountId"));
                    setTimestamp(metadata, "lastUpdatedAt", mb::setLastUpdatedAt);
                    summary.setSummaryMetadata(mb);
                }
                b.setAiSummary(summary);
            }
            case DATABASE_FIELD_TYPE_NUMBER -> {
                if (value.isTextual()) {
                    b.setNumber(value.asText());
                }
            }
            case DATABASE_FIELD_TYPE_CHECKBOX -> {
                if (value.isBoolean()) {
                    b.setCheckbox(value.asBoolean());
                }
            }
            case DATABASE_FIELD_TYPE_DATE -> {
                if (value.isTextual()) {
                    b.setDate(value.asText());
                }
            }
            case DATABASE_FIELD_TYPE_SELECT -> {
                if (value.isArray()) {
                    DatabaseSelectValue.Builder select = DatabaseSelectValue.newBuilder();
                    value.forEach(option -> select.addOptions(toSelectOption(option)));
                    b.setSelect(select);
                }
            }
            case DATABASE_FIELD_TYPE_HYPERLINK -> {
                if (value.isArray()) {
                    DatabaseHyperlinkValue.Builder links = DatabaseHyperlinkValue.newBuilder();
                    value.forEach(item -> links.addItems(DatabaseHyperlinkItem.newBuilder()
                            .setUrl(text(item, "url")).setLabel(text(item, "label"))));
                    b.setHyperlink(links);
                }
            }
            case DATABASE_FIELD_TYPE_USER -> {
                if (value.isArray()) {
                    DatabaseUserValue.Builder users = DatabaseUserValue.newBuilder();
                    value.forEach(item -> users.addAccountIds(text(item, "accountId")));
                    b.setUser(users);
                }
            }
            case DATABASE_FIELD_TYPE_ATTACHMENT -> {
                if (value.isArray()) {
                    DatabaseAttachmentValue.Builder media = DatabaseAttachmentValue.newBuilder();
                    value.forEach(item -> media.addMediaIds(text(item, "mediaId")));
                    b.setAttachment(media);
                }
            }
            case DATABASE_FIELD_TYPE_JIRA_ISSUE -> {
                if (value.isArray()) {
                    DatabaseJiraIssueValue.Builder issues = DatabaseJiraIssueValue.newBuilder();
                    value.forEach(item -> issues.addIssues(DatabaseJiraIssueRef.newBuilder()
                            .setKey(text(item, "key"))
                            .setCloudId(text(item, "cloudId"))
                            .setServerId(text(item, "serverId"))));
                    b.setJiraIssue(issues);
                }
            }
            case DATABASE_FIELD_TYPE_PAGE_LINK -> {
                if (value.isArray()) {
                    DatabasePageLinkValue.Builder pages = DatabasePageLinkValue.newBuilder();
                    value.forEach(item -> pages.addContentIds(text(item, "contentId")));
                    b.setPageLink(pages);
                }
            }
            case DATABASE_FIELD_TYPE_ENTRY_LINK -> {
                if (value.isArray()) {
                    DatabaseEntryLinkValue.Builder entries = DatabaseEntryLinkValue.newBuilder();
                    value.forEach(item -> entries.addEntries(DatabaseEntryRef.newBuilder()
                            .setEntryId(text(item, "entryId"))
                            .setDatabaseContentId(text(item, "databaseContentId"))));
                    b.setEntryLink(entries);
                }
            }
            default -> {
                // lookup fields (entryDetails, backlink, pageDetails, pageLabels,
                // pageStatus, jiraIssueDetails) always carry a null value
            }
        }
        return b.build();
    }

    /**
     * Maps one database view ({@code /databases/{id}/views}).
     *
     * @param node the API object
     * @return the proto; missing fields stay unset
     */
    public DatabaseView toDatabaseView(JsonNode node) {
        return DatabaseView.newBuilder()
                .setId(text(node, "id"))
                .setName(text(node, "name"))
                .setType(wireEnum(DatabaseViewType.class, "DATABASE_VIEW_TYPE_", text(node, "type"),
                        DatabaseViewType.DATABASE_VIEW_TYPE_UNSPECIFIED))
                .build();
    }

    private static DatabaseSelectOption toSelectOption(JsonNode node) {
        return DatabaseSelectOption.newBuilder()
                .setId(text(node, "id"))
                .setLabel(text(node, "label"))
                .build();
    }

    /** An ADF document is serialized content in a declared format, kept as its JSON text. */
    private static BodyType adf(JsonNode document) {
        return BodyType.newBuilder()
                .setFormat(BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT)
                .setValue(document.toString())
                .build();
    }

    private static DatabaseFieldType databaseFieldType(String wire) {
        return wireEnum(DatabaseFieldType.class, "DATABASE_FIELD_TYPE_", snake(wire),
                DatabaseFieldType.DATABASE_FIELD_TYPE_UNSPECIFIED);
    }

    // ======================================================================
    // INCLUDE-* EXPANSIONS
    // ======================================================================

    /**
     * Copies the {@code include-*} expansions present on a single-entity read
     * into the matching repeated fields of the builder, by field name. A
     * field the message does not declare is skipped, so one routine serves
     * every content kind. Space properties land in {@code space_properties}.
     *
     * @param b the builder of the mapped entity
     * @param node the API object
     */
    void applyIncludes(Message.Builder b, JsonNode node) {
        Descriptors.Descriptor type = b.getDescriptorForType();
        boolean space = type.getName().equals("Space");
        addEach(b, "labels", node.path("labels"), IncludeExpansion.INCLUDE_EXPANSION_LABELS,
                this::toLabel);
        addEach(b, "operations", node.path("operations"), IncludeExpansion.INCLUDE_EXPANSION_OPERATIONS,
                ConfluenceMapper::toOperation);
        addEach(b, "likes", node.path("likes"), IncludeExpansion.INCLUDE_EXPANSION_LIKES,
                like -> Like.newBuilder().setAccountId(text(like, "accountId")).build());
        addEach(b, "versions", node.path("versions"), IncludeExpansion.INCLUDE_EXPANSION_VERSIONS,
                this::toVersion);
        addEach(b, "direct_children", node.path("directChildren"),
                IncludeExpansion.INCLUDE_EXPANSION_DIRECT_CHILDREN, this::toTreeEntry);
        if (space) {
            addEach(b, "space_properties", node.path("properties"),
                    IncludeExpansion.INCLUDE_EXPANSION_PROPERTIES, this::toSpaceProperty);
            addEach(b, "permissions", node.path("permissions"),
                    IncludeExpansion.INCLUDE_EXPANSION_PERMISSIONS, this::toSpacePermissionAssignment);
            addEach(b, "role_assignments", node.path("roleAssignments"),
                    IncludeExpansion.INCLUDE_EXPANSION_ROLE_ASSIGNMENTS, this::toSpaceRoleAssignment);
        } else {
            addEach(b, "properties", node.path("properties"),
                    IncludeExpansion.INCLUDE_EXPANSION_PROPERTIES, this::toContentProperty);
        }
        Descriptors.FieldDescriptor favorited = type.findFieldByName("is_favorited_by_current_user");
        if (favorited != null && node.path("isFavoritedByCurrentUser").isBoolean()) {
            b.setField(favorited, node.path("isFavoritedByCurrentUser").asBoolean());
        }
    }

    /**
     * An include expansion is {@code {results: [...], meta: {hasMore, cursor}}};
     * anything else is not an expansion. A preview upstream cut short
     * ({@code meta.hasMore}) is recorded in {@code truncated_includes}; the
     * paging cursor is transport and is dropped.
     */
    private static void addEach(Message.Builder b, String fieldName, JsonNode expansion,
            IncludeExpansion kind, java.util.function.Function<JsonNode, Message> map) {
        Descriptors.FieldDescriptor field = b.getDescriptorForType().findFieldByName(fieldName);
        JsonNode results = expansion.path("results");
        if (field == null || !field.isRepeated() || !results.isArray()) {
            return;
        }
        results.forEach(item -> b.addRepeatedField(field, map.apply(item)));
        Descriptors.FieldDescriptor truncated =
                b.getDescriptorForType().findFieldByName("truncated_includes");
        if (truncated != null && expansion.path("meta").path("hasMore").asBoolean(false)) {
            b.addRepeatedField(truncated, kind.getValueDescriptor());
        }
    }

    private static Operation toOperation(JsonNode node) {
        return Operation.newBuilder()
                .setOperation(text(node, "operation"))
                .setTargetType(text(node, "targetType"))
                .build();
    }

    /** camelCase wire values ({@code aiSummary}, {@code userProfile}) to snake_case. */
    static String snake(String wire) {
        if (wire == null) {
            return "";
        }
        StringBuilder out = new StringBuilder(wire.length() + 4);
        for (int i = 0; i < wire.length(); i++) {
            char c = wire.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                out.append('_');
            }
            out.append(Character.toLowerCase(c));
        }
        return out.toString();
    }

    // ======================================================================
    // SHARED PIECES
    // ======================================================================

    /** Body slots: storage, atlas_doc_format, view, raw. Representation string wins over the slot default. */
    private void setBody(JsonNode node, java.util.function.Consumer<Body> bodySetter) {
        JsonNode body = node.path("body");
        if (!body.isObject()) {
            return;
        }
        Body.Builder b = Body.newBuilder();
        JsonNode storage = body.path("storage");
        if (storage.isObject()) {
            b.setStorage(toBodyType(storage, BodyFormat.BODY_FORMAT_STORAGE_XHTML));
        }
        JsonNode adf = body.path("atlas_doc_format");
        if (adf.isObject()) {
            b.setAtlasDocFormat(toBodyType(adf, BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT));
        }
        JsonNode view = body.path("view");
        if (view.isObject()) {
            b.setView(toBodyType(view, BodyFormat.BODY_FORMAT_RENDERED_XHTML));
        }
        JsonNode raw = body.path("raw");
        if (raw.isObject()) {
            b.setRaw(toBodyType(raw, BodyFormat.BODY_FORMAT_RAW));
        }
        bodySetter.accept(b.build());
    }

    private BodyType toBodyType(JsonNode node, BodyFormat slotDefault) {
        String representation = text(node, "representation");
        BodyFormat format = representation.isEmpty() ? slotDefault : bodyFormat(representation);
        return BodyType.newBuilder()
                .setFormat(format)
                .setValue(text(node, "value"))
                .build();
    }

    /**
     * The API's representation strings to the BodyFormat enum; unknown maps to UNSPECIFIED.
     *
     * @param representation the API {@code representation} string, or {@code null}
     * @return the matching {@link BodyFormat}; unknown values are {@code UNSPECIFIED}
     */
    public static BodyFormat bodyFormat(String representation) {
        return switch (representation == null ? ""
                : representation.trim().toLowerCase(Locale.ROOT)) {
            case "storage" -> BodyFormat.BODY_FORMAT_STORAGE_XHTML;
            case "atlas_doc_format" -> BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT;
            case "view" -> BodyFormat.BODY_FORMAT_RENDERED_XHTML;
            case "export_view" -> BodyFormat.BODY_FORMAT_EXPORT_XHTML;
            case "anonymous_export_view" -> BodyFormat.BODY_FORMAT_ANONYMOUS_EXPORT_XHTML;
            case "styled_view" -> BodyFormat.BODY_FORMAT_STYLED_XHTML;
            case "editor" -> BodyFormat.BODY_FORMAT_EDITOR;
            case "wiki" -> BodyFormat.BODY_FORMAT_WIKI;
            case "raw" -> BodyFormat.BODY_FORMAT_RAW;
            case "plain" -> BodyFormat.BODY_FORMAT_PLAIN_TEXT;
            default -> BodyFormat.BODY_FORMAT_UNSPECIFIED;
        };
    }

    private void setVersion(JsonNode node, java.util.function.Consumer<Version> versionSetter) {
        JsonNode version = node.path("version");
        if (version.isObject()) {
            versionSetter.accept(toVersion(version));
        }
    }

    private void setTimestamp(JsonNode node, String field,
            java.util.function.Consumer<Timestamp> setter) {
        Timestamp at = timestamp(text(node, field));
        if (at != null) {
            setter.accept(at);
        }
    }

    /**
     * RFC3339 / ISO-8601 to Timestamp. Tolerant of offset forms
     * ({@code +00:00} as well as {@code Z}); blank or unparseable input
     * yields null so the field stays unset.
     *
     * @param rfc3339 an RFC3339 / ISO-8601 timestamp, or blank
     * @return the proto timestamp, or {@code null} when input is blank or unparseable
     */
    public static Timestamp timestamp(String rfc3339) {
        if (rfc3339 == null || rfc3339.isBlank()) {
            return null;
        }
        String trimmed = rfc3339.trim();
        Instant instant;
        try {
            instant = Instant.parse(trimmed);
        } catch (DateTimeParseException e) {
            try {
                instant = OffsetDateTime.parse(trimmed).toInstant();
            } catch (DateTimeParseException e2) {
                return null;
            }
        }
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }

    /**
     * Composes an absolute URL from a server-relative path. Confluence Cloud
     * returns {@code webui} links without the {@code /wiki} prefix and
     * {@code _links.next} with it, so both forms are handled: paths already
     * carrying the base path resolve against the origin, bare paths against
     * the full base URL.
     *
     * @param url a relative or absolute URL, or blank
     * @return the absolute URL, or empty when {@code url} is blank
     */
    public String absolute(String url) {
        if (url == null || url.isBlank()) {
            return "";
        }
        if (url.startsWith("http")) {
            return url;
        }
        String path = url.startsWith("/") ? url : "/" + url;
        if (!basePath.isEmpty() && path.startsWith(basePath + "/")) {
            return origin + path;
        }
        return baseUrl + path;
    }

    private static String webui(JsonNode node) {
        return text(node.path("_links"), "webui");
    }

    /** Generic enum mapping: wire value uppercased, '-' to '_', prefixed; unknown yields the fallback. */
    static <E extends Enum<E>> E wireEnum(Class<E> type, String prefix, String wire, E fallback) {
        if (wire == null || wire.isBlank()) {
            return fallback;
        }
        String name = prefix + wire.trim().replace('-', '_').toUpperCase(Locale.ROOT);
        try {
            return Enum.valueOf(type, name);
        } catch (IllegalArgumentException e) {
            return fallback;
        }
    }

    /** Jackson tree to protobuf Struct, for property values that genuinely are structured JSON. */
    static Struct toStruct(JsonNode node) {
        Struct.Builder b = Struct.newBuilder();
        for (Map.Entry<String, JsonNode> field : node.properties()) {
            b.putFields(field.getKey(), toValue(field.getValue()));
        }
        return b.build();
    }

    /** Jackson array to protobuf ListValue, element by element. */
    static ListValue toListValue(JsonNode array) {
        ListValue.Builder list = ListValue.newBuilder();
        array.forEach(item -> list.addValues(toValue(item)));
        return list.build();
    }

    private static Value toValue(JsonNode node) {
        Value.Builder b = Value.newBuilder();
        if (node == null || node.isNull() || node.isMissingNode()) {
            return b.setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build();
        }
        if (node.isTextual()) {
            return b.setStringValue(node.asText()).build();
        }
        if (node.isBoolean()) {
            return b.setBoolValue(node.asBoolean()).build();
        }
        if (node.isNumber()) {
            return b.setNumberValue(node.doubleValue()).build();
        }
        if (node.isArray()) {
            return b.setListValue(toListValue(node)).build();
        }
        if (node.isObject()) {
            return b.setStructValue(toStruct(node)).build();
        }
        return b.setStringValue(node.asText()).build();
    }

    private static boolean hasText(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() && !value.asText().isEmpty();
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.path(field);
        return value.isTextual() ? value.asText() : "";
    }

    // ======================================================================
    // VERSION HISTORY ENTRIES
    // ======================================================================

    /**
     * Maps one {@code /{content-type}/{id}/versions} entry: the version
     * fields plus its entity snapshot, which the API nests under the
     * content kind's key ({@code page}, {@code blogpost}, {@code attachment},
     * {@code comment} or {@code custom}).
     *
     * @param node the API object
     * @return the proto with {@code entity} set when the API sent a snapshot
     */
    public Version toVersionEntry(JsonNode node) {
        Version.Builder b = toVersion(node).toBuilder();
        for (String key : List.of("page", "blogpost", "attachment", "comment", "custom")) {
            JsonNode snapshot = node.path(key);
            if (snapshot.isObject()) {
                ai.pipestream.confluence.v1.VersionedEntity.Builder entity =
                        ai.pipestream.confluence.v1.VersionedEntity.newBuilder()
                                .setId(text(snapshot, "id"))
                                .setTitle(text(snapshot, "title"));
                setBody(snapshot, entity::setBody);
                b.setEntity(entity.build());
                break;
            }
        }
        return b.build();
    }
}
