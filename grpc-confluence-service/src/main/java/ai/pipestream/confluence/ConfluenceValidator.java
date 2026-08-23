package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.AdminKeyResponse;
import ai.pipestream.confluence.v1.AppProperty;
import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.BlogPost;
import ai.pipestream.confluence.v1.Body;
import ai.pipestream.confluence.v1.BodyFormat;
import ai.pipestream.confluence.v1.BodyType;
import ai.pipestream.confluence.v1.BulkTransitionTaskStatusResponse;
import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ClassificationLevel;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.confluence.v1.ContentProperty;
import ai.pipestream.confluence.v1.ContentTreeEntry;
import ai.pipestream.confluence.v1.CustomContent;
import ai.pipestream.confluence.v1.DataPolicy;
import ai.pipestream.confluence.v1.Database;
import ai.pipestream.confluence.v1.DetailedVersion;
import ai.pipestream.confluence.v1.Folder;
import ai.pipestream.confluence.v1.Label;
import ai.pipestream.confluence.v1.Like;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.PropertyKey;
import ai.pipestream.confluence.v1.Redaction;
import ai.pipestream.confluence.v1.RedactionResponse;
import ai.pipestream.confluence.v1.RedactionSectionResponse;
import ai.pipestream.confluence.v1.SmartLink;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.SpaceDescription;
import ai.pipestream.confluence.v1.SpaceProperty;
import ai.pipestream.confluence.v1.SpacePropertyVersion;
import ai.pipestream.confluence.v1.Task;
import ai.pipestream.confluence.v1.User;
import ai.pipestream.confluence.v1.Version;
import ai.pipestream.confluence.v1.VersionedEntity;
import ai.pipestream.confluence.v1.Whiteboard;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Programmatic stand-in for the {@code validate.v1} options the Confluence
 * domain model carried in protomolt. Plain gRPC has no field-level
 * validators, so the same identity, numeric-floor, and cross-field rules
 * run here before an entity leaves the mapper, the crawler, or the gRPC
 * facade.
 */
public final class ConfluenceValidator {

    private static final ConfluenceValidator INSTANCE = new ConfluenceValidator();
    private static final Pattern EMAIL = Pattern.compile("^[^ @]+@[^ @]+[.][^ @]+$");
    private static final Pattern UUID = Pattern.compile(
            "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private ConfluenceValidator() {
    }

    /**
     * Returns the shared validator.
     *
     * @return the singleton instance
     */
    public static ConfluenceValidator create() {
        return INSTANCE;
    }

    /**
     * One rule failure: proto field path, rule id, and human message.
     *
     * @param path the proto field path, empty for the message itself
     * @param ruleId a stable rule identifier
     * @param message a human-readable failure
     */
    public record Violation(String path, String ruleId, String message) {
    }

    /**
     * The collected violations for one message (empty = valid).
     *
     * @param violations the failures; copied
     */
    public record ValidationResult(List<Violation> violations) {
        /** Validates and normalizes fields. */
        public ValidationResult {
            violations = List.copyOf(violations);
        }

        /**
         * Whether the message passed every rule.
         *
         * @return {@code true} when {@code violations} is empty
         */
        public boolean isValid() {
            return violations.isEmpty();
        }

        /** Throws {@code FAILED_PRECONDITION} when this result is not valid. */
        public void throwIfInvalid() {
            if (violations.isEmpty()) {
                return;
            }
            StringBuilder sb = new StringBuilder("confluence validation failed:");
            for (Violation violation : violations) {
                sb.append(" [").append(violation.ruleId()).append(" at ")
                        .append(violation.path().isEmpty() ? "<message>" : violation.path())
                        .append(": ").append(violation.message()).append(']');
            }
            throw Status.FAILED_PRECONDITION.withDescription(sb.toString()).asRuntimeException();
        }
    }

    /**
     * Checks {@code message} against the domain rules.
     *
     * @param message a Confluence domain proto
     * @return the collected violations (empty when valid)
     */
    public ValidationResult validate(Message message) {
        List<Violation> violations = new ArrayList<>();
        visit("", message, violations);
        return new ValidationResult(violations);
    }

    /**
     * Validates {@code message} and throws if any rule fails.
     *
     * @param message a Confluence domain proto
     */
    public void requireValid(Message message) {
        validate(message).throwIfInvalid();
    }

    private static void visit(String prefix, Message message, List<Violation> out) {
        switch (message) {
            case Page page -> visitPage(prefix, page, out);
            case BlogPost post -> visitBlogPost(prefix, post, out);
            case Comment comment -> visitComment(prefix, comment, out);
            case Attachment attachment -> visitAttachment(prefix, attachment, out);
            case Space space -> visitSpace(prefix, space, out);
            case User user -> visitUser(prefix, user, out);
            case Task task -> visitTask(prefix, task, out);
            case Label label -> visitLabel(prefix, label, out);
            case Like like -> visitLike(prefix, like, out);
            case CustomContent custom -> visitCustomContent(prefix, custom, out);
            case Database database -> visitDatabase(prefix, database, out);
            case Folder folder -> visitFolder(prefix, folder, out);
            case SmartLink smartLink -> visitSmartLink(prefix, smartLink, out);
            case Whiteboard whiteboard -> visitWhiteboard(prefix, whiteboard, out);
            case ContentTreeEntry entry -> required(prefix, "id", entry.getId(), out);
            case ClassificationLevel level -> required(prefix, "id", level.getId(), out);
            case DataPolicy policy -> required(prefix, "id", policy.getId(), out);
            case ContentProperty property -> visitContentProperty(prefix, property, out);
            case SpaceProperty property -> visitSpaceProperty(prefix, property, out);
            case AppProperty property -> visitAppProperty(prefix, property, out);
            case AdminKeyResponse key -> required(prefix, "account_id", key.getAccountId(), out);
            case ConfluenceEntity entity -> visitEntity(prefix, entity, out);
            case ConfluenceSnapshot snapshot -> required(prefix, "snapshot_id", snapshot.getSnapshotId(), out);
            case ConfluenceChange change -> visitChange(prefix, change, out);
            case BodyType body -> visitBodyType(prefix, body, out);
            case Body body -> visitBody(prefix, body, out);
            case Version version -> visitVersion(prefix, version, out);
            case DetailedVersion version -> visitDetailedVersion(prefix, version, out);
            case VersionedEntity entity -> {
                required(prefix, "id", entity.getId(), out);
                if (entity.hasBody()) {
                    visitBody(child(prefix, "body"), entity.getBody(), out);
                }
            }
            case SpaceDescription description -> {
                if (description.hasPlain()) {
                    visitBodyType(child(prefix, "plain"), description.getPlain(), out);
                }
                if (description.hasView()) {
                    visitBodyType(child(prefix, "view"), description.getView(), out);
                }
            }
            case SpacePropertyVersion version -> gte(prefix, "number", version.getNumber(), 0, "int32.gte", out);
            case Redaction redaction -> visitRedaction(prefix, redaction, out);
            case RedactionSectionResponse section -> {
                for (int i = 0; i < section.getRedactionsCount(); i++) {
                    visitRedaction(child(prefix, "redactions[" + i + "]"), section.getRedactions(i), out);
                }
            }
            case RedactionResponse response -> {
                if (response.hasBody()) {
                    visit(child(prefix, "body"), response.getBody(), out);
                }
                if (response.hasTitle()) {
                    visit(child(prefix, "title"), response.getTitle(), out);
                }
            }
            case BulkTransitionTaskStatusResponse task -> required(prefix, "task_id", task.getTaskId(), out);
            default -> {
                // Unknown message: nothing to check at this node.
            }
        }
    }

    private static void visitPage(String prefix, Page page, List<Violation> out) {
        required(prefix, "id", page.getId(), out);
        required(prefix, "space_id", page.getSpaceId(), out);
        if (page.hasBody()) {
            visitBody(child(prefix, "body"), page.getBody(), out);
        }
        if (page.hasVersion()) {
            visitVersion(child(prefix, "version"), page.getVersion(), out);
        }
        visitLabels(prefix, page.getLabelsList(), out);
        visitContentProperties(prefix, page.getPropertiesList(), out);
        visitLikes(prefix, page.getLikesList(), out);
        visitVersions(prefix, page.getVersionsList(), out);
    }

    private static void visitBlogPost(String prefix, BlogPost post, List<Violation> out) {
        required(prefix, "id", post.getId(), out);
        required(prefix, "space_id", post.getSpaceId(), out);
        if (post.hasBody()) {
            visitBody(child(prefix, "body"), post.getBody(), out);
        }
        if (post.hasVersion()) {
            visitVersion(child(prefix, "version"), post.getVersion(), out);
        }
        visitLabels(prefix, post.getLabelsList(), out);
        visitContentProperties(prefix, post.getPropertiesList(), out);
        visitLikes(prefix, post.getLikesList(), out);
        visitVersions(prefix, post.getVersionsList(), out);
    }

    private static void visitComment(String prefix, Comment comment, List<Violation> out) {
        required(prefix, "id", comment.getId(), out);
        if (comment.hasBody()) {
            visitBody(child(prefix, "body"), comment.getBody(), out);
        }
        if (comment.hasVersion()) {
            visitVersion(child(prefix, "version"), comment.getVersion(), out);
        }
        visitContentProperties(prefix, comment.getPropertiesList(), out);
        visitLikes(prefix, comment.getLikesList(), out);
        visitVersions(prefix, comment.getVersionsList(), out);
    }

    private static void visitAttachment(String prefix, Attachment attachment, List<Violation> out) {
        required(prefix, "id", attachment.getId(), out);
        gte(prefix, "file_size", attachment.getFileSize(), 0, "int64.gte", out);
        if (attachment.hasVersion()) {
            visitVersion(child(prefix, "version"), attachment.getVersion(), out);
        }
        visitLabels(prefix, attachment.getLabelsList(), out);
        visitContentProperties(prefix, attachment.getPropertiesList(), out);
        visitVersions(prefix, attachment.getVersionsList(), out);
    }

    private static void visitSpace(String prefix, Space space, List<Violation> out) {
        required(prefix, "id", space.getId(), out);
        required(prefix, "key", space.getKey(), out);
        if (space.hasDescription()) {
            visit(child(prefix, "description"), space.getDescription(), out);
        }
        visitLabels(prefix, space.getLabelsList(), out);
        visitContentProperties(prefix, space.getPropertiesList(), out);
    }

    private static void visitUser(String prefix, User user, List<Violation> out) {
        required(prefix, "account_id", user.getAccountId(), out);
        if (!user.getEmail().isEmpty() && !EMAIL.matcher(user.getEmail()).matches()) {
            out.add(new Violation(child(prefix, "email"), "user.email_format",
                    "email must be empty or a valid address"));
        }
    }

    private static void visitTask(String prefix, Task task, List<Violation> out) {
        required(prefix, "id", task.getId(), out);
        if (task.hasBody()) {
            visitBody(child(prefix, "body"), task.getBody(), out);
        }
    }

    private static void visitLabel(String prefix, Label label, List<Violation> out) {
        required(prefix, "name", label.getName(), out);
    }

    private static void visitLike(String prefix, Like like, List<Violation> out) {
        required(prefix, "account_id", like.getAccountId(), out);
    }

    private static void visitCustomContent(String prefix, CustomContent custom, List<Violation> out) {
        required(prefix, "id", custom.getId(), out);
        if (custom.hasBody()) {
            visitBody(child(prefix, "body"), custom.getBody(), out);
        }
        if (custom.hasVersion()) {
            visitVersion(child(prefix, "version"), custom.getVersion(), out);
        }
        visitLabels(prefix, custom.getLabelsList(), out);
        visitContentProperties(prefix, custom.getPropertiesList(), out);
        visitVersions(prefix, custom.getVersionsList(), out);
    }

    private static void visitDatabase(String prefix, Database database, List<Violation> out) {
        required(prefix, "id", database.getId(), out);
        if (database.hasVersion()) {
            visitVersion(child(prefix, "version"), database.getVersion(), out);
        }
    }

    private static void visitFolder(String prefix, Folder folder, List<Violation> out) {
        required(prefix, "id", folder.getId(), out);
        if (folder.hasVersion()) {
            visitVersion(child(prefix, "version"), folder.getVersion(), out);
        }
    }

    private static void visitSmartLink(String prefix, SmartLink smartLink, List<Violation> out) {
        required(prefix, "id", smartLink.getId(), out);
        if (smartLink.hasVersion()) {
            visitVersion(child(prefix, "version"), smartLink.getVersion(), out);
        }
    }

    private static void visitWhiteboard(String prefix, Whiteboard whiteboard, List<Violation> out) {
        required(prefix, "id", whiteboard.getId(), out);
        if (whiteboard.hasVersion()) {
            visitVersion(child(prefix, "version"), whiteboard.getVersion(), out);
        }
    }

    private static void visitContentProperty(String prefix, ContentProperty property, List<Violation> out) {
        required(prefix, "id", property.getId(), out);
        customKey(prefix, property.getKey(), property.getCustomKey(), out);
        if (property.hasVersion()) {
            visitVersion(child(prefix, "version"), property.getVersion(), out);
        }
    }

    private static void visitSpaceProperty(String prefix, SpaceProperty property, List<Violation> out) {
        required(prefix, "id", property.getId(), out);
        customKey(prefix, property.getKey(), property.getCustomKey(), out);
        if (property.hasVersion()) {
            visit(child(prefix, "version"), property.getVersion(), out);
        }
    }

    private static void visitAppProperty(String prefix, AppProperty property, List<Violation> out) {
        customKey(prefix, property.getKey(), property.getCustomKey(), out);
    }

    private static void visitEntity(String prefix, ConfluenceEntity entity, List<Violation> out) {
        required(prefix, "entity_id", entity.getEntityId(), out);
        requiredTimestamp(prefix, "ingested_at", entity.hasIngestedAt(), entity.getIngestedAt(), out);
        if (entity.getEntityCase() != ConfluenceEntity.EntityCase.ENTITY_NOT_SET) {
            visit(child(prefix, fieldName(entity.getEntityCase())), nestedEntity(entity), out);
        }
    }

    private static void visitChange(String prefix, ConfluenceChange change, List<Violation> out) {
        required(prefix, "change_id", change.getChangeId(), out);
        if (change.getOperation() == ChangeOperation.CHANGE_OPERATION_UPSERT && !change.hasEntity()) {
            out.add(new Violation(prefix, "change.upsert_has_entity",
                    "an UPSERT change must carry the entity it upserts"));
        }
        if (change.hasEntity()) {
            visitEntity(child(prefix, "entity"), change.getEntity(), out);
        }
    }

    private static void visitBodyType(String prefix, BodyType body, List<Violation> out) {
        if (!body.getValue().isEmpty() && body.getFormat() == BodyFormat.BODY_FORMAT_UNSPECIFIED) {
            out.add(new Violation(prefix, "body_type.format_declared",
                    "a populated body value must declare its format"));
        }
    }

    private static void visitBody(String prefix, Body body, List<Violation> out) {
        if (body.hasStorage()) {
            visitBodyType(child(prefix, "storage"), body.getStorage(), out);
        }
        if (body.hasAtlasDocFormat()) {
            visitBodyType(child(prefix, "atlas_doc_format"), body.getAtlasDocFormat(), out);
        }
        if (body.hasView()) {
            visitBodyType(child(prefix, "view"), body.getView(), out);
        }
        if (body.hasRaw()) {
            visitBodyType(child(prefix, "raw"), body.getRaw(), out);
        }
    }

    private static void visitVersion(String prefix, Version version, List<Violation> out) {
        gte(prefix, "number", version.getNumber(), 0, "int32.gte", out);
        if (version.hasEntity()) {
            visit(child(prefix, "entity"), version.getEntity(), out);
        }
    }

    private static void visitDetailedVersion(String prefix, DetailedVersion version, List<Violation> out) {
        gte(prefix, "number", version.getNumber(), 0, "int32.gte", out);
        if (version.hasPrevVersion()) {
            gte(prefix, "prev_version", version.getPrevVersion(), 1, "int32.gte", out);
        }
        if (version.hasNextVersion()) {
            gte(prefix, "next_version", version.getNextVersion(), 1, "int32.gte", out);
        }
    }

    private static void visitRedaction(String prefix, Redaction redaction, List<Violation> out) {
        required(prefix, "pointer", redaction.getPointer(), out);
        gte(prefix, "from", redaction.getFrom(), 0, "int32.gte", out);
        gte(prefix, "to", redaction.getTo(), 0, "int32.gte", out);
        if (redaction.getTo() != 0 && redaction.getFrom() > redaction.getTo()) {
            out.add(new Violation(prefix, "redaction.range",
                    "a redaction range must not end before it starts"));
        }
        if (!redaction.getRedactionId().isEmpty() && !UUID.matcher(redaction.getRedactionId()).matches()) {
            out.add(new Violation(child(prefix, "redaction_id"), "redaction.id_uuid",
                    "redaction_id must be empty or a UUID"));
        }
    }

    private static void visitLabels(String prefix, List<Label> labels, List<Violation> out) {
        for (int i = 0; i < labels.size(); i++) {
            visitLabel(child(prefix, "labels[" + i + "]"), labels.get(i), out);
        }
    }

    private static void visitContentProperties(String prefix, List<ContentProperty> properties,
            List<Violation> out) {
        for (int i = 0; i < properties.size(); i++) {
            visitContentProperty(child(prefix, "properties[" + i + "]"), properties.get(i), out);
        }
    }

    private static void visitLikes(String prefix, List<Like> likes, List<Violation> out) {
        for (int i = 0; i < likes.size(); i++) {
            visitLike(child(prefix, "likes[" + i + "]"), likes.get(i), out);
        }
    }

    private static void visitVersions(String prefix, List<Version> versions, List<Violation> out) {
        for (int i = 0; i < versions.size(); i++) {
            visitVersion(child(prefix, "versions[" + i + "]"), versions.get(i), out);
        }
    }

    private static void customKey(String prefix, PropertyKey key, String customKey, List<Violation> out) {
        boolean custom = key == PropertyKey.PROPERTY_KEY_CUSTOM;
        boolean hasCustomKey = customKey != null && !customKey.isEmpty();
        if (custom != hasCustomKey) {
            out.add(new Violation(prefix, "property.custom_key",
                    "custom_key is required when key is PROPERTY_KEY_CUSTOM and forbidden otherwise"));
        }
    }

    private static void required(String prefix, String field, String value, List<Violation> out) {
        if (value == null || value.isEmpty()) {
            out.add(new Violation(child(prefix, field), "required", field + " is required"));
        }
    }

    private static void requiredTimestamp(String prefix, String field, boolean present, Timestamp value,
            List<Violation> out) {
        if (!present || (value.getSeconds() == 0 && value.getNanos() == 0)) {
            out.add(new Violation(child(prefix, field), "required", field + " is required"));
        }
    }

    private static void gte(String prefix, String field, long value, long floor, String ruleId,
            List<Violation> out) {
        if (value < floor) {
            out.add(new Violation(child(prefix, field), ruleId, field + " must be >= " + floor));
        }
    }

    private static String child(String prefix, String field) {
        return prefix == null || prefix.isEmpty() ? field : prefix + "." + field;
    }

    private static String fieldName(ConfluenceEntity.EntityCase entityCase) {
        return switch (entityCase) {
            case PAGE -> "page";
            case BLOG_POST -> "blog_post";
            case COMMENT -> "comment";
            case ATTACHMENT -> "attachment";
            case SPACE -> "space";
            case LABEL -> "label";
            case TASK -> "task";
            case USER -> "user";
            case WHITEBOARD -> "whiteboard";
            case DATABASE -> "database";
            case FOLDER -> "folder";
            case CUSTOM_CONTENT -> "custom_content";
            case SMART_LINK -> "smart_link";
            case CLASSIFICATION_LEVEL -> "classification_level";
            case CONTENT_PROPERTY -> "content_property";
            case SPACE_PROPERTY -> "space_property";
            case APP_PROPERTY -> "app_property";
            case DATA_POLICY -> "data_policy";
            case LIKE -> "like";
            case ANCESTOR -> "ancestor";
            case VERSION -> "version";
            case ADMIN_KEY -> "admin_key";
            case OPERATION -> "operation";
            case REDACTION -> "redaction";
            case SPACE_ROLE -> "space_role";
            case SPACE_PERMISSION_ASSIGNMENT -> "space_permission_assignment";
            case TREE_ENTRY -> "tree_entry";
            case ENTITY_NOT_SET -> "entity";
        };
    }

    private static Message nestedEntity(ConfluenceEntity entity) {
        return switch (entity.getEntityCase()) {
            case PAGE -> entity.getPage();
            case BLOG_POST -> entity.getBlogPost();
            case COMMENT -> entity.getComment();
            case ATTACHMENT -> entity.getAttachment();
            case SPACE -> entity.getSpace();
            case LABEL -> entity.getLabel();
            case TASK -> entity.getTask();
            case USER -> entity.getUser();
            case WHITEBOARD -> entity.getWhiteboard();
            case DATABASE -> entity.getDatabase();
            case FOLDER -> entity.getFolder();
            case CUSTOM_CONTENT -> entity.getCustomContent();
            case SMART_LINK -> entity.getSmartLink();
            case CLASSIFICATION_LEVEL -> entity.getClassificationLevel();
            case CONTENT_PROPERTY -> entity.getContentProperty();
            case SPACE_PROPERTY -> entity.getSpaceProperty();
            case APP_PROPERTY -> entity.getAppProperty();
            case DATA_POLICY -> entity.getDataPolicy();
            case LIKE -> entity.getLike();
            case ANCESTOR -> entity.getAncestor();
            case VERSION -> entity.getVersion();
            case ADMIN_KEY -> entity.getAdminKey();
            case OPERATION -> entity.getOperation();
            case REDACTION -> entity.getRedaction();
            case SPACE_ROLE -> entity.getSpaceRole();
            case SPACE_PERMISSION_ASSIGNMENT -> entity.getSpacePermissionAssignment();
            case TREE_ENTRY -> entity.getTreeEntry();
            case ENTITY_NOT_SET -> entity;
        };
    }
}
