package ai.pipestream.output;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.BlogPost;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.Task;
import ai.pipestream.microsoft.v1.Drive;
import ai.pipestream.microsoft.v1.DriveItem;
import ai.pipestream.microsoft.v1.GraphUser;
import ai.pipestream.microsoft.v1.MicrosoftChange;
import ai.pipestream.microsoft.v1.MicrosoftEntity;
import ai.pipestream.microsoft.v1.Site;
import com.google.protobuf.Message;

/**
 * Store keys organized by Confluence / Graph hierarchy. Segments are
 * filesystem-safe; result has no leading slash.
 */
public final class ObjectKeys {

    private ObjectKeys() {
    }

    /**
     * Hierarchy key for a change message, or {@code records/{hash}} when the
     * type is unknown.
     *
     * @param record a change
     * @return store key without extension
     */
    public static String of(Message record) {
        if (record instanceof ConfluenceChange change && change.hasEntity()) {
            return confluence(change.getEntity());
        }
        if (record instanceof MicrosoftChange change && change.hasEntity()) {
            return microsoft(change.getEntity());
        }
        return join("records", Integer.toHexString(System.identityHashCode(record)));
    }

    /**
     * Confluence hierarchy: {@code {space}/pages/{id}},
     * {@code {space}/pages/{pageId}/comments/{id}},
     * {@code {space}/pages/{pageId}/attachments/{id}}, …
     *
     * @param entity wrapped entity
     * @return key without extension
     */
    public static String confluence(ConfluenceEntity entity) {
        return switch (entity.getEntityCase()) {
            case PAGE -> {
                Page page = entity.getPage();
                yield join(space(page.getSpaceId()), "pages", id(page.getId(), entity));
            }
            case BLOG_POST -> {
                BlogPost post = entity.getBlogPost();
                yield join(space(post.getSpaceId()), "blogs", id(post.getId(), entity));
            }
            case COMMENT -> {
                Comment comment = entity.getComment();
                String parent = comment.getPageId().isBlank()
                        ? join("blogs", segment(comment.getBlogPostId()))
                        : join("pages", segment(comment.getPageId()));
                yield join(parent, "comments", id(comment.getId(), entity));
            }
            case ATTACHMENT -> {
                Attachment attachment = entity.getAttachment();
                String parent = attachment.getPageId().isBlank()
                        ? join("blogs", segment(attachment.getBlogPostId()))
                        : join("pages", segment(attachment.getPageId()));
                yield join(parent, "attachments", id(attachment.getId(), entity));
            }
            case SPACE -> {
                Space space = entity.getSpace();
                String key = space.getKey().isBlank() ? space.getId() : space.getKey();
                yield join(space(key), "spaces", id(key, entity));
            }
            case TASK -> {
                Task task = entity.getTask();
                yield join(space(task.getSpaceId()), "tasks", id(task.getId(), entity));
            }
            case LABEL -> join("labels", id(entity.getLabel().getId(), entity));
            case USER -> join("users", id(entity.getUser().getAccountId(), entity));
            case CONTENT_PROPERTY -> join("properties", "content",
                    id(entity.getContentProperty().getId(), entity));
            case SPACE_PROPERTY -> join("properties", "space",
                    id(entity.getSpaceProperty().getId(), entity));
            default -> join("entities",
                    segment(entity.getEntityCase().name().toLowerCase()),
                    id(entity.getEntityId(), entity));
        };
    }

    /**
     * Graph hierarchy: {@code sites/{id}}, {@code sites/{site}/drives/{id}},
     * {@code drives/{driveId}/items/{id}}, {@code users/{id}}.
     *
     * @param entity wrapped entity
     * @return key without extension
     */
    public static String microsoft(MicrosoftEntity entity) {
        return switch (entity.getEntityCase()) {
            case SITE -> {
                Site site = entity.getSite();
                yield join("sites", id(site.getId(), entity));
            }
            case DRIVE -> {
                Drive drive = entity.getDrive();
                yield drive.getSiteId().isBlank()
                        ? join("drives", id(drive.getId(), entity))
                        : join("sites", segment(drive.getSiteId()), "drives",
                                id(drive.getId(), entity));
            }
            case DRIVE_ITEM -> {
                DriveItem item = entity.getDriveItem();
                yield join("drives", segment(item.getDriveId()), "items",
                        id(item.getId(), entity));
            }
            case USER -> {
                GraphUser user = entity.getUser();
                yield join("users", id(user.getId(), entity));
            }
            default -> join("entities", id(entity.getEntityId(), entity));
        };
    }

    /**
     * Joins prefix and key with {@code /}, skipping blanks.
     *
     * @param prefix optional prefix
     * @param key relative key
     * @return combined key
     */
    public static String under(String prefix, String key) {
        if (prefix == null || prefix.isBlank()) {
            return key;
        }
        return join(prefix, key);
    }

    /**
     * Joins segments with {@code /}.
     *
     * @param parts path pieces
     * @return relative path
     */
    public static String join(String... parts) {
        StringBuilder out = new StringBuilder();
        for (String part : parts) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String cleaned = part.replace('\\', '/');
            while (cleaned.startsWith("/")) {
                cleaned = cleaned.substring(1);
            }
            while (cleaned.endsWith("/")) {
                cleaned = cleaned.substring(0, cleaned.length() - 1);
            }
            if (cleaned.isEmpty()) {
                continue;
            }
            if (out.length() > 0) {
                out.append('/');
            }
            out.append(cleaned);
        }
        return out.isEmpty() ? "_" : out.toString();
    }

    /**
     * Sanitizes one path segment.
     *
     * @param raw raw id
     * @return safe segment
     */
    public static String segment(String raw) {
        if (raw == null || raw.isBlank()) {
            return "_";
        }
        StringBuilder out = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (Character.isLetterOrDigit(c) || c == '.' || c == '-' || c == '_') {
                out.append(c);
            } else {
                out.append('_');
            }
        }
        return out.isEmpty() ? "_" : out.toString();
    }

    private static String space(String spaceId) {
        return spaceId == null || spaceId.isBlank() ? "" : segment(spaceId);
    }

    private static String id(String nativeId, ConfluenceEntity entity) {
        if (nativeId != null && !nativeId.isBlank()) {
            return segment(nativeId);
        }
        return segment(entity.getEntityId());
    }

    private static String id(String nativeId, MicrosoftEntity entity) {
        if (nativeId != null && !nativeId.isBlank()) {
            return segment(nativeId);
        }
        return segment(entity.getEntityId());
    }
}
