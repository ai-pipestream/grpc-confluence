package ai.pipestream.confluence.output;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.BlogPost;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.Task;
import ai.pipestream.output.ObjectKeys;

/**
 * Store keys organized by Confluence hierarchy. Segments are
 * filesystem-safe; result has no leading slash.
 */
public final class ConfluenceObjectKeys {

    private ConfluenceObjectKeys() {
    }

    /**
     * Hierarchy key for a change, or {@code records/{hash}} when the
     * change carries no entity.
     *
     * @param change a crawler change
     * @return store key without extension
     */
    public static String of(ConfluenceChange change) {
        if (change.hasEntity()) {
            return of(change.getEntity());
        }
        return ObjectKeys.join("records", Integer.toHexString(System.identityHashCode(change)));
    }

    /**
     * Confluence hierarchy: {@code {space}/pages/{id}},
     * {@code {space}/pages/{pageId}/comments/{id}},
     * {@code {space}/pages/{pageId}/attachments/{id}}, …
     *
     * @param entity wrapped entity
     * @return key without extension
     */
    public static String of(ConfluenceEntity entity) {
        return switch (entity.getEntityCase()) {
            case PAGE -> {
                Page page = entity.getPage();
                yield ObjectKeys.join(space(page.getSpaceId()), "pages", id(page.getId(), entity));
            }
            case BLOG_POST -> {
                BlogPost post = entity.getBlogPost();
                yield ObjectKeys.join(space(post.getSpaceId()), "blogs", id(post.getId(), entity));
            }
            case COMMENT -> {
                Comment comment = entity.getComment();
                String parent = comment.getPageId().isBlank()
                        ? ObjectKeys.join("blogs", ObjectKeys.segment(comment.getBlogPostId()))
                        : ObjectKeys.join("pages", ObjectKeys.segment(comment.getPageId()));
                yield ObjectKeys.join(parent, "comments", id(comment.getId(), entity));
            }
            case ATTACHMENT -> {
                Attachment attachment = entity.getAttachment();
                String parent = attachment.getPageId().isBlank()
                        ? ObjectKeys.join("blogs", ObjectKeys.segment(attachment.getBlogPostId()))
                        : ObjectKeys.join("pages", ObjectKeys.segment(attachment.getPageId()));
                yield ObjectKeys.join(parent, "attachments", id(attachment.getId(), entity));
            }
            case SPACE -> {
                Space space = entity.getSpace();
                String key = space.getKey().isBlank() ? space.getId() : space.getKey();
                yield ObjectKeys.join(space(key), "spaces", id(key, entity));
            }
            case TASK -> {
                Task task = entity.getTask();
                yield ObjectKeys.join(space(task.getSpaceId()), "tasks", id(task.getId(), entity));
            }
            case LABEL -> ObjectKeys.join("labels", id(entity.getLabel().getId(), entity));
            case USER -> ObjectKeys.join("users", id(entity.getUser().getAccountId(), entity));
            case CONTENT_PROPERTY -> ObjectKeys.join("properties", "content",
                    id(entity.getContentProperty().getId(), entity));
            case SPACE_PROPERTY -> ObjectKeys.join("properties", "space",
                    id(entity.getSpaceProperty().getId(), entity));
            default -> ObjectKeys.join("entities",
                    ObjectKeys.segment(entity.getEntityCase().name().toLowerCase()),
                    id(entity.getEntityId(), entity));
        };
    }

    private static String space(String spaceId) {
        return spaceId == null || spaceId.isBlank() ? "" : ObjectKeys.segment(spaceId);
    }


    private static String id(String nativeId, ConfluenceEntity entity) {
        if (nativeId != null && !nativeId.isBlank()) {
            return ObjectKeys.segment(nativeId);
        }
        return ObjectKeys.segment(entity.getEntityId());
    }
}
