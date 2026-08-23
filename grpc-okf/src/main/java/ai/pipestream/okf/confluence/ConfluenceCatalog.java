package ai.pipestream.okf.confluence;

import ai.pipestream.confluence.v1.Attachment;
import ai.pipestream.confluence.v1.BlogPost;
import ai.pipestream.confluence.v1.Body;
import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.Comment;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ContentProperty;
import ai.pipestream.confluence.v1.Label;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.SpaceProperty;
import ai.pipestream.okf.CatalogEntry;
import ai.pipestream.okf.OkfActor;
import ai.pipestream.okf.OkfConcept;
import ai.pipestream.okf.OkfPaths;
import com.google.protobuf.Timestamp;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Maps Confluence protobuf entities onto OKF catalog entries (one concept
 * per entity, live {@code web_url} as {@code WARC-Target-URI}).
 */
public final class ConfluenceCatalog {

    /** Actor recorded on generated concepts. */
    public static final String ACTOR = OkfActor.process("grpc-confluence/okf-producer");

    private ConfluenceCatalog() {
    }

    /**
     * Maps a change. Deletes become {@code status: deprecated} concepts when
     * identity fields are present; empty identity yields empty.
     *
     * @param change a crawler change
     * @return the catalog entry, or empty when the change has no entity
     */
    public static java.util.Optional<CatalogEntry> from(ConfluenceChange change) {
        if (!change.hasEntity()) {
            return java.util.Optional.empty();
        }
        CatalogEntry entry = from(change.getEntity());
        if (change.getOperation() == ChangeOperation.CHANGE_OPERATION_DELETE) {
            OkfConcept deprecated = withStatus(entry.concept(), OkfConcept.Status.DEPRECATED);
            entry = new CatalogEntry(entry.path(), deprecated, entry.targetUri(),
                    entry.mediaType(), entry.resourceBody(), entry.kind());
        }
        return java.util.Optional.of(entry);
    }

    /**
     * Maps an entity.
     *
     * @param entity wrapped Confluence entity
     * @return the catalog entry
     */
    public static CatalogEntry from(ConfluenceEntity entity) {
        Instant at = instant(entity.getIngestedAt());
        return switch (entity.getEntityCase()) {
            case PAGE -> page(entity.getPage(), at);
            case BLOG_POST -> blog(entity.getBlogPost(), at);
            case COMMENT -> comment(entity.getComment(), at);
            case ATTACHMENT -> attachment(entity.getAttachment(), at);
            case SPACE -> space(entity.getSpace(), at);
            case LABEL -> label(entity.getLabel(), at);
            case CONTENT_PROPERTY -> contentProperty(entity.getContentProperty(), at);
            case SPACE_PROPERTY -> spaceProperty(entity.getSpaceProperty(), at);
            default -> fallback(entity, at);
        };
    }

    private static CatalogEntry page(Page page, Instant at) {
        String path = OkfPaths.join("pages", OkfPaths.segment(page.getId()) + ".md");
        String uri = uriOrUrn(page.getWebUrl(), "page", page.getId());
        String html = bodyHtml(page.getBody());
        OkfConcept.Builder b = base("Page", page.getTitle(), page.getTitle(), uri, at)
                .extra("confluence_id", page.getId())
                .extra("space_id", page.getSpaceId())
                .extra("parent_id", page.getParentId())
                .extra("author_id", page.getAuthorId())
                .tags(labelNames(page.getLabelsList()))
                .body(html.isBlank() ? "_No body fetched._" : html);
        return new CatalogEntry(path, b.build(), uri,
                html.isBlank() ? "text/plain" : "text/html",
                html.isBlank() ? page.getTitle().getBytes(StandardCharsets.UTF_8)
                        : html.getBytes(StandardCharsets.UTF_8),
                "page");
    }

    private static CatalogEntry blog(BlogPost post, Instant at) {
        String path = OkfPaths.join("blogs", OkfPaths.segment(post.getId()) + ".md");
        String uri = uriOrUrn(post.getWebUrl(), "blog", post.getId());
        String html = bodyHtml(post.getBody());
        OkfConcept concept = base("BlogPost", post.getTitle(), post.getTitle(), uri, at)
                .extra("confluence_id", post.getId())
                .extra("space_id", post.getSpaceId())
                .extra("author_id", post.getAuthorId())
                .tags(labelNames(post.getLabelsList()))
                .body(html.isBlank() ? "_No body fetched._" : html)
                .build();
        return new CatalogEntry(path, concept, uri,
                html.isBlank() ? "text/plain" : "text/html",
                html.isBlank() ? post.getTitle().getBytes(StandardCharsets.UTF_8)
                        : html.getBytes(StandardCharsets.UTF_8),
                "blog");
    }

    private static CatalogEntry comment(Comment comment, Instant at) {
        String path = OkfPaths.join("comments", OkfPaths.segment(comment.getId()) + ".md");
        String uri = uriOrUrn(comment.getWebUrl(), "comment", comment.getId());
        String html = bodyHtml(comment.getBody());
        String title = comment.getTitle().isBlank() ? "Comment " + comment.getId() : comment.getTitle();
        OkfConcept concept = base("Comment", title, title, uri, at)
                .extra("confluence_id", comment.getId())
                .extra("page_id", comment.getPageId())
                .extra("blog_post_id", comment.getBlogPostId())
                .body(html.isBlank() ? "_No body fetched._" : html)
                .build();
        return new CatalogEntry(path, concept, uri,
                html.isBlank() ? "text/plain" : "text/html",
                html.isBlank() ? title.getBytes(StandardCharsets.UTF_8)
                        : html.getBytes(StandardCharsets.UTF_8),
                "comment");
    }

    private static CatalogEntry attachment(Attachment attachment, Instant at) {
        String path = OkfPaths.join("attachments", OkfPaths.segment(attachment.getId()) + ".md");
        String uri = firstNonBlank(attachment.getDownloadUrl(), attachment.getWebUrl());
        uri = uriOrUrn(uri, "attachment", attachment.getId());
        byte[] payload = attachment.hasContent() && !attachment.getContent().isEmpty()
                ? attachment.getContent().toByteArray()
                : attachment.getTitle().getBytes(StandardCharsets.UTF_8);
        String media = attachment.getMediaType().isBlank()
                ? (attachment.hasContent() ? "application/octet-stream" : "text/plain")
                : attachment.getMediaType();
        StringBuilder body = new StringBuilder();
        body.append("File size: ").append(attachment.getFileSize()).append(" bytes\n\n");
        if (!attachment.getComment().isBlank()) {
            body.append(attachment.getComment()).append("\n");
        }
        OkfConcept.Builder b = base("Attachment", attachment.getTitle(),
                attachment.getMediaTypeDescription(), uri, at)
                .extra("confluence_id", attachment.getId())
                .extra("page_id", attachment.getPageId())
                .extra("blog_post_id", attachment.getBlogPostId())
                .extra("file_id", attachment.getFileId())
                .extra("media_type", attachment.getMediaType())
                .extra("file_size", Long.toString(attachment.getFileSize()))
                .tags(labelNames(attachment.getLabelsList()))
                .body(body.toString());
        if (attachment.hasContent() && !attachment.getContent().isEmpty()) {
            b.extra("sha256", ai.pipestream.okf.warc.WarcDigest.sha256Hex(payload));
        }
        return new CatalogEntry(path, b.build(), uri, media, payload, "attachment");
    }

    private static CatalogEntry space(Space space, Instant at) {
        String id = space.getKey().isBlank() ? space.getId() : space.getKey();
        String path = OkfPaths.join("spaces", OkfPaths.segment(id) + ".md");
        String uri = uriOrUrn(space.getWebUrl(), "space", id);
        String title = space.getName().isBlank() ? space.getKey() : space.getName();
        OkfConcept concept = base("Space", title, space.getKey(), uri, at)
                .extra("confluence_id", space.getId())
                .extra("space_key", space.getKey())
                .extra("homepage_id", space.getHomepageId())
                .tags(labelNames(space.getLabelsList()))
                .body("Space `" + space.getKey() + "`.")
                .build();
        return CatalogEntry.text(path, concept, uri, "text/plain", title, "space");
    }

    private static CatalogEntry label(Label label, Instant at) {
        String path = OkfPaths.join("labels", OkfPaths.segment(label.getId()) + ".md");
        String uri = uriOrUrn("", "label", label.getId());
        OkfConcept concept = base("Label", label.getName(), label.getPrefix(), uri, at)
                .extra("confluence_id", label.getId())
                .extra("prefix", label.getPrefix())
                .tag(label.getName())
                .body("Label `" + label.getPrefix() + ":" + label.getName() + "`.")
                .build();
        return CatalogEntry.text(path, concept, uri, "text/plain", label.getName(), "label");
    }

    private static CatalogEntry contentProperty(ContentProperty property, Instant at) {
        String path = OkfPaths.join("properties", "content",
                OkfPaths.segment(property.getId()) + ".md");
        String uri = uriOrUrn("", "content-property", property.getId());
        String title = property.getKey().name() + " " + property.getId();
        OkfConcept concept = base("ContentProperty", title, property.getCustomKey(), uri, at)
                .extra("confluence_id", property.getId())
                .extra("custom_key", property.getCustomKey())
                .body("Content property `" + property.getId() + "`.")
                .build();
        return CatalogEntry.text(path, concept, uri, "text/plain", title, "content-property");
    }

    private static CatalogEntry spaceProperty(SpaceProperty property, Instant at) {
        String path = OkfPaths.join("properties", "space",
                OkfPaths.segment(property.getId()) + ".md");
        String uri = uriOrUrn("", "space-property", property.getId());
        String title = "Space property " + property.getId();
        OkfConcept concept = base("SpaceProperty", title, "", uri, at)
                .extra("confluence_id", property.getId())
                .body("Space property `" + property.getId() + "`.")
                .build();
        return CatalogEntry.text(path, concept, uri, "text/plain", title, "space-property");
    }

    private static CatalogEntry fallback(ConfluenceEntity entity, Instant at) {
        String kind = entity.getEntityCase().name().toLowerCase(Locale.ROOT);
        String id = entity.getEntityId();
        String path = OkfPaths.join("entities", OkfPaths.segment(kind),
                OkfPaths.segment(id) + ".md");
        String uri = uriOrUrn("", kind, id);
        OkfConcept concept = base(titleCase(kind), id, kind, uri, at)
                .extra("confluence_id", id)
                .extra("entity_kind", kind)
                .body("Captured Confluence `" + kind + "` `" + id + "`.")
                .build();
        return CatalogEntry.text(path, concept, uri, "text/plain", id, kind);
    }

    private static OkfConcept.Builder base(String type, String title, String description,
            String resource, Instant at) {
        return OkfConcept.of(type)
                .title(title)
                .description(description)
                .resource(resource)
                .generated(new OkfConcept.Generated(ACTOR, at))
                .source(new OkfConcept.Source(null, resource, title, ACTOR, null, at, null));
    }

    private static OkfConcept withStatus(OkfConcept concept, OkfConcept.Status status) {
        OkfConcept.Builder b = OkfConcept.of(concept.type())
                .title(concept.title().orElse(null))
                .description(concept.description().orElse(null))
                .resource(concept.resource().orElse(null))
                .tags(concept.tags())
                .status(status)
                .body(concept.body());
        concept.generated().ifPresent(b::generated);
        concept.sources().forEach(b::source);
        concept.extra().forEach(b::extra);
        return b.build();
    }

    private static String bodyHtml(Body body) {
        if (body == null) {
            return "";
        }
        if (!body.getView().getValue().isBlank()) {
            return body.getView().getValue();
        }
        if (!body.getStorage().getValue().isBlank()) {
            return body.getStorage().getValue();
        }
        if (!body.getAtlasDocFormat().getValue().isBlank()) {
            return "```json\n" + body.getAtlasDocFormat().getValue() + "\n```\n";
        }
        return "";
    }

    private static List<String> labelNames(List<Label> labels) {
        List<String> names = new ArrayList<>();
        for (Label label : labels) {
            if (!label.getName().isBlank()) {
                names.add(label.getName());
            }
        }
        return names;
    }

    private static String uriOrUrn(String url, String kind, String id) {
        if (url != null && !url.isBlank()) {
            return url;
        }
        return "urn:okf:0.2:confluence:" + kind + ":" + (id == null || id.isBlank() ? "_" : id);
    }

    private static String firstNonBlank(String a, String b) {
        if (a != null && !a.isBlank()) {
            return a;
        }
        return b == null ? "" : b;
    }

    private static Instant instant(Timestamp timestamp) {
        if (timestamp == null || (timestamp.getSeconds() == 0 && timestamp.getNanos() == 0)) {
            return Instant.now();
        }
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private static String titleCase(String kind) {
        if (kind == null || kind.isBlank()) {
            return "Entity";
        }
        String[] parts = kind.split("_");
        StringBuilder out = new StringBuilder();
        for (String part : parts) {
            if (part.isEmpty()) {
                continue;
            }
            if (out.length() > 0) {
                out.append(' ');
            }
            out.append(Character.toUpperCase(part.charAt(0)));
            if (part.length() > 1) {
                out.append(part.substring(1).toLowerCase(Locale.ROOT));
            }
        }
        return out.toString();
    }
}
