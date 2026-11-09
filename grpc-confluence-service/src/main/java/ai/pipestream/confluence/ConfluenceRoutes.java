package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.BlogPostContentStatus;
import ai.pipestream.confluence.v1.BodyFormat;
import ai.pipestream.confluence.v1.ContentRef;
import ai.pipestream.confluence.v1.ContentStatus;
import ai.pipestream.confluence.v1.ContentType;
import ai.pipestream.confluence.v1.SortField;
import ai.pipestream.confluence.v1.SortOrder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The request-vocabulary half of the REST v2 mapping: which
 * {@code {content-type}} path segment a {@link ContentRef} names, and the
 * exact v2 wire values of sort orders, body representations and statuses.
 * Every translation checks the value against what the chosen route accepts
 * (per the vendored OpenAPI document) and answers {@code INVALID_ARGUMENT}
 * otherwise, so a filter is never silently dropped.
 */
final class ConfluenceRoutes {

    /** The list routes' {@code PrimaryBodyRepresentation}. */
    static final Set<BodyFormat> PRIMARY_BODY = EnumSet.of(
            BodyFormat.BODY_FORMAT_STORAGE_XHTML, BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT);
    /** Single page / blog post / comment reads ({@code PrimaryBodyRepresentationSingle}). */
    static final Set<BodyFormat> SINGLE_BODY = EnumSet.of(
            BodyFormat.BODY_FORMAT_STORAGE_XHTML, BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT,
            BodyFormat.BODY_FORMAT_RENDERED_XHTML, BodyFormat.BODY_FORMAT_EXPORT_XHTML,
            BodyFormat.BODY_FORMAT_ANONYMOUS_EXPORT_XHTML, BodyFormat.BODY_FORMAT_STYLED_XHTML,
            BodyFormat.BODY_FORMAT_EDITOR);
    /** Custom content lists and version lists ({@code CustomContentBodyRepresentation}). */
    static final Set<BodyFormat> CUSTOM_BODY = EnumSet.of(BodyFormat.BODY_FORMAT_RAW,
            BodyFormat.BODY_FORMAT_STORAGE_XHTML, BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT);
    /** Single custom content reads ({@code CustomContentBodyRepresentationSingle}). */
    static final Set<BodyFormat> CUSTOM_SINGLE_BODY = EnumSet.of(BodyFormat.BODY_FORMAT_RAW,
            BodyFormat.BODY_FORMAT_STORAGE_XHTML, BodyFormat.BODY_FORMAT_ATLAS_DOC_FORMAT,
            BodyFormat.BODY_FORMAT_RENDERED_XHTML, BodyFormat.BODY_FORMAT_EXPORT_XHTML,
            BodyFormat.BODY_FORMAT_ANONYMOUS_EXPORT_XHTML);

    /** {@code SpaceSortOrder}. */
    static final Set<SortField> SPACE_SORT = EnumSet.of(SortField.SORT_FIELD_ID,
            SortField.SORT_FIELD_KEY, SortField.SORT_FIELD_NAME);
    /** {@code PageSortOrder}. */
    static final Set<SortField> PAGE_SORT = EnumSet.of(SortField.SORT_FIELD_ID,
            SortField.SORT_FIELD_CREATED_DATE, SortField.SORT_FIELD_MODIFIED_DATE,
            SortField.SORT_FIELD_TITLE);
    /** {@code BlogPostSortOrder}. */
    static final Set<SortField> BLOG_POST_SORT = EnumSet.of(SortField.SORT_FIELD_ID,
            SortField.SORT_FIELD_CREATED_DATE, SortField.SORT_FIELD_MODIFIED_DATE);
    /** {@code AttachmentSortOrder} and {@code CommentSortOrder}. */
    static final Set<SortField> DATED_SORT = EnumSet.of(SortField.SORT_FIELD_CREATED_DATE,
            SortField.SORT_FIELD_MODIFIED_DATE);
    /** {@code CustomContentSortOrder}. */
    static final Set<SortField> CUSTOM_CONTENT_SORT = PAGE_SORT;
    /** {@code ContentSortOrder} (direct children of hierarchical content). */
    static final Set<SortField> TREE_SORT = EnumSet.of(SortField.SORT_FIELD_ID,
            SortField.SORT_FIELD_CREATED_DATE, SortField.SORT_FIELD_MODIFIED_DATE,
            SortField.SORT_FIELD_CHILD_POSITION, SortField.SORT_FIELD_TITLE);
    /** {@code ChildCustomContentSortOrder}. */
    static final Set<SortField> CHILD_CUSTOM_CONTENT_SORT = BLOG_POST_SORT;
    /** {@code LabelSortOrder}. */
    static final Set<SortField> LABEL_SORT = EnumSet.of(SortField.SORT_FIELD_ID,
            SortField.SORT_FIELD_CREATED_DATE, SortField.SORT_FIELD_NAME);
    /** {@code VersionSortOrder}. */
    static final Set<SortField> VERSION_SORT = EnumSet.of(SortField.SORT_FIELD_MODIFIED_DATE);
    /** {@code ContentPropertySortOrder}. */
    static final Set<SortField> PROPERTY_SORT = EnumSet.of(SortField.SORT_FIELD_KEY);

    /** Content kinds with a restriction-state / permissions / classification route. */
    static final Set<ContentType> PERMISSION_TARGETS = EnumSet.of(ContentType.CONTENT_TYPE_PAGE,
            ContentType.CONTENT_TYPE_BLOG_POST, ContentType.CONTENT_TYPE_DATABASE,
            ContentType.CONTENT_TYPE_WHITEBOARD, ContentType.CONTENT_TYPE_FOLDER);
    /** Content kinds of the content tree (direct-children, descendants, ancestors). */
    static final Set<ContentType> TREE_NODES = EnumSet.of(ContentType.CONTENT_TYPE_PAGE,
            ContentType.CONTENT_TYPE_WHITEBOARD, ContentType.CONTENT_TYPE_DATABASE,
            ContentType.CONTENT_TYPE_FOLDER, ContentType.CONTENT_TYPE_SMART_LINK);

    private ConfluenceRoutes() {
    }

    /** The v2 {@code {content-type}} path segment of a built-in content kind. */
    static String segment(ContentType type) {
        return switch (type) {
            case CONTENT_TYPE_PAGE -> "pages";
            case CONTENT_TYPE_BLOG_POST -> "blogposts";
            case CONTENT_TYPE_ATTACHMENT -> "attachments";
            case CONTENT_TYPE_FOOTER_COMMENT -> "footer-comments";
            case CONTENT_TYPE_INLINE_COMMENT -> "inline-comments";
            case CONTENT_TYPE_CUSTOM_CONTENT -> "custom-content";
            case CONTENT_TYPE_WHITEBOARD -> "whiteboards";
            case CONTENT_TYPE_DATABASE -> "databases";
            case CONTENT_TYPE_FOLDER -> "folders";
            case CONTENT_TYPE_SMART_LINK -> "embeds";
            case CONTENT_TYPE_UNSPECIFIED, UNRECOGNIZED -> throw invalid("content type is required");
        };
    }

    /**
     * {@code /api/v2/{content-type}/{id}} for a reference the route accepts.
     *
     * @param ref the content reference
     * @param allowed the content kinds the route serves
     * @param route a human name of the route, for the error message
     */
    static String contentPath(ContentRef ref, Set<ContentType> allowed, String route) {
        if (ref == null || ref.getType() == ContentType.CONTENT_TYPE_UNSPECIFIED) {
            throw invalid(route + ": content type is required");
        }
        if (!allowed.contains(ref.getType())) {
            throw invalid(route + " is not served for " + ref.getType() + "; accepted: " + allowed);
        }
        return "/api/v2/" + segment(ref.getType()) + "/" + requireId(ref.getId(), "content id");
    }

    /** The id, or INVALID_ARGUMENT naming the missing field. */
    static String requireId(String id, String field) {
        if (id == null || id.isBlank()) {
            throw invalid(field + " is required");
        }
        return id;
    }

    /**
     * The {@code [-]field} wire value of a sort order, checked against the
     * route's sort enum; {@code null} when no sort is requested.
     */
    static String sort(SortOrder sort, Set<SortField> allowed, String route) {
        if (sort == null || sort.getField() == SortField.SORT_FIELD_UNSPECIFIED) {
            return null;
        }
        if (!allowed.contains(sort.getField())) {
            throw invalid(sort.getField() + " is not sortable on " + route + "; sortable: " + allowed);
        }
        String field = switch (sort.getField()) {
            case SORT_FIELD_ID -> "id";
            case SORT_FIELD_CREATED_DATE -> "created-date";
            case SORT_FIELD_MODIFIED_DATE -> "modified-date";
            case SORT_FIELD_TITLE -> "title";
            case SORT_FIELD_CHILD_POSITION -> "child-position";
            case SORT_FIELD_KEY -> "key";
            case SORT_FIELD_NAME -> "name";
            case SORT_FIELD_UNSPECIFIED, UNRECOGNIZED -> throw invalid("unknown sort field");
        };
        return (sort.getDescending() ? "-" : "") + field;
    }

    /** The v2 {@code body-format} value, checked against what the route accepts. */
    static String bodyFormat(BodyFormat format, Set<BodyFormat> allowed, String route) {
        if (!allowed.contains(format)) {
            throw invalid(format + " is not a body-format of " + route + "; accepted: " + allowed);
        }
        return representation(format);
    }

    /** The API representation string of a body format. */
    static String representation(BodyFormat format) {
        return switch (format) {
            case BODY_FORMAT_STORAGE_XHTML -> "storage";
            case BODY_FORMAT_ATLAS_DOC_FORMAT -> "atlas_doc_format";
            case BODY_FORMAT_RENDERED_XHTML -> "view";
            case BODY_FORMAT_EXPORT_XHTML -> "export_view";
            case BODY_FORMAT_ANONYMOUS_EXPORT_XHTML -> "anonymous_export_view";
            case BODY_FORMAT_STYLED_XHTML -> "styled_view";
            case BODY_FORMAT_EDITOR -> "editor";
            case BODY_FORMAT_WIKI -> "wiki";
            case BODY_FORMAT_RAW -> "raw";
            case BODY_FORMAT_PLAIN_TEXT -> "plain";
            case BODY_FORMAT_UNSPECIFIED, UNRECOGNIZED -> throw invalid("unsupported body format: " + format);
        };
    }

    /** Comma-joined wire values of content statuses, each checked against the route's enum. */
    static String contentStatuses(List<ContentStatus> statuses, Set<String> allowed, String route) {
        List<String> wire = new ArrayList<>();
        for (ContentStatus status : statuses) {
            wire.add(status(status.getValueDescriptor().getName(), "CONTENT_STATUS_", allowed, route));
        }
        return String.join(",", wire);
    }

    /** Comma-joined wire values of blog post statuses, each checked against the route's enum. */
    static String blogPostStatuses(List<BlogPostContentStatus> statuses, Set<String> allowed,
            String route) {
        List<String> wire = new ArrayList<>();
        for (BlogPostContentStatus status : statuses) {
            wire.add(status(status.getValueDescriptor().getName(), "BLOG_POST_CONTENT_STATUS_",
                    allowed, route));
        }
        return String.join(",", wire);
    }

    /** One enum value's wire form ({@code PREFIX_FOO_BAR} to {@code foo_bar}), checked. */
    static String status(String enumName, String prefix, Set<String> allowed, String route) {
        String wire = enumName.startsWith(prefix)
                ? enumName.substring(prefix.length()).toLowerCase(Locale.ROOT) : "";
        if (!allowed.contains(wire)) {
            throw invalid(enumName + " is not a status of " + route + "; accepted: " + allowed);
        }
        return wire;
    }

    /** The given wire values as an immutable set. */
    static Set<String> values(String... wire) {
        return Set.of(wire);
    }

    /** An INVALID_ARGUMENT status exception. */
    static StatusRuntimeException invalid(String description) {
        return Status.INVALID_ARGUMENT.withDescription(description).asRuntimeException();
    }

    /**
     * A sorted query-parameter map with typed putters that skip empty
     * values, so the REST call carries exactly what the request set.
     */
    static final class Query {
        private final Map<String, String> params = new TreeMap<>();

        Query put(String name, String value) {
            if (value != null && !value.isEmpty()) {
                params.put(name, value);
            }
            return this;
        }

        Query list(String name, List<String> values) {
            if (values != null && !values.isEmpty()) {
                params.put(name, String.join(",", values));
            }
            return this;
        }

        Query flag(String name, boolean set) {
            if (set) {
                params.put(name, "true");
            }
            return this;
        }

        boolean has(String name) {
            return params.containsKey(name);
        }

        Map<String, String> map() {
            return params;
        }
    }
}
