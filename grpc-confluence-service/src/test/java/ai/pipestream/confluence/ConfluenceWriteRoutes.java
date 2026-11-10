package ai.pipestream.confluence;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** The REST routes each unary write rpc is served by, as the upstream documents name them. */
final class ConfluenceWriteRoutes {

    /** Write rpcs served by REST API v2: the operations of the vendored OpenAPI document. */
    static final Map<String, List<String>> V2 = Map.ofEntries(
            Map.entry("CreatePage", List.of(
                    "POST /pages")),
            Map.entry("UpdatePage", List.of(
                    "PUT /pages/{id}")),
            Map.entry("DeletePage", List.of(
                    "DELETE /pages/{id}")),
            Map.entry("UpdatePageTitle", List.of(
                    "PUT /pages/{id}/title")),
            Map.entry("CreateBlogPost", List.of(
                    "POST /blogposts")),
            Map.entry("UpdateBlogPost", List.of(
                    "PUT /blogposts/{id}")),
            Map.entry("DeleteBlogPost", List.of(
                    "DELETE /blogposts/{id}")),
            Map.entry("CreateFooterComment", List.of(
                    "POST /footer-comments")),
            Map.entry("UpdateFooterComment", List.of(
                    "PUT /footer-comments/{comment-id}")),
            Map.entry("DeleteFooterComment", List.of(
                    "DELETE /footer-comments/{comment-id}")),
            Map.entry("CreateInlineComment", List.of(
                    "POST /inline-comments")),
            Map.entry("UpdateInlineComment", List.of(
                    "PUT /inline-comments/{comment-id}")),
            Map.entry("DeleteInlineComment", List.of(
                    "DELETE /inline-comments/{comment-id}")),
            Map.entry("CreateCustomContent", List.of(
                    "POST /custom-content")),
            Map.entry("UpdateCustomContent", List.of(
                    "PUT /custom-content/{id}")),
            Map.entry("DeleteCustomContent", List.of(
                    "DELETE /custom-content/{id}")),
            Map.entry("CreateDatabase", List.of(
                    "POST /databases")),
            Map.entry("DeleteDatabase", List.of(
                    "DELETE /databases/{id}")),
            Map.entry("CreateFolder", List.of(
                    "POST /folders")),
            Map.entry("DeleteFolder", List.of(
                    "DELETE /folders/{id}")),
            Map.entry("CreateWhiteboard", List.of(
                    "POST /whiteboards")),
            Map.entry("DeleteWhiteboard", List.of(
                    "DELETE /whiteboards/{id}")),
            Map.entry("CreateSmartLink", List.of(
                    "POST /embeds")),
            Map.entry("DeleteSmartLink", List.of(
                    "DELETE /embeds/{id}")),
            Map.entry("DeleteAttachment", List.of(
                    "DELETE /attachments/{id}")),
            Map.entry("CreateSpace", List.of(
                    "POST /spaces")),
            Map.entry("UpdateTask", List.of(
                    "PUT /tasks/{id}")),
            Map.entry("CreateContentProperty", List.of(
                    "POST /pages/{page-id}/properties",
                    "POST /blogposts/{blogpost-id}/properties",
                    "POST /attachments/{attachment-id}/properties",
                    "POST /custom-content/{custom-content-id}/properties",
                    "POST /comments/{comment-id}/properties",
                    "POST /whiteboards/{id}/properties",
                    "POST /databases/{id}/properties",
                    "POST /folders/{id}/properties",
                    "POST /embeds/{id}/properties")),
            Map.entry("UpdateContentProperty", List.of(
                    "PUT /pages/{page-id}/properties/{property-id}",
                    "PUT /blogposts/{blogpost-id}/properties/{property-id}",
                    "PUT /attachments/{attachment-id}/properties/{property-id}",
                    "PUT /custom-content/{custom-content-id}/properties/{property-id}",
                    "PUT /comments/{comment-id}/properties/{property-id}",
                    "PUT /whiteboards/{whiteboard-id}/properties/{property-id}",
                    "PUT /databases/{database-id}/properties/{property-id}",
                    "PUT /folders/{folder-id}/properties/{property-id}",
                    "PUT /embeds/{embed-id}/properties/{property-id}")),
            Map.entry("DeleteContentProperty", List.of(
                    "DELETE /pages/{page-id}/properties/{property-id}",
                    "DELETE /blogposts/{blogpost-id}/properties/{property-id}",
                    "DELETE /attachments/{attachment-id}/properties/{property-id}",
                    "DELETE /custom-content/{custom-content-id}/properties/{property-id}",
                    "DELETE /comments/{comment-id}/properties/{property-id}",
                    "DELETE /whiteboards/{whiteboard-id}/properties/{property-id}",
                    "DELETE /databases/{database-id}/properties/{property-id}",
                    "DELETE /folders/{folder-id}/properties/{property-id}",
                    "DELETE /embeds/{embed-id}/properties/{property-id}")),
            Map.entry("CreateSpaceProperty", List.of(
                    "POST /spaces/{space-id}/properties")),
            Map.entry("UpdateSpaceProperty", List.of(
                    "PUT /spaces/{space-id}/properties/{property-id}")),
            Map.entry("DeleteSpaceProperty", List.of(
                    "DELETE /spaces/{space-id}/properties/{property-id}")),
            Map.entry("SetAppProperty", List.of(
                    "PUT /app/properties/{propertyKey}")),
            Map.entry("DeleteAppProperty", List.of(
                    "DELETE /app/properties/{propertyKey}")),
            Map.entry("AddContentPermissions", List.of(
                    "POST /pages/{id}/permissions",
                    "POST /blogposts/{id}/permissions",
                    "POST /databases/{id}/permissions",
                    "POST /whiteboards/{id}/permissions",
                    "POST /folders/{id}/permissions")),
            Map.entry("SetContentPermissions", List.of(
                    "PUT /pages/{id}/permissions",
                    "PUT /blogposts/{id}/permissions",
                    "PUT /databases/{id}/permissions",
                    "PUT /whiteboards/{id}/permissions",
                    "PUT /folders/{id}/permissions")),
            Map.entry("DeleteContentPermissions", List.of(
                    "DELETE /pages/{id}/permissions",
                    "DELETE /blogposts/{id}/permissions",
                    "DELETE /databases/{id}/permissions",
                    "DELETE /whiteboards/{id}/permissions",
                    "DELETE /folders/{id}/permissions")),
            Map.entry("SetRestrictionState", List.of(
                    "PUT /pages/{id}/restriction-state",
                    "PUT /blogposts/{id}/restriction-state",
                    "PUT /databases/{id}/restriction-state",
                    "PUT /whiteboards/{id}/restriction-state",
                    "PUT /folders/{id}/restriction-state")),
            Map.entry("SetContentClassificationLevel", List.of(
                    "PUT /pages/{id}/classification-level",
                    "PUT /blogposts/{id}/classification-level",
                    "PUT /databases/{id}/classification-level",
                    "PUT /whiteboards/{id}/classification-level")),
            Map.entry("ResetContentClassificationLevel", List.of(
                    "POST /pages/{id}/classification-level/reset",
                    "POST /blogposts/{id}/classification-level/reset",
                    "POST /databases/{id}/classification-level/reset",
                    "POST /whiteboards/{id}/classification-level/reset")),
            Map.entry("UpdateSpaceDefaultClassificationLevel", List.of(
                    "PUT /spaces/{id}/classification-level/default")),
            Map.entry("DeleteSpaceDefaultClassificationLevel", List.of(
                    "DELETE /spaces/{id}/classification-level/default")),
            Map.entry("CreateSpaceRole", List.of(
                    "POST /space-roles")),
            Map.entry("UpdateSpaceRole", List.of(
                    "PUT /space-roles/{id}")),
            Map.entry("DeleteSpaceRole", List.of(
                    "DELETE /space-roles/{id}")),
            Map.entry("SetSpaceRoleAssignments", List.of(
                    "POST /spaces/{id}/role-assignments")),
            Map.entry("BulkAssignSpaceRoles", List.of(
                    "POST /space-permissions/transition/role-assignments")),
            Map.entry("BulkRemoveSpaceAccess", List.of(
                    "POST /space-permissions/transition/access-removals")),
            Map.entry("InviteByEmail", List.of(
                    "POST /user/access/invite-by-email")),
            Map.entry("RedactPage", List.of(
                    "POST /pages/{id}/redact")),
            Map.entry("RedactBlogPost", List.of(
                    "POST /blogposts/{id}/redact")),
            Map.entry("EnableAdminKey", List.of(
                    "POST /admin-key")),
            Map.entry("DisableAdminKey", List.of(
                    "DELETE /admin-key")));

    /** Write rpcs served by REST API v1, whose document is not vendored. */
    static final Map<String, List<String>> V1 = Map.ofEntries(
            Map.entry("AddLabels", List.of(
                    "POST /rest/api/content/{id}/label")),
            Map.entry("RemoveLabel", List.of(
                    "DELETE /rest/api/content/{id}/label")));

    /** The streaming write rpcs. */
    static final Set<String> STREAMING = Set.of("UploadAttachment", "Mutate");

    private ConfluenceWriteRoutes() {
    }

    /** Every write rpc. */
    static Set<String> rpcs() {
        Set<String> rpcs = new TreeSet<>(V2.keySet());
        rpcs.addAll(V1.keySet());
        rpcs.addAll(STREAMING);
        return rpcs;
    }
}
