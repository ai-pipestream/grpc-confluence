# Permissions the service user needs

The least privilege each rpc needs in Confluence Cloud, and how each row is
known. Update this file in the same pull request that adds an rpc or changes
what the service user must hold; `PermissionsDocTest` fails the build when an
rpc is not named here.

Every live run so far used one account that is an organization and site
admin, on the Free plan until 2026-11-22 and on a Premium trial after. So:

- **Passed as admin**: the rpc passed in a live run with that account. That a
  lesser account is enough is Atlassian's documented requirement (the "Needs"
  column) and is not proven.
- **Proven**: the limit itself was seen live (a refusal on one plan or
  credential, a pass on another).
- **Not run**: never called against a real site.

Last checked against the test site: 2026-11-27.

## The service user

An Atlassian account with Confluence product access, and an API token for it
(`CONFLUENCE_EMAIL`, `CONFLUENCE_API_TOKEN`; HTTP Basic). The token can do
whatever the account can do, so least privilege is set on the account: its
space roles (or classic space permissions), its global permissions and
whether it is an admin. Scoped API tokens, OAuth and Atlassian service
accounts were not tried.

Smallest account for each use (Atlassian's requirement, not proven with a
lesser account):

| Use | Account | Plan |
|---|---|---|
| Index content | Product access; view on each space | Any (passed on Free and Premium) |
| Index content with its access control | The same. It only sees restricted content it is named on; to see all of it, an organization or site admin with the admin key on | Admin key: Premium (passed there; Free and Standard not tried) |
| Write content | Add, edit and delete for each content type on the space | Any (passed on Free and Premium) |
| Write restrictions | Edit on the content, or space admin | Standard or higher (proven refused on Free, pass on Premium; Standard assumed) |
| Spaces, space roles, groups | Organization or site admin | Passed on Premium; other plans not tried |
| Change stream | The Forge app, installed by a site admin | Passed on Premium; other plans not tried |

## By rpc

| rpcs | Needs | How it is known |
|---|---|---|
| `ProbeConnection`, `ListSpaces`, `StreamSpaces`, `GetSpace`, `GetPage`, `GetBlogPost`, `ListPages`, `ListBlogPosts`, `GetAttachment`, `ListAttachments`, `DownloadAttachment`, `DownloadAttachmentThumbnail`, `ListComments`, `GetComment`, `ListCustomContent`, `GetCustomContent`, `GetWhiteboard`, `GetDatabase`, `GetFolder`, `GetSmartLink`, `ListChildren`, `ListDescendants`, `ListAncestors`, `ListDatabaseFields`, `ListDatabaseEntries`, `GetDatabaseEntry`, `ListDatabaseViews`, `ListLabels`, `ListVersions`, `GetVersion`, `ListLikes`, `GetLikeCount`, `ListOperations`, `ListTasks`, `GetTask`, `ListContentProperties`, `GetContentProperty`, `ListSpaceProperties`, `GetSpaceProperty`, `ResolveContentTypes`, `GetLongRunningTask`, `Sync` | Product access and view on the space; view on the content where it is restricted | Passed as admin. The read coverage run is local only and its report is not kept, so there is no per-rpc record for the reads |
| `GetRestrictionState`, `ListContentPermissions`, `ListSpacePermissions`, `ListSpacePermissionAssignments`, `ListSpaceRoles`, `GetSpaceRole`, `ListSpaceRoleAssignments`, `GetSpaceRoleMode`, `GetContentAccess`, `ListGroups`, `GetGroup`, `ListGroupMembers`, `ListUserGroups`, `CheckAccessByEmail` | Product access; view on the space or content read | Passed as admin (access control suite, recorded) |
| `BatchGetUsers` | Product access and permission to view user profiles | Passed as admin |
| `CheckContentPermission` | Product access for the caller's own access; Confluence administrator to check another user or a group | Passed as admin |
| `CreatePage`, `UpdatePage`, `UpdatePageTitle`, `DeletePage`, `CopyPage`, `CopyPageHierarchy`, `CreateBlogPost`, `UpdateBlogPost`, `DeleteBlogPost`, `CreateFooterComment`, `UpdateFooterComment`, `DeleteFooterComment`, `CreateInlineComment`, `UpdateInlineComment`, `DeleteInlineComment`, `CreateDatabase`, `DeleteDatabase`, `CreateFolder`, `DeleteFolder`, `CreateWhiteboard`, `DeleteWhiteboard`, `CreateSmartLink`, `DeleteSmartLink`, `UploadAttachment`, `DeleteAttachment`, `UpdateTask`, `CreateContentProperty`, `UpdateContentProperty`, `DeleteContentProperty`, `AddLabels`, `RemoveLabel`, `Mutate` | View on the space plus add, edit or delete for that content type; a purge also needs `manage/content` on the space | Passed as admin (write suite, recorded) |
| `ArchivePages` | Archive permission on the space | Passed as admin |
| `CreateCustomContent`, `UpdateCustomContent`, `DeleteCustomContent` | The same as other content, and a custom content type that an installed app declares (`grpc-confluence-forge` declares one) | Passed as admin with the Forge type |
| `AddContentPermissions`, `SetContentPermissions`, `DeleteContentPermissions`, `SetRestrictionState` | Edit on the content, or space admin. Standard plan or higher | Proven: refused on Free, pass on Premium |
| `CreateSpaceProperty`, `UpdateSpaceProperty`, `DeleteSpaceProperty`, `DeleteSpace` | Space admin | Passed as admin |
| `SetSpaceRoleAssignments` | Permission to manage roles in the space | Passed as admin (Premium) |
| `CreateSpace` | The "create spaces" global permission | Passed as admin |
| `CreateSpaceRole`, `UpdateSpaceRole`, `DeleteSpaceRole` | Organization or site admin | Passed as admin (Premium) |
| `GetAdminKey`, `EnableAdminKey`, `DisableAdminKey` | Organization or site admin | Passed as admin (Premium) |
| `CreateGroup`, `DeleteGroup`, `AddGroupMember`, `RemoveGroupMember` | Site admin | Passed as admin (change suite, recorded) |
| `ListSpacePermissionTransitionCombinations`, `GetSpacePermissionTransitionTask` | Confluence administrator | Passed as admin |
| `BulkAssignSpaceRoles`, `BulkRemoveSpaceAccess` | Confluence administrator | Not run: they change every space of the site |
| `InviteByEmail` | Product access | Not run: it sends real mail |
| `ListClassificationLevels`, `GetClassificationLevel` | Product access; view on the content | Passed as admin; the test site defines no levels |
| `SetContentClassificationLevel`, `ResetContentClassificationLevel` | Edit on the content. Levels exist only with Atlassian Guard Premium | Not run |
| `UpdateSpaceDefaultClassificationLevel`, `DeleteSpaceDefaultClassificationLevel` | `manage/space` on the space. Atlassian Guard Premium | Not run |
| `RedactPage`, `RedactBlogPost` | Edit on the content. Atlassian Guard Premium | Not run |
| `ListAppProperties`, `GetAppProperty`, `SetAppProperty`, `DeleteAppProperty` | A Forge app: Confluence serves them to apps only. They go through the `app-api` trigger of `grpc-confluence-forge` (`CONFLUENCE_FORGE_APP_URL`, `CONFLUENCE_FORGE_APP_SECRET`) | Proven: passed through the app |
| `ListDataPolicySpaces`, `GetDataPolicyMetadata` | An app credential; no route for an API token | Proven: `PERMISSION_DENIED` with the token. Not served today |
| `WatchChanges` | The Forge app's event queue (`CONFLUENCE_FORGE_QUEUE_URL`, `CONFLUENCE_FORGE_QUEUE_SECRET`), and for the service user what the reads above need | Passed as admin, on the app's development and production environments |

## The Forge app

`grpc-confluence-forge` is installed on the site once by a site admin. It
holds these scopes (`manifest.yml`): `storage:app`,
`read:confluence-content.summary`, `read:confluence-content.all`,
`read:confluence-space.summary`, `read:confluence-user`,
`read:confluence-groups`, `write:confluence-content`,
`write:confluence-space`, `read:app-data:confluence`,
`write:app-data:confluence`, `read:custom-content:confluence`,
`write:custom-content:confluence`, `delete:custom-content:confluence`. A
scope change needs a new major version of the app and an upgrade of the
install. Whether each scope is needed was not tried one by one.

## Open

- Nothing is proven with a non-admin account. A second account on the test
  site with only a viewer role, then only a collaborator role, would prove
  the first three rows of the table above.
- The read coverage run is not in CI, so its per-rpc results are not on
  record.
- The Premium trial of the test site ends about 2026-12-22. After that the
  restriction writes are refused again in live runs.
- Classification and redaction need Atlassian Guard Premium and stay
  unproven by the owner's choice.
