# Confluence Cloud REST API v2 vs `ai.pipestream.confluence.v1` — gap analysis and proto proposal

Date: 2026-11-08. Scope: `grpc-confluence-api/src/main/proto/ai/pipestream/confluence/v1/` (domain protos + `confluence_service.proto`). Protos are edited in place to the proposal below; no Java, docs, or anything outside that directory was touched.

## 0. Sources and what was actually verified

| Source | Status | What it gave |
|---|---|---|
| Confluence v2 OpenAPI document (`openapi-v2.v3.json`), pinned mirror in `dichovsky/atlassian-api-client` `spec/confluence-v2.json`, pinned **2026-11-08**, upstream version `2.0.0`, 165 paths / 252 operations / 1 deprecated, SHA-256 `40535e2a…cfa31` | **Verified** (downloaded, parsed with a script; saved at `…/scratchpad/upstream/confluence-v2-openapi.json`) | Every endpoint, parameter, schema, enum, and description cited below. This is the single source of truth for this analysis. |
| Live CDN copy `https://dac-static.atlassian.com/cloud/confluence/openapi-v2.v3.json` | **Partially verified** (WebFetch returned the first ~100 KB: `info.version 2.0.0`, first 21 paths starting with the five `restriction-state` and five `permissions` routes, then Admin Key / Attachment) | Confirms the mirror is the current document (same ordering, same new Content Permissions group). |
| `developer.atlassian.com/cloud/confluence/changelog` | **Verified** (page only lists 25 Aug – 16 Oct 2026) | 14 Oct 2026: new v2 Content Permissions APIs (`GET/PUT /{content-type}/{id}/restriction-state`, `GET/POST/PUT/DELETE /{content-type}/{id}/permissions`, for pages, blog posts, databases, whiteboards, folders). 28 Sep 2026: approval-required publishing → `PUT /pages/{id}` and `/pages/{id}/title` return 409 in approval spaces. 15 Oct 2026: v1 restrictions semantics fix. No v2 deprecations announced. |
| `developer.atlassian.com/cloud/confluence/rest/v2/api-group-*` pages | **Not readable** (client-side rendered; WebFetch only saw the nav index) | Only the group list: Admin Key, Attachment, App Properties, Ancestors, Blog Post, Children, Classification Level, Comment, Content, **Content Permissions**, Content Properties, Custom Content, Database, Data Policies, Descendants, Folder, Label, Like, Operation, Page, Redactions, Smart Link, Space, Space Permissions, Space Permission Transition, Space Properties, Space Roles, Task, User, Version, Whiteboard — identical to the spec's tag list. |
| Older mirror (`modbender/skill-library-mcp`, 146 paths, no permissions, no database entries) | Verified, used only as a diff baseline | Shows what is *new* in the current spec: Content Permissions (10 paths), Database `fields/entries/views` (4 paths), `space-role-mode`, label→content routes, etc. |

Nothing below is inferred from memory; where the spec is silent (e.g. the shape of `include-collaborators`), it is called out as unknown rather than modeled.

## 1. Read scope (and why writes are out)

`ConfluenceService` is the read proxy for an indexer. Every v2 **read** route family gets one rpc. Writes are out: create/update/delete of content, comments, properties, classification, redactions (`POST …/redact`), admin key enable/disable, permission mutation (`POST/PUT/DELETE …/permissions`, `PUT …/restriction-state`), space-permission transition tasks, role/assignment mutation. Permission **reads** are in scope because search security trimming needs the direct restriction state, the direct user/group grants, the space permission assignments and the space role assignments to compute effective visibility.

Also kept out (see §5): Forge-only `app/properties`, `check-access-by-email`, `invite-by-email`, thumbnail download, like counts (derivable), space permission transition reads (admin migration tooling), admin-key read (session state).

## 2. Endpoint-group coverage: before → after

Legend: **present** = existed; **added** = new rpc in this proposal; **drifted** = existed but request/entity lagged the spec.

| v2 group | Read routes in spec | Before (9 rpcs) | After (50 rpcs) | Notes |
|---|---|---|---|---|
| Space | `GET /spaces`, `GET /spaces/{id}` | `ListSpaces` (keys, limit only) | `ListSpaces` **drifted→extended** (ids, type, status, labels, favorited-by, sort, description-format, include-icon); `GetSpace` **added** (id or key; includes icon/operations/properties/permissions/role-assignments/labels) | `ListSpaces` stays unary (wire compat); see open question 1. |
| Page | `GET /pages`, `GET /pages/{id}`, `GET /spaces/{id}/pages`, `GET /labels/{id}/pages` | `GetPage`, `ListPages` (space_id, body_format) | `GetPage` **extended** (version, get-draft, statuses, includes); `ListPages` **extended** (ids, statuses, title, sort, subtype live/page, label_id, space_ids, root_only=`depth=root`) | `include-collaborators`, `include-webresources` not modeled (shape not in spec). |
| Blog Post | `GET /blogposts`, `GET /blogposts/{id}`, `GET /spaces/{id}/blogposts`, `GET /labels/{id}/blogposts` | `GetBlogPost`, `ListBlogPosts` | both **extended** symmetrically with pages (no subtype) | |
| Attachment | `GET /attachments`, `GET /attachments/{id}`, `GET /{pages,blogposts,custom-content}/{id}/attachments`, `GET /labels/{id}/attachments`, `GET /attachments/{id}/thumbnail/download` | `GetAttachment` (25 MiB cap), `ListAttachments` (page/blogpost) | `GetAttachment` **fixed** (no fixed cap; optional `max_content_bytes`, version, includes); `ListAttachments` **extended** (custom_content_id, label_id, workspace-wide, statuses, media_type, filename, sort); `DownloadAttachment` **added** (server-stream: metadata then chunks) | Thumbnail download left out (§5). |
| Comment | `GET /footer-comments`, `GET /inline-comments`, `GET /{footer,inline}-comments/{id}`, `GET /{footer,inline}-comments/{id}/children`, `GET /{pages,blogposts}/{id}/{footer,inline}-comments`, `GET /{attachments,custom-content}/{id}/footer-comments` | none (crawled by `Sync` only) | `ListComments` **added** (container / parent_comment_id / workspace; kind footer|inline|both; status, resolution-status, sort); `GetComment` **added** | Biggest functional gap: comments are indexed content with no read rpc. |
| Custom Content | `GET /custom-content?type=`, `GET /custom-content/{id}`, `GET /{spaces,pages,blogposts}/{id}/custom-content` | none | `ListCustomContent`, `GetCustomContent` **added** | `type` is required on every v2 list route. |
| Whiteboard / Database / Folder / Smart Link | `GET /whiteboards/{id}`, `GET /databases/{id}`, `GET /folders/{id}`, `GET /embeds/{id}` (each with `include-operations/-properties/-direct-children/-collaborators`) | none (entities modeled, never served) | `GetWhiteboard`, `GetDatabase`, `GetFolder`, `GetSmartLink` **added**; entities **extended** with `properties`, `operations`, `direct_children` | No list routes exist upstream; discovery is via children/descendants. |
| Children / Descendants / Ancestors | `GET /{pages,whiteboards,databases,folders,embeds}/{id}/direct-children`, `GET /custom-content/{id}/children`, `GET /…/descendants?depth`, `GET /…/ancestors`; `GET /pages/{id}/children` is the spec's one **deprecated** op | none (`ContentTreeEntry`, `Ancestor` modeled) | `ListChildren`, `ListDescendants`, `ListAncestors` **added** | Server maps to `direct-children`, never the deprecated `children`. |
| Database (data) — **new upstream, experimental** | `GET /databases/{id}/fields`, `/entries`, `/entries/{entryId}`, `/views` | none | `ListDatabaseFields`, `ListDatabaseEntries` (typed filters + sort), `GetDatabaseEntry`, `ListDatabaseViews` **added**; new `database.proto` | Rows are indexable content; typed per field kind, no Struct. |
| Label | `GET /labels`, `GET /spaces/{id}/labels`, `GET /spaces/{id}/content/labels`, `GET /{content}/{id}/labels` | none (inlined via `include-labels` only) | `ListLabels` **added** (scope oneof: workspace / space / space content / content) | Content-by-label routes surface as `label_id` filters on `ListPages/ListBlogPosts/ListAttachments`. |
| Like | `GET /{pages,blogposts,footer-comments,inline-comments}/{id}/likes/users`, `…/likes/count` | none (inlined) | `ListLikes` **added** | Count route left out (count the stream). |
| Operation | `GET /{every content type, spaces}/{id}/operations` | none (inlined) | covered by `ContentIncludes.operations` / `SpaceIncludes.operations` | No standalone rpc; see §5. |
| Version | `GET /{pages,blogposts,attachments,footer-comments,inline-comments,custom-content}/{id}/versions`, `…/versions/{n}` | none (inlined history only) | `ListVersions` (Version + entity snapshot), `GetVersion` (DetailedVersion) **added** | `Version.entity` already modeled the snapshot; `DetailedVersion` gets a `ConfluenceEntity` arm. |
| Task | `GET /tasks` (15 filters), `GET /tasks/{id}` | none | `ListTasks`, `GetTask` **added** | Epoch-millis filters surface as `Timestamp`s. |
| User | `POST /users-bulk` (read), `POST /user/access/check-access-by-email`, `POST /user/access/invite-by-email` | none | `BatchGetUsers` **added** | The two access routes are out (§5). |
| Classification Level | `GET /classification-levels`, `GET /{pages,blogposts,databases,whiteboards}/{id}/classification-level`, `GET /spaces/{id}/classification-level/default` | none | `ListClassificationLevels`, `GetClassificationLevel` (content or space default) **added** | Governance signal for trimming/compliance. |
| Data Policies | `GET /data-policies/spaces`, `GET /data-policies/metadata` (apps only) | none | `ListDataPolicySpaces`, `GetDataPolicyMetadata` **added** | Metadata route is app-credential only upstream; rpc comment says so. |
| Content Properties / Space Properties | `GET /{9 content types}/{id}/properties[/{property-id}]`, `GET /spaces/{id}/properties[/{property-id}]` | none (inlined; `Sync` crawls page/blog properties) | `ListContentProperties`, `GetContentProperty`, `ListSpaceProperties`, `GetSpaceProperty` **added** | |
| App Properties | `GET /app/properties[/{key}]` | none | **left out** | Forge `asApp()` only; proxy uses user credentials. `AppProperty` message stays. |
| **Content Permissions — new upstream (Oct 2026)** | `GET /{pages,blogposts,databases,whiteboards,folders}/{id}/restriction-state`, `…/permissions` (filters operation, principal-type, principal-id; max 250/page) | none | `GetRestrictionState`, `ListContentPermissions` **added**; new `RestrictionState`, `ContentPermission`, enums | Security trimming. Writes out. |
| Space Permissions | `GET /space-permissions`, `GET /spaces/{id}/permissions` | none (`SpacePermission*` modeled) | `ListSpacePermissions`, `ListSpacePermissionAssignments` **added** | |
| Space Roles | `GET /space-roles`, `GET /space-roles/{id}`, `GET /spaces/{id}/role-assignments`, `GET /space-role-mode` | none (`SpaceRole*` modeled) | `ListSpaceRoles`, `GetSpaceRole`, `ListSpaceRoleAssignments`, `GetSpaceRoleMode` **added**; new `SpaceRoleMode` enum | RBAC tenants. |
| Space Permission Transition | `GET …/transition/combinations`, `GET …/transition/tasks/{taskId}` | none (messages modeled) | **left out** | Admin migration tooling, not indexer input. Messages stay. |
| Content | `POST /content/convert-ids-to-types` (read) | none (`ContentIdToContentTypeResponse` modeled) | `ResolveContentTypes` **added** (typed `ResolvedContentType`) | Useful when a webhook hands an id without a type. |
| Redactions | `POST /{pages,blogposts}/{id}/redact` | none | **left out** (write) | `Redaction*` messages stay. |
| Admin Key | `GET/POST/DELETE /admin-key` | none | **left out** | Caller-session admin state; not content. |
| Sync / Probe | n/a (crawler) | `Sync`, `ProbeConnection` | unchanged | |

## 3. Entity and enum drift vs the current spec

| Entity / enum | Spec | Contract before | Change |
|---|---|---|---|
| `SpaceStatus` | `current, archived, trashed` | missing `trashed` | **added** `SPACE_STATUS_TRASHED = 3` |
| `Space.properties` | `GET /spaces/{id}?include-properties` returns **space** properties (`SpaceProperty`: created_at, created_by, own version block) | typed `repeated ContentProperty properties = 14` | **retype impossible** → field 14 `[deprecated = true]`, **added** `repeated SpaceProperty space_properties = 18` |
| `Space` role assignments | `include-role-assignments` | absent | **added** `repeated SpaceRoleAssignment role_assignments = 19` |
| `Page` direct children | `GET /pages/{id}?include-direct-children` → `ChildrenResponse[]` | absent | **added** `repeated ContentTreeEntry direct_children = 22` (page.proto now imports content.proto; no cycle) |
| `Database`, `Folder`, `Whiteboard`, `SmartLink` | `include-properties`, `include-operations`, `include-direct-children` | only identity/hierarchy fields | **added** `properties`, `operations`, `direct_children` (14/15/16; SmartLink 15/16/17) |
| Content permissions | `ContentRestrictionState {OPEN, EDIT_RESTRICTED, VIEW_RESTRICTED}`, `RestrictionStateResponse {state, createdAt, createdBy, createdByAaid(deprecated)}`, `ContentPermissionOutput {operation read|update, principal {user|group, id}, createdAt, createdBy}` | absent | **added** in permission.proto: `ContentRestrictionState`, `RestrictionState`, `ContentPermissionOperation`, `ContentPermissionPrincipalType`, `ContentPermissionPrincipal`, `ContentPermission`. `createdByAaid` is a deprecated alias upstream and is not modeled. |
| Space role mode | `GET /space-role-mode` → `PRE_ROLES, ROLES_TRANSITION, ROLES` | absent | **added** `SpaceRoleMode` |
| Database data | `DatabaseField` (18 field types, per-type settings), `DatabaseEntry {id, databaseId, values: map<fieldId, DatabaseFieldValue>}`, `DatabaseFieldValue` (13 value shapes, `null` for empty and lookup types), `DatabaseView {id, name, type table/gallery/board/calendar/timeline}` | absent | **added** `database.proto` (see §4.5) |
| `ConfluenceEntity` arms | — | 36 arms | **added** 37–44: `database_entry`, `database_field`, `database_view`, `content_permission`, `restriction_state`, `space_role_assignment`, `space_permission`, `detailed_version` |
| `ContentType` / `ContentRef` | the `{content-type}` path segment (pages, blogposts, attachments, footer-comments, inline-comments, custom-content, whiteboards, databases, folders, embeds, spaces) | absent | **added** to common.proto |
| `Page`, `BlogPost`, `Attachment`, `Comment`, `CustomContent`, `Version`, `DetailedVersion`, `Task`, `User`, `Label`, `Like`, `Operation`, `ClassificationLevel`, `DataPolicy`, `SpacePermission*`, `SpaceRole*`, `BulkTransition*`, `Redaction*`, `AdminKeyResponse`, `BodyFormat`, `ContentStatus`, `BlogPostContentStatus`, `ParentContentType`, `AncestorType`, `InlineCommentResolutionStatus`, `SpaceType`, `AccountType/Status`, `ClassificationLevel*`, `PrincipalType`, `RoleType`, `SpacePermissionOperationKey/TargetType` | checked field-by-field and value-by-value against the spec | match | no change |

Not drift, but noted: the spec's `AttachmentSingle.title` description says "Title of the comment" (upstream typo); `ContentTreeEntry.type` stays a string because custom-content children carry the custom type name; `BulkTransitionCombinationEntry.principal_types` stays `repeated string` (spec enum includes `GUEST`, `ANONYMOUS`, `TEAM` etc.; a typed enum is a possible additive later).

## 4. Proposal details and rationale

### 4.1 One rpc per v2 read route family, `ContentRef` for the `{content-type}` axis
The v2 API multiplies routes by content type (`/pages/{id}/properties`, `/blogposts/{blogpost-id}/properties`, … nine times). Modeling that as nine rpcs per sub-resource would be transport leaking into the contract. Instead every per-content sub-resource rpc takes `ContentRef {ContentType type; string id}`; the server picks the route and answers `INVALID_ARGUMENT` when the API has no such route for that kind (e.g. inline comments on an attachment, permissions on a comment). The rpc comments list the valid kinds.

### 4.2 Request vocabulary kept in `confluence_service.proto`
`SortOrder {SortField, descending}` (union of the spec's eight sort enums; unsupported field → `INVALID_ARGUMENT`), `ContentIncludes` and `SpaceIncludes` (the `include-*` flags, ignored where the API lacks them), `SpaceDescriptionFormat`, `PageSubtype`, `CommentKind`, `LabelPrefix`, `DatabaseEntryFilter{Operator}`, `DatabaseEntrySort`, `ResolvedContentType`. These are request semantics, so they live with the service, not in the domain files. `metadata = 99` is kept on every request and unary response, as before; per-item streamed wrappers stay bare, as before.

### 4.3 No payload caps
`GetAttachmentRequest.include_content` loses the "refused above CONFLUENCE_ATTACHMENT_MAX_BYTES (25 MiB)" contract text; the only ceiling is the new optional `max_content_bytes` (0 = no cap; exceeding it is `FAILED_PRECONDITION` with a pointer to `DownloadAttachment`). `DownloadAttachment` is a server stream: first message `Attachment` metadata (content unset), then `bytes data` chunks; `chunk_bytes` (0 = server default) and `max_bytes` (0 = no cap) are per-request. Any default remains server configuration, not a contract constant. Java follow-up: `ConfluenceGrpcService.withContent` and the `CONFLUENCE_ATTACHMENT_MAX_BYTES` plumbing / README sentence must change to match (not done here).

### 4.4 Deprecate, never retype
`Space.properties` (field 14, `ContentProperty`) is wrong-typed against the spec. It is marked `[deprecated = true]` and `space_properties = 18` (`SpaceProperty`) is added. The mapper should stop filling 14. Nothing was renumbered or removed; a descriptor diff against `HEAD` confirms it (§6).

### 4.5 `database.proto`: typed rows, no Struct
Upstream describes the Databases API as experimental but it is the only way to read database rows, which are content. `DatabaseField` is `{id, name, description, type, oneof settings}` with one settings message per field type that has settings (text, aiSummary, number, select, date, jiraIssue, entryDetails, jiraIssueDetails, pageDetails, source [pageLabels/pageStatus], backlink); checkbox/hyperlink/user/attachment/pageLink/entryLink set no arm. `DatabaseFieldValue` is `{type, oneof value}` with text/aiSummary (plain + rich as `BodyType` in `ATLAS_DOC_FORMAT`, consistent with every other body), number as canonical decimal **string** (precision), checkbox bool, date as ISO **string** (date-only values have no instant), select/hyperlink/user/attachment/jiraIssue/pageLink/entryLink as typed lists. Empty cells and the six lookup-derived types (always `null` upstream) have `type` set and no arm. `DatabaseEntry.values` is `map<string, DatabaseFieldValue>` keyed by field id, exactly as the API keys it. `DatabaseView` is id/name/layout. Filtering is typed too: `DatabaseEntryFilter {field_id, operator EQUALS|IN|EMPTY|GT|GTE|LT|LTE, values}`.

### 4.6 Comments
One `ListComments` covers five route families via scope resolution (replies → container → workspace) and `CommentKind` (footer, inline, or both). `GetComment` needs the kind because footer and inline live on different routes; `UNSPECIFIED` lets the server try footer then inline so callers holding a bare id (e.g. from `Sync`) still work.

### 4.7 Permissions read model
`GetRestrictionState` + `ListContentPermissions` (direct grants) + `ListSpacePermissionAssignments` + `ListSpaceRoles/ListSpaceRoleAssignments` + `GetSpaceRoleMode` give an indexer everything v2 exposes to compute effective visibility; inherited restrictions are explicitly *not* represented upstream, so the consumer walks `ListAncestors`. Added entity arms let the crawler emit permission records on the same `ConfluenceEntity` stream later.

## 5. Left out, and why

| Item | Why |
|---|---|
| All writes (content, comments, properties, classification PUT/DELETE, permissions POST/PUT/DELETE, restriction-state PUT, redact, space/role mutation, transition tasks POST) | Read proxy for an indexer. The 28 Sep 2026 approval-409 behavior only affects writes and is therefore irrelevant to this contract. |
| `GET /attachments/{id}/thumbnail/download` | A 302 to a rendered thumbnail; not indexer input. Trivially additive later as a second streaming rpc if a UI needs it. |
| `GET …/likes/count` | Derivable from `ListLikes`; one fewer rpc. |
| Standalone operations rpc (`GET …/operations`) | Covered by `ContentIncludes.operations` / `SpaceIncludes.operations`; "what the *calling* user may do" is not end-user trimming data. |
| `include-collaborators`, `include-webresources` | Response shapes are not in the OpenAPI document (`PageSingle` has no such properties). Not modeled rather than guessed. Open question 4. |
| `GET /app/properties` | Forge `asApp()` only. `AppProperty` message stays for the entity stream. |
| `POST /user/access/check-access-by-email`, `invite-by-email` | Admin/user-management; not content. |
| Space Permission Transition reads | Tenant migration tooling. Messages already exist; rpcs can be added if an admin UI wants them. |
| `GET /admin-key` | Caller's own admin-key session state. |
| Streaming `ListSpaces` | Existing unary rpc; changing it breaks the wire. Spaces are few. Open question 1. |

## 6. Evidence this compiles and is wire-compatible

- `protoc` (via `grpcio-tools`, protobuf 7.x) compiles all 20 files with `--include_imports` (descriptor set at `…/scratchpad/out/confluence.fds`).
- `buf` 1.54.0 could not be downloaded in this sandbox (external-binary download denied), so a descriptor-based approximation of STANDARD + COMMENTS was run (`…/scratchpad/tools/buflint.py`): every service/rpc/message/field/oneof/enum/enum value has a leading comment; every enum's first value is `*_UNSPECIFIED = 0`; every enum value carries the enum-name prefix; every rpc's request/response is `<Rpc>Request`/`<Rpc>Response` and used by exactly one rpc; field names are lower_snake; intra-package imports are all used. Please run real `buf lint` in CI; the one rule family I could not approximate is `PACKAGE_*` consistency, which is unchanged.
- Wire compatibility (`…/scratchpad/tools/compat.py`, old descriptors built from `git show HEAD:`): **no field renumbered, retyped or removed; no enum value or rpc changed**. 111 → 305 messages, 23 → 39 enums, 9 → 50 rpcs.

## 7. Java follow-ups implied (not done here; outside the proto directory)

1. `ConfluenceGrpcService`: implement the 41 new rpcs (all are thin `walk`/`get` + mapper calls over the routes named in §8); in `getAttachment`/`withContent` replace the constructor-injected `attachmentMaxBytes` (`DEFAULT_ATTACHMENT_MAX_BYTES = 25 MiB`, `ConfluenceServer.ENV_ATTACHMENT_MAX_BYTES`) with the request's `max_content_bytes`; add the chunked `DownloadAttachment`. (The Microsoft service has the same `DEFAULT_ATTACHMENT_MAX_BYTES` pattern; out of scope here but the same defect.)
2. `ConfluenceMapper`: `toSpace` must fill `space_properties`/`role_assignments` and stop filling deprecated `properties`; add mappers for `ContentPermission`, `RestrictionState`, `DatabaseField/Entry/View`, `DetailedVersion`, `SpaceRoleMode`; map `SpaceStatus.trashed`.
3. `ConfluenceClient`: `POST` support for `users-bulk` and `convert-ids-to-types`; `Link`-header + `_links.next` pagination already exists; Databases API paginates via `_links.next` too.
4. README/architecture: remove the "25 MiB cap" sentences; `CONFLUENCE_ATTACHMENT_MAX_BYTES` becomes at most a server default for callers that set no ceiling, or is dropped.
5. `FakeConfluenceServer` fixtures for the new routes; `ConfluenceValidator` rules for new identities (`DatabaseEntry.id` + `database_id`, `DatabaseField.id`, `ContentPermission.principal`).

## 8. rpc → v2 endpoint map (complete)

| rpc | v2 route(s) |
|---|---|
| `ListSpaces` | `GET /spaces` |
| `GetSpace` | `GET /spaces/{id}` (key → `GET /spaces?keys=`) |
| `GetPage` | `GET /pages/{id}` |
| `GetBlogPost` | `GET /blogposts/{id}` |
| `ListPages` | `GET /pages`, `GET /spaces/{id}/pages`, `GET /labels/{id}/pages` |
| `ListBlogPosts` | `GET /blogposts`, `GET /spaces/{id}/blogposts`, `GET /labels/{id}/blogposts` |
| `GetAttachment` | `GET /attachments/{id}` (+ `downloadLink`) |
| `ListAttachments` | `GET /attachments`, `GET /pages/{id}/attachments`, `GET /blogposts/{id}/attachments`, `GET /custom-content/{id}/attachments`, `GET /labels/{id}/attachments` |
| `DownloadAttachment` | `GET /attachments/{id}` then `downloadLink` (CDN redirect followed) |
| `ListComments` | `GET /footer-comments`, `GET /inline-comments`, `GET /{pages,blogposts}/{id}/footer-comments`, `GET /{pages,blogposts}/{id}/inline-comments`, `GET /{attachments,custom-content}/{id}/footer-comments`, `GET /{footer,inline}-comments/{id}/children` |
| `GetComment` | `GET /footer-comments/{comment-id}`, `GET /inline-comments/{comment-id}` |
| `ListCustomContent` | `GET /custom-content`, `GET /spaces/{id}/custom-content`, `GET /pages/{id}/custom-content`, `GET /blogposts/{id}/custom-content` |
| `GetCustomContent` | `GET /custom-content/{id}` |
| `GetWhiteboard` / `GetDatabase` / `GetFolder` / `GetSmartLink` | `GET /whiteboards/{id}` / `GET /databases/{id}` / `GET /folders/{id}` / `GET /embeds/{id}` |
| `ListChildren` | `GET /{pages,whiteboards,databases,folders,embeds}/{id}/direct-children`, `GET /custom-content/{id}/children` |
| `ListDescendants` | `GET /{pages,whiteboards,databases,folders,embeds}/{id}/descendants` |
| `ListAncestors` | `GET /{pages,whiteboards,databases,folders,embeds}/{id}/ancestors` |
| `ListDatabaseFields` / `ListDatabaseEntries` / `GetDatabaseEntry` / `ListDatabaseViews` | `GET /databases/{id}/fields` / `/entries` / `/entries/{entryId}` / `/views` |
| `ListLabels` | `GET /labels`, `GET /spaces/{id}/labels`, `GET /spaces/{id}/content/labels`, `GET /{pages,blogposts,attachments,custom-content}/{id}/labels` |
| `ListLikes` | `GET /{pages,blogposts,footer-comments,inline-comments}/{id}/likes/users` |
| `ListVersions` | `GET /{pages,blogposts,attachments,footer-comments,inline-comments,custom-content}/{id}/versions` |
| `GetVersion` | `GET /{…}/{id}/versions/{version-number}` |
| `ListTasks` / `GetTask` | `GET /tasks` / `GET /tasks/{id}` |
| `BatchGetUsers` | `POST /users-bulk` (≤250 ids per call; server chunks) |
| `ListClassificationLevels` | `GET /classification-levels` |
| `GetClassificationLevel` | `GET /{pages,blogposts,databases,whiteboards}/{id}/classification-level`, `GET /spaces/{id}/classification-level/default` |
| `ListDataPolicySpaces` / `GetDataPolicyMetadata` | `GET /data-policies/spaces` / `GET /data-policies/metadata` |
| `ListContentProperties` / `GetContentProperty` | `GET /{9 content types}/{id}/properties` / `…/properties/{property-id}` |
| `ListSpaceProperties` / `GetSpaceProperty` | `GET /spaces/{space-id}/properties` / `…/{property-id}` |
| `GetRestrictionState` | `GET /{pages,blogposts,databases,whiteboards,folders}/{id}/restriction-state` |
| `ListContentPermissions` | `GET /{pages,blogposts,databases,whiteboards,folders}/{id}/permissions` |
| `ListSpacePermissions` / `ListSpacePermissionAssignments` | `GET /space-permissions` / `GET /spaces/{id}/permissions` |
| `ListSpaceRoles` / `GetSpaceRole` / `ListSpaceRoleAssignments` / `GetSpaceRoleMode` | `GET /space-roles` / `GET /space-roles/{id}` / `GET /spaces/{id}/role-assignments` / `GET /space-role-mode` |
| `ResolveContentTypes` | `POST /content/convert-ids-to-types` (≤100 ids per call; server chunks) |
| `Sync`, `ProbeConnection` | crawler (unchanged) |

## 9. Open questions for Kristian

1. **`ListSpaces` stays unary** (wire compat) while every other list streams. Accept the inconsistency, or add a streaming `StreamSpaces` twin and deprecate the unary one?
2. **Space.properties deprecation**: OK to deprecate field 14 and have the mapper stop filling it immediately, or keep filling it (as `ContentProperty` with the subset of fields) for one release?
3. **Database data (experimental upstream)**: include in v1 now (as proposed) or hold `database.proto` + the four rpcs behind a separate `v1alpha`/`experimental` file until Atlassian drops the experimental flag? Their pagination is "best effort while the database is changing", which affects `Sync` consistency semantics if the crawler ever emits rows.
4. **`include-collaborators` / `include-webresources`**: the OpenAPI document has no response schema for either. Should I probe a live tenant to learn the shape (likely `collaborators: {results: [accountId…]}`) and add `repeated string collaborator_ids`, or leave them out?
5. **`ContentRef` vs per-type rpcs**: the proposal uses one `ContentRef`-keyed rpc per sub-resource (matches the vendor's `{content-type}` axis; 50 rpcs total). Alternative is per-type rpcs (`ListPageVersions`, `ListBlogPostVersions`, …; ~110 rpcs). Preference?
6. **Attachment chunk default**: `DownloadAttachmentRequest.chunk_bytes = 0` means "server default". Is a server default of ~1 MiB per gRPC message acceptable, and should the server env var replacing `CONFLUENCE_ATTACHMENT_MAX_BYTES` exist at all (as a default ceiling applied only when the caller sets none), or should it be dropped entirely?
7. **Should the crawler (`Sync`) start emitting the new entity arms** (restriction state, content permissions, space role assignments, database rows) so the index mirror has trimming data without a second pass? That is a crawler-scope change, not a contract one, but the arms are there for it.
8. **`DataPolicyMetadata`** is app-credential only upstream; keep the rpc (fails `PERMISSION_DENIED` with basic auth) or drop it until the proxy supports Connect/Forge credentials?

## Review (Opus)

Reviewed 2026-11-08 against the pinned v2 OpenAPI (`…/scratchpad/upstream/confluence-v2-openapi.json`), checking every kept route's parameters, enums and response schema, plus `HEAD~1` and the house style. Nothing is committed. `grpc-microsoft-api` was not touched.

### Build status

- `bash ./gradlew -q :grpc-confluence-api:compileJava :grpc-confluence-service:compileJava :grpc-confluence-service:compileTestJava` → **passes** (it failed on the draft).
- `bash ./gradlew -q :grpc-okf:compileTestJava :grpc-output-spi:compileTestJava :grpc-output-filesystem:compileTestJava :grpc-output-s3:compileTestJava :grpc-mcp:compileTestJava :grpc-connect:compileTestJava` → **passes**. These are all the modules that depend on `grpc-confluence-api` in `settings.gradle.kts`, directly or through other modules. They also depend on `grpc-microsoft-api`, so the result reflects the other reviewer's tree as it stood when I ran it.
- Docker-backed tests were not run, as instructed.
- Wire compatibility against `HEAD~1` (descriptor diff, `…/scratchpad/opus/compat_conf.py`): **0 incompatibilities**. Counts: 111 → 268 messages, 23 → 38 enums, 9 → 41 rpcs.
- Approximate buf STANDARD+COMMENTS lint (`tools/buflint.py` on `…/scratchpad/opus/new.fds`): **OK**.

### What I changed and why

1. **Build fix.** I renamed the `ConfluenceEntity` arm `database_field` to `database_field_definition`. protoc-java generates a private `getDatabaseFieldBuilder()` for the existing `database` arm (19), so the new arm clashed with it. The existing field was not renamed.
2. **Java, only what compilation needed.** `ConfluenceValidator.fieldName` and `nestedEntity` are exhaustive switches over `EntityCase`. I added the five kept new arms to both. Every other switch over `EntityCase` (`ObjectKeys`, `ConfluenceCatalog`, `SyncTableChangeSink`, `ConfluenceGrpcService.stripBodies`) has a `default` branch and needed no change.
3. **Cut 9 rpcs (50 → 41)**, with their Request/Response messages:

| Cut | Why |
|---|---|
| `ListDataPolicySpaces`, `GetDataPolicyMetadata` | The spec says **both** routes are "Only apps can make this request". The draft flagged only the metadata route. The proxy uses user/basic credentials, so both rpcs could only ever return `PERMISSION_DENIED`. |
| `ListDatabaseViews` | Views expose only id, name and layout. The spec says entry results "are based on the database, not a particular view", so views have no indexing value. |
| `GetVersion` | `DetailedVersion` (change-set prev/next, collaborators) is not indexer input. `ListVersions` plus `GetPage/GetBlogPost/...(version=n)` already gives history and historical bodies. |
| `ListLikes` | This is a social signal, not content, and `ContentIncludes.likes` inlines a preview. If ranking ever needs popularity, `…/likes/count` is the right thing to add, not a stream of every account that liked something. |
| `GetContentProperty`, `GetSpaceProperty` | Redundant: the list rpcs already filter by `key`, which is how properties are addressed in practice. |
| `GetSpaceRole` | Redundant with `ListSpaceRoles`, which returns the tenant's small role catalogue. |
| `ListSpacePermissions` | The tenant's catalogue of permission *definitions* (display names and descriptions). Trimming does not need it: `SpacePermissionAssignment` already carries a typed `operation {key, target_type}`. |

4. **Entity arms cut from 8 to 5, renumbered contiguously to 37–41.** The renumbering is safe because none of these arms exist in `HEAD~1`. Kept: `database_entry`, `database_field_definition`, `content_permission`, `restriction_state`, `space_role_assignment`. Dropped `database_view`, `space_permission` and `detailed_version`, because their rpcs were cut and they have no trimming or indexing use. Their domain messages stay as unserved parity models, the same way `BulkTransition*` and `Redaction*` already exist.
5. **Removed `CONTENT_TYPE_SPACE`.** No rpc that takes a `ContentRef` accepted a space; every space-scoped rpc takes `space_id`. The value was a dead, contradictory option. The enum comment now says so.
6. **Documented the upstream 50-entry cap.** The spec caps every `include-*` expansion at 50 entries. The `ContentIncludes`, `SpaceIncludes` and `Space.role_assignments` comments now say the inlined lists are previews and name the list rpc that returns the full set. This matters for security trimming: a consumer reading `Space.permissions` as complete would get trimming wrong. I also noted that `include-role-assignments` is EAP-only upstream.
7. **Comment fixes.**
   - Comment properties use `GET /comments/{id}/properties` for both comment kinds. The draft's `{content-type}` mapping did not say this.
   - `Space.properties` is "never populated": the mapper never filled it, so "no longer populates" was wrong. This also settles open question 2.
   - Section headers updated to match the cuts.

Verified against the spec and found correct: the `SortField` union (exactly id, created-date, modified-date, title, child-position, key, name), the database field and value model (18 types, 13 value shapes, filter matrix), the content-permission and restriction-state schemas, `SpaceRoleMode`, `SpaceStatus.TRASHED`, the `Space.properties` retype rationale (`SpaceSingle.properties` really is `SpaceProperty`), and the task filters (epoch-millis → `Timestamp`).

### Still flag for Kristian

1. **The contract is ahead of the implementation.** Reflection will advertise 41 rpcs while the Java service implements 9; the other 32 return `UNIMPLEMENTED`. `GetAttachmentRequest` now promises "no fixed cap", but `ConfluenceGrpcService` still enforces `CONFLUENCE_ATTACHMENT_MAX_BYTES` (25 MiB). Land the proto together with the Java follow-ups in §7, or land the protos rpc-group by rpc-group.
2. **Experimental Databases API in `v1`** (open question 3) is still open. I kept it because rows are content. Atlassian reserves the right to make backwards-incompatible changes, and we would absorb those in the mapper.
3. **`Comment` has no footer/inline discriminator.** `ListComments` with `kind = UNSPECIFIED` interleaves both kinds, and consumers have to infer the kind from `inline_properties`. An additive `Comment` field would fix this. That needs a decision because `CommentKind` currently lives in the service file.
4. **Pre-existing naming hazard.** `ContentIdToContentTypeResponse` and `ListSpacePermissionCombinationsResponse` are domain messages with rpc-style names. `ResolveContentTypesResponse` now duplicates the first one in typed form. If an rpc with one of those names is ever added, buf's naming and uniqueness rules will collide with the existing message.
5. **Minor inconsistency.** `ListContentPermissionsRequest` passes `principal_type` and `principal_id` as flat fields, while `ListSpaceRolesRequest` takes a `Principal` message. I left it, because each mirrors its route's query parameters.
6. **The lint and breaking checks here are approximations.** Run real `buf lint` and `buf breaking --against main` (category FILE) in CI.
7. Open questions 1, 4, 5, 6 and 7 above still stand. Questions 2 and 8 are settled by this review.

### Final rpc list (41)

`ListSpaces`, `GetSpace`, `GetPage`, `GetBlogPost`, `ListPages`, `ListBlogPosts`, `GetAttachment`, `ListAttachments`, `DownloadAttachment`, `ListComments`, `GetComment`, `ListCustomContent`, `GetCustomContent`, `GetWhiteboard`, `GetDatabase`, `GetFolder`, `GetSmartLink`, `ListChildren`, `ListDescendants`, `ListAncestors`, `ListDatabaseFields`, `ListDatabaseEntries`, `GetDatabaseEntry`, `ListLabels`, `ListVersions`, `ListTasks`, `GetTask`, `BatchGetUsers`, `ListClassificationLevels`, `GetClassificationLevel`, `ListContentProperties`, `ListSpaceProperties`, `GetRestrictionState`, `ListContentPermissions`, `ListSpacePermissionAssignments`, `ListSpaceRoles`, `ListSpaceRoleAssignments`, `GetSpaceRoleMode`, `ResolveContentTypes`, `Sync`, `ProbeConnection`.

§2, §5 and §8 above describe the draft's 50-rpc state. This section supersedes them wherever they disagree.

## Decisions (Kristian, 2026-11-08)

Answers to the open questions in §9. Where they disagree with earlier sections, these win.

1. **Streaming over unary.** `StreamSpaces` (server-streaming, same filters) is added and implemented. The unary `ListSpaces` is deprecated and kept for wire compatibility.
2. **`Space.properties`** stays deprecated in favour of `space_properties`.
3. **Databases API** stays in `v1` even though it is experimental upstream.
4. **`include-collaborators` / `include-webresources`.** Every Atlassian spec must be downloaded and vendored: see `docs/upstream/atlassian/`. The v2 document declares both flags but no response schema, so `ConfluenceLiveSmokeIT` now prints the field names and JSON types (never values) that a real tenant returns. The fields get typed from that output. The REST v1 document is not vendored yet because the build sandbox cannot reach Atlassian's hosts.
5. **Keyed by default, per-type when asked.** The `ContentRef`-keyed rpcs stay the default surface. A per-type twin (for example `ListPageVersions`) is added only when a caller has a concrete need for it, not wholesale.
6. **`chunk_bytes` vs `max_bytes`.** These are different things. `chunk_bytes` is gRPC framing: the size of each streamed message. The server reads the file from Confluence and re-chunks it, so this is not a Confluence parameter, and 0 means the server implementation's default. `max_bytes` is an optional ceiling on the whole file, where 0 means no cap. There is no hardcoded cap in the contract.
7. **The crawler should emit the new arms.** `Sync` should emit restriction state, content permissions, space role assignments and database rows, so the index has the data it needs for security trimming without a second pass. This is implementation work for the services phase.
8. **Data policies are tracked.** `ListDataPolicySpaces` and `GetDataPolicyMetadata` are restored. Upstream serves them only to app (Connect/Forge) credentials, so with API-token auth they fail with `PERMISSION_DENIED`.

Lint: real `buf lint` (1.54.0, STANDARD+COMMENTS) is clean. `buf breaking --against main` is clean for WIRE_JSON. FILE reports only the Microsoft messages that moved into new files (same package and Java names).
