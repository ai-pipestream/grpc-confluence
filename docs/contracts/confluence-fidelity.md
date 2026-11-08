# Confluence v2 field fidelity

Status: audit of 2026-11-08, branch `claude/project-thread-h5j2my-fidelity`.

This document checks the gRPC contract against the vendored Confluence Cloud
REST API v2 spec ([`docs/upstream/atlassian/confluence-v2.json`](../upstream/atlassian/confluence-v2.json)).
It answers one question: does every property of every response that a
`ConfluenceService` rpc reads reach a typed proto field, with nothing lost? It
supplements [confluence-gap-analysis.md](confluence-gap-analysis.md). That
document's "Decisions (Kristian, 2026-11-08)" section still wins where the
two disagree.

## Method

A throwaway audit script runs outside the repository. It reads the spec with
`python3 -I` and works on the response schemas of these routes:

- every GET route named in the comments of `confluence_service.proto`;
- `POST /users-bulk`;
- `POST /content/convert-ids-to-types`.

For the list routes it takes the `results[]` item. It resolves `$ref` and
`allOf`, and it expands `oneOf` per discriminator value. The database field
and value unions are walked one variant at a time. The script then lists
every property path.

For each path the script checks two things:

1. A proto field exists to hold it. The script follows the field's message
   type into nested objects and include-expansion items. Spec enums are
   checked value by value against the proto enum, using the mapper's
   `wireEnum` rule: upper-case, `-` becomes `_`, plus a prefix, and `snake()`
   for camelCase values.
2. `ConfluenceMapper` reads the JSON key.

The status column has these values:

- **yes**: the field was already modeled and mapped before this audit.
- **added (proto + mapper)**: a new proto field, mapped in this change.
- **added (mapper)**: the proto field already existed but nothing filled it.
  It is now mapped.
- **dropped: reason**: left out on purpose. The domain model allows only two
  reasons. One is hypermedia (`_links`, except `webui` and `download`, which
  become `web_url` and `download_url`). The other is pagination transport
  (`meta.cursor`; for the database lists also `meta.pageSize`, which is not
  data).

The script reports 0 gaps after this change. Each table row is a distinct
spec path across all the route variants of an entity. Bulk and single
variants are merged: the domain model keeps one message per entity kind.

## What changed

### Gaps found and fixed

| # | Gap (data lost before) | Fix |
|---|---|---|
| 1 | The `meta.hasMore` of include-* expansions was dropped. A consumer could not tell that a preview of at most 50 entries (labels, properties, operations, likes, versions, direct children, space permissions, role assignments) was partial. | New enum `IncludeExpansion` (common.proto). Each entity with include expansions gets `repeated IncludeExpansion truncated_includes`. The mapper adds the matching value when `meta.hasMore` is true. `meta.cursor` stays dropped as transport. |
| 2 | Version-history entries (`GET /{content-type}/{id}/versions`) carry a snapshot under `page`, `blogpost`, `attachment`, `custom` or `comment`. `toVersion` ignored it, although `Version.entity` already existed. | `toVersion` fills `Version.entity` (`VersionedEntity`: id, title, body) from whichever snapshot key is present. |
| 3 | Content and space property values that are a top-level JSON array went to `toStruct()`, which reads object fields only. The array came out as an **empty** Struct, so its contents were lost. | New oneof arm `PropertyValue.json_list_value` (`google.protobuf.ListValue`, field 6). Objects still use `json_value`. This is an interim fix; see open question 1. |
| 4 | `RestrictionState.created_by` and `ContentPermission.created_by` preferred `createdByAaid`, which the spec marks as a deprecated alias of `createdBy`. | `createdBy` now wins, and the alias is the fallback. Both spec keys still map to the one field. |
| 5 | Nothing mapped `POST /content/convert-ids-to-types` into the typed `ResolvedContentType`. `wireEnum` cannot do it, because `blogpost` and `footer-comment` do not match the `ContentType` member names. | New `ConfluenceMapper.toResolvedContentTypes()` and `contentType()`. Built-in values become `ContentType`, anything else goes to `custom_type` verbatim. |
| 6 | `DatabaseView` had a proto message but no mapper. | New `ConfluenceMapper.toDatabaseView()`. There is still no rpc; see open question 5. |
| 7 | `ConfluenceValidator` validated the deprecated `Space.properties` but not `Space.space_properties`. | It now visits `space_properties` and rejects `INCLUDE_EXPANSION_UNSPECIFIED` in `truncated_includes`. |

### Proto fields added per entity

All additions are wire-compatible. They use new field numbers only, and
`metadata = 99` stays last. `buf lint` (STANDARD+COMMENTS) is clean, and so is
`buf breaking` (WIRE_JSON) against `main` and against the branch base.

| Message | Field added | Number |
|---|---|---|
| Page | `repeated IncludeExpansion truncated_includes` | 25 |
| BlogPost | `repeated IncludeExpansion truncated_includes` | 18 |
| Attachment | `repeated IncludeExpansion truncated_includes` | 23 |
| Comment | `repeated IncludeExpansion truncated_includes` | 21 |
| CustomContent | `repeated IncludeExpansion truncated_includes` | 19 |
| Space | `repeated IncludeExpansion truncated_includes` | 20 |
| Whiteboard | `repeated IncludeExpansion truncated_includes` | 18 |
| Database | `repeated IncludeExpansion truncated_includes` | 18 |
| Folder | `repeated IncludeExpansion truncated_includes` | 18 |
| SmartLink | `repeated IncludeExpansion truncated_includes` | 19 |
| PropertyValue | oneof arm `google.protobuf.ListValue json_list_value` | 6 |
| (new enum) | `IncludeExpansion`: UNSPECIFIED, LABELS, PROPERTIES, OPERATIONS, LIKES, VERSIONS, DIRECT_CHILDREN, PERMISSIONS, ROLE_ASSIGNMENTS | - |

**Why this shape for truncation.** The cheapest wire-compatible option is one
repeated enum per entity. One field covers every include, there is no wrapper
message, and the existing `repeated Label labels` (and the other include
fields) keep their types. It is also strongly typed, unlike a `map<string,bool>`.
Two alternatives were rejected:

- A typed preview wrapper such as `LabelPreview { repeated Label items; bool has_more; }`
  would change the type of every existing include field, and that is not
  wire-compatible.
- Per-field `bool labels_truncated` flags would mean about 40 new fields for
  the same information.

### Items checked and found already complete

Several fields were named as things to check. All of them are present in the
spec and mapped:

- `createdByAaid` (restriction state, content permissions)
- `Space.spaceOwnerId` and `currentActiveAlias`
- all attachment fields, including `webuiLink`, `downloadLink`, `fileId` and `fileSize`
- all task and user fields
- the custom-content containers (`spaceId`, `pageId`, `blogPostId`, `customContentId`)
- the inline-comment resolution fields and highlighted-text properties
- the whiteboard, database, folder and embed fields (including `embedUrl`)
- every database field-settings and field-value variant
- the classification level fields, including the `order` number and the `color` and `status` enums
- every spec enum value (the script checks them all)

Some names are not in the vendored spec at all, so there is nothing to map:

- `version.ncsStepVersion` (or anything similar): the spec `Version` has only
  createdAt, message, number, minorEdit and authorId.
- A label-prefix enum: the spec types `Label.prefix` as a free string with no
  enum. See open question 3.

### Deliberately dropped (only hypermedia and pagination)

- `_links.*` on every entity, except `webui` (to `web_url`) and `download` (to
  `download_url`). Dropped keys: `base`, `editui`, `tinyui`, `self`, `next`, and
  each include expansion's `_links.self`. These URLs can be derived from the
  tenant base URL and the entity id.
- `meta.cursor` on every include expansion. `meta.hasMore` is kept, as
  `truncated_includes`.
- `_links` and `meta` (pageSize, cursor, hasMore) on the list envelopes. The
  streaming rpcs page internally, so these never reach the protos.

### Beyond the spec (from a live tenant run, per Decision 4)

The spec does not declare these, but the contract types them:
`collaborator_account_ids` (`include-collaborators`), `WebResources web_resources`
(`include-webresources`), and `direct_children` (`include-direct-children`,
including on whiteboard, database, folder and embed). The spec schemas for
whiteboard, database, folder and embed do not list `properties` or
`operations` either, even though those routes accept the include flags. The
contract models them anyway, and the mapper fills them through the shared
`applyIncludes`.

## Open questions for the owner

1. **Typed property values (data is not JSON).** `ContentProperty`,
   `SpaceProperty` and `AppProperty` values that are JSON objects still go to
   `PropertyValue.json_value` (`google.protobuf.Struct`). Top-level arrays now
   go to `json_list_value` (`ListValue`), so they are no longer lost. This was
   not redesigned in this change, as instructed. Struct also has a fidelity
   flaw of its own: every number becomes a double, so integers above 2^53
   inside an object lose precision. Three typed alternatives:
   - **(a) Per-key typed messages.** Add a typed oneof arm per well-known
     property key (for example `EditorProperty`, `ContentAppearance`, and the
     Confluence-owned keys the crawler sees, such as `emoji-title-published`
     and `cover-picture-id-published`). Keep `CUSTOM` plus a serialized
     fallback only for app-defined keys. This gives the strongest typing, but
     the catalogue of keys keeps growing.
   - **(b) A recursive typed value.** Define our own
     `PropertyValue { oneof { string; int64; double; bool; PropertyObject; PropertyList; } }`,
     where `PropertyObject` is a `map<string, PropertyValue>` and
     `PropertyList` is a `repeated PropertyValue`. This keeps int64 versus
     double exactly and removes the dependency on `google.protobuf.Struct`. It
     is still schema-less, but it is our type, and it can be generated from
     and validated against the wire.
   - **(c) Serialized content in a declared format.** Treat an app-defined
     value the way bodies are treated: `BodyType`-style `{format: JSON, value:
     string}`, since the value belongs to the app that wrote it. This is
     lossless and simple, but opaque to consumers.

   Recommendation: (b) for app-defined keys, plus (a) for the handful of
   Confluence-owned keys we act on.
2. **ADF documents as strings.** Bodies in `atlas_doc_format`, and the
   database `rich` text and AI-summary values (`DatabaseAdfDocument`), travel
   as serialized JSON in a `BodyType` with format `ATLAS_DOC_FORMAT`. The
   domain model allows this, because it is serialized content in a declared
   format, like XHTML storage. Is that acceptable for good, or should ADF get
   a typed node tree (`AdfNode { type; attrs; content; marks; text }`)?
3. **Label prefix.** The spec types it as a free string. Common values are
   `global`, `my` and `team`. Should we add a `LabelPrefix` enum next to the
   string (wire-compatible, keeping the raw string for unknown values), or
   keep the string?
4. **Other free strings the spec leaves open.** `Operation.operation` and
   `target_type`, `ContentTreeEntry.type`, the `type` of whiteboard, database,
   folder and embed, and `ResolvedContentType.custom_type` are all strings in
   the spec. The same two choices apply: add an enum next to the string, or
   keep the string.
5. **`GET /databases/{id}/views`.** The `DatabaseView` message and its mapper
   exist now, but no rpc serves them. The task excludes the gRPC service, so
   none was added here. Should `ListDatabaseViews` be added?
6. **Unknown body representations.** A `representation` string that is not
   one of the ten documented values maps to `BODY_FORMAT_UNSPECIFIED`, and the
   raw string is not kept. The value itself is kept. Should we add a
   `string representation` to `BodyType` as an escape hatch?
7. **Null property values.** A property whose value is JSON `null` comes out
   with `value` unset, the same as an absent value. The v2 contract does not
   distinguish the two, so nothing is lost today. Option (b) above would add a
   null arm if we ever need one.

## Per-entity tables

### Space

Routes: GET /spaces, GET /spaces/{id}

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `Space.id` | yes |
| `key` | `Space.key` | yes |
| `name` | `Space.name` | yes |
| `type` | `Space.type` | yes |
| `status` | `Space.status` | yes |
| `authorId` | `Space.author_id` | yes |
| `spaceOwnerId` | `Space.space_owner_id` | yes |
| `currentActiveAlias` | `Space.current_active_alias` | yes |
| `createdAt` | `Space.created_at` | yes |
| `homepageId` | `Space.homepage_id` | yes |
| `description` | `Space.description` | yes |
| `description.plain` | `SpaceDescription.plain` | yes |
| `description.plain.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `description.plain.value` | `BodyType.value` | yes |
| `description.view` | `SpaceDescription.view` | yes |
| `description.view.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `description.view.value` | `BodyType.value` | yes |
| `icon` | `Space.icon` | yes |
| `icon.path` | `SpaceIcon.path` | yes |
| `icon.apiDownloadLink` | `SpaceIcon.api_download_url` | yes |
| `_links.webui` | `Space.web_url` | yes |
| `labels.results[]` | `Space.labels` | yes |
| `labels.results[].id` | `Label.id` | yes |
| `labels.results[].name` | `Label.name` | yes |
| `labels.results[].prefix` | `Label.prefix` | yes |
| `labels.meta.hasMore` | `Space.truncated_includes += INCLUDE_EXPANSION_LABELS` | added (proto + mapper) |
| `labels.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `labels._links.self` | - | dropped: hypermedia (`_links`) |
| `properties.results[]` | `Space.space_properties` | yes |
| `properties.results[].id` | `SpaceProperty.id` | yes |
| `properties.results[].key` | `SpaceProperty.key` | yes |
| `properties.results[].value` | `SpaceProperty.value` | yes - typed oneof by JSON kind (string/integer/bool/double); objects to Struct `json_value`; top-level arrays to `json_list_value` (added: they were silently emptied before); see open question 1 |
| `properties.results[].createdAt` | `SpaceProperty.created_at` | yes |
| `properties.results[].createdBy` | `SpaceProperty.created_by` | yes |
| `properties.results[].version` | `SpaceProperty.version` | yes |
| `properties.results[].version.createdAt` | `SpacePropertyVersion.created_at` | yes |
| `properties.results[].version.createdBy` | `SpacePropertyVersion.created_by` | yes |
| `properties.results[].version.message` | `SpacePropertyVersion.message` | yes |
| `properties.results[].version.number` | `SpacePropertyVersion.number` | yes |
| `properties.meta.hasMore` | `Space.truncated_includes += INCLUDE_EXPANSION_PROPERTIES` | added (proto + mapper) |
| `properties.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `properties._links.self` | - | dropped: hypermedia (`_links`) |
| `operations.results[]` | `Space.operations` | yes |
| `operations.results[].operation` | `Operation.operation` | yes |
| `operations.results[].targetType` | `Operation.target_type` | yes |
| `operations.meta.hasMore` | `Space.truncated_includes += INCLUDE_EXPANSION_OPERATIONS` | added (proto + mapper) |
| `operations.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `operations._links.self` | - | dropped: hypermedia (`_links`) |
| `permissions.results[]` | `Space.permissions` | yes |
| `permissions.results[].id` | `SpacePermissionAssignment.id` | yes |
| `permissions.results[].principal` | `SpacePermissionAssignment.principal` | yes |
| `permissions.results[].principal.type` | `SpacePermissionPrincipal.type` | yes |
| `permissions.results[].principal.id` | `SpacePermissionPrincipal.id` | yes |
| `permissions.results[].operation` | `SpacePermissionAssignment.operation` | yes |
| `permissions.results[].operation.key` | `SpacePermissionOperation.key` | yes |
| `permissions.results[].operation.targetType` | `SpacePermissionOperation.target_type` | yes |
| `permissions.meta.hasMore` | `Space.truncated_includes += INCLUDE_EXPANSION_PERMISSIONS` | added (proto + mapper) |
| `permissions.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `permissions._links.self` | - | dropped: hypermedia (`_links`) |
| `_links.base` | - | dropped: hypermedia (`_links`) |

### Page

Routes: GET /pages, GET /spaces/{id}/pages, GET /labels/{id}/pages, GET /pages/{id}

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `Page.id` | yes |
| `status` | `Page.status` | yes |
| `title` | `Page.title` | yes |
| `spaceId` | `Page.space_id` | yes |
| `parentId` | `Page.parent_id` | yes |
| `parentType` | `Page.parent_type` | yes |
| `position` | `Page.position` | yes |
| `authorId` | `Page.author_id` | yes |
| `ownerId` | `Page.owner_id` | yes |
| `lastOwnerId` | `Page.last_owner_id` | yes |
| `subtype` | `Page.subtype` | yes |
| `createdAt` | `Page.created_at` | yes |
| `version` | `Page.version` | yes |
| `version.createdAt` | `Version.created_at` | yes |
| `version.message` | `Version.message` | yes |
| `version.number` | `Version.number` | yes |
| `version.minorEdit` | `Version.minor_edit` | yes |
| `version.authorId` | `Version.author_id` | yes |
| `body` | `Page.body` | yes |
| `body.storage` | `Body.storage` | yes |
| `body.storage.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.storage.value` | `BodyType.value` | yes |
| `body.atlas_doc_format` | `Body.atlas_doc_format` | yes |
| `body.atlas_doc_format.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.atlas_doc_format.value` | `BodyType.value` | yes |
| `_links.webui` | `Page.web_url` | yes |
| `_links.editui` | - | dropped: hypermedia (`_links`) |
| `_links.tinyui` | - | dropped: hypermedia (`_links`) |
| `body.view` | `Body.view` | yes |
| `body.view.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.view.value` | `BodyType.value` | yes |
| `labels.results[]` | `Page.labels` | yes |
| `labels.results[].id` | `Label.id` | yes |
| `labels.results[].name` | `Label.name` | yes |
| `labels.results[].prefix` | `Label.prefix` | yes |
| `labels.meta.hasMore` | `Page.truncated_includes += INCLUDE_EXPANSION_LABELS` | added (proto + mapper) |
| `labels.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `labels._links.self` | - | dropped: hypermedia (`_links`) |
| `properties.results[]` | `Page.properties` | yes |
| `properties.results[].id` | `ContentProperty.id` | yes |
| `properties.results[].key` | `ContentProperty.key` | yes |
| `properties.results[].value` | `ContentProperty.value` | yes - typed oneof by JSON kind (string/integer/bool/double); objects to Struct `json_value`; top-level arrays to `json_list_value` (added: they were silently emptied before); see open question 1 |
| `properties.results[].version` | `ContentProperty.version` | yes |
| `properties.results[].version.createdAt` | `Version.created_at` | yes |
| `properties.results[].version.message` | `Version.message` | yes |
| `properties.results[].version.number` | `Version.number` | yes |
| `properties.results[].version.minorEdit` | `Version.minor_edit` | yes |
| `properties.results[].version.authorId` | `Version.author_id` | yes |
| `properties.meta.hasMore` | `Page.truncated_includes += INCLUDE_EXPANSION_PROPERTIES` | added (proto + mapper) |
| `properties.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `properties._links.self` | - | dropped: hypermedia (`_links`) |
| `operations.results[]` | `Page.operations` | yes |
| `operations.results[].operation` | `Operation.operation` | yes |
| `operations.results[].targetType` | `Operation.target_type` | yes |
| `operations.meta.hasMore` | `Page.truncated_includes += INCLUDE_EXPANSION_OPERATIONS` | added (proto + mapper) |
| `operations.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `operations._links.self` | - | dropped: hypermedia (`_links`) |
| `likes.results[]` | `Page.likes` | yes |
| `likes.results[].accountId` | `Like.account_id` | yes |
| `likes.meta.hasMore` | `Page.truncated_includes += INCLUDE_EXPANSION_LIKES` | added (proto + mapper) |
| `likes.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `likes._links.self` | - | dropped: hypermedia (`_links`) |
| `versions.results[]` | `Page.versions` | yes |
| `versions.results[].createdAt` | `Version.created_at` | yes |
| `versions.results[].message` | `Version.message` | yes |
| `versions.results[].number` | `Version.number` | yes |
| `versions.results[].minorEdit` | `Version.minor_edit` | yes |
| `versions.results[].authorId` | `Version.author_id` | yes |
| `versions.meta.hasMore` | `Page.truncated_includes += INCLUDE_EXPANSION_VERSIONS` | added (proto + mapper) |
| `versions.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `versions._links.self` | - | dropped: hypermedia (`_links`) |
| `isFavoritedByCurrentUser` | `Page.is_favorited_by_current_user` | yes |
| `_links.base` | - | dropped: hypermedia (`_links`) |

### BlogPost

Routes: GET /blogposts, GET /spaces/{id}/blogposts, GET /labels/{id}/blogposts, GET /blogposts/{id}

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `BlogPost.id` | yes |
| `status` | `BlogPost.status` | yes |
| `title` | `BlogPost.title` | yes |
| `spaceId` | `BlogPost.space_id` | yes |
| `authorId` | `BlogPost.author_id` | yes |
| `createdAt` | `BlogPost.created_at` | yes |
| `version` | `BlogPost.version` | yes |
| `version.createdAt` | `Version.created_at` | yes |
| `version.message` | `Version.message` | yes |
| `version.number` | `Version.number` | yes |
| `version.minorEdit` | `Version.minor_edit` | yes |
| `version.authorId` | `Version.author_id` | yes |
| `body` | `BlogPost.body` | yes |
| `body.storage` | `Body.storage` | yes |
| `body.storage.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.storage.value` | `BodyType.value` | yes |
| `body.atlas_doc_format` | `Body.atlas_doc_format` | yes |
| `body.atlas_doc_format.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.atlas_doc_format.value` | `BodyType.value` | yes |
| `_links.webui` | `BlogPost.web_url` | yes |
| `_links.editui` | - | dropped: hypermedia (`_links`) |
| `_links.tinyui` | - | dropped: hypermedia (`_links`) |
| `body.view` | `Body.view` | yes |
| `body.view.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.view.value` | `BodyType.value` | yes |
| `labels.results[]` | `BlogPost.labels` | yes |
| `labels.results[].id` | `Label.id` | yes |
| `labels.results[].name` | `Label.name` | yes |
| `labels.results[].prefix` | `Label.prefix` | yes |
| `labels.meta.hasMore` | `BlogPost.truncated_includes += INCLUDE_EXPANSION_LABELS` | added (proto + mapper) |
| `labels.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `labels._links.self` | - | dropped: hypermedia (`_links`) |
| `properties.results[]` | `BlogPost.properties` | yes |
| `properties.results[].id` | `ContentProperty.id` | yes |
| `properties.results[].key` | `ContentProperty.key` | yes |
| `properties.results[].value` | `ContentProperty.value` | yes - typed oneof by JSON kind (string/integer/bool/double); objects to Struct `json_value`; top-level arrays to `json_list_value` (added: they were silently emptied before); see open question 1 |
| `properties.results[].version` | `ContentProperty.version` | yes |
| `properties.results[].version.createdAt` | `Version.created_at` | yes |
| `properties.results[].version.message` | `Version.message` | yes |
| `properties.results[].version.number` | `Version.number` | yes |
| `properties.results[].version.minorEdit` | `Version.minor_edit` | yes |
| `properties.results[].version.authorId` | `Version.author_id` | yes |
| `properties.meta.hasMore` | `BlogPost.truncated_includes += INCLUDE_EXPANSION_PROPERTIES` | added (proto + mapper) |
| `properties.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `properties._links.self` | - | dropped: hypermedia (`_links`) |
| `operations.results[]` | `BlogPost.operations` | yes |
| `operations.results[].operation` | `Operation.operation` | yes |
| `operations.results[].targetType` | `Operation.target_type` | yes |
| `operations.meta.hasMore` | `BlogPost.truncated_includes += INCLUDE_EXPANSION_OPERATIONS` | added (proto + mapper) |
| `operations.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `operations._links.self` | - | dropped: hypermedia (`_links`) |
| `likes.results[]` | `BlogPost.likes` | yes |
| `likes.results[].accountId` | `Like.account_id` | yes |
| `likes.meta.hasMore` | `BlogPost.truncated_includes += INCLUDE_EXPANSION_LIKES` | added (proto + mapper) |
| `likes.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `likes._links.self` | - | dropped: hypermedia (`_links`) |
| `versions.results[]` | `BlogPost.versions` | yes |
| `versions.results[].createdAt` | `Version.created_at` | yes |
| `versions.results[].message` | `Version.message` | yes |
| `versions.results[].number` | `Version.number` | yes |
| `versions.results[].minorEdit` | `Version.minor_edit` | yes |
| `versions.results[].authorId` | `Version.author_id` | yes |
| `versions.meta.hasMore` | `BlogPost.truncated_includes += INCLUDE_EXPANSION_VERSIONS` | added (proto + mapper) |
| `versions.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `versions._links.self` | - | dropped: hypermedia (`_links`) |
| `isFavoritedByCurrentUser` | `BlogPost.is_favorited_by_current_user` | yes |
| `_links.base` | - | dropped: hypermedia (`_links`) |

### Attachment

Routes: GET /attachments, GET /pages/{id}/attachments, GET /blogposts/{id}/attachments, GET /custom-content/{id}/attachments, GET /labels/{id}/attachments, GET /attachments/{id}

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `Attachment.id` | yes |
| `status` | `Attachment.status` | yes |
| `title` | `Attachment.title` | yes |
| `createdAt` | `Attachment.created_at` | yes |
| `pageId` | `Attachment.page_id` | yes |
| `blogPostId` | `Attachment.blog_post_id` | yes |
| `customContentId` | `Attachment.custom_content_id` | yes |
| `mediaType` | `Attachment.media_type` | yes |
| `mediaTypeDescription` | `Attachment.media_type_description` | yes |
| `comment` | `Attachment.comment` | yes |
| `fileId` | `Attachment.file_id` | yes |
| `fileSize` | `Attachment.file_size` | yes |
| `webuiLink` | `Attachment.web_url` | yes - same URL as `_links.webui`; composed absolute |
| `downloadLink` | `Attachment.download_url` | yes - composed absolute |
| `version` | `Attachment.version` | yes |
| `version.createdAt` | `Version.created_at` | yes |
| `version.message` | `Version.message` | yes |
| `version.number` | `Version.number` | yes |
| `version.minorEdit` | `Version.minor_edit` | yes |
| `version.authorId` | `Version.author_id` | yes |
| `_links.webui` | `Attachment.web_url` | yes |
| `_links.download` | `Attachment.download_url` | yes |
| `labels.results[]` | `Attachment.labels` | yes |
| `labels.results[].id` | `Label.id` | yes |
| `labels.results[].name` | `Label.name` | yes |
| `labels.results[].prefix` | `Label.prefix` | yes |
| `labels.meta.hasMore` | `Attachment.truncated_includes += INCLUDE_EXPANSION_LABELS` | added (proto + mapper) |
| `labels.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `labels._links.self` | - | dropped: hypermedia (`_links`) |
| `properties.results[]` | `Attachment.properties` | yes |
| `properties.results[].id` | `ContentProperty.id` | yes |
| `properties.results[].key` | `ContentProperty.key` | yes |
| `properties.results[].value` | `ContentProperty.value` | yes - typed oneof by JSON kind (string/integer/bool/double); objects to Struct `json_value`; top-level arrays to `json_list_value` (added: they were silently emptied before); see open question 1 |
| `properties.results[].version` | `ContentProperty.version` | yes |
| `properties.results[].version.createdAt` | `Version.created_at` | yes |
| `properties.results[].version.message` | `Version.message` | yes |
| `properties.results[].version.number` | `Version.number` | yes |
| `properties.results[].version.minorEdit` | `Version.minor_edit` | yes |
| `properties.results[].version.authorId` | `Version.author_id` | yes |
| `properties.meta.hasMore` | `Attachment.truncated_includes += INCLUDE_EXPANSION_PROPERTIES` | added (proto + mapper) |
| `properties.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `properties._links.self` | - | dropped: hypermedia (`_links`) |
| `operations.results[]` | `Attachment.operations` | yes |
| `operations.results[].operation` | `Operation.operation` | yes |
| `operations.results[].targetType` | `Operation.target_type` | yes |
| `operations.meta.hasMore` | `Attachment.truncated_includes += INCLUDE_EXPANSION_OPERATIONS` | added (proto + mapper) |
| `operations.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `operations._links.self` | - | dropped: hypermedia (`_links`) |
| `versions.results[]` | `Attachment.versions` | yes |
| `versions.results[].createdAt` | `Version.created_at` | yes |
| `versions.results[].message` | `Version.message` | yes |
| `versions.results[].number` | `Version.number` | yes |
| `versions.results[].minorEdit` | `Version.minor_edit` | yes |
| `versions.results[].authorId` | `Version.author_id` | yes |
| `versions.meta.hasMore` | `Attachment.truncated_includes += INCLUDE_EXPANSION_VERSIONS` | added (proto + mapper) |
| `versions.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `versions._links.self` | - | dropped: hypermedia (`_links`) |
| `_links.base` | - | dropped: hypermedia (`_links`) |

### Comment (footer + inline)

Routes: GET /pages/{id}/footer-comments, GET /blogposts/{id}/footer-comments, GET /attachments/{id}/footer-comments, GET /custom-content/{id}/footer-comments, GET /pages/{id}/inline-comments, GET /blogposts/{id}/inline-comments, GET /footer-comments, GET /inline-comments, GET /footer-comments/{comment-id}, GET /inline-comments/{comment-id}, GET /footer-comments/{id}/children, GET /inline-comments/{id}/children

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `Comment.id` | yes |
| `status` | `Comment.status` | yes |
| `title` | `Comment.title` | yes |
| `pageId` | `Comment.page_id` | yes |
| `version` | `Comment.version` | yes |
| `version.createdAt` | `Version.created_at` | yes |
| `version.message` | `Version.message` | yes |
| `version.number` | `Version.number` | yes |
| `version.minorEdit` | `Version.minor_edit` | yes |
| `version.authorId` | `Version.author_id` | yes |
| `body` | `Comment.body` | yes |
| `body.storage` | `Body.storage` | yes |
| `body.storage.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.storage.value` | `BodyType.value` | yes |
| `body.atlas_doc_format` | `Body.atlas_doc_format` | yes |
| `body.atlas_doc_format.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.atlas_doc_format.value` | `BodyType.value` | yes |
| `_links.webui` | `Comment.web_url` | yes |
| `blogPostId` | `Comment.blog_post_id` | yes |
| `attachmentId` | `Comment.attachment_id` | yes |
| `body.view` | `Body.view` | yes |
| `body.view.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.view.value` | `BodyType.value` | yes |
| `customContentId` | `Comment.custom_content_id` | yes |
| `resolutionStatus` | `Comment.resolution_status` | yes |
| `properties` | `Comment.inline_properties` | yes |
| `properties.inlineMarkerRef` | `InlineCommentProperties.inline_marker_ref` | yes |
| `properties.inlineOriginalSelection` | `InlineCommentProperties.inline_original_selection` | yes |
| `parentCommentId` | `Comment.parent_comment_id` | yes |
| `properties.results[]` | `Comment.properties` | yes |
| `properties.results[].id` | `ContentProperty.id` | yes |
| `properties.results[].key` | `ContentProperty.key` | yes |
| `properties.results[].value` | `ContentProperty.value` | yes - typed oneof by JSON kind (string/integer/bool/double); objects to Struct `json_value`; top-level arrays to `json_list_value` (added: they were silently emptied before); see open question 1 |
| `properties.results[].version` | `ContentProperty.version` | yes |
| `properties.results[].version.createdAt` | `Version.created_at` | yes |
| `properties.results[].version.message` | `Version.message` | yes |
| `properties.results[].version.number` | `Version.number` | yes |
| `properties.results[].version.minorEdit` | `Version.minor_edit` | yes |
| `properties.results[].version.authorId` | `Version.author_id` | yes |
| `properties.meta.hasMore` | `Comment.truncated_includes += INCLUDE_EXPANSION_PROPERTIES` | added (proto + mapper) |
| `properties.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `properties._links.self` | - | dropped: hypermedia (`_links`) |
| `operations.results[]` | `Comment.operations` | yes |
| `operations.results[].operation` | `Operation.operation` | yes |
| `operations.results[].targetType` | `Operation.target_type` | yes |
| `operations.meta.hasMore` | `Comment.truncated_includes += INCLUDE_EXPANSION_OPERATIONS` | added (proto + mapper) |
| `operations.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `operations._links.self` | - | dropped: hypermedia (`_links`) |
| `likes.results[]` | `Comment.likes` | yes |
| `likes.results[].accountId` | `Like.account_id` | yes |
| `likes.meta.hasMore` | `Comment.truncated_includes += INCLUDE_EXPANSION_LIKES` | added (proto + mapper) |
| `likes.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `likes._links.self` | - | dropped: hypermedia (`_links`) |
| `versions.results[]` | `Comment.versions` | yes |
| `versions.results[].createdAt` | `Version.created_at` | yes |
| `versions.results[].message` | `Version.message` | yes |
| `versions.results[].number` | `Version.number` | yes |
| `versions.results[].minorEdit` | `Version.minor_edit` | yes |
| `versions.results[].authorId` | `Version.author_id` | yes |
| `versions.meta.hasMore` | `Comment.truncated_includes += INCLUDE_EXPANSION_VERSIONS` | added (proto + mapper) |
| `versions.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `versions._links.self` | - | dropped: hypermedia (`_links`) |
| `resolutionLastModifierId` | `Comment.resolution_last_modifier_id` | yes |
| `resolutionLastModifiedAt` | `Comment.resolution_last_modified_at` | yes |
| `_links.base` | - | dropped: hypermedia (`_links`) |

### CustomContent

Routes: GET /custom-content, GET /spaces/{id}/custom-content, GET /pages/{id}/custom-content, GET /blogposts/{id}/custom-content, GET /custom-content/{id}

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `CustomContent.id` | yes |
| `type` | `CustomContent.type` | yes |
| `status` | `CustomContent.status` | yes |
| `title` | `CustomContent.title` | yes |
| `spaceId` | `CustomContent.space_id` | yes |
| `pageId` | `CustomContent.page_id` | yes |
| `blogPostId` | `CustomContent.blog_post_id` | yes |
| `customContentId` | `CustomContent.custom_content_id` | yes |
| `authorId` | `CustomContent.author_id` | yes |
| `createdAt` | `CustomContent.created_at` | yes |
| `version` | `CustomContent.version` | yes |
| `version.createdAt` | `Version.created_at` | yes |
| `version.message` | `Version.message` | yes |
| `version.number` | `Version.number` | yes |
| `version.minorEdit` | `Version.minor_edit` | yes |
| `version.authorId` | `Version.author_id` | yes |
| `body` | `CustomContent.body` | yes |
| `body.raw` | `Body.raw` | yes |
| `body.raw.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.raw.value` | `BodyType.value` | yes |
| `body.storage` | `Body.storage` | yes |
| `body.storage.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.storage.value` | `BodyType.value` | yes |
| `body.atlas_doc_format` | `Body.atlas_doc_format` | yes |
| `body.atlas_doc_format.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.atlas_doc_format.value` | `BodyType.value` | yes |
| `_links.webui` | `CustomContent.web_url` | yes |
| `labels.results[]` | `CustomContent.labels` | yes |
| `labels.results[].id` | `Label.id` | yes |
| `labels.results[].name` | `Label.name` | yes |
| `labels.results[].prefix` | `Label.prefix` | yes |
| `labels.meta.hasMore` | `CustomContent.truncated_includes += INCLUDE_EXPANSION_LABELS` | added (proto + mapper) |
| `labels.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `labels._links.self` | - | dropped: hypermedia (`_links`) |
| `properties.results[]` | `CustomContent.properties` | yes |
| `properties.results[].id` | `ContentProperty.id` | yes |
| `properties.results[].key` | `ContentProperty.key` | yes |
| `properties.results[].value` | `ContentProperty.value` | yes - typed oneof by JSON kind (string/integer/bool/double); objects to Struct `json_value`; top-level arrays to `json_list_value` (added: they were silently emptied before); see open question 1 |
| `properties.results[].version` | `ContentProperty.version` | yes |
| `properties.results[].version.createdAt` | `Version.created_at` | yes |
| `properties.results[].version.message` | `Version.message` | yes |
| `properties.results[].version.number` | `Version.number` | yes |
| `properties.results[].version.minorEdit` | `Version.minor_edit` | yes |
| `properties.results[].version.authorId` | `Version.author_id` | yes |
| `properties.meta.hasMore` | `CustomContent.truncated_includes += INCLUDE_EXPANSION_PROPERTIES` | added (proto + mapper) |
| `properties.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `properties._links.self` | - | dropped: hypermedia (`_links`) |
| `operations.results[]` | `CustomContent.operations` | yes |
| `operations.results[].operation` | `Operation.operation` | yes |
| `operations.results[].targetType` | `Operation.target_type` | yes |
| `operations.meta.hasMore` | `CustomContent.truncated_includes += INCLUDE_EXPANSION_OPERATIONS` | added (proto + mapper) |
| `operations.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `operations._links.self` | - | dropped: hypermedia (`_links`) |
| `versions.results[]` | `CustomContent.versions` | yes |
| `versions.results[].createdAt` | `Version.created_at` | yes |
| `versions.results[].message` | `Version.message` | yes |
| `versions.results[].number` | `Version.number` | yes |
| `versions.results[].minorEdit` | `Version.minor_edit` | yes |
| `versions.results[].authorId` | `Version.author_id` | yes |
| `versions.meta.hasMore` | `CustomContent.truncated_includes += INCLUDE_EXPANSION_VERSIONS` | added (proto + mapper) |
| `versions.meta.cursor` | - | dropped: pagination transport (`meta.cursor`) |
| `versions._links.self` | - | dropped: hypermedia (`_links`) |
| `body.view` | `Body.view` | yes |
| `body.view.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.view.value` | `BodyType.value` | yes |
| `_links.base` | - | dropped: hypermedia (`_links`) |

### Whiteboard

Routes: GET /whiteboards/{id}

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `Whiteboard.id` | yes |
| `type` | `Whiteboard.type` | yes |
| `status` | `Whiteboard.status` | yes |
| `title` | `Whiteboard.title` | yes |
| `parentId` | `Whiteboard.parent_id` | yes |
| `parentType` | `Whiteboard.parent_type` | yes |
| `position` | `Whiteboard.position` | yes |
| `authorId` | `Whiteboard.author_id` | yes |
| `ownerId` | `Whiteboard.owner_id` | yes |
| `createdAt` | `Whiteboard.created_at` | yes |
| `spaceId` | `Whiteboard.space_id` | yes |
| `version` | `Whiteboard.version` | yes |
| `version.createdAt` | `Version.created_at` | yes |
| `version.message` | `Version.message` | yes |
| `version.number` | `Version.number` | yes |
| `version.minorEdit` | `Version.minor_edit` | yes |
| `version.authorId` | `Version.author_id` | yes |
| `_links.base` | - | dropped: hypermedia (`_links`) |

### Database

Routes: GET /databases/{id}

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `Database.id` | yes |
| `type` | `Database.type` | yes |
| `status` | `Database.status` | yes |
| `title` | `Database.title` | yes |
| `parentId` | `Database.parent_id` | yes |
| `parentType` | `Database.parent_type` | yes |
| `position` | `Database.position` | yes |
| `authorId` | `Database.author_id` | yes |
| `ownerId` | `Database.owner_id` | yes |
| `createdAt` | `Database.created_at` | yes |
| `spaceId` | `Database.space_id` | yes |
| `version` | `Database.version` | yes |
| `version.createdAt` | `Version.created_at` | yes |
| `version.message` | `Version.message` | yes |
| `version.number` | `Version.number` | yes |
| `version.minorEdit` | `Version.minor_edit` | yes |
| `version.authorId` | `Version.author_id` | yes |
| `_links.base` | - | dropped: hypermedia (`_links`) |

### Folder

Routes: GET /folders/{id}

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `Folder.id` | yes |
| `type` | `Folder.type` | yes |
| `status` | `Folder.status` | yes |
| `title` | `Folder.title` | yes |
| `parentId` | `Folder.parent_id` | yes |
| `parentType` | `Folder.parent_type` | yes |
| `position` | `Folder.position` | yes |
| `authorId` | `Folder.author_id` | yes |
| `ownerId` | `Folder.owner_id` | yes |
| `createdAt` | `Folder.created_at` | yes |
| `spaceId` | `Folder.space_id` | yes |
| `version` | `Folder.version` | yes |
| `version.createdAt` | `Version.created_at` | yes |
| `version.message` | `Version.message` | yes |
| `version.number` | `Version.number` | yes |
| `version.minorEdit` | `Version.minor_edit` | yes |
| `version.authorId` | `Version.author_id` | yes |
| `_links.base` | - | dropped: hypermedia (`_links`) |

### SmartLink (embed)

Routes: GET /embeds/{id}

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `SmartLink.id` | yes |
| `type` | `SmartLink.type` | yes |
| `status` | `SmartLink.status` | yes |
| `title` | `SmartLink.title` | yes |
| `parentId` | `SmartLink.parent_id` | yes |
| `parentType` | `SmartLink.parent_type` | yes |
| `position` | `SmartLink.position` | yes |
| `authorId` | `SmartLink.author_id` | yes |
| `ownerId` | `SmartLink.owner_id` | yes |
| `createdAt` | `SmartLink.created_at` | yes |
| `embedUrl` | `SmartLink.embed_url` | yes |
| `spaceId` | `SmartLink.space_id` | yes |
| `version` | `SmartLink.version` | yes |
| `version.createdAt` | `Version.created_at` | yes |
| `version.message` | `Version.message` | yes |
| `version.number` | `Version.number` | yes |
| `version.minorEdit` | `Version.minor_edit` | yes |
| `version.authorId` | `Version.author_id` | yes |
| `_links.base` | - | dropped: hypermedia (`_links`) |

### ContentTreeEntry (children / descendants)

Routes: GET /pages/{id}/direct-children, GET /whiteboards/{id}/direct-children, GET /databases/{id}/direct-children, GET /embeds/{id}/direct-children, GET /folders/{id}/direct-children, GET /pages/{id}/descendants, GET /whiteboards/{id}/descendants, GET /databases/{id}/descendants, GET /embeds/{id}/descendants, GET /folders/{id}/descendants, GET /custom-content/{id}/children

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `ContentTreeEntry.id` | yes |
| `status` | `ContentTreeEntry.status` | yes |
| `title` | `ContentTreeEntry.title` | yes |
| `type` | `ContentTreeEntry.type` | yes |
| `spaceId` | `ContentTreeEntry.space_id` | yes |
| `childPosition` | `ContentTreeEntry.child_position` | yes |
| `parentId` | `ContentTreeEntry.parent_id` | yes |
| `depth` | `ContentTreeEntry.depth` | yes |

### Ancestor

Routes: GET /pages/{id}/ancestors, GET /whiteboards/{id}/ancestors, GET /databases/{id}/ancestors, GET /embeds/{id}/ancestors, GET /folders/{id}/ancestors

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `Ancestor.id` | yes |
| `type` | `Ancestor.type` | yes |

### Label

Routes: GET /labels, GET /spaces/{id}/labels, GET /spaces/{id}/content/labels, GET /pages/{id}/labels, GET /blogposts/{id}/labels, GET /attachments/{id}/labels, GET /custom-content/{id}/labels

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `Label.id` | yes |
| `name` | `Label.name` | yes |
| `prefix` | `Label.prefix` | yes |

### Version (history entries)

Routes: GET /pages/{id}/versions, GET /blogposts/{id}/versions, GET /attachments/{id}/versions, GET /footer-comments/{id}/versions, GET /inline-comments/{id}/versions, GET /custom-content/{custom-content-id}/versions

| Spec property | Proto field | Mapped? |
|---|---|---|
| `createdAt` | `Version.created_at` | yes |
| `message` | `Version.message` | yes |
| `number` | `Version.number` | yes |
| `minorEdit` | `Version.minor_edit` | yes |
| `authorId` | `Version.author_id` | yes |
| `page` | `Version.entity` | added (mapper) |
| `page.title` | `VersionedEntity.title` | added (mapper) |
| `page.id` | `VersionedEntity.id` | added (mapper) |
| `page.body` | `VersionedEntity.body` | added (mapper) |
| `page.body.storage` | `Body.storage` | added (mapper) |
| `page.body.storage.representation` | `BodyType.format` | added (mapper) - representation string to the BodyFormat enum (bodyFormat()) |
| `page.body.storage.value` | `BodyType.value` | added (mapper) |
| `page.body.atlas_doc_format` | `Body.atlas_doc_format` | added (mapper) |
| `page.body.atlas_doc_format.representation` | `BodyType.format` | added (mapper) - representation string to the BodyFormat enum (bodyFormat()) |
| `page.body.atlas_doc_format.value` | `BodyType.value` | added (mapper) |
| `blogpost` | `Version.entity` | added (mapper) |
| `blogpost.title` | `VersionedEntity.title` | added (mapper) |
| `blogpost.id` | `VersionedEntity.id` | added (mapper) |
| `blogpost.body` | `VersionedEntity.body` | added (mapper) |
| `blogpost.body.storage` | `Body.storage` | added (mapper) |
| `blogpost.body.storage.representation` | `BodyType.format` | added (mapper) - representation string to the BodyFormat enum (bodyFormat()) |
| `blogpost.body.storage.value` | `BodyType.value` | added (mapper) |
| `blogpost.body.atlas_doc_format` | `Body.atlas_doc_format` | added (mapper) |
| `blogpost.body.atlas_doc_format.representation` | `BodyType.format` | added (mapper) - representation string to the BodyFormat enum (bodyFormat()) |
| `blogpost.body.atlas_doc_format.value` | `BodyType.value` | added (mapper) |
| `attachment` | `Version.entity` | added (mapper) |
| `attachment.title` | `VersionedEntity.title` | added (mapper) |
| `attachment.id` | `VersionedEntity.id` | added (mapper) |
| `attachment.body` | `VersionedEntity.body` | added (mapper) |
| `attachment.body.storage` | `Body.storage` | added (mapper) |
| `attachment.body.storage.representation` | `BodyType.format` | added (mapper) - representation string to the BodyFormat enum (bodyFormat()) |
| `attachment.body.storage.value` | `BodyType.value` | added (mapper) |
| `attachment.body.atlas_doc_format` | `Body.atlas_doc_format` | added (mapper) |
| `attachment.body.atlas_doc_format.representation` | `BodyType.format` | added (mapper) - representation string to the BodyFormat enum (bodyFormat()) |
| `attachment.body.atlas_doc_format.value` | `BodyType.value` | added (mapper) |
| `comment` | `Version.entity` | added (mapper) |
| `comment.title` | `VersionedEntity.title` | added (mapper) |
| `comment.id` | `VersionedEntity.id` | added (mapper) |
| `comment.body` | `VersionedEntity.body` | added (mapper) |
| `comment.body.storage` | `Body.storage` | added (mapper) |
| `comment.body.storage.representation` | `BodyType.format` | added (mapper) - representation string to the BodyFormat enum (bodyFormat()) |
| `comment.body.storage.value` | `BodyType.value` | added (mapper) |
| `comment.body.atlas_doc_format` | `Body.atlas_doc_format` | added (mapper) |
| `comment.body.atlas_doc_format.representation` | `BodyType.format` | added (mapper) - representation string to the BodyFormat enum (bodyFormat()) |
| `comment.body.atlas_doc_format.value` | `BodyType.value` | added (mapper) |
| `custom` | `Version.entity` | added (mapper) |
| `custom.title` | `VersionedEntity.title` | added (mapper) |
| `custom.id` | `VersionedEntity.id` | added (mapper) |
| `custom.body` | `VersionedEntity.body` | added (mapper) |
| `custom.body.storage` | `Body.storage` | added (mapper) |
| `custom.body.storage.representation` | `BodyType.format` | added (mapper) - representation string to the BodyFormat enum (bodyFormat()) |
| `custom.body.storage.value` | `BodyType.value` | added (mapper) |
| `custom.body.atlas_doc_format` | `Body.atlas_doc_format` | added (mapper) |
| `custom.body.atlas_doc_format.representation` | `BodyType.format` | added (mapper) - representation string to the BodyFormat enum (bodyFormat()) |
| `custom.body.atlas_doc_format.value` | `BodyType.value` | added (mapper) |

### ContentProperty

Routes: GET /pages/{page-id}/properties, GET /blogposts/{blogpost-id}/properties, GET /attachments/{attachment-id}/properties, GET /custom-content/{custom-content-id}/properties, GET /comments/{comment-id}/properties, GET /whiteboards/{id}/properties, GET /databases/{id}/properties, GET /embeds/{id}/properties, GET /folders/{id}/properties

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `ContentProperty.id` | yes |
| `key` | `ContentProperty.key` | yes |
| `value` | `ContentProperty.value` | yes - typed oneof by JSON kind (string/integer/bool/double); objects to Struct `json_value`; top-level arrays to `json_list_value` (added: they were silently emptied before); see open question 1 |
| `version` | `ContentProperty.version` | yes |
| `version.createdAt` | `Version.created_at` | yes |
| `version.message` | `Version.message` | yes |
| `version.number` | `Version.number` | yes |
| `version.minorEdit` | `Version.minor_edit` | yes |
| `version.authorId` | `Version.author_id` | yes |

### SpaceProperty

Routes: GET /spaces/{space-id}/properties

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `SpaceProperty.id` | yes |
| `key` | `SpaceProperty.key` | yes |
| `value` | `SpaceProperty.value` | yes - typed oneof by JSON kind (string/integer/bool/double); objects to Struct `json_value`; top-level arrays to `json_list_value` (added: they were silently emptied before); see open question 1 |
| `createdAt` | `SpaceProperty.created_at` | yes |
| `createdBy` | `SpaceProperty.created_by` | yes |
| `version` | `SpaceProperty.version` | yes |
| `version.createdAt` | `SpacePropertyVersion.created_at` | yes |
| `version.createdBy` | `SpacePropertyVersion.created_by` | yes |
| `version.message` | `SpacePropertyVersion.message` | yes |
| `version.number` | `SpacePropertyVersion.number` | yes |

### Task

Routes: GET /tasks, GET /tasks/{id}

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `Task.id` | yes |
| `localId` | `Task.local_id` | yes |
| `spaceId` | `Task.space_id` | yes |
| `pageId` | `Task.page_id` | yes |
| `blogPostId` | `Task.blog_post_id` | yes |
| `status` | `Task.status` | yes |
| `body` | `Task.body` | yes |
| `body.storage` | `Body.storage` | yes |
| `body.storage.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.storage.value` | `BodyType.value` | yes |
| `body.atlas_doc_format` | `Body.atlas_doc_format` | yes |
| `body.atlas_doc_format.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `body.atlas_doc_format.value` | `BodyType.value` | yes |
| `createdBy` | `Task.created_by` | yes |
| `assignedTo` | `Task.assigned_to` | yes |
| `completedBy` | `Task.completed_by` | yes |
| `createdAt` | `Task.created_at` | yes |
| `updatedAt` | `Task.updated_at` | yes |
| `dueAt` | `Task.due_at` | yes |
| `completedAt` | `Task.completed_at` | yes |

### User

Routes: POST /users-bulk

| Spec property | Proto field | Mapped? |
|---|---|---|
| `displayName` | `User.display_name` | yes |
| `timeZone` | `User.time_zone` | yes |
| `personalSpaceId` | `User.personal_space_id` | yes |
| `isExternalCollaborator` | `User.is_external_collaborator` | yes |
| `accountStatus` | `User.account_status` | yes |
| `accountId` | `User.account_id` | yes |
| `email` | `User.email` | yes |
| `accountType` | `User.account_type` | yes |
| `publicName` | `User.public_name` | yes |
| `profilePicture` | `User.profile_picture` | yes |
| `profilePicture.path` | `Icon.path` | yes |
| `profilePicture.isDefault` | `Icon.is_default` | yes |

### ClassificationLevel

Routes: GET /classification-levels, GET /pages/{id}/classification-level, GET /spaces/{id}/classification-level/default

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `ClassificationLevel.id` | yes |
| `status` | `ClassificationLevel.status` | yes |
| `order` | `ClassificationLevel.order` | yes |
| `name` | `ClassificationLevel.name` | yes |
| `description` | `ClassificationLevel.description` | yes |
| `guideline` | `ClassificationLevel.guideline` | yes |
| `color` | `ClassificationLevel.color` | yes |

### DataPolicy (space)

Routes: GET /data-policies/spaces

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `DataPolicy.id` | yes |
| `key` | `DataPolicy.key` | yes |
| `name` | `DataPolicy.name` | yes |
| `description` | `DataPolicy.description` | yes |
| `description.plain` | `SpaceDescription.plain` | yes |
| `description.plain.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `description.plain.value` | `BodyType.value` | yes |
| `description.view` | `SpaceDescription.view` | yes |
| `description.view.representation` | `BodyType.format` | yes - representation string to the BodyFormat enum (bodyFormat()) |
| `description.view.value` | `BodyType.value` | yes |
| `dataPolicy.anyContentBlocked` | `DataPolicy.any_content_blocked` | yes |
| `icon` | `DataPolicy.icon` | yes |
| `icon.path` | `SpaceIcon.path` | yes |
| `icon.apiDownloadLink` | `SpaceIcon.api_download_url` | yes |
| `_links.webui` | `DataPolicy.web_url` | yes |

### DataPolicyMetadata

Routes: GET /data-policies/metadata

| Spec property | Proto field | Mapped? |
|---|---|---|
| `anyContentBlocked` | `DataPolicyMetadata.any_content_blocked` | yes |

### RestrictionState

Routes: GET /pages/{id}/restriction-state, GET /blogposts/{id}/restriction-state, GET /whiteboards/{id}/restriction-state, GET /databases/{id}/restriction-state, GET /folders/{id}/restriction-state

| Spec property | Proto field | Mapped? |
|---|---|---|
| `state` | `RestrictionState.state` | yes |
| `createdAt` | `RestrictionState.created_at` | yes |
| `createdBy` | `RestrictionState.created_by` | yes |
| `createdByAaid` | `RestrictionState.created_by` | yes - deprecated alias of `createdBy`; folded into `created_by` (createdBy wins) |

### ContentPermission

Routes: GET /pages/{id}/permissions, GET /blogposts/{id}/permissions, GET /whiteboards/{id}/permissions, GET /databases/{id}/permissions, GET /folders/{id}/permissions

| Spec property | Proto field | Mapped? |
|---|---|---|
| `operation` | `ContentPermission.operation` | yes |
| `principal` | `ContentPermission.principal` | yes |
| `principal.principalType` | `ContentPermissionPrincipal.principal_type` | yes |
| `principal.principalId` | `ContentPermissionPrincipal.principal_id` | yes |
| `createdAt` | `ContentPermission.created_at` | yes |
| `createdBy` | `ContentPermission.created_by` | yes |
| `createdByAaid` | `ContentPermission.created_by` | yes - deprecated alias of `createdBy`; folded into `created_by` (createdBy wins) |

### SpacePermissionAssignment

Routes: GET /spaces/{id}/permissions

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `SpacePermissionAssignment.id` | yes |
| `principal` | `SpacePermissionAssignment.principal` | yes |
| `principal.type` | `SpacePermissionPrincipal.type` | yes |
| `principal.id` | `SpacePermissionPrincipal.id` | yes |
| `operation` | `SpacePermissionAssignment.operation` | yes |
| `operation.key` | `SpacePermissionOperation.key` | yes |
| `operation.targetType` | `SpacePermissionOperation.target_type` | yes |

### SpaceRole

Routes: GET /space-roles

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `SpaceRole.id` | yes |
| `type` | `SpaceRole.type` | yes |
| `name` | `SpaceRole.name` | yes |
| `description` | `SpaceRole.description` | yes |
| `spacePermissions` | `SpaceRole.space_permissions` | yes |

### SpaceRoleAssignment

Routes: GET /spaces/{id}/role-assignments

| Spec property | Proto field | Mapped? |
|---|---|---|
| `principal` | `SpaceRoleAssignment.principal` | yes |
| `principal.principalType` | `Principal.principal_type` | yes |
| `principal.principalId` | `Principal.principal_id` | yes |
| `roleId` | `SpaceRoleAssignment.role_id` | yes |

### Space role mode

Routes: GET /space-role-mode

| Spec property | Proto field | Mapped? |
|---|---|---|
| `mode` | `GetSpaceRoleModeResponse.mode` | yes |

### DatabaseView

Routes: GET /databases/{id}/views

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `DatabaseView.id` | added (mapper) - toDatabaseView |
| `name` | `DatabaseView.name` | added (mapper) - toDatabaseView |
| `type` | `DatabaseView.type` | added (mapper) - toDatabaseView |

### DatabaseField

Routes: GET /databases/{id}/fields

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `DatabaseField.id` | yes |
| `name` | `DatabaseField.name` | yes |
| `description` | `DatabaseField.description` | yes |
| `type` | `DatabaseField.type` | yes |
| `settings (type=text)` | `DatabaseField.text` | yes |
| `settings (type=text).richText` | `DatabaseTextFieldSettings.rich_text` | yes |
| `settings (type=aiSummary)` | `DatabaseField.ai_summary` | yes |
| `settings (type=aiSummary).aiRunMetadata.lastAiRunUserAccountId` | `DatabaseAiSummaryFieldSettings.last_ai_run_user_account_id` | yes |
| `settings (type=number)` | `DatabaseField.number` | yes |
| `settings (type=number).format` | `DatabaseNumberFieldSettings.format` | yes |
| `settings (type=number).format.type` | `DatabaseNumberFormat.type` | yes |
| `settings (type=number).format.prefix` | `DatabaseNumberFormat.prefix` | yes |
| `settings (type=number).format.suffix` | `DatabaseNumberFormat.suffix` | yes |
| `settings (type=number).format.decimalPlaces` | `DatabaseNumberFormat.decimal_places` | yes |
| `settings (type=checkbox)` | - | dropped: empty settings object (DatabaseEmptyFieldSettings): nothing to carry |
| `settings (type=date)` | `DatabaseField.date` | yes |
| `settings (type=date).includesTime` | `DatabaseDateFieldSettings.includes_time` | yes |
| `settings (type=select)` | `DatabaseField.select` | yes |
| `settings (type=select).options` | `DatabaseSelectFieldSettings.options` | yes |
| `settings (type=select).options[].id` | `DatabaseSelectOption.id` | yes |
| `settings (type=select).options[].label` | `DatabaseSelectOption.label` | yes |
| `settings (type=hyperlink)` | - | dropped: empty settings object (DatabaseEmptyFieldSettings): nothing to carry |
| `settings (type=user)` | - | dropped: empty settings object (DatabaseEmptyFieldSettings): nothing to carry |
| `settings (type=attachment)` | - | dropped: empty settings object (DatabaseEmptyFieldSettings): nothing to carry |
| `settings (type=jiraIssue)` | `DatabaseField.jira_issue` | yes |
| `settings (type=jiraIssue).cloudId` | `DatabaseJiraIssueFieldSettings.cloud_id` | yes |
| `settings (type=jiraIssue).serverId` | `DatabaseJiraIssueFieldSettings.server_id` | yes |
| `settings (type=pageLink)` | - | dropped: empty settings object (DatabaseEmptyFieldSettings): nothing to carry |
| `settings (type=entryLink)` | - | dropped: empty settings object (DatabaseEmptyFieldSettings): nothing to carry |
| `settings (type=entryDetails)` | `DatabaseField.entry_details` | yes |
| `settings (type=entryDetails).sourceFieldId` | `DatabaseEntryDetailsFieldSettings.source_field_id` | yes |
| `settings (type=entryDetails).lookupFieldId` | `DatabaseEntryDetailsFieldSettings.lookup_field_id` | yes |
| `settings (type=backlink)` | `DatabaseField.backlink` | yes |
| `settings (type=backlink).sourceDatabaseContentId` | `DatabaseBacklinkFieldSettings.source_database_content_id` | yes |
| `settings (type=pageDetails)` | `DatabaseField.page_details` | yes |
| `settings (type=pageDetails).sourceFieldId` | `DatabasePageDetailsFieldSettings.source_field_id` | yes |
| `settings (type=pageDetails).pagePropertyKey` | `DatabasePageDetailsFieldSettings.page_property_key` | yes |
| `settings (type=pageLabels)` | `DatabaseField.source` | yes |
| `settings (type=pageLabels).sourceFieldId` | `DatabaseSourceFieldSettings.source_field_id` | yes |
| `settings (type=pageStatus)` | `DatabaseField.source` | yes |
| `settings (type=pageStatus).sourceFieldId` | `DatabaseSourceFieldSettings.source_field_id` | yes |
| `settings (type=jiraIssueDetails)` | `DatabaseField.jira_issue_details` | yes |
| `settings (type=jiraIssueDetails).sourceFieldId` | `DatabaseJiraIssueDetailsFieldSettings.source_field_id` | yes |
| `settings (type=jiraIssueDetails).jiraFieldKey` | `DatabaseJiraIssueDetailsFieldSettings.jira_field_key` | yes |

### DatabaseEntry

Routes: GET /databases/{id}/entries, GET /databases/{id}/entries/{entryId}

| Spec property | Proto field | Mapped? |
|---|---|---|
| `id` | `DatabaseEntry.id` | yes |
| `databaseId` | `DatabaseEntry.database_id` | yes |
| `values{}` | `DatabaseEntry.values (map<field id, DatabaseFieldValue>)` | yes |
| `type` | `DatabaseFieldValue.type` | yes |
| `plain (type=text)` | `DatabaseFieldValue.text.plain` | yes |
| `rich (type=text)` | `DatabaseFieldValue.text.rich.value` | yes - the ADF document (version/type/content) is kept whole as serialized JSON in format ATLAS_DOC_FORMAT |
| `plain (type=aiSummary)` | `DatabaseFieldValue.ai_summary.plain` | yes |
| `rich (type=aiSummary)` | `DatabaseFieldValue.ai_summary.rich.value` | yes - the ADF document (version/type/content) is kept whole as serialized JSON in format ATLAS_DOC_FORMAT |
| `metadata (type=aiSummary)` | `DatabaseFieldValue.ai_summary.summary_metadata` | yes |
| `metadata (type=aiSummary)[].lastUpdatedAt` | `DatabaseAiSummaryValueMetadata.last_updated_at` | yes |
| `metadata (type=aiSummary)[].lastUpdatedBy` | `DatabaseAiSummaryValueMetadata.last_updated_by` | yes |
| `metadata (type=aiSummary)[].lastUpdatedUserAccountId` | `DatabaseAiSummaryValueMetadata.last_updated_user_account_id` | yes |
| `value (type=number)` | `DatabaseFieldValue.number` | yes |
| `value (type=checkbox)` | `DatabaseFieldValue.checkbox` | yes |
| `value (type=date)` | `DatabaseFieldValue.date` | yes |
| `value (type=select)` | `DatabaseFieldValue.select.options` | yes |
| `value (type=select)[].id` | `DatabaseSelectOption.id` | yes |
| `value (type=select)[].label` | `DatabaseSelectOption.label` | yes |
| `value (type=hyperlink)` | `DatabaseFieldValue.hyperlink.items` | yes |
| `value (type=hyperlink)[].url` | `DatabaseHyperlinkItem.url` | yes |
| `value (type=hyperlink)[].label` | `DatabaseHyperlinkItem.label` | yes |
| `value (type=user)` | `DatabaseFieldValue.user.account_ids` | yes |
| `value (type=user)[].accountId` | `DatabaseUserValue.account_ids` | yes |
| `value (type=attachment)` | `DatabaseFieldValue.attachment.media_ids` | yes |
| `value (type=attachment)[].mediaId` | `DatabaseAttachmentValue.media_ids` | yes |
| `value (type=jiraIssue)` | `DatabaseFieldValue.jira_issue.issues` | yes |
| `value (type=jiraIssue)[].key` | `DatabaseJiraIssueRef.key` | yes |
| `value (type=jiraIssue)[].cloudId` | `DatabaseJiraIssueRef.cloud_id` | yes |
| `value (type=jiraIssue)[].serverId` | `DatabaseJiraIssueRef.server_id` | yes |
| `value (type=pageLink)` | `DatabaseFieldValue.page_link.content_ids` | yes |
| `value (type=pageLink)[].contentId` | `DatabasePageLinkValue.content_ids` | yes |
| `value (type=entryLink)` | `DatabaseFieldValue.entry_link.entries` | yes |
| `value (type=entryLink)[].entryId` | `DatabaseEntryRef.entry_id` | yes |
| `value (type=entryLink)[].databaseContentId` | `DatabaseEntryRef.database_content_id` | yes |
| `value (type=entryDetails)` | - | dropped: lookup field: the spec fixes `value` to null on read; `type` is kept |
| `value (type=backlink)` | - | dropped: lookup field: the spec fixes `value` to null on read; `type` is kept |
| `value (type=pageDetails)` | - | dropped: lookup field: the spec fixes `value` to null on read; `type` is kept |
| `value (type=pageLabels)` | - | dropped: lookup field: the spec fixes `value` to null on read; `type` is kept |
| `value (type=pageStatus)` | - | dropped: lookup field: the spec fixes `value` to null on read; `type` is kept |
| `value (type=jiraIssueDetails)` | - | dropped: lookup field: the spec fixes `value` to null on read; `type` is kept |

### ResolvedContentType

Routes: POST /content/convert-ids-to-types

| Spec property | Proto field | Mapped? |
|---|---|---|
| `results{id}` | `ResolvedContentType.id` | yes |
| `results{id} (built-in value)` | `ResolvedContentType.type (ContentType)` | added (mapper) - toResolvedContentTypes |
| `results{id} (custom value)` | `ResolvedContentType.custom_type` | added (mapper) - toResolvedContentTypes |

Totals across all tables: added (mapper): 55, added (proto + mapper): 26, dropped: 78, yes: 563; gaps: 0.
