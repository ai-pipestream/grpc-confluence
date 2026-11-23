# AGENTS.md: grpc-confluence

Read the workspace guide first:
[grpc-integration/AGENTS.md](https://github.com/ai-pipestream/grpc-integration/blob/main/AGENTS.md).
It has the repository map, where the open work lives, the standing rules,
the testing model and what to focus a review on. This file adds what is
specific to the Confluence connector.

## Which branch this describes

On `main` this repository is the read side only. Writes, the recordings,
ACL data and the change stream are on open pull requests (#25 writes,
#28 ACL, #29 changes; see the workspace guide). The sections below describe
the write branch `claude/project-thread-i7updt` unless they say "ACL
branch" or "change branch".

## Build

Check out [grpc-integration](https://github.com/ai-pipestream/grpc-integration)
next to this repository (same branch name when it exists), then:

```
./gradlew build      # unit, fake-server and replay tests; javadoc; never calls Atlassian
buf lint             # STANDARD + COMMENTS on grpc-confluence-api, buf 1.54.0
```

Java 25. Any compiler, javadoc or Gradle deprecation warning fails the
build. Do not change how tests are run.

## Modules

| Module | What |
|---|---|
| `grpc-confluence-api` | `ai.pipestream.confluence.v1`. `confluence_service.proto` is the service; `write.proto` the write messages; `adf.proto` the typed ADF document model; `property.proto` typed property values (no `Struct`); `permission.proto` and, on the ACL branch, `access.proto`; on the change branch, `change_feed.proto` and `events.proto`. |
| `grpc-confluence-service` | The process (port 9095). `ConfluenceGrpcService` (rpc handlers), `ConfluenceClient` (REST, retries), `ConfluenceRoutes`, `ConfluenceMapper` (JSON to proto), `ConfluenceWriter` and `AdfCodec` (proto to JSON), `ConfluenceValidator`, `ConfluenceCrawler` (`Sync`), the change sinks (`SyncTableChangeSink`, `KafkaChangeSink`, `OutputChangeSink`, `OkfChangeSink`). On the change branch: `ConfluenceChangeGrpcService`, `ForgeEventQueue`. |
| `grpc-confluence-forge` | (change branch) Forge app with no UI: product events into Forge KVS, `event-queue` and `app-api` web triggers behind a shared-secret header. `npm test`. |
| `grpc-confluence-output` | Store keys and OKF catalog mapping (`OkfCatalogSource`). |
| `grpc-confluence-mcp` | `confluence_*` tools for the combined MCP server. |
| `grpc-confluence-connect` | Kafka Connect source. |

## Contract references

- `docs/upstream/atlassian/confluence-v2.json`: the vendored Confluence
  Cloud REST v2 spec. `adf-schema-full.json`: the ADF schema.
- `docs/contracts/confluence-gap-analysis.md`: rpc to route map. Its
  "Decisions" section is the owner's ruling and wins over earlier sections.
- `docs/contracts/confluence-fidelity.md`: field-by-field check that every
  response property reaches a typed field.
- v1 routes are used only where v2 has none (groups, attachments upload,
  labels, `permission/check`, space delete).

## Tests

| Suite | Live task (opt-in) | Replay in `test` | Recordings |
|---|---|---|---|
| `WriteScenarios` | `liveWriteTest` (`CONFLUENCE_LIVE_WRITE=1`) | `ConfluenceWriteReplayTest` | `recordings/confluence/` |
| app properties | `liveAppPropertiesTest` | `ConfluenceWriteReplayTest` | `recordings/confluence-app-properties/` |
| `AclScenarios` (ACL branch) | `liveAclTest` | `ConfluenceAclReplayTest` | `recordings/confluence-acl/` |
| `ChangeScenarios` (change branch) | `liveChangesTest` | `ConfluenceChangeReplayTest` | `recordings/confluence-changes/` |
| read coverage | `liveCoverageTest` | none | report only |
| smoke | `liveSmokeTest` | none | none |

Recording and scrubbing rules:
`grpc-confluence-service/src/test/resources/recordings/README.md`.
`RecordingsGuardTest` checks every committed file in every build.

Live runs go through `.github/workflows/ci.yml` `workflow_dispatch` inputs
(`run_live_write`, `import_recordings_run_id`, and on the stacked branches
`run_live_acl`, `run_live_changes`). Reviewers do not dispatch them.

## Confluence specifics to keep in mind

- Live behaviour found so far (each has a test or a mapping): the first
  DELETE of a blog post after an update can answer 500 and succeed on retry;
  decimals in property values are stored as doubles, so `decimal_value` is
  refused on writes; a property over the size limit answers 400; one byte
  over the site attachment limit answers a bare 500, mapped to `INTERNAL`
  naming `CONFLUENCE_MAX_ATTACHMENT_BYTES`; attachment, property and label
  writes do not bump the page version; a v2 move fires `updated:page`;
  restrictions that drop the caller's own edit access are refused
  (allow-self-lockout); live sites return `access-class` principals the v2
  spec does not list; a missing group id answers 400, not 404.
- App property rpcs are served to Forge apps only, so they go through the
  Forge `app-api` trigger (`CONFLUENCE_FORGE_APP_URL` /
  `CONFLUENCE_FORGE_APP_SECRET`).
- Data policy rpcs answer `PERMISSION_DENIED` with API-token auth; upstream
  serves them to app credentials only.
- Not proven live: classification levels and Guard Premium redaction (the
  owner chose not to start a Guard Premium trial), custom content without a
  configured type.

## Review hot spots

- `ConfluenceCrawler`: cursor choice, subtree errors, ACL chain reads,
  `Reconcile` after a full sweep (issues #35, #38, #39, #40).
- `ConfluenceClient`: paging on 3xx and empty bodies (#43), retry rules.
- `ConfluenceGrpcService` request validation against what the response type
  can hold (#41).
- `ForgeEventQueue` and `ConfluenceChangeGrpcService`: ack only after
  emit, coalescing, events Forge never sends (#31, #32).
- `RecordingScrubber`: anything that could leak tenant data.
