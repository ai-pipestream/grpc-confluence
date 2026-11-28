# grpc-confluence

Licensed under Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

A gRPC version of the Confluence API. The proxy speaks Confluence Cloud
REST v2 upstream and typed gRPC downstream: clients read and write spaces,
pages, blog posts, comments, attachments, labels, and properties over one
generated contract (create, update, delete, move, and restore), pick up
**automatic updates** through cursor-based incremental `Sync` streams, and
**archive the site live** — every completed crawl can land as an Open
Knowledge Format bundle with a sibling WARC 1.1 archive, a raw protobuf
Kafka change stream, and a queryable sync ledger.

The gRPC interface was taken from the latest Confluence API spec and designed
to have all of the same features. So there's no data loss: if the API sends it,
gRPC also captures it.

Attachments are supported and linked, and `GetAttachment` can inline the
bytes (no size cap by default, redirect to the media CDN followed server-side).

An **MCP endpoint** (Streamable HTTP, official MCP Java SDK) exposes the
same surface as tools so an LLM can set up connections, configure output,
and run syncs at runtime.

Runnable client examples in Python, Go, and Java live under
[examples/](examples/).

The shared, source-neutral pieces (sync ledger and connection catalog,
output SPI with filesystem and S3 stores, the OKF / WARC writer, the
combined MCP server, Kafka Connect base classes) live in
[grpc-integration](https://github.com/ai-pipestream/grpc-integration);
see its `docs/architecture.md` for process topology. The Microsoft Graph
connector lives in
[grpc-sharepoint](https://github.com/ai-pipestream/grpc-sharepoint)
(becoming grpc-microsoft). [CONTRIBUTING.md](CONTRIBUTING.md) has the
lint bar.

## Modules

Group `ai.pipestream.confluence`, version `0.1.0-SNAPSHOT`.

| Module | Port | What it is |
|---|---|---|
| `grpc-confluence-api` | | The API proto contract. 1:1 Confluence API fully validated for gRPC. |
| `grpc-confluence-service` | 9095 | Live Confluence Cloud proxy. REST in, typed gRPC out (reads and writes), plus crawl and sync. |
| `grpc-confluence-output` | | Confluence store keys, OKF catalog mapping, and its `OkfCatalogSource` registration. |
| `grpc-confluence-mcp` | | `confluence_*` and `connection_test` tools for the combined `grpc-mcp` server (`McpToolProvider`). `run` starts that server with these tools. |
| `grpc-confluence-connect` | | Kafka Connect source (`ConfluenceSourceConnector`) and its plugin zip. |

## Building

The core modules are Maven dependencies
(`ai.pipestream.integration:grpc-sync-api:0.1.0-SNAPSHOT` and friends).
There are two ways to get them:

1. **Sibling checkout (recommended).** Clone grpc-integration next to
   this repo. `settings.gradle.kts` sees `../grpc-integration` and
   includes it as a Gradle composite build, so the core builds from source
   and no registry access is needed:

   ```
   git clone https://github.com/ai-pipestream/grpc-integration
   git clone https://github.com/ai-pipestream/grpc-confluence
   cd grpc-confluence
   ./gradlew build
   ```

   A checkout elsewhere: `./gradlew build -PintegrationDir=/path/to/grpc-integration`.

2. **Published snapshots.** Without a checkout the snapshots resolve from
   the ai.pipestream Forgejo registry
   (`https://git.rokkon.com/api/packages/ai-pipestream/maven`, no
   credentials needed to read).

CI checks out both repositories side by side and uses option 1.

## Confluence proxy

```
CONFLUENCE_BASE_URL=https://example.atlassian.net/wiki
CONFLUENCE_EMAIL=bot@example.com
CONFLUENCE_API_TOKEN=...
# aliases: CONFLUENCE_USER / CONFLUENCE_TOKEN
CONFLUENCE_SPACES=ENG,DOCS          # optional allowlist
# optional: the site's attachment size limit; larger uploads are refused before they are sent
# CONFLUENCE_MAX_ATTACHMENT_BYTES=104857600
# optional: a Forge app's web trigger; the app property rpcs are served to a Forge app only
# CONFLUENCE_FORGE_APP_URL=https://...
# CONFLUENCE_FORGE_APP_SECRET=...
CONFLUENCE_GRPC_PORT=9095
# optional raw-bytes Kafka sink
CONFLUENCE_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

Every key of the space allowlist (`CONFLUENCE_SPACES`, or `space_keys` on a
`Sync`) has to come back from Confluence. Confluence lists the spaces it has
and can show the credentials and says nothing about the other keys, so a
`Sync` checks the answer first: when a key is missing (misspelled, keys
match exactly including case; the space is gone; or the credentials cannot
see it) the `Sync` fails with `NOT_FOUND` naming the keys and crawls
nothing, instead of crawling the rest and reporting the allowlist covered.

```
./gradlew :grpc-confluence-service:run
```

The service user needs Confluence product access and view on each space to
read; add, edit and delete on the space to write; and is an organization or
site admin only for spaces, space roles and groups.
[docs/permissions.md](docs/permissions.md) lists what each rpc needs, the plan
it needs, and which of that is proven against a real site.

RPCs: `ListSpaces`, `GetPage`, `GetBlogPost`, `ListPages`, `ListBlogPosts`,
`GetAttachment` (`include_content`, optional `CONFLUENCE_ATTACHMENT_MAX_BYTES` cap), `ListAttachments` (stream),
`Sync` (stream), `ProbeConnection`. Handlers run on virtual threads.
`ProbeConnection` reports a failed probe in-band (`ok=false` +
`error_message`); only catalog-resolution problems (unknown connection,
wrong kind, missing credentials) fail the rpc.

Point the proxy at a running sync-table (`grpc-sync-service` from
grpc-integration) to record crawl / update / delete rows (attachments
included):

```
SYNC_TABLE_TARGET=localhost:9097
SYNC_TABLE_PLAINTEXT=true
```

The resume cursor a `Sync` ends with is the time the pass began, less five
minutes of overlap (`ConfluenceCrawler.CURSOR_OVERLAP`), read before the
pass lists anything. It is not the newest modification time the pass saw:
a crawl runs for a long time, and a page edited after it was listed would
sit behind such a cursor and never be emitted. The next incremental pass
emits again whatever changed inside the overlap, so consumers must treat a
change as an upsert. A cursor stored by an older version may already be
past edits that version skipped; one full `Sync` (empty `since_cursor`)
picks them up.

With the Kafka sink on, every change and snapshot of a pass has to reach
its topic. The producer retries on its own (`delivery.timeout.ms`); a record
it gives up on ends the `Sync` with an error whose description starts
`kafka delivery failed for <n> records of this pass` and names the first
key and topic: `UNAVAILABLE` when the failure is one Kafka calls retriable
(the broker could not be reached or timed out; run the `Sync` again),
`INTERNAL` for a refusal such as a record that is too large. The pass stops
at the next change after the failure is known, and before its end it waits
for every outstanding record. No resume cursor is handed out, because the
next pass would start after the changes that were lost.

A ledger write that fails ends the `Sync` with an error whose description
starts `sync-table <rpc> failed for <row>`: `UNAVAILABLE` when the ledger
could not be reached in time (run the `Sync` again), `INTERNAL` for any
other refusal. No resume cursor is handed out, and a full crawl does not
reconcile, because reconciling by run id after a dropped upsert would mark
a page that still exists as deleted.

The whiteboards, databases, folders and Smart Links below a page, and a
database's fields and entries, are read through the tree and database
routes. A 404 on the first read of such a route means there is nothing to
read there (the tenant does not serve the route, or the item is not visible
to the credentials), and the crawl goes on. Any other refusal (401, 403,
400, or a failure partway through a list) ends the `Sync` with that status
and a description that names the read, for example
`Confluence 403 reading direct children at /api/v2/pages/200/direct-children`.
No resume cursor is handed out and nothing is reconciled, because a pass
that ended OK without a subtree would be taken for the whole space.

Only a 2xx answer is an answer. The client does not follow a redirect on an
API route (the credential must not travel to another origin), so a 3xx
fails the call with `FAILED_PRECONDITION` and a description naming the
status and the `Location` it pointed to: the connection's base URL is
`http://`, or names a site that has moved. A list route that answers 2xx
with no `results` array (an empty body, a 204, an error object sent with a
success status) fails with `INTERNAL` naming the route. Neither is read as
an empty last page, so a `Sync` cannot end OK and a probe cannot report a
healthy connection on the strength of one.

Completed `Sync` runs can also write an [Open Knowledge Format](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
v0.2 bundle (directory + zip) and a **sibling** WARC 1.1 file. WARC is a
per-URI archive (ISO 28500): one `resource` record per live `web_url`, a
`conversion` record for the OKF markdown, and an HTML collection page whose
links are those same URIs. The zip is **not** stored inside the WARC.

A `resource` record exists only for bytes read from Confluence. `Sync`
emits attachment metadata without the binary, and with `include_bodies`
off it emits pages, blog posts and comments without their bodies. Those
entities keep their OKF markdown, marked `capture: not_fetched` in its
frontmatter, and their link on the collection page, and they get no
`resource` record: nothing is written in place of content that was not
fetched.

```
OUTPUT_DIR=/data/okf/confluence-run   # alias: OKF_DIR
# OUTPUT_STORE=filesystem             # default; set s3 when grpc-output-s3 is loaded
# OUTPUT_FORMATS=okf                  # also: protobuf,json
# OUTPUT_PREFIX=run-2026-08-23
# OUTPUT_S3_BUCKET=knowledge
# OUTPUT_S3_PREFIX=confluence
# OUTPUT_S3_REGION=us-east-1
# optional path-based OKF siblings when writing only the legacy OkfOutput:
# OKF_ZIP=/data/okf/confluence-run.zip
# OKF_WARC=/data/okf/confluence-run.warc.gz
```

The stores and formats come from the grpc-integration output SPI.
`OutputStores.load().has("s3")` is true only when the S3 jar is on the
classpath. Filesystem is the default store. S3 object keys follow the
Confluence hierarchy (`{space}/pages/{id}.md`,
`pages/{pageId}/comments/{id}.pb`, ...). OKF trees land under `{prefix}/okf/`
with `bundle.zip` and `bundle.warc.gz` beside them. Protobuf is the same
binary Kafka Connect already publishes; JSON is a file export, not the
gRPC wire.

The bundle is written when a `Sync` pass completes, whatever kind of pass it
was. A full crawl of everything the connection covers replaces that
connection's part of the bundle with exactly what the crawl emitted. An
incremental `Sync` (`since_cursor` set), or a `Sync` of requested
`space_keys`, cannot show that anything it did not emit is gone, so it merges
its changes into the bundle of the last full crawl and rewrites the tree, zip
and WARC. The process keeps that bundle in memory only for the output set
through `OUTPUT_DIR` / `OKF_DIR`. When there is no full crawl to merge into
(the process restarted, or the output comes from the catalog, which is opened
for one `Sync` at a time), the pass writes its own changes as an increment
and leaves the full bundle alone: under `{prefix}/increments/{run-id}/` in an
output store, or beside the `OKF_DIR` / `OKF_ZIP` / `OKF_WARC` paths with
`-increments-{run-id}` added to each name. The run id of an incremental pass
is the cursor it resumed from, with characters a file name cannot carry
replaced by `_`; when the process-wide output serves several connections the
connection id comes before it. A pass that emitted nothing writes nothing.
Per-record formats (`protobuf`, `json`) are written as each change arrives,
in every kind of pass.

Output can also be set at runtime through MCP (`app_set_output` or
`connection_set_output`) without restarting the process.

A `Sync` runs without an output only when none is configured. When the
connection or the process has one and it cannot be used, the `Sync` fails
before it streams anything: `FAILED_PRECONDITION` when the store is not
loaded or cannot be opened (the text names the store or the path),
`UNAVAILABLE` when the catalog could not be reached to read the process
output (run the `Sync` again), `INTERNAL` when the catalog refused that
read.

## Multiple connections

The grpc-integration sync-table process hosts `ConnectionService` on the
same port as `SyncTableService` (`:9097`). That gRPC service is the catalog: create,
get, list, update, delete, record-probe, plus process settings. Confluence,
MCP, and a future UI call the generated stub. They never open the SQLite file.

```
SYNC_TABLE_JDBC_URL=jdbc:sqlite:/data/sync-table.db
# or SYNC_TABLE_DB=/data/sync-table.db
```

`connection_id` on Confluence `ListSpaces` / `Sync` / `ProbeConnection`
selects the catalog row. Ledger `asset_id` is
`{source}:{connection_id}:{kind}:{native_id}`. Env credentials still work
as connection `default`.

## Kafka Connect

`./gradlew :grpc-confluence-connect:connectPluginZip` builds
`grpc-confluence-connect/build/distributions/grpc-confluence-connect-0.1.0-SNAPSHOT-plugin.zip`.
Drop it on a Connect worker `plugin.path`.

The source emits raw `ConfluenceChange` protobuf bytes. Prefer
`grpc.target` so the worker talks to the already-running proxy (one
credential surface). Direct mode uses the in-process crawler and the same
credential keys as the proxy.

```json
{
  "name": "confluence-source",
  "config": {
    "connector.class": "ai.pipestream.connect.ConfluenceSourceConnector",
    "topic": "confluence.changes",
    "grpc.target": "localhost:9095",
    "grpc.plaintext": "true",
    "include.bodies": "false"
  }
}
```

## MCP (Streamable HTTP)

There is one combined MCP server, `grpc-mcp` in grpc-integration.
`grpc-confluence-mcp` registers `ConfluenceMcpToolProvider`
(`META-INF/services/ai.pipestream.mcp.McpToolProvider`); with its jar on
the server's classpath the endpoint adds:

```
confluence_list_spaces / confluence_get_page / confluence_list_attachments
confluence_sync
connection_test        # ConfluenceService.ProbeConnection
```

next to the core `app_*`, `connection_*`, and `sync_table_*` tools. To run
that server with the Confluence tools from this repo:

```
MCP_PORT=8090
CONFLUENCE_GRPC_TARGET=localhost:9095
SYNC_TABLE_TARGET=localhost:9097
./gradlew :grpc-confluence-mcp:run
```

For the Docker image from grpc-integration, put the
`grpc-confluence-mcp` and `grpc-confluence-api` jars in `/opt/mcp/ext`.
Add the Microsoft tool jars too for one endpoint over both sources.

## Proto lint (Buf)

`grpc-confluence-api` lints with Buf **STANDARD** plus **COMMENTS**. Every
service, rpc, message, field, oneof, enum, and enum value needs a
non-empty comment. Codegen stays `protoc` via the Gradle protobuf plugin;
Buf is lint-only here.

```
buf lint
```

CI runs this on every PR (`buf` job, Buf 1.54.0). Install from
https://buf.build or use the version CI pins.

## Build and tests

Java 25 toolchain. `./gradlew build` is the fake/unit suite, javadoc
(`-Xdoclint:all -Xwerror` on handwritten API), and never talks to
Atlassian.

Write rpcs are tested against the real site by `liveWriteTest` (below), and
its recorded exchanges are replayed in `test`.

The live smoke is **read-only**, excluded from `test`, and skips unless
credentials are in the environment:

```
./gradlew :grpc-confluence-service:liveSmokeTest
```

It runs `ListSpaces` (limit 1), then the space homepage
`GetPage` and `ListAttachments`. It never runs a full `Sync` and never
downloads attachment bytes.

The live write run proves the write rpcs against a real workspace. It is
excluded from `test` and skips unless the same credentials are set **and**
`CONFLUENCE_LIVE_WRITE=1`:

```
CONFLUENCE_LIVE_WRITE=1 CONFLUENCE_SPACE_KEY=CLAUDEGRPC \
  ./gradlew :grpc-confluence-service:liveWriteTest
```

It creates a parent page `pipestream-live-write-<UTC timestamp>-<random>` in
the space (`CONFLUENCE_SPACE_KEY`, default `CLAUDEGRPC`), runs every write
rpc under it, reads each result back, then deletes and purges what it made.
`build/live-write.md` has one row per rpc (PASS / FAIL / SKIPPED) and lists
anything left behind. Tenant-level writes (space create, space roles, role
assignments, space default classification, invitations, admin key) need
`CONFLUENCE_LIVE_WRITE_ADMIN=1`; custom content needs a type in
`CONFLUENCE_LIVE_WRITE_CUSTOM_CONTENT_TYPE`.

The run also records every HTTP exchange, scrubbed, under
`grpc-confluence-service/src/test/resources/recordings/confluence`.
`ConfluenceWriteReplayTest` runs the same steps against that recording in the
default `test` task, with no network, and is skipped while there is no
recording. See the
[recordings README](grpc-confluence-service/src/test/resources/recordings/README.md).
In CI the run is manual only: start the `ci` workflow with `run_live_write`.

### GitHub Actions secrets

Repo secrets help **CI**, not a local agent checkout. After they are
set, `live-confluence` runs on same-repo PRs and
pushes (fork PRs never receive secrets). Missing secrets skip the job
step so the repo stays green until you add them.

Settings, Secrets and variables, Actions:

| Secret | Required |
|---|---|
| `CONFLUENCE_EMAIL` | yes |
| `CONFLUENCE_API_TOKEN` | yes |
| `CONFLUENCE_BASE_URL` | no (defaults in the smoke test) |
| `CONFLUENCE_SPACES` | no |

Do not paste tokens into issues, PR text, or agent chat. GitHub redacts
exact secret values in logs; the tests never print them.

```
./gradlew build
```

## Docker

```
docker build -t grpc-confluence -f Dockerfile .
# build against a local core checkout instead of the published snapshots:
docker build --build-context integration=../grpc-integration -t grpc-confluence -f Dockerfile .
```

The sync-table and MCP images build from grpc-integration.
