# grpc-confluence

Licensed under Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

A gRPC version of the Confluence API. The proxy speaks Confluence Cloud
REST v2 upstream and typed gRPC downstream: clients read spaces, pages,
blog posts, comments, and attachments over one generated contract, pick up
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
| `grpc-confluence-service` | 9095 | Live Confluence Cloud proxy. REST v2 in, typed gRPC out, plus crawl and sync. |
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
CONFLUENCE_GRPC_PORT=9095
# optional raw-bytes Kafka sink
CONFLUENCE_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

```
./gradlew :grpc-confluence-service:run
```

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

Completed `Sync` runs can also write an [Open Knowledge Format](https://github.com/GoogleCloudPlatform/knowledge-catalog/blob/main/okf/SPEC.md)
v0.2 bundle (directory + zip) and a **sibling** WARC 1.1 file. WARC is a
per-URI archive (ISO 28500): one `resource` record per live `web_url`, a
`conversion` record for the OKF markdown, and an HTML collection page whose
links are those same URIs. The zip is **not** stored inside the WARC.

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

Output can also be set at runtime through MCP (`app_set_output` or
`connection_set_output`) without restarting the process.

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

The live smoke is **read-only**, excluded from `test`, and skips unless
credentials are in the environment:

```
./gradlew :grpc-confluence-service:liveSmokeTest
```

It runs `ListSpaces` (limit 1), then the space homepage
`GetPage` and `ListAttachments`. It never runs a full `Sync` and never
downloads attachment bytes.

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
