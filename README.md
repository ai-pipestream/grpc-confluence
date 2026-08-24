# grpc-confluence

All-in-one gRPC interface for Confluence operations and Confluence
synchronizing. Compatible as a Microsoft connector as well.

This is a gRPC Confluence API that listens to Confluence data and sends it to a
filesystem, S3, or Microsoft Graph. At runtime, Confluence connectivity
utilizes the Kafka Connect standard.

The gRPC interface was taken from the latest Confluence API spec and designed
to have all of the same features. So there's no data loss: if the API sends it,
gRPC also captures it.

Attachments are supported and linked.

An MCP endpoint is designed to help an LLM perform most of the setup and
configuration at runtime.

See [docs/architecture.md](docs/architecture.md) for process topology and
[CONTRIBUTING.md](CONTRIBUTING.md) for the lint bar.

The Microsoft Copilot connector wire contracts are copied from
[Custom-Copilot-Connector-using-Connector-SDK](https://github.com/microsoft/Custom-Copilot-Connector-using-Connector-SDK)
(MIT). See `grpc-microsoft-connector/NOTICE`.

## Modules

| Module | Port | What it is |
|---|---|---|
| `grpc-confluence-api` | | The API proto contract. 1:1 Confluence API fully validated for gRPC. |
| `grpc-confluence-service` | 9095 | Live Confluence Cloud proxy. REST v2 in, typed gRPC out, plus crawl and sync. |
| `grpc-microsoft-api` | | The API proto contract. 1:1 Microsoft Graph surface we crawl, validated for gRPC. |
| `grpc-microsoft-service` | 9096 | Live Graph proxy for sites, drives, and files. |
| `grpc-microsoft-connector` | 30303 | Microsoft Copilot / GCA adapter that forwards crawls to MicrosoftService. |
| `grpc-connect` | | Kafka Connect source plugins. Pulls Sync streams as protobuf bytes. |
| `grpc-sync-api` | | Ledger and connection catalog proto contract. |
| `grpc-sync-service` | 9097 | Asset ledger plus ConnectionService. Memory or SQLite. |
| `grpc-okf` | | Writes Open Knowledge Format v0.2 bundles and a sibling WARC 1.1 archive. |
| `grpc-output-spi` | | How crawl output is written. Pick a store (filesystem, S3) and formats (OKF, protobuf, JSON). |
| `grpc-output-filesystem` | | Writes artifacts to a local directory. |
| `grpc-output-s3` | | Writes artifacts to S3 using Confluence or Graph path keys. |
| `grpc-mcp` | 8090 | Streamable HTTP MCP tools so an LLM can set up connections, output, and run syncs. |

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
`GetAttachment` (`include_content`, 25 MiB cap), `ListAttachments` (stream),
`Sync` (stream), `ProbeConnection`. Handlers run on virtual threads.

Point the proxy at a running sync-table to record crawl / update / delete
rows (attachments included):

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
# OUTPUT_FORMATS=okf                  # also: protobuf,json,microsoft-connector
# OUTPUT_PREFIX=run-2026-08-23
# OUTPUT_S3_BUCKET=knowledge
# OUTPUT_S3_PREFIX=confluence
# OUTPUT_S3_REGION=us-east-1
# optional path-based OKF siblings when writing only the legacy OkfOutput:
# OKF_ZIP=/data/okf/confluence-run.zip
# OKF_WARC=/data/okf/confluence-run.warc.gz
```

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

The sync-table process hosts `ConnectionService` on the same port as
`SyncTableService` (`:9097`). That gRPC service is the catalog: create,
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

## Microsoft Graph proxy

```
MICROSOFT_TENANT_ID=...
MICROSOFT_CLIENT_ID=...
MICROSOFT_CLIENT_SECRET=...
MICROSOFT_SITE_ID=...               # optional; empty = signed-in user's drive
MICROSOFT_DRIVE_IDS=...             # optional comma-separated allowlist
MICROSOFT_GRPC_PORT=9096
```

```
./gradlew :grpc-microsoft-service:run
```

RPCs: `GetMe`, `ListSites`, `ListDrives`, `ListChildren` (stream), `GetItem`,
`DownloadItem`, `Sync` (stream). Same `SYNC_TABLE_TARGET` / `SYNC_TABLE_PLAINTEXT`
env as Confluence. Same `OKF_DIR` / `OKF_ZIP` / `OKF_WARC` as Confluence.
Handlers run on virtual threads.

`GetItem`, `DownloadItem`, and `Sync` flatten SharePoint list-item columns
into typed `ListColumn`s (scalars and one-level nested objects; `@odata.*`
skipped). `ListChildren` stays a listing RPC and does not fetch columns.

To land the OKF payload **on** a SharePoint library (markdown tree + zip +
`.warc.gz`), set a destination drive. Files larger than 4 MiB use a Graph
upload session; the session URL is called **without** `Authorization`.
Needs `Files.ReadWrite.All` (or equivalent) on the app. Live CI smokes stay
read-only and do not set these.

```
OKF_SPO_DRIVE_ID=b!...
OKF_SPO_FOLDER_PATH=/Knowledge/okf-run
```

## Copilot connector (GCA)

A separate process that implements Microsoft's four connector services
(`ConnectorInfoService`, `ConnectionManagementService`,
`ConnectorCrawlerService`, `ConnectorOAuthService`) and forwards crawls to
`MicrosoftService`. Graph tokens stay on the Microsoft proxy.

Connector id (stable, put this in the GCA manifest):
`43760992-c66e-46bd-b937-b63e021aa63b`

Custom configuration JSON (GCA does not interpret this string):

```json
{
  "target": "localhost:9096",
  "plaintext": true,
  "driveIds": [],
  "folderPath": "/",
  "includeContent": false
}
```

`ValidateAuthentication` calls `GetMe`. When custom configuration is empty,
a `host:port` or `grpc://host:port` datasource URL is treated as the
MicrosoftService target.

```
MICROSOFT_GRPC_TARGET=localhost:9096
CONNECTOR_GRPC_PORT=30303
./gradlew :grpc-microsoft-connector:run
```

## Kafka Connect

`./gradlew :grpc-connect:connectPluginZip` builds
`grpc-connect/build/distributions/grpc-connect-0.1.0-SNAPSHOT-plugin.zip`.
Drop it on a Connect worker `plugin.path`.

Both sources emit raw protobuf bytes (`ConfluenceChange` /
`MicrosoftChange`). Prefer `grpc.target` so the worker talks to the
already-running proxy (one credential surface). Direct mode uses the
in-process crawler and the same credential keys as the proxies.

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

```json
{
  "name": "microsoft-source",
  "config": {
    "connector.class": "ai.pipestream.connect.MicrosoftSourceConnector",
    "topic": "microsoft.changes",
    "grpc.target": "localhost:9096",
    "grpc.plaintext": "true"
  }
}
```

## Sync table

A source-agnostic gRPC ledger of where each asset lives, its phase
(initial crawl / update / delete), sync status, and whether it is an
attachment. Confluence and Microsoft `Sync` streams write it; MCP and
any other client can query it.

```
SYNC_TABLE_GRPC_PORT=9097
./gradlew :grpc-sync-service:run
```

RPCs: `UpsertAsset`, `GetAsset`, `ListAssets` (stream), `Watch` (stream),
`DeleteAsset`, `Reconcile`, `GetCheckpoint`, `PutCheckpoint`.
`ConnectionService` on the same port owns connections and runtime settings.

`asset_id` is `{source}:{connection_id}:{kind}:{native_id}`. After a full
crawl the proxies call `Reconcile` with that run's id so rows not seen
this pass become `DELETED` even when the upstream API only upserts.
Attachments set `attachment=true` and `parent_asset_id`. The store is
in-memory unless `SYNC_TABLE_JDBC_URL` or `SYNC_TABLE_DB` points at SQLite.
Handlers run on virtual threads.

## MCP (Streamable HTTP)

There is no JDK built-in MCP API. This jar uses the official MCP Java
SDK 2.0.1 (spec 2025-11-25) with Streamable HTTP at `/mcp`, hosted on
Jetty 12 with a virtual-thread pool. Tool handlers are the SDK's
blocking `McpServer.sync` API (virtual-thread friendly) and consume
the streaming gRPC RPCs, returning a bounded JSON summary.

```
MCP_PORT=8090
CONFLUENCE_GRPC_TARGET=localhost:9095
MICROSOFT_GRPC_TARGET=localhost:9096
SYNC_TABLE_TARGET=localhost:9097
./gradlew :grpc-mcp:run
```

Setup and configure the running app (these call `ConnectionService`):

```
app_status
app_set_output
app_set_kafka
connection_create / connection_list / connection_get / connection_update
connection_set_output / connection_test / connection_delete
```

Read and sync:

```
confluence_list_spaces / confluence_get_page / confluence_list_attachments
confluence_sync
microsoft_get_me / microsoft_sync
sync_table_get_asset / sync_table_list_assets
```

Do not call unbounded `Watch` from a tool; use `ListAssets`. Secrets are
write-only. Get, list, and MCP rows return `hasToken`, never the token.

## Proto lint (Buf)

Our domain protos (`grpc-confluence-api`, `grpc-microsoft-api`,
`grpc-sync-api`) lint with Buf **STANDARD** plus **COMMENTS**. Every
service, rpc, message, field, oneof, enum, and enum value needs a
non-empty comment. Codegen stays `protoc` via the Gradle protobuf plugin;
Buf is lint-only here.

Microsoft's GCA `Contracts/` tree is a frozen MIT copy of their wire. It is
**not** in the Buf workspace (their lint bar, not ours). The proto package
stays `Microsoft.Graph.Connectors.Contracts.Grpc`.

```
buf lint
```

CI runs this on every PR (`buf` job, Buf 1.54.0). Install from
https://buf.build or use the version CI pins.

## Build and tests

Java 25 toolchain. `./gradlew build` is the fake/unit suite, javadoc
(`-Xdoclint:all -Werror` on handwritten API), and never talks to Atlassian
or Graph.

Live smokes are **read-only**, excluded from `test`, and skip unless
credentials are in the environment:

```
./gradlew :grpc-confluence-service:liveSmokeTest
./gradlew :grpc-microsoft-service:liveSmokeTest
```

Confluence live smoke: `ListSpaces` (limit 1), then the space homepage
`GetPage` and `ListAttachments`. It never runs a full `Sync` and never
downloads attachment bytes.

Microsoft live smoke uses **Entra application** (client credentials),
not a user mailbox login. `/me` does not exist on an app-only token, so
the probe is `ListSites` and, when a site id is known, `ListDrives`.
Create an app registration in the M365 tenant with application
permissions `Sites.Read.All` and `Files.Read.All` (admin consent), then
a client secret.

### GitHub Actions secrets

Repo secrets help **CI**, not a local agent checkout. After they are
set, `live-confluence` and `live-microsoft` run on same-repo PRs and
pushes (fork PRs never receive secrets). Missing secrets skip the job
step so the repo stays green until you add them.

Confluence (Settings, Secrets and variables, Actions):

| Secret | Required |
|---|---|
| `CONFLUENCE_EMAIL` | yes |
| `CONFLUENCE_API_TOKEN` | yes |
| `CONFLUENCE_BASE_URL` | no (defaults in the smoke test) |
| `CONFLUENCE_SPACES` | no |

Microsoft:

| Secret | Required |
|---|---|
| `MICROSOFT_TENANT_ID` | yes |
| `MICROSOFT_CLIENT_ID` | yes |
| `MICROSOFT_CLIENT_SECRET` | yes |
| `MICROSOFT_SITE_ID` | no (first `ListSites` hit is used for drives) |
| `MICROSOFT_DRIVE_IDS` | no |

Do not paste tokens into issues, PR text, or agent chat. GitHub redacts
exact secret values in logs; the tests never print them.

```
./gradlew build
```

## Docker

```
docker build -t grpc-confluence -f Dockerfile .
docker build -t grpc-microsoft -f Dockerfile.microsoft .
docker build -t grpc-microsoft-connector -f Dockerfile.connector .
docker build -t grpc-sync -f Dockerfile.sync .
docker build -t grpc-mcp -f Dockerfile.mcp .
```
