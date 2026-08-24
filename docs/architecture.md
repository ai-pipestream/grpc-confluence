# Architecture

Standalone Java processes that expose Confluence Cloud and Microsoft Graph
as typed gRPC, plus three adapters that consume those services: Kafka
Connect, a Microsoft Graph Connector Agent (GCA) process, and a Streamable
HTTP MCP endpoint. A generic **sync-table** gRPC service is the current
database of where a source asset lives and whether it has been crawled,
updated, or deleted.

Domain rules that used to live in proto options are enforced in Java
validators before a message leaves the process.

```
                    ┌─────────────┐
   MCP clients ────►│  grpc-mcp   │  :8090  /mcp  (Netty + VT)
                    └──────┬──────┘
           gRPC plaintext  │
     ┌─────────────────────┼─────────────────────┐
     ▼                     ▼                     ▼
┌─────────────┐     ┌─────────────┐       ┌─────────────┐
│ confluence  │     │  microsoft  │       │  sync-table │
│   :9095     │     │    :9096    │       │    :9097    │
└──────┬──────┘     └──────┬──────┘       └──────▲──────┘
       │  REST v2          │  Graph REST         │
       ▼                   ▼                     │
  Atlassian Cloud     Microsoft Graph            │
                           ▲                     │
                    ┌──────┴──────┐              │
   GCA ────────────►│  connector  │ :30303       │
                    └─────────────┘              │
                                                 │
   Kafka Connect ──► grpc-connect ── Sync() ─────┘
                     (or in-process crawler)
```

When `SYNC_TABLE_TARGET` is set, each Confluence / Microsoft `Sync` also
writes the ledger (attachments included) and calls `Reconcile` after a
full crawl so rows not seen in that run become `DELETED`. The same
process exposes `ConnectionService`: create / get / list / update /
delete / record-probe. Confluence, MCP, and a future UI call that gRPC
service; they do not open the store. `connection_id` on `Sync` and
`ListSpaces` selects which catalog row to use. `asset_id` is
`{source}:{connection_id}:{kind}:{native_id}`. Reconcile is
connection-scoped so two Confluence Clouds do not delete each other.

When `OUTPUT_DIR` / `OKF_DIR` or `OUTPUT_S3_BUCKET` is set, each completed
`Sync` writes crawl artifacts through the output SPI. `OutputStore`
implementations are ServiceLoader jars: filesystem is the default;
`grpc-output-s3` is optional (`OutputStores.has("s3")`). `OUTPUT_FORMATS`
selects protobuf (the same binary Kafka already publishes), JSON (file
export), OKF markdown + sibling WARC, and microsoft-connector. S3 keys
follow Confluence / Graph hierarchy. Microsoft can additionally upload an
OKF payload to a SharePoint folder (`OKF_SPO_DRIVE_ID`).

## Processes

| Process | Port | Threading | Streaming |
|---|---|---|---|
| `grpc-confluence-service` | 9095 | Netty + virtual-thread executor | `ListPages`, `ListBlogPosts`, `ListAttachments`, `Sync` |
| `grpc-microsoft-service` | 9096 | same | `ListChildren`, `Sync` |
| `grpc-microsoft-connector` | 30303 | same | GCA crawl stream → `MicrosoftService.Sync` |
| `grpc-sync-service` | 9097 | same | `SyncTableService` + `ConnectionService` |
| `grpc-mcp` | 8090 | Netty HTTP + virtual-thread handlers | Streamable HTTP; tools consume gRPC streams and return a bounded JSON summary |
| `grpc-connect` | — | Connect worker threads | Pulls `Sync` streams; values are protobuf bytes |

There is no JDK built-in MCP API. `grpc-mcp` uses MCP Java SDK 2.0.1
(spec 2025-11-25) on the same shaded Netty as the gRPC processes. Do
not call unbounded `Watch` from an MCP tool.

## Wire

Proto packages are `ai.pipestream.<domain>.v1`. Buf lint is STANDARD plus
COMMENTS (every service, rpc, message, field, oneof, enum, and enum value
commented). The Microsoft GCA `Contracts/` protos are a frozen MIT copy;
their proto package stays `Microsoft.Graph.Connectors.Contracts.Grpc` so
GCA can talk to this process. They are not in the Buf workspace.

Codegen is `protoc` via the Gradle protobuf plugin, not `buf generate`.

## Sync-table

`asset_id` is `{source}:{connection_id}:{kind}:{native_id}`. The ledger
owns phase: a re-upsert is `UPDATE` even when a crawler still labels the
write `INITIAL_CRAWL`. `DELETE` and `SNAPSHOT` stay caller-controlled.
Attachments set `attachment=true` and `parent_asset_id`. `AssetStore` is
the in-memory store; `JdbcLedger` (SQLite via `SYNC_TABLE_JDBC_URL` or
`SYNC_TABLE_DB`) is the durable one. Both sit behind
`SyncTableService` and `ConnectionService` on the same port. Secrets on
a connection are write-only; Get/List return `has_token` instead.

## Credentials

Proxies hold upstream credentials. Kafka Connect should set `grpc.target`
so the worker does not also hold tokens. GCA Graph tokens stay on
`MicrosoftService`; the connector only forwards.

Live CI smokes are read-only. Microsoft live smoke uses Entra
**application** credentials (client credentials). `/me` does not exist on
an app-only token.
