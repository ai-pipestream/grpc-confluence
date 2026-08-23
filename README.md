# grpc-confluence

Standalone Java gRPC bundles for Confluence Cloud and Microsoft Graph, plus
a Kafka Connect plugin and a Microsoft Graph Connector Agent (GCA) adapter.
Nothing here depends on the ProtoMolt platform: no index hints, no proto
validate options, no Kafka serde module. Domain rules that used to live in
proto options are enforced in Java validators before a message leaves the
process.

The Microsoft Copilot connector wire contracts are copied from
[Custom-Copilot-Connector-using-Connector-SDK](https://github.com/microsoft/Custom-Copilot-Connector-using-Connector-SDK)
(MIT). See `grpc-microsoft-connector/NOTICE`.

## Modules

| Module | Port | What it is |
|---|---|---|
| `grpc-confluence-api` | — | Confluence domain + `ConfluenceService` protos |
| `grpc-confluence-service` | 9095 | REST v2 client, crawler, validator, Netty proxy |
| `grpc-microsoft-api` | — | Graph domain + `MicrosoftService` protos |
| `grpc-microsoft-service` | 9096 | Graph client, crawler, validator, Netty proxy |
| `grpc-microsoft-connector` | 30303 | GCA SDK services → `MicrosoftService` |
| `grpc-connect` | — | Kafka Connect sources (protobuf bytes) |

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
`GetAttachment` (`include_content`, 25 MiB cap), `Sync`.

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

RPCs: `GetMe`, `ListSites`, `ListDrives`, `ListChildren`, `GetItem`,
`DownloadItem`, `Sync`.

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

## Build

Java 25 toolchain (same as gRPOIc). Live smoke tests are excluded from
`test` and run only as `liveSmokeTest` when credentials are present.

```
./gradlew build
```

## Docker

```
docker build -t grpc-confluence -f Dockerfile .
docker build -t grpc-microsoft -f Dockerfile.microsoft .
docker build -t grpc-microsoft-connector -f Dockerfile.connector .
```
