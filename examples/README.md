# Client examples

Runnable clients for the Confluence gRPC proxy, one per language. Each
example lists spaces, fetches one page with a body format, streams the
page list, and runs a bounded `Sync` that returns a resume cursor - the
loop a real consumer runs to keep an archive current.

| Directory | Language | Stubs come from |
|---|---|---|
| [`python/`](python/) | Python 3.10+ | `grpcio-tools` codegen (`generate.sh`) |
| [`go/`](go/) | Go 1.22+ | `buf generate` managed mode (`generate.sh`) |
| [`java/`](java/) | Java 21+ | the repo's own `grpc-confluence-api` jars |

The Python directory also drives the **MCP endpoint** (`mcp_example.py`,
stdlib only - no MCP SDK needed).

## Prerequisites

A running proxy. From the repo root:

```bash
export CONFLUENCE_BASE_URL=https://example.atlassian.net/wiki
export CONFLUENCE_EMAIL=bot@example.com      # alias: CONFLUENCE_USER
export CONFLUENCE_API_TOKEN=...              # alias: CONFLUENCE_TOKEN
./gradlew :grpc-confluence-service:run       # gRPC on :9095
./gradlew :grpc-mcp:run                      # MCP on :8090 (optional)
```

Every example takes the target from `CONFLUENCE_GRPC_TARGET`
(default `localhost:9095`); the MCP example uses `MCP_URL`
(default `http://localhost:8090/mcp`). The examples are read-only
except `Sync`, which writes wherever the *server* is configured to
write (ledger, OKF, Kafka) - point them at a test site first.

The server exposes gRPC reflection, so `grpcurl` works without any
codegen at all:

```bash
grpcurl -plaintext localhost:9095 list
grpcurl -plaintext -d '{}' localhost:9095 \
  ai.pipestream.confluence.v1.ConfluenceService/ProbeConnection
```
