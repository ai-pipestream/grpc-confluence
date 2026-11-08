# Atlassian upstream specs

Pinned snapshots of the upstream API documents the Confluence contract
(`grpc-confluence-api`) is checked against. Grep them for an endpoint's
parameters and response schema before adding or changing an rpc.

| File | Source | Pinned | Upstream version | Notes |
|---|---|---|---|---|
| `confluence-v2.json` | https://developer.atlassian.com/cloud/confluence/openapi-v2.v3.json | 2026-11-08 | 2.0.0 (OpenAPI 3.0.3), 165 paths / 252 operations | SHA-256 `40535e2a2c01547d53d824d68299098c9073e5bf7ed98a71634706759becfa31`. Copied from a pinned mirror; the first ~100 KB were cross-checked against the live CDN copy. |

Not yet vendored: the Confluence REST v1 document
(https://developer.atlassian.com/cloud/confluence/swagger.v3.json). The
build sandbox cannot reach developer.atlassian.com or dac-static.atlassian.com,
so it has to be added from a machine that can.

To refresh: download the source URL, replace the file, and update the
pinned date, version, path count and SHA-256 above.
