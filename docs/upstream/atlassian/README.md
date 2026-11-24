# Atlassian upstream specs

Pinned snapshots of the upstream API documents the Confluence contract
(`grpc-confluence-api`) is checked against. Grep them for an endpoint's
parameters and response schema before adding or changing an rpc.

| File | Source | Pinned | Upstream version | Notes |
|---|---|---|---|---|
| `confluence-v2.json` | https://developer.atlassian.com/cloud/confluence/openapi-v2.v3.json | 2026-11-08 | 2.0.0 (OpenAPI 3.0.3), 165 paths / 252 operations | SHA-256 `40535e2a2c01547d53d824d68299098c9073e5bf7ed98a71634706759becfa31`. Copied from a pinned mirror; the first ~100 KB were cross-checked against the live CDN copy. |
| `confluence-v1.json` | https://dac-static.atlassian.com/cloud/confluence/swagger.v3.json | 2026-11-24 | 1.0.0 (OpenAPI 3.0.1), 89 paths / 130 operations | SHA-256 `f80279e4beace901a20174044ff3dc1cf047ea9486c3dd6eff57e33e4161bf12`. The REST v1 document. v1 routes are used only where v2 has none; `ConfluenceWriteRoutes.V1` lists the write rpcs they serve. |
| `adf-schema-full.json` | npm `@atlaskit/adf-schema`, file `dist/json-schema/v1/full.json` (https://registry.npmjs.org/@atlaskit/adf-schema) | 2026-11-10 | 57.7.39 (JSON Schema draft-04, Apache-2.0) | SHA-256 `5128562b75278c8a83e7e3619a570205bc80d59696985ec31a7a7883cff66fbe`. The Atlassian Document Format schema `adf.proto` models. |


To refresh: download the source URL, replace the file, and update the
pinned date, version, path count and SHA-256 above.
