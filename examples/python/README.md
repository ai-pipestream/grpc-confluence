# Python example

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
./generate.sh                                  # stubs into gen/
CONFLUENCE_GRPC_TARGET=localhost:9095 python3 confluence_example.py
MCP_URL=http://localhost:8090/mcp python3 mcp_example.py
```

`confluence_example.py` probes the connection, lists spaces, streams one
space's pages, fetches a page body in storage format, and runs a bounded
`Sync`, printing the resume cursor to persist for the next incremental
pass.

`mcp_example.py` speaks MCP Streamable HTTP with only the standard
library: initialize, `tools/list`, then `confluence_list_spaces`.

`gen/` is codegen output - regenerate with `generate.sh` after any proto
change; do not commit it.
