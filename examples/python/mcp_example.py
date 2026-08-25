"""Drive the MCP endpoint (Streamable HTTP) with the standard library.

Initializes a session, lists the tools, and calls confluence_list_spaces
- the same handshake any MCP client performs, spelled out as three POSTs.

    MCP_URL=http://localhost:8090/mcp python3 mcp_example.py
"""

import json
import os
import urllib.request


def post(url: str, payload: dict, session_id: str | None = None) -> tuple[dict | None, str | None]:
    """POST one JSON-RPC message; returns (parsed result, session id)."""
    request = urllib.request.Request(url, method="POST",
            data=json.dumps(payload).encode())
    request.add_header("Content-Type", "application/json")
    request.add_header("Accept", "application/json, text/event-stream")
    if session_id:
        request.add_header("Mcp-Session-Id", session_id)
    with urllib.request.urlopen(request) as response:
        session = response.headers.get("Mcp-Session-Id", session_id)
        body = response.read().decode()
    # Streamable HTTP answers either bare JSON or an SSE frame; a
    # notification gets no body at all.
    for line in body.splitlines():
        if line.startswith("data: "):
            return json.loads(line[len("data: "):]), session
    return (json.loads(body) if body.strip() else None), session


def main() -> None:
    url = os.environ.get("MCP_URL", "http://localhost:8090/mcp")

    init, session = post(url, {
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": {"protocolVersion": "2025-06-18", "capabilities": {},
                   "clientInfo": {"name": "mcp-example", "version": "0"}}})
    server = init["result"]["serverInfo"]
    print(f"connected to {server['name']} {server['version']} (session {session})")
    post(url, {"jsonrpc": "2.0", "method": "notifications/initialized"}, session)

    tools, _ = post(url, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}, session)
    print("tools:", ", ".join(t["name"] for t in tools["result"]["tools"]))

    call, _ = post(url, {
        "jsonrpc": "2.0", "id": 3, "method": "tools/call",
        "params": {"name": "confluence_list_spaces", "arguments": {}}}, session)
    result = call["result"]
    if result.get("isError"):
        raise SystemExit(f"tool error: {result['content'][0]['text']}")
    payload = json.loads(result["content"][0]["text"])
    print("confluence_list_spaces ->", json.dumps(payload, indent=2)[:400])


if __name__ == "__main__":
    main()
