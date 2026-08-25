"""Read a Confluence site through the gRPC proxy.

Lists spaces, fetches one page in storage format, streams the space's
pages, and runs a bounded Sync - printing the resume cursor a consumer
would persist for the next incremental pass.

Run ./generate.sh once, then:

    CONFLUENCE_GRPC_TARGET=localhost:9095 python3 confluence_example.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "gen"))

import grpc  # noqa: E402

from ai.pipestream.confluence.v1 import (  # noqa: E402
    confluence_service_pb2 as api,
    confluence_service_pb2_grpc as rpc,
    common_pb2 as common,
)


def main() -> None:
    target = os.environ.get("CONFLUENCE_GRPC_TARGET", "localhost:9095")
    with grpc.insecure_channel(target) as channel:
        stub = rpc.ConfluenceServiceStub(channel)

        probe = stub.ProbeConnection(api.ProbeConnectionRequest(limit=3))
        if not probe.ok:
            raise SystemExit(f"probe failed: {probe.error_message}")
        print(f"connected; sample spaces: {list(probe.space_keys)}")

        spaces = stub.ListSpaces(api.ListSpacesRequest()).spaces
        for space in spaces:
            print(f"space {space.key!r} id={space.id} name={space.name!r}")
        if not spaces:
            return

        # Stream every page of the first space; keep the first page id.
        space = spaces[0]
        first_page_id = None
        for response in stub.ListPages(api.ListPagesRequest(space_id=space.id)):
            page = response.page
            first_page_id = first_page_id or page.id
            print(f"page {page.id} {page.title!r}")

        if first_page_id:
            got = stub.GetPage(api.GetPageRequest(
                id=first_page_id,
                body_format=common.BODY_FORMAT_STORAGE_XHTML))
            body = got.page.body.storage.value
            print(f"page {got.page.title!r} storage body: {len(body)} chars")

        # One bounded sync pass. Persist resume_cursor and pass it back as
        # since_cursor to receive only what changed since this run.
        changes = 0
        cursor = ""
        for event in stub.Sync(api.SyncRequest(space_keys=[space.key])):
            kind = event.WhichOneof("event")
            if kind == "change":
                changes += 1
            elif kind == "resume_cursor":
                cursor = event.resume_cursor
        print(f"sync: {changes} changes, resume_cursor={cursor!r}")


if __name__ == "__main__":
    main()
