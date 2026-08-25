// Reads a Confluence site through the gRPC proxy: probes the connection,
// lists spaces, streams one space's pages, fetches a page body in storage
// format, and runs a bounded Sync, printing the resume cursor a consumer
// would persist for the next incremental pass.
//
// Run ./generate.sh once, then:
//
//	CONFLUENCE_GRPC_TARGET=localhost:9095 go run .
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "confluence-example/gen/ai/pipestream/confluence/v1"
)

func main() {
	target := os.Getenv("CONFLUENCE_GRPC_TARGET")
	if target == "" {
		target = "localhost:9095"
	}
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", target, err)
	}
	defer conn.Close()
	client := pb.NewConfluenceServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	probe, err := client.ProbeConnection(ctx, &pb.ProbeConnectionRequest{Limit: 3})
	if err != nil {
		log.Fatalf("probe rpc: %v", err)
	}
	if !probe.GetOk() {
		log.Fatalf("probe failed: %s", probe.GetErrorMessage())
	}
	fmt.Printf("connected; sample spaces: %v\n", probe.GetSpaceKeys())

	spaces, err := client.ListSpaces(ctx, &pb.ListSpacesRequest{})
	if err != nil {
		log.Fatalf("list spaces: %v", err)
	}
	for _, space := range spaces.GetSpaces() {
		fmt.Printf("space %q id=%s name=%q\n", space.GetKey(), space.GetId(), space.GetName())
	}
	if len(spaces.GetSpaces()) == 0 {
		return
	}
	space := spaces.GetSpaces()[0]

	// Stream every page of the first space; keep the first page id.
	pages, err := client.ListPages(ctx, &pb.ListPagesRequest{SpaceId: space.GetId()})
	if err != nil {
		log.Fatalf("list pages: %v", err)
	}
	firstPageID := ""
	for {
		response, err := pages.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatalf("page stream: %v", err)
		}
		page := response.GetPage()
		if firstPageID == "" {
			firstPageID = page.GetId()
		}
		fmt.Printf("page %s %q\n", page.GetId(), page.GetTitle())
	}

	if firstPageID != "" {
		got, err := client.GetPage(ctx, &pb.GetPageRequest{
			Id:         firstPageID,
			BodyFormat: pb.BodyFormat_BODY_FORMAT_STORAGE_XHTML,
		})
		if err != nil {
			log.Fatalf("get page: %v", err)
		}
		body := got.GetPage().GetBody().GetStorage().GetValue()
		fmt.Printf("page %q storage body: %d chars\n", got.GetPage().GetTitle(), len(body))
	}

	// One bounded sync pass. Persist the resume cursor and pass it back as
	// since_cursor to receive only what changed since this run.
	sync, err := client.Sync(ctx, &pb.SyncRequest{SpaceKeys: []string{space.GetKey()}})
	if err != nil {
		log.Fatalf("sync: %v", err)
	}
	changes, cursor := 0, ""
	for {
		event, err := sync.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatalf("sync stream: %v", err)
		}
		switch event.GetEvent().(type) {
		case *pb.SyncResponse_Change:
			changes++
		case *pb.SyncResponse_ResumeCursor:
			cursor = event.GetResumeCursor()
		}
	}
	fmt.Printf("sync: %d changes, resume_cursor=%q\n", changes, cursor)
}
