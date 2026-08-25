import ai.pipestream.confluence.v1.BodyFormat;
import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.GetPageRequest;
import ai.pipestream.confluence.v1.GetPageResponse;
import ai.pipestream.confluence.v1.ListPagesRequest;
import ai.pipestream.confluence.v1.ListPagesResponse;
import ai.pipestream.confluence.v1.ListSpacesRequest;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.ProbeConnectionRequest;
import ai.pipestream.confluence.v1.ProbeConnectionResponse;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.SyncRequest;
import ai.pipestream.confluence.v1.SyncResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Reads a Confluence site through the gRPC proxy: probes the connection,
 * lists spaces, streams one space's pages, fetches a page body in storage
 * format, and runs a bounded {@code Sync}, printing the resume cursor a
 * consumer would persist for the next incremental pass.
 *
 * <p>See README.md for build and run commands; the classpath is the
 * repo's own {@code grpc-confluence-service} distribution jars.</p>
 */
public final class ConfluenceExample {

    private ConfluenceExample() {
    }

    public static void main(String[] args) throws InterruptedException {
        String target = System.getenv().getOrDefault("CONFLUENCE_GRPC_TARGET", "localhost:9095");
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
        try {
            ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub =
                    ConfluenceServiceGrpc.newBlockingStub(channel);

            ProbeConnectionResponse probe = stub.probeConnection(
                    ProbeConnectionRequest.newBuilder().setLimit(3).build());
            if (!probe.getOk()) {
                throw new IllegalStateException("probe failed: " + probe.getErrorMessage());
            }
            System.out.println("connected; sample spaces: " + probe.getSpaceKeysList());

            List<Space> spaces = stub.listSpaces(ListSpacesRequest.getDefaultInstance())
                    .getSpacesList();
            for (Space space : spaces) {
                System.out.printf("space %s id=%s name=%s%n",
                        space.getKey(), space.getId(), space.getName());
            }
            if (spaces.isEmpty()) {
                return;
            }
            Space space = spaces.getFirst();

            // Stream every page of the first space; keep the first page id.
            String firstPageId = "";
            Iterator<ListPagesResponse> pages = stub.listPages(
                    ListPagesRequest.newBuilder().setSpaceId(space.getId()).build());
            while (pages.hasNext()) {
                Page page = pages.next().getPage();
                if (firstPageId.isEmpty()) {
                    firstPageId = page.getId();
                }
                System.out.printf("page %s %s%n", page.getId(), page.getTitle());
            }

            if (!firstPageId.isEmpty()) {
                GetPageResponse got = stub.getPage(GetPageRequest.newBuilder()
                        .setId(firstPageId)
                        .setBodyFormat(BodyFormat.BODY_FORMAT_STORAGE_XHTML)
                        .build());
                System.out.printf("page %s storage body: %d chars%n",
                        got.getPage().getTitle(),
                        got.getPage().getBody().getStorage().getValue().length());
            }

            // One bounded sync pass. Persist the resume cursor and pass it
            // back as since_cursor to receive only what changed since.
            int changes = 0;
            String cursor = "";
            Iterator<SyncResponse> sync = stub.sync(SyncRequest.newBuilder()
                    .addSpaceKeys(space.getKey()).build());
            while (sync.hasNext()) {
                SyncResponse event = sync.next();
                switch (event.getEventCase()) {
                    case CHANGE -> changes++;
                    case RESUME_CURSOR -> cursor = event.getResumeCursor();
                    default -> {
                    }
                }
            }
            System.out.printf("sync: %d changes, resume_cursor=%s%n", changes, cursor);
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
