package ai.pipestream.mcp;

import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.GetPageRequest;
import ai.pipestream.confluence.v1.GetPageResponse;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.confluence.v1.Space;
import ai.pipestream.confluence.v1.StreamSpacesRequest;
import ai.pipestream.confluence.v1.StreamSpacesResponse;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConfluenceMcpToolsTest {

    private Server grpc;
    private McpHttpServer mcp;

    @AfterEach
    void stop() {
        if (mcp != null) {
            mcp.close();
        }
        if (grpc != null) {
            grpc.shutdownNow();
        }
    }

    @Test
    void providerIsRegisteredForTheCombinedServer() {
        assertThat(McpToolProviders.load())
                .filteredOn(provider -> provider instanceof ConfluenceMcpToolProvider)
                .singleElement()
                .satisfies(provider -> {
                    assertThat(provider.name()).isEqualTo("confluence");
                    assertThat(provider.targetEnvironmentVariable()).isEqualTo("CONFLUENCE_GRPC_TARGET");
                    assertThat(provider.defaultTarget()).isEqualTo("localhost:9095");
                });
    }

    @Test
    void confluenceToolsCallStreamingStubs() throws Exception {
        String name = InProcessServerBuilder.generateName();
        grpc = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new ConfluenceServiceGrpc.ConfluenceServiceImplBase() {
                    @Override
                    public void streamSpaces(StreamSpacesRequest request,
                            StreamObserver<StreamSpacesResponse> observer) {
                        observer.onNext(StreamSpacesResponse.newBuilder()
                                .setSpace(Space.newBuilder()
                                        .setId("100")
                                        .setKey("ENG")
                                        .setName("Engineering"))
                                .build());
                        observer.onCompleted();
                    }

                    @Override
                    public void getPage(GetPageRequest request,
                            StreamObserver<GetPageResponse> observer) {
                        observer.onNext(GetPageResponse.newBuilder()
                                .setPage(Page.newBuilder()
                                        .setId(request.getId())
                                        .setSpaceId("100")
                                        .setTitle("Design")
                                        .setWebUrl("https://example/wiki/pages/" + request.getId()))
                                .build());
                        observer.onCompleted();
                    }
                })
                .build()
                .start();
        var confluence = ConfluenceServiceGrpc.newBlockingStub(
                InProcessChannelBuilder.forName(name).directExecutor().build());
        List<io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification> tools =
                new ArrayList<>();
        tools.addAll(ConfluenceMcpTools.of(confluence));
        mcp = McpHttpServer.start(0, "test-mcp", tools);

        HttpClientStreamableHttpTransport transport = HttpClientStreamableHttpTransport
                .builder("http://127.0.0.1:" + mcp.port())
                .endpoint(McpHttpServer.ENDPOINT)
                .build();
        try (McpSyncClient client = McpClient.sync(transport)
                .requestTimeout(Duration.ofSeconds(15))
                .build()) {
            client.initialize();
            assertThat(client.listTools().tools())
                    .extracting(McpSchema.Tool::name)
                    .contains("confluence_list_spaces", "confluence_get_page",
                            "confluence_list_attachments", "confluence_sync", "connection_test");

            McpSchema.CallToolResult spaces = client.callTool(McpSchema.CallToolRequest.builder("confluence_list_spaces")
                    .arguments(Map.of("limit", 1))
                    .build());
            assertThat(spaces.isError()).isFalse();
            assertThat(spaces.content().get(0).toString()).contains("ENG");

            McpSchema.CallToolResult page = client.callTool(McpSchema.CallToolRequest.builder("confluence_get_page")
                    .arguments(Map.of("id", "200"))
                    .build());
            assertThat(page.isError()).isFalse();
            assertThat(page.content().get(0).toString()).contains("Design");
        }
    }
}
