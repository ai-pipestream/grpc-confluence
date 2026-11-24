package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.Body;
import ai.pipestream.confluence.v1.BodyFormat;
import ai.pipestream.confluence.v1.BodyType;
import ai.pipestream.confluence.v1.CommentKind;
import ai.pipestream.confluence.v1.ConfluenceServiceGrpc;
import ai.pipestream.confluence.v1.GetBlogPostRequest;
import ai.pipestream.confluence.v1.GetCommentRequest;
import ai.pipestream.confluence.v1.GetCustomContentRequest;
import ai.pipestream.confluence.v1.GetPageRequest;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * A single read accepts every body format its route does, so the answer in
 * that format has to come back: Confluence keys the body by representation
 * ({@code body.export_view}, {@code body.styled_view}, ...), and a
 * representation with no slot in {@code Body} left the reply OK with an
 * empty body. Whatever Confluence puts under {@code body} either lands in a
 * typed slot or fails the read.
 */
class SingleReadBodyFormatTest {

    private FakeConfluenceServer fake;
    private Server server;
    private ManagedChannel channel;
    private ConfluenceServiceGrpc.ConfluenceServiceBlockingStub stub;

    @BeforeEach
    void start() throws Exception {
        fake = FakeConfluenceServer.start();
        ConfluenceConnectorConfig config = ConfluenceConnectorConfig.builder()
                .baseUrl(fake.baseUrl())
                .email("bot@pipestream.ai")
                .apiToken("token-123")
                .build();
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .addService(new ConfluenceGrpcService(config,
                        new ConfluenceClient(config.baseUrl(), config.email(), config.apiToken(),
                                Duration.ZERO),
                        ConfluenceGrpcService.DEFAULT_ATTACHMENT_MAX_BYTES))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        stub = ConfluenceServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void stop() {
        channel.shutdownNow();
        server.shutdownNow();
        fake.close();
    }

    @ParameterizedTest
    @EnumSource(value = BodyFormat.class, names = {"BODY_FORMAT_STORAGE_XHTML",
            "BODY_FORMAT_ATLAS_DOC_FORMAT", "BODY_FORMAT_RENDERED_XHTML", "BODY_FORMAT_EXPORT_XHTML",
            "BODY_FORMAT_ANONYMOUS_EXPORT_XHTML", "BODY_FORMAT_STYLED_XHTML", "BODY_FORMAT_EDITOR"})
    void getPageReturnsTheBodyInEveryFormatItAccepts(BodyFormat format) {
        String key = ConfluenceRoutes.representation(format);
        fake.stub("/wiki/api/v2/pages/200", page(body(key, "<p>page as " + key + "</p>")));

        Body body = stub.getPage(GetPageRequest.newBuilder().setId("200").setBodyFormat(format)
                .build()).getPage().getBody();

        assertThat(fake.lastTo("/wiki/api/v2/pages/200").params()).containsEntry("body-format", key);
        assertThat(bodies(body)).as("the %s body of the page", key)
                .containsExactly(BodyType.newBuilder().setFormat(format)
                        .setValue("<p>page as " + key + "</p>").build());
    }

    @ParameterizedTest
    @EnumSource(value = BodyFormat.class, names = {"BODY_FORMAT_EXPORT_XHTML",
            "BODY_FORMAT_ANONYMOUS_EXPORT_XHTML", "BODY_FORMAT_STYLED_XHTML", "BODY_FORMAT_EDITOR"})
    void getBlogPostReturnsTheBodyInTheRenderedFormats(BodyFormat format) {
        String key = ConfluenceRoutes.representation(format);
        fake.stub("/wiki/api/v2/blogposts/300", """
                {"id":"300","status":"current","title":"News","spaceId":"100","authorId":"acc-1",
                 "createdAt":"2024-02-03T09:00:00.000Z",
                 "version":{"createdAt":"2024-02-03T09:00:00.000Z","number":1,"authorId":"acc-1"},
                 "body":%s,"_links":{"webui":"/spaces/ENG/blogposts/300"}}
                """.formatted(body(key, "<p>post</p>")));

        Body body = stub.getBlogPost(GetBlogPostRequest.newBuilder().setId("300")
                .setBodyFormat(format).build()).getBlogPost().getBody();

        assertThat(bodies(body)).as("the %s body of the blog post", key)
                .containsExactly(BodyType.newBuilder().setFormat(format).setValue("<p>post</p>")
                        .build());
    }

    @ParameterizedTest
    @EnumSource(value = BodyFormat.class, names = {"BODY_FORMAT_EXPORT_XHTML",
            "BODY_FORMAT_ANONYMOUS_EXPORT_XHTML", "BODY_FORMAT_STYLED_XHTML", "BODY_FORMAT_EDITOR"})
    void getCommentReturnsTheBodyInTheRenderedFormats(BodyFormat format) {
        String key = ConfluenceRoutes.representation(format);
        fake.stub("/wiki/api/v2/footer-comments/c1", """
                {"id":"c1","status":"current","title":"re: page","pageId":"200",
                 "version":{"createdAt":"2024-02-05T11:00:00.000Z","number":1,"authorId":"acc-3"},
                 "body":%s,"_links":{"webui":"/spaces/ENG/pages/200#comment-c1"}}
                """.formatted(body(key, "<p>Nice</p>")));

        Body body = stub.getComment(GetCommentRequest.newBuilder().setId("c1")
                .setKind(CommentKind.COMMENT_KIND_FOOTER).setBodyFormat(format).build())
                .getComment().getBody();

        assertThat(bodies(body)).as("the %s body of the comment", key)
                .containsExactly(BodyType.newBuilder().setFormat(format).setValue("<p>Nice</p>")
                        .build());
    }

    @ParameterizedTest
    @EnumSource(value = BodyFormat.class, names = {"BODY_FORMAT_RAW", "BODY_FORMAT_RENDERED_XHTML",
            "BODY_FORMAT_EXPORT_XHTML", "BODY_FORMAT_ANONYMOUS_EXPORT_XHTML"})
    void getCustomContentReturnsTheBodyInEveryFormatItAccepts(BodyFormat format) {
        String key = ConfluenceRoutes.representation(format);
        fake.stub("/wiki/api/v2/custom-content/cc1", custom(body(key, "thing as " + key)));

        Body body = stub.getCustomContent(GetCustomContentRequest.newBuilder().setId("cc1")
                .setBodyFormat(format).build()).getCustomContent().getBody();

        assertThat(bodies(body)).as("the %s body of the custom content", key)
                .containsExactly(BodyType.newBuilder().setFormat(format)
                        .setValue("thing as " + key).build());
    }

    @Test
    void aBodyUnderAKeyWithNoSlotFailsTheReadInsteadOfComingBackEmpty() {
        fake.stub("/wiki/api/v2/pages/200",
                page("{\"exportView\":{\"representation\":\"export_view\",\"value\":\"<p>x</p>\"}}"));

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.getPage(GetPageRequest.newBuilder().setId("200")
                        .setBodyFormat(BodyFormat.BODY_FORMAT_EXPORT_XHTML).build()));

        assertThat(failure).as("GetPage whose answer keeps the body under a key Body cannot hold")
                .isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(failure.getStatus().getDescription()).contains("body", "exportView");
    }

    @Test
    void aCustomContentBodyUnderAKeyWithNoSlotFailsTheRead() {
        fake.stub("/wiki/api/v2/custom-content/cc1",
                custom("{\"wiki\":{\"representation\":\"wiki\",\"value\":\"h1. x\"}}"));

        StatusRuntimeException failure = catchThrowableOfType(StatusRuntimeException.class,
                () -> stub.getCustomContent(GetCustomContentRequest.newBuilder().setId("cc1")
                        .build()));

        assertThat(failure).as("GetCustomContent whose answer holds a body Body cannot hold")
                .isNotNull();
        assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
        assertThat(failure.getStatus().getDescription()).contains("wiki");
    }

    @Test
    void anAnswerWithNoBodyIsNotAFailure() {
        fake.stub("/wiki/api/v2/pages/200", page("{}"));

        Body body = stub.getPage(GetPageRequest.newBuilder().setId("200")
                .setBodyFormat(BodyFormat.BODY_FORMAT_EXPORT_XHTML).build()).getPage().getBody();

        assertThat(bodies(body)).isEmpty();
    }

    /** Every populated slot of {@code body}, whatever the slot is called. */
    private static List<BodyType> bodies(Body body) {
        return body.getAllFields().values().stream().map(BodyType.class::cast).toList();
    }

    private static String body(String key, String value) {
        return "{\"" + key + "\":{\"representation\":\"" + key + "\",\"value\":\"" + value + "\"}}";
    }

    private static String page(String body) {
        return """
                {"id":"200","status":"current","title":"Design Doc","spaceId":"100",
                 "parentType":"page","authorId":"acc-1","createdAt":"2024-02-01T10:00:00.000Z",
                 "version":{"createdAt":"2024-03-02T00:00:00Z","number":3,"authorId":"acc-2"},
                 "body":%s,"_links":{"webui":"/spaces/ENG/pages/200/Design+Doc"}}
                """.formatted(body);
    }

    private static String custom(String body) {
        return """
                {"id":"cc1","type":"ac:app:thing","status":"current","title":"Thing",
                 "spaceId":"100","pageId":"200","authorId":"acc-1",
                 "createdAt":"2024-02-01T10:00:00.000Z",
                 "version":{"number":4,"createdAt":"2024-02-02T10:00:00.000Z"},
                 "body":%s,"_links":{"webui":"/spaces/ENG/customcontent/cc1"}}
                """.formatted(body);
    }
}
