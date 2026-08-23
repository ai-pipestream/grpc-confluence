package ai.pipestream.okf.confluence;

import ai.pipestream.confluence.v1.Body;
import ai.pipestream.confluence.v1.BodyType;
import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.okf.CatalogEntry;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConfluenceCatalogTest {

    @Test
    void pageUsesLiveWebUrlAsResource() {
        ConfluenceChange change = ConfluenceChange.newBuilder()
                .setChangeId("c1")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setIngestedAt(Timestamp.newBuilder().setSeconds(1))
                        .setPage(Page.newBuilder()
                                .setId("200")
                                .setTitle("Doc")
                                .setWebUrl("https://example.atlassian.net/wiki/pages/200")
                                .setBody(Body.newBuilder().setStorage(
                                        BodyType.newBuilder().setValue("<p>hi</p>")))))
                .build();
        CatalogEntry entry = ConfluenceCatalog.from(change).orElseThrow();
        assertThat(entry.path()).isEqualTo("pages/200.md");
        assertThat(entry.targetUri()).isEqualTo("https://example.atlassian.net/wiki/pages/200");
        assertThat(entry.kind()).isEqualTo("page");
        assertThat(entry.concept().type()).isEqualTo("Page");
        assertThat(entry.concept().resource()).contains(entry.targetUri());
        assertThat(new String(entry.resourceBody())).contains("<p>hi</p>");
    }

    @Test
    void deleteMarksDeprecated() {
        CatalogEntry entry = ConfluenceCatalog.from(ConfluenceChange.newBuilder()
                .setChangeId("c2")
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setIngestedAt(Timestamp.newBuilder().setSeconds(1))
                        .setPage(Page.newBuilder()
                                .setId("200")
                                .setTitle("Doc")
                                .setWebUrl("https://example/wiki/pages/200")))
                .build()).orElseThrow();
        assertThat(entry.concept().status()).contains(ai.pipestream.okf.OkfConcept.Status.DEPRECATED);
    }
}
