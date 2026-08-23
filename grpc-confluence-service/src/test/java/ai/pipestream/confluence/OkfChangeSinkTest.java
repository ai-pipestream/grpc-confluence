package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.okf.OkfOutput;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class OkfChangeSinkTest {

    @Test
    void completeRunWritesOkfZipAndWarc(@TempDir Path dir) throws Exception {
        Path tree = dir.resolve("okf");
        OkfChangeSink sink = new OkfChangeSink(new OkfOutput(
                tree, dir.resolve("okf.zip"), dir.resolve("okf.warc.gz")));
        sink.emit(ConfluenceChange.newBuilder()
                .setChangeId("c1")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setIngestedAt(Timestamp.newBuilder().setSeconds(1))
                        .setPage(Page.newBuilder()
                                .setId("200")
                                .setTitle("Doc")
                                .setWebUrl("https://example.atlassian.net/wiki/pages/200")))
                .build());
        sink.completeRun("run-1");

        assertThat(tree.resolve("pages/200.md")).exists();
        assertThat(tree.resolve("collection.html")).exists();
        assertThat(Files.readString(tree.resolve("collection.html")))
                .contains("href=\"https://example.atlassian.net/wiki/pages/200\"");
        assertThat(dir.resolve("okf.zip")).exists();
        assertThat(dir.resolve("okf.warc.gz")).exists();
    }
}
