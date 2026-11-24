package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.okf.OkfBundle;
import ai.pipestream.okf.OkfConformance;
import ai.pipestream.okf.OkfOutput;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class OkfChangeSinkTest {

    private static Timestamp ts() {
        return Timestamp.newBuilder().setSeconds(1).build();
    }

    @Test
    void completeRunWritesOkfZipAndWarc(@TempDir Path dir) throws Exception {
        Path tree = dir.resolve("okf");
        OkfChangeSink sink = new OkfChangeSink(new OkfOutput(
                tree, dir.resolve("okf.zip"), dir.resolve("okf.warc.gz")));
        sink.emit(page("200", "Doc"));
        sink.completeRun("run-1");

        assertThat(tree.resolve("pages/200.md")).exists();
        assertThat(tree.resolve("collection.html")).exists();
        assertThat(Files.readString(tree.resolve("collection.html")))
                .contains("href=\"https://example.atlassian.net/wiki/pages/200\"");
        assertThat(dir.resolve("okf.zip")).exists();
        assertThat(dir.resolve("okf.warc.gz")).exists();
        assertThat(Files.readString(tree.resolve("pages/200.md"))).contains("title: Doc");
    }

    @Test
    void lastUpsertWinsAndDeleteMarksDeprecated(@TempDir Path dir) throws Exception {
        Path tree = dir.resolve("okf");
        OkfChangeSink sink = new OkfChangeSink(new OkfOutput(tree, dir.resolve("okf.zip"),
                dir.resolve("okf.warc.gz")));
        sink.emit(page("200", "Draft"));
        sink.emit(page("200", "Published"));
        sink.emit(ConfluenceChange.newBuilder()
                .setChangeId("del")
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setIngestedAt(ts())
                        .setPage(Page.newBuilder().setId("200").setTitle("Published")
                                .setWebUrl("https://example.atlassian.net/wiki/pages/200")))
                .build());
        sink.completeRun("run-2");
        String md = Files.readString(tree.resolve("pages/200.md"));
        assertThat(md).contains("title: Published");
        assertThat(md).doesNotContain("title: Draft");
        assertThat(md).contains("status: deprecated");
        OkfBundle check = new OkfBundle();
        check.putText("pages/200.md", md);
        assertThat(OkfConformance.check(check)).isEmpty();
    }

    @Test
    void anIncrementalPassMergesIntoTheBundleOfTheLastFullSweep(@TempDir Path dir) throws Exception {
        Path tree = dir.resolve("okf");
        OkfChangeSink sink = new OkfChangeSink(new OkfOutput(tree, dir.resolve("okf.zip"),
                dir.resolve("okf.warc.gz")));
        sink.emit(page("200", "Doc"));
        sink.emit(page("201", "Other"));
        sink.completeRun("run-1", true);

        sink.emit(page("200", "Doc, edited"));
        sink.completeRun("2026-01-01T00:00:00Z", false);

        assertThat(Files.readString(tree.resolve("pages/200.md"))).contains("title: \"Doc, edited\"");
        assertThat(Files.readString(tree.resolve("index.md"))).contains("pages");
        assertThat(Files.readString(tree.resolve("pages/index.md"))).contains("200.md", "201.md");
    }

    @Test
    void aFullSweepDropsWhatItDidNotEmit(@TempDir Path dir) throws Exception {
        Path tree = dir.resolve("okf");
        OkfChangeSink sink = new OkfChangeSink(new OkfOutput(tree, dir.resolve("okf.zip"),
                dir.resolve("okf.warc.gz")));
        sink.emit(page("200", "Doc"));
        sink.emit(page("201", "Other"));
        sink.completeRun("run-1", true);

        sink.emit(page("200", "Doc"));
        sink.completeRun("run-2", true);

        assertThat(Files.readString(tree.resolve("pages/index.md")))
                .contains("200.md").doesNotContain("201.md");
    }

    @Test
    void anIncrementalPassWithNoFullSweepBeforeItWritesBesideTheBundle(@TempDir Path dir)
            throws Exception {
        Path tree = dir.resolve("okf");
        OkfChangeSink sink = new OkfChangeSink(new OkfOutput(tree, dir.resolve("okf.zip"),
                dir.resolve("okf.warc.gz")));
        sink.emit(page("200", "Doc, edited"));

        sink.completeRun("2026-01-01T00:00:00Z", false);

        Path increment = dir.resolve("okf-increments-2026-01-01T00_00_00Z");
        assertThat(increment.resolve("pages/200.md")).exists();
        assertThat(dir.resolve("okf-increments-2026-01-01T00_00_00Z.zip")).exists();
        assertThat(dir.resolve("okf-increments-2026-01-01T00_00_00Z.warc.gz")).exists();
        assertThat(tree).doesNotExist();
        assertThat(dir.resolve("okf.zip")).doesNotExist();
    }

    @Test
    void anIncrementalPassThatEmittedNothingWritesNothing(@TempDir Path dir) {
        Path tree = dir.resolve("okf");
        OkfChangeSink sink = new OkfChangeSink(new OkfOutput(tree, dir.resolve("okf.zip"),
                dir.resolve("okf.warc.gz")));

        sink.completeRun("2026-01-01T00:00:00Z", false);

        assertThat(dir).isEmptyDirectory();
    }

    @Test
    void aFullSweepOfOneConnectionKeepsTheOtherConnectionInTheBundle(@TempDir Path dir)
            throws Exception {
        Path tree = dir.resolve("okf");
        OkfChangeSink sink = new OkfChangeSink(new OkfOutput(tree, dir.resolve("okf.zip"),
                dir.resolve("okf.warc.gz")));
        ChangeSink acme = sink.forConnection("acme");
        ChangeSink globex = sink.forConnection("globex");
        acme.emit(page("200", "Acme doc"));
        acme.completeRun("run-a", true);
        globex.emit(page("900", "Globex doc"));
        globex.completeRun("run-g", true);

        ChangeSink acmeAgain = sink.forConnection("acme");
        acmeAgain.emit(page("201", "Acme second doc"));
        acmeAgain.completeRun("run-a2", true);

        assertThat(Files.readString(tree.resolve("pages/index.md")))
                .contains("201.md", "900.md").doesNotContain("200.md");
    }

    @Test
    void anIncrementOfOneConnectionAmongSeveralCarriesTheConnectionInItsName(@TempDir Path dir) {
        OkfChangeSink sink = new OkfChangeSink(new OkfOutput(dir.resolve("okf"),
                dir.resolve("okf.zip"), dir.resolve("okf.warc.gz")));
        ChangeSink globex = sink.forConnection("globex");
        globex.emit(page("900", "Globex doc"));

        globex.completeRun("2026-01-01T00:00:00Z", false);

        assertThat(dir.resolve("okf-increments-globex-2026-01-01T00_00_00Z/pages/900.md")).exists();
    }

    private static ConfluenceChange page(String id, String title) {
        return ConfluenceChange.newBuilder()
                .setChangeId(id + title)
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId(id)
                        .setIngestedAt(ts())
                        .setPage(Page.newBuilder()
                                .setId(id)
                                .setTitle(title)
                                .setWebUrl("https://example.atlassian.net/wiki/pages/" + id)))
                .build();
    }
}
