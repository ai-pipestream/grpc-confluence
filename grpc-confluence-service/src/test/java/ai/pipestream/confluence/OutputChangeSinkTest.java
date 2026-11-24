package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.output.OutputEnv;
import ai.pipestream.output.OutputFormat;
import ai.pipestream.output.OutputFormats;
import ai.pipestream.output.OutputObject;
import ai.pipestream.output.OutputStore;
import ai.pipestream.output.OutputStores;
import ai.pipestream.output.ProtobufOutputFormat;
import ai.pipestream.output.filesystem.FilesystemOutputStore;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutputChangeSinkTest {

    @Test
    void writesProtobufUnderConfluenceHierarchy(@TempDir Path dir) throws Exception {
        FilesystemOutputStore store = new FilesystemOutputStore(dir);
        store.open(Map.of());
        OutputChangeSink sink = new OutputChangeSink(store, List.of(new ProtobufOutputFormat()),
                "run-1");
        ConfluenceChange change = ConfluenceChange.newBuilder()
                .setChangeId("c1")
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setPage(Page.newBuilder().setId("200").setSpaceId("ENG").setTitle("Doc")
                                .setWebUrl("https://example/wiki/pages/200")))
                .build();
        sink.emit(change);
        sink.completeRun("run-1");
        Path pb = dir.resolve("run-1/ENG/pages/200.pb");
        assertThat(pb).exists();
        assertThat(Files.readAllBytes(pb)).isEqualTo(change.toByteArray());
    }

    @Test
    void fromSelectsFilesystemAndLoadedFormats(@TempDir Path dir) {
        OutputStores stores = OutputStores.of(new FilesystemOutputStore());
        OutputFormats formats = OutputFormats.of(new ProtobufOutputFormat());
        OutputChangeSink sink = OutputChangeSink.from(
                Map.of(OutputEnv.DIR, dir.toString(), OutputEnv.FORMATS, "protobuf"),
                stores, formats);
        assertThat(sink.store().id()).isEqualTo("filesystem");
        assertThat(sink.formats()).extracting(OutputFormat::id).containsExactly("protobuf");
        sink.close();
    }

    @Test
    void recordingStoreCollectsPuts() {
        List<OutputObject> objects = new ArrayList<>();
        OutputStore store = new OutputStore() {
            @Override
            public String id() {
                return "memory";
            }

            @Override
            public boolean available(Map<String, String> env) {
                return true;
            }

            @Override
            public void open(Map<String, String> env) {
            }

            @Override
            public void put(OutputObject object) {
                objects.add(object);
            }
        };
        OutputChangeSink sink = new OutputChangeSink(store, List.of(new ProtobufOutputFormat()), "");
        sink.emit(ConfluenceChange.newBuilder()
                .setChangeId("c")
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setPage(Page.newBuilder().setId("200").setSpaceId("ENG")))
                .build());
        assertThat(objects).extracting(OutputObject::key).containsExactly("ENG/pages/200.pb");
    }

    /** An end-of-run format that records what each completed pass handed it. */
    private static final class RecordingBundleFormat implements OutputFormat {

        private final List<String> prefixes = new ArrayList<>();
        private final List<List<String>> titles = new ArrayList<>();

        @Override
        public String id() {
            return "recording";
        }

        @Override
        public boolean supports(Message record) {
            return true;
        }

        @Override
        public void complete(OutputStore store, List<Message> records, String prefix) {
            prefixes.add(prefix);
            titles.add(records.stream()
                    .map(r -> ((ConfluenceChange) r).getEntity().getPage().getTitle())
                    .toList());
        }
    }

    private static OutputChangeSink sink(RecordingBundleFormat format, Path dir, String prefix)
            throws Exception {
        FilesystemOutputStore store = new FilesystemOutputStore(dir);
        store.open(Map.of());
        return new OutputChangeSink(store, List.of(format), prefix);
    }

    private static ConfluenceChange page(String id, String space, String title) {
        return ConfluenceChange.newBuilder()
                .setChangeId(id + title)
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId(id)
                        .setPage(Page.newBuilder().setId(id).setSpaceId(space).setTitle(title)))
                .build();
    }

    @Test
    void anIncrementalPassHandsTheEndOfRunFormatsTheMergedBundle(@TempDir Path dir)
            throws Exception {
        RecordingBundleFormat format = new RecordingBundleFormat();
        OutputChangeSink sink = sink(format, dir, "kb");
        sink.emit(page("200", "ENG", "Doc"));
        sink.emit(page("201", "ENG", "Other"));
        sink.completeRun("run-1", true);

        sink.emit(page("200", "ENG", "Doc, edited"));
        sink.completeRun("2026-01-01T00:00:00Z", false);

        assertThat(format.prefixes).containsExactly("kb", "kb");
        assertThat(format.titles.get(1)).containsExactly("Doc, edited", "Other");
    }

    @Test
    void aFullSweepHandsTheEndOfRunFormatsOnlyWhatItEmitted(@TempDir Path dir) throws Exception {
        RecordingBundleFormat format = new RecordingBundleFormat();
        OutputChangeSink sink = sink(format, dir, "kb");
        sink.emit(page("200", "ENG", "Doc"));
        sink.emit(page("201", "ENG", "Other"));
        sink.completeRun("run-1");

        sink.emit(page("200", "ENG", "Doc"));
        sink.emit(page("200", "ENG", "Doc, again"));
        sink.completeRun("run-2");

        assertThat(format.titles.get(1)).containsExactly("Doc, again");
    }

    @Test
    void anIncrementalPassWithNoFullSweepBeforeItGoesUnderIncrements(@TempDir Path dir)
            throws Exception {
        RecordingBundleFormat format = new RecordingBundleFormat();
        OutputChangeSink sink = sink(format, dir, "kb");
        sink.emit(page("200", "ENG", "Doc, edited"));

        sink.completeRun("2026-01-01T00:00:00Z", false);

        assertThat(format.prefixes).containsExactly("kb/increments/2026-01-01T00_00_00Z");
        assertThat(format.titles.get(0)).containsExactly("Doc, edited");
    }

    @Test
    void anIncrementWithNoPrefixStillGoesUnderIncrements(@TempDir Path dir) throws Exception {
        RecordingBundleFormat format = new RecordingBundleFormat();
        OutputChangeSink sink = sink(format, dir, "");
        sink.emit(page("200", "ENG", "Doc, edited"));

        sink.completeRun("2026-01-01T00:00:00Z", false);

        assertThat(format.prefixes).containsExactly("increments/2026-01-01T00_00_00Z");
    }

    @Test
    void anIncrementalPassThatEmittedNothingWritesNoBundle(@TempDir Path dir) throws Exception {
        RecordingBundleFormat format = new RecordingBundleFormat();
        OutputChangeSink sink = sink(format, dir, "kb");
        sink.emit(page("200", "ENG", "Doc"));
        sink.completeRun("run-1", true);

        sink.completeRun("2026-01-01T00:00:00Z", false);

        assertThat(format.prefixes).containsExactly("kb");
    }

    @Test
    void aFullSweepOfOneConnectionKeepsTheOtherConnectionInTheBundle(@TempDir Path dir)
            throws Exception {
        RecordingBundleFormat format = new RecordingBundleFormat();
        OutputChangeSink sink = sink(format, dir, "kb");
        ChangeSink acme = sink.forConnection("acme");
        acme.emit(page("200", "ENG", "Acme doc"));
        acme.completeRun("run-a", true);
        ChangeSink globex = sink.forConnection("globex");
        globex.emit(page("900", "OPS", "Globex doc"));
        globex.completeRun("run-g", true);

        ChangeSink acmeAgain = sink.forConnection("acme");
        acmeAgain.emit(page("201", "ENG", "Acme second doc"));
        acmeAgain.completeRun("run-a2", true);

        assertThat(format.titles.getLast())
                .containsExactlyInAnyOrder("Globex doc", "Acme second doc");
    }

    @Test
    void anIncrementOfOneConnectionAmongSeveralGoesUnderItsConnection(@TempDir Path dir)
            throws Exception {
        RecordingBundleFormat format = new RecordingBundleFormat();
        OutputChangeSink sink = sink(format, dir, "kb");
        ChangeSink globex = sink.forConnection("globex");
        globex.emit(page("900", "OPS", "Globex doc"));

        globex.completeRun("2026-01-01T00:00:00Z", false);

        assertThat(format.prefixes).containsExactly("kb/increments/globex/2026-01-01T00_00_00Z");
    }

    @Test
    void perRecordFormatsStillWriteEachChangeOfABoundConnection(@TempDir Path dir)
            throws Exception {
        FilesystemOutputStore store = new FilesystemOutputStore(dir);
        store.open(Map.of());
        OutputChangeSink sink = new OutputChangeSink(store, List.of(new ProtobufOutputFormat()),
                "kb");

        sink.forConnection("acme").emit(page("200", "ENG", "Doc"));

        assertThat(dir.resolve("kb/ENG/pages/200.pb")).exists();
    }
}
