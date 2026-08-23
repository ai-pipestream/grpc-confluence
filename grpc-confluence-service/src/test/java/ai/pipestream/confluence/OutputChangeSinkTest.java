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
}
