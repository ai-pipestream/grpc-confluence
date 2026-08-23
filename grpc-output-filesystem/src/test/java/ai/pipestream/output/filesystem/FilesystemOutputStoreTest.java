package ai.pipestream.output.filesystem;

import ai.pipestream.output.OutputEnv;
import ai.pipestream.output.OutputObject;
import ai.pipestream.output.OutputStores;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FilesystemOutputStoreTest {

    @Test
    void writesHierarchyAndRegistersWithServiceLoader(@TempDir Path dir) throws Exception {
        FilesystemOutputStore store = new FilesystemOutputStore();
        assertThat(store.available(Map.of())).isFalse();
        store.open(Map.of(OutputEnv.DIR, dir.toString()));
        store.put(OutputObject.of("ENG/pages/200.pb", "hi".getBytes(), "application/x-protobuf"));
        assertThat(Files.readString(dir.resolve("ENG/pages/200.pb"))).isEqualTo("hi");

        OutputStores loaded = OutputStores.load(FilesystemOutputStore.class.getClassLoader());
        assertThat(loaded.has("filesystem")).isTrue();
        assertThat(loaded.has("s3")).isFalse();
    }

    @Test
    void boundRootWritesWithoutEnv(@TempDir Path dir) throws Exception {
        FilesystemOutputStore store = new FilesystemOutputStore(dir);
        store.open(Map.of());
        store.put(OutputObject.of("ok.bin", new byte[] {1}, "application/octet-stream"));
        assertThat(Files.readAllBytes(dir.resolve("ok.bin"))).containsExactly(1);
    }
}
