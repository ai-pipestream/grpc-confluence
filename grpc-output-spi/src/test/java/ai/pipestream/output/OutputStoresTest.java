package ai.pipestream.output;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutputStoresTest {

    @Test
    void hasAndSelectDefaultFilesystem() throws Exception {
        RecordingOutputStore filesystem = new RecordingOutputStore("filesystem", true);
        RecordingOutputStore s3 = new RecordingOutputStore("s3", true);
        OutputStores stores = OutputStores.of(filesystem, s3);
        assertThat(stores.has("filesystem")).isTrue();
        assertThat(stores.has("s3")).isTrue();
        assertThat(stores.has("gcs")).isFalse();
        assertThat(stores.ids()).containsExactly("filesystem", "s3");
        assertThat(stores.select(Map.of()).id()).isEqualTo("filesystem");
        assertThat(filesystem.opened).isTrue();
        assertThat(stores.select(Map.of(OutputEnv.STORE, "s3")).id()).isEqualTo("s3");
    }

    @Test
    void selectFailsWhenStoreNotLoaded() {
        OutputStores stores = OutputStores.of(new RecordingOutputStore("filesystem", true));
        assertThatThrownBy(() -> stores.select(Map.of(OutputEnv.STORE, "s3")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not loaded");
    }

    @Test
    void selectFailsWhenLoadedButNotConfigured() {
        OutputStores stores = OutputStores.of(new RecordingOutputStore("s3", false));
        assertThatThrownBy(() -> stores.select(Map.of(OutputEnv.STORE, "s3")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not configured");
    }

    @Test
    void availableFilters() {
        OutputStores stores = OutputStores.of(
                new RecordingOutputStore("filesystem", true),
                new RecordingOutputStore("s3", false));
        assertThat(stores.available(Map.of())).extracting(OutputStore::id)
                .containsExactly("filesystem");
    }
}
