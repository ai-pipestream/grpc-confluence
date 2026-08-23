package ai.pipestream.output.s3;

import ai.pipestream.output.OutputEnv;
import ai.pipestream.output.OutputObject;
import ai.pipestream.output.OutputStores;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class S3OutputStoreTest {

    record Put(String bucket, String key, byte[] body, String contentType) {
    }

    @Test
    void putsUnderPrefixAndRegistersWithServiceLoader() throws Exception {
        List<Put> puts = new ArrayList<>();
        S3OutputStore store = new S3OutputStore(
                (bucket, key, body, contentType) -> puts.add(new Put(bucket, key, body, contentType)),
                "knowledge", "okf-runs");
        store.open(Map.of());
        store.put(OutputObject.of("ENG/pages/200.md", "# Doc".getBytes(),
                "text/markdown; charset=utf-8"));
        assertThat(puts).hasSize(1);
        assertThat(puts.get(0).bucket()).isEqualTo("knowledge");
        assertThat(puts.get(0).key()).isEqualTo("okf-runs/ENG/pages/200.md");
        assertThat(new String(puts.get(0).body())).isEqualTo("# Doc");

        OutputStores loaded = OutputStores.load(S3OutputStore.class.getClassLoader());
        assertThat(loaded.has("s3")).isTrue();
        assertThat(loaded.find("s3").orElseThrow().available(Map.of(OutputEnv.S3_BUCKET, "b")))
                .isTrue();
        assertThat(loaded.find("s3").orElseThrow().available(Map.of())).isFalse();
    }

    @Test
    void availableRequiresBucket() {
        S3OutputStore store = new S3OutputStore();
        assertThat(store.available(Map.of())).isFalse();
        assertThat(store.available(Map.of(OutputEnv.S3_BUCKET, "bucket"))).isTrue();
    }
}
