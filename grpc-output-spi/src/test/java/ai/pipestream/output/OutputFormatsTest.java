package ai.pipestream.output;

import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutputFormatsTest {

    @Test
    void protobufAndJsonWriteThroughStore() throws Exception {
        RecordingOutputStore store = new RecordingOutputStore("memory", true);
        StringValue record = StringValue.of("Doc title");
        new ProtobufOutputFormat().emit(store, record, "ENG/pages/200");
        new JsonOutputFormat().emit(store, record, "ENG/pages/200");
        assertThat(store.objects).extracting(OutputObject::key)
                .containsExactly("ENG/pages/200.pb", "ENG/pages/200.json");
        assertThat(store.objects.get(0).content()).isEqualTo(record.toByteArray());
        assertThat(new String(store.objects.get(1).content(), StandardCharsets.UTF_8))
                .contains("Doc title");
    }

    @Test
    void selectHonorsOutputFormatsAndSkipsMissing() {
        OutputFormats formats = OutputFormats.of(new ProtobufOutputFormat(), new JsonOutputFormat());
        assertThat(formats.has("protobuf")).isTrue();
        assertThat(formats.has("okf")).isFalse();
        assertThat(formats.select(Map.of(OutputEnv.FORMATS, "protobuf,okf,json")))
                .extracting(OutputFormat::id)
                .containsExactly("protobuf", "json");
    }

    @Test
    void envParsing() {
        assertThat(OutputEnv.storeId(Map.of())).isEqualTo("filesystem");
        assertThat(OutputEnv.formats(Map.of())).containsExactly("okf");
        assertThat(OutputEnv.formats(Map.of(OutputEnv.FORMATS, "protobuf, json")))
                .containsExactly("protobuf", "json");
        assertThat(OutputEnv.destinationSet(Map.of(OutputEnv.S3_BUCKET, "b"))).isTrue();
        assertThat(OutputEnv.destinationSet(Map.of())).isFalse();
        assertThat(OutputEnv.directory(Map.of(OutputEnv.OKF_DIR, "/tmp/okf"))).isEqualTo("/tmp/okf");
    }
}
