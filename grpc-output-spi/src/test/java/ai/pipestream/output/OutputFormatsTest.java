package ai.pipestream.output;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.microsoft.v1.DriveItem;
import ai.pipestream.microsoft.v1.MicrosoftChange;
import ai.pipestream.microsoft.v1.MicrosoftEntity;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutputFormatsTest {

    @Test
    void protobufAndJsonAndConnectorWriteThroughStore() throws Exception {
        RecordingOutputStore store = new RecordingOutputStore("memory", true);
        ConfluenceChange page = ConfluenceChange.newBuilder()
                .setChangeId("c")
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setPage(Page.newBuilder().setId("200").setTitle("Doc").setSpaceId("ENG")))
                .build();
        new ProtobufOutputFormat().emit(store, page, "ENG/pages/200");
        new JsonOutputFormat().emit(store, page, "ENG/pages/200");
        assertThat(store.objects).extracting(OutputObject::key)
                .containsExactly("ENG/pages/200.pb", "ENG/pages/200.json");
        assertThat(store.objects.get(0).content()).isEqualTo(page.toByteArray());
        assertThat(new String(store.objects.get(1).content(), StandardCharsets.UTF_8))
                .contains("Doc").contains("title");

        MicrosoftChange file = MicrosoftChange.newBuilder()
                .setChangeId("m")
                .setEntity(MicrosoftEntity.newBuilder()
                        .setEntityId("file-1")
                        .setDriveItem(DriveItem.newBuilder().setId("file-1").setDriveId("d")))
                .build();
        MicrosoftConnectorOutputFormat connector = new MicrosoftConnectorOutputFormat();
        assertThat(connector.supports(page)).isFalse();
        assertThat(connector.supports(file)).isTrue();
        connector.emit(store, file, "drives/d/items/file-1");
        assertThat(store.objects.get(2).key()).isEqualTo("drives/d/items/file-1.connector.pb");
        assertThat(store.objects.get(2).content()).isEqualTo(file.toByteArray());
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
