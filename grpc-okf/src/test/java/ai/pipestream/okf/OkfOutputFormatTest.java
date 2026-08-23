package ai.pipestream.okf;

import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.Page;
import ai.pipestream.output.OutputObject;
import ai.pipestream.output.OutputStore;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OkfOutputFormatTest {

    @Test
    void completeWritesOkfTreeZipAndWarcThroughStore() throws Exception {
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
        ConfluenceChange change = ConfluenceChange.newBuilder()
                .setChangeId("c")
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setPage(Page.newBuilder().setId("200").setTitle("Doc")
                                .setWebUrl("https://example/wiki/pages/200")))
                .build();
        new OkfOutputFormat().complete(store, List.<Message>of(change), "run-1");
        assertThat(objects).extracting(OutputObject::key)
                .anyMatch(k -> k.equals("run-1/okf/pages/200.md"))
                .anyMatch(k -> k.equals("run-1/okf/index.md"))
                .anyMatch(k -> k.equals("run-1/okf/collection.html"))
                .anyMatch(k -> k.equals("run-1/bundle.zip"))
                .anyMatch(k -> k.equals("run-1/bundle.warc.gz"));
        assertThat(objects).filteredOn(o -> o.key().equals("run-1/bundle.zip"))
                .isNotEmpty()
                .allMatch(o -> o.content().length > 4
                        && o.content()[0] == 'P' && o.content()[1] == 'K');
    }
}
