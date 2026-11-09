package ai.pipestream.okf;

import ai.pipestream.output.OutputObject;
import ai.pipestream.output.OutputStore;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class OkfOutputFormatTest {

    /** Minimal source that owns {@link StringValue} records. */
    private static final class StringSource implements OkfCatalogSource {

        @Override
        public String id() {
            return "strings";
        }

        @Override
        public boolean supports(Message record) {
            return record instanceof StringValue;
        }

        @Override
        public Optional<CatalogEntry> entry(Message record) {
            String value = ((StringValue) record).getValue();
            String actor = actor();
            OkfConcept concept = OkfConcept.of("Page")
                    .title(value)
                    .generated(new OkfConcept.Generated(actor, java.time.Instant.EPOCH))
                    .build();
            return Optional.of(CatalogEntry.text("pages/" + value + ".md", concept,
                    "https://example/" + value, "text/markdown", value, "page"));
        }

        @Override
        public String title() {
            return "String crawl";
        }

        @Override
        public String description() {
            return "Test source.";
        }

        @Override
        public String actor() {
            return OkfActor.process("test/okf-producer");
        }

        @Override
        public String software() {
            return "test/okf-producer";
        }
    }

    @Test
    void completeWritesOneBundlePerSourceAndSkipsUnknownRecords() throws Exception {
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
        OkfOutputFormat format = new OkfOutputFormat(List.of(new StringSource()));
        assertThat(format.supports(StringValue.of("doc"))).isTrue();
        assertThat(format.supports(com.google.protobuf.Int32Value.of(1))).isFalse();

        format.complete(store, List.of(StringValue.of("doc"),
                com.google.protobuf.Int32Value.of(1)), "run-1");
        assertThat(objects).extracting(OutputObject::key)
                .contains("run-1/okf/index.md", "run-1/bundle.zip", "run-1/bundle.warc.gz");
    }

    @Test
    void noSourcesWritesNothing() throws Exception {
        OkfOutputFormat format = new OkfOutputFormat(List.of());
        assertThat(format.supports(StringValue.of("doc"))).isFalse();
    }
}
