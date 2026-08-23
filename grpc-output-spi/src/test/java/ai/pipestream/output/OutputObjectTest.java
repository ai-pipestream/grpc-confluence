package ai.pipestream.output;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutputObjectTest {

    @Test
    void copiesBytesAndRejectsTraversal() {
        byte[] payload = {1, 2};
        OutputObject object = OutputObject.of("ENG/pages/200.pb", payload, "application/x-protobuf");
        payload[0] = 9;
        assertThat(object.content()[0]).isEqualTo((byte) 1);
        assertThatThrownBy(() -> OutputObject.of("../escape", new byte[0], "text/plain"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OutputObject.of("/abs", new byte[0], "text/plain"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
