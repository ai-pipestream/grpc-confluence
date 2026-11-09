package ai.pipestream.output;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ObjectKeysTest {

    @Test
    void underAndSegment() {
        assertThat(ObjectKeys.under("run-1", "ENG/pages/200")).isEqualTo("run-1/ENG/pages/200");
        assertThat(ObjectKeys.under("", "ENG/pages/200")).isEqualTo("ENG/pages/200");
        assertThat(ObjectKeys.segment("a/b c")).isEqualTo("a_b_c");
    }
}
