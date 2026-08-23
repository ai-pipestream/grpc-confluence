package ai.pipestream.okf;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OkfActorTest {

    @Test
    void prefixes() {
        assertThat(OkfActor.process("grpc-confluence/okf-producer"))
                .isEqualTo("process:grpc-confluence/okf-producer");
        assertThat(OkfActor.human("ada")).isEqualTo("human:ada");
        assertThat(OkfActor.agent("codex", "gpt")).isEqualTo("codex/gpt");
    }

    @Test
    void humanActorIsCaseInsensitivePrefix() {
        assertThat(OkfActor.humanActor("human:ada")).isTrue();
        assertThat(OkfActor.humanActor("HUMAN:ada")).isTrue();
        assertThat(OkfActor.humanActor("process:bot")).isFalse();
        assertThat(OkfActor.humanActor(null)).isFalse();
    }

    @Test
    void rejectsNullName() {
        assertThatThrownBy(() -> OkfActor.process(null)).isInstanceOf(NullPointerException.class);
    }
}
