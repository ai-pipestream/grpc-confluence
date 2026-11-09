package ai.pipestream.confluence;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The regex-free helpers match the {@code String.split} / pattern behaviour they replace. */
class StringsTest {

    @Test
    void stripTrailingSlashes() {
        assertThat(Strings.stripTrailingSlashes("https://x/wiki///")).isEqualTo("https://x/wiki");
        assertThat(Strings.stripTrailingSlashes("https://x/wiki")).isEqualTo("https://x/wiki");
        assertThat(Strings.stripTrailingSlashes("///")).isEmpty();
        assertThat(Strings.stripTrailingSlashes("")).isEmpty();
    }

    @Test
    void splitMatchesStringSplitForALiteralSeparator() {
        for (String value : List.of("", "a", "a,b", "a,,b", "a,b,", "a,b,,", ",a", ",", ",,", " a , b ")) {
            assertThat(Strings.split(value, ','))
                    .as("split of '%s'", value)
                    .containsExactly(value.split(","));
        }
    }

    @Test
    void uuidAndEmailShapes() {
        assertThat(Strings.isUuid("123e4567-e89b-12d3-a456-426614174000")).isTrue();
        assertThat(Strings.isUuid("123E4567-E89B-12D3-A456-426614174000")).isTrue();
        assertThat(Strings.isUuid("123e4567e89b12d3a456426614174000")).isFalse();
        assertThat(Strings.isUuid("123e4567-e89b-12d3-a456-42661417400g")).isFalse();
        assertThat(Strings.isEmailShaped("ada@example.com")).isTrue();
        assertThat(Strings.isEmailShaped("ada@sub.example.co")).isTrue();
        for (String bad : List.of("ada", "@example.com", "ada@", "ada@example", "ada@.com",
                "ada@example.", "a da@example.com", "ada@ex@ample.com")) {
            assertThat(Strings.isEmailShaped(bad)).as(bad).isFalse();
        }
    }
}
