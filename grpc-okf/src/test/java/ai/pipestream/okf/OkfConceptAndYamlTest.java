package ai.pipestream.okf;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OkfConceptAndYamlTest {

    private static final Instant AT = Instant.parse("2026-08-23T12:00:00Z");

    @Test
    void typeIsRequiredAndAttestedComputationNeedsRuntime() {
        assertThatThrownBy(() -> OkfConcept.of("")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OkfConcept.of("Attested Computation").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("runtime");
        assertThat(OkfConcept.of("Attested Computation").runtime("grpc").build().runtime())
                .contains("grpc");
    }

    @Test
    void yamlKeyOrderAndQuoting() {
        OkfConcept concept = OkfConcept.of("Page")
                .title("true")
                .description("a: b")
                .resource("https://example/wiki/pages/200")
                .tag("eng")
                .tag("needs quote")
                .generated(new OkfConcept.Generated("process:test", AT))
                .verified(new OkfConcept.Verified("human:ada", AT))
                .status(OkfConcept.Status.STABLE)
                .source(new OkfConcept.Source("s1", "https://example/wiki/pages/200", "Doc",
                        "process:test", 2L, AT,
                        new OkfConcept.UsageWindow(AT, AT.plusSeconds(60))))
                .extra("confluence_id", "200")
                .body("hello")
                .build();
        String md = OkfYaml.render(concept);
        assertThat(md).startsWith("---\n");
        assertThat(md.indexOf("type:")).isLessThan(md.indexOf("title:"));
        assertThat(md.indexOf("title:")).isLessThan(md.indexOf("description:"));
        assertThat(md).contains("title: \"true\"");
        assertThat(md).contains("description: \"a: b\"");
        assertThat(md).contains("tags: [eng, needs quote]");
        assertThat(md).contains("status: stable");
        assertThat(md).contains("confluence_id: \"200\"");
        assertThat(md).contains("usage_count: 2");
        assertThat(md).endsWith("hello\n");
    }

    @Test
    void multipleVerifiedEventsAreAList() {
        OkfConcept concept = OkfConcept.of("Page")
                .verified(new OkfConcept.Verified("human:ada", AT))
                .verified(new OkfConcept.Verified("human:bob", AT.plusSeconds(1)))
                .build();
        String md = OkfYaml.render(concept);
        assertThat(md).contains("verified:\n  - { by: \"human:ada\"");
        assertThat(md).contains("  - { by: \"human:bob\"");
    }

    @Test
    void attestedComputationFrontmatter() {
        OkfConcept concept = OkfSkills.computation("process:test", AT);
        String md = OkfYaml.render(concept);
        assertThat(md).contains("type: Attested Computation");
        assertThat(md).contains("runtime: grpc");
        assertThat(md).contains("executor:");
        assertThat(md).contains("attester:");
        assertThat(md).contains("parameters:");
    }

    @Test
    void quotesEmptyNumbersAndEscapes() {
        assertThat(OkfYaml.scalar("")).isEqualTo("\"\"");
        assertThat(OkfYaml.scalar(null)).isEqualTo("\"\"");
        assertThat(OkfYaml.scalar("42")).isEqualTo("\"42\"");
        assertThat(OkfYaml.scalar("false")).isEqualTo("\"false\"");
        assertThat(OkfYaml.scalar("plain")).isEqualTo("plain");
        assertThat(OkfYaml.scalar("say \"hi\"")).isEqualTo("\"say \\\"hi\\\"\"");
        OkfConcept concept = OkfConcept.of("Page")
                .title("  padded  ")
                .staleAfter(AT)
                .usageWindow(new OkfConcept.UsageWindow(AT, AT.plusSeconds(1)))
                .body("")
                .build();
        String md = OkfYaml.render(concept);
        assertThat(md).contains("stale_after:");
        assertThat(md).contains("usage_window:");
        assertThat(md).endsWith("---\n");
    }
}
