package ai.pipestream.okf;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Attested Computation contract files recorded in every bundle (§10). The
 * producer writes the contract; it does not execute it.
 */
public final class OkfSkills {

    /** Bundle path of the Attested Computation concept. */
    public static final String COMPUTATION_PATH = "attested-computations/run-grpc-sync.md";
    /** Executor skill path. */
    public static final String EXECUTOR_PATH = "references/skills/run-grpc-sync.md";
    /** Attester path. */
    public static final String ATTESTER_PATH = "references/attesters/okf-conformance.md";

    private OkfSkills() {
    }

    /**
     * The Attested Computation concept for a gRPC Sync run.
     *
     * @param generatedBy actor
     * @param at generation instant
     * @return the concept
     */
    public static OkfConcept computation(String generatedBy, Instant at) {
        return OkfConcept.of("Attested Computation")
                .title("Run gRPC Sync")
                .description("Contract for the crawl that produced this OKF bundle.")
                .runtime("grpc")
                .parameter(new OkfConcept.Parameter("drive_ids", "repeated string", false))
                .parameter(new OkfConcept.Parameter("folder_path", "string", false))
                .parameter(new OkfConcept.Parameter("include_content", "bool", false))
                .parameter(new OkfConcept.Parameter("space_keys", "repeated string", false))
                .executor(new OkfConcept.Executor("/" + EXECUTOR_PATH,
                        java.util.List.of("run_id", "entity_count", "okf_sha256")))
                .attester(new OkfConcept.Attester("/" + ATTESTER_PATH))
                .generated(new OkfConcept.Generated(generatedBy, at))
                .body("This document records the attested-computation contract. "
                        + "The producer does not execute the runtime.")
                .build();
    }

    /**
     * Executor skill markdown (itself a concept so §11 applies).
     *
     * @param generatedBy actor
     * @param at generation instant
     * @return the concept
     */
    public static OkfConcept executorSkill(String generatedBy, Instant at) {
        return OkfConcept.of("Skill")
                .title("run-grpc-sync")
                .description("How to invoke ConfluenceService.Sync or MicrosoftService.Sync.")
                .generated(new OkfConcept.Generated(generatedBy, at))
                .body("""
                        Call the streaming `Sync` RPC on the matching gRPC proxy.

                        Receipt fields a run must return:

                        * `run_id` — crawl run identifier
                        * `entity_count` — entities emitted
                        * `okf_sha256` — SHA-256 of the companion ZIP
                        """)
                .build();
    }

    /**
     * Attester concept: OKF §11 conformance.
     *
     * @param generatedBy actor
     * @param at generation instant
     * @return the concept
     */
    public static OkfConcept attester(String generatedBy, Instant at) {
        return OkfConcept.of("Attester")
                .title("okf-conformance")
                .description("Deterministic OKF v0.2 frontmatter checks.")
                .generated(new OkfConcept.Generated(generatedBy, at))
                .body("Reject the bundle when any non-reserved markdown file lacks a "
                        + "parseable YAML frontmatter `type`, or when reserved "
                        + "`index.md` / `log.md` violate §8 / §9.")
                .build();
    }

    /**
     * UTF-8 bytes of a skill/attester file (used when putting via {@link OkfBundle#putConcept}).
     *
     * @param concept concept
     * @return rendered markdown
     */
    public static byte[] render(OkfConcept concept) {
        return OkfYaml.render(concept).getBytes(StandardCharsets.UTF_8);
    }
}
