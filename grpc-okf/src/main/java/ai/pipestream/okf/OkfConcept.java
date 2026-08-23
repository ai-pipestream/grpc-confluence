package ai.pipestream.okf;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * One OKF concept document (§4): required {@code type}, recommended fields,
 * provenance/trust/lifecycle families (§5), and attested-computation
 * contract fields (§10) when {@code type} is {@code Attested Computation}.
 */
public final class OkfConcept {

    /** Lifecycle values for {@code status} (§5.4). */
    public enum Status {
        /** Not yet reviewed. */
        DRAFT("draft"),
        /** Ready for consumption (default when omitted). */
        STABLE("stable"),
        /** Kept for links; no longer current. */
        DEPRECATED("deprecated");

        private final String wire;

        Status(String wire) {
            this.wire = wire;
        }

        /**
         * The YAML value.
         *
         * @return {@code draft}, {@code stable}, or {@code deprecated}
         */
        public String wire() {
            return wire;
        }
    }

    /**
     * One {@code sources[]} entry (§5.1).
     *
     * @param id optional footnote join key
     * @param resource URL, bundle path, or scope descriptor
     * @param title optional label
     * @param author optional actor
     * @param usageCount optional exercise count
     * @param lastModified when the source last changed
     * @param usageWindow per-entry override of the shared window
     */
    public record Source(String id, String resource, String title, String author,
            Long usageCount, Instant lastModified, UsageWindow usageWindow) {
        /**
         * Creates a source. {@code resource} is required within an entry.
         *
         * @param id optional footnote join key
         * @param resource URL, bundle path, or scope descriptor
         * @param title optional label
         * @param author optional actor
         * @param usageCount optional exercise count
         * @param lastModified when the source last changed
         * @param usageWindow per-entry override of the shared window
         */
        public Source {
            Objects.requireNonNull(resource, "resource");
            if (resource.isBlank()) {
                throw new IllegalArgumentException("sources[].resource is required");
            }
        }
    }

    /**
     * Shared or per-source {@code usage_window} (§5.1).
     *
     * @param from start instant
     * @param to end instant
     */
    public record UsageWindow(Instant from, Instant to) {
        /**
         * Creates a window.
         *
         * @param from start instant
         * @param to end instant
         */
        public UsageWindow {
            Objects.requireNonNull(from, "from");
            Objects.requireNonNull(to, "to");
        }
    }

    /**
     * {@code generated} (§5.2).
     *
     * @param by actor
     * @param at last meaningful content change
     */
    public record Generated(String by, Instant at) {
        /**
         * Creates a generated record. {@code by} is required within {@code generated}.
         *
         * @param by actor
         * @param at last meaningful content change
         */
        public Generated {
            Objects.requireNonNull(by, "by");
            if (by.isBlank()) {
                throw new IllegalArgumentException("generated.by is required");
            }
        }
    }

    /**
     * One {@code verified} event (§5.2).
     *
     * @param by actor
     * @param at when confirmation happened
     */
    public record Verified(String by, Instant at) {
        /**
         * Creates a verification event.
         *
         * @param by actor
         * @param at when confirmation happened
         */
        public Verified {
            Objects.requireNonNull(by, "by");
            Objects.requireNonNull(at, "at");
        }
    }

    /**
     * One attested-computation parameter (§10.2).
     *
     * @param name bind name
     * @param type runtime type name
     * @param required whether the agent must supply it
     */
    public record Parameter(String name, String type, boolean required) {
        /**
         * Creates a parameter.
         *
         * @param name bind name
         * @param type runtime type name
         * @param required whether the agent must supply it
         */
        public Parameter {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
        }
    }

    /**
     * {@code executor} contract (§10.2).
     *
     * @param resource path to run instructions
     * @param receipt field names a run must return
     */
    public record Executor(String resource, List<String> receipt) {
        /**
         * Creates an executor.
         *
         * @param resource path to run instructions
         * @param receipt field names a run must return
         */
        public Executor {
            Objects.requireNonNull(resource, "resource");
            receipt = List.copyOf(receipt == null ? List.of() : receipt);
        }
    }

    /**
     * {@code attester} contract (§10.2).
     *
     * @param resource path to deterministic check code
     */
    public record Attester(String resource) {
        /**
         * Creates an attester.
         *
         * @param resource path to deterministic check code
         */
        public Attester {
            Objects.requireNonNull(resource, "resource");
        }
    }

    private final String type;
    private final String title;
    private final String description;
    private final String resource;
    private final List<String> tags;
    private final List<Source> sources;
    private final UsageWindow usageWindow;
    private final Generated generated;
    private final List<Verified> verified;
    private final Status status;
    private final Instant staleAfter;
    private final String runtime;
    private final List<Parameter> parameters;
    private final String computation;
    private final Executor executor;
    private final Attester attester;
    private final Map<String, String> extra;
    private final String body;

    private OkfConcept(Builder builder) {
        this.type = builder.type;
        this.title = builder.title;
        this.description = builder.description;
        this.resource = builder.resource;
        this.tags = List.copyOf(builder.tags);
        this.sources = List.copyOf(builder.sources);
        this.usageWindow = builder.usageWindow;
        this.generated = builder.generated;
        this.verified = List.copyOf(builder.verified);
        this.status = builder.status;
        this.staleAfter = builder.staleAfter;
        this.runtime = builder.runtime;
        this.parameters = List.copyOf(builder.parameters);
        this.computation = builder.computation;
        this.executor = builder.executor;
        this.attester = builder.attester;
        this.extra = Map.copyOf(builder.extra);
        this.body = builder.body == null ? "" : builder.body;
    }

    /**
     * Starts a concept of {@code type}.
     *
     * @param type required type string
     * @return a builder
     */
    public static Builder of(String type) {
        return new Builder(type);
    }

    /**
     * Required type.
     *
     * @return type
     */
    public String type() {
        return type;
    }

    /**
     * Display title, if any.
     *
     * @return title or empty
     */
    public Optional<String> title() {
        return Optional.ofNullable(title);
    }

    /**
     * One-line description, if any.
     *
     * @return description or empty
     */
    public Optional<String> description() {
        return Optional.ofNullable(description);
    }

    /**
     * Canonical URI of the underlying asset, if any.
     *
     * @return resource or empty
     */
    public Optional<String> resource() {
        return Optional.ofNullable(resource);
    }

    /**
     * Tags.
     *
     * @return tags, possibly empty
     */
    public List<String> tags() {
        return tags;
    }

    /**
     * Provenance sources.
     *
     * @return sources, possibly empty
     */
    public List<Source> sources() {
        return sources;
    }

    /**
     * Shared usage window, if any.
     *
     * @return window or empty
     */
    public Optional<UsageWindow> usageWindow() {
        return Optional.ofNullable(usageWindow);
    }

    /**
     * How the content was produced, if recorded.
     *
     * @return generated or empty
     */
    public Optional<Generated> generated() {
        return Optional.ofNullable(generated);
    }

    /**
     * Verification events.
     *
     * @return events, possibly empty
     */
    public List<Verified> verified() {
        return verified;
    }

    /**
     * Lifecycle status, if set (absent means stable for consumers).
     *
     * @return status or empty
     */
    public Optional<Status> status() {
        return Optional.ofNullable(status);
    }

    /**
     * Absolute stale instant, if any.
     *
     * @return stale_after or empty
     */
    public Optional<Instant> staleAfter() {
        return Optional.ofNullable(staleAfter);
    }

    /**
     * Attested computation runtime, if this is that type.
     *
     * @return runtime or empty
     */
    public Optional<String> runtime() {
        return Optional.ofNullable(runtime);
    }

    /**
     * Computation parameters.
     *
     * @return parameters, possibly empty
     */
    public List<Parameter> parameters() {
        return parameters;
    }

    /**
     * Path to an external computation file, if any.
     *
     * @return computation path or empty
     */
    public Optional<String> computation() {
        return Optional.ofNullable(computation);
    }

    /**
     * Executor contract, if any.
     *
     * @return executor or empty
     */
    public Optional<Executor> executor() {
        return Optional.ofNullable(executor);
    }

    /**
     * Attester contract, if any.
     *
     * @return attester or empty
     */
    public Optional<Attester> attester() {
        return Optional.ofNullable(attester);
    }

    /**
     * Producer-defined extra frontmatter keys (string values).
     *
     * @return extra keys
     */
    public Map<String, String> extra() {
        return extra;
    }

    /**
     * Markdown body after the frontmatter.
     *
     * @return body, never null
     */
    public String body() {
        return body;
    }

    /** Builder for {@link OkfConcept}. */
    public static final class Builder {
        private final String type;
        private String title;
        private String description;
        private String resource;
        private final List<String> tags = new ArrayList<>();
        private final List<Source> sources = new ArrayList<>();
        private UsageWindow usageWindow;
        private Generated generated;
        private final List<Verified> verified = new ArrayList<>();
        private Status status;
        private Instant staleAfter;
        private String runtime;
        private final List<Parameter> parameters = new ArrayList<>();
        private String computation;
        private Executor executor;
        private Attester attester;
        private final Map<String, String> extra = new LinkedHashMap<>();
        private String body;

        private Builder(String type) {
            if (type == null || type.isBlank()) {
                throw new IllegalArgumentException("type is required");
            }
            this.type = type.trim();
        }

        /**
         * Sets the display title.
         *
         * @param title title
         * @return this builder
         */
        public Builder title(String title) {
            this.title = blankToNull(title);
            return this;
        }

        /**
         * Sets the one-line description.
         *
         * @param description description
         * @return this builder
         */
        public Builder description(String description) {
            this.description = blankToNull(description);
            return this;
        }

        /**
         * Sets the canonical resource URI.
         *
         * @param resource URI
         * @return this builder
         */
        public Builder resource(String resource) {
            this.resource = blankToNull(resource);
            return this;
        }

        /**
         * Adds a tag.
         *
         * @param tag tag
         * @return this builder
         */
        public Builder tag(String tag) {
            if (tag != null && !tag.isBlank()) {
                tags.add(tag.trim());
            }
            return this;
        }

        /**
         * Adds tags.
         *
         * @param tags tags
         * @return this builder
         */
        public Builder tags(Iterable<String> tags) {
            if (tags != null) {
                tags.forEach(this::tag);
            }
            return this;
        }

        /**
         * Adds a provenance source.
         *
         * @param source source
         * @return this builder
         */
        public Builder source(Source source) {
            sources.add(Objects.requireNonNull(source, "source"));
            return this;
        }

        /**
         * Sets the shared usage window.
         *
         * @param usageWindow window
         * @return this builder
         */
        public Builder usageWindow(UsageWindow usageWindow) {
            this.usageWindow = usageWindow;
            return this;
        }

        /**
         * Sets how the content was produced.
         *
         * @param generated generated
         * @return this builder
         */
        public Builder generated(Generated generated) {
            this.generated = generated;
            return this;
        }

        /**
         * Adds a verification event.
         *
         * @param verified event
         * @return this builder
         */
        public Builder verified(Verified verified) {
            this.verified.add(Objects.requireNonNull(verified, "verified"));
            return this;
        }

        /**
         * Sets lifecycle status.
         *
         * @param status status
         * @return this builder
         */
        public Builder status(Status status) {
            this.status = status;
            return this;
        }

        /**
         * Sets the absolute stale instant.
         *
         * @param staleAfter instant
         * @return this builder
         */
        public Builder staleAfter(Instant staleAfter) {
            this.staleAfter = staleAfter;
            return this;
        }

        /**
         * Sets attested-computation runtime (required for that type).
         *
         * @param runtime runtime name
         * @return this builder
         */
        public Builder runtime(String runtime) {
            this.runtime = blankToNull(runtime);
            return this;
        }

        /**
         * Adds a computation parameter.
         *
         * @param parameter parameter
         * @return this builder
         */
        public Builder parameter(Parameter parameter) {
            parameters.add(Objects.requireNonNull(parameter, "parameter"));
            return this;
        }

        /**
         * Sets a path to an external computation file.
         *
         * @param computation path
         * @return this builder
         */
        public Builder computation(String computation) {
            this.computation = blankToNull(computation);
            return this;
        }

        /**
         * Sets the executor contract.
         *
         * @param executor executor
         * @return this builder
         */
        public Builder executor(Executor executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Sets the attester contract.
         *
         * @param attester attester
         * @return this builder
         */
        public Builder attester(Attester attester) {
            this.attester = attester;
            return this;
        }

        /**
         * Adds a producer-defined string frontmatter key.
         *
         * @param key key
         * @param value value; blank values are skipped
         * @return this builder
         */
        public Builder extra(String key, String value) {
            if (key != null && !key.isBlank() && value != null && !value.isBlank()) {
                extra.put(key, value);
            }
            return this;
        }

        /**
         * Sets the markdown body.
         *
         * @param body body
         * @return this builder
         */
        public Builder body(String body) {
            this.body = body;
            return this;
        }

        /**
         * Builds the concept. Attested Computation requires {@code runtime}.
         *
         * @return the concept
         */
        public OkfConcept build() {
            if ("Attested Computation".equals(type) && (runtime == null || runtime.isBlank())) {
                throw new IllegalArgumentException("Attested Computation requires runtime");
            }
            return new OkfConcept(this);
        }

        private static String blankToNull(String value) {
            return value == null || value.isBlank() ? null : value.trim();
        }
    }
}
