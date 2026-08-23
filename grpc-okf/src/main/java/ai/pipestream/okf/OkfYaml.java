package ai.pipestream.okf;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

/**
 * YAML frontmatter emitter for OKF concept documents. Keys are written in
 * spec order so diffs stay stable; values are quoted when they need it.
 */
public final class OkfYaml {

    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_INSTANT;

    private OkfYaml() {
    }

    /**
     * Renders a concept as UTF-8 markdown with YAML frontmatter.
     *
     * @param concept the concept
     * @return the file contents
     */
    public static String render(OkfConcept concept) {
        StringBuilder yaml = new StringBuilder();
        yaml.append("---\n");
        field(yaml, "type", concept.type());
        concept.title().ifPresent(v -> field(yaml, "title", v));
        concept.description().ifPresent(v -> field(yaml, "description", v));
        concept.resource().ifPresent(v -> field(yaml, "resource", v));
        if (!concept.tags().isEmpty()) {
            yaml.append("tags: [");
            List<String> tags = concept.tags();
            for (int i = 0; i < tags.size(); i++) {
                if (i > 0) {
                    yaml.append(", ");
                }
                yaml.append(scalar(tags.get(i)));
            }
            yaml.append("]\n");
        }
        concept.generated().ifPresent(g -> {
            yaml.append("generated: { by: ").append(scalar(g.by()));
            if (g.at() != null) {
                yaml.append(", at: ").append(scalar(iso(g.at())));
            }
            yaml.append(" }\n");
        });
        if (concept.verified().size() == 1) {
            OkfConcept.Verified v = concept.verified().get(0);
            yaml.append("verified: { by: ").append(scalar(v.by()))
                    .append(", at: ").append(scalar(iso(v.at()))).append(" }\n");
        } else if (concept.verified().size() > 1) {
            yaml.append("verified:\n");
            for (OkfConcept.Verified v : concept.verified()) {
                yaml.append("  - { by: ").append(scalar(v.by()))
                        .append(", at: ").append(scalar(iso(v.at()))).append(" }\n");
            }
        }
        concept.status().ifPresent(s -> field(yaml, "status", s.wire()));
        concept.staleAfter().ifPresent(t -> field(yaml, "stale_after", iso(t)));
        if (!concept.sources().isEmpty()) {
            yaml.append("sources:\n");
            for (OkfConcept.Source source : concept.sources()) {
                yaml.append("  - resource: ").append(scalar(source.resource())).append('\n');
                if (source.id() != null && !source.id().isBlank()) {
                    yaml.append("    id: ").append(scalar(source.id())).append('\n');
                }
                if (source.title() != null && !source.title().isBlank()) {
                    yaml.append("    title: ").append(scalar(source.title())).append('\n');
                }
                if (source.author() != null && !source.author().isBlank()) {
                    yaml.append("    author: ").append(scalar(source.author())).append('\n');
                }
                if (source.usageCount() != null) {
                    yaml.append("    usage_count: ").append(source.usageCount()).append('\n');
                }
                if (source.lastModified() != null) {
                    yaml.append("    last_modified: ").append(scalar(iso(source.lastModified())))
                            .append('\n');
                }
                if (source.usageWindow() != null) {
                    yaml.append("    usage_window: { from: ")
                            .append(scalar(iso(source.usageWindow().from())))
                            .append(", to: ")
                            .append(scalar(iso(source.usageWindow().to())))
                            .append(" }\n");
                }
            }
        }
        concept.usageWindow().ifPresent(w -> yaml.append("usage_window: { from: ")
                .append(scalar(iso(w.from())))
                .append(", to: ")
                .append(scalar(iso(w.to())))
                .append(" }\n"));
        concept.runtime().ifPresent(v -> field(yaml, "runtime", v));
        if (!concept.parameters().isEmpty()) {
            yaml.append("parameters:\n");
            for (OkfConcept.Parameter p : concept.parameters()) {
                yaml.append("  - { name: ").append(scalar(p.name()))
                        .append(", type: ").append(scalar(p.type()))
                        .append(", required: ").append(p.required())
                        .append(" }\n");
            }
        }
        concept.computation().ifPresent(v -> field(yaml, "computation", v));
        concept.executor().ifPresent(e -> {
            yaml.append("executor:\n");
            yaml.append("  resource: ").append(scalar(e.resource())).append('\n');
            if (!e.receipt().isEmpty()) {
                yaml.append("  receipt: [");
                for (int i = 0; i < e.receipt().size(); i++) {
                    if (i > 0) {
                        yaml.append(", ");
                    }
                    yaml.append(scalar(e.receipt().get(i)));
                }
                yaml.append("]\n");
            }
        });
        concept.attester().ifPresent(a -> {
            yaml.append("attester:\n");
            yaml.append("  resource: ").append(scalar(a.resource())).append('\n');
        });
        for (Map.Entry<String, String> extra : concept.extra().entrySet()) {
            field(yaml, extra.getKey(), extra.getValue());
        }
        yaml.append("---\n");
        String body = concept.body();
        if (body == null || body.isBlank()) {
            return yaml.toString();
        }
        if (!body.startsWith("\n")) {
            yaml.append('\n');
        }
        yaml.append(body);
        if (!body.endsWith("\n")) {
            yaml.append('\n');
        }
        return yaml.toString();
    }

    /**
     * Formats an instant as ISO-8601 UTC with a {@code Z} offset.
     *
     * @param instant instant
     * @return wire timestamp
     */
    public static String iso(Instant instant) {
        return ISO.format(instant.atOffset(ZoneOffset.UTC));
    }

    private static void field(StringBuilder yaml, String key, String value) {
        yaml.append(key).append(": ").append(scalar(value)).append('\n');
    }

    static String scalar(String value) {
        if (value == null) {
            return "\"\"";
        }
        boolean needsQuote = value.isEmpty()
                || value.chars().anyMatch(c -> ":#{}[]&*?|>!%@`'\",\n\r\t".indexOf(c) >= 0)
                || value.startsWith(" ")
                || value.endsWith(" ")
                || looksLikeNumberOrBool(value);
        if (!needsQuote) {
            return value;
        }
        return '"' + value.replace("\\", "\\\\").replace("\"", "\\\"") + '"';
    }

    private static boolean looksLikeNumberOrBool(String value) {
        return "true".equals(value) || "false".equals(value) || "null".equals(value)
                || value.matches("-?\\d+(\\.\\d+)?");
    }
}
