package ai.pipestream.okf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * OKF v0.2 conformance (§11): parseable frontmatter with non-empty {@code type}
 * on every non-reserved markdown file; reserved {@code index.md}/{@code log.md}
 * follow §8/§9 when present.
 */
public final class OkfConformance {

    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private OkfConformance() {
    }

    /**
     * Checks a bundle.
     *
     * @param bundle the bundle
     * @return violations; empty means conformant
     */
    public static List<String> check(OkfBundle bundle) {
        List<String> violations = new ArrayList<>();
        boolean sawRootIndex = false;
        for (Map.Entry<String, byte[]> entry : bundle.files().entrySet()) {
            String path = entry.getKey();
            if (!path.endsWith(".md")) {
                continue;
            }
            String filename = path.substring(path.lastIndexOf('/') + 1);
            String text = new String(entry.getValue(), StandardCharsets.UTF_8);
            if (OkfPaths.reserved(filename)) {
                if ("index.md".equals(filename)) {
                    checkIndex(path, text, path.equals("index.md"), violations);
                    if (path.equals("index.md")) {
                        sawRootIndex = true;
                    }
                } else {
                    checkLog(path, text, violations);
                }
                continue;
            }
            Frontmatter fm = split(text);
            if (fm == null) {
                violations.add(path + ": missing YAML frontmatter delimited by ---");
                continue;
            }
            try {
                JsonNode node = YAML.readTree(fm.yaml());
                JsonNode type = node.path("type");
                if (!type.isTextual() || type.asText().isBlank()) {
                    violations.add(path + ": type is required and must be a non-empty string");
                }
            } catch (Exception e) {
                violations.add(path + ": unparseable YAML frontmatter: " + e.getMessage());
            }
        }
        if (sawRootIndex) {
            String index = new String(bundle.files().get("index.md"), StandardCharsets.UTF_8);
            Frontmatter fm = split(index);
            if (fm != null && !fm.yaml().isBlank()) {
                try {
                    JsonNode node = YAML.readTree(fm.yaml());
                    if (node.has("okf_version") && !OkfVersion.VALUE.equals(node.path("okf_version").asText())) {
                        violations.add("index.md: okf_version should be \"" + OkfVersion.VALUE + "\"");
                    }
                } catch (Exception ignored) {
                    // already reported in checkIndex
                }
            }
        }
        return violations;
    }

    /**
     * Throws if the bundle is not conformant.
     *
     * @param bundle the bundle
     */
    public static void require(OkfBundle bundle) {
        List<String> violations = check(bundle);
        if (!violations.isEmpty()) {
            throw new IllegalStateException("OKF bundle is not conformant:\n - "
                    + String.join("\n - ", violations));
        }
    }

    private static void checkIndex(String path, String text, boolean root, List<String> violations) {
        if (!text.contains("#")) {
            violations.add(path + ": index.md should contain at least one markdown heading (§8)");
        }
        if (root) {
            Frontmatter fm = split(text);
            if (fm != null && !fm.yaml().isBlank()) {
                try {
                    YAML.readTree(fm.yaml());
                } catch (Exception e) {
                    violations.add(path + ": root index.md frontmatter is not parseable YAML");
                }
            }
        } else if (text.startsWith("---")) {
            violations.add(path + ": nested index.md must not carry frontmatter (§8)");
        }
    }

    private static void checkLog(String path, String text, List<String> violations) {
        if (!text.contains("## ")) {
            violations.add(path + ": log.md should contain ISO date headings (§9)");
        }
    }

    private record Frontmatter(String yaml, String body) {
    }

    static Frontmatter split(String text) {
        if (!text.startsWith("---")) {
            return null;
        }
        int start = 0;
        if (text.startsWith("---\n")) {
            start = 4;
        } else if (text.startsWith("---\r\n")) {
            start = 5;
        } else {
            return null;
        }
        int endNl = text.indexOf("\n---", start);
        if (endNl < 0) {
            return null;
        }
        String yaml = text.substring(start, endNl).replace("\r", "");
        int bodyStart = endNl + 4;
        if (bodyStart < text.length() && text.charAt(bodyStart) == '\n') {
            bodyStart++;
        }
        String body = bodyStart <= text.length() ? text.substring(bodyStart) : "";
        return new Frontmatter(yaml, body);
    }
}
