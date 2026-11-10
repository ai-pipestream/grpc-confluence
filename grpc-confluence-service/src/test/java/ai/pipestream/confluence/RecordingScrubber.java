package ai.pipestream.confluence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Replaces the site, account and credential values of a recording with
 * placeholders. A real value always gets the same placeholder, and once a
 * value has a placeholder it is replaced wherever it appears, so ids in a
 * response still line up with the requests that use them.
 */
final class RecordingScrubber {

    /** The domain of every placeholder host and address. */
    static final String PLACEHOLDER_DOMAIN = "example.invalid";

    private static final String UUID =
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
    private static final Pattern AUTHORIZATION =
            Pattern.compile("\\b(?:Basic|Bearer)\\s+([A-Za-z0-9+/=._-]{16,})");
    private static final Pattern JWT =
            Pattern.compile("eyJ[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]*");
    private static final Pattern API_TOKEN = Pattern.compile("\\bAT[A-Z]TT[A-Za-z0-9_=-]{8,}");
    private static final Pattern SITE =
            Pattern.compile("(?i)[a-z0-9-]+(?:\\.[a-z0-9-]+)*\\.atlassian\\.net");
    private static final Pattern CLOUD_ID = Pattern.compile(
            "(?:/ex/confluence/|ari:cloud:[a-z.-]*:|cloudId=|/tenantinfo/)(" + UUID + ")");
    private static final Pattern ACCOUNT_ID = Pattern.compile("\\b[0-9]{4,8}:" + UUID + "\\b"
            + "|(?<![0-9a-fA-F-])[0-9a-f]{24}(?![0-9a-fA-F-])");
    private static final Pattern EMAIL =
            Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\\.[A-Za-z0-9-]+)+");
    private static final Pattern OPAQUE = Pattern.compile("[A-Za-z0-9_]{32,}={0,2}");

    private static final Set<String> NAME_KEYS = Set.of("displayname", "publicname");
    private static final Set<String> ACCOUNT_KEYS = Set.of("accountid");
    private static final Set<String> CLOUD_KEYS = Set.of("cloudid");
    private static final Set<String> PICTURE_KEYS = Set.of("profilepicture", "avatarurls");
    /** A display name this short is replaced only where it is a display name. */
    private static final int MIN_NAME_LITERAL = 5;

    /** Real value to placeholder, per kind. */
    private final Map<String, Map<String, String>> tables = new LinkedHashMap<>();
    /** Real values replaced wherever they appear. */
    private final Map<String, String> literals = new LinkedHashMap<>();
    private final Set<String> issued = new HashSet<>();

    /** Names the site host (with its port, if any) so that it is replaced like an Atlassian host. */
    void site(String authority) {
        literal(authority, site(authority, true));
    }

    /** Names the account email of the run. */
    void email(String email) {
        literal(email, placeholder("email", email, n -> "user-" + n + "@" + PLACEHOLDER_DOMAIN));
    }

    /** Names a credential of the run. */
    void secret(String secret) {
        literal(secret, token(secret));
    }

    /** A copy of {@code node} with every key and string value scrubbed. */
    JsonNode scrub(JsonNode node) {
        return scrub(node, "", false);
    }

    private JsonNode scrub(JsonNode node, String key, boolean picture) {
        if (node instanceof ObjectNode object) {
            ObjectNode out = object.objectNode();
            object.properties().forEach(field -> {
                String name = field.getKey().toLowerCase(Locale.ROOT);
                out.set(scrub(field.getKey()), scrub(field.getValue(), name,
                        picture || PICTURE_KEYS.contains(name)));
            });
            return out;
        }
        if (node instanceof ArrayNode array) {
            ArrayNode out = array.arrayNode();
            array.forEach(element -> out.add(scrub(element, key, picture)));
            return out;
        }
        if (!node.isTextual()) {
            return node;
        }
        String value = node.asText();
        if (value.isEmpty() || issued.contains(value)) {
            return node;
        }
        if (picture) {
            return TextNode.valueOf(literal(value,
                    placeholder("picture", value, n -> "/avatars/avatar-" + n + ".png")));
        }
        if (NAME_KEYS.contains(key)) {
            String name = placeholder("name", value, n -> "User " + n);
            return TextNode.valueOf(value.length() < MIN_NAME_LITERAL ? name : literal(value, name));
        }
        if (ACCOUNT_KEYS.contains(key)) {
            return TextNode.valueOf(literal(value, account(value)));
        }
        if (CLOUD_KEYS.contains(key)) {
            return TextNode.valueOf(literal(value, cloud(value)));
        }
        return TextNode.valueOf(scrub(value));
    }

    /** {@code text} with every sensitive value in it replaced. */
    String scrub(String text) {
        if (text == null || text.isEmpty() || issued.contains(text)) {
            return text;
        }
        Matcher cloud = CLOUD_ID.matcher(text);
        while (cloud.find()) {
            literal(cloud.group(1), cloud(cloud.group(1)));
        }
        String out = literals(text);
        out = replace(out, AUTHORIZATION, match -> credential(match) ? token(match) : match);
        out = replace(out, JWT, this::token);
        out = replace(out, API_TOKEN, this::token);
        out = replace(out, SITE, match -> site(match, false));
        out = replace(out, ACCOUNT_ID, match -> literal(match, account(match)));
        out = replace(out, EMAIL, match -> placeholderAddress(match) ? match
                : literal(match, placeholder("email", match, n -> "user-" + n + "@" + PLACEHOLDER_DOMAIN)));
        out = replace(out, OPAQUE, match -> opaque(match)
                ? literal(match, placeholder("opaque", match, n -> "opaque-" + n)) : match);
        return out;
    }

    /**
     * The rules a recording still breaks: the names of the checks that
     * found a site host, an address, a credential or an account id in
     * {@code text}. Empty for a clean recording.
     */
    static List<String> leaks(String text) {
        List<String> found = new ArrayList<>();
        if (text.toLowerCase(Locale.ROOT).contains("atlassian.net")) {
            found.add("site hostname");
        }
        if (matches(text, EMAIL, match -> !placeholderAddress(match))) {
            found.add("email address");
        }
        if (matches(text, AUTHORIZATION, RecordingScrubber::credential)) {
            found.add("authorization credential");
        }
        if (matches(text, JWT, match -> true) || matches(text, API_TOKEN, match -> true)
                || matches(text, OPAQUE, RecordingScrubber::opaque)) {
            found.add("token");
        }
        if (matches(text, ACCOUNT_ID, match -> true)) {
            found.add("account id");
        }
        return found;
    }

    private static boolean matches(String text, Pattern pattern, Function<String, Boolean> counts) {
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            if (counts.apply(matcher.group())) {
                return true;
            }
        }
        return false;
    }

    private static boolean placeholderAddress(String address) {
        return address.toLowerCase(Locale.ROOT).endsWith("@" + PLACEHOLDER_DOMAIN);
    }

    /** Whether an Authorization-like match carries a credential and not prose. */
    private static boolean credential(String match) {
        String value = match.substring(match.indexOf(' ') + 1).trim();
        return value.chars().anyMatch(Character::isDigit)
                && value.chars().anyMatch(Character::isUpperCase);
    }

    /** Whether a long run of token characters mixes upper case, lower case and digits. */
    private static boolean opaque(String match) {
        return match.chars().anyMatch(Character::isDigit)
                && match.chars().anyMatch(Character::isUpperCase)
                && match.chars().anyMatch(Character::isLowerCase);
    }

    private String site(String host, boolean literal) {
        String name = placeholder("site", host.toLowerCase(Locale.ROOT),
                n -> "site-" + n + "." + PLACEHOLDER_DOMAIN);
        return literal ? name : literal(host, name);
    }

    private String account(String id) {
        return placeholder("account", id, n -> "account-" + n);
    }

    private String cloud(String id) {
        return placeholder("cloud", id.toLowerCase(Locale.ROOT),
                n -> String.format(Locale.ROOT, "00000000-0000-0000-0000-%012d", n));
    }

    private String token(String token) {
        return literal(token, placeholder("token", token, n -> "token-" + n));
    }

    private String placeholder(String kind, String real, IntFunction<String> format) {
        Map<String, String> table = tables.computeIfAbsent(kind, k -> new LinkedHashMap<>());
        String placeholder = table.computeIfAbsent(real, k -> format.apply(table.size() + 1));
        issued.add(placeholder);
        return placeholder;
    }

    /** Has {@code real} replaced by {@code placeholder} wherever it appears from now on. */
    private String literal(String real, String placeholder) {
        if (real != null && real.length() >= 4 && !real.equals(placeholder)) {
            literals.put(real, placeholder);
        }
        return placeholder;
    }

    private String literals(String text) {
        List<Map.Entry<String, String>> longestFirst = new ArrayList<>(literals.entrySet());
        longestFirst.sort(Comparator.comparingInt(
                (Map.Entry<String, String> entry) -> entry.getKey().length()).reversed());
        String out = text;
        for (Map.Entry<String, String> entry : longestFirst) {
            out = out.replace(entry.getKey(), entry.getValue());
        }
        return out;
    }

    private static String replace(String text, Pattern pattern, Function<String, String> replacement) {
        Matcher matcher = pattern.matcher(text);
        StringBuilder out = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(out, Matcher.quoteReplacement(replacement.apply(matcher.group())));
        }
        matcher.appendTail(out);
        return out.toString();
    }
}
