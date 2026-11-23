package ai.pipestream.confluence;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RecordingScrubberTest {

    private static final String ACCOUNT = "712020:0a1b2c3d-1111-4222-8333-444455556666";
    private static final String LEGACY_ACCOUNT = "5b10a2844c20165700ede21a";
    private static final String CLOUD = "9f8e7d6c-aaaa-4bbb-8ccc-ddddeeeeffff";
    private static final String JWT =
            "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.c2lnbmF0dXJlLXZhbHVl";
    private static final String CURSOR = "eyJpZCI6IjEyMzQ1Njc4OSIsImNvbnRlbnRPcmRlciI6ImlkIn0=";

    private static JsonNode json(String text) throws Exception {
        return RecordedExchange.JSON.readTree(text);
    }

    @Test
    void siteAccountAndCredentialValuesBecomePlaceholders() throws Exception {
        RecordingScrubber scrubber = new RecordingScrubber();
        JsonNode out = scrubber.scrub(json("""
                {"authorId":"%s","lastOwnerId":"%s",
                 "accountId":"unusual-account-form",
                 "email":"ada@acme.example","displayName":"Ada Lovelace","publicName":"Ada",
                 "profilePicture":{"path":"/wiki/aa-avatar/abc","isDefault":false},
                 "ari":"ari:cloud:confluence:%s:page/123",
                 "location":"https://api.media.atlassian.com/file/1/binary?token=%s&client=x",
                 "header":"Basic Ym90QHBpcGVzdHJlYW0uYWk6dG9rZW4xMjM=",
                 "apiToken":"ATATT3xFfGF0T4JNjabcdefGHIJ1234567890",
                 "_links":{"base":"https://acme.atlassian.net/wiki",
                           "next":"/wiki/api/v2/pages?cursor=%s"}}
                """.formatted(ACCOUNT, LEGACY_ACCOUNT, CLOUD, JWT, CURSOR)));

        assertThat(out.path("authorId").asText()).isEqualTo("account-1");
        assertThat(out.path("lastOwnerId").asText()).isEqualTo("account-2");
        assertThat(out.path("accountId").asText()).isEqualTo("account-3");
        assertThat(out.path("email").asText()).isEqualTo("user-1@example.invalid");
        assertThat(out.path("displayName").asText()).isEqualTo("User 1");
        assertThat(out.path("publicName").asText()).isEqualTo("User 2");
        assertThat(out.path("profilePicture").path("path").asText()).isEqualTo("/avatars/avatar-1.png");
        assertThat(out.path("profilePicture").path("isDefault").asBoolean()).isFalse();
        assertThat(out.path("ari").asText())
                .isEqualTo("ari:cloud:confluence:00000000-0000-0000-0000-000000000001:page/123");
        assertThat(out.path("location").asText())
                .isEqualTo("https://api.media.atlassian.com/file/1/binary?token=token-1&client=x");
        assertThat(out.path("header").asText()).isEqualTo("token-2");
        assertThat(out.path("apiToken").asText()).isEqualTo("token-3");
        assertThat(out.path("_links").path("base").asText())
                .isEqualTo("https://site-1.example.invalid/wiki");
        assertThat(out.path("_links").path("next").asText())
                .isEqualTo("/wiki/api/v2/pages?cursor=opaque-1");
        assertThat(RecordingScrubber.leaks(out.toString())).isEmpty();
    }

    @Test
    void aValueKeepsItsPlaceholderWhereverItAppears() throws Exception {
        RecordingScrubber scrubber = new RecordingScrubber();
        scrubber.site("wiki.internal.example:8443");
        scrubber.email("bot@pipestream.ai");
        scrubber.secret("s3cret-Token-value");
        scrubber.scrub(json("{\"accountId\":\"unusual-account-form\",\"displayName\":\"Ada Lovelace\","
                + "\"cloudId\":\"" + CLOUD + "\"}"));

        assertThat(scrubber.scrub("/wiki/api/v2/users/unusual-account-form"))
                .isEqualTo("/wiki/api/v2/users/account-1");
        assertThat(scrubber.scrub("<p>Written by Ada Lovelace for bot@pipestream.ai</p>"))
                .isEqualTo("<p>Written by User 1 for user-1@example.invalid</p>");
        assertThat(scrubber.scrub("https://wiki.internal.example:8443/wiki/x?key=s3cret-Token-value&c="
                + CLOUD)).isEqualTo("https://site-1.example.invalid/wiki/x?key=token-1&c="
                        + "00000000-0000-0000-0000-000000000001");
        assertThat(scrubber.scrub("first " + ACCOUNT + " then " + LEGACY_ACCOUNT + " then " + ACCOUNT))
                .isEqualTo("first account-2 then account-3 then account-2");
    }

    @Test
    void ordinaryValuesAreKept() throws Exception {
        RecordingScrubber scrubber = new RecordingScrubber();
        scrubber.scrub(json("{\"displayName\":\"Page\"}"));
        String kept = """
                {"id":"123456789","type":"Page","title":"pipestream-live-write-20261110T120000Z-ab12cd34",
                 "localId":"0a1b2c3d-1111-4222-8333-444455556666",
                 "sha256":"ba0c6921a1cfcbbc79de31f15a30d7e6a1f86790997ca88a64899eb22dbe1c27",
                 "text":"Basic information about a Bearer of news",
                 "embedUrl":"https://www.atlassian.com/",
                 "webui":"/spaces/CLAUDEGRPC/pages/123456789/pipestream-live-write-20261110T120000Z",
                 "number":1.50}""";
        JsonNode in = json(kept);
        assertThat(scrubber.scrub(in)).isEqualTo(in);
        assertThat(RecordingScrubber.leaks(kept)).isEmpty();
    }

    @Test
    void scrubbingAgainChangesNothing() throws Exception {
        RecordingScrubber scrubber = new RecordingScrubber();
        JsonNode once = scrubber.scrub(json("{\"accountId\":\"" + ACCOUNT + "\",\"displayName\":\"Ada\","
                + "\"email\":\"ada@acme.example\",\"profilePicture\":{\"path\":\"/a.png\"},"
                + "\"self\":\"https://acme.atlassian.net/wiki/rest/api/user?accountId=" + ACCOUNT + "\"}"));
        assertThat(scrubber.scrub(once)).isEqualTo(once);
    }

    @Test
    void leaksNamesWhatAScrubMustRemove() {
        assertThat(RecordingScrubber.leaks("https://acme.atlassian.net/wiki")).contains("site hostname");
        assertThat(RecordingScrubber.leaks("mail ada@acme.example")).containsExactly("email address");
        assertThat(RecordingScrubber.leaks("Authorization: Basic Ym90QHBpcGVzdHJlYW0uYWk6dG9rZW4xMjM="))
                .contains("authorization credential");
        assertThat(RecordingScrubber.leaks(JWT)).containsExactly("token");
        assertThat(RecordingScrubber.leaks("ATATT3xFfGF0T4JNjabcdefGHIJ1234567890")).contains("token");
        assertThat(RecordingScrubber.leaks("cursor=" + CURSOR)).containsExactly("token");
        assertThat(RecordingScrubber.leaks(ACCOUNT)).containsExactly("account id");
        assertThat(RecordingScrubber.leaks("id " + LEGACY_ACCOUNT)).containsExactly("account id");
        assertThat(RecordingScrubber.leaks("user-1@example.invalid at site-1.example.invalid")).isEmpty();
    }
}
