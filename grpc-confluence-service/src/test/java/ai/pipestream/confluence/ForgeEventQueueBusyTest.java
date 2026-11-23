package ai.pipestream.confluence;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ForgeEventQueueBusyTest {

    private static final String PATH = "/x/queue";
    private static final String EVENTS = """
            {"events":[{"key":"k1","event":{"eventType":"avi:confluence:updated:page",
            "content":{"id":"42","type":"page"}}}]}""";

    private FakeConfluenceServer fake;
    private final List<Duration> waits = new ArrayList<>();
    private ForgeEventQueue queue;

    @BeforeEach
    void setUp() throws Exception {
        fake = FakeConfluenceServer.start();
        String url = fake.baseUrl().replace("/wiki", "") + PATH;
        queue = new ForgeEventQueue(url, "secret", HttpClient.newHttpClient(), waits::add);
    }

    @AfterEach
    void tearDown() {
        fake.close();
    }

    @Test
    void retriesBusyAnswersHonoringRetryAfter() throws Exception {
        fake.stubOnce(PATH, new FakeConfluenceServer.Stub(429, "<html>busy</html>",
                Map.of("Retry-After", "7")));
        fake.stubOnce(PATH, new FakeConfluenceServer.Stub(503, "", Map.of()));
        fake.stub(PATH, EVENTS);

        List<ForgeEventQueue.Queued> taken = queue.take(10);

        assertThat(taken).extracting(ForgeEventQueue.Queued::key).containsExactly("k1");
        assertThat(waits).containsExactly(Duration.ofSeconds(7), Duration.ofSeconds(4));
        assertThat(fake.requestsTo(PATH)).hasSize(3);
    }

    @Test
    void staysBusyAfterEveryAttempt() {
        fake.stub(PATH, new FakeConfluenceServer.Stub(429, "busy", Map.of("Retry-After", "600")));

        assertThatThrownBy(() -> queue.take(10)).isInstanceOf(ForgeEventQueue.Busy.class)
                .hasMessageContaining("HTTP 429");
        assertThat(fake.requestsTo(PATH)).hasSize(ForgeEventQueue.ATTEMPTS);
        assertThat(waits).hasSize(ForgeEventQueue.ATTEMPTS - 1)
                .allMatch(wait -> wait.equals(ForgeEventQueue.MAX_WAIT));
    }

    @Test
    void otherFailuresAreNotRetried() {
        fake.stub(PATH, new FakeConfluenceServer.Stub(401, "no", Map.of()));

        assertThatThrownBy(() -> queue.take(10)).isNotInstanceOf(ForgeEventQueue.Busy.class)
                .hasMessageContaining("HTTP 401");
        assertThat(fake.requestsTo(PATH)).hasSize(1);
        assertThat(waits).isEmpty();
    }
}
