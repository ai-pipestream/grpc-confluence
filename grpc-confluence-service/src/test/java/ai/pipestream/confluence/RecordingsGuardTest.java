package ai.pipestream.confluence;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fails when a file under the recordings directory holds a site hostname,
 * an email address, a credential or token, or an Atlassian account id.
 * Binary response bodies ({@code *.bin}) are not read.
 */
class RecordingsGuardTest {

    private static final Path RECORDINGS = Path.of("src/test/resources/recordings");

    @Test
    void noRecordingHoldsASiteAccountOrCredentialValue() throws IOException {
        List<String> leaking = new ArrayList<>();
        try (Stream<Path> files = Files.walk(RECORDINGS)) {
            for (Path file : files.filter(Files::isRegularFile)
                    .filter(file -> !file.getFileName().toString().endsWith(".bin")).toList()) {
                List<String> leaks = RecordingScrubber.leaks(
                        Files.readString(file, StandardCharsets.UTF_8));
                if (!leaks.isEmpty()) {
                    leaking.add(RECORDINGS.relativize(file) + " " + leaks);
                }
            }
        }
        assertThat(leaking).as("recordings that must be scrubbed or removed").isEmpty();
    }
}
