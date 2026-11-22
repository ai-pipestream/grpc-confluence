package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ConfluenceEvent;
import ai.pipestream.confluence.v1.ConfluenceEventObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The keys under which a drain reads repeated events of one object once. */
class ConfluenceChangeCoalesceTest {

    private static ConfluenceEvent event(String type, String contentId) {
        return ConfluenceEvent.newBuilder().setEventType(type).setContentId(contentId).build();
    }

    @Test
    void sameTypeAndObjectShareAKey() {
        assertThat(ConfluenceChangeGrpcService.coalesceKey(event("avi:confluence:updated:page", "1")))
                .isEqualTo(ConfluenceChangeGrpcService.coalesceKey(event("avi:confluence:updated:page", "1")));
    }

    @Test
    void otherTypeOrObjectDoesNot() {
        String key = ConfluenceChangeGrpcService.coalesceKey(event("avi:confluence:updated:page", "1"));
        assertThat(ConfluenceChangeGrpcService.coalesceKey(event("avi:confluence:trashed:page", "1")))
                .isNotEqualTo(key);
        assertThat(ConfluenceChangeGrpcService.coalesceKey(event("avi:confluence:updated:page", "2")))
                .isNotEqualTo(key);
    }

    @Test
    void spaceEventsUseTheSpaceIdAndBlankIdsNeverCoalesce() {
        ConfluenceEvent space = ConfluenceEvent.newBuilder().setEventType("avi:confluence:updated:space:V2")
                .setObject(ConfluenceEventObject.CONFLUENCE_EVENT_OBJECT_SPACE).setSpaceId("9").build();
        assertThat(ConfluenceChangeGrpcService.coalesceKey(space)).endsWith("|9");
        assertThat(ConfluenceChangeGrpcService.coalesceKey(event("avi:confluence:created:user", "")))
                .isNull();
    }
}
