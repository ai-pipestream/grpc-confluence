package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ChangeSource;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceEntity;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import ai.pipestream.confluence.v1.Page;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/** What {@link KafkaChangeSink} does with the outcome of each send. */
class KafkaChangeSinkTest {

    private static MockProducer<String, byte[]> producer(boolean autoComplete) {
        return new MockProducer<>(autoComplete, new StringSerializer(), new ByteArraySerializer());
    }

    private static KafkaChangeSink sink(MockProducer<String, byte[]> producer) {
        return new KafkaChangeSink(producer, "changes", "snapshots");
    }

    @Test
    void deliveredRecordsCompleteThePass() {
        MockProducer<String, byte[]> producer = producer(true);
        ChangeSink pass = sink(producer).forConnection("acme");
        ConfluenceChange change = change("c1");

        pass.emit(change);
        pass.snapshot(snapshot("s1"));

        assertThatCode(() -> pass.completeRun("run-1", true)).doesNotThrowAnyException();
        assertThat(producer.history()).extracting(r -> r.topic() + "/" + r.key())
                .containsExactly("changes/c1", "snapshots/s1");
        assertThat(producer.history().get(0).value()).isEqualTo(change.toByteArray());
    }

    @Test
    void completingThePassWaitsForRecordsStillOnTheirWay() {
        MockProducer<String, byte[]> producer = producer(false);
        ChangeSink pass = sink(producer).forConnection("acme");
        pass.emit(change("c1"));
        assertThat(producer.flushed()).isFalse();

        assertThatCode(() -> pass.completeRun("since", false)).doesNotThrowAnyException();

        assertThat(producer.flushed()).isTrue();
    }

    @Test
    void aRecordThatFailsAfterTheLastEmitFailsThePassWhenItCompletes() {
        MockProducer<String, byte[]> producer = producer(false);
        ChangeSink pass = sink(producer).forConnection("acme");
        pass.emit(change("c1"));
        producer.errorNext(new TimeoutException("broker did not answer"));

        KafkaChangeSink.DeliveryFailedException failure = catchThrowableOfType(
                KafkaChangeSink.DeliveryFailedException.class,
                () -> pass.completeRun("since", false));

        assertThat(failure).isNotNull();
        assertThat(failure.failed()).isEqualTo(1);
        assertThat(failure.retriable()).isTrue();
        assertThat(failure.getMessage()).contains("1 record", "c1", "changes",
                "broker did not answer");
    }

    @Test
    void aRecordThatFailedStopsThePassAtTheNextEmit() {
        MockProducer<String, byte[]> producer = producer(false);
        ChangeSink pass = sink(producer).forConnection("acme");
        pass.emit(change("c1"));
        pass.emit(change("c2"));
        producer.errorNext(new TimeoutException("broker did not answer"));
        producer.errorNext(new TimeoutException("broker did not answer"));

        KafkaChangeSink.DeliveryFailedException failure = catchThrowableOfType(
                KafkaChangeSink.DeliveryFailedException.class, () -> pass.emit(change("c3")));

        assertThat(failure).isNotNull();
        assertThat(failure.failed()).isEqualTo(2);
        assertThat(failure.getMessage()).contains("2 records", "first: key c1");
        assertThat(producer.history()).as("nothing more is sent on a pass that already lost a record")
                .hasSize(2);
    }

    @Test
    void aSendTheProducerRefusesOutrightFailsThatEmit() {
        MockProducer<String, byte[]> producer = producer(true);
        producer.sendException = new IllegalStateException("producer is closed");
        ChangeSink pass = sink(producer).forConnection("acme");

        KafkaChangeSink.DeliveryFailedException failure = catchThrowableOfType(
                KafkaChangeSink.DeliveryFailedException.class, () -> pass.emit(change("c1")));

        assertThat(failure).isNotNull();
        assertThat(failure.retriable()).isFalse();
        assertThat(failure.getMessage()).contains("c1", "producer is closed");
    }

    @Test
    void aSnapshotThatFailsCountsLikeAChange() {
        MockProducer<String, byte[]> producer = producer(false);
        ChangeSink pass = sink(producer).forConnection("acme");
        pass.snapshot(snapshot("s1"));
        producer.errorNext(new RecordTooLargeException("too large"));

        KafkaChangeSink.DeliveryFailedException failure = catchThrowableOfType(
                KafkaChangeSink.DeliveryFailedException.class,
                () -> pass.completeRun("run-1", true));

        assertThat(failure).isNotNull();
        assertThat(failure.retriable()).isFalse();
        assertThat(failure.getMessage()).contains("s1", "snapshots");
    }

    @Test
    void oneSyncsFailedRecordDoesNotFailAnotherSyncsPass() {
        MockProducer<String, byte[]> producer = producer(false);
        KafkaChangeSink sink = sink(producer);
        ChangeSink acme = sink.forConnection("acme");
        ChangeSink globex = sink.forConnection("globex");
        acme.emit(change("a1"));
        globex.emit(change("g1"));
        producer.errorNext(new TimeoutException("a1 timed out"));
        producer.completeNext();

        assertThatCode(() -> globex.completeRun("run-g", true)).doesNotThrowAnyException();
        assertThat(catchThrowableOfType(KafkaChangeSink.DeliveryFailedException.class,
                () -> acme.completeRun("run-a", true))).isNotNull();
    }

    @Test
    void theSinkItselfIsOnePass() {
        MockProducer<String, byte[]> producer = producer(false);
        KafkaChangeSink sink = sink(producer);
        sink.emit(change("c1"));
        producer.errorNext(new TimeoutException("broker did not answer"));

        assertThat(catchThrowableOfType(KafkaChangeSink.DeliveryFailedException.class,
                () -> sink.completeRun("run-1"))).isNotNull();
        // The failure belonged to that pass; the next one starts clean.
        sink.emit(change("c2"));
        assertThatCode(() -> sink.completeRun("run-2", true)).doesNotThrowAnyException();
    }

    @Test
    void syncAnswersUnavailableForARetriableFailureAndInternalOtherwise() {
        KafkaChangeSink.DeliveryFailedException retriable =
                new KafkaChangeSink.DeliveryFailedException(1, "changes", "c1",
                        new TimeoutException("broker did not answer"));
        KafkaChangeSink.DeliveryFailedException refused =
                new KafkaChangeSink.DeliveryFailedException(1, "changes", "c1",
                        new RecordTooLargeException("too large"));

        assertThat(ConfluenceGrpcService.Failure.of(retriable).status().getCode())
                .isEqualTo(Status.Code.UNAVAILABLE);
        assertThat(ConfluenceGrpcService.Failure.of(refused).status().getCode())
                .isEqualTo(Status.Code.INTERNAL);
        assertThat(ConfluenceGrpcService.Failure.of(refused).status().getDescription())
                .contains("kafka", "changes", "c1");
    }

    private static ConfluenceChange change(String id) {
        return ConfluenceChange.newBuilder()
                .setChangeId(id)
                .setOperation(ChangeOperation.CHANGE_OPERATION_UPSERT)
                .setSource(ChangeSource.CHANGE_SOURCE_CRAWL)
                .setCursor("run")
                .setOccurredAt(Timestamp.newBuilder().setSeconds(1))
                .setEntity(ConfluenceEntity.newBuilder()
                        .setEntityId("200")
                        .setIngestedAt(Timestamp.newBuilder().setSeconds(1))
                        .setPage(Page.newBuilder().setId("200").setSpaceId("100").setTitle("Doc")
                                .setWebUrl("https://example.atlassian.net/wiki/pages/200")))
                .build();
    }

    private static ConfluenceSnapshot snapshot(String id) {
        return ConfluenceSnapshot.newBuilder()
                .setSnapshotId(id)
                .setSpaceKey("ENG")
                .setCursor("2026-01-01T00:00:00Z")
                .setStartedAt(Timestamp.newBuilder().setSeconds(1))
                .build();
    }
}
