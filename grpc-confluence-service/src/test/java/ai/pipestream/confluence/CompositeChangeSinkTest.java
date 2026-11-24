package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.ChangeOperation;
import ai.pipestream.confluence.v1.ConfluenceChange;
import ai.pipestream.confluence.v1.ConfluenceSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Fan-out semantics of {@link CompositeChangeSink}. */
class CompositeChangeSinkTest {

    @Test
    void everySinkSeesEveryEmission() {
        InMemoryChangeSink first = new InMemoryChangeSink();
        InMemoryChangeSink second = new InMemoryChangeSink();
        CompositeChangeSink composite = new CompositeChangeSink(List.of(first, second));

        ConfluenceChange change = ConfluenceChange.newBuilder()
                .setChangeId("c1")
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .build();
        ConfluenceSnapshot snapshot = ConfluenceSnapshot.newBuilder()
                .setSnapshotId("s1")
                .build();
        composite.emit(change);
        composite.snapshot(snapshot);

        assertThat(first.changes()).containsExactly(change);
        assertThat(second.changes()).containsExactly(change);
        assertThat(first.snapshots()).containsExactly(snapshot);
        assertThat(second.snapshots()).containsExactly(snapshot);
    }

    @Test
    void anEarlyFailureSkipsLaterSinksAndPropagates() {
        ChangeSink failing = new ChangeSink() {
            @Override
            public void emit(ConfluenceChange change) {
                throw new IllegalStateException("boom");
            }

            @Override
            public void snapshot(ConfluenceSnapshot snapshot) {
            }
        };
        InMemoryChangeSink later = new InMemoryChangeSink();
        CompositeChangeSink composite = new CompositeChangeSink(List.of(failing, later));

        assertThatThrownBy(() -> composite.emit(ConfluenceChange.getDefaultInstance()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(later.changes()).isEmpty();
    }

    @Test
    void completeRunFansOut() {
        InMemoryChangeSink first = new InMemoryChangeSink();
        InMemoryChangeSink second = new InMemoryChangeSink();
        new CompositeChangeSink(List.of(first, second)).completeRun("run-9");
        assertThat(first.completedRuns()).containsExactly("run-9");
        assertThat(second.completedRuns()).containsExactly("run-9");
    }

    @Test
    void aPassThatIsNotAFullSweepFansOutAsOne() {
        InMemoryChangeSink first = new InMemoryChangeSink();
        InMemoryChangeSink second = new InMemoryChangeSink();
        new CompositeChangeSink(List.of(first, second)).completeRun("since-1", false);
        assertThat(first.partialRuns()).containsExactly("since-1");
        assertThat(second.partialRuns()).containsExactly("since-1");
        assertThat(first.completedRuns()).isEmpty();
        assertThat(second.completedRuns()).isEmpty();
    }

    @Test
    void aSinkThatOnlyKnowsFullSweepsIsNotToldAboutOtherPasses() {
        List<String> reconciled = new java.util.ArrayList<>();
        ChangeSink ledgerLike = new ChangeSink() {
            @Override
            public void emit(ConfluenceChange change) {
            }

            @Override
            public void snapshot(ConfluenceSnapshot snapshot) {
            }

            @Override
            public void completeRun(String runId) {
                reconciled.add(runId);
            }
        };
        CompositeChangeSink composite = new CompositeChangeSink(List.of(ledgerLike));

        composite.completeRun("since-1", false);
        composite.completeRun("run-2", true);

        assertThat(reconciled).containsExactly("run-2");
    }

    @Test
    void forConnectionBindsEverySink() {
        List<String> bound = new java.util.ArrayList<>();
        InMemoryChangeSink inner = new InMemoryChangeSink();
        ChangeSink stateful = new ChangeSink() {
            @Override
            public void emit(ConfluenceChange change) {
            }

            @Override
            public void snapshot(ConfluenceSnapshot snapshot) {
            }

            @Override
            public ChangeSink forConnection(String connectionId) {
                bound.add(connectionId);
                return inner;
            }
        };
        InMemoryChangeSink stateless = new InMemoryChangeSink();

        ChangeSink view = new CompositeChangeSink(List.of(stateful, stateless)).forConnection("acme");
        view.completeRun("run-1", true);

        assertThat(bound).containsExactly("acme");
        assertThat(inner.completedRuns()).containsExactly("run-1");
        assertThat(stateless.completedRuns()).containsExactly("run-1");
    }
}
