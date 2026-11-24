package ai.pipestream.confluence;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** What each kind of pass leaves in a {@link RunMirror}. */
class RunMirrorTest {

    @Test
    void aFullSweepReplacesWhatTheConnectionHeld() {
        RunMirror<String> mirror = new RunMirror<>();
        RunMirror<String>.Pass pass = mirror.pass("");
        pass.put("pages/1", "one");
        pass.put("pages/2", "two");
        assertThat(pass.complete(true).values()).containsExactly("one", "two");

        pass.put("pages/2", "two, edited");
        RunMirror.Completed<String> second = pass.complete(true);

        assertThat(second.values()).containsExactly("two, edited");
        assertThat(second.increment()).isFalse();
        assertThat(second.changed()).isTrue();
    }

    @Test
    void aPassThatIsNotAFullSweepMergesIntoTheLastFullSweep() {
        RunMirror<String> mirror = new RunMirror<>();
        RunMirror<String>.Pass pass = mirror.pass("");
        pass.put("pages/1", "one");
        pass.put("pages/2", "two");
        pass.complete(true);

        pass.put("pages/2", "two, edited");
        pass.put("pages/3", "three");
        RunMirror.Completed<String> merged = pass.complete(false);

        assertThat(merged.values()).containsExactly("one", "two, edited", "three");
        assertThat(merged.increment()).isFalse();
    }

    @Test
    void aPassWithNoFullSweepBeforeItIsAnIncrementOfItsOwnRecords() {
        RunMirror<String> mirror = new RunMirror<>();
        RunMirror<String>.Pass pass = mirror.pass("");
        pass.put("pages/2", "two, edited");

        RunMirror.Completed<String> first = pass.complete(false);
        pass.put("pages/3", "three");
        RunMirror.Completed<String> second = pass.complete(false);

        assertThat(first.values()).containsExactly("two, edited");
        assertThat(first.increment()).isTrue();
        // Increments do not pile up into something that looks like a full bundle.
        assertThat(second.values()).containsExactly("three");
        assertThat(second.increment()).isTrue();
    }

    @Test
    void aPassThatIsNotAFullSweepAndEmittedNothingChangesNothing() {
        RunMirror<String> mirror = new RunMirror<>();
        RunMirror<String>.Pass pass = mirror.pass("");
        assertThat(pass.complete(false).changed()).isFalse();

        pass.put("pages/1", "one");
        pass.complete(true);
        RunMirror.Completed<String> idle = pass.complete(false);

        assertThat(idle.changed()).isFalse();
        assertThat(idle.values()).containsExactly("one");
    }

    @Test
    void aFullSweepThatEmittedNothingStillEmptiesTheConnection() {
        RunMirror<String> mirror = new RunMirror<>();
        RunMirror<String>.Pass pass = mirror.pass("");
        pass.put("pages/1", "one");
        pass.complete(true);

        RunMirror.Completed<String> emptied = pass.complete(true);

        assertThat(emptied.changed()).isTrue();
        assertThat(emptied.values()).isEmpty();
    }

    @Test
    void aFullSweepOfOneConnectionKeepsTheRecordsOfAnother() {
        RunMirror<String> mirror = new RunMirror<>();
        RunMirror<String>.Pass acme = mirror.pass("acme");
        RunMirror<String>.Pass globex = mirror.pass("globex");
        acme.put("ENG/pages/1", "acme one");
        acme.complete(true);
        globex.put("OPS/pages/7", "globex seven");
        globex.complete(true);

        acme.put("ENG/pages/2", "acme two");
        RunMirror.Completed<String> swept = acme.complete(true);

        assertThat(swept.values()).containsExactlyInAnyOrder("globex seven", "acme two");
    }

    @Test
    void aConnectionWithNoFullSweepYetWritesAnIncrementWhileAnotherHasItsBundle() {
        RunMirror<String> mirror = new RunMirror<>();
        RunMirror<String>.Pass acme = mirror.pass("acme");
        acme.put("ENG/pages/1", "acme one");
        acme.complete(true);

        RunMirror<String>.Pass globex = mirror.pass("globex");
        globex.put("OPS/pages/7", "globex seven");
        RunMirror.Completed<String> increment = globex.complete(false);

        assertThat(increment.increment()).isTrue();
        assertThat(increment.values()).containsExactly("globex seven");
        assertThat(globex.connectionId()).isEqualTo("globex");
    }

    @Test
    void twoPassesOfOneConnectionAtOnceDoNotMixTheirRecords() {
        RunMirror<String> mirror = new RunMirror<>();
        RunMirror<String>.Pass first = mirror.pass("acme");
        RunMirror<String>.Pass second = mirror.pass("acme");
        first.put("ENG/pages/1", "one");
        second.put("ENG/pages/2", "two");

        assertThat(first.complete(false).values()).containsExactly("one");
        assertThat(second.complete(false).values()).containsExactly("two");
    }
}
