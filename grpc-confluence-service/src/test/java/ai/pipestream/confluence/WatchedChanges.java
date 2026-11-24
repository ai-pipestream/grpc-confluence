package ai.pipestream.confluence;

import ai.pipestream.confluence.v1.WatchChangesResponse;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The client end of one {@code WatchChanges} stream in a test: the messages
 * that arrived, and the drains the service ran for it.
 *
 * <p>The service tells the end of a drain from the stream's own thread, while
 * the messages that drain streamed still travel to this observer. So
 * {@link #awaitDrain} also waits for the last of them: a step that read the
 * stream as soon as the drain ended would find it empty on a loaded machine,
 * drain again, and send the queue a request the recording does not hold.
 */
final class WatchedChanges implements StreamObserver<WatchChangesResponse> {

    private final BlockingQueue<ConfluenceChangeGrpcService.Drain> drains =
            new LinkedBlockingQueue<>();
    private final BlockingQueue<WatchChangesResponse> messages = new LinkedBlockingQueue<>();
    private final Object arrival = new Object();
    /** The sequence of the last message that arrived; guarded by {@link #arrival}. */
    private long arrived;
    private volatile Throwable error;

    /**
     * Listens to the drains of {@code service}.
     *
     * @param service the service the stream is opened on
     */
    WatchedChanges(ConfluenceChangeGrpcService service) {
        service.onDrain(drains::add);
    }

    @Override
    public void onNext(WatchChangesResponse value) {
        synchronized (arrival) {
            messages.add(value);
            arrived = value.getSequence();
            arrival.notifyAll();
        }
    }

    @Override
    public void onError(Throwable t) {
        synchronized (arrival) {
            error = t;
            arrival.notifyAll();
        }
    }

    @Override
    public void onCompleted() {
        // Ends on cancel.
    }

    /**
     * Waits for the next drain to end and for every message the stream had
     * sent by then to arrive.
     *
     * @param wait how long to wait for both
     * @return the events the drain took
     * @throws InterruptedException when interrupted
     */
    List<ForgeEventQueue.Queued> awaitDrain(Duration wait) throws InterruptedException {
        long deadline = System.nanoTime() + wait.toNanos();
        ConfluenceChangeGrpcService.Drain drain = drains.poll(wait.toMillis(), TimeUnit.MILLISECONDS);
        if (error != null) {
            throw new AssertionError("the stream failed: " + error);
        }
        if (drain == null) {
            throw new AssertionError("no drain completed");
        }
        synchronized (arrival) {
            while (arrived < drain.lastSequence() && error == null) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    throw new AssertionError("the drain ended at message " + drain.lastSequence()
                            + " of the stream but only " + arrived + " arrived");
                }
                TimeUnit.NANOSECONDS.timedWait(arrival, left);
            }
        }
        if (error != null) {
            throw new AssertionError("the stream failed: " + error);
        }
        return drain.taken();
    }

    /**
     * Moves the messages that arrived since the last call to {@code into}.
     *
     * @param into receives them, in stream order
     */
    void drainTo(List<WatchChangesResponse> into) {
        messages.drainTo(into);
    }

    /**
     * The failure the stream ended with.
     *
     * @return the failure, or {@code null} while the stream is open
     */
    Throwable error() {
        return error;
    }
}
