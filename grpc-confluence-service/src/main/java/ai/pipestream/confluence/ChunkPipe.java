package ai.pipestream.confluence;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A bounded hand-off of byte chunks from a writer to the stream a reader
 * consumes; the writer waits while the reader is behind.
 */
final class ChunkPipe extends InputStream {

    /** Chunks held before the writer waits. */
    private static final int CAPACITY = 8;
    private static final ByteString END = ByteString.copyFrom(new byte[] {0});

    private final BlockingQueue<ByteString> queue = new ArrayBlockingQueue<>(CAPACITY);
    private ByteBuffer current = ByteBuffer.allocate(0);
    private boolean ended;
    private volatile IOException failure;

    /**
     * Hands one chunk to the reader, waiting for room.
     *
     * @param chunk the bytes
     * @param reader the task consuming this stream
     * @throws IOException when the reader finished before taking the chunk
     * @throws InterruptedException if the calling thread is interrupted
     */
    void write(ByteString chunk, Future<?> reader) throws IOException, InterruptedException {
        if (!chunk.isEmpty()) {
            offer(chunk, reader);
        }
    }

    /**
     * Marks the end of the stream.
     *
     * @param reader the task consuming this stream
     * @throws IOException when the reader finished before the end was reached
     * @throws InterruptedException if the calling thread is interrupted
     */
    void finish(Future<?> reader) throws IOException, InterruptedException {
        try {
            offer(END, reader);
        } catch (IOException e) {
            // The reader already has its answer; the caller collects it.
        }
    }

    /**
     * Makes the reader fail instead of reading further.
     *
     * @param cause the failure the reader sees
     */
    void fail(IOException cause) {
        failure = cause;
        queue.clear();
        queue.offer(END);
    }

    private void offer(ByteString chunk, Future<?> reader) throws IOException, InterruptedException {
        while (!queue.offer(chunk, 100, TimeUnit.MILLISECONDS)) {
            if (reader.isDone()) {
                throw new IOException("the upload ended before the whole file was sent");
            }
        }
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] target, int offset, int length) throws IOException {
        if (length == 0) {
            return 0;
        }
        while (!current.hasRemaining()) {
            if (failure != null) {
                throw failure;
            }
            if (ended) {
                return -1;
            }
            ByteString next;
            try {
                next = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted while waiting for upload bytes");
            }
            if (next == END) {
                ended = true;
            } else {
                current = next.asReadOnlyByteBuffer();
            }
        }
        int count = Math.min(length, current.remaining());
        current.get(target, offset, count);
        return count;
    }
}
