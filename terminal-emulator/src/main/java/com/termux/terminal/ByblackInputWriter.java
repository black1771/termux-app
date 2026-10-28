package com.termux.terminal;

import java.util.ArrayDeque;
import java.util.Arrays;

final class ByblackInputWriter {

    static final int MAX_PENDING_BYTES = 256 * 1024;

    private final ByteQueue output;
    private final ArrayDeque<byte[]> pending = new ArrayDeque<>();
    private final Thread worker;

    private int pendingBytes;
    private boolean closed;

    ByblackInputWriter(ByteQueue output) {
        this.output = output;
        worker = new Thread(this::runWriter, "ByblackInputWriter");
        worker.setDaemon(true);
        worker.start();
    }


    enum SubmitResult {
        ACCEPTED,
        FULL,
        CLOSED
    }

    synchronized SubmitResult trySubmit(
            byte[] data, int offset, int count) {

        if (data == null)
            throw new NullPointerException("data");

        if (offset < 0 || count <= 0 ||
                offset > data.length - count)
            throw new IllegalArgumentException("Invalid input range");

        if (closed)
            return SubmitResult.CLOSED;

        if (count > MAX_PENDING_BYTES - pendingBytes)
            return SubmitResult.FULL;

        byte[] copy =
            java.util.Arrays.copyOfRange(data, offset, offset + count);

        pending.addLast(copy);
        pendingBytes += count;
        notifyAll();

        return SubmitResult.ACCEPTED;
    }

    synchronized boolean submit(byte[] data, int offset, int count) {
        if (data == null) throw new NullPointerException("data");
        if (offset < 0 || count <= 0 ||
                offset > data.length - count)
            throw new IllegalArgumentException("Invalid input range");

        if (closed || count > MAX_PENDING_BYTES - pendingBytes)
            return false;

        byte[] copy = Arrays.copyOfRange(data, offset, offset + count);
        pending.addLast(copy);
        pendingBytes += count;
        notifyAll();
        return true;
    }


    /**
     * Submit an entire input buffer in bounded chunks.
     * Returns false if capacity is exhausted.
     *
     * This method is non-blocking, but is not atomic:
     * earlier chunks may already have been accepted.
     */
    synchronized boolean submitChunked(
            byte[] data, int offset, int count) {

        if (data == null)
            throw new NullPointerException("data");

        if (offset < 0 || count <= 0 ||
                offset > data.length - count)
            throw new IllegalArgumentException("Invalid input range");

        final int chunkSize = 4096;

        for (int position = 0; position < count;) {
            int length = Math.min(chunkSize, count - position);

            if (!submit(data, offset + position, length))
                return false;

            position += length;
        }

        return true;
    }


    /**
     * Blocking producer API. Must never be called on the Android UI thread.
     * Waits for capacity instead of dropping input.
     */
    private final java.util.concurrent.locks.ReentrantLock
        producerLock = new java.util.concurrent.locks.ReentrantLock(true);

    boolean submitBlocking(
            byte[] data, int offset, int count) {

        try {
            producerLock.lockInterruptibly();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        try {
            return submitBlockingOrdered(data, offset, count);
        } finally {
            producerLock.unlock();
        }
    }

    private synchronized boolean submitBlockingOrdered(
            byte[] data, int offset, int count) {

        if (data == null)
            throw new NullPointerException("data");

        if (offset < 0 || count <= 0 ||
                offset > data.length - count)
            throw new IllegalArgumentException("Invalid input range");

        int position = 0;

        while (position < count) {
            int length = Math.min(
                4096, count - position);

            while (!closed &&
                   length > MAX_PENDING_BYTES - pendingBytes) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }

            if (closed)
                return false;

            byte[] copy = java.util.Arrays.copyOfRange(
                data, offset + position,
                offset + position + length);

            pending.addLast(copy);
            pendingBytes += length;
            notifyAll();

            position += length;
        }

        return true;
    }

    private void runWriter() {
        while (true) {
            byte[] next;
            synchronized (this) {
                while (pending.isEmpty() && !closed) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        if (closed) return;
                    }
                }
                if (closed) return;
                next = pending.removeFirst();
            }

            if (!output.write(next, 0, next.length))
                return;

            synchronized (this) {
                pendingBytes -= next.length;
                notifyAll();
            }
        }
    }

    synchronized void close() {
        closed = true;
        pending.clear();
        notifyAll();
        output.close();
        worker.interrupt();
    }
}
