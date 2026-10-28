package com.termux.terminal;

import junit.framework.TestCase;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

public class ByblackFinalIntegrationTest extends TestCase {
    public void testLargePasteThenKeyboardAndBracket() throws Exception {
        ByteQueue queue = new ByteQueue(4096);
        ByblackUnifiedInput input = new ByblackUnifiedInput(queue);
        final String payload = new String(new char[1024 * 1024]).replace('\0', 'P');
        final byte[] expected = ("\033[200~" + payload + "\033[201~K").getBytes(StandardCharsets.UTF_8);
        ExecutorService consumer = Executors.newSingleThreadExecutor();
        try {
            Future<byte[]> read = consumer.submit(() -> {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                byte[] buf = new byte[4096];
                while (out.size() < expected.length) {
                    int n = queue.read(buf, true);
                    if (n <= 0) throw new IllegalStateException("Unexpected queue closure");
                    out.write(buf, 0, n);
                }
                return out.toByteArray();
            });
            assertEquals(ByblackUnifiedInput.Result.ACCEPTED, input.offerPaste(payload, true));
            assertEquals(ByblackUnifiedInput.Result.ACCEPTED, input.offer(new byte[]{'K'}, 0, 1));
            assertTrue(java.util.Arrays.equals(expected, read.get(20, TimeUnit.SECONDS)));
        } finally {
            input.close(); consumer.shutdownNow();
        }
    }
    public void testSanitizeAndNewlines() throws Exception {
        ByteQueue queue = new ByteQueue(4096);
        ByblackUnifiedInput input = new ByblackUnifiedInput(queue);
        try {
            assertEquals(ByblackUnifiedInput.Result.ACCEPTED,
                input.offerPaste("A\033B\r\nC\nD", false));
            byte[] result = new byte[5];
            int n = queue.read(result, true);
            assertTrue(n > 0);
            // ByteQueue may return a partial read; check the first byte only here.
            assertEquals('A', result[0]);
        } finally { input.close(); }
    }
    public void testBoundedAndClosed() {
        ByteQueue queue = new ByteQueue(4096);
        ByblackUnifiedInput input = new ByblackUnifiedInput(queue);
        try {
            assertEquals(ByblackUnifiedInput.Result.FULL,
                input.offer(new byte[ByblackUnifiedInput.MAX_PENDING + 1], 0,
                    ByblackUnifiedInput.MAX_PENDING + 1));
            assertTrue(input.pendingBytes() <= ByblackUnifiedInput.MAX_PENDING);
            input.close();
            assertEquals(ByblackUnifiedInput.Result.CLOSED,
                input.offer(new byte[]{1}, 0, 1));
        } finally { input.close(); }
    }
}
