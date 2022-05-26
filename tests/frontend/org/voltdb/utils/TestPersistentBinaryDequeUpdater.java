/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.function.Consumer;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.test.utils.RandomTestRule;
import org.voltdb.utils.BinaryDeque.UpdateResult;

@RunWith(Parameterized.class)
public class TestPersistentBinaryDequeUpdater {
    private static final VoltLogger s_log = new VoltLogger(TestPersistentBinaryDequeUpdater.class.getSimpleName());
    @Rule
    public final TemporaryFolder m_dir = new TemporaryFolder();
    @Rule
    public final TestName m_name = new TestName();
    @Rule
    public final RandomTestRule m_random = new RandomTestRule();

    // Should entries be compressed
    private final boolean m_compress;
    // Should an entry be added to have an active segment
    private final boolean m_activeSegment;
    // Entry in the active segment
    private ByteBuffer m_entryInActiveSegment;

    // PBD instance being tested
    private PersistentBinaryDeque<Object> m_pbd;

    private ReaderThread m_reader;

    /**
     * @return all of the possible combinations for {@link #m_compress} and {@link #m_activeSegment}
     */
    @Parameterized.Parameters
    public static Object[][] parameters() {
        return new Object[][] { { false, false }, { true, false }, { false, true }, { true, true } };
    }

    public TestPersistentBinaryDequeUpdater(boolean compress, boolean activeSegment) {
        m_compress = compress;
        m_activeSegment = activeSegment;
    }

    @Before
    public void setup() throws Exception {
        m_pbd = createPbd();

        m_reader = new ReaderThread();
        m_reader.start();
    }

    @After
    public void cleanup() throws Throwable {
        m_reader.m_running = false;
        m_reader.join();
        if (m_reader.m_error != null) {
            throw m_reader.m_error;
        }
        m_pbd.closeAndDelete();

        // Force finalizers on BBContainers to run to detect leaks
        System.gc();
        System.runFinalization();
    }

    /*
     * Test that entries in segments are iterated in reverse order
     */
    @Test
    public void reverseIteration() throws Exception {
        Deque<ByteBuffer> entries = new ArrayDeque<>();

        fillEntries(5, 20, entries::add);

        m_pbd.updateEntries((m, e) -> {
            if (e != null) {
                assertEquals(entries.pollLast(), e);
            }
            return UpdateResult.KEEP;
        });
        assertTrue(entries.isEmpty());

        // Segment count should not change
        assertPbdSegmentCount(5);
    }

    /*
     * Delete all entries in all segments
     */
    @Test
    public void deleteAllEntries() throws Exception {
        fillEntries(5, 20);

        m_pbd.updateEntries((o, e) -> {
            return e == null ? UpdateResult.KEEP : UpdateResult.DELETE;
        });

        PersistentBinaryDeque<Object>.ReadCursor cursor = m_pbd.openForRead(m_name.getMethodName());
        if (m_activeSegment) {
            BBContainer container = cursor.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            container.discard();
        }
        assertNull(cursor.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        // All segments should now be deleted
        assertPbdSegmentCount(0);
    }

    /*
     * Delete all entries in the first segment and every other entry in all other segments
     */
    @Test
    public void deleteEveryOtherEntryAndAllFirstSegment() throws Exception {
        fillEntries(5, 20);

        MutableInt count = new MutableInt(0);
        Deque<ByteBuffer> entries = new ArrayDeque<>(4 * 20 / 2);

        m_pbd.updateEntries((m, e) -> {
            if (e == null) {
                return UpdateResult.KEEP;
            }
            int index = count.incrementAndGet();
            if (index < 4 * 20 && index % 2 == 0) {
                entries.add((ByteBuffer) ByteBuffer.allocate(e.remaining()).put(e).flip());
                return UpdateResult.KEEP;
            } else {
                return UpdateResult.DELETE;
            }
        });

        assertAllEntriesInDeque(entries);

        // First segment should be deleted
        assertPbdSegmentCount(4);
    }

    /*
     * Update all entries
     */
    @Test
    public void updateAllEntries() throws Exception {
        fillEntries(5, 20);

        Deque<ByteBuffer> entries = new ArrayDeque<>(5 * 20);

        m_pbd.updateEntries((m, e) -> {
            if (e == null) {
                return UpdateResult.KEEP;
            }
            ByteBuffer update = randomBuffer();
            entries.add(update.asReadOnlyBuffer());
            return UpdateResult.update(update);
        });

        assertAllEntriesInDeque(entries);

        assertPbdSegmentCount(5);
    }

    /*
     * Test a random distribution of different update results based on a result frequency
     */
    @Test
    public void randomUpdateResults() throws Exception {
        Deque<ByteBuffer> allEntries = new ArrayDeque<>(5 * 20);
        fillEntries(5, 20, allEntries::add);

        Deque<ByteBuffer> entries = new ArrayDeque<>(5 * 20);

        m_pbd.updateEntries((m, e) -> {
            float value = m_random.nextFloat();
            if (value < 0.01) {
                // 1% chance to stop
                while (!allEntries.isEmpty()) {
                    entries.add(allEntries.removeLast());
                }
                return UpdateResult.STOP;
            }

            if (e != null) {
                allEntries.removeLast();
            }

            if (value < 0.21) {
                // 20% chance to delete
                return UpdateResult.DELETE;
            } else if (value < 0.51) {
                // 30% chance to keep
                if (e != null) {
                    entries.add((ByteBuffer) ByteBuffer.allocate(e.remaining()).put(e).flip());
                }
                return UpdateResult.KEEP;
            }

            // Everything else is an update
            ByteBuffer update = randomBuffer();
            entries.add(update.asReadOnlyBuffer());
            return UpdateResult.update(update);
        });

        assertTrue(allEntries.isEmpty());

        assertAllEntriesInDeque(entries);
    }

    /*
     * Test that newEligibleUpdateEntries returns the correct entry count before and after update and upon creation
     */
    @Test
    public void newEligibleUpdateEntries() throws Exception {
        assertEquals(0, m_pbd.newEligibleUpdateEntries());

        fillEntries(3, 10);

        assertEquals(30, m_pbd.newEligibleUpdateEntries());

        m_pbd.updateEntries((m, e) -> UpdateResult.KEEP);

        assertEquals(0, m_pbd.newEligibleUpdateEntries());

        fillEntries(3, 10);

        assertEquals(30 + (m_activeSegment ? 1 : 0), m_pbd.newEligibleUpdateEntries());

        PersistentBinaryDeque<Object> other = createPbd();
        assertEquals(60 + (m_activeSegment ? 2 : 0), other.newEligibleUpdateEntries());
        other.close();
    }

    private void assertPbdSegmentCount(int count) {
        String[] segments = m_dir.getRoot().list();
        assertEquals(Arrays.toString(segments), count + (m_activeSegment ? 1 : 0), segments.length);
    }

    private void assertAllEntriesInDeque(Deque<ByteBuffer> entries) throws IOException {
        if (m_activeSegment) {
            entries.addFirst(m_entryInActiveSegment);
        }
        m_pbd.scanEntries(b -> {
            assertEquals(entries.pollLast(), b.b());
            return -1;
        });

        assertTrue(entries.isEmpty());
    }

    private void fillEntries(int segments, int entriesPerSegment) throws IOException {
        fillEntries(segments, entriesPerSegment, null);
    }

    private void fillEntries(int segments, int entriesPerSegment, Consumer<ByteBuffer> entryConsumer)
            throws IOException {

        int previousSegmentCount = Math.max(m_dir.getRoot().list().length - (m_activeSegment ? 1 : 0), 0);

        for (int i = 0; i < segments; i++) {
            for (int j = 0; j < entriesPerSegment; ++j) {
                ByteBuffer b = randomBuffer();
                if (entryConsumer != null) {
                    entryConsumer.accept(b.asReadOnlyBuffer());
                }
                m_pbd.offer(DBBPool.dummyWrapBB(b));
            }
            m_pbd.updateExtraHeader(this);
        }

        if (m_activeSegment) {
            // Update doesn't iterate over active segments so do not send it to the entryConsumer
            m_entryInActiveSegment = randomBuffer();
            m_pbd.offer(DBBPool.dummyWrapBB(m_entryInActiveSegment.duplicate()));
        }

        assertPbdSegmentCount(previousSegmentCount + segments);
    }

    private ByteBuffer randomBuffer() {
        ByteBuffer b = ByteBuffer.allocateDirect(m_random.nextInt(16 * 1024) + 1024);
        // Only fill part of the buffer so that it is usually compressible
        b.limit(1024);
        m_random.nextBytes(b);
        b.clear();
        return b;
    }

    private class ReaderThread extends Thread {
        volatile boolean m_running = true;
        volatile Throwable m_error = null;

        public ReaderThread() {
            super(m_name.getMethodName());
        }

        @Override
        public void run() {
            try {
                String cursorId = Long.toString(getId());

                while (m_running) {
                    PersistentBinaryDeque<Object>.ReadCursor cursor = m_pbd.openForRead(cursorId, true);
                    BBContainer cont;
                    while ((cont = cursor.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
                        cont.discard();
                        Thread.yield();
                    }
                    m_pbd.closeCursor(cursorId);
                }
            } catch (Throwable t) {
                t.printStackTrace();
                m_error = t;
            }
        }
    }

    private PersistentBinaryDeque<Object> createPbd() throws IOException {
        return PersistentBinaryDeque.builder(m_name.getMethodName(), m_dir.getRoot(), s_log).compression(m_compress)
                .initialExtraHeader(this, new BinaryDequeSerializer<Object>() {
                    @Override
                    public int getMaxSize(Object object) throws IOException {
                        return 0;
                    }

                    @Override
                    public void write(Object object, ByteBuffer buffer) throws IOException {}

                    @Override
                    public Object read(ByteBuffer buffer) throws IOException {
                        return null;
                    }
                }).build();
    }
}
