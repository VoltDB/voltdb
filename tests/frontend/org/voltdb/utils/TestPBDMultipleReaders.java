/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.voltdb.utils.TestPersistentBinaryDeque.SEGMENT_FILL_COUNT;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;

public class TestPBDMultipleReaders {

    private final static VoltLogger logger = new VoltLogger("EXPORT");

    private PersistentBinaryDeque<?> m_pbd;

    private class PBDReader {
        private String m_readerId;
        private int m_totalRead;
        private BinaryDequeReader<?> m_reader;

        public PBDReader(String readerId) throws IOException {
            m_readerId = readerId;
            m_reader = m_pbd.openForRead(m_readerId);
        }

        public int readToStartOfNextSegment(boolean firstSegment) throws Exception {
            int end = (m_totalRead/SEGMENT_FILL_COUNT + 1) * SEGMENT_FILL_COUNT;
            if (firstSegment) {
                ++end;
            }
            boolean done = false;
            int numRead = 0;
            for (int i=m_totalRead; i<end && !done; i++) {
                BBContainer bbC = m_reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                if (bbC==null) {
                    done = true;
                    continue;
                }
                numRead++;
                bbC.discard();
            }

            return numRead;
        }
    }

    @Test
    public void testMultipleParallelReaders() throws Exception {
        int numBuffers = 100;
        for (int i=0; i<numBuffers; i++) {
            m_pbd.offer( DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(i)) );
        }
        int numSegments = TestPersistentBinaryDeque.getSortedDirectoryListing().size();

        int numReaders = 3;
        PBDReader[] readers = new PBDReader[numReaders];
        for (int i=0; i<numReaders; i++) {
            readers[i] = new PBDReader("reader" + i);
        }

        int currNumSegments = numSegments;
        for (int i=0; i<numSegments; i++) {
            // One reader finishing shouldn't discard the segment
            readers[0].readToStartOfNextSegment(i == 0);
            assertEquals(currNumSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

            // Once all readers finish reading, the segment should get discarded.
            for (int j=1; j<numReaders; j++) {
                readers[j].readToStartOfNextSegment(i == 0);
            }
            if (i < numSegments-1) {
                currNumSegments--;
            }
            assertEquals(currNumSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        }
    }

    @Test
    public void testNoDeleteUntilAllAcked() throws Exception {
        int numReaders = 3;
        BinaryDequeReader<?>[] readers = new BinaryDequeReader[numReaders];
        for (int i=0; i<numReaders; i++) {
            readers[i] = m_pbd.openForRead("reader" + i);
        }

        int numSegments = 5;
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(i)));
            m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(i)));
        }

        @SuppressWarnings("unchecked")
        ArrayList<BBContainer>[] pollResults = new ArrayList[readers.length-1];
        // all readers read, but only the last one ack
        for (int i=0; i<readers.length; i++) {
            BBContainer bbc = null;
            while ((bbc = readers[i].poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
                if (i==readers.length-1) {
                    bbc.discard();
                } else {
                    ArrayList<BBContainer> bbcs = pollResults[i];
                    if (bbcs == null) {
                        bbcs = new ArrayList<>();
                        pollResults[i] = bbcs;
                    }
                    bbcs.add(bbc);
                }
            }
            // nothing should be deleted
            assertEquals(numSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        }

        // ack from readers one by one
        for (int i=0; i<pollResults.length; i++) {
            ArrayList<BBContainer> bbcs = pollResults[i];
            for (BBContainer bbc : bbcs) {
                bbc.discard();
            }
            assertEquals((i==pollResults.length-1) ? 1 : numSegments,
                    TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        }
    }

    @Test
    public void testOpenReaders() throws Exception {
        String cursorId = "reader";
        BinaryDequeReader<?> reader1 = m_pbd.openForRead(cursorId);
        BinaryDequeReader<?> reader2 = m_pbd.openForRead(cursorId);
        BinaryDequeReader<?> another = m_pbd.openForRead("another");
        assertTrue(reader1==reader2);
        assertFalse(reader1==another);

        int numBuffers = 50;
        for (int i=0; i<numBuffers; i++) {
            m_pbd.offer( DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(i)) );
        }

        for (int j=0; j<numBuffers; j++) {
            BBContainer bbC = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            bbC.discard();
        }
        assertTrue(reader1.isEmpty());
        assertTrue(reader2.isEmpty());
        assertFalse(another.isEmpty());
    }

    @Test
    public void testSegmentClosingWriterMultipleReaders() throws Exception {
        assertEquals(0, m_pbd.numOpenSegments());

        int numSegments = 5;
        for (int i=0; i<numSegments; i++) {
            for (int j=0; j<SEGMENT_FILL_COUNT; j++) {
                m_pbd.offer( DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(j)) );
            }
            assertEquals(1, m_pbd.numOpenSegments());
        }

        BinaryDequeReader<?> reader0 = m_pbd.openForRead("reader0");
        BinaryDequeReader<?> reader1 = m_pbd.openForRead("reader1");
        // Position first reader0 on penultimate segment and reader1 on first segment
        for (int i=0; i<numSegments-1; i++) {
            for (int j = 0; j < SEGMENT_FILL_COUNT - (i == 0 ? 0 : 1); j++) {
                BBContainer bbC = reader0.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                bbC.discard();
                if (i==0) {
                    bbC = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                    bbC.discard();
                }
            }

            BBContainer bbC = reader0.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            bbC.discard();
            assertEquals(numSegments, m_pbd.numberOfSegments());
        }

        BBContainer bbC = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        bbC.discard();
        // Both readers finished reading first segment, so that is closed and deleted,
        // which reduces the # of segments by 1
        assertEquals(numSegments - 1, m_pbd.numberOfSegments());

        // reader0 at penultimate. Move reader1 through segments and check open segments
        for (int i=1; i<numSegments-1; i++) {
            for (int j = 0; j < SEGMENT_FILL_COUNT - 1; j++) {
                bbC = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                bbC.discard();
            }
            assertEquals(numSegments - i, m_pbd.numberOfSegments());

            bbC = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            bbC.discard();
            assertEquals(numSegments - i - 1, m_pbd.numberOfSegments());
        }

        // read the last segment
        for (int j = 0; j < SEGMENT_FILL_COUNT - 1; j++) {
            bbC = reader0.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            bbC.discard();
            bbC = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            bbC.discard();
        }

        assertNull(reader0.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));
        assertNull(reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));
        assertEquals(1, m_pbd.numberOfSegments());
    }

    @Test
    public void testTransientReaderSkipPastDeleted() throws Exception {
        // Test to verify that once regular reader finishes reading a segment,
        // those segments get deleted, even if transient reader has not read it.
        PersistentBinaryDeque<?>.ReadCursor transientReader = m_pbd.openForRead("transient", true);
        PersistentBinaryDeque<?>.ReadCursor regularReader = m_pbd.openForRead("nontransient");
        int numSegments = 3;
        int numBuffers = 10;
        // Create multiple segments
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            for (int j=0; j<numBuffers; j++) {
                m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(i)));
            }
        }

        // Both readers read just one buffer
        readBuffer(transientReader);
        readBuffer(regularReader);

        for (int i=0; i<numSegments; i++) {
            for (int j=0; j<numBuffers; j++) {
                readBuffer(regularReader);
            }
            readBuffer(transientReader);
            // Regular reader reads past one segment, while transient one doesn't.
            // Verify that on next read, transient reader gets the buffer after the segment that got deleted.
            assertEquals(regularReader.getCurrentSegment().m_id, transientReader.getCurrentSegment().m_id);
        }

        // Write more and read all using regular reader.
        // Transient reader should only find the one buffer in the last segment that is not deleted.
        for (int i=0; i<numBuffers; i++) {
            m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(i)));
        }
        m_pbd.updateExtraHeader(null);
        m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)));
        for (int i=0; i<=numBuffers; i++) {
            readBuffer(regularReader);
        }
        readBuffer(transientReader);
        assertNull(transientReader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));
    }

    @Test
    public void testTransientReaderDoesNotDelete() throws Exception {
        int numSegments = 5;
        int numBuffers = 10;
        // Create multiple segments
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            for (int j=0; j<numBuffers; j++) {
                m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(i)));
            }
        }

        // Verify that transient readers read through and no segments get deleted
        int totalBuffers = numSegments * numBuffers;
        PersistentBinaryDeque<?>.ReadCursor transient1 = m_pbd.openForRead("transient1", true);
        assertEquals(totalBuffers, readMultipleBuffers(transient1, totalBuffers));
        assertEquals(numSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        PersistentBinaryDeque<?>.ReadCursor transient2 = m_pbd.openForRead("transient2", true);
        assertEquals(totalBuffers, readMultipleBuffers(transient2, totalBuffers));
        assertEquals(numSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

        // Verify that regular reader reads through and segments get deleted
        PersistentBinaryDeque<?>.ReadCursor regular = m_pbd.openForRead("regular");
        assertEquals(totalBuffers, readMultipleBuffers(regular, totalBuffers));
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
    }

    private int readMultipleBuffers(PersistentBinaryDeque<?>.ReadCursor reader, int numBuffers) throws IOException {
        int count = 0;
        for (int i=0; i<numBuffers; i++) {
            BBContainer bb = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            if (bb != null) {
                bb.discard();
                count++;
            } else {
                break;
            }
        }

        return count;
    }

    private void readBuffer(PersistentBinaryDeque<?>.ReadCursor reader) throws IOException {
        BBContainer bb = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        if (bb != null) {
            bb.discard();
        }
    }

    @Before
    public void setUp() throws Exception {
        TestPersistentBinaryDeque.setupTestDir();
        m_pbd = PersistentBinaryDeque
                .builder(TestPersistentBinaryDeque.TEST_NONCE, TestPersistentBinaryDeque.TEST_DIR, logger).build();
    }

    @After
    public void tearDown() throws Exception {
        try {
            m_pbd.close();
        } catch (Exception e) {}
        try {
            TestPersistentBinaryDeque.tearDownTestDir();
        } finally {
            m_pbd = null;
        }
        System.gc();
        System.runFinalization();
    }
}
