/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
        assertEquals(1, m_pbd.numOpenSegments());

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
