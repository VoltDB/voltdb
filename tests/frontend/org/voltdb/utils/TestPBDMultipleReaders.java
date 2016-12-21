/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.BinaryDeque.BinaryDequeReader;

public class TestPBDMultipleReaders {

    private final static VoltLogger logger = new VoltLogger("EXPORT");

    private static final int s_segmentFillCount = 47;
    private PersistentBinaryDeque m_pbd;

    private class PBDReader {
        private String m_readerId;
        private int m_totalRead;
        private BinaryDequeReader m_reader;

        public PBDReader(String readerId) throws IOException {
            m_readerId = readerId;
            m_reader = m_pbd.openForRead(m_readerId);
        }

        public int readToEndOfSegment() throws Exception {
            int end = (m_totalRead/s_segmentFillCount + 1) * s_segmentFillCount;
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
            readers[0].readToEndOfSegment();
            assertEquals(currNumSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

            // Once all readers finish reading, the segment should get discarded.
            for (int j=1; j<numReaders; j++) {
                readers[j].readToEndOfSegment();
            }
            if (i < numSegments-1) currNumSegments--;
            assertEquals(currNumSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        }
    }

    @Test
    public void testOpenReaders() throws Exception {
        String cursorId = "reader";
        BinaryDequeReader reader1 = m_pbd.openForRead(cursorId);
        BinaryDequeReader reader2 = m_pbd.openForRead(cursorId);
        BinaryDequeReader another = m_pbd.openForRead("another");
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
    public void testSegmentClosingWriterOnly() throws Exception {
        // Initially no readers and nothing written. Open segments must be 1
        assertEquals(1, m_pbd.numOpenSegments());

        for (int i=0; i<3; i++) {
            for (int j=0; j<s_segmentFillCount; j++) {
                m_pbd.offer( DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(j)) );
            }
            assertEquals(1, m_pbd.numOpenSegments());
        }
    }

    @Test
    public void testSegmentClosingWriterReaderLockStep() throws Exception {
        assertEquals(1, m_pbd.numOpenSegments());
        BinaryDequeReader reader = m_pbd.openForRead("reader0");

        for (int i=0; i<3; i++) {
            for (int j=0; j<s_segmentFillCount; j++) {
                m_pbd.offer( DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(j)) );
                BBContainer bbC = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                bbC.discard();
            }
            assertEquals(1, m_pbd.numOpenSegments());
        }
    }

    @Test
    public void testSegmentClosingWriterReader() throws Exception {
        assertEquals(1, m_pbd.numOpenSegments());

        int numSegments = 3;
        for (int i=0; i<numSegments; i++) {
            for (int j=0; j<s_segmentFillCount; j++) {
                m_pbd.offer( DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(j)) );
            }
            assertEquals(1, m_pbd.numOpenSegments());
        }

        BinaryDequeReader reader = m_pbd.openForRead("reader0");
        for (int i=0; i<numSegments; i++) {
            for (int j=0; j<46; j++) {
                BBContainer bbC = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                bbC.discard();
            }
            int expected = (i == numSegments-1) ? 1 : 2;
            assertEquals(expected, m_pbd.numOpenSegments());

            BBContainer bbC = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            bbC.discard();
            // there should only be 1 open because last discard closes and deletes
            assertEquals(1, m_pbd.numOpenSegments());
        }
    }

    @Test
    public void testSegmentClosingWriterMultipleReaders() throws Exception {
        assertEquals(1, m_pbd.numOpenSegments());

        int numSegments = 5;
        for (int i=0; i<numSegments; i++) {
            for (int j=0; j<s_segmentFillCount; j++) {
                m_pbd.offer( DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(j)) );
            }
            assertEquals(1, m_pbd.numOpenSegments());
        }

        BinaryDequeReader reader0 = m_pbd.openForRead("reader0");
        BinaryDequeReader reader1 = m_pbd.openForRead("reader1");
        // Position first reader0 on penultimate segment and reader1 on first segment
        for (int i=0; i<numSegments-1; i++) {
            for (int j=0; j<46; j++) {
                BBContainer bbC = reader0.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                bbC.discard();
                if (i==0) {
                    bbC = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                    bbC.discard();
                }
            }

            BBContainer bbC = reader0.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            bbC.discard();
            if (i==0) {
                assertEquals(2, m_pbd.numOpenSegments());
            } else {
                assertEquals(3, m_pbd.numOpenSegments());
            }
        }

        BBContainer bbC = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        bbC.discard();
        // Both readers finished reading first segment, so that is closed and deleted,
        // which reduces the # of open segments by 1
        assertEquals(2, m_pbd.numOpenSegments());

        // reader0 at penultimate. Move reader1 through segments and check open segments
        for (int i=1; i<numSegments-1; i++) {
            for (int j=0; j<46; j++) {
                bbC = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                bbC.discard();
            }
            int expected = (i == numSegments-2) ? 2 : 3;
            assertEquals(expected, m_pbd.numOpenSegments());

            bbC = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            bbC.discard();
            expected = (i == numSegments-2) ? 1 : 2;
            assertEquals(expected, m_pbd.numOpenSegments());
        }

        // read the last segment
        for (int j=0; j<s_segmentFillCount; j++) {
            bbC = reader0.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            bbC.discard();
            bbC = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            bbC.discard();
        }
        assertEquals(1, m_pbd.numOpenSegments());
    }

    @Before
    public void setUp() throws Exception {
        TestPersistentBinaryDeque.setupTestDir();
        m_pbd = new PersistentBinaryDeque(TestPersistentBinaryDeque.TEST_NONCE, TestPersistentBinaryDeque.TEST_DIR, logger );
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
