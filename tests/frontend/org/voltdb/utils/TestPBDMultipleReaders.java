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

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;

public class TestPBDMultipleReaders {

    private final static VoltLogger logger = new VoltLogger("EXPORT");

    private PersistentBinaryDeque m_pbd;
    
    private class PBDReader {
        private String m_readerId;
        private int m_totalRead;
        
        public PBDReader(String readerId) throws IOException {
            m_readerId = readerId;
            m_pbd.openForRead(m_readerId);
        }
        
        public void readToEndOfSegment() throws Exception {
            int end = (m_totalRead/47 + 1) * 47;
            boolean done = false;
            for (int i=m_totalRead; i<end && !done; i++) {
                BBContainer bbC = m_pbd.poll(m_readerId, PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                if (bbC==null) {
                    done = true;
                    continue;
                }
                Thread.sleep(50);
                bbC.discard();
            }
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
