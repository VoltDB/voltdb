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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.BinaryDeque.BinaryDequeReader;

public class TestPBDMultipleReaders {

    private final static VoltLogger logger = new VoltLogger("EXPORT");

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
            int end = (m_totalRead/47 + 1) * 47;
            boolean done = false;
            int numRead = 0;
            for (int i=m_totalRead; i<end && !done; i++) {
                BBContainer bbC = m_reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                if (bbC==null) {
                    done = true;
                    continue;
                }
                numRead++;
                Thread.sleep(50);
                bbC.discard();
            }

            return numRead;
        }
    }

    @Test
    public void testDemo() throws Exception {
        int numBuffers = 300;
        for (int i=0; i<numBuffers; i++) {
            m_pbd.offer( DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(i)) );
        }
        System.out.println("Initial directory contents: " + TestPersistentBinaryDeque.getSortedDirectoryListing());

        ExecutorService es = Executors.newFixedThreadPool(2);
        es.submit(new ReaderWorker(new PBDReader("consumer1")));
        es.submit(new ReaderWorker(new PBDReader("consumer2")));

        es.shutdown();
        es.awaitTermination(10, TimeUnit.MINUTES);
    }

    private static class ReaderWorker implements Runnable {
        private final PBDReader m_reader;
        public ReaderWorker(PBDReader reader) {
            m_reader = reader;
        }

        public void run() {
            try {
                int numSegments = 0;
                while (true) {
                    int numRead = m_reader.readToEndOfSegment();
                    if (numRead > 0) {
                        System.out.println(m_reader.m_readerId + " read segment " + numSegments);
                        System.out.println("Directory contents:");
                        System.out.println("\t" + TestPersistentBinaryDeque.getSortedDirectoryListing());
                        numSegments++;
                    } else {
                        System.out.println(m_reader.m_readerId + " has reached end of DR log");
                    }
                }
            } catch(Exception e) {
                e.printStackTrace();
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

    @Test
    public void testSavedReaders() throws Exception {
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

        // Close and finish reading using one reader.
        // This shouldn't delete the segments.
        m_pbd.close();
        m_pbd = new PersistentBinaryDeque(TestPersistentBinaryDeque.TEST_NONCE, TestPersistentBinaryDeque.TEST_DIR, logger );
        readers[0] = new PBDReader("reader0");
        readers[0].readToEndOfSegment();
        assertEquals(numSegments+1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

        // Once the rest of readers finish reading the segment, the segments should be deleted
        for (int i=1; i<numReaders; i++) {
            readers[i] = new PBDReader("reader" + i);
            readers[i].readToEndOfSegment();
        }
        assertEquals(numSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

        //Finish reading all segments
        for (int i=1; i<numSegments; i++) {
            for (int j=0; j<numReaders; j++) {
                readers[j].readToEndOfSegment();
            }
        }
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
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
