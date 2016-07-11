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

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.BinaryDeque.BinaryDequeTruncator;
import org.voltdb.utils.BinaryDeque.TruncatorResponse;

import com.google_voltpatches.common.collect.Sets;

public class TestPBDMultipleReaders {

    private final static VoltLogger logger = new VoltLogger("EXPORT");

    private PersistentBinaryDeque m_pbd;
    
    private class PBDReader implements Runnable {
        private String m_readerId;
        private PBDReader m_dependsOn;
        private int m_totalRead;
        private Exception m_error;
        private boolean m_isLastReader;
        
        public PBDReader(String readerId, PBDReader dependsOn, boolean isLastReader) throws IOException {
            m_readerId = readerId;
            m_dependsOn = dependsOn;
            m_isLastReader = isLastReader;
            m_pbd.openForRead(m_readerId);
        }
        
        public void run() {
            try {
                TreeSet<String> segments = TestPersistentBinaryDeque.getSortedDirectoryListing();
                while (true) {
                    System.out.println(m_readerId + " totalRead=" + m_totalRead);
                    if (m_dependsOn != null && m_dependsOn.getTotalRead() <= m_totalRead) {
                        Thread.sleep(250);
                        continue;
                    }
                    BBContainer bbC = m_pbd.poll(m_readerId, PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                    if (bbC==null) {
                        System.out.println("bbC is null. Breaking out");
                        break;
                    }
                    bbC.discard();
                    int segmentsReadFully = (m_totalRead+1)/47;
                    if (!m_isLastReader) {
                        int min = segments.size() - (segmentsReadFully - (segmentsReadFully%47==0 ? 1 : 0));
                        assertTrue(TestPersistentBinaryDeque.getSortedDirectoryListing().size() >= min);
                    } else {
                        assertEquals(segments.size()-segmentsReadFully,
                                TestPersistentBinaryDeque.getSortedDirectoryListing().size());
                    }
                    m_totalRead++;
                }
            } catch(Exception e) {
                e.printStackTrace(); //TODO: remove
                m_error = e;
            }
            System.out.println("Exiting thread");
        }
        
        public int getTotalRead() {
            return m_totalRead;
        }
    }

    @Test
    public void testMultipleParallelReaders() throws Exception {
        int numBuffers = 200;
        for (int i=0; i<numBuffers; i++) {
            m_pbd.offer( DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(i)) );
        }
        
        int numReaders = 2;
        PBDReader prevReader = null;
        PBDReader[] readers = new PBDReader[numReaders];
        ExecutorService es = Executors.newFixedThreadPool(numReaders);
        for (int i=0; i<numReaders; i++) {
            readers[i] = new PBDReader("reader" + i, prevReader, (i == (numReaders-1)));
            prevReader = readers[i];
            es.submit(readers[i]);
        }
        
        es.shutdown();
        es.awaitTermination(60, TimeUnit.SECONDS);
        for (int i=0; i<numReaders; i++) {
            if (readers[i].m_error != null) {
                readers[i].m_error.printStackTrace();
                fail();
            }
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
