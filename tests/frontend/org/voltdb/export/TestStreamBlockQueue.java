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

package org.voltdb.export;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.MockVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.utils.BinaryDequeReader;

import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;

public class TestStreamBlockQueue extends TestCase {

    private final static String TEST_DIR = "/tmp/" + System.getProperty("user.name");

    private static final String TEST_NONCE = "pbd_nonce";

    private static final ByteBuffer defaultBuffer = getFilledBuffer((byte)42);

    private StreamBlockQueue m_sbq;

    private static long g_seqNo = 0;
    private MockVoltDB m_mockVoltDB;
    private static ByteBuffer getFilledBuffer(byte fillValue) {
        //8 bytes is magic prefix space
        ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 2 + StreamBlock.HEADER_SIZE);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        while (buf.hasRemaining()) {
            buf.put(fillValue);
        }
        buf.clear();
        return buf;
    }

    private static StreamBlock getStreamBlockWithFill(byte fillValue) {
        g_seqNo += 100;
        BBContainer cont = DBBPool.wrapBB(getFilledBuffer(fillValue));
        return new StreamBlock(BinaryDequeReader.Entry.wrap(cont), g_seqNo, g_seqNo, 1, 0L);
    }

    @Override
    @Before
    public void setUp() throws Exception {
        m_mockVoltDB = new MockVoltDB();
        m_mockVoltDB.addTable("TableName", false);
        m_mockVoltDB.addColumnToTable("TableName", "COL1", VoltType.INTEGER, false, null, VoltType.INTEGER);
        m_mockVoltDB.addColumnToTable("TableName", "COL2", VoltType.STRING, false, null, VoltType.STRING);
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        g_seqNo = 0;
        File testDir = new File(TEST_DIR);
        FileUtils.deleteDirectory(testDir);
        testDir.mkdir();
        m_sbq = new StreamBlockQueue(TEST_DIR, TEST_NONCE, "TableName", 1, m_mockVoltDB.getCatalogContext().m_genId);
        defaultBuffer.clear();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            m_mockVoltDB.shutdown(null);
            m_sbq.close();
        } finally {
            try {
                File testDir = new File(TEST_DIR);
                if (testDir.exists()) {
                    for (File f : testDir.listFiles()) {
                        f.delete();
                    }
                    testDir.delete();
                }
            } finally {
                m_sbq = null;
            }
        }
        System.gc();
        System.gc();
        System.gc();
        System.runFinalization();
    }

    @Test
    public void testOfferCloseReopen() throws Exception {
        StreamBlock sb = null;
        assertTrue(m_sbq.isEmpty());
        sb = getStreamBlockWithFill((byte)1);
        m_sbq.offer(sb);
        StreamBlock fromPeek = m_sbq.peek();
        assertEquals(sb.startSequenceNumber(), fromPeek.startSequenceNumber());
        assertEquals(sb.totalSize(), fromPeek.totalSize());
        assertFalse(m_sbq.isEmpty());
        assertEquals(m_sbq.sizeInBytes(), 1024 * 1024 * 2);
        m_sbq.close(); // sb is also discarded here
        System.gc();
        System.gc();
        System.gc();
        System.runFinalization();

        m_sbq = new StreamBlockQueue(TEST_DIR, TEST_NONCE, "TableName", 1, m_sbq.getGenerationIdCreated());
        sb = m_sbq.peek();
        assertFalse(m_sbq.isEmpty());
        assertEquals(m_sbq.sizeInBytes(), 1024 * 1024 * 2);//USO and length prefix on disk
        assertEquals(sb, m_sbq.poll());
        assertTrue(sb.startSequenceNumber() == g_seqNo);
        assertEquals(sb.totalSize(), 1024 * 1024 * 2);
        assertTrue(m_sbq.isEmpty());
        assertNull(m_sbq.peek());
        assertNull(m_sbq.peek());
        assertNull(m_sbq.poll());
        assertTrue(m_sbq.isEmpty());

        BBContainer cont = sb.unreleasedContainer();
        ByteBuffer buffer = cont.b();
        try {
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            while (buffer.hasRemaining()) {
                assertEquals(buffer.get(), 1);
            }
        } finally {
            cont.discard();
        }
        sb.discard();
    }

    @Test
    public void testFuzz() throws Exception {
        byte zero = (byte)0;
        Random r = new java.util.Random(0);

        /*
         * Found a lot of issues running the first 1500 iterations, and after
         * that I stopped having problems. This test is more useful with assertions
         * enabled.
         */
        for (int iteration = 0; iteration < 1500; iteration++) {
            int action = r.nextInt(6);
            iteration++;
            switch (action) {
                case 5:
                case 0:
                    System.out.println("Iteration " + iteration + " Action offer");
                m_sbq.offer(getStreamBlockWithFill(zero));
                    break;
                case 1:
                    System.out.println("Iteration " + iteration + " Action sync");
                    m_sbq.sync();
                    break;
                case 2:
                    System.out.println("Iteration " + iteration + " Action peek");
                    m_sbq.peek();
                    break;
                case 3:
                    System.out.println("Iteration " + iteration + " Action pop");
                    try {
                        m_sbq.pop().discard();
                    } catch (NoSuchElementException e) {
                        e.printStackTrace();
                    }
                    break;
                case 4:
                    System.out.println("Iteration " + iteration + " Action iterate");
                    Iterator<StreamBlock> i = m_sbq.iterator();
                    StreamBlock firstBlock = null;
                    while (i.hasNext()) {
                       if (firstBlock == null) {
                           firstBlock = i.next();
                       } else {
                           i.next();
                       }
                    }
                    if (firstBlock != null) {
                        m_sbq.pop().discard();
                    }
            }
        }
    }

    @Test
    public void testIterator() throws Exception {
        for (byte ii = 0; ii < 32; ii++) {
            StreamBlock sb = getStreamBlockWithFill(ii);
            m_sbq.offer(sb);
            if (ii == 2) {
                m_sbq.poll().discard();
            }
        }
        assertEquals(m_sbq.sizeInBytes(), ((1024 * 1024 * 2) * 31));
        Iterator<StreamBlock> iter = m_sbq.iterator();

        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        long seqnum = 200; // first block was discarded
        for (int ii = 1; ii < 32; ii++) {
            assertTrue(iter.hasNext());
            StreamBlock sb = iter.next();
            blocks.add(sb);
            assertEquals(sb.startSequenceNumber(), seqnum);
            seqnum += 100;
            assertEquals(sb.totalSize(), 1024 * 1024 * 2);
            BBContainer cont = sb.unreleasedContainer();
            ByteBuffer buf = cont.b();
            try {
                while (buf.hasRemaining()) {
                    assertEquals(buf.get(), ii);
             }
            } finally {
                cont.discard();
            }
        }

        boolean threw = false;
        try {
            iter.next();
        } catch (NoSuchElementException e) {
            threw = true;
        }
        assertTrue(threw);

        assertEquals(m_sbq.sizeInBytes(), (1024 * 1024 * 2) * 31);

        iter = m_sbq.iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }

        for (StreamBlock sb : blocks) {
            sb.discard();
        }
        assertEquals(m_sbq.sizeInBytes(), 0);
        assertTrue(m_sbq.isEmpty());

    }

    /**
     * This variation triggers some slightly different logic in offer
     * @throws Exception
     */
    @Test
    public void testIterator2() throws Exception {
        for (byte ii = 0; ii < 32; ii++) {
            StreamBlock sb = getStreamBlockWithFill(ii);
            m_sbq.offer(sb);
            if (ii == 2) {
                m_sbq.poll().discard();
                m_sbq.poll().discard();
            }
        }
        long weirdSizeValue = (1024 * 1024 * 2 * 30);
        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);
        Iterator<StreamBlock> iter = m_sbq.iterator();

        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        long seqnum = 300; // first two block were discarded
        for (int ii = 2; ii < 32; ii++) {
            assertTrue(iter.hasNext());
            StreamBlock sb = iter.next();
            blocks.add(sb);
            assertEquals(sb.startSequenceNumber(), seqnum);
            seqnum += 100;
            assertEquals(sb.totalSize(), 1024 * 1024 * 2);
            BBContainer cont = sb.unreleasedContainer();
            ByteBuffer buf = cont.b();
            try {
                while (buf.hasRemaining()) {
                    assertEquals(buf.get(), ii);
                }
            } finally {
                cont.discard();
            }
        }

        boolean threw = false;
        try {
            iter.next();
        } catch (NoSuchElementException e) {
            threw = true;
        }
        assertTrue(threw);

        assertEquals(m_sbq.sizeInBytes(), 1024 * 1024 * 2 * 30);

        iter = m_sbq.iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }

        for (StreamBlock sb : blocks) {
            sb.discard();
        }
        assertEquals(m_sbq.sizeInBytes(), 0);
        assertTrue(m_sbq.isEmpty());

    }

    /**
     * This variation triggers some slightly different logic in offer
     * @throws Exception
     */
    @Test
    public void testIterator2WithSync() throws Exception {
        for (byte ii = 0; ii < 32; ii++) {
            StreamBlock sb = getStreamBlockWithFill(ii);
            m_sbq.offer(sb);
            if (ii == 2) {
                m_sbq.poll().discard();
                m_sbq.poll().discard();
            }
        }
        //Strange scenario where one block in the memory deque is persisted and the others isn't
        long weirdSizeValue = 1024 * 1024 * 2 * 30;
        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);

        m_sbq.sync();

        Iterator<StreamBlock> iter = m_sbq.iterator();

        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        long seqnum = 300; // first two blocks were discarded
        for (int ii = 2; ii < 32; ii++) {
            assertTrue(iter.hasNext());
            StreamBlock sb = iter.next();
            blocks.add(sb);
            assertEquals(sb.startSequenceNumber(), seqnum);
            seqnum += 100;
            assertEquals(sb.totalSize(), 1024 * 1024 * 2);
            BBContainer cont = sb.unreleasedContainer();
            ByteBuffer buf = cont.b();
            try {
                while (buf.hasRemaining()) {
                    assertEquals(buf.get(), ii);
                }
            } finally {
                cont.discard();
            }
        }

        boolean threw = false;
        try {
            iter.next();
        } catch (NoSuchElementException e) {
            threw = true;
        }
        assertTrue(threw);

        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);

        iter = m_sbq.iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }

        for (StreamBlock sb : blocks) {
            sb.discard();
        }
        assertEquals(m_sbq.sizeInBytes(), 0);
        assertTrue(m_sbq.isEmpty());

    }

    /**
     * @throws Exception
     */
    @Test
    public void testSyncReopenThenPop() throws Exception {
        for (byte ii = 0; ii < 32; ii++) {
            StreamBlock sb = getStreamBlockWithFill(ii);
            m_sbq.offer(sb);
            if (ii == 2) {
                m_sbq.poll().discard();
                m_sbq.poll().discard();
            }
        }
        //Strange scenario where one block in the memory deque is persisted and the others isn't
        long weirdSizeValue = ((1024 * 1024 * 2) * 30);
        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);

        m_sbq.sync();
        long genId = m_sbq.getGenerationIdCreated();

        m_sbq.close();
        m_sbq = null;
        System.gc();
        System.runFinalization();
        m_sbq = new StreamBlockQueue(  TEST_DIR, TEST_NONCE, "TableName", 1, genId);
        System.gc();
        System.runFinalization();
        StreamBlock sb = null;
        long seqnum = 100; // SBQ was reopened.
        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        int ii = 0;
        while (ii < 32 && (sb = m_sbq.pop()) != null) {
            blocks.add(sb);
            assertEquals(sb.startSequenceNumber(), seqnum);
            seqnum += 100;
            assertEquals(sb.totalSize(), 1024 * 1024 * 2);
            BBContainer cont = sb.unreleasedContainer();
            ByteBuffer buf = cont.b();
            try {
                while (buf.hasRemaining()) {
                    assertEquals(buf.get(), ii);
                }
            } finally {
                cont.discard();
            }
            ii++;
        }
        System.gc();
        System.runFinalization();
        boolean threw = false;
        try {
            m_sbq.pop();
        } catch (NoSuchElementException e) {
            threw = true;
        }
        assertTrue(threw);

        for (StreamBlock streamBlock : blocks) {
            streamBlock.discard();
        }
        assertEquals(m_sbq.sizeInBytes(), 0);
        assertTrue(m_sbq.isEmpty());
        System.gc();
        System.runFinalization();
    }

    /**
     * Some more slightly different logic in offer
     * @throws Exception
     */
    @Test
    public void testIterator3() throws Exception {
        for (byte ii = 0; ii < 32; ii++) {
            StreamBlock sb = getStreamBlockWithFill(ii);
            m_sbq.offer(sb);
            if (ii == 3) {
                m_sbq.poll().discard();
                m_sbq.poll().discard();
            }
        }
        //Strange scenario where one block in the memory deque is persisted and the others isn't
        long weirdSizeValue = 1024 * 1024 * 2 * 30;
        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);
        Iterator<StreamBlock> iter = m_sbq.iterator();

        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        long seqnum = 300;
        for (int ii = 2; ii < 32; ii++) {
            assertTrue(iter.hasNext());
            StreamBlock sb = iter.next();
            blocks.add(sb);
            assertEquals(sb.startSequenceNumber(), seqnum);
            seqnum += 100;
            assertEquals(sb.totalSize(), 1024 * 1024 * 2);
            BBContainer cont = sb.unreleasedContainer();
            ByteBuffer buf = cont.b();
            try {
                while (buf.hasRemaining()) {
                    assertEquals(buf.get(), ii);
                }
            } finally {
                cont.discard();
            }
        }

        boolean threw = false;
        try {
            iter.next();
        } catch (NoSuchElementException e) {
            threw = true;
        }
        assertTrue(threw);

        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);

        iter = m_sbq.iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }

        for (StreamBlock sb : blocks) {
            sb.discard();
        }
        assertEquals(m_sbq.sizeInBytes(), 0);
        assertTrue(m_sbq.isEmpty());

    }

    /**
     * Test a branch in iterator where there is nothing in the in memory deque.
     * This happens when hasNext() isn't invoked
     * @throws Exception
     */
    @Test
    public void testIterator4() throws Exception {
        for (byte ii = 0; ii < 32; ii++) {
            StreamBlock sb = getStreamBlockWithFill(ii);
            m_sbq.offer(sb);
        }
        //Strange scenario where one block in the memory deque is persisted and the others isn't
        long weirdSizeValue = 1024 * 1024 * 2 * 32;
        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);
        Iterator<StreamBlock> iter = m_sbq.iterator();

        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        long seqnum = 100;
        for (int ii = 0; ii < 32; ii++) {
            StreamBlock sb = iter.next();
            blocks.add(sb);
            assertEquals(sb.startSequenceNumber(), seqnum);
            seqnum += 100;
            assertEquals(sb.totalSize(), 1024 * 1024 * 2);
            BBContainer cont = sb.unreleasedContainer();
            ByteBuffer buf = cont.b();
            try {
                while (buf.hasRemaining()) {
                    assertEquals(buf.get(), ii);
                }
            } finally {
                cont.discard();
            }
        }

        boolean threw = false;
        try {
            iter.next();
        } catch (NoSuchElementException e) {
            threw = true;
        }
        assertTrue(threw);

        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);

        iter = m_sbq.iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }

        for (StreamBlock sb : blocks) {
            sb.discard();
        }
        assertEquals(m_sbq.sizeInBytes(), 0);
        assertTrue(m_sbq.isEmpty());

    }
}
