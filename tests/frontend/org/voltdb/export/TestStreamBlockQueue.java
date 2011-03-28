/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.*;
import java.nio.*;
import java.util.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.VoltFile;

public class TestStreamBlockQueue {

    private final static String TEST_DIR = "/tmp/" + System.getProperty("user.name");

    private static final String TEST_NONCE = "pbd_nonce";

    private static final ByteBuffer defaultBuffer = getFilledBuffer((byte)42);

    private StreamBlockQueue m_sbq;

    private static long g_uso = 0;
    private static ByteBuffer getFilledBuffer(byte fillValue) {
        ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 2);
        while (buf.hasRemaining()) {
            buf.put(fillValue);
        }
        buf.clear();
        return buf;
    }

    private static StreamBlock getStreamBlockWithFill(byte fillValue) {
        g_uso += 1024 * 1024 * 2;
        return new StreamBlock(DBBPool.wrapBB(getFilledBuffer(fillValue)), g_uso, false);
    }

    @Test
    public void testOfferCloseReopen() throws Exception {
        assertTrue(m_sbq.isEmpty());
        StreamBlock sb = getStreamBlockWithFill((byte)1);
        m_sbq.offer(sb);
        assertEquals(sb, m_sbq.peek());
        assertFalse(m_sbq.isEmpty());
        assertEquals(m_sbq.sizeInBytes(), 1024 * 1024 * 2);
        m_sbq.close();
        System.gc();
        System.gc();
        System.gc();
        System.runFinalization();
        m_sbq = new StreamBlockQueue(  TEST_DIR, TEST_NONCE);
        sb = m_sbq.peek();
        assertFalse(m_sbq.isEmpty());
        assertEquals(m_sbq.sizeInBytes(), 1024 * 1024 * 2 + 12);//USO and length prefix on disk
        assertEquals(sb, m_sbq.poll());
        assertTrue(sb.uso() == g_uso);
        assertEquals(sb.totalUso(), 1024 * 1024 * 2);
        assertTrue(m_sbq.isEmpty());
        assertNull(m_sbq.peek());
        assertNull(m_sbq.peek());
        assertNull(m_sbq.poll());
        assertTrue(m_sbq.isEmpty());

        ByteBuffer buffer = sb.unreleasedBuffer();
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(buffer.getInt(), 1024 * 1024 * 2);
        while (buffer.hasRemaining()) {
            assertEquals(buffer.get(), 1);
        }
        sb.deleteContent();
    }

    @Test
    public void testIterator() throws Exception {
        for (byte ii = 0; ii < 32; ii++) {
            StreamBlock sb = getStreamBlockWithFill(ii);
            m_sbq.offer(sb);
            if (ii == 2) {
                m_sbq.poll().deleteContent();
            }
        }
        assertEquals(m_sbq.sizeInBytes(), (1024 * 1024 * 2) + ((1024 * 1024 * 2 + 12) * 30));
        Iterator<StreamBlock> iter = m_sbq.iterator();

        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        long uso = 1024 * 1024 * 4;
        for (int ii = 1; ii < 32; ii++) {
            assertTrue(iter.hasNext());
            StreamBlock sb = iter.next();
            blocks.add(sb);
            assertEquals(sb.uso(), uso);
            uso += 1024 * 1024 * 2;
            assertEquals(sb.totalUso(), 1024 * 1024 * 2);
            ByteBuffer buf = sb.unreleasedBuffer();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            assertEquals(buf.getInt(), 1024 * 1024 * 2);
            while (buf.hasRemaining()) {
                assertEquals(buf.get(), ii);
            }
        }

        boolean threw = false;
        try {
            iter.next();
        } catch (NoSuchElementException e) {
            threw = true;
        }
        assertTrue(threw);

        assertEquals(m_sbq.sizeInBytes(), (1024 * 1024 * 2) + ((1024 * 1024 * 2 + 12) * 30));

        iter = m_sbq.iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
        }

        for (StreamBlock sb : blocks) {
            sb.deleteContent();
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
                m_sbq.poll().deleteContent();
                m_sbq.poll().deleteContent();
            }
        }
        long weirdSizeValue = ((1024 * 1024 * 2 + 12) * 30);
        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);
        Iterator<StreamBlock> iter = m_sbq.iterator();

        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        long uso = 1024 * 1024 * 6;
        for (int ii = 2; ii < 32; ii++) {
            assertTrue(iter.hasNext());
            StreamBlock sb = iter.next();
            blocks.add(sb);
            assertEquals(sb.uso(), uso);
            uso += 1024 * 1024 * 2;
            assertEquals(sb.totalUso(), 1024 * 1024 * 2);
            ByteBuffer buf = sb.unreleasedBuffer();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            assertEquals(buf.getInt(), 1024 * 1024 * 2);
            while (buf.hasRemaining()) {
                assertEquals(buf.get(), ii);
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
            sb.deleteContent();
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
                m_sbq.poll().deleteContent();
                m_sbq.poll().deleteContent();
            }
        }
        //Strange scenario where one block in the memory deque is persisted and the others isn't
        long weirdSizeValue = ((1024 * 1024 * 2 + 12) * 30);
        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);

        m_sbq.sync(true);

        Iterator<StreamBlock> iter = m_sbq.iterator();

        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        long uso = 1024 * 1024 * 6;
        for (int ii = 2; ii < 32; ii++) {
            assertTrue(iter.hasNext());
            StreamBlock sb = iter.next();
            blocks.add(sb);
            assertEquals(sb.uso(), uso);
            uso += 1024 * 1024 * 2;
            assertEquals(sb.totalUso(), 1024 * 1024 * 2);
            ByteBuffer buf = sb.unreleasedBuffer();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            assertEquals(buf.getInt(), 1024 * 1024 * 2);
            while (buf.hasRemaining()) {
                assertEquals(buf.get(), ii);
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
            sb.deleteContent();
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
                m_sbq.poll().deleteContent();
                m_sbq.poll().deleteContent();
            }
        }
        //Strange scenario where one block in the memory deque is persisted and the others isn't
        long weirdSizeValue = ((1024 * 1024 * 2 + 12) * 30);
        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);

        m_sbq.sync(true);

        m_sbq.close();
        m_sbq = null;
        System.gc();
        System.runFinalization();
        m_sbq = new StreamBlockQueue(  TEST_DIR, TEST_NONCE);
        System.gc();
        System.runFinalization();
        StreamBlock sb = null;
        long uso = 1024 * 1024 * 6;
        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        int ii = 2;
        while (ii < 32 && (sb = m_sbq.pop()) != null) {
            blocks.add(sb);
            assertEquals(sb.uso(), uso);
            uso += 1024 * 1024 * 2;
            assertEquals(sb.totalUso(), 1024 * 1024 * 2);
            ByteBuffer buf = sb.unreleasedBuffer();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            assertEquals(buf.getInt(), 1024 * 1024 * 2);
            while (buf.hasRemaining()) {
                assertEquals(buf.get(), ii);
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
            streamBlock.deleteContent();
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
                m_sbq.poll().deleteContent();
                m_sbq.poll().deleteContent();
            }
        }
        //Strange scenario where one block in the memory deque is persisted and the others isn't
        long weirdSizeValue = ((1024 * 1024 * 2 + 12) * 30);
        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);
        Iterator<StreamBlock> iter = m_sbq.iterator();

        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        long uso = 1024 * 1024 * 6;
        for (int ii = 2; ii < 32; ii++) {
            assertTrue(iter.hasNext());
            StreamBlock sb = iter.next();
            blocks.add(sb);
            assertEquals(sb.uso(), uso);
            uso += 1024 * 1024 * 2;
            assertEquals(sb.totalUso(), 1024 * 1024 * 2);
            ByteBuffer buf = sb.unreleasedBuffer();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            assertEquals(buf.getInt(), 1024 * 1024 * 2);
            while (buf.hasRemaining()) {
                assertEquals(buf.get(), ii);
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
            sb.deleteContent();
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
        long weirdSizeValue = ((1024 * 1024 * 2) * 2) + ((1024 * 1024 * 2 + 12) * 30);
        assertEquals(m_sbq.sizeInBytes(), weirdSizeValue);
        Iterator<StreamBlock> iter = m_sbq.iterator();

        ArrayList<StreamBlock> blocks = new ArrayList<StreamBlock>();
        long uso = 1024 * 1024 * 2;
        for (int ii = 0; ii < 32; ii++) {
            StreamBlock sb = iter.next();
            blocks.add(sb);
            assertEquals(sb.uso(), uso);
            uso += 1024 * 1024 * 2;
            assertEquals(sb.totalUso(), 1024 * 1024 * 2);
            ByteBuffer buf = sb.unreleasedBuffer();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            assertEquals(buf.getInt(), 1024 * 1024 * 2);
            while (buf.hasRemaining()) {
                assertEquals(buf.get(), ii);
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
            sb.deleteContent();
        }
        assertEquals(m_sbq.sizeInBytes(), 0);
        assertTrue(m_sbq.isEmpty());

    }

    @Before
    public void setUp() throws Exception {
        g_uso = 0;
        File testDir = new File(TEST_DIR);
        if (testDir.exists()) {
            for (File f : testDir.listFiles()) {
                VoltFile.recursivelyDelete(f);
            }
            testDir.delete();
        }
        testDir.mkdir();
        m_sbq = new StreamBlockQueue(  TEST_DIR, TEST_NONCE);
        defaultBuffer.clear();
    }

    @After
    public void tearDown() throws Exception {
        try {
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

}