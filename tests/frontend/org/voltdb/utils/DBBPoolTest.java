/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import junit.framework.TestCase;

import org.voltdb.utils.DBBPool.BBContainer;

public class DBBPoolTest extends TestCase {

    final int NUM_BUFFERS = 10;
    final int INITIAL_ALLOCATION = 8096;
    @Override
    public void setUp() {
    }

    public void testDefaultPoolCtor() {
        DBBPool p1 = new DBBPool();
        assertEquals(0, p1.bytesLoanedLocally());
        BBContainer containers1[] = p1.acquire( NUM_BUFFERS, INITIAL_ALLOCATION);
        assertTrue((NUM_BUFFERS * INITIAL_ALLOCATION) <= p1.bytesAllocatedGlobally());
        for (BBContainer c : containers1) {
            c.discard();
        }

        // verify that globalbytes is really all pools
        DBBPool p2 = new DBBPool();
        BBContainer containers2[] = p2.acquire( 33, INITIAL_ALLOCATION);
        assertTrue(p1.bytesAllocatedGlobally() >=
                     (p1.bytesAllocatedLocally() + p2.bytesAllocatedLocally()));

        for (BBContainer c : containers2) {
            c.discard();
        }

        // discard the buffer and see that the pool has no bytes loaned.
        assertEquals(0, p1.bytesLoanedLocally());
        assertEquals(0, p2.bytesLoanedLocally());

        p1.clear();
        p2.clear();
    }

    public void testGrowingPool() {
        int bufsize = 8096;
        int bufsize2 = 80480;

        DBBPool p1 = new DBBPool();
        assertEquals(0, p1.bytesLoanedLocally());

        // test that pool will grow if necessary
        ArrayList<DBBPool.BBContainer> list = new ArrayList<DBBPool.BBContainer>();

        for (int i=1; i<= 500; ++i) {
            p1.acquire(bufsize2).discard();
        }

        for (int i=1; i<= 500; ++i) {
            list.add(p1.acquire(bufsize));

            assertTrue(bufsize*i <= p1.bytesLoanedLocally());
        }

        // and give them all back
        for (DBBPool.BBContainer c : list) {
            assertTrue(bufsize <= c.b.capacity());
            long loaned = p1.bytesLoanedLocally();
            c.discard();
            c = null;
            // see if bufsize bytes are no longer on loan.
            assertTrue(loaned >= p1.bytesLoanedLocally() + bufsize);
        }

        // pool by design should shrink from that max size of 100MB.
        //assertTrue(100*bufsize > p1.bytesAllocatedLocally());// Only give a warning now
        assertEquals(0, p1.bytesLoanedLocally());
        p1.clear();
    }

    public void testInputStreamReadWrite() {
        DBBPool p1 = new DBBPool();
        BBInputStream is = new BBInputStream();
        byte val = 0;

        // fill buffers with incrementing byte values, wrapping
        // when byte reaches 255.
        for (int i=0; i<100; ++i) {
            DBBPool.BBContainer p1c = p1.acquire(4096);
            for (int j=0; j < 50; ++j) {
                p1c.b.put(val);
                if (val == 255) {
                    val = 0;
                } else {
                    val++;
                }
            }
            p1c.b.flip();
            is.offer(p1c);
        }

        // and read back the inputstream content, verifying the data.
        try {
            val = 0;
            for (int i=0; i<100; i++) {
                for (int j=0; j < 50; ++j) {
                    assertEquals(val, is.read());
                    if (val == 255) {
                        val = 0;
                    } else {
                        val++;
                    }
                }
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        p1.clear();
    }

    public void testInputStreamReadByteArray() {
        DBBPool p1 = new DBBPool();
        BBInputStream is = new BBInputStream();
        int numbufs = 5;

        // fill buffers with incrementing byte values, wrapping
        // when byte reaches 255.
        byte val = 0;

        for (int i=0; i < numbufs; ++i) {
            DBBPool.BBContainer p1c = p1.acquire(8096);
            for (int j=0; j < 50; ++j) {
                p1c.b.put(val);
                if (val == 255) {
                    val = 0;
                } else {
                    val++;
                }
            }
            p1c.b.flip();
            is.offer(p1c);
        }

        // read the full contents with the same wrapping logic as above
        byte content[] = new byte[50*numbufs];
        try {
            int bytes_read = is.read(content, 0, 50*numbufs);
            assertEquals(50*numbufs, bytes_read);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }

        val = 0;
        for (int j=0; j < 50*numbufs; ++j) {
            Byte b1 = new Byte(val);
            Byte b2 = new Byte(content[j]);
            assertEquals(0, b1.compareTo(b2));

            if (val == 255) {
                val = 0;
            } else {
                val++;
            }
        }
//        is.close();
        p1.clear();
    }

    public void testCorrectArenaAllocation() {
        DBBPool p = new DBBPool();
        int allocationSize = 1;

        DBBPool.BBContainer c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(16, c.b.capacity());
        c.discard();

        allocationSize = 2;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(16, c.b.capacity());
        c.discard();


        allocationSize = 4;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(16, c.b.capacity());
        c.discard();

        allocationSize = 8;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(16, c.b.capacity());
        c.discard();

        allocationSize = 16;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 32;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 64;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 128;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 256;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 512;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 1024;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 2048;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 4096;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 8192;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 16384;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 32768;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 65536;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 131072;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 262144;
        c = p.acquire(allocationSize);
        assertTrue(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();

        allocationSize = 262145;
        c = p.acquire(allocationSize);
        assertFalse(c.b.isDirect());
        assertEquals(allocationSize, c.b.capacity());
        c.discard();
    }

    public void testInputStreamAvailable() {
        // fill up an array with random data and read it in chunks,
        // checking that the stream reports valid available().  and
        // that the data returned is valid.

        int avail = 4096;
        int bytes_read = 0;
        byte content[] = new byte[avail];
        byte read[] = new byte[avail];

        DBBPool p = new DBBPool();
        DBBPool.BBContainer pc = p.acquire(avail);

        Random random = new Random(0);
        random.nextBytes(content);
        pc.b.put(content);
        pc.b.flip();

        BBInputStream is = new BBInputStream();
        is.offer(pc);
        is.EOF();

        try {
            // verify initial available() size.
            System.out.println("available: " + is.available());
            assertEquals(avail, is.available());

            // read 200 byte chunks and verify
            avail -= 200;
            bytes_read = is.read(read, 0, 200);
            assertEquals(200, bytes_read);
            assertEquals(avail, is.available());
            for (int i=0; i < 200; i++) {
                assertEquals(content[i], read[i]);
            }

            // read 200 byte chunks and verify data.
            avail -= 200;
            bytes_read = is.read(read, 0, 200);
            assertEquals(200, bytes_read);
            assertEquals(avail, is.available());
            for (int i=0; i < 200; i++) {
                assertEquals(content[i+200], read[i]);
            }

            // read 200 byte chunks and verify data.
            avail -= 200;
            bytes_read = is.read(read, 0, 200);
            assertEquals(200, bytes_read);
            assertEquals(avail, is.available());
            for (int i=0; i < 200; i++) {
                assertEquals(content[i+400], read[i]);
            }

            // read exactly the rest of the data and verify
            bytes_read = is.read(read, 0, avail);
            assertEquals(avail, bytes_read);
            assertEquals(0, is.available());
            for (int i=0; i < avail; i++) {
                assertEquals(content[i+600], read[i]);
            }

            // one more read should get eof
            bytes_read = is.read(read, 0, 200);
            assertEquals(-1, bytes_read);
            assertEquals(0, is.available());
        }
        catch (IOException e) {
            fail(e.getMessage());
        }
        p.clear();
    }

    public void testInputStreamLastRead() {
        // fill up an array with random data and read it in two large
        // chunks, asking for more than is available in the second
        // read.

        int avail = 4096;
        int bytes_read = 0;
        byte content[] = new byte[avail];
        byte read[] = new byte[avail];

        DBBPool p = new DBBPool();
        DBBPool.BBContainer pc = p.acquire(avail);

        Random random = new Random(0);
        random.nextBytes(content);
        pc.b.put(content);
        pc.b.flip();

        BBInputStream is = new BBInputStream();
        is.offer(pc);
        is.EOF();

        try {
            // read most of the data.
            bytes_read = is.read(read, 0, 4000);
            assertEquals(4000, bytes_read);
            assertEquals(96, is.available());
            for (int i=0; i < 4000; i++) {
                assertEquals(content[i], read[i]);
            }

            // read the rest, requesting > avail.
            bytes_read = is.read(read, 0, 4000);
            assertEquals(96, bytes_read);
            assertEquals(0, is.available());
            for (int i=0; i < 96; i++) {
                assertEquals(content[i+4000],read[i]);
            }

            // one more read should get eof
            bytes_read = is.read(read, 0, 200);
            assertEquals(-1, bytes_read);
            assertEquals(0, is.available());
        }
        catch (IOException e) {
            fail(e.getMessage());
        }
        p.clear();
    }

    public void testMultiBufferRead() {
        // offer 2 4k buffers from a stream and read them
        // back with a single 8k read.
        int avail = 4096;
        int bytes_read = 0;
        byte content1[] = new byte[avail];
        byte content2[] = new byte[avail];
        byte read[] = new byte[avail*2];

        BBInputStream is = new BBInputStream();
        DBBPool p = new DBBPool();
        DBBPool.BBContainer pc = p.acquire(avail);

        Random random = new Random(0);
        random.nextBytes(content1);
        pc.b.put(content1);
        pc.b.flip();
        is.offer(pc);

        pc = p.acquire(avail);
        random.nextBytes(content2);
        pc.b.put(content2);
        pc.b.flip();
        is.offer(pc);
        is.EOF();
        try {
            assertEquals(avail*2, is.available());

            bytes_read = is.read(read, 0, avail*2);
            assertEquals(avail*2, bytes_read);
            assertEquals(0, is.available());
            for (int i=0; i < avail; i++) {
                assertEquals(content1[i], read[i]);
            }
            for (int i=0; i < avail; i++) {
                assertEquals(content2[i], read[avail+i]);
            }
        }
        catch (IOException e) {
            fail (e.getMessage());
        }
        p.clear();
    }
}
