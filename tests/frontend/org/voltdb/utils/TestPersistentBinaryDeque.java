/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.utils.BinaryDeque.BinaryDequeTruncator;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;

public class TestPersistentBinaryDeque {

    private final static File TEST_DIR = new File("/tmp/" + System.getProperty("user.name"));

    private static final String TEST_NONCE = "pbd_nonce";

    private static ByteBuffer defaultBuffer() {
        return getFilledBuffer(42);
    }

    private static BBContainer defaultContainer() {
        return DBBPool.wrapBB(defaultBuffer());
    }

    private PersistentBinaryDeque m_pbd;

    private static ByteBuffer getFilledBuffer(long fillValue) {
        ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 1024 * 2);
        Random r = new Random(42);
        while (buf.remaining() > 15) {
            buf.putLong(fillValue);
            buf.putLong(r.nextLong());
        }
        buf.clear();
        return buf;
    }

    private static ByteBuffer getFilledSmallBuffer(long fillValue) {
        ByteBuffer buf = ByteBuffer.allocateDirect(1024);
        while (buf.remaining() > 15) {
            buf.putLong(fillValue);
            buf.putLong(fillValue);
        }
        buf.clear();
        return buf;
    }

    private static TreeSet<String> getSortedDirectoryListing() {
        TreeSet<String> names = new TreeSet<String>();
        for (File f : TEST_DIR.listFiles()) {
            names.add(f.getName());
        }
        return names;
    }

    @Test
    public void testTruncateFirstElement() throws Exception {
        System.out.println("Running testTruncateFirstElement");
        TreeSet<String> listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 1);

        for (int ii = 0; ii < 150; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 4);

        m_pbd.close();

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 4);

        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR );


        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 5);

        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            @Override
            public ByteBuffer parse(ByteBuffer b) {
                return ByteBuffer.allocate(0);
            }

        });

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 1);
        assertNull(m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));
    }

    @Test
    public void testEmptyTruncation() throws Exception {
        System.out.println("Running testEmptyTruncation");
        TreeSet<String> listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 1);
        m_pbd.close();

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 0);

        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR );


        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 1);

        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            @Override
            public ByteBuffer parse(ByteBuffer b) {
                fail();
                return null;
            }

        });

        for (int ii = 0; ii < 96; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        for (long ii = 0; ii < 96; ii++) {
            BBContainer cont = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            assertNotNull(cont);
            assertEquals(cont.b().remaining(), 1024 * 1024 * 2);
            while (cont.b().remaining() > 15) {
                assertEquals(cont.b().getLong(), ii);
                cont.b().getLong();
            }
            cont.discard();
        }
    }

    @Test
    public void testTruncatorWithEmptyBufferReturn() throws Exception {
        System.out.println("Running testTruncatorWithEmptyBufferReturn");
        assertNull(m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        for (int ii = 0; ii < 150; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        m_pbd.close();

        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR );

        TreeSet<String> listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 5);

        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            private long m_objectsParsed = 0;
            @Override
            public ByteBuffer parse(ByteBuffer b) {
                if (b.getLong(0) != m_objectsParsed) {
                    System.out.println("asd");
                }
                assertEquals(b.getLong(0), m_objectsParsed);
                assertEquals(b.remaining(), 1024 * 1024 * 2 );
                if (b.getLong(0) == 45) {
                    b.limit(b.remaining() / 2);
                    return ByteBuffer.allocate(0);
                }
                while (b.remaining() > 15) {
                    assertEquals(b.getLong(), m_objectsParsed);
                    b.getLong();
                }
                m_objectsParsed++;
                return null;
            }

        });

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 2);

        for (int ii = 46; ii < 96; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        long blocksFound = 0;
        BBContainer cont = null;
        while ((cont = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
            try {
                ByteBuffer buffer = cont.b();
                if (blocksFound == 45) {
                    blocksFound++;//white lie, so we expect the right block contents
                }
                assertEquals(buffer.remaining(), 1024 * 1024 * 2);
                while (buffer.remaining() > 15) {
                    assertEquals(buffer.getLong(), blocksFound);
                    buffer.getLong();
                }
            } finally {
                blocksFound++;
                cont.discard();
            }
        }
        assertEquals(blocksFound, 96);
    }

    @Test
    public void testTruncator() throws Exception {
        System.out.println("Running testTruncator");
        assertNull(m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        for (int ii = 0; ii < 160; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        m_pbd.close();

        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR );

        TreeSet<String> listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 5);

        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            private long m_objectsParsed = 0;
            @Override
            public ByteBuffer parse(ByteBuffer b) {
                assertEquals(b.getLong(0), m_objectsParsed);
                assertEquals(b.remaining(), 1024 * 1024 * 2 );
                if (b.getLong(0) == 45) {
                    b.limit(b.remaining() / 2);
                    return b.slice();
                }
                while (b.remaining() > 15) {
                    assertEquals(b.getLong(), m_objectsParsed);
                    b.getLong();
                }
                m_objectsParsed++;
                return null;
            }

        });

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 2);

        for (int ii = 46; ii < 96; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        long blocksFound = 0;
        BBContainer cont = null;
        while ((cont = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
            try {
                ByteBuffer buffer = cont.b();
                if (blocksFound == 45) {
                    assertEquals(buffer.remaining(), 1024 * 1024);
                } else {
                    assertEquals(buffer.remaining(), 1024 * 1024 * 2);
                }
                while (buffer.remaining() > 15) {
                    assertEquals(buffer.getLong(), blocksFound);
                    buffer.getLong();
                }
            } finally {
                blocksFound++;
                cont.discard();
            }
        }
        assertEquals(blocksFound, 96);
    }

    @Test
    public void testOfferThanPoll() throws Exception {
        System.out.println("Running testOfferThanPoll");
        assertNull(m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        //Make sure a single file with the appropriate data is created
        m_pbd.offer(defaultContainer());
        File files[] = TEST_DIR.listFiles();
        assertEquals( 1, files.length);
        assertEquals( "pbd_nonce.0.pbd", files[0].getName());

        //Now make sure the current write file is stolen and a new write file created
        BBContainer retval = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        retval.discard();
    }

    @Test
    public void testOfferThenPushThenPoll() throws Exception {
        System.out.println("Running testOfferThenPushThenPoll");
        assertTrue(m_pbd.isEmpty());
        //Make it create two full segments
        for (int ii = 0; ii < 96; ii++) {
            m_pbd.offer(defaultContainer());
            assertFalse(m_pbd.isEmpty());
        }
        //Compression results in a weird magic number
        assertEquals( 1024 * 1024 * 2 * 96, m_pbd.sizeInBytes());
        File files[] = TEST_DIR.listFiles();
        assertEquals( 3, files.length);

        //Now create two buffers with different data to push at the front
        final ByteBuffer buffer1 = getFilledBuffer(16);
        final ByteBuffer buffer2 = getFilledBuffer(32);
        BBContainer pushContainers[] = new BBContainer[2];
        pushContainers[0] = DBBPool.dummyWrapBB(buffer1);
        pushContainers[1] = DBBPool.dummyWrapBB(buffer2);

        m_pbd.push(pushContainers);
        assertEquals( 1024 * 1024 * 2 * 98, m_pbd.sizeInBytes());

        //Expect this to create a single new file
        TreeSet<String> names = getSortedDirectoryListing();
        assertEquals( 4, names.size());
        assertTrue(names.first().equals("pbd_nonce.-1.pbd"));
        assertTrue(names.contains("pbd_nonce.0.pbd"));
        assertTrue(names.contains("pbd_nonce.1.pbd"));
        assertTrue(names.last().equals("pbd_nonce.2.pbd"));

        //Poll the two at the front and check that the contents are what is expected
        BBContainer retval1 = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        try {
            buffer1.clear();
            System.err.println(Long.toHexString(buffer1.getLong(0)) + " " + Long.toHexString(retval1.b().getLong(0)));
            assertEquals(retval1.b(), buffer1);
            assertEquals(1024 * 1024 * 2 * 97, m_pbd.sizeInBytes());

            BBContainer retval2 = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            try {
                buffer2.clear();
                assertEquals(retval2.b(), buffer2);
                assertEquals(1024 * 1024 * 2 * 96, m_pbd.sizeInBytes());


                //Expect the file for the two polled objects to still be there
                //until the discard
                names = getSortedDirectoryListing();
                assertEquals( 4, names.size());
            } finally {
                retval2.discard();
            }

        } finally {
            retval1.discard();
        }

        assertEquals(1024 * 1024 * 2 * 96, m_pbd.sizeInBytes());

        names = getSortedDirectoryListing();
        assertEquals( 3, names.size());
        assertTrue(names.first().equals("pbd_nonce.0.pbd"));

        ByteBuffer defaultBuffer = defaultBuffer();
        //Now poll the rest and make sure the data is correct
        for (int ii = 0; ii < 96; ii++) {
            defaultBuffer.clear();
            BBContainer retval = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            assertTrue(defaultBuffer.equals(retval.b()));
            retval.discard();
        }

        assertEquals( 0, m_pbd.sizeInBytes());
        assertTrue(m_pbd.isEmpty());

        //Expect just the current write segment
        names = getSortedDirectoryListing();
        assertEquals( 1, names.size());
        assertTrue(names.first().equals("pbd_nonce.2.pbd"));
    }

    @Test
    public void testOfferCloseThenReopen() throws Exception {
        System.out.println("Running testOfferCloseThenReopen");
        //Make it create two full segments
        for (int ii = 0; ii < 96; ii++) {
            m_pbd.offer(defaultContainer());
        }
        File files[] = TEST_DIR.listFiles();
        assertEquals( 3, files.length);

        m_pbd.sync();
        m_pbd.close();

        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR );

        ByteBuffer defaultBuffer = defaultBuffer();
        //Now poll all of it and make sure the data is correct
        for (int ii = 0; ii < 96; ii++) {
            defaultBuffer.clear();
            BBContainer retval = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            assertTrue(defaultBuffer.equals(retval.b()));
            retval.discard();
        }

        //Expect just the current write segment
        TreeSet<String> names = getSortedDirectoryListing();
        assertEquals( 1, names.size());
        assertTrue(names.first().equals("pbd_nonce.3.pbd"));
    }

    @Test
    public void testInvalidDirectory() throws Exception {
        System.out.println("Running testInvalidDirectory");
        m_pbd.close();
        try {
            m_pbd = new PersistentBinaryDeque( "foo", new File("/usr/bin"));
        } catch (IOException e) {
            return;
        }
        fail();
    }

    @Test
    public void testMissingSegment() throws Exception {
        System.out.println("Running testMissingSegment");
        for (int ii = 0; ii < 256; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(64)) );
        }
        m_pbd.close();
        File toDelete = new File(TEST_DIR + File.separator + TEST_NONCE + ".4.pbd");
        assertTrue(toDelete.exists());
        assertTrue(toDelete.delete());
        try {
            m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR);
        } catch (IOException e) {
            return;
        }
        fail();
    }

    @Test
    public void testOfferFailsWhenClosed() throws Exception {
        System.out.println("Running testOfferFailsWhenClosed");
        m_pbd.close();
        BBContainer cont = DBBPool.wrapBB(ByteBuffer.allocate(20));
        try {
            m_pbd.offer( cont );
        } catch (IOException e) {
            return;
        } finally {
            cont.discard();
        }
        fail();
    }

    @Test
    public void testPushFailsWhenClosed() throws Exception {
        System.out.println("Running testPushFailsWhenClosed");
        m_pbd.close();
        BBContainer objs[] = new BBContainer[] { DBBPool.wrapBB(ByteBuffer.allocate(20)) };
        try {
            m_pbd.push(objs);
        } catch (IOException e) {
            return;
        } finally {
            objs[0].discard();
        }
        fail();
    }

    @Test
    public void testPushMultipleSegments() throws Exception {
        System.out.println("Running testPushMultipleSegments");
        m_pbd.push(new BBContainer[] {
                DBBPool.wrapBB(ByteBuffer.allocateDirect(1024 * 1024 * 32)) ,
                DBBPool.wrapBB(ByteBuffer.allocateDirect(1024 * 1024 * 32)) ,
                DBBPool.wrapBB(ByteBuffer.allocateDirect(1024 * 1024 * 32)) });
    }

    @Test
    public void testPollWhileClosed() throws Exception {
        System.out.println("Running testPollWhileClosed");
        m_pbd.close();
        try {
            m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        } catch (IOException e) {
            return;
        }
        fail();
    }

    @Test
    public void testSyncWhileClosed() throws Exception {
        System.out.println("Running testSyncWhileClosed");
        m_pbd.close();
        try {
            m_pbd.sync();
        } catch (IOException e) {
            return;
        }
        fail();
    }

    @Test
    public void testIsEmptyWhileClosed() throws Exception {
        System.out.println("Running testIsEmptyWhileClosed");
        m_pbd.close();
        try {
            m_pbd.isEmpty();
        } catch (IOException e) {
            return;
        }
        fail();
    }

    @Test
    public void testPushMaxSize() throws Exception {
        System.out.println("Running testPushMaxSize");
        BBContainer objs[] = new BBContainer[] {
                DBBPool.wrapBB(ByteBuffer.allocateDirect(1024 * 1024 * 64)) };
        try {
            m_pbd.push(objs);
        } catch (IOException e) {
            return;
        } finally {
            objs[0].discard();
        }
        fail();
    }

    @Test
    public void testOfferMaxSize() throws Exception {
        System.out.println("Running testOfferMaxSize");
        BBContainer cont = DBBPool.wrapBB(ByteBuffer.allocateDirect(1024 * 1024 * 64));
        try {
            m_pbd.offer( cont );
        } catch (IOException e) {
            return;
        } finally {
            cont.discard();
        }
        fail();
    }

    @Test
    public void testOverlappingNonces() throws Exception {
        System.out.println("Running testOverlappingNonces");
        for (int i = 0; i < 20; i++) {
            PersistentBinaryDeque pbd = new PersistentBinaryDeque(Integer.toString(i), TEST_DIR);
            pbd.offer(defaultContainer());
            pbd.close();
        }

        PersistentBinaryDeque pbd = new PersistentBinaryDeque("1", TEST_DIR);
        pbd.close();
    }

    @Test
    public void testNonceWithDots() throws Exception {
        System.out.println("Running testNonceWithDots");
        PersistentBinaryDeque pbd = new PersistentBinaryDeque("ha.ha", TEST_DIR);
        pbd.offer(defaultContainer());
        pbd.close();

        pbd = new PersistentBinaryDeque("ha.ha", TEST_DIR);
        BBContainer bb = pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        try {
            ByteBuffer defaultBuffer = defaultBuffer();
            defaultBuffer.clear();
            assertEquals(defaultBuffer, bb.b());
            pbd.close();
        } finally {
            bb.discard();
        }
    }

    @Test
    public void testOfferCloseReopenOffer() throws Exception {
        System.out.println("Running testOfferCloseThenReopen");
        //Make it create two full segments
        for (int ii = 0; ii < 96; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }
        File files[] = TEST_DIR.listFiles();
        assertEquals(3, files.length);

        m_pbd.sync();
        m_pbd.close();

        m_pbd = new PersistentBinaryDeque(TEST_NONCE, TEST_DIR);
        int cnt = m_pbd.getNumObjects();
        assertEquals(cnt, 96);

        for (int ii = 96; ii < 192; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }
        m_pbd.sync();
        cnt = m_pbd.getNumObjects();
        assertEquals(cnt, 192);

        //Now poll all of it and make sure the data is correct
        for (int ii = 0; ii < 192; ii++) {
            ByteBuffer defaultBuffer = getFilledBuffer(ii);
            BBContainer retval = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            assertTrue(defaultBuffer.equals(retval.b()));
            retval.discard();
            defaultBuffer.clear();
        }

        //Expect just the current write segment
        TreeSet<String> names = getSortedDirectoryListing();
        assertEquals(1, names.size());
        assertTrue(names.first().equals("pbd_nonce.5.pbd"));
    }

    @Test
    public void testOfferCloseReopenOfferSmall() throws Exception {
        System.out.println("Running testOfferCloseReopenOfferSmall");
        final String SMALL_TEST_NONCE = "asmall_pbd_nonce";

        PersistentBinaryDeque small_pbd = new PersistentBinaryDeque(SMALL_TEST_NONCE, TEST_DIR);
        //Keep in 1 segment.
        for (int ii = 0; ii < 10; ii++) {
            small_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(ii)));
        }
        File files[] = TEST_DIR.listFiles();
        //We have the default pbd and new one.
        assertEquals(2, files.length);

        small_pbd.sync();
        small_pbd.close();
        System.gc();
        System.runFinalization();

        small_pbd = new PersistentBinaryDeque(SMALL_TEST_NONCE, TEST_DIR);
        int cnt = small_pbd.getNumObjects();
        assertEquals(cnt, 10);

        for (int ii = 10; ii < 20; ii++) {
            small_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(ii)));
        }
        small_pbd.sync();
        cnt = small_pbd.getNumObjects();
        assertEquals(cnt, 20);
        small_pbd.sync();
        small_pbd.close();
        small_pbd = null;
        System.gc();
        System.runFinalization();

        TreeSet<String> names = getSortedDirectoryListing();
        assertEquals(3, names.size());
        assertTrue(names.first().equals("asmall_pbd_nonce.0.pbd"));

        small_pbd = new PersistentBinaryDeque(SMALL_TEST_NONCE, TEST_DIR);
        //Now poll all of it and make sure the data is correct dont poll everything out.
        for (int ii = 0; ii < 10; ii++) {
            ByteBuffer defaultBuffer = getFilledSmallBuffer(ii);
            BBContainer retval = small_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            assertTrue(defaultBuffer.equals(retval.b()));
            retval.discard();
            defaultBuffer.clear();
        }
        small_pbd.sync();
        small_pbd.close();
        small_pbd = null;
        System.gc();
        System.runFinalization();
    }

    @Test
    public void testOfferCloseReopenOfferLeaveData() throws Exception {
        System.out.println("Running testOfferCloseHoleReopenOffer");
        //Make it create two full segments
        for (int ii = 0; ii < 96; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }
        File files[] = TEST_DIR.listFiles();
        assertEquals(3, files.length);
        m_pbd.sync();
        m_pbd.close();
        m_pbd = null;
        System.gc();
        System.runFinalization();
        m_pbd = new PersistentBinaryDeque(TEST_NONCE, TEST_DIR);
        int cnt = m_pbd.getNumObjects();
        assertEquals(cnt, 96);
        for (int ii = 96; ii < 192; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }
        m_pbd.sync();
        cnt = m_pbd.getNumObjects();
        assertEquals(cnt, 192);

        //Now poll half of it and make sure the data is correct
        for (int ii = 0; ii < 96; ii++) {
            ByteBuffer defaultBuffer = getFilledBuffer(ii);
            BBContainer retval = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            assertTrue(defaultBuffer.equals(retval.b()));
            retval.discard();
            defaultBuffer.clear();
        }
        m_pbd.sync();
        m_pbd.close();
        m_pbd = null;
        System.gc();
        System.runFinalization();

        //Expect just the current write segment
        TreeSet<String> names = getSortedDirectoryListing();
        assertEquals(3, names.size());
        assertTrue(names.first().equals("pbd_nonce.3.pbd"));

        //Reload
        m_pbd = new PersistentBinaryDeque(TEST_NONCE, TEST_DIR);
        cnt = m_pbd.getNumObjects();
        assertEquals(cnt, 96);
        //Expect just the current write segment hole should be deleted.
        names = getSortedDirectoryListing();
        assertEquals(4, names.size());
        assertTrue(names.first().equals("pbd_nonce.3.pbd"));

        for (int ii = 96; ii < 192; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }
        m_pbd.sync();
        cnt = m_pbd.getNumObjects();
        assertEquals(cnt, 192);
        //Now poll half of it and make sure the data is correct
        for (int ii = 96; ii < 192; ii++) {
            ByteBuffer defaultBuffer = getFilledBuffer(ii);
            BBContainer retval = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            assertTrue(defaultBuffer.equals(retval.b()));
            retval.discard();
            defaultBuffer.clear();
        }
        //Expect just the current write segment
        names = getSortedDirectoryListing();
        assertEquals(3, names.size());
        assertTrue(names.first().equals("pbd_nonce.6.pbd"));

        //Poll and leave one behind.
        for (int ii = 96; ii < 191; ii++) {
            ByteBuffer defaultBuffer = getFilledBuffer(ii);
            BBContainer retval = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            assertTrue(defaultBuffer.equals(retval.b()));
            retval.discard();
            defaultBuffer.clear();
        }
        //Expect just the current write segment
        names = getSortedDirectoryListing();
        assertEquals(1, names.size());
        assertTrue(names.first().equals("pbd_nonce.8.pbd"));

        //Push to get more segments at head
        BBContainer objs[] = new BBContainer[]{
            DBBPool.wrapBB(ByteBuffer.allocateDirect(1024 * 1024 * 32)),
            DBBPool.wrapBB(ByteBuffer.allocateDirect(1024 * 1024 * 32)),
            DBBPool.wrapBB(ByteBuffer.allocateDirect(1024 * 1024 * 32))};
        m_pbd.push(objs);
        names = getSortedDirectoryListing();
        assertEquals(4, names.size());
        assertTrue(names.first().equals("pbd_nonce.5.pbd"));

        BBContainer retval = m_pbd.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        retval.discard();
        names = getSortedDirectoryListing();
        assertEquals(3, names.size());
        assertTrue(names.first().equals("pbd_nonce.6.pbd"));

    }

    @Before
    public void setUp() throws Exception {
        if (TEST_DIR.exists()) {
            for (File f : TEST_DIR.listFiles()) {
                VoltFile.recursivelyDelete(f);
            }
            TEST_DIR.delete();
        }
        TEST_DIR.mkdir();
        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR );
    }

    @After
    public void tearDown() throws Exception {
        try {
            m_pbd.close();
        } catch (Exception e) {}
        try {
            if (TEST_DIR.exists()) {
                for (File f : TEST_DIR.listFiles()) {
                    f.delete();
                }
                TEST_DIR.delete();
            }
        } finally {
            m_pbd = null;
        }
        System.gc();
        System.runFinalization();
    }

}
