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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.BinaryDeque.BinaryDequeReader;
import org.voltdb.utils.BinaryDeque.BinaryDequeTruncator;
import org.voltdb.utils.BinaryDeque.TruncatorResponse;

import com.google_voltpatches.common.collect.Sets;

public class TestPersistentBinaryDeque {

    public final static File TEST_DIR = new File("/tmp/" + System.getProperty("user.name"));
    public static final String TEST_NONCE = "pbd_nonce";
    private static final String CURSOR_ID = "testPBD";
    private final static VoltLogger logger = new VoltLogger("EXPORT");

    private static ByteBuffer defaultBuffer() {
        return getFilledBuffer(42);
    }

    private static BBContainer defaultContainer() {
        return DBBPool.wrapBB(defaultBuffer());
    }

    private PersistentBinaryDeque m_pbd;

    public static ByteBuffer getFilledBuffer(long fillValue) {
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

    public static TreeSet<String> getSortedDirectoryListing() {
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

        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR, logger );

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 5);

        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            @Override
            public TruncatorResponse parse(BBContainer bbc) {
                return PersistentBinaryDeque.fullTruncateResponse();
            }

        });

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 1);
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));
    }

    @Test
    public void testCloseEmptyShouldNotDelete() throws Exception {
        System.out.println("Running testCloseEmptyShouldNotDelete");
        TreeSet<String> listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 1);
        m_pbd.close();

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 1);

        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR, logger );

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 1);

        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            @Override
            public TruncatorResponse parse(BBContainer bbc) {
                fail();
                return null;
            }

        });

        for (int ii = 0; ii < 96; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        for (long ii = 0; ii < 96; ii++) {
            BBContainer cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            try {
                assertNotNull(cont);
                assertEquals(cont.b().remaining(), 1024 * 1024 * 2);
                while (cont.b().remaining() > 15) {
                    assertEquals(ii, cont.b().getLong());
                    cont.b().getLong();
                }
            } catch (Throwable t) {
                System.err.println("Something threw");
                t.printStackTrace();
                throw t;
            } finally {
                cont.discard();
            }
        }
    }

    @Test
    public void testTruncatorWithFullTruncateReturn() throws Exception {
        System.out.println("Running testTruncatorWithFullTruncateReturn");
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        for (int ii = 0; ii < 150; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        m_pbd.close();

        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR, logger );

        TreeSet<String> listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 5);

        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            private long m_objectsParsed = 0;
            @Override
            public TruncatorResponse parse(BBContainer bbc) {
                ByteBuffer b = bbc.b();
                if (b.getLong(0) != m_objectsParsed) {
                    System.out.println("asd");
                }
                assertEquals(b.getLong(0), m_objectsParsed);
                assertEquals(b.remaining(), 1024 * 1024 * 2 );
                if (b.getLong(0) == 45) {
                    b.limit(b.remaining() / 2);
                    return PersistentBinaryDeque.fullTruncateResponse();
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

        reader = m_pbd.openForRead(CURSOR_ID);
        long actualSizeInBytes = 0;
        long reportedSizeInBytes = reader.sizeInBytes();
        long blocksFound = 0;
        BBContainer cont = null;
        while ((cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
            try {
                ByteBuffer buffer = cont.b();
                if (blocksFound == 45) {
                    blocksFound++;//white lie, so we expect the right block contents
                }
                assertEquals(buffer.remaining(), 1024 * 1024 * 2);
                actualSizeInBytes += buffer.remaining();
                while (buffer.remaining() > 15) {
                    assertEquals(buffer.getLong(), blocksFound);
                    buffer.getLong();
                }
            } finally {
                blocksFound++;
                cont.discard();
            }
        }
        assertEquals(actualSizeInBytes, reportedSizeInBytes);
        assertEquals(blocksFound, 96);
    }

    @Test
    public void testTruncator() throws Exception {
        System.out.println("Running testTruncator");
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        for (int ii = 0; ii < 160; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        m_pbd.close();

        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR, logger );

        TreeSet<String> listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 5);

        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            private long m_objectsParsed = 0;
            @Override
            public TruncatorResponse parse(BBContainer bbc) {
                ByteBuffer b = bbc.b();
                assertEquals(b.getLong(0), m_objectsParsed);
                assertEquals(b.remaining(), 1024 * 1024 * 2 );
                if (b.getLong(0) == 45) {
                    b.limit(b.remaining() / 2);
                    return new PersistentBinaryDeque.ByteBufferTruncatorResponse(b.slice());
                }
                while (b.remaining() > 15) {
                    assertEquals(b.getLong(), m_objectsParsed);
                    b.getLong();
                }
                m_objectsParsed++;
                return null;
            }

        });
        reader = m_pbd.openForRead(CURSOR_ID);
        assertEquals(95420416, reader.sizeInBytes());

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 2);

        for (int ii = 46; ii < 96; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        long actualSizeInBytes = 0;
        long reportedSizeInBytes = reader.sizeInBytes();
        long blocksFound = 0;
        BBContainer cont = null;
        while ((cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
            try {
                ByteBuffer buffer = cont.b();
                if (blocksFound == 45) {
                    assertEquals(buffer.remaining(), 1024 * 1024);
                } else {
                    assertEquals(buffer.remaining(), 1024 * 1024 * 2);
                }
                actualSizeInBytes += buffer.remaining();
                while (buffer.remaining() > 15) {
                    assertEquals(buffer.getLong(), blocksFound);
                    buffer.getLong();
                }
            } finally {
                blocksFound++;
                cont.discard();
            }
        }
        assertEquals(actualSizeInBytes, reportedSizeInBytes);
        assertEquals(blocksFound, 96);
    }

    @Test
    public void testReaderIsEmpty() throws Exception {
        System.out.println("Running testReaderIsEmpty");
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        assertTrue(reader.isEmpty());
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));
        assertTrue(reader.isEmpty());

        m_pbd.offer(defaultContainer());
        assertFalse(reader.isEmpty());

        BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        retval.discard();
        assertTrue(reader.isEmpty());

        // more than one segment
        for (int i = 0; i < 50; i++) {
            m_pbd.offer(defaultContainer());
        }
        assertFalse(reader.isEmpty());
        for (int i = 0; i < 50; i++) {
            retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            retval.discard();
            if (i<49) {
                assertFalse(reader.isEmpty());
            } else {
                assertTrue(reader.isEmpty());
            }
        }
    }

    @Test
    public void testReaderNumObjects() throws Exception {
        System.out.println("Running testReaderNumObjects");
        String cursor1 = "testPBD1";
        BinaryDequeReader reader1 = m_pbd.openForRead(cursor1);
        int count = 0;
        int totalAdded = 0;
        assertEquals(count, reader1.getNumObjects());
        assertNull(reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));
        assertEquals(count, reader1.getNumObjects());

        count++;
        m_pbd.offer(defaultContainer());
        totalAdded++;
        assertEquals(count, reader1.getNumObjects());

        // a second reader
        String cursor2 = "testPBD2";
        BinaryDequeReader reader2 = m_pbd.openForRead(cursor2);

        pollDiscard(reader1);
        assertEquals(count-1, reader1.getNumObjects());
        assertEquals(count, reader2.getNumObjects());
        pollDiscard(reader2);
        count--;
        assertEquals(count, reader1.getNumObjects());
        assertEquals(count, reader2.getNumObjects());

        // offer segments
        for (int i = 0; i < 50; i++) {
            m_pbd.offer(defaultContainer());
            totalAdded++;
            count++;
            assertEquals(count, reader1.getNumObjects());
            assertEquals(count, reader2.getNumObjects());
        }

        for (int i = 0; i < 50; i++) {
            pollDiscard(reader1);
            assertEquals(count-1, reader1.getNumObjects());
            assertEquals(count, reader2.getNumObjects());
            pollDiscard(reader2);
            count--;
            assertEquals(count, reader1.getNumObjects());
            assertEquals(count, reader2.getNumObjects());
        }

        final int segmentfullCount = 47;
        // start a 3rd reader after segments have been deleted
        String cursor3 = "testPBD3";
        BinaryDequeReader lateReader = m_pbd.openForRead(cursor3);
        int toAddForLate = totalAdded%segmentfullCount;
        assertEquals(count+toAddForLate, lateReader.getNumObjects());

        // offer segments with all 3 readers
        for (int i = 0; i < 50; i++) {
            m_pbd.offer(defaultContainer());
            count++;
            assertEquals(count, reader1.getNumObjects());
            assertEquals(count, reader2.getNumObjects());
            assertEquals(count+toAddForLate, lateReader.getNumObjects());
        }

        for (int i = 0; i < 50; i++) {
            pollDiscard(reader1);
            assertEquals(count-1, reader1.getNumObjects());
            assertEquals(count, reader2.getNumObjects());
            assertEquals(count+toAddForLate, lateReader.getNumObjects());
            pollDiscard(reader2);
            assertEquals(count-1, reader1.getNumObjects());
            assertEquals(count-1, reader2.getNumObjects());
            assertEquals(count+toAddForLate, lateReader.getNumObjects());
            pollDiscard(lateReader);
            count--;
            assertEquals(count, reader1.getNumObjects());
            assertEquals(count, reader2.getNumObjects());
            assertEquals(count+toAddForLate, lateReader.getNumObjects());
        }

        assert(count==0);
        for (int i=0; i < toAddForLate; i++) {
            pollDiscard(lateReader);
            assertEquals(toAddForLate-i-1, lateReader.getNumObjects());
        }

        assertEquals(0, reader1.getNumObjects());
        assertEquals(0, reader2.getNumObjects());
        assertEquals(0, lateReader.getNumObjects());
    }

    private void pollDiscard(BinaryDequeReader reader) throws IOException {
        BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        retval.discard();
    }

    @Test
    public void testOfferThenPoll() throws Exception {
        System.out.println("Running testOfferThanPoll");
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        //Make sure a single file with the appropriate data is created
        m_pbd.offer(defaultContainer());
        File files[] = TEST_DIR.listFiles();
        assertEquals( 1, files.length);
        assertTrue( "pbd_nonce.0.pbd".equals(files[0].getName()));

        //Now make sure the current write file is stolen and a new write file created
        BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        retval.discard();
    }

    @Test
    public void testCloseOldSegments() throws Exception {
        System.out.println("Running testCloseOldSegments");
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        final int total = 100;

        //Make sure several files with the appropriate data is created
        for (int i = 0; i < total; i++) {
            m_pbd.offer(defaultContainer());
        }
        File files[] = TEST_DIR.listFiles();
        assertEquals( 3, files.length);
        Set<String> actualFiles = Sets.newHashSet();
        for (File f : files) {
            actualFiles.add(f.getName());
        }
        Set<String> expectedFiles = Sets.newHashSet();
        for (int i = 0; i < 3; i++) {
            expectedFiles.add("pbd_nonce." + i + ".pbd");
        }
        Assert.assertEquals(expectedFiles, actualFiles);

        //Now make sure the current write file is stolen and a new write file created
        for (int i = 0; i < total; i++) {
            BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            retval.discard();
        }
    }

    @Test
    public void testDontCloseReadSegment() throws Exception {
        System.out.println("Running testOfferPollOfferMoreThanPoll");
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        final int total = 100;

        //Make sure a single file with the appropriate data is created
        for (int i = 0; i < 5; i++) {
            m_pbd.offer(defaultContainer());
        }
        assertEquals(1, TEST_DIR.listFiles().length);

        // Read one buffer from the segment so that it's considered being polled from.
        reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY).discard();

        for (int i = 5; i < total; i++) {
            m_pbd.offer(defaultContainer());
        }
        File files[] = TEST_DIR.listFiles();
        assertEquals( 3, files.length);
        Set<String> actualFiles = Sets.newHashSet();
        for (File f : files) {
            actualFiles.add(f.getName());
        }
        Set<String> expectedFiles = Sets.newHashSet();
        for (int i = 0; i < 3; i++) {
            expectedFiles.add("pbd_nonce." + i + ".pbd");
        }
        Assert.assertEquals(expectedFiles, actualFiles);

        //Now make sure the current write file is stolen and a new write file created
        for (int i = 1; i < total; i++) {
            BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            retval.discard();
        }
    }

    @Test
    public void testOfferThenPushThenPoll() throws Exception {
        System.out.println("Running testOfferThenPushThenPoll");
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        assertTrue(reader.isEmpty());
        //Make it create two full segments
        for (int ii = 0; ii < 96; ii++) {
            m_pbd.offer(defaultContainer());
            assertFalse(reader.isEmpty());
        }
        File files[] = TEST_DIR.listFiles();
        assertEquals( 3, files.length);

        //Now create two buffers with different data to push at the front
        final ByteBuffer buffer1 = getFilledBuffer(16);
        final ByteBuffer buffer2 = getFilledBuffer(32);
        BBContainer pushContainers[] = new BBContainer[2];
        pushContainers[0] = DBBPool.dummyWrapBB(buffer1);
        pushContainers[1] = DBBPool.dummyWrapBB(buffer2);

        m_pbd.push(pushContainers);

        //Expect this to create a single new file
        TreeSet<String> names = getSortedDirectoryListing();
        assertEquals( 4, names.size());
        assertTrue(names.first().equals("pbd_nonce.-1.pbd"));
        assertTrue(names.contains("pbd_nonce.0.pbd"));
        assertTrue(names.contains("pbd_nonce.1.pbd"));
        assertTrue(names.last().equals("pbd_nonce.2.pbd"));

        //Poll the two at the front and check that the contents are what is expected
        BBContainer retval1 = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        try {
            buffer1.clear();
            System.err.println(Long.toHexString(buffer1.getLong(0)) + " " + Long.toHexString(retval1.b().getLong(0)));
            assertEquals(retval1.b(), buffer1);

            BBContainer retval2 = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            try {
                buffer2.clear();
                assertEquals(retval2.b(), buffer2);

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

        names = getSortedDirectoryListing();
        assertEquals( 3, names.size());
        assertTrue(names.first().equals("pbd_nonce.0.pbd"));

        ByteBuffer defaultBuffer = defaultBuffer();
        //Now poll the rest and make sure the data is correct
        for (int ii = 0; ii < 96; ii++) {
            defaultBuffer.clear();
            BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            assertTrue(defaultBuffer.equals(retval.b()));
            retval.discard();
        }

        assertTrue(reader.isEmpty());

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

        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR, logger );
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);

        ByteBuffer defaultBuffer = defaultBuffer();
        //Now poll all of it and make sure the data is correct
        for (int ii = 0; ii < 96; ii++) {
            defaultBuffer.clear();
            BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
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
            m_pbd = new PersistentBinaryDeque( "foo", new File("/usr/bin"), logger);
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
            m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR, logger );
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
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        m_pbd.close();
        try {
            reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
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
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        m_pbd.close();
        try {
            reader.isEmpty();
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
            PersistentBinaryDeque pbd = new PersistentBinaryDeque(Integer.toString(i), TEST_DIR, logger);
            pbd.offer(defaultContainer());
            pbd.close();
        }

        PersistentBinaryDeque pbd = new PersistentBinaryDeque("1", TEST_DIR, logger);
        pbd.close();
    }

    @Test
    public void testNonceWithDots() throws Exception {
        System.out.println("Running testNonceWithDots");
        PersistentBinaryDeque pbd = new PersistentBinaryDeque("ha.ha", TEST_DIR, logger);
        pbd.offer(defaultContainer());
        pbd.close();

        pbd = new PersistentBinaryDeque("ha.ha", TEST_DIR, logger);
        BinaryDequeReader reader = pbd.openForRead(CURSOR_ID);
        BBContainer bb = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
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

        m_pbd = new PersistentBinaryDeque(TEST_NONCE, TEST_DIR, logger);
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        int cnt = reader.getNumObjects();
        assertEquals(cnt, 96);

        for (int ii = 96; ii < 192; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }
        m_pbd.sync();
        cnt = reader.getNumObjects();
        assertEquals(cnt, 192);

        //Now poll all of it and make sure the data is correct
        for (int ii = 0; ii < 192; ii++) {
            ByteBuffer defaultBuffer = getFilledBuffer(ii);
            BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            try {
                assertTrue(defaultBuffer.equals(retval.b()));
            } finally {
                retval.discard();
            }
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

        PersistentBinaryDeque small_pbd = new PersistentBinaryDeque(SMALL_TEST_NONCE, TEST_DIR, logger);
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

        small_pbd = new PersistentBinaryDeque(SMALL_TEST_NONCE, TEST_DIR, logger);
        BinaryDequeReader reader = small_pbd.openForRead(CURSOR_ID);
        int cnt = reader.getNumObjects();
        assertEquals(cnt, 10);

        for (int ii = 10; ii < 20; ii++) {
            small_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(ii)));
        }
        small_pbd.sync();
        cnt = reader.getNumObjects();
        assertEquals(cnt, 20);
        small_pbd.sync();
        small_pbd.close();
        small_pbd = null;
        System.gc();
        System.runFinalization();

        TreeSet<String> names = getSortedDirectoryListing();
        assertEquals(3, names.size());
        assertTrue(names.first().equals("asmall_pbd_nonce.0.pbd"));

        small_pbd = new PersistentBinaryDeque(SMALL_TEST_NONCE, TEST_DIR, logger);
        reader = small_pbd.openForRead(CURSOR_ID);
        //Now poll all of it and make sure the data is correct dont poll everything out.
        for (int ii = 0; ii < 10; ii++) {
            ByteBuffer defaultBuffer = getFilledSmallBuffer(ii);
            BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            try {
                assertTrue(defaultBuffer.equals(retval.b()));
            } finally {
                retval.discard();
            }
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
        m_pbd = new PersistentBinaryDeque(TEST_NONCE, TEST_DIR, logger);
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        int cnt = reader.getNumObjects();
        assertEquals(cnt, 96);
        for (int ii = 96; ii < 192; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }
        m_pbd.sync();
        cnt = reader.getNumObjects();
        assertEquals(cnt, 192);

        //Now poll half of it and make sure the data is correct
        for (int ii = 0; ii < 96; ii++) {
            ByteBuffer defaultBuffer = getFilledBuffer(ii);
            BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
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
        m_pbd = new PersistentBinaryDeque(TEST_NONCE, TEST_DIR, logger);
        reader = m_pbd.openForRead(CURSOR_ID);
        cnt = reader.getNumObjects();
        assertEquals(cnt, 96);
        //Expect just the current write segment hole should be deleted.
        names = getSortedDirectoryListing();
        assertEquals(4, names.size());
        assertTrue(names.first().equals("pbd_nonce.3.pbd"));

        for (int ii = 96; ii < 192; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }
        m_pbd.sync();
        cnt = reader.getNumObjects();
        assertEquals(cnt, 192);
        //Now poll half of it and make sure the data is correct
        for (int ii = 96; ii < 192; ii++) {
            ByteBuffer defaultBuffer = getFilledBuffer(ii);
            BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
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
            BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
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

        BBContainer retval = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        retval.discard();
        names = getSortedDirectoryListing();
        assertEquals(3, names.size());
        assertTrue(names.first().equals("pbd_nonce.6.pbd"));
    }

    @Test
    public void testDeleteOnNonEmptyNextSegment() throws Exception {
        System.out.println("Running testOfferPollOfferMoreThanPoll");
        BinaryDequeReader reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        final int total = 47;      // Number of buffers it takes to fill a segment

        //Make sure a single file with the appropriate data is created
        for (int i = 0; i < total; i++) {
            m_pbd.offer(defaultContainer());
        }
        assertEquals(1, TEST_DIR.listFiles().length);

        // Read read all the buffers from the segment (isEmpty() returns true)
        for (int i = 0; i < total; i++) {
            reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY).discard();
        }

        assert(reader.isEmpty());
        File files[] = TEST_DIR.listFiles();
        assertEquals(1, files.length);
        assert(files[0].getName().equals("pbd_nonce.0.pbd"));

        m_pbd.offer(defaultContainer());

        files = TEST_DIR.listFiles();
        // Make sure a new segment was created and the old segment was deleted
        assertEquals(1, files.length);
        assert(files[0].getName().equals("pbd_nonce.1.pbd"));
    }

    @Before
    public void setUp() throws Exception {
        setupTestDir();
        m_pbd = new PersistentBinaryDeque( TEST_NONCE, TEST_DIR, logger );
    }

    public static void setupTestDir() throws IOException {
        if (TEST_DIR.exists()) {
            for (File f : TEST_DIR.listFiles()) {
                VoltFile.recursivelyDelete(f);
            }
            TEST_DIR.delete();
        }
        TEST_DIR.mkdir();
    }

    @After
    public void tearDown() throws Exception {
        try {
            m_pbd.close();
        } catch (Exception e) {}
        try {
            tearDownTestDir();
        } finally {
            m_pbd = null;
        }
        System.gc();
        System.runFinalization();
    }

    public static void tearDownTestDir() {
        if (TEST_DIR.exists()) {
            for (File f : TEST_DIR.listFiles()) {
                f.delete();
            }
            TEST_DIR.delete();
        }
    }

}
