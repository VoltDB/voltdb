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
package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.test.utils.RandomTestRule;
import org.voltdb.utils.BinaryDeque.BinaryDequeTruncator;
import org.voltdb.utils.BinaryDeque.TruncatorResponse;
import org.voltdb.utils.BinaryDequeReader.SeekErrorRule;

import com.google_voltpatches.common.collect.Sets;
import org.apache.commons.io.FileUtils;

public class TestPersistentBinaryDeque {

    public final static File TEST_DIR = new File("/tmp/" + System.getProperty("user.name"));
    public static final String TEST_NONCE = "pbd_nonce";
    // Number of entries which fit in a segment. 64MB segment / (2MB entries + headers) = 31 entries per segment.
    static final int SEGMENT_FILL_COUNT = 31;
    private static final String CURSOR_ID = "testPBD";
    private final static VoltLogger logger = new VoltLogger("EXPORT");

    @Rule
    public final RandomTestRule m_random = new RandomTestRule();

    static ByteBuffer defaultBuffer() {
        return getFilledBuffer(42);
    }

    static BBContainer defaultContainer() {
        return DBBPool.wrapBB(defaultBuffer());
    }

    private PersistentBinaryDeque<ExtraHeaderMetadata> m_pbd;
    ExtraHeaderMetadata m_metadata;

    @Before
    public void setUp() throws Exception {
        setupTestDir();
        m_metadata = new ExtraHeaderMetadata(m_random);
        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(true)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
    }

    public static void setupTestDir() throws IOException {
        FileUtils.deleteDirectory(TEST_DIR);
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
            m_metadata = null;
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

    static ByteBuffer getFilledSmallBuffer(long fillValue) {
        ByteBuffer buf = ByteBuffer.allocateDirect(1024);
        while (buf.remaining() > 15) {
            buf.putLong(fillValue);
            buf.putLong(fillValue);
        }
        buf.clear();
        return buf;
    }

    /**
     * Return a "sorted" directory listing.
     *
     * Note: since the nonce is not passed in to filter out the output, this should only be
     * used for tests that use only 1 nonce.
     *
     * Since the files are not guaranteed to be sorted by name, decode the file sequence.
     *
     * @return
     * @throws IOException
     */
    public static List<File> getSortedDirectoryListing() throws IOException {
        return getSortedDirectoryListing(false);
    }

    public static List<File>  getSortedDirectoryListing(boolean isPBDClosed) throws IOException {

        TreeMap<Long, File> filesById = new TreeMap<>();

        for (File f : TEST_DIR.listFiles()) {
            String fname = f.getName();
            if (f.length()  ==0) {
                System.out.println("Ignoring empty file " + fname);
                continue;
            }
            PbdSegmentName segmentName = PbdSegmentName.parseFile(logger, f);
            assertEquals(PbdSegmentName.Result.OK, segmentName.m_result);

            filesById.put(segmentName.m_id, f);
        }
        if (filesById.size() == 0) {
            return Collections.<File> emptyList();
        }

        LinkedList<File> sorted = new LinkedList<>(filesById.values());
        // Verify the PBD segment finalization
        File lastEntry = sorted.peekLast() != null ? sorted.removeLast() : null;
        if (lastEntry != null) {
            if (isPBDClosed) {
                // When PBD is closed, last entry SHOULD be final
                assertTrue(PBDSegment.isFinal(lastEntry) || isMacOs());
            }
        }
        File penultimate = sorted.peekLast() != null ? sorted.removeLast() : null;
        if (penultimate != null) {
            if (isPBDClosed) {
                // When PBD is closed, penultimate entry SHOULD be final
                assertTrue(PBDSegment.isFinal(penultimate) || isMacOs());
            }
        }

        if (penultimate != null) {
            sorted.addLast(penultimate);
        }
        if (lastEntry != null) {
            sorted.addLast(lastEntry);
        }
        return sorted;
    }

    private static boolean isMacOs() {
        return System.getProperty("os.name").toLowerCase().contains("mac os");
    }

    @Test (timeout = 10_000)
    public void testRetentionThreads() {
        PersistentBinaryDeque.setupRetentionPolicyMgr(2, 1);
        assertEquals(2, PersistentBinaryDeque.getRetentionPolicyMgr().getRetentionThreadPoolSize());
        PersistentBinaryDeque.setupRetentionPolicyMgr(5, 1);
        assertEquals(5, PersistentBinaryDeque.getRetentionPolicyMgr().getRetentionThreadPoolSize());
    }

    @Test (timeout = 10_000)
    public void testFileNamesNoIds() throws Exception {
        testFileNames(false);
    }

    @Test (timeout = 10_000)
    public void testFileNamesWithIds() throws Exception {
        testFileNames(true);
    }

    private void testFileNames(boolean requiresId) throws Exception {
        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(true)
                .requiresId(requiresId)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
        // Make files and verify names
        int numFiles = 3;
        long startSeqNo = 1;
        for (int i=0; i<numFiles; i++) {
            if (requiresId) {
                m_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(i)), startSeqNo + i * 10, startSeqNo + (i + 1) * 10 - 1,
                        System.currentTimeMillis());
            } else {
                m_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(i)));
            }
            m_pbd.updateExtraHeader(null);
        }

        List<File> files = getSortedDirectoryListing();
        assertEquals(numFiles, files.size());
        for (int i=0; i<files.size(); i++) {
            PbdSegmentName name = PbdSegmentName.parseFile(logger, files.get(i));
            if (requiresId) {
                assertEquals(startSeqNo + i*10, name.m_id);
            } else {
                assertEquals(i+1, name.m_id);
            }
        }

        // close and open. Add 2 more
        m_pbd.close();
        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(true)
                .requiresId(requiresId)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
        for (int i=numFiles; i<numFiles+2; i++) {
            if (requiresId) {
                m_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(i)), startSeqNo + i * 10, startSeqNo + (i + 1) * 10 - 1,
                        System.currentTimeMillis());
            } else {
                m_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(i)));
            }
            m_pbd.updateExtraHeader(null);
        }
        numFiles += 2;
        files = getSortedDirectoryListing();
        assertEquals(numFiles, files.size());
        for (int i=0; i<files.size(); i++) {
            PbdSegmentName name = PbdSegmentName.parseFile(logger, files.get(i));
            if (requiresId) {
                assertEquals(startSeqNo + i*10, name.m_id);
            } else {
                assertEquals(i+1, name.m_id);
            }
        }
    }

    @Test
    public void testFileSizeDiff() throws Exception {
        List<File> files = getSortedDirectoryListing();
        assert(files.size() == 0);
        long size = 0;
        Random random = new Random(System.currentTimeMillis());
        for (int i=0; i<5; i++) {
            int offered = 0;
            if (random.nextBoolean()) {
                offered = m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(i)) );
            } else {
                offered = m_pbd.offer( DBBPool.wrapBB(getFilledSmallBuffer(i)));
            }
            if (i == 0) {
                files = getSortedDirectoryListing();
            }
            long newSize = files.get(0).length();
            if (i==0) {
                assertTrue(newSize > offered + PBDSegment.ENTRY_HEADER_BYTES);
            } else {
                assertEquals(offered + PBDSegment.ENTRY_HEADER_BYTES, newSize - size);
            }
            size = newSize;
        }
    }

    @Test
    public void testReopenReader() throws Exception {
        System.out.println("Running testReopenReader");
        int count = 10;
        for (int ii = 0; ii < count; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        assert(count >= 2);
        // Read 2 and close and reopen
        BBContainer cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        cont.discard();
        cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        cont.discard();
        m_pbd.closeCursor(CURSOR_ID);
        reader = m_pbd.openForRead(CURSOR_ID);
        int readCount = 0;
        while((cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
            cont.discard();
            readCount++;
        }
        assertEquals(count, readCount);
    }

    @Test
    public void testReopenReaderMultiSegment() throws Exception {
        System.out.println("Running testReopenReaderMultiSegment");

        // ENG-18635: verify that last cursor closing does not purge PBD segments
        commonReopenReaderMultiSegment(150, 150, false);
    }

    @Test
    public void testReopenReaderMultiSegmentDR() throws Exception {
        System.out.println("Running testReopenReaderMultiSegment");

        // ENG-18635: verify that last cursor closing does purge PBD segments for DR
        commonReopenReaderMultiSegment(150, 9, true);
    }

    private void commonReopenReaderMultiSegment(int initialCount, int finalCount, boolean purgeOnLastCursor)
            throws IOException {
        List<File> listing = getSortedDirectoryListing();
        assertEquals(0, listing.size());

        for (int ii = 0; ii < initialCount; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 4);

        // Open and close a read cursor
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        m_pbd.closeCursor(CURSOR_ID, purgeOnLastCursor);

        // Verify behavior on last closing cursor
        reader = m_pbd.openForRead(CURSOR_ID);
        int readCount = 0;
        BBContainer cont;
        while((cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
            cont.discard();
            readCount++;
        }
        assertEquals(readCount, finalCount);
    }

    @Test
    public void testTruncateFirstElement() throws Exception {
        System.out.println("Running testTruncateFirstElement");
        List<File> listing = getSortedDirectoryListing();
        assertEquals(0, listing.size());

        for (int ii = 0; ii < 150; ii++) {
            m_pbd.offer( DBBPool.wrapBB(getFilledBuffer(ii)) );
        }

        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 4);

        m_pbd.close();

        // Get directory listing with PBD closed
        listing = getSortedDirectoryListing(true);
        assertEquals(listing.size(), 4);

        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger)
                .initialExtraHeader(m_metadata, SERIALIZER).build();

        listing = getSortedDirectoryListing();
        assertEquals(4, listing.size());

        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            @Override
            public TruncatorResponse parse(BBContainer bbc) {
                return PersistentBinaryDeque.fullTruncateResponse();
            }

        });

        listing = getSortedDirectoryListing();
        assertEquals(0, listing.size());
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));
    }

    @Test
    public void testCloseEmptyShouldNotDelete() throws Exception {
        System.out.println("Running testCloseEmptyShouldNotDelete");
        List<File> listing = getSortedDirectoryListing();
        assertEquals(0, listing.size());
        m_pbd.close();

        // Test directlry listing on closed PBD
        listing = getSortedDirectoryListing(true);
        assertEquals(0, listing.size());

        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(true)
                .initialExtraHeader(m_metadata, SERIALIZER).build();

        listing = getSortedDirectoryListing();
        assertEquals(0, listing.size());

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

        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
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
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        for (int ii = 0; ii < 150; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }

        m_pbd.close();

        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(true)
                .initialExtraHeader(m_metadata, SERIALIZER).build();

        List<File> listing = getSortedDirectoryListing();
        assertEquals(4, listing.size());

        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            private long m_objectsParsed = 0;
            @Override
            public TruncatorResponse parse(BBContainer bbc) {
                ByteBuffer b = bbc.b();
                if (b.getLong(0) != m_objectsParsed) {
                    System.out.println("asd");
                }
                assertEquals(m_objectsParsed, b.getLong(0));
                assertEquals(1024 * 1024 * 2, b.remaining());
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
        assertEquals(1, listing.size());

        for (int ii = 46; ii < 96; ii++) {
            // Note: new segment after truncate?
            if (ii == 46) {
                m_pbd.updateExtraHeader(new ExtraHeaderMetadata(m_random));
            }
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }

        reader = m_pbd.openForRead(CURSOR_ID);
        long actualSizeInBytes = 0;
        long reportedSizeInBytes = reader.sizeInBytes();
        long blocksFound = 0;
        BinaryDequeReader.Entry<ExtraHeaderMetadata> entry = null;
        while ((entry = pollOnceWithoutDiscard(reader)) != null) {
            try {
                ByteBuffer buffer = entry.getData();
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
                entry.release();
            }
        }
        assertEquals(actualSizeInBytes, reportedSizeInBytes);
        assertEquals(blocksFound, 96);
    }

    @Test
    public void testTruncator() throws Exception {
        System.out.println("Running testTruncator");
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        for (int ii = 0; ii < 160; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }

        m_pbd.close();

        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
        ;

        List<File> listing = getSortedDirectoryListing();
        assertEquals(4, listing.size());

        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            private long m_objectsParsed = 0;
            @Override
            public TruncatorResponse parse(BBContainer bbc) {
                ByteBuffer b = bbc.b();
                assertEquals(m_objectsParsed, b.getLong(0));
                assertEquals(1024 * 1024 * 2, b.remaining());
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
        assertEquals(1, listing.size());

        for (int ii = 46; ii < 96; ii++) {
            // Note: new segment after truncate?
            if (ii == 46) {
                m_pbd.updateExtraHeader(new ExtraHeaderMetadata(m_random));
            }
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }

        long actualSizeInBytes = 0;
        long reportedSizeInBytes = reader.sizeInBytes();
        long blocksFound = 0;
        BinaryDequeReader.Entry<ExtraHeaderMetadata> entry = null;
        while ((entry = pollOnceWithoutDiscard(reader)) != null) {
            try {
                ByteBuffer buffer = entry.getData();
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
                entry.release();
            }
        }
        assertEquals(actualSizeInBytes, reportedSizeInBytes);
        assertEquals(blocksFound, 96);
    }

    @Test
    public void testReaderIsEmpty() throws Exception {
        System.out.println("Running testReaderIsEmpty");
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        assertTrue(reader.isEmpty());
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));
        assertTrue(reader.isEmpty());

        m_pbd.offer(defaultContainer());
        assertFalse(reader.isEmpty());
        pollOnce(reader);
        assertTrue(reader.isEmpty());

        // more than one segment
        for (int i = 0; i < 50; i++) {
            m_pbd.offer(defaultContainer());
        }
        assertFalse(reader.isEmpty());
        for (int i = 0; i < 50; i++) {
            pollOnce(reader);
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
        BinaryDequeReader<ExtraHeaderMetadata> reader1 = m_pbd.openForRead(cursor1);
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
        BinaryDequeReader<ExtraHeaderMetadata> reader2 = m_pbd.openForRead(cursor2);

        pollOnce(reader1);
        assertEquals(count-1, reader1.getNumObjects());
        assertEquals(count, reader2.getNumObjects());
        pollOnce(reader2);
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
            pollOnce(reader1);
            assertEquals(count-1, reader1.getNumObjects());
            assertEquals(count, reader2.getNumObjects());
            pollOnce(reader2);
            count--;
            assertEquals(count, reader1.getNumObjects());
            assertEquals(count, reader2.getNumObjects());
        }

        final int segmentfullCount = 47;
        // start a 3rd reader after segments have been deleted
        String cursor3 = "testPBD3";
        BinaryDequeReader<ExtraHeaderMetadata> lateReader = m_pbd.openForRead(cursor3);
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
            pollOnce(reader1);
            assertEquals(count-1, reader1.getNumObjects());
            assertEquals(count, reader2.getNumObjects());
            assertEquals(count+toAddForLate, lateReader.getNumObjects());
            pollOnce(reader2);
            assertEquals(count-1, reader1.getNumObjects());
            assertEquals(count-1, reader2.getNumObjects());
            assertEquals(count+toAddForLate, lateReader.getNumObjects());
            pollOnce(lateReader);
            count--;
            assertEquals(count, reader1.getNumObjects());
            assertEquals(count, reader2.getNumObjects());
            assertEquals(count+toAddForLate, lateReader.getNumObjects());
        }

        assert(count==0);
        for (int i=0; i < toAddForLate; i++) {
            pollOnce(lateReader);
            assertEquals(toAddForLate-i-1, lateReader.getNumObjects());
        }

        assertEquals(0, reader1.getNumObjects());
        assertEquals(0, reader2.getNumObjects());
        assertEquals(0, lateReader.getNumObjects());
    }

    @Test
    public void testOfferThenPoll() throws Exception {
        System.out.println("Running testOfferThenPoll");
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        //Make sure a single file with the appropriate data is created
        m_pbd.offer(defaultContainer());
        File files[] = TEST_DIR.listFiles();
        assertEquals( 1, files.length);
        assertTrue(createSegmentName(1).equals(files[0].getName()));

        //Now make sure the current write file is stolen and a new write file created
        pollOnce(reader);
    }

    @Test
    public void testCloseOldSegments() throws Exception {
        System.out.println("Running testCloseOldSegments");
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        final int total = 100;

        //Make sure several files with the appropriate data is created
        for (int i = 0; i < total; i++) {
            m_pbd.offer(defaultContainer());
        }
        List<File> listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 3);

        Set<String> actualFiles = Sets.newHashSet();
        for (File f : listing) {
            actualFiles.add(f.getName());
        }
        Set<String> expectedFiles = Sets.newHashSet();
        expectedFiles.add(createSegmentName(1));
        expectedFiles.add(createSegmentName(2));
        expectedFiles.add(createSegmentName(3));
        assertEquals(expectedFiles, actualFiles);

        //Now make sure the current write file is stolen and a new write file created
        for (int i = 0; i < total; i++) {
            pollOnce(reader);
        }
        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 1);
    }

    @Test
    public void testDontCloseReadSegment() throws Exception {
        System.out.println("Running testDontCloseReadSegment");
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        assertNull(reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));

        final int total = 100;

        //Make sure a single file with the appropriate data is created
        for (int i = 0; i < 5; i++) {
            m_pbd.offer(defaultContainer());
        }
        assertEquals(1, TEST_DIR.listFiles().length);

        // Read one buffer from the segment so that it's considered being polled from.
        pollOnce(reader);

        for (int i = 5; i < total; i++) {
            m_pbd.offer(defaultContainer());
        }
        List<File> listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 3);

        Set<String> actualFiles = Sets.newHashSet();
        for (File f : listing) {
            actualFiles.add(f.getName());
        }
        Set<String> expectedFiles = Sets.newHashSet();
        expectedFiles.add(createSegmentName(1));
        expectedFiles.add(createSegmentName(2));
        expectedFiles.add(createSegmentName(3));
        assertEquals(expectedFiles, actualFiles);

        //Now make sure the current write file is stolen and a new write file created
        for (int i = 1; i < total; i++) {
            pollOnce(reader);
        }
        listing = getSortedDirectoryListing();
        assertEquals(listing.size(), 1);
    }

    @Test
    public void testOfferThenPushThenPoll() throws Exception {
        System.out.println("Running testOfferThenPushThenPoll");
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
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

        m_pbd.push(pushContainers, m_metadata);

        //Expect this to create a single new file
        List<File> listing = getSortedDirectoryListing();
        assertEquals(4, listing.size());

        // Check the expected ordering of the PBDs
        File f0 = listing.remove(0);
        assertEquals(createSegmentName(0), f0.getName());
        f0 = listing.remove(0);
        assertEquals(createSegmentName(1), f0.getName());
        f0 = listing.remove(0);
        assertEquals(createSegmentName(2), f0.getName());
        f0 = listing.remove(0);
        assertEquals(createSegmentName(3), f0.getName());

        //Poll the two at the front and check that the contents are what is expected
        buffer1.clear();
        pollOnceAndVerify(reader, buffer1);
        buffer2.clear();
        pollOnceAndVerify(reader, buffer2);

        listing = getSortedDirectoryListing();
        assertEquals(4, listing.size());

        ByteBuffer defaultBuffer = defaultBuffer();
        pollOnceAndVerify(reader, defaultBuffer);
        listing = getSortedDirectoryListing();
        assertEquals(3, listing.size());

        f0 = listing.remove(0);
        assertEquals(createSegmentName(1), f0.getName());
        f0 = listing.remove(0);
        assertEquals(createSegmentName(2), f0.getName());
        f0 = listing.remove(0);
        assertEquals(createSegmentName(3), f0.getName());

        //Now poll the rest and make sure the data is correct
        for (int ii = 0; ii < 95; ii++) {
            defaultBuffer.clear();
            pollOnceAndVerify(reader, defaultBuffer);
        }

        assertTrue(reader.isEmpty());

        //Expect just the current write segment
        listing = getSortedDirectoryListing();
        assertEquals( 1, listing.size());
        f0 = listing.remove(0);
        assertEquals(createSegmentName(3), f0.getName());
    }

    @Test
    public void testOfferCloseThenReopen() throws Exception {
        System.out.println("Running testOfferCloseThenReopen");
        //Make it create two full segments
        for (int ii = 0; ii < 96; ii++) {
            m_pbd.offer(defaultContainer());
        }
        File files[] = TEST_DIR.listFiles();
        assertEquals(3, files.length);

        m_pbd.sync();
        m_pbd.close();

        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
        ;
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);

        ByteBuffer defaultBuffer = defaultBuffer();
        //Now poll all of it and make sure the data is correct
        for (int ii = 0; ii < 96; ii++) {
            defaultBuffer.clear();
            pollOnceAndVerify(reader, defaultBuffer);
        }

        // Expect the current write segment and previous
        List<File> listing = getSortedDirectoryListing();
        assertEquals(1, listing.size());

        m_pbd.offer(defaultContainer());
        defaultBuffer.clear();
        pollOnceAndVerify(reader, defaultBuffer);

        // Expect just the current write segment
        listing = getSortedDirectoryListing();
        assertEquals(1, listing.size());
    }

    @Test
    public void testInvalidDirectory() throws Exception {
        System.out.println("Running testInvalidDirectory");
        m_pbd.close();
        try {
            m_pbd = PersistentBinaryDeque.builder("foo", new File("/usr/bin"), logger)
                    .initialExtraHeader(m_metadata, SERIALIZER).build();
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

        List<File> listing = getSortedDirectoryListing(true);
        assertEquals(listing.size(), 6);

        // Delete the fourth file
        File toDelete = null;
        for (int i = 0; i < 4; i++) {
            toDelete = listing.remove(0);
        }
        assertTrue(toDelete.exists());
        assertTrue(toDelete.delete());
        try {
            m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger)
                    .initialExtraHeader(m_metadata, SERIALIZER).build();
            ;
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
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
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
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        m_pbd.close();
        try {
            reader.isEmpty();
        } catch (IOException e) {
            return;
        }
        fail();
    }

    @Test
    public void testDeferredDiscards() throws Exception {
        // Set up deferred discarder
        Timer timer = new Timer();
        DeferredDiscarder discarder = new DeferredDiscarder();
        long timerPeriod = 500;
        timer.schedule(discarder, timerPeriod, timerPeriod);
        m_pbd.registerDeferredDeleter(discarder);

        // Insert and read buffers. Verify that the discards happen by verifying the segment count
        int numSegments = 5;
        int numBuffers = 25;
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            for (int j=0; j<numBuffers; j++) {
                m_pbd.offer(DBBPool.wrapBB(ByteBuffer.allocate(100)));
            }
        }

        assertEquals(numSegments, getSortedDirectoryListing().size());

        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead("testreader");
        for (int i=0; i<numSegments; i++) {
            for (int j=0; j<numBuffers; j++) {
                reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY).discard();
                if (i>0 && j==0) { // deletes happen on the first discard on the next segment.
                    Thread.sleep(timerPeriod*2); // sleep double the time to make sure the timer task is executed.
                    assertEquals(numSegments-i, getSortedDirectoryListing().size());
                }
            }
        }

        assertEquals(1, getSortedDirectoryListing().size());
        assertNull(discarder.m_error);
        discarder.cancel();
    }

    @Test
    public void testDeferredDiscardsAfterClose() throws Exception {
        // Set up deferred discarder
        Timer timer = new Timer();
        DeferredDiscarder discarder = new DeferredDiscarder();
        long timerPeriod = 1000;
        timer.schedule(discarder, timerPeriod, timerPeriod);
        m_pbd.registerDeferredDeleter(discarder);

        // Insert and read buffers. Verify that the discards happen by verifying the segment count
        int numSegments = 5;
        int numBuffers = 25;
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            for (int j=0; j<numBuffers; j++) {
                m_pbd.offer(DBBPool.wrapBB(ByteBuffer.allocate(100)));
            }
        }

        assertEquals(numSegments, getSortedDirectoryListing().size());

        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead("testreader");
        for (int i=0; i<numSegments*numBuffers; i++) {
            reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY).discard();
        }

        m_pbd.closeCursor("testreader");
        Thread.sleep(timerPeriod + 500); // wait for timer to execute
        assertNull(discarder.m_error);
        discarder.cancel();
    }

    class DeferredDiscarder extends TimerTask implements Executor {

        ConcurrentLinkedQueue<Runnable> m_actions = new ConcurrentLinkedQueue<>();
        Throwable m_error;

        @Override
        public void execute(Runnable runnable) {
            m_actions.add(runnable);
        }

        @Override
        public void run() {
            List<Runnable> copy = new ArrayList<>();
            int size = m_actions.size();
            for (int i=0; i<size; i++) {
                copy.add(m_actions.poll());
            }

            for (Runnable runnable : copy) {
                try {
                    runnable.run();
                } catch(Throwable t) {
                    m_error = t;
                }
            }
        }
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
        }
        fail();
    }

    @Test
    public void testOverlappingNonces() throws Exception {
        System.out.println("Running testOverlappingNonces");
        for (int i = 0; i < 20; i++) {
            PersistentBinaryDeque<ExtraHeaderMetadata> pbd = PersistentBinaryDeque
                    .builder(
                    Integer.toString(i), TEST_DIR, logger)
                    .initialExtraHeader(m_metadata, SERIALIZER).build();
            pbd.offer(defaultContainer());
            pbd.close();
        }

        PersistentBinaryDeque<ExtraHeaderMetadata> pbd = PersistentBinaryDeque.builder("1", TEST_DIR, logger)
                .initialExtraHeader(m_metadata, SERIALIZER)
                .build();
        pbd.close();
    }

    @Test
    public void testNonceWithDots() throws Exception {
        System.out.println("Running testNonceWithDots");
        PersistentBinaryDeque<ExtraHeaderMetadata> pbd = PersistentBinaryDeque.builder("ha.ha", TEST_DIR, logger)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
        pbd.offer(defaultContainer());
        pbd.close();

        pbd = PersistentBinaryDeque.builder("ha.ha", TEST_DIR, logger).initialExtraHeader(null, SERIALIZER).build();
        BinaryDequeReader<ExtraHeaderMetadata> reader = pbd.openForRead(CURSOR_ID);
        ByteBuffer defaultBuffer = defaultBuffer();
        defaultBuffer.clear();
        pollOnceAndVerify(reader, defaultBuffer);
        pbd.close();
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

        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        long cnt = reader.getNumObjects();
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
            pollOnceAndVerify(reader, defaultBuffer);
            defaultBuffer.clear();
        }

        //Expect just the current write segment
        List<File> listing = getSortedDirectoryListing();
        assertEquals(1, listing.size());
    }

    @Test
    public void testOfferCloseReopenOfferSmall() throws Exception {
        System.out.println("Running testOfferCloseReopenOfferSmall");
        final String SMALL_TEST_NONCE = "asmall_pbd_nonce";

        PersistentBinaryDeque<ExtraHeaderMetadata> small_pbd = PersistentBinaryDeque
                .builder(SMALL_TEST_NONCE, TEST_DIR, logger)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
        //Keep in 1 segment.
        for (int ii = 0; ii < 10; ii++) {
            small_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(ii)));
        }
        File files[] = TEST_DIR.listFiles();
        //We have the default pbd and new one.
        assertEquals(1, files.length);

        small_pbd.sync();
        small_pbd.close();
        System.gc();
        System.runFinalization();

        small_pbd = PersistentBinaryDeque.builder(SMALL_TEST_NONCE, TEST_DIR, logger)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
        BinaryDequeReader<ExtraHeaderMetadata> reader = small_pbd.openForRead(CURSOR_ID);
        long cnt = reader.getNumObjects();
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

        files = TEST_DIR.listFiles();
        assertEquals(2, files.length);

        small_pbd = PersistentBinaryDeque.builder(SMALL_TEST_NONCE, TEST_DIR, logger)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
        reader = small_pbd.openForRead(CURSOR_ID);
        //Now poll all of it and make sure the data is correct dont poll everything out.
        for (int ii = 0; ii < 10; ii++) {
            ByteBuffer defaultBuffer = getFilledSmallBuffer(ii);
            pollOnceAndVerify(reader, defaultBuffer);
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
        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(true)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        long cnt = reader.getNumObjects();
        assertEquals(cnt, 96);
        for (int ii = 96; ii < 192; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }
        m_pbd.sync();
        cnt = reader.getNumObjects();
        assertEquals(cnt, 192);

        //Now poll half of it and make sure the data is correct
        for (int ii = 0; ii < 96; ii++) {
            pollOnceAndVerify(reader, getFilledBuffer(ii));
        }
        pollOnceAndVerify(reader, getFilledBuffer(96));
        m_pbd.sync();
        m_pbd.close();
        m_pbd = null;
        System.gc();
        System.runFinalization();

        //Expect just the current write segment
        List<File> listing = getSortedDirectoryListing(true);
        assertEquals(3, listing.size());
        //Reload
        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(true)
                .initialExtraHeader(m_metadata, SERIALIZER).build();
        reader = m_pbd.openForRead(CURSOR_ID);
        cnt = reader.getNumObjects();
        assertEquals(cnt, 96);
        //Expect just the current write segment hole should be deleted.
        listing = getSortedDirectoryListing();
        assertEquals(3, listing.size());

        for (int ii = 96; ii < 192; ii++) {
            m_pbd.offer(DBBPool.wrapBB(getFilledBuffer(ii)));
        }
        m_pbd.sync();
        cnt = reader.getNumObjects();
        assertEquals(cnt, 192);
        //Now poll half of it and make sure the data is correct
        for (int ii = 96; ii < 192; ii++) {
            ByteBuffer defaultBuffer = getFilledBuffer(ii);
            pollOnceAndVerify(reader, defaultBuffer);
            defaultBuffer.clear();
        }

        pollOnceAndVerify(reader, getFilledBuffer(96));

        //Expect just the current write segment
        listing = getSortedDirectoryListing();
        assertEquals(3, listing.size());

        //Poll and leave one behind.
        for (int ii = 97; ii < 191; ii++) {
            ByteBuffer defaultBuffer = getFilledBuffer(ii);
            pollOnceAndVerify(reader, defaultBuffer);
            defaultBuffer.clear();
        }
        assertEquals(1, reader.getNumObjects());
        //Expect just the current write segment
        listing = getSortedDirectoryListing();
        assertEquals(1, listing.size());

        //Push to get more segments at head
        BBContainer objs[] = new BBContainer[]{
            DBBPool.wrapBB(ByteBuffer.allocateDirect(1024 * 1024 * 32)),
            DBBPool.wrapBB(ByteBuffer.allocateDirect(1024 * 1024 * 32)),
            DBBPool.wrapBB(ByteBuffer.allocateDirect(1024 * 1024 * 32))};
        m_pbd.push(objs);
        listing = getSortedDirectoryListing();
        assertEquals(4, listing.size());

        for (int i = 0; i < 4; ++i) {
            pollOnce(reader);
            assertEquals(4 - (i + 1), reader.getNumObjects());
            listing = getSortedDirectoryListing();
            assertEquals(4 - i, listing.size());
        }
    }

    @Test
    public void testUpdateExtraHeader() throws Exception {
        List<ExtraHeaderMetadata> extraHeaders = new ArrayList<>();
        extraHeaders.add(m_metadata);
        for (int i = 0; i < 5; ++i) {
            for (int j = 0; j < 2; j++) {
                m_pbd.offer(defaultContainer());
            }
            assertEquals(i + 1, TEST_DIR.listFiles().length);

            ExtraHeaderMetadata ehm = new ExtraHeaderMetadata(m_random);
            extraHeaders.add(ehm);
            m_pbd.updateExtraHeader(ehm);
        }
        assertEquals(5, TEST_DIR.listFiles().length);
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        // Open second reader to prevent segments from being deleted
        m_pbd.openForRead(CURSOR_ID + 1);

        ByteBuffer expectedData = defaultBuffer();
        for (int i = 0; i< 5 ;++i) {
            ExtraHeaderMetadata expectedMetadata = extraHeaders.get(i);
            for (int j = 0; j < 2; j++) {
                pollOnceAndVerify(reader, expectedData.asReadOnlyBuffer(), expectedMetadata);
            }
        }

        m_pbd.sync();
        m_pbd.close();
        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(true)
                .initialExtraHeader(m_metadata, SERIALIZER).build();

        reader = m_pbd.openForRead(CURSOR_ID);
        for (int i = 0; i < 5; ++i) {
            ExtraHeaderMetadata expectedMetadata = extraHeaders.get(i);
            for (int j = 0; j < 2; j++) {
                pollOnceAndVerify(reader, expectedData.asReadOnlyBuffer(), expectedMetadata);
            }
        }
    }

    @Test
    public void testCloseLastReader() throws Exception {
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        pollOnceAndVerify(reader, null);
        m_pbd.closeCursor(CURSOR_ID);
        m_pbd.offer(defaultContainer());
    }

    @Test
    public void testSegmentClosingWriterOnly() throws Exception {
        // Initially no readers and nothing written. Open segments must be 0
        assertEquals(0, m_pbd.numOpenSegments());

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < SEGMENT_FILL_COUNT; j++) {
                m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(j)));
            }
            assertEquals(1, m_pbd.numOpenSegments());
        }
    }

    @Test
    public void testSegmentClosingWriterReaderLockStep() throws Exception {
        assertEquals(0, m_pbd.numOpenSegments());
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead("reader0");

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < SEGMENT_FILL_COUNT; j++) {
                m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(j)));
                BBContainer bbC = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
                bbC.discard();
            }
            assertEquals(1, m_pbd.numOpenSegments());
        }
    }

    @Test
    public void testOutOfOrderAcks() throws Exception {
        int numEntries = 2;
        // write 2 segments
        for (int i=0; i<2; i++) {
            if (i>0) {
                m_pbd.updateExtraHeader(null);
            }
            for (int j=0; j<numEntries; j++) {
                m_pbd.offer( DBBPool.wrapBB(getFilledSmallBuffer(0)));
            }
        }
        assertEquals(2, getSortedDirectoryListing().size());

        // read all entries.
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead("testreader");
        BBContainer cont = null;
        int index=0;
        BBContainer[] entries = new BBContainer[2*numEntries];
        while ((cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
            entries[index++] = cont;
        }
        assertEquals(2, getSortedDirectoryListing().size());

        // Ack backwards
        index = entries.length-1;
        for (int i=0; i<numEntries; i++) {
            entries[index--].discard();
        }
        assertEquals(2, getSortedDirectoryListing().size());
        for (int i=0; i<numEntries; i++) {
            entries[index--].discard();
        }
        assertEquals(1, getSortedDirectoryListing().size());
    }

    @Test(timeout = 10_000)
    public void testDiscardAfterSkipPast() throws Exception {
        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(true)
                .requiresId(true)
                .initialExtraHeader(m_metadata, SERIALIZER).build();

        long seqNo = 1;
        int numSegments = 3;
        for (int i=0; i<numSegments; i++) {
            if (i>0) {
                m_pbd.updateExtraHeader(null);
            }
            for (int j=0; j<3; j++) {
                long startSeqNo = seqNo + i*3*10 + j*10;
                m_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(0)), startSeqNo, startSeqNo + 9,
                        System.currentTimeMillis());
            }
        }
        long lastSeqNo = numSegments*3*10;
        assertEquals(numSegments, getSortedDirectoryListing().size());

        // read an entry, mark rest read and discarded, then discard the read entry
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead("testreader");
        BBContainer cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        reader.skipPast(lastSeqNo);
        assertEquals(numSegments, getSortedDirectoryListing().size());

        cont.discard();
        assertEquals(1, getSortedDirectoryListing().size());
    }

    /*
     * Test that seeking backwards on a reader works when there are buffers which are not discarded
     */
    @Test(timeout = 2_000)
    public void seekBackwardWithOutstandingBuffers() throws Exception {
        m_pbd.close();
        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(false).requiresId(true)
                .initialExtraHeader(m_metadata, SERIALIZER).build();

        int numEntries = 2;
        // write 4 segments
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < numEntries; j++) {
                long entry = i * numEntries + j;
                m_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(0)), entry, entry, System.currentTimeMillis());
            }
            m_pbd.updateExtraHeader(null);
        }

        assertEquals(4, getSortedDirectoryListing().size());

        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead("testreader");
        ArrayDeque<BinaryDequeReader.Entry<ExtraHeaderMetadata>> entries = new ArrayDeque<>();
        // poll the first 3 segments + 1
        for (int i = 0; i < numEntries * 3 + 1; ++i) {
            entries.add(reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));
        }

        // Free the last 2 segment of entries. Leaving 3 outstanding buffers
        for (int i = 0; i < numEntries * 2; ++i) {
            entries.pollLast().free();
        }

        // seek to 3rd segment
        reader.seekToSegment(numEntries * 2, SeekErrorRule.THROW);
        assertEquals(4, getSortedDirectoryListing().size());

        // release remaining polled entries which should delete the first segment
        for (int i = 0; i < numEntries +1; ++i) {
            entries.poll().release();
        }

        assertTrue(entries.isEmpty());
        assertEquals(3, getSortedDirectoryListing().size());

        // poll and discard the first entry of the segment after the seek should delete another segment
        reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY).discard();
        assertEquals(2, getSortedDirectoryListing().size());
    }

    /*
     * Test that Reader can seek forward with outstanding buffers
     */
    @Test(timeout = 2_000)
    public void seekForwardWithOutstandingBuffers() throws Exception {
        m_pbd.close();
        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(false).requiresId(true)
                .initialExtraHeader(m_metadata, SERIALIZER).build();

        int numEntries = 2;
        // write 4 segments
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < numEntries; j++) {
                long entry = i * numEntries + j;
                m_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(0)), entry, entry, System.currentTimeMillis());
            }
            m_pbd.updateExtraHeader(null);
        }

        assertEquals(4, getSortedDirectoryListing().size());

        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead("testreader");
        ArrayDeque<BinaryDequeReader.Entry<ExtraHeaderMetadata>> entries = new ArrayDeque<>();
        // poll the first segment + 1
        for (int i = 0; i < numEntries + 1; ++i) {
            entries.add(reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY));
        }

        // Seek to third segment
        reader.seekToSegment(numEntries * 2, SeekErrorRule.THROW);
        assertEquals(4, getSortedDirectoryListing().size());

        // release all previously polled segments this should delete the first one
        for (int i = 0; i < numEntries + 1; ++i) {
            entries.poll().release();
        }
        assertEquals(4, getSortedDirectoryListing().size());

        // poll and discard the first entry of the segment after the seek should delete another segment
        reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY).discard();
        assertEquals(2, getSortedDirectoryListing().size());
    }

    /*
     * Test that a cursor can skip multiple times and be closed and reopened
     */
    @Test
    public void reopenCursorAfterSkip() throws Exception {
        m_pbd.close();
        m_pbd = PersistentBinaryDeque.builder(TEST_NONCE, TEST_DIR, logger).compression(false).requiresId(true)
                .initialExtraHeader(m_metadata, SERIALIZER).build();

        int numEntries = 2;
        // write 4 segments
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < numEntries; j++) {
                long entry = i * numEntries + j;
                m_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(0)), entry, entry, System.currentTimeMillis());
            }
            m_pbd.updateExtraHeader(null);
        }

        String cursorId = "testreader";
        BinaryDequeReader<ExtraHeaderMetadata> cursor = m_pbd.openForRead(cursorId);

        // Open a cursor and skip to the third segment
        BBContainer container = cursor.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        assertEquals(1, openSegmentReaderCount(cursorId));
        cursor.skipPast(numEntries * 2);
        assertEquals(1, openSegmentReaderCount(cursorId));
        cursor.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY).discard();
        assertEquals(2, openSegmentReaderCount(cursorId));
        assertFalse(cursor.isEmpty());
        container.discard();
        assertEquals(1, openSegmentReaderCount(cursorId));

        // Close and reopen the cursor and skip again
        m_pbd.closeCursor(cursorId);
        assertEquals(0, openSegmentReaderCount(cursorId));
        cursor = m_pbd.openForRead(cursorId);
        assertEquals(0, openSegmentReaderCount(cursorId));
        cursor.skipPast(numEntries * 3);
        assertEquals(0, openSegmentReaderCount(cursorId));
        assertFalse(cursor.isEmpty());
    }

    /*
     * Test that when a transient reader is closed any segments prior to any other reader are not closed as well.
     */
    @Test
    public void closingTransientReaderDoesNotDelete() throws IOException {
        BinaryDequeReader<ExtraHeaderMetadata> reader1 = m_pbd.openForRead(CURSOR_ID + 1, true);
        BinaryDequeReader<ExtraHeaderMetadata> reader2 = m_pbd.openForRead(CURSOR_ID + 2, true);

        final int total = 100;

        // Make sure several files with the appropriate data is created
        for (int i = 0; i < total; i++) {
            m_pbd.offer(defaultContainer());
        }

        assertEquals(3, getSortedDirectoryListing().size());

        // Move reader one to the end of the PBD
        BBContainer bbc;
        while ((bbc = reader1.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
            bbc.discard();
        }

        // Close the cursor still at the begning and make sure no segments are deleted
        m_pbd.closeCursor(CURSOR_ID + 1);

        assertEquals(3, getSortedDirectoryListing().size());
    }

    @Test
    public void testSegmentTTL() throws IOException, InterruptedException {
        int segmentCount = m_pbd.numberOfSegments();
        segmentCount = segmentCount == 0 ? 1 : segmentCount;
        m_pbd.setSegmentRollTimeLimit(TimeUnit.SECONDS.toNanos(2));
        // write 1 segment
        for (int i = 0; i < 10; i++) {
            m_pbd.offer(defaultContainer());
        }
        assertEquals(segmentCount, m_pbd.numberOfSegments());

        Thread.sleep(2000);

        // after 4 seconds, a new segment should be added.
        for (int i = 0; i < 10; i++) {
            m_pbd.offer(defaultContainer());
        }
        segmentCount++;
        assertEquals(segmentCount, m_pbd.numberOfSegments());
        // update max segment open time
        m_pbd.setSegmentRollTimeLimit(TimeUnit.SECONDS.toNanos(4));
        Thread.sleep(2000);
        for (int i = 0; i < 10; i++) {
            m_pbd.offer(defaultContainer());
        }
        // No new segment should be created
        assertEquals(segmentCount, m_pbd.numberOfSegments());

        Thread.sleep(3000);
        for (int i = 0; i < 10; i++) {
            m_pbd.offer(defaultContainer());
        }
        // Another segment should be added
        segmentCount++;
        assertEquals(segmentCount, m_pbd.numberOfSegments());
    }

    /*
     * Test that when poll entry is provided with a max size and an entry exceeds that value null is returned and if the
     * entry does not exceed that value it is returned
     */
    @Test
    public void pollMaxSize() throws Exception {
        m_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(798464565L)));
        m_pbd.offer(defaultContainer());
        m_pbd.offer(DBBPool.wrapBB(getFilledSmallBuffer(-798464565L)));
        m_pbd.offer(defaultContainer());

        BinaryDequeReader<?> reader = m_pbd.openForRead(CURSOR_ID);

        BinaryDequeReader.Entry<?> entry = reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY,
                1 * 1024 * 1024);
        assertNotNull(entry);
        entry.release();

        // Second entry should be too big to poll at 1MB max
        entry = reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY, 1 * 1024 * 1024);
        assertNull(entry);

        // Should now be able to poll at 2MB
        entry = reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY, 2 * 1024 * 1024);
        assertNotNull(entry);
        entry.release();

        // Go really small for third entry
        entry = reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY, 512);
        assertNull(entry);

        // Can be polled at exact size
        entry = reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY, 1024);
        assertNotNull(entry);
        entry.release();

        // Can poll last large entry
        entry = reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY, 4 * 1024 * 1024);
        assertNotNull(entry);
        entry.release();

        // No more entries in the PBD
        entry = reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        assertNull(entry);

        m_pbd.closeCursor(CURSOR_ID);
    }

    private int openSegmentReaderCount(String cursorId) {
        return m_pbd.getSegments().values().stream().mapToInt(r -> r.getReader(cursorId) == null ? 0 : 1).sum();
    }

    static <M> BinaryDequeReader.Entry<M> pollOnceWithoutDiscard(BinaryDequeReader<M> reader) throws IOException {
        BinaryDequeReader.Entry<M> entry = reader
                .pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        if (entry != null) {
            try {
                assertNotNull(entry.getExtraHeader());
            } catch (Throwable t) {
                entry.release();
                throw t;
            }
        }
        return entry;
    }

    static void pollOnce(BinaryDequeReader<ExtraHeaderMetadata> reader) throws IOException {
        BinaryDequeReader.Entry<ExtraHeaderMetadata> retval = pollOnceWithoutDiscard(reader);
        if (retval != null) {
            retval.release();
        }
    }

    void pollOnceAndVerify(BinaryDequeReader<ExtraHeaderMetadata> reader, ByteBuffer destBuf) throws IOException {
        pollOnceAndVerify(reader, destBuf, m_metadata);
    }

    static <M> void pollOnceAndVerify(BinaryDequeReader<M> reader, ByteBuffer expectedData, M expectedMetadata)
            throws IOException {
        BinaryDequeReader.Entry<M> retval = pollOnceWithoutDiscard(reader);
        try {
            if (expectedData == null) {
                assertNull(retval);
            } else {
                assertEquals(expectedMetadata, retval.getExtraHeader());
                assertEquals(expectedData, retval.getData());
            }
        } finally {
            if (retval != null) {
                retval.release();
            }
        }
    }

    private static String createSegmentName(long id) {
        return PbdSegmentName.createName(TEST_NONCE, id, false);
    }

    static class ExtraHeaderMetadata {
        final int m_int;
        final long m_long;
        final byte[] m_bytes;

        ExtraHeaderMetadata(Random random) {
            m_int = random.nextInt();
            m_long = random.nextLong();
            m_bytes = new byte[random.nextInt(256)];
            random.nextBytes(m_bytes);
        }

        ExtraHeaderMetadata(ByteBuffer buffer) {
            m_int = buffer.getInt();
            m_long = buffer.getLong();
            m_bytes = new byte[buffer.getInt()];
            buffer.get(m_bytes);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(m_bytes);
            result = prime * result + m_int;
            result = prime * result + (int) (m_long ^ (m_long >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ExtraHeaderMetadata other = (ExtraHeaderMetadata) obj;
            return m_int == other.m_int && m_long == other.m_long && Arrays.equals(m_bytes, other.m_bytes);
        }
    }

    static final BinaryDequeSerializer<ExtraHeaderMetadata> SERIALIZER = new BinaryDequeSerializer<ExtraHeaderMetadata>() {
        @Override
        public int getMaxSize(ExtraHeaderMetadata object) throws IOException {
            return Integer.BYTES + Long.BYTES + Integer.BYTES + object.m_bytes.length;
        }

        @Override
        public void write(ExtraHeaderMetadata object, ByteBuffer buffer) throws IOException {
            buffer.putInt(object.m_int);
            buffer.putLong(object.m_long);
            buffer.putInt(object.m_bytes.length);
            buffer.put(object.m_bytes);
        }

        @Override
        public ExtraHeaderMetadata read(ByteBuffer buffer) throws IOException {
            return new ExtraHeaderMetadata(buffer);
        }
    };
}
