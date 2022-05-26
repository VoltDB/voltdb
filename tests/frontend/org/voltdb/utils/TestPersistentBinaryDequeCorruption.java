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
import static org.voltdb.utils.TestPersistentBinaryDeque.defaultBuffer;
import static org.voltdb.utils.TestPersistentBinaryDeque.defaultContainer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.test.utils.RandomTestRule;
import org.voltdb.utils.TestPersistentBinaryDeque.ExtraHeaderMetadata;

import com.google_voltpatches.common.collect.ImmutableList;

/**
 * Test handling of different types of PBD corruption using the two detection mechanisms
 * {@link PersistentBinaryDeque#parseAndTruncate(org.voltdb.utils.BinaryDeque.BinaryDequeTruncator)} and
 * {@link PersistentBinaryDeque#scanEntries(org.voltdb.utils.BinaryDeque.BinaryDequeScanner)}
 */
@RunWith(Parameterized.class)
public class TestPersistentBinaryDequeCorruption {
    private static final VoltLogger LOG = new VoltLogger("TEST");
    private static final String TEST_NONCE = "pbd_nonce";
    private static final String CURSOR_ID = "TestPersistentBinaryDequeCorruption";

    private static int ENTRY_COUNT = 10;
    private static int ENTRY_FIRST = 1;
    private static int ENTRY_MIDDLE = ENTRY_COUNT / 2;
    private static int ENTRY_LAST = ENTRY_COUNT;

    @Rule
    public final TemporaryFolder testDir = new TemporaryFolder();

    @Rule
    public final RandomTestRule random = new RandomTestRule();

    private final Map<String, Long> m_corruptionPoints;
    private final CorruptionChecker m_checker;
    private PersistentBinaryDeque<ExtraHeaderMetadata> m_pbd;
    private ExtraHeaderMetadata m_extraHeader;

    @Parameters
    public static Collection<Object[]> parameters() {
        CorruptionChecker scanEntries = pbd -> pbd.scanEntries(b -> { return -1; });
        CorruptionChecker parseAndTruncate = pbd -> pbd.parseAndTruncate(b -> null);

        ImmutableList.Builder<Object[]> args = ImmutableList.builder();
        for (CorruptionChecker cc : new CorruptionChecker[] {scanEntries, parseAndTruncate}) {
            for (Boolean throwIoException : new Boolean[] {false, true}) {
                args.add(new Object[] { cc, throwIoException });
            }
        }
        return args.build();
    }

    public TestPersistentBinaryDequeCorruption(CorruptionChecker checker, boolean throwIoException) {
        m_checker = checker;
        m_corruptionPoints = throwIoException ? new HashMap<>() : null;
    }

    @Before
    public void setup() throws IOException {
        m_extraHeader = new ExtraHeaderMetadata(random);
        m_pbd = newPbd();
    }

    @After
    public void tearDown() throws IOException {
        if (m_pbd != null) {
            m_pbd.close();
        }
    }

    /**
     * Test handling of a corrupt entry at start of segment
     */
    @Test
    public void testCorruptedEntryFirst() throws Exception {
        testCorruptedEntry(ENTRY_FIRST);
    }

    /**
     * Test handling of a corrupt entry in the middle of the segment
     */
    @Test
    public void testCorruptedEntryMiddle() throws Exception {
        testCorruptedEntry(ENTRY_MIDDLE);
    }

    /**
     * Test handling of a corrupt entry at the end of the segment
     */
    @Test
    public void testCorruptedEntryLast() throws Exception {
        testCorruptedEntry(ENTRY_LAST);
    }

    /**
     * Test handling of a corrupt entry length when the length of the first entry is shorter
     */
    @Test
    public void testCorruptedEntryLengthShorterFirstEntry() throws Exception {
        testCorruptedLength(ENTRY_FIRST, -100, false);
    }

    /**
     * Test handling of a corrupt entry length when the length of the first entry is longer than the remainder of the
     * file
     */
    @Test
    public void testCorruptedEntryLengthLongerFirstEntry() throws Exception {
        testCorruptedLength(ENTRY_FIRST, 100, false);
    }

    /**
     * Test handling of a corrupt entry length when the length of the first entry is negative
     */
    @Test
    public void testCorruptedEntryLengthNegativeFirstEntry() throws Exception {
        testCorruptedLength(ENTRY_FIRST, -100, true);
    }

    /**
     * Test handling of a corrupt entry length when the length of the first entry is way too big
     */
    @Test
    public void testCorruptedEntryLengthTooLongFirstEntry() throws Exception {
        testCorruptedLength(ENTRY_FIRST, Integer.MAX_VALUE / 2, true);
    }

    /**
     * Test handling of a corrupt entry length when the length of the middle entry is shorter
     */
    @Test
    public void testCorruptedEntryLengthShorterMiddleEntry() throws Exception {
        testCorruptedLength(ENTRY_MIDDLE, -100, false);
    }

    /**
     * Test handling of a corrupt entry length when the length of the middle entry is longer than the remainder of the
     * file
     */
    @Test
    public void testCorruptedEntryLengthLongerMiddleEntry() throws Exception {
        testCorruptedLength(ENTRY_MIDDLE, 100, false);
    }

    /**
     * Test handling of a corrupt entry length when the length of the middle entry is negative
     */
    @Test
    public void testCorruptedEntryLengthNegativeMiddleEntry() throws Exception {
        testCorruptedLength(ENTRY_MIDDLE, -100, true);
    }

    /**
     * Test handling of a corrupt entry length when the length of the middle entry is way too big
     */
    @Test
    public void testCorruptedEntryLengthTooLongMiddleEntry() throws Exception {
        testCorruptedLength(ENTRY_MIDDLE, Integer.MAX_VALUE / 2, true);
    }

    /**
     * Test handling of a corrupt entry length when the length of the last entry is shorter
     */
    @Test
    public void testCorruptedEntryLengthShorterLastEntry() throws Exception {
        testCorruptedLength(ENTRY_LAST, -100, false);
    }

    /**
     * Test handling of a corrupt entry length when the length of the last entry is longer than the remainder of the
     * file
     */
    @Test
    public void testCorruptedEntryLengthLongerLastEntry() throws Exception {
        testCorruptedLength(ENTRY_LAST, 100, false);
    }

    /**
     * Test handling of a corrupt entry length when the length of the last entry is negative
     */
    @Test
    public void testCorruptedEntryLengthNegativeLastEntry() throws Exception {
        testCorruptedLength(ENTRY_LAST, -100, true);
    }

    /**
     * Test handling of a corrupt entry length when the length of the last entry is way too big
     */
    @Test
    public void testCorruptedEntryLengthTooLongLastEntry() throws Exception {
        testCorruptedLength(ENTRY_LAST, Integer.MAX_VALUE / 2, true);
    }

    /**
     * Test handling of a corrupt segment header
     */
    @Test
    public void testCorruptSegmentHeader() throws Exception {
        m_pbd.offer(defaultContainer());
        ByteBuffer bb = ByteBuffer.allocateDirect(Integer.BYTES);
        bb.putInt(100);
        bb.flip();
        corruptLastSegment(bb, PBDSegment.HEADER_NUM_OF_ENTRY_OFFSET);

        verifySegmentCount(1, 0);
        runCheckerNewPbd(ENTRY_FIRST);
        verifySegmentCount(1, 0);
    }

    /**
     * Test handling of a corrupt segment extra header data
     */
    @Test
    public void testCorruptExtraHeader() throws Exception {
        m_pbd.offer(defaultContainer());
        ByteBuffer bb = ByteBuffer.allocateDirect(40);
        corruptLastSegment(bb, PBDSegment.HEADER_EXTRA_HEADER_OFFSET + 15);

        verifySegmentCount(1, 0);
        runCheckerNewPbd(ENTRY_FIRST);
        verifySegmentCount(1, 0);
    }

    /**
     * Test that quarantined files are cleaned up when they are passed by all readers
     */
    @Test
    public void testQuarantinedFileDeletedWhenPassed() throws Exception {
        ByteBuffer data = defaultBuffer();
        for (int i = 0; i < 5; ++i) {
            for (int j = 0; j < 5; ++j) {
                m_pbd.offer(defaultContainer());
            }
            m_pbd.updateExtraHeader(m_extraHeader);
        }
        assertEquals(5, testDir.getRoot().list().length);

        int i = 0;
        for (PBDSegment<?> segment : getSegmentMap().values()) {
            switch (i++) {
            case 1:
                corruptSegment(segment, ByteBuffer.allocateDirect(10), PBDSegment.HEADER_NUM_OF_ENTRY_OFFSET);
                break;
            case 3:
                corruptSegment(segment, ByteBuffer.allocateDirect(10), PBDSegment.HEADER_EXTRA_HEADER_OFFSET + 20);
                break;
            }
        }

        verifySegmentCount(5, 0);

        m_checker.run(m_pbd);
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead(CURSOR_ID);
        BinaryDequeReader<ExtraHeaderMetadata> reader2 = m_pbd.openForRead(CURSOR_ID + 2);

        verifySegmentCount(5, 2);

        for (i = 0; i < 15; ++i) {
            pollOnceAndVerify(reader2, data);
        }
        pollOnceAndVerify(reader2, null);

        for (i = 0; i < 3; ++i) {
            for (int j = 0; j < 5; ++j) {
                pollOnceAndVerify(reader, data);
                if (j == 0) {
                    assertEquals(5 - (i * 2), testDir.getRoot().list().length);
                }
            }
        }
        pollOnceAndVerify(reader, null);

        verifySegmentCount(1, 0);
        m_pbd.offer(defaultContainer());
        pollOnceAndVerify(reader, data);
        verifySegmentCount(2, 0);
        pollOnceAndVerify(reader2, data);
        verifySegmentCount(1, 0);
    }

    @Test
    public void testFileShorterThanExpectedFirst() throws Exception {
        m_pbd.offer(defaultContainer());
        File segment = getSegmentMap().lastEntry().getValue().file();
        m_pbd.close();
        try (FileChannel channel = FileChannel.open(segment.toPath(), StandardOpenOption.WRITE)) {
            channel.truncate(channel.size() - 45);
        }
        runCheckerNewPbd(ENTRY_FIRST);
    }

    private int putInitialEntries() throws IOException {
        int entryLength = -1;
        for (int i = 0; i < ENTRY_COUNT; ++i) {
            BBContainer container = defaultContainer();
            entryLength = container.b().remaining();
            m_pbd.offer(container);
        }
        return entryLength;
    }

    private int startOfEntry(int entryNumber, int entrySize) throws IOException {
        return PBDSegment.SEGMENT_HEADER_BYTES + TestPersistentBinaryDeque.SERIALIZER.getMaxSize(m_extraHeader)
                + ((entryNumber - 1) * (entrySize + PBDSegment.ENTRY_HEADER_BYTES));
    }

    private void testCorruptedLength(int entryToCorrupt, int lengthModifier, boolean absolute) throws Exception {
        int origLength = putInitialEntries();

        ByteBuffer bb = ByteBuffer.allocateDirect(Integer.BYTES);
        bb.putInt(absolute ? lengthModifier : origLength + lengthModifier);
        bb.flip();
        corruptLastSegment(bb,
                startOfEntry(entryToCorrupt, origLength) + PBDSegment.ENTRY_HEADER_TOTAL_BYTES_OFFSET);

        runCheckerNewPbd(entryToCorrupt);
    }

    private void testCorruptedEntry(int entryToCorrupt) throws Exception {
        int entryLength = putInitialEntries();

        corruptLastSegment(ByteBuffer.allocateDirect(35), startOfEntry(entryToCorrupt, entryLength) + 35);

        runCheckerNewPbd(entryToCorrupt);
    }

    private void verifySegmentCount(int segmentCount, int expectedQuarantinedCount) {
        int quarantinedCount = 0;
        File[] entries = testDir.getRoot().listFiles();
        assertEquals(Arrays.toString(entries), segmentCount, entries.length);
        for (File entry : entries) {
            if (PbdSegmentName.parseFile(null, entry).m_quarantined) {
                ++quarantinedCount;
            }
        }

        assertEquals(Arrays.toString(entries), expectedQuarantinedCount, quarantinedCount);
    }

    private void runCheckerNewPbd(int corruptedEntry) throws IOException {
        // Use a parallel PBD instance since PBD finalizes all entries on close and that is not wanted
        verifySegmentCount(1, 0);
        PersistentBinaryDeque<ExtraHeaderMetadata> pbd = newPbd();
        BinaryDequeReader<ExtraHeaderMetadata> reader = pbd.openForRead(CURSOR_ID);
        try {
            ByteBuffer data = defaultBuffer();
            m_checker.run(pbd);
            verifySegmentCount(1, corruptedEntry == ENTRY_FIRST ? 1 : 0);
            for (int i = 0; i < corruptedEntry - 1; ++i) {
                pollOnceAndVerify(reader, data);
            }
            pollOnceAndVerify(reader, null);
            verifySegmentCount(1, corruptedEntry == ENTRY_FIRST ? 1 : 0);
            pbd.offer(defaultContainer());
            pollOnceAndVerify(reader, data);
            verifySegmentCount(1, 0);
        } finally {
            pbd.close();
        }
    }

    private void corruptLastSegment(ByteBuffer corruptData, int position) throws Exception {
        corruptSegment(getSegmentMap().lastEntry().getValue(), corruptData, position);
    }

    private void corruptSegment(PBDSegment<?> segment, ByteBuffer corruptData, int position) throws IOException {
        File file = segment.file();
        if (m_corruptionPoints == null) {
            try (FileChannel channel = FileChannel.open(Paths.get(file.getPath()), StandardOpenOption.WRITE)) {
                channel.write(corruptData, position);
            }
        } else {
            m_corruptionPoints.put(file.getName(), Long.valueOf(position));
        }
    }

    @SuppressWarnings("unchecked")
    private NavigableMap<Long, PBDSegment<?>> getSegmentMap() throws IllegalArgumentException, IllegalAccessException {
        return ((NavigableMap<Long, PBDSegment<?>>) FieldUtils
                .getDeclaredField(PersistentBinaryDeque.class, "m_segments", true).get(m_pbd));
    }

    private PersistentBinaryDeque<ExtraHeaderMetadata> newPbd() throws IOException {
        PersistentBinaryDeque.Builder<ExtraHeaderMetadata> builder = PersistentBinaryDeque
                .builder(TEST_NONCE, testDir.getRoot(), LOG)
                .initialExtraHeader(m_extraHeader, TestPersistentBinaryDeque.SERIALIZER);

        if (m_corruptionPoints != null) {
            builder.pbdSegmentFactory(CorruptingPBDSegment::new);
        }

        return builder.build();
    }

    private void pollOnceAndVerify(BinaryDequeReader<ExtraHeaderMetadata> reader, ByteBuffer expectedData)
            throws IOException {
        TestPersistentBinaryDeque.pollOnceAndVerify(reader, expectedData, m_extraHeader);
    }

    private interface CorruptionChecker {
        void run(PersistentBinaryDeque<?> pbd) throws IOException;
    }

    private class CorruptingPBDSegment<M> extends PBDRegularSegment<M> {
        CorruptingPBDSegment(long id, File file, VoltLogger usageSpecificLog,
                BinaryDequeSerializer<M> extraHeaderSerializer) {
            super(id, file, usageSpecificLog, extraHeaderSerializer);
        }

        @Override
        FileChannelWrapper openFile(File file, boolean forWrite) throws IOException {
            return new PBDRegularSegment.FileChannelWrapper(file, forWrite) {
                // Only need to track position changes from read since position is set before any read
                private long m_position = 0;

                @Override
                public int read(ByteBuffer dst) throws IOException {
                    throwIfCorrupt(m_position, dst.remaining());
                    int read = super.read(dst);
                    if (read > 0) {
                        m_position += read;
                    }
                    return read;
                }

                @Override
                public int read(ByteBuffer dst, long position) throws IOException {
                    throwIfCorrupt(position, dst.remaining());
                    return super.read(dst, position);
                }

                @Override
                public FileChannel position(long newPosition) throws IOException {
                    super.position(newPosition);
                    m_position = newPosition;
                    return this;
                }

                private void throwIfCorrupt(long position, int length) throws IOException {
                    Long corruptionPoint = m_corruptionPoints.get(file.getName());
                    if (corruptionPoint != null && corruptionPoint >= position && corruptionPoint < position + length) {
                        throw new IOException("Imaginary corruption");
                    }
                }
            };
        }
    }
}
