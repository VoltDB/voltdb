/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.voltcore.utils.Bits;
import org.voltdb.VoltDB;
import org.voltdb.test.utils.RandomTestRule;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.collect.Maps;

public class TestDurableCursor {

    public final static File TEST_DIR = new File("/tmp/" + System.getProperty("user.name"));
    private static final String PATH_STR = "TestExportCursor";
    private final static TreeSet<Integer> PARTITION_IDS = new TreeSet<>(Arrays.asList(0, 2, 4, 8));
    private final byte[] STARTING_GENID = {0, 0, 0, 0, 0, 0, 0, 8};
    private static final String STREAM_NAME_PREFIX = "teststream_";
    private static final int START_STREAM_COUNT = 10;
    private static final int STREAMS_PER_PAGE = Bits.pageSize() / DurableCursor.CURSOR_SIZE / PARTITION_IDS.size();

    @Rule
    public final RandomTestRule m_random = new RandomTestRule();

    private DurableCursor m_startCursor;

    private TreeMap<Integer, TreeMap<Integer, Long>> m_expectedValues = new TreeMap<>();

    private void changeCursor(int streamSuffix, int pid, long cursor, boolean expect) {
        if (expect) {
            m_expectedValues.get(streamSuffix).put(pid, cursor);
        }
        int slot = m_startCursor.slotForBlockId(pid, STREAM_NAME_PREFIX + streamSuffix);
        m_startCursor.updateCursor(slot, cursor);
    }

    private int getRandomPartitionId() {
        int pid;
        do {
            pid = m_random.nextInt(9);
        } while ((pid & 1) == 1 || pid == 6);
        return pid;
    }

    private void fillNextCycle(int numUpdates, boolean expect) {
        for (int ii = 0; ii < numUpdates; ii++) {
            Random m_random = new Random();
            int maxStreamSuffix = m_expectedValues.lastKey() + 1;
            int targetSuffix;
            do {
                targetSuffix = m_random.nextInt(maxStreamSuffix);
            } while (!m_expectedValues.containsKey(targetSuffix));
            changeCursor(targetSuffix, getRandomPartitionId(), Math.abs(m_random.nextLong()), expect);
        }
    }

    class CursorUpdater implements Runnable {
        final int m_loopCnt;
        final int m_firstStream;
        final int m_lastStream;
        final RandomTestRule m_random = new RandomTestRule();

        CursorUpdater(int loopCnt, int firstStream, int lastStream) {
            m_loopCnt = loopCnt;
            m_firstStream = firstStream;
            m_lastStream = lastStream;
        }

        @Override
        public void run() {
            System.out.println("starting updater for " + m_firstStream + " through " + m_lastStream);
            try {
                for (int ii = 0; ii < m_loopCnt; ii++)  {
                    int nextStream;
                    do {
                        nextStream = m_random.nextInt(m_lastStream+1);
                    } while (nextStream < m_firstStream);
                    int pid = getRandomPartitionId();
                    changeCursor(nextStream, pid, Math.abs(m_random.nextLong()), true);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unexpected exception in log sync thread", true, e);
            }
            System.out.println("stoping updater for " + m_firstStream + " through " + m_lastStream);
        }
    }


    private void configureStreamBlock(int streamSuffix, byte[] genId) {
        String streamName = STREAM_NAME_PREFIX + streamSuffix;
        m_startCursor.addCursorBlock(streamName, genId);
        TreeMap<Integer, Long> vals = new TreeMap<>();
        for(Integer pid : PARTITION_IDS) {
            vals.put(pid, 0L);
        }
        TreeMap<Integer, Long> oldVals = m_expectedValues.put(streamSuffix, vals);
        assert(oldVals == null);
    }

    private void verifyExpected(DurableCursor cursor, int maxStreams) {
        for (int ii = 0; ii < maxStreams; ii++) {
            Map<Integer, Long> result = cursor.getCursors(STREAM_NAME_PREFIX + ii);
            assertTrue(Maps.difference(result, m_expectedValues.get(ii)).areEqual());
        }
    }

    @Before
    public void setUp() throws Exception {
        setupTestDir();
        m_startCursor = new DurableCursor(5, TEST_DIR, PATH_STR, 10, PARTITION_IDS, STARTING_GENID);
        for (int ii = 0; ii < START_STREAM_COUNT; ii++) {
            configureStreamBlock(ii, STARTING_GENID);
        }
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
            m_startCursor.shutdown();
        } catch (Exception e) {}
        try {
            tearDownTestDir();
        } finally {
            m_startCursor.shutdown();
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

    @Test
    public void testSimple() throws Exception {
        System.out.println("Running testSimple");
        fillNextCycle(15, true);
        m_startCursor.writeFile(m_startCursor.m_0);
        fillNextCycle(15, true);
        m_startCursor.writeFile(m_startCursor.m_1);
        m_startCursor.m_0.closeFile();
        m_startCursor.m_1.closeFile();
        DurableCursor recovery = DurableCursor.RecoverBestCursor(TEST_DIR, PATH_STR);
        verifyExpected(recovery, START_STREAM_COUNT);
        recovery.shutdown();
    }


    @Test
    public void testGrowCursorFiles() throws Exception {
        System.out.println("Running testGrowCursorFiles");
        fillNextCycle(15, true);
        m_startCursor.writeFile(m_startCursor.m_1);
        int newStreamCount = START_STREAM_COUNT + STREAMS_PER_PAGE;
        for (int ii = START_STREAM_COUNT; ii < newStreamCount; ii ++) {
            configureStreamBlock(ii, STARTING_GENID);
        }
        fillNextCycle(500, true);
        m_startCursor.writeFile(m_startCursor.m_0);
        m_startCursor.m_0.closeFile();
        m_startCursor.m_1.closeFile();
        DurableCursor recovery = DurableCursor.RecoverBestCursor(TEST_DIR, PATH_STR);
        verifyExpected(recovery, newStreamCount);
        recovery.shutdown();
    }

    @Test
    public void testMissingCursorFiles() throws Exception {
        System.out.println("Running testMissingCursorFile");
        m_startCursor.shutdown();
        try {
            DurableCursor recovery = DurableCursor.RecoverBestCursor(TEST_DIR, PATH_STR);
            assertEquals(recovery, null);
        }
        catch(Exception e) {
            fail();
        }
    }

    @Test
    public void testMissingFirstCursorFile() throws Exception {
        System.out.println("Running testMissingFirstCursorFile");
        fillNextCycle(40, true);
        m_startCursor.writeFile(m_startCursor.m_1);
        m_startCursor.m_1.closeFile();
        DurableCursor recovery = DurableCursor.RecoverBestCursor(TEST_DIR, PATH_STR);
        verifyExpected(recovery, START_STREAM_COUNT);
        recovery.shutdown();
    }

    @Test
    public void testSaveThenUpdate() throws Exception {
        System.out.println("Running testSaveThenUpdate");
        fillNextCycle(40, true);
        m_startCursor.writeFile(m_startCursor.m_0);
        fillNextCycle(100, false);
        m_startCursor.m_0.closeFile();
        m_startCursor.m_1.closeFile();
        m_startCursor.shutdown();
        DurableCursor recovery = DurableCursor.RecoverBestCursor(TEST_DIR, PATH_STR);
        verifyExpected(recovery, START_STREAM_COUNT);
        recovery.shutdown();
    }

    @Test
    public void testInvalidDirectory() throws Exception {
        System.out.println("Running testInvalidDirectory");
        m_startCursor.shutdown();
        try {
            m_startCursor = new DurableCursor(1000, new File("/usr/bin"),
                    PATH_STR, 10, PARTITION_IDS, STARTING_GENID);
        } catch (IOException e) {
            return;
        }
        fail();
    }

    @Test
    public void testCorruptFile() throws Exception {
        System.out.println("Running testCorruptFile");
        fillNextCycle(40, true);
        m_startCursor.writeFile(m_startCursor.m_0);
        fillNextCycle(100, false);
        m_startCursor.writeFile(m_startCursor.m_1);
        m_startCursor.shutdown();

        // Currupt the m_1 file. Even though the version of m_1 is better, m_0 should always be recovered.
        RandomAccessFile ras = new RandomAccessFile(m_startCursor.m_1.m_durabilityMarker, "rw");
        FileChannel ch = ras.getChannel();
        long randFileOffset = m_random.nextInt((int)ch.size());
        ByteBuffer b = ByteBuffer.allocateDirect(1);
        int readCnt = ch.read(b, randFileOffset);
        assertEquals(readCnt, 1);
        b.position(0);
        byte oldVal = b.get(0);
        byte newVal;
        do {
            newVal = (byte) m_random.nextInt(256);
        } while (newVal == oldVal);
        b.put(newVal);
        b.position(0);
        ch.write(b, randFileOffset);
        ch.close();
        DurableCursor recovery = DurableCursor.RecoverBestCursor(TEST_DIR, PATH_STR);
        assertEquals(recovery.m_1, null);
        verifyExpected(recovery, START_STREAM_COUNT);
        recovery.shutdown();
    }

    @Test
    public void testSimultaneousUpdates() throws Exception {
        System.out.println("Running testSimultaneousUpdates");
        fillNextCycle(40, true);
        m_startCursor.startupScheduledWriter();
        Thread t1 = new Thread(new CursorUpdater(1000000, 0, 4));
        Thread t2 = new Thread(new CursorUpdater(1000000, 5, 8));
        Thread t3 = new Thread(new CursorUpdater(1000000, 9, 9));
        t1.start();
        t2.start();
        t3.start();
        t1.join();
        t2.join();
        t3.join();
        m_startCursor.shutdown();
        DurableCursor recovery = DurableCursor.RecoverBestCursor(TEST_DIR, PATH_STR);
        verifyExpected(recovery, START_STREAM_COUNT);
        // In local tests, the sequence number (number of IOs) got to 300 on SSD so there should be more then
        // be more then 2 on any test machine.
        assertTrue(recovery.getCurrentSequence() > 2);
        recovery.shutdown();
    }

}