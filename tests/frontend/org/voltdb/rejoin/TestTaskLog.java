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

package org.voltdb.rejoin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.iv2.UniqueIdGenerator;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import org.apache.commons.io.FileUtils;

public class TestTaskLog {
    private TaskLogImpl m_log = null;
    private File m_tempDir = null;

    File getTempDir() throws IOException {
        File overflowDir = File.createTempFile("test-tasklog", "");
        overflowDir.delete();
        assertTrue(overflowDir.mkdir());
        return overflowDir;
    }

    @Before
    public void setUp() throws IOException {
        m_tempDir = getTempDir();
        m_log = new TaskLogImpl(0, m_tempDir);
        m_log.enableRecording(Long.MIN_VALUE);
    }

    @After
    public void tearDown() throws IOException {
        if (m_log != null) {
            m_log.close(true);
        }
        FileUtils.deleteDirectory(m_tempDir);
        m_tempDir = null;
        m_log = null;
        System.gc();
        System.runFinalization();
    }

    private final UniqueIdGenerator m_uniqueIdGenerator = new UniqueIdGenerator(0, 0);

    @Test
    public void testBasic() throws IOException {
        System.out.println("Running testBasic");
        // push 200 single-partition task messages to the queue, won't fill a buffer in the task log,
        // tests the short-circuit tail->head logic
        TxnEgo spego = TxnEgo.makeZero(0);
        for (int i = 0; i < 200; i++) {
            final long uniqueId = m_uniqueIdGenerator.getNextUniqueId();
            StoredProcedureInvocation invocation = new StoredProcedureInvocation();
            invocation.setProcName("hah");
            invocation.setParams(1l, "haha", 3.3);

            Iv2InitiateTaskMessage task =
                new Iv2InitiateTaskMessage(1, 2, Long.MIN_VALUE, spego.getTxnId(), uniqueId,
                        false, true, false, invocation, 4, 5, false);
            task.setSpHandle(spego.getTxnId());
            spego = spego.makeNext();
            m_log.logTask(task);
        }

        // pull all task messages in the queue and verify if they are the same
        int count = 0;
        spego = TxnEgo.makeZero(0);
        while (!m_log.isEmpty()) {
            Iv2InitiateTaskMessage nextMessage = (Iv2InitiateTaskMessage) m_log.getNextMessage();
            if (nextMessage == null) {
                nextMessage = (Iv2InitiateTaskMessage) m_log.getNextMessage();
            }
            if (!m_log.isEmpty()) {
                if (nextMessage == null) {
                    continue;
                }
            }
            assertEquals(1, nextMessage.getInitiatorHSId());
            assertEquals(2, nextMessage.getCoordinatorHSId());

            assertEquals(spego.getTxnId(), nextMessage.getTxnId());
            assertEquals(spego.getTxnId(), nextMessage.getSpHandle());
            spego = spego.makeNext();

            StoredProcedureInvocation nextInvocation = nextMessage.getStoredProcedureInvocation();
            assertEquals("hah", nextInvocation.getProcName());
            count++;
            if (m_log.isEmpty()) break;
        }
        assertEquals(200, count);
    }

    @Test
    public void testBufferFill() throws IOException {
        System.out.println("Running testBufferFill");
        // push two InvocationBuffers worth of single-partition task messages
        // to the queue, fill two internal buffers and verify we get them both
        // back
        TxnEgo spego = TxnEgo.makeZero(0);
        for (int i = 0; i < 5; i++) {
            final long uniqueId = m_uniqueIdGenerator.getNextUniqueId();
            StoredProcedureInvocation invocation = new StoredProcedureInvocation();
            invocation.setProcName("hah");
            invocation.setParams(1l, "haha", 3.3, new byte[1024 * 128]);

            Iv2InitiateTaskMessage task =
                new Iv2InitiateTaskMessage(1, 2, Long.MIN_VALUE, spego.getTxnId(), uniqueId,
                        false, true, false, invocation, 4, 5, false);
            task.setSpHandle(spego.getTxnId());
            spego = spego.makeNext();
            m_log.logTask(task);
        }

        // pull all task messages in the queue and verify if they are the same
        int count = 0;
        spego = TxnEgo.makeZero(0);
        while (!m_log.isEmpty()) {
            Iv2InitiateTaskMessage nextMessage = (Iv2InitiateTaskMessage) m_log.getNextMessage();
            if (nextMessage == null) {
                nextMessage = (Iv2InitiateTaskMessage) m_log.getNextMessage();
            }
            if (!m_log.isEmpty()) {
                if (nextMessage == null) {
                    continue;
                }
            }
            assertEquals(1, nextMessage.getInitiatorHSId());
            assertEquals(2, nextMessage.getCoordinatorHSId());

            assertEquals(spego.getTxnId(), nextMessage.getTxnId());
            assertEquals(spego.getTxnId(), nextMessage.getSpHandle());
            spego = spego.makeNext();

            StoredProcedureInvocation nextInvocation = nextMessage.getStoredProcedureInvocation();
            assertEquals("hah", nextInvocation.getProcName());
            count++;
        }
        assertEquals(5, count);
    }

    @Test
    public void testAppendWhilePulling() throws IOException {
        System.out.println("Running testAppendWhilePulling");
        // push 100 task messages to the queue first
        TxnEgo spego = TxnEgo.makeZero(0);
        for (int i = 0; i < 100; i++) {
            final long uniqueId = m_uniqueIdGenerator.getNextUniqueId();
            StoredProcedureInvocation invocation = new StoredProcedureInvocation();
            invocation.setProcName("hah");
            invocation.setParams(1l, "haha", 3.3);

            Iv2InitiateTaskMessage task =
                new Iv2InitiateTaskMessage(1, 2, Long.MIN_VALUE, spego.getTxnId(), uniqueId,
                        false, true, false, invocation, 4, 5, false);
            task.setSpHandle(spego.getTxnId());
            spego = spego.makeNext();
            m_log.logTask(task);
        }

        // pull 50 messages out
        int count = 0;
        TxnEgo readSpego = TxnEgo.makeZero(0);
        while (!m_log.isEmpty() && count < 50) {
            Iv2InitiateTaskMessage nextMessage = (Iv2InitiateTaskMessage) m_log.getNextMessage();
            if (nextMessage == null) {
                nextMessage = (Iv2InitiateTaskMessage) m_log.getNextMessage();
            }
            if (!m_log.isEmpty()) {
                if (nextMessage == null) {
                    continue;
                }
            } else {
                // reached end of queue
                break;
            }
            assertEquals(1, nextMessage.getInitiatorHSId());
            assertEquals(2, nextMessage.getCoordinatorHSId());

            assertEquals(readSpego.getTxnId(), nextMessage.getTxnId());
            assertEquals(readSpego.getTxnId(), nextMessage.getSpHandle());
            readSpego = readSpego.makeNext();

            StoredProcedureInvocation nextInvocation = nextMessage.getStoredProcedureInvocation();
            assertEquals("hah", nextInvocation.getProcName());
            count++;
        }

        // push 100 more
        for (int i = 100; i < 200; i++) {
            final long uniqueId = m_uniqueIdGenerator.getNextUniqueId();
            StoredProcedureInvocation invocation = new StoredProcedureInvocation();
            invocation.setProcName("hah");
            invocation.setParams(1l, "haha", 3.3);

            Iv2InitiateTaskMessage task =
                new Iv2InitiateTaskMessage(1, 2, Long.MIN_VALUE, spego.getTxnId(), uniqueId,
                        false, true, false, invocation, 4, 5, false);
            task.setSpHandle(spego.getTxnId());
            spego = spego.makeNext();
            m_log.logTask(task);
        }

        // pull the rest of the messages
        while (!m_log.isEmpty()) {
            Iv2InitiateTaskMessage nextMessage = (Iv2InitiateTaskMessage) m_log.getNextMessage();
            if (nextMessage == null) {
                nextMessage = (Iv2InitiateTaskMessage) m_log.getNextMessage();
            }
            if (!m_log.isEmpty()) {
                if (nextMessage == null) {
                    continue;
                }
            }
            assertEquals(1, nextMessage.getInitiatorHSId());
            assertEquals(2, nextMessage.getCoordinatorHSId());

            assertEquals(readSpego.getTxnId(), nextMessage.getTxnId());
            assertEquals(readSpego.getTxnId(), nextMessage.getSpHandle());
            readSpego = readSpego.makeNext();

            StoredProcedureInvocation nextInvocation = nextMessage.getStoredProcedureInvocation();
            assertEquals("hah", nextInvocation.getProcName());
            count++;
        }

        assertEquals(200, count);
    }

    @Test
    public void testFuzz() throws Exception {
        for (int zz = 0; zz < 10; zz++) {
            final long seed = System.currentTimeMillis();
            System.out.println("Running testFuzz with seed " + seed);
            final Random r = new Random(seed);

            long counter = 0;
            final int initialBuffers = r.nextInt(1000);
            for (int ii = 0; ii < initialBuffers; ii++) {
                StoredProcedureInvocation invocation = new StoredProcedureInvocation();
                invocation.setProcName("hah");
                ByteBuffer buf = ByteBuffer.allocate(1024 * 64);
                buf.putLong(counter);
                counter++;
                invocation.setParams(1l, "haha", buf.array());

                Iv2InitiateTaskMessage task =
                    new Iv2InitiateTaskMessage(1, 2, Long.MIN_VALUE, 0, 0,
                            false, true, false, invocation, 4, 5, false);
                m_log.logTask(task);
            }

            long expectedCounter = 0;
            while (!m_log.isEmpty()) {
                double nextAction = r.nextDouble();
                if (nextAction > .6) {
                    StoredProcedureInvocation invocation = new StoredProcedureInvocation();
                    invocation.setProcName("hah");
                    ByteBuffer buf = ByteBuffer.allocate(1024 * 64);
                    buf.putLong(counter);
                    counter++;
                    invocation.setParams(1l, "haha", buf.array());

                    Iv2InitiateTaskMessage task =
                        new Iv2InitiateTaskMessage(1, 2, Long.MIN_VALUE, 0, 0,
                                false, true, false, invocation, 4, 5, false);
                    m_log.logTask(task);
                } else {
                    Iv2InitiateTaskMessage message = null;
                    while ((message = (Iv2InitiateTaskMessage)m_log.getNextMessage()) == null) {}
                    if (message != null) {
                        ByteBuffer buf =
                                ByteBuffer.wrap((byte[])message.getParameters()[2]);
                        assertEquals(expectedCounter, buf.getLong());
                        expectedCounter++;
                    }
                }
            }
            assertNull(m_log.getNextMessage());
            tearDown();
            setUp();
            Thread.sleep(10);
        }
    }

    @Test
    public void testSetSnapshotTxnIdAfter() throws IOException {
        // push 200 single-partition task messages to the queue, won't fill a buffer in the task log,
        // set the snapshot spHandle when we have pushed 100 SPs in there, the first 100 SPs should
        // be dropped when we poll.
        TxnEgo spego = TxnEgo.makeZero(0);
        long snapshotSpHandle = 0;
        for (int i = 0; i < 200; i++) {
            final long uniqueId = m_uniqueIdGenerator.getNextUniqueId();
            StoredProcedureInvocation invocation = new StoredProcedureInvocation();
            invocation.setProcName("hah");
            invocation.setParams(1l, "haha", 3.3);

            Iv2InitiateTaskMessage task =
                    new Iv2InitiateTaskMessage(1, 2, Long.MIN_VALUE, spego.getTxnId(), uniqueId,
                            false, true, false, invocation, 4, 5, false);
            task.setSpHandle(spego.getTxnId());
            spego = spego.makeNext();
            m_log.logTask(task);

            if (i == 99) {
                // The snapshot fragment comes in the middle
                snapshotSpHandle = spego.getTxnId();
                m_log.enableRecording(snapshotSpHandle);
                spego = spego.makeNext();
            }
        }

        // pull all task messages in the queue and verify if they are the same
        int count = 0;
        spego = new TxnEgo(snapshotSpHandle).makeNext();
        while (!m_log.isEmpty()) {
            Iv2InitiateTaskMessage nextMessage = (Iv2InitiateTaskMessage) m_log.getNextMessage();
            if (nextMessage == null) {
                nextMessage = (Iv2InitiateTaskMessage) m_log.getNextMessage();
            }
            if (!m_log.isEmpty()) {
                if (nextMessage == null) {
                    continue;
                }
            }
            assertEquals(1, nextMessage.getInitiatorHSId());
            assertEquals(2, nextMessage.getCoordinatorHSId());

            assertEquals(spego.getTxnId(), nextMessage.getTxnId());
            assertEquals(spego.getTxnId(), nextMessage.getSpHandle());
            spego = spego.makeNext();

            StoredProcedureInvocation nextInvocation = nextMessage.getStoredProcedureInvocation();
            assertEquals("hah", nextInvocation.getProcName());
            count++;
            if (m_log.isEmpty()) break;
        }
        assertEquals(100, count);
    }
}
