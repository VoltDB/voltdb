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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltcore.messaging.MockMailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltdb.MockVoltDB;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.VoltDB;
import org.voltdb.utils.CompressionService;

import com.google_voltpatches.common.primitives.Ints;
import com.google_voltpatches.common.util.concurrent.Callables;

public class TestStreamSnapshotDataTarget {
    private MockMailbox m_mb;
    private StreamSnapshotDataTarget.SnapshotSender m_sender;
    private StreamSnapshotAckReceiver m_ack;
    private List<SnapshotTableInfo> m_tables;

    private ExecutorService m_es = CoreUtils.getCachedSingleThreadExecutor("Close stream thread", 10000);

    @BeforeClass
    public static void sestUpBeforeClass()
    {
        VoltDB.replaceVoltDBInstanceForTest(new MockVoltDB());
    }

    @Before
    public void setup()
    {
        m_mb = new MockMailbox();
        m_mb.setHSId(100);
        VoltDB.instance().getHostMessenger().createMailbox(100l, m_mb);

        m_sender = new StreamSnapshotDataTarget.SnapshotSender(m_mb);
        m_ack = new StreamSnapshotAckReceiver(m_mb);
        new Thread(m_sender, "test sender").start();
        new Thread(m_ack, "test ack").start();

        m_tables = new ArrayList<>(20);
        for (int i = 0; i < 20; i++) {
            SnapshotTableInfo table = new SnapshotTableInfo(Integer.toString(i), i);
            table.setSchema(Ints.toByteArray(i), 0);
            m_tables.add(table);
        }
    }

    @After
    public void tearDown()
    {
        VoltDB.instance().getHostMessenger().removeMailbox(100l);
    }

    private StreamSnapshotDataTarget makeDataTarget(long srcHSId, long destHSId, boolean sendHashinator, boolean lowestSite)
    {
        byte[] hashinatorBytes = null;
        if (sendHashinator) {
            ByteBuffer hashinator = ByteBuffer.allocate(8 + 3);
            hashinator.putLong(3l);
            hashinator.put(new byte[] {4, 5, 6});
            hashinatorBytes = hashinator.array();
        }

        return new StreamSnapshotDataTarget(srcHSId, destHSId, lowestSite, new HashSet<Long>(Arrays.asList(destHSId)),
                hashinatorBytes, m_tables, m_sender, m_ack);
    }

    private Callable<DBBPool.BBContainer> makeTuples()
    {
        DBBPool.BBContainer container =
            DBBPool.wrapBB(ByteBuffer.allocate(StreamSnapshotDataTarget.contentOffset));
        return Callables.returning(container);
    }

    private void verifyData(RejoinDataMessage msg,
                            long targetId,
                            StreamSnapshotMessageType type,
                            int tableId,
                            int blockIndex)
        throws IOException
    {
        assertEquals(targetId, msg.getTargetId());

        ByteBuffer data = ByteBuffer.wrap(CompressionService.decompressBytes(msg.getData()));
        assertEquals(type.ordinal(), data.get(StreamSnapshotBase.typeOffset));
        assertEquals(blockIndex, data.getInt(StreamSnapshotBase.blockIndexOffset));

        // Only data block and schema blocks have table ID
        if (type == StreamSnapshotMessageType.SCHEMA || type == StreamSnapshotMessageType.DATA) {
            assertEquals(tableId, data.getInt(StreamSnapshotBase.tableIdOffset));
        }
    }

    private void writeAndVerify(StreamSnapshotDataTarget dut, int tableId, boolean hasSchema)
        throws IOException
    {
        dut.write(makeTuples(), tableId);

        // verify data is sent
        if (hasSchema) {
            while (m_mb.noSentMessages()) {
                Thread.yield();
            }
            verifyData((RejoinDataMessage) m_mb.pollMessage(),
                       dut.m_targetId,
                       StreamSnapshotMessageType.SCHEMA,
                       tableId,
                       dut.m_blockIndex - 2);
        }

        while (m_mb.noSentMessages()) {
            Thread.yield();
        }

        verifyData((RejoinDataMessage) m_mb.pollMessage(),
                   dut.m_targetId,
                   StreamSnapshotMessageType.DATA,
                   tableId,
                   dut.m_blockIndex - 1);
    }

    private void ack(boolean isEOS, long targetId, int blockIndex)
    {
        // send ack
        m_mb.deliver(new RejoinDataAckMessage(isEOS, targetId, blockIndex));
    }

    private void closeStream(final StreamSnapshotDataTarget dut)
        throws IOException, InterruptedException, ExecutionException
    {
        Future<?> closeWork = m_es.submit(new Runnable() {
            @Override
            public void run()
            {
                // close data target 1, the sender thread and the ack thread should stay up
                // and the mailbox should still be reachable from the host messenger
                try {
                    dut.close();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        });

        // Wait until the END message is sent
        while (m_mb.noSentMessages()) {
            Thread.yield();
        }
        verifyData((RejoinDataMessage) m_mb.pollMessage(),
                   dut.m_targetId,
                   StreamSnapshotMessageType.END,
                   /* tableId = */ 0,
                   /* blockIndex = */ dut.m_blockIndex - 1);

        // Ack the END message
        // there is a small window for the test that END message is sent but
        // m_ackCounter hasn't been setup.
        Thread.sleep(1000);
        ack(true, dut.m_targetId, dut.m_blockIndex - 1);

        closeWork.get();
    }

    @Test
    public void testStreamClose() throws IOException, InterruptedException, ExecutionException
    {
        StreamSnapshotDataTarget dut1 = makeDataTarget(100, 1000, false, true);
        StreamSnapshotDataTarget dut2 = makeDataTarget(101, 1001, false, false);

        closeStream(dut1);
        assertNotNull(VoltDB.instance().getHostMessenger().getMailbox(m_mb.getHSId()));

        // write some data to data target 2 and make sure the sender sends it and we can still ack
        writeAndVerify(/* dataTarget = */ dut2, /* tableId = */ 0, /* hasSchema = */ true);

        // ack schema block
        ack(false, dut2.m_targetId, dut2.m_blockIndex - 2);
        // ack data block
        ack(false, dut2.m_targetId, dut2.m_blockIndex - 1);

        closeStream(dut2);
    }

    @Test
    public void testMultiplexing() throws IOException, InterruptedException, ExecutionException
    {
        StreamSnapshotDataTarget dut1 = makeDataTarget(100, 1000, false, true);
        StreamSnapshotDataTarget dut2 = makeDataTarget(101, 1001, false, false);

        writeAndVerify(/* dataTarget = */ dut1, /* tableId = */ 0, /* hasSchema = */ true);
        writeAndVerify(/* dataTarget = */ dut2, /* tableId = */ 1, /* hasSchema = */ true);

        // 2 outstanding works, 1 for schema, 1 for data
        assertEquals(2, dut1.m_outstandingWorkCount.get());
        assertEquals(2, dut2.m_outstandingWorkCount.get());

        ack(false, dut1.m_targetId, dut1.m_blockIndex - 2);
        ack(false, dut1.m_targetId, dut1.m_blockIndex - 1);
        while (dut1.m_outstandingWorkCount.get() != 0) {
            Thread.yield();
        }

        ack(false, dut2.m_targetId, dut2.m_blockIndex - 2);
        ack(false, dut2.m_targetId, dut2.m_blockIndex - 1);
        while (dut2.m_outstandingWorkCount.get() != 0) {
            Thread.yield();
        }

        closeStream(dut1);
        closeStream(dut2);
    }

    @Test
    public void testSendHashinatorConfig() throws IOException, ExecutionException, InterruptedException
    {
        StreamSnapshotDataTarget dut = makeDataTarget(100, 1000, true, true);

        assertEquals(1, dut.m_outstandingWorkCount.get());
        while (m_mb.noSentMessages()) {
            Thread.yield();
        }
        // verify that the first block is the hashinator config bytes
        verifyData((RejoinDataMessage) m_mb.pollMessage(),
                   dut.m_targetId,
                   StreamSnapshotMessageType.HASHINATOR,
                   -1,
                   0);
        ack(false, dut.m_targetId, 0);

        closeStream(dut);
    }
}
