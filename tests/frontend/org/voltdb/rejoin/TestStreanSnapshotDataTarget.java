/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import static junit.framework.Assert.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Callables;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltcore.messaging.MockMailbox;
import org.voltcore.utils.DBBPool;
import org.voltdb.MockVoltDB;
import org.voltdb.SnapshotTableTask;
import org.voltdb.VoltDB;
import org.voltdb.utils.CompressionService;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class TestStreanSnapshotDataTarget {
    private MockMailbox m_mb;
    private StreamSnapshotDataTarget.SnapshotSender m_sender;
    private Thread m_senderThread;
    private StreamSnapshotAckReceiver m_ack;
    private Thread m_ackThread;
    private Map<Integer, byte[]> m_schemas;

    private List<StreamSnapshotDataTarget> m_duts = new ArrayList<StreamSnapshotDataTarget>();

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
        m_senderThread = new Thread(m_sender, "test sender");
        m_ackThread = new Thread(m_ack, "test ack");
        m_senderThread.start();
        m_ackThread.start();

        m_schemas = Maps.newHashMap();
        for (int i = 0; i < 20; i++) {
            m_schemas.put(i, Ints.toByteArray(i));
        }
    }

    @After
    public void tearDown()
    {
        VoltDB.instance().getHostMessenger().removeMailbox(100l);
    }

    private StreamSnapshotDataTarget makeDataTarget(long destHSId)
    {
        StreamSnapshotDataTarget dut =
            new StreamSnapshotDataTarget(destHSId, m_schemas, m_mb, m_sender, m_ack);
        m_duts.add(dut);
        return dut;
    }

    private SnapshotTableTask makeTask(int tableId)
    {
        SnapshotTableTask task = mock(SnapshotTableTask.class);
        doReturn(tableId).when(task).getTableId();
        return task;
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
        if (type != StreamSnapshotMessageType.END && type != StreamSnapshotMessageType.FAILURE) {
            assertEquals(tableId, data.getInt(StreamSnapshotBase.tableIdOffset));

            // Only data block has block index
            if (type == StreamSnapshotMessageType.DATA) {
                assertEquals(blockIndex, data.getInt(StreamSnapshotBase.blockIndexOffset));
            }
        }
    }

    private void writeAndVerify(StreamSnapshotDataTarget dut, int tableId, boolean hasSchema)
        throws IOException
    {
        dut.write(makeTuples(), makeTask(tableId));

        // verify data is sent
        if (hasSchema) {
            while (m_mb.noSentMessages()) {
                Thread.yield();
            }
            verifyData((RejoinDataMessage) m_mb.pollMessage(),
                       dut.m_targetId,
                       StreamSnapshotMessageType.SCHEMA,
                       tableId,
                       dut.m_blockIndex - 1);
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

    private void ack(StreamSnapshotDataTarget dut)
    {
        // send ack
        m_mb.deliver(new RejoinDataAckMessage(dut.m_targetId, dut.m_blockIndex - 1));
    }

    @Test
    public void testStreamClose() throws IOException, InterruptedException
    {
        StreamSnapshotDataTarget dut1 = makeDataTarget(1000);
        StreamSnapshotDataTarget dut2 = makeDataTarget(1001);

        // close data target 1, the sender thread and the ack thread should stay up
        // and the mailbox should still be reachable from the host messenger
        dut1.close();
        verifyData((RejoinDataMessage) m_mb.pollMessage(),
                   dut1.m_targetId,
                   StreamSnapshotMessageType.END,
                   /* tableId = */ 0,
                   /* blockIndex = */ 0);
        assertNotNull(VoltDB.instance().getHostMessenger().getMailbox(m_mb.getHSId()));

        // write some data to data target 2 and make sure the sender sends it and we can still ack
        writeAndVerify(/* dataTarget = */ dut2, /* tableId = */ 0, /* hasSchema = */ true);
        ack(dut2);
        dut2.close();
    }

    @Test
    public void testMultiplexing() throws IOException, InterruptedException
    {
        StreamSnapshotDataTarget dut1 = makeDataTarget(1000);
        StreamSnapshotDataTarget dut2 = makeDataTarget(1001);

        writeAndVerify(/* dataTarget = */ dut1, /* tableId = */ 0, /* hasSchema = */ true);
        writeAndVerify(/* dataTarget = */ dut2, /* tableId = */ 1, /* hasSchema = */ true);

        assertEquals(1, dut1.m_outstandingWorkCount.get());
        assertEquals(1, dut2.m_outstandingWorkCount.get());

        ack(dut1);
        while (dut1.m_outstandingWorkCount.get() != 0) {
            Thread.yield();
        }

        ack(dut2);
        while (dut2.m_outstandingWorkCount.get() != 0) {
            Thread.yield();
        }

        dut1.close();
        dut2.close();
    }
}
