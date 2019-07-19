/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.iv2.TxnEgo;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Message from a stored procedure coordinator to an execution site
 * which is participating in the transaction. This message specifies
 * which planfragment to run and with which parameters.
 *
 * This message should NEVER go over the network, so there
 */
public class BorrowTaskMessage extends TransactionInfoBaseMessage
{
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    Map<Integer, List<VoltTable>> m_inputDeps = null;
    FragmentTaskMessage m_fragTask;

    /** Empty constructor for de-serialization */
    BorrowTaskMessage() {
        m_subject = Subject.DEFAULT.getId();
    }

    public BorrowTaskMessage(FragmentTaskMessage frag)
    {
        super(frag.getInitiatorHSId(),
              frag.getCoordinatorHSId(),
              frag.getTxnId(),
              frag.getUniqueId(),
              frag.isReadOnly(), false);
        m_subject = Subject.DEFAULT.getId();
        m_fragTask = frag;
    }

    public FragmentTaskMessage getFragmentTaskMessage()
    {
        return m_fragTask;
    }

    public void addInputDepMap(Map<Integer, List<VoltTable>> inputDeps)
    {
        final ImmutableMap.Builder<Integer, List<VoltTable>> builder = ImmutableMap.builder();
        for (Map.Entry<Integer, List<VoltTable>> e : inputDeps.entrySet()) {
            builder.put(e.getKey(), ImmutableList.copyOf(e.getValue()));
        }
        m_inputDeps = builder.build();
    }

    public Map<Integer, List<VoltTable>> getInputDepMap()
    {
        return m_inputDeps;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        // serialize the map
        msgsize += 4; // number of map keys: int
        for (Map.Entry<Integer, List<VoltTable>> e : m_inputDeps.entrySet()) {
            msgsize += 4; // entry key
            msgsize += 4; // VoltTable count: int
            for (VoltTable vt: e.getValue()) {
                msgsize += vt.getSerializedSize();
            }       }

        assert(m_fragTask != null);
        msgsize += m_fragTask.getSerializedSize();

        return msgsize;

    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltDbMessageFactory.BORROW_TASK_ID);
        super.flattenToBuffer(buf);

        buf.putInt(m_inputDeps.size());
        for (Map.Entry<Integer, List<VoltTable>> e : m_inputDeps.entrySet()) {
            buf.putInt(e.getKey());
            buf.putInt(e.getValue().size());
            for (VoltTable vt: e.getValue()) {
                vt.flattenToBuffer(buf);
            }
        }

        assert(m_fragTask != null);
        ByteBuffer paybuf = ByteBuffer.allocate(m_fragTask.getSerializedSize());
        m_fragTask.flattenToBuffer(paybuf);
        if (paybuf.position() != 0) {
            paybuf.flip();
        }
        buf.put(paybuf);
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        super.initFromBuffer(buf);

        int numKeys = buf.getInt();
        Map<Integer, List<VoltTable>> depTables = new HashMap<>();
        for (int i = 0; i < numKeys; i++) {
            int key = buf.getInt();
            int vtSize = buf.getInt();
            List<VoltTable> vtList = new ArrayList<>();
            for (int j = 0; j < vtSize; j++) {
                VoltTable vt = PrivateVoltTableFactory.createVoltTableFromSharedBuffer(buf);
                vtList.add(vt);
            }
            depTables.put(key, vtList);
        }
        addInputDepMap(depTables);

        VoltDbMessageFactory messageFactory = new VoltDbMessageFactory();
        m_fragTask = (FragmentTaskMessage) messageFactory.createMessageFromBuffer(buf, m_sourceHSId);
        assert(m_fragTask != null);
    }

    @Override
    public void toDuplicateCounterString(StringBuilder sb) {
        sb.append("Unexpected Borrow task");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("BORROW_TASK (FROM ");
        sb.append(CoreUtils.hsIdToString(m_coordinatorHSId));
        sb.append(") FOR TXN ");
        sb.append(TxnEgo.txnIdToString(m_txnId));

        sb.append("\n");
        sb.append(" UNIQUE ID ").append(m_uniqueId).append("\n");
        if (m_isReadOnly)
            sb.append("  READ, COORD ");
        else
            sb.append("  WRITE, COORD ");
        sb.append(CoreUtils.hsIdToString(m_coordinatorHSId));

        sb.append("\nInput dependency: \n");
        for (Map.Entry<Integer, List<VoltTable>> e : m_inputDeps.entrySet()) {
            sb.append("\t").append(e.getKey()).append(" -> ");
            for (VoltTable vt: e.getValue()) {
                sb.append(vt.toJSONString()).append(", ");
            }
        }
        return sb.toString();
    }
}
