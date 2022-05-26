/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.iv2.DummyTransactionTask;
import org.voltdb.iv2.TxnEgo;

/**
 * Message from an execution site to initiator for DummyTransaction
 */
public class DummyTransactionResponseMessage extends VoltMessage {
    private long m_txnId;
    private long m_spHandle;
    private long m_spiHSId;

    /** Empty constructor for de-serialization */
    public DummyTransactionResponseMessage()
    {
        m_spiHSId = -1;
        m_subject = Subject.DEFAULT.getId();
    }

    /**
     * IV2 constructor
     */
    public DummyTransactionResponseMessage(DummyTransactionTask task) {
        m_txnId = task.getTxnId();
        m_spHandle = task.getSpHandle();
        m_spiHSId = task.getSPIHSId();
        m_subject = Subject.DEFAULT.getId();
    }

    public long getTxnId() {
        return m_txnId;
    }

    public long getSpHandle() {
        return m_spHandle;
    }

    public long getSPIHSId() {
        return m_spiHSId;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8 // txnId
            + 8 // m_spHandle
            + 8; // SPI HSId
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.DUMMY_TRANSACTION_RESPONSE_ID);
        buf.putLong(m_txnId);
        buf.putLong(m_spHandle);
        buf.putLong(m_spiHSId);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        m_txnId = buf.getLong();
        m_spHandle = buf.getLong();
        m_spiHSId = buf.getLong();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DUMMY_TRANSACTION_RESPONSE (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(" FOR TXN ").append(TxnEgo.txnIdToString(m_txnId)).append(")");
        sb.append(" SPI HSID: ").append(CoreUtils.hsIdToString(m_spiHSId));

        return sb.toString();
    }

    @Override
    public String getMessageInfo() {
        return "DummyTransactionResponseMessage TxnId:" + TxnEgo.txnIdToString(m_txnId);
    }
}
