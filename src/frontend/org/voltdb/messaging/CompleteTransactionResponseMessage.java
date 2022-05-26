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

import java.nio.ByteBuffer;

import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.iv2.TxnEgo;

public class CompleteTransactionResponseMessage extends VoltMessage
{
    long m_txnId;
    long m_spHandle;
    long m_spiHSId;
    boolean m_isRestart;
    boolean m_isRecovering = false;
    boolean m_ackRequired = false;
    boolean m_isAborted;

    /** Empty constructor for de-serialization */
    CompleteTransactionResponseMessage() {
        super();
    }

    public CompleteTransactionResponseMessage(CompleteTransactionMessage msg)
    {
        m_txnId = msg.getTxnId();
        m_spHandle = msg.getSpHandle();
        m_isRestart = msg.isRestart();
        m_spiHSId = msg.getCoordinatorHSId();
        m_ackRequired = msg.requiresAck();
        m_isAborted = msg.isRestart() || msg.isAbortDuringRepair();
    }

    public long getTxnId()
    {
        return m_txnId;
    }

    public long getSpHandle()
    {
        return m_spHandle;
    }

    public long getSPIHSId()
    {
        return m_spiHSId;
    }

    public boolean isRestart()
    {
        return m_isRestart;
    }

    public boolean isAborted()
    {
        return m_isAborted;
    }

    public boolean isRecovering()
    {
        return m_isRecovering;
    }

    public void setIsRecovering(boolean recovering)
    {
        m_isRecovering = recovering;
    }

    //used in partition leader migration.
    public boolean requireAck() {
        return m_ackRequired;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8 + 8 + 8 + 1 + 1 + 1 + 1;
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf)
    {
        buf.put(VoltDbMessageFactory.COMPLETE_TRANSACTION_RESPONSE_ID);
        buf.putLong(m_txnId);
        buf.putLong(m_spHandle);
        buf.putLong(m_spiHSId);
        buf.put((byte) (m_isRestart ? 1 : 0));
        buf.put((byte) (m_isRecovering ? 1 : 0));
        buf.put((byte) (m_ackRequired ? 1 : 0));
        buf.put((byte) (m_isAborted ? 1 : 0));
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf)
    {
        m_txnId = buf.getLong();
        m_spHandle = buf.getLong();
        m_spiHSId = buf.getLong();
        m_isRestart = buf.get() == 1;
        m_isRecovering = buf.get() == 1;
        m_ackRequired = buf.get() == 1;
        m_isAborted = buf.get() == 1;
        assert(buf.capacity() == buf.position());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("COMPLETE_TRANSACTION_RESPONSE (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(" TO ");
        sb.append(CoreUtils.hsIdToString(m_spiHSId));
        sb.append(" FOR TXN ID: ");
        sb.append(TxnEgo.txnIdToString(m_txnId));
        sb.append(" SPHANDLE: ");
        sb.append(TxnEgo.txnIdToString(m_spHandle));
        sb.append(" SPI ");
        sb.append(CoreUtils.hsIdToString(m_spiHSId));
        sb.append(" ISRESTART: ");
        sb.append(m_isRestart);
        sb.append(" ISABORTED: ");
        sb.append(m_isAborted);
        sb.append(" RECOVERING ");
        sb.append(m_isRecovering);

        return sb.toString();
    }

    @Override
    public String getMessageInfo() {
        return "CompleteTransactionResponseMessage TxnId:" + TxnEgo.txnIdToString(m_txnId);
    }
}
