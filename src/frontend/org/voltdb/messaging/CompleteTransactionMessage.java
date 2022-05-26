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
import java.util.Arrays;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.iv2.MpRestartSequenceGenerator;
import org.voltdb.iv2.TxnEgo;

public class CompleteTransactionMessage extends TransactionInfoBaseMessage
{
    long m_timestamp = INITIAL_TIMESTAMP;
    int m_hash;
    int m_flags = 0;

    // Note: flags below are not mutual exclusive, a CompleteTransactionMessage
    // may have one or more flags, e.g. rollback and restart.
    /**
     * Indicate the current MP transaction needs to be rollback.
     */
    static final int ISROLLBACK = 0;
    /**
     *  Not in use.
     */
    static final int REQUIRESACK = 1;
    /**
     *  MPI sends it when in progress MP transaction is interrupted by
     *  leader changes, don't apply to non-restartable sysproc.
     */
    static final int ISRESTART = 2;
    /**
     * Indicate whether the message deliver needs to be coordinated by the scoreboard.
     */
    static final int ISNPARTTXN = 3;
    /**
     *  A special type of completion that sends from MpPromoteAlgo to clean up
     *  transaction task queue, transaction state and the scoreboard of SP site.
     *  Only non-restartable sysproc fragment in the repair set can trigger it.
     */
    static final int ISABORTDURINGREPAIR = 4;
    /**
     * Indicate whether this is for an empty DR transaction.
     */
    static final int ISEMPTYDRTXN = 5;

    private void setBit(int position, boolean value)
    {
        if (value) {
            m_flags |= (1 << position);
        }
        else {
            m_flags &= ~(1 << position);
        }
    }

    private boolean getBit(int position)
    {
        return (((m_flags >> position) & 0x1) == 1);
    }

    /** Empty constructor for de-serialization */
    CompleteTransactionMessage() {
        super();
    }

    /**
     * These four args needed for base class
     * @param initiatorHSId
     * @param coordinatorHSId
     * @param txnId
     * @param isReadOnly
     *
     * @param isRollback  Should the recipient rollback this transaction to complete it?
     * @param requiresAck  Does the recipient need to respond to this message
     *                     with a CompleteTransactionResponseMessage?
     * @param isRestart   Does this CompleteTransactionMessage indicate a restart of this transaction?
     */
    public CompleteTransactionMessage(long initiatorHSId, long coordinatorHSId,
                                      long txnId, boolean isReadOnly, int hash,
                                      boolean isRollback, boolean requiresAck,
                                      boolean isRestart, boolean isForReplay,
                                      boolean isNPartTxn, boolean isAbortDuringRepair,
                                      boolean isEmptyDRTxn)
    {
        super(initiatorHSId, coordinatorHSId, txnId, 0, isReadOnly, isForReplay);
        m_hash = hash;
        setBit(ISROLLBACK, isRollback);
        setBit(REQUIRESACK, requiresAck);
        setBit(ISRESTART, isRestart);
        setBit(ISNPARTTXN, isNPartTxn);
        setBit(ISABORTDURINGREPAIR, isAbortDuringRepair);
        setBit(ISEMPTYDRTXN, isEmptyDRTxn);
    }

    public CompleteTransactionMessage(long initiatorHSId, long coordinatorHSId, CompleteTransactionMessage msg)
    {
        super(initiatorHSId, coordinatorHSId, msg);
        m_hash = msg.m_hash;
        m_flags = msg.m_flags;
    }

    public boolean isRollback()
    {
        return getBit(ISROLLBACK);
    }

    public boolean requiresAck()
    {
        return getBit(REQUIRESACK);
    }

    public boolean isRestart()
    {
        return getBit(ISRESTART);
    }

    public boolean isNPartTxn()
    {
        return getBit(ISNPARTTXN);
    }

    public boolean isAbortDuringRepair() {
        return getBit(ISABORTDURINGREPAIR);
    }

    public boolean isEmptyDRTxn() {
        return getBit(ISEMPTYDRTXN);
    }

    public int getHash() {
        return m_hash;
    }

    public void setRequireAck(boolean requireAck) {
        setBit(REQUIRESACK, requireAck);
    }

    public boolean needsCoordination() {
        return !isNPartTxn() && !isReadOnly();
    }

    // This is used when MP txn is restarted.
    public void setTimestamp(long timestamp) {
        m_timestamp = timestamp;
    }

    public long getTimestamp() {
        return m_timestamp;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 4 + 4 + 8;
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.COMPLETE_TRANSACTION_ID);
        super.flattenToBuffer(buf);
        buf.putInt(m_hash);
        buf.putInt(m_flags);
        buf.putLong(m_timestamp);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        super.initFromBuffer(buf);
        m_hash = buf.getInt();
        m_flags = buf.getInt();
        m_timestamp = buf.getLong();
        assert(buf.capacity() == buf.position());
    }

    public void indentedString(StringBuilder sb, int indentCnt) {
        char[] array = new char[indentCnt];
        Arrays.fill(array, ' ');
        String indent = new String("\n" + new String(array));
        sb.append("COMPLETE_TRANSACTION (FROM COORD: ");
        sb.append(CoreUtils.hsIdToString(m_coordinatorHSId));
        sb.append(") FOR TXN ");
        sb.append(TxnEgo.txnIdToString(m_txnId) + "(" + m_txnId + ")");
        sb.append(indent).append("SP HANDLE: ");
        sb.append(TxnEgo.txnIdToString(getSpHandle()));
        sb.append(indent).append("FLAGS: ").append(m_flags);

        if (isNPartTxn())
            sb.append(indent).append("  N Partition TXN");

        sb.append(indent).append("TIMESTAMP: ");
        MpRestartSequenceGenerator.restartSeqIdToString(m_timestamp, sb);
        sb.append(indent).append("TRUNCATION HANDLE:" + TxnEgo.txnIdToString(getTruncationHandle()));
        sb.append(indent).append("HASH: " + String.valueOf(m_hash));

        if (isRollback())
            sb.append(indent).append("THIS IS AN ROLLBACK REQUEST");

        if (requiresAck())
            sb.append(indent).append("THIS MESSAGE REQUIRES AN ACK");

        if (isRestart()) {
            sb.append(indent).append("THIS IS A TRANSACTION RESTART");
        }

        if (!isForReplica()) {
            sb.append(indent).append("SEND TO LEADER");
        }

        if(isAbortDuringRepair()) {
            sb.append(indent).append("THIS IS NOT RESTARTABLE (ABORT) REPAIR");
        }
    }

    @Override
    public void toDuplicateCounterString(StringBuilder sb) {
        sb.append("COMPLETION: ");
        if (isRestart()) {
            assert(!isAbortDuringRepair());
            assert(isRollback());
            sb.append("RESTARTABLE Rollback ");
            if (m_timestamp != INITIAL_TIMESTAMP) {
                MpRestartSequenceGenerator.restartSeqIdToString(m_timestamp, sb);
            }
        }
        else
        if (isAbortDuringRepair()) {
            assert(!isRestart());
            assert(isRollback());
            sb.append("ABORT Rollback ");
            if (m_timestamp != INITIAL_TIMESTAMP) {
                MpRestartSequenceGenerator.restartSeqIdToString(m_timestamp, sb);
            }
        }
        else
        if (isRollback()) {
            sb.append("ROLLBACK");
        }
        else {
            sb.append("COMMIT");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        indentedString(sb, 5);
        return sb.toString();
    }
}
