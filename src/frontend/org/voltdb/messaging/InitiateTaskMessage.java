/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.iv2.TxnEgo;


/**
 * Message from an initiator to an execution site, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
public class InitiateTaskMessage extends TransactionInfoBaseMessage {

    boolean m_isSinglePartition;
    StoredProcedureInvocation m_invocation;
    long m_lastSafeTxnID; // this is the largest txn acked by all partitions running the java for it
    AtomicBoolean m_isDurable;

    /*
     * For multi-part txn ONLY.
     *
     * The initiator will send the list of non-coordinator sites it has sent
     * participant notices to to the coordinator so that the coordinator will
     * send fragment works to all sites.
     *
     * Only the coordinator will have this, coordinator replicas and other sites
     * won't have this.
     */
    long[] m_nonCoordinatorSites = null;

    /** Empty constructor for de-serialization */
    InitiateTaskMessage() {
        super();
    }

    /**
     * Used for multi-part txn initiation only.
     *
     * @param nonCoordinatorSites
     *            Non-coordinator sites, including coordinator replicas and
     *            other sites.
     */
    public InitiateTaskMessage(long initiatorHSId,
                               long coordinatorHSId,
                               long txnId,
                               boolean isReadOnly,
                               boolean isSinglePartition,
                               StoredProcedureInvocation invocation,
                               long lastSafeTxnID,
                               long[] nonCoordinatorSites) {
        super(initiatorHSId,
                coordinatorHSId,
                txnId,
                txnId,
                isReadOnly,
                false);
        m_isSinglePartition = isSinglePartition;
        m_invocation = invocation;
        m_lastSafeTxnID = lastSafeTxnID;
        m_nonCoordinatorSites = nonCoordinatorSites;
    }

    public InitiateTaskMessage(long initiatorHSId,
                        long coordinatorHSId,
                        long txnId,
                        boolean isReadOnly,
                        boolean isSinglePartition,
                        StoredProcedureInvocation invocation,
                        long lastSafeTxnID) {
        super(initiatorHSId,
                coordinatorHSId,
                txnId,
                txnId,
                isReadOnly,
                false);
        m_isSinglePartition = isSinglePartition;
        m_invocation = invocation;
        m_lastSafeTxnID = lastSafeTxnID;
        m_nonCoordinatorSites = null;
    }

    @Override
    public boolean isReadOnly() {
        return m_isReadOnly;
    }

    @Override
    public boolean isSinglePartition() {
        return m_isSinglePartition;
    }

    public StoredProcedureInvocation getStoredProcedureInvocation() {
        return m_invocation;
    }

    public String getStoredProcedureName() {
        assert(m_invocation != null);
        return m_invocation.getProcName();
    }

    public int getParameterCount() {
        assert(m_invocation != null);
        if (m_invocation.getParams() == null)
            return 0;
        return m_invocation.getParams().toArray().length;
    }

    public Object[] getParameters() {
        return m_invocation.getParams().toArray();
    }

    public long getLastSafeTxnId() {
        return m_lastSafeTxnID;
    }

    public AtomicBoolean getDurabilityFlag() {
        assert(!m_isReadOnly);
        if (m_isDurable == null) {
            m_isDurable = new AtomicBoolean();
        }
        return m_isDurable;
    }

    public AtomicBoolean getDurabilityFlagIfItExists() {
        return m_isDurable;
    }

    public long[] getNonCoordinatorSites() {
        return m_nonCoordinatorSites;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8 // m_lastSafeTxnId
            + 1 // is single partition flag?
            + 1; // is m_nonCoordinatorSites null?

        if (m_nonCoordinatorSites != null) {
            msgsize += 4 + (m_nonCoordinatorSites.length * 8); // m_nonCoordinatorSites
        }

        msgsize += m_invocation.getSerializedSize();

        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.INITIATE_TASK_ID);
        super.flattenToBuffer(buf);
        buf.putLong(m_lastSafeTxnID);
        buf.put(m_isSinglePartition ? (byte) 1 : (byte) 0);

        buf.put(m_nonCoordinatorSites == null ? 1 : (byte) 0);
        if (m_nonCoordinatorSites != null) {
            buf.putInt(m_nonCoordinatorSites.length);
            for (long hsId : m_nonCoordinatorSites) {
                buf.putLong(hsId);
            }
        }

        m_invocation.flattenToBuffer(buf);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        super.initFromBuffer(buf);

        m_lastSafeTxnID = buf.getLong();
        m_isSinglePartition = buf.get() == 1;

        if (buf.get() == 0) {
            m_nonCoordinatorSites = new long[buf.getInt()];
            for (int i = 0; i < m_nonCoordinatorSites.length; i++) {
                m_nonCoordinatorSites[i] = buf.getLong();
            }
        }

        m_invocation = new StoredProcedureInvocation();
        m_invocation.initFromBuffer(buf);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("INITITATE_TASK (FROM ");
        sb.append(CoreUtils.hsIdToString(getInitiatorHSId()));
        sb.append(" TO ");
        sb.append(CoreUtils.hsIdToString(getCoordinatorHSId()));
        sb.append(") FOR TXN ");
        sb.append(TxnEgo.txnIdToString(m_txnId));

        sb.append("\n");
        if (m_isReadOnly)
            sb.append("  READ, ");
        else
            sb.append("  WRITE, ");
        if (m_isSinglePartition)
            sb.append("SINGLE PARTITION, ");
        else
            sb.append("MULTI PARTITION, ");
        sb.append("COORD ");
        sb.append(CoreUtils.hsIdToString(getCoordinatorHSId()));

        sb.append("\n  PROCEDURE: ");
        sb.append(m_invocation.getProcName());
        sb.append("\n  PARAMS: ");
        sb.append(m_invocation.getParams().toString());

        return sb.toString();
    }

    public ByteBuffer getSerializedParams() {
        return m_invocation.getSerializedParams();
    }
}
