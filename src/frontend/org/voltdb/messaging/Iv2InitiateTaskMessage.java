/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.StoredProcedureInvocation;


/**
 * Message from a client interface to an initiator, instructing the
 * site to begin executing a stored procedure, coordinating other
 * execution sites if needed.
 *
 */
public class Iv2InitiateTaskMessage extends TransactionInfoBaseMessage {

    // The default MP transaction id set by client interface when
    // initiating a single-partition transaction.
    public static final long UNUSED_MP_TXNID = Long.MIN_VALUE;

    // allow PI to signal RI repair log truncation with a new task.
    private long m_truncationHandle;
    long m_clientInterfaceHandle;
    boolean m_isSinglePartition;
    StoredProcedureInvocation m_invocation;

    // not serialized.
    AtomicBoolean m_isDurable;

    /** Empty constructor for de-serialization */
    Iv2InitiateTaskMessage() {
        super();
    }

    // MpScheduler creates msgs w/o truncation handles.
    public Iv2InitiateTaskMessage(long initiatorHSId,
                        long coordinatorHSId,
                        long txnId,
                        boolean isReadOnly,
                        boolean isSinglePartition,
                        StoredProcedureInvocation invocation,
                        long clientInterfaceHandle)
    {
        this(initiatorHSId, coordinatorHSId, Long.MIN_VALUE,
            txnId, isReadOnly, isSinglePartition, invocation,
            clientInterfaceHandle);
    }

    // SpScheduler creates messages with truncation handles.
    public Iv2InitiateTaskMessage(long initiatorHSId,
                        long coordinatorHSId,
                        long truncationHandle,
                        long txnId,
                        boolean isReadOnly,
                        boolean isSinglePartition,
                        StoredProcedureInvocation invocation,
                        long clientInterfaceHandle)
    {
        super(initiatorHSId, coordinatorHSId, txnId, isReadOnly);
        m_isSinglePartition = isSinglePartition;
        m_invocation = invocation;
        m_truncationHandle = truncationHandle;
        m_clientInterfaceHandle = clientInterfaceHandle;
    }

    /** Copy constructor for repair. */
    public Iv2InitiateTaskMessage(long initiatorHSId,
            long coordinatorHSId, Iv2InitiateTaskMessage rhs)
    {
        super(initiatorHSId, coordinatorHSId, rhs.m_txnId, rhs.isReadOnly());
        m_isSinglePartition = rhs.m_isSinglePartition;
        m_invocation = rhs.m_invocation;
        m_truncationHandle = rhs.m_truncationHandle;
        m_clientInterfaceHandle = rhs.m_clientInterfaceHandle;
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

    public long getClientInterfaceHandle() {
        return m_clientInterfaceHandle;
    }

    public AtomicBoolean getDurabilityFlag() {
        assert(!m_isReadOnly);
        if (m_isDurable == null) {
            m_isDurable = new AtomicBoolean();
        }
        return m_isDurable;
    }

    public long getTruncationHandle() {
        return m_truncationHandle;
    }

    public AtomicBoolean getDurabilityFlagIfItExists() {
        return m_isDurable;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8; // m_truncationHandle
        msgsize += 8; // m_clientInterfaceHandle
        msgsize += 1; // is single partition flag
        msgsize += m_invocation.getSerializedSize();
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_INITIATE_TASK_ID);
        super.flattenToBuffer(buf);
        buf.putLong(m_truncationHandle);
        buf.putLong(m_clientInterfaceHandle);
        buf.put(m_isSinglePartition ? (byte) 1 : (byte) 0);
        m_invocation.flattenToBuffer(buf);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        super.initFromBuffer(buf);

        m_truncationHandle = buf.getLong();
        m_clientInterfaceHandle = buf.getLong();
        m_isSinglePartition = buf.get() == 1;
        m_invocation = new StoredProcedureInvocation();
        m_invocation.initFromBuffer(buf);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("IV2 INITITATE_TASK (FROM ");
        sb.append(CoreUtils.hsIdToString(getInitiatorHSId()));
        sb.append(" TO ");
        sb.append(CoreUtils.hsIdToString(getCoordinatorHSId()));
        sb.append(") FOR TXN ");
        sb.append(m_txnId).append("\n");
        sb.append(") TRUNC HANDLE ");
        sb.append(m_truncationHandle).append("\n");
        sb.append("SP HANDLE: ").append(m_spHandle).append("\n");
        sb.append("CLIENT INTERFACE HANDLE: ").append(m_clientInterfaceHandle);
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
