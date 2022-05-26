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
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.iv2.UniqueIdGenerator;
import org.voltdb.sysprocs.AdHocBase;

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

    public static int SINGLE_PARTITION_MASK = 1;
    public static int N_PARTITION_MASK = 2;
    public static int SHOULD_RETURN_TABLES_MASK = 4;
    public static int EVERY_PARTITION_MASK = 8;

    long m_clientInterfaceHandle;
    long m_connectionId;
    boolean m_isSinglePartition;
    boolean m_isEveryPartition;
    int[] m_nPartitions;
    //Flag to indicate that the replica applying the write transaction
    //doesn't need to send back the result tables
    boolean m_shouldReturnResultTables = true;
    StoredProcedureInvocation m_invocation;

    // not serialized.
    AtomicBoolean m_isDurable;

    /** Empty constructor for de-serialization */
    public Iv2InitiateTaskMessage() {
        super();
    }

    public Iv2InitiateTaskMessage(long initiatorHSId,
            long coordinatorHSId,
            long truncationHandle,
            long txnId,
            long uniqueId,
            boolean isReadOnly,
            boolean isSinglePartition,
            boolean isEveryPartition,
            StoredProcedureInvocation invocation,
            long clientInterfaceHandle,
            long connectionId,
            boolean isForReplay)
    {
        this(initiatorHSId,
            coordinatorHSId,
            truncationHandle,
            txnId,
            uniqueId,
            isReadOnly,
            isSinglePartition,
            isEveryPartition,
            invocation,
            clientInterfaceHandle,
            connectionId,
            isForReplay,
            false);
    }

    // SpScheduler creates messages with truncation handles.
    public Iv2InitiateTaskMessage(long initiatorHSId,
                        long coordinatorHSId,
                        long truncationHandle,
                        long txnId,
                        long uniqueId,
                        boolean isReadOnly,
                        boolean isSinglePartition,
                        boolean isEveryPartition,
                        StoredProcedureInvocation invocation,
                        long clientInterfaceHandle,
                        long connectionId,
                        boolean isForReplay,
                        boolean toReplica) {
        super(initiatorHSId, coordinatorHSId, txnId, uniqueId, isReadOnly, isForReplay);
        setTruncationHandle(truncationHandle);
        m_isSinglePartition = isSinglePartition;
        m_isEveryPartition = isEveryPartition;
        m_invocation = invocation;
        m_clientInterfaceHandle = clientInterfaceHandle;
        m_connectionId = connectionId;
        m_isForReplica = toReplica;
    }

    // SpScheduler creates messages with truncation handles.
    public Iv2InitiateTaskMessage(long initiatorHSId,
            long coordinatorHSId,
            long truncationHandle,
            long txnId,
            long uniqueId,
            boolean isReadOnly,
            boolean isSinglePartition,
            boolean isEveryPartition,
            int[] nPartitions,
            StoredProcedureInvocation invocation,
            long clientInterfaceHandle,
            long connectionId,
            boolean isForReplay)
    {
        super(initiatorHSId, coordinatorHSId, txnId, uniqueId, isReadOnly, isForReplay);

        setTruncationHandle(truncationHandle);
        m_isSinglePartition = isSinglePartition;
        m_isEveryPartition = isEveryPartition;
        m_invocation = invocation;
        m_clientInterfaceHandle = clientInterfaceHandle;
        m_connectionId = connectionId;
        m_nPartitions = nPartitions;
    }

    /** Copy constructor for repair. */
    public Iv2InitiateTaskMessage(long initiatorHSId,
            long coordinatorHSId, Iv2InitiateTaskMessage rhs)
    {
        this(initiatorHSId, coordinatorHSId, rhs, false);
    }

    /** Copy constructor for repair. */
    public Iv2InitiateTaskMessage(long initiatorHSId,
            long coordinatorHSId, Iv2InitiateTaskMessage rhs, boolean toReplica)
    {
        super(initiatorHSId, coordinatorHSId, rhs);
        m_isSinglePartition = rhs.m_isSinglePartition;
        m_isEveryPartition = rhs.m_isEveryPartition;
        m_invocation = rhs.m_invocation;
        m_clientInterfaceHandle = rhs.m_clientInterfaceHandle;
        m_connectionId = rhs.m_connectionId;
        m_nPartitions = rhs.m_nPartitions;
        m_shouldReturnResultTables = false;
        m_isForReplica = toReplica;
    }

    @Override
    public boolean isReadOnly() {
        return m_isReadOnly;
    }

    @Override
    public boolean isSinglePartition() {
        return m_isSinglePartition;
    }

    public boolean isEveryPartition() {
        return m_isEveryPartition;
    }

    public short getNPartCount() {
        return (short)(m_nPartitions != null ? m_nPartitions.length : 0);
    }

    public boolean shouldReturnResultTables() {
        return m_shouldReturnResultTables;
    }

    public void setShouldReturnResultTables(boolean shouldReturnResultTables) {
        m_shouldReturnResultTables = shouldReturnResultTables;
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
        if (m_invocation.getParams() == null) {
            return 0;
        }
        return m_invocation.getParams().size();
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

    public AtomicBoolean getDurabilityFlagIfItExists() {
        return m_isDurable;
    }

    public long getConnectionId() {
        return m_connectionId;
    }

    public int[] getNPartitionIds() {
        return m_nPartitions;
    }


    public int getFixedHeaderSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8; // m_clientInterfaceHandle
        msgsize += 8; // m_connectionId
        msgsize += 1; // flags (SP/NP/return tables)
        if (m_nPartitions != null) {
            msgsize += 2 + m_nPartitions.length * 4; // 2 for length prefix and 4 each
        }
        return msgsize;
    }

    @Override
    public int getSerializedSize()
    {
        return getFixedHeaderSize() + m_invocation.getSerializedSize();
    }


    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        byte flags = 0;
        if (m_isSinglePartition) {
            flags |= SINGLE_PARTITION_MASK;
        }
        if (m_isEveryPartition) {
            flags |= EVERY_PARTITION_MASK;
        }
        if (m_shouldReturnResultTables) {
            flags |= SHOULD_RETURN_TABLES_MASK;
        }
        if (m_nPartitions != null) {
            flags |= N_PARTITION_MASK;
        }

        //Should never generate a response if we have to forward to a replica
        //if (m_shouldReturnResultTables) flags |= SHOULD_RETURN_TABLES_MASK;

        buf.put(VoltDbMessageFactory.IV2_INITIATE_TASK_ID);
        super.flattenToBuffer(buf);
        buf.putLong(m_clientInterfaceHandle);
        buf.putLong(m_connectionId);
        buf.put(flags);
        if (m_nPartitions != null) {
            buf.putShort((short) m_nPartitions.length);
            for (int i : m_nPartitions) {
                buf.putInt(i);
            }
        }
        m_invocation.flattenToBuffer(buf);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        super.initFromBuffer(buf);
        m_clientInterfaceHandle = buf.getLong();
        m_connectionId = buf.getLong();
        byte flags = buf.get();
        m_isSinglePartition = (flags & SINGLE_PARTITION_MASK) != 0;
        m_isEveryPartition = (flags & EVERY_PARTITION_MASK) != 0;
        m_shouldReturnResultTables = (flags & SHOULD_RETURN_TABLES_MASK) != 0;
        if ((flags & N_PARTITION_MASK) != 0) {
            int partitionCount = buf.getShort();
            m_nPartitions = new int[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                m_nPartitions[i] = buf.getInt();
            }
        }
        m_invocation = new StoredProcedureInvocation();
        m_invocation.initFromBuffer(buf);
    }

    // Use this version when it is possible for multiple threads to make a string from the invocation
    // at the same time (seems to only be an issue if the parameter to the procedure is a VoltTable)
    public void toShortString(StringBuilder sb) {
        sb.append("IV2 INITITATE_TASK (FROM ");
        sb.append(CoreUtils.hsIdToString(getInitiatorHSId()));
        sb.append(" TO ");
        sb.append(CoreUtils.hsIdToString(getCoordinatorHSId()));
        sb.append(") FOR TXN ").append(TxnEgo.txnIdToString(m_txnId));
        sb.append(" UNIQUE ID ").append(m_uniqueId).append(" (").append(UniqueIdGenerator.toString(m_uniqueId));
        sb.append(")").append("\n");
        sb.append(") TRUNC HANDLE ");
        sb.append(TxnEgo.txnIdToString(getTruncationHandle())).append("\n");
        sb.append("SP HANDLE: ").append(TxnEgo.txnIdToString(getSpHandle())).append("\n");
        sb.append("CLIENT INTERFACE HANDLE: ").append(m_clientInterfaceHandle);
        sb.append("\n");
        sb.append("CONNECTION ID: ").append(m_connectionId).append("\n");
        if (m_isReadOnly) {
            sb.append("  READ, ");
        } else {
            sb.append("  WRITE, ");
        }
        if (m_isSinglePartition) {
            sb.append("SINGLE PARTITION, ");
        } else
        if (getNPartCount() != 0) {
            sb.append("N PARTITION (").append(m_nPartitions).append("), ");
        } else {
            sb.append("MULTI PARTITION, ");
        }
        if (isForReplay()) {
            sb.append("FOR REPLAY, ");
        } else {
            sb.append("NOT REPLAY, ");
        }
        sb.append("COORD ");
        sb.append(CoreUtils.hsIdToString(getCoordinatorHSId()));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        toShortString(sb);

        if (m_invocation != null) {
            sb.append("\n  PROCEDURE: ");
            sb.append(m_invocation.getProcName());
            sb.append("\n  PARAMS: ");
            sb.append(m_invocation.getParams().toString());
            // print out extra information for adhoc
            if (m_invocation.getProcName().startsWith("@AdHoc")) {
                sb.append("\n  ADHOC INFO: ");
                sb.append(AdHocBase.adHocSQLFromInvocationForDebug(m_invocation));
            }

        } else {
            sb.append("\n NO INVOCATION");
        }

        return sb.toString();
    }

    public ByteBuffer getSerializedParams() {
        return m_invocation.getSerializedParams();
    }

    @Override
    public String getMessageInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("Iv2InitiateTaskMessage TxnId:" + TxnEgo.txnIdToString(m_txnId));
        if (m_invocation != null) {
            builder.append(" Procedure: ");
            builder.append(m_invocation.getProcName());
        }
        return builder.toString();
    }

    @Override
    public void toDuplicateCounterString(StringBuilder sb) {
        sb.append("INITIATE TASK");
    }
}
