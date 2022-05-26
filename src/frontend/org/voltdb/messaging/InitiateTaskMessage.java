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

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.iv2.TxnEgo;


/**
 * Intermediate message used by CommandLogReinitiator to queue messages from the Generator to the Consumer
 *
 */
public class InitiateTaskMessage extends TransactionInfoBaseMessage {

    StoredProcedureInvocation m_invocation;

    /** Empty constructor for de-serialization */
    InitiateTaskMessage() {
        super();
    }

    public InitiateTaskMessage(long initiatorHSId,
                               long txnId,
                               StoredProcedureInvocation invocation) {
        super(initiatorHSId,
              0,
              txnId,
              txnId,
              false,
              false);
        m_invocation = invocation;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean isSinglePartition() {
        return false;
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
        return m_invocation.getParams().toArray().length;
    }

    public Object[] getParameters() {
        return m_invocation.getParams().toArray();
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();

        msgsize += m_invocation.getSerializedSize();

        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.INITIATE_TASK_ID);
        super.flattenToBuffer(buf);

        m_invocation.flattenToBuffer(buf);

        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException {
        super.initFromBuffer(buf);

        m_invocation = new StoredProcedureInvocation();
        m_invocation.initFromBuffer(buf);
    }

    @Override
    public void toDuplicateCounterString(StringBuilder sb) {
        sb.append("UNEXPECTED IV1 INITIATE TASK");
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

        sb.append("\n  PROCEDURE: ");
        sb.append(m_invocation.getProcName());
        sb.append("\n  PRIORITY: ");
        sb.append(m_invocation.getRequestPriority());
        sb.append("\n  PARAMS: ");
        sb.append(m_invocation.getParams().toString());

        return sb.toString();
    }

    public ByteBuffer getSerializedParams() {
        return m_invocation.getSerializedParams();
    }
}
