package org.voltdb.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.StoredProcedureInvocation;

public class Iv2InitiateTaskMessage extends InitiateTaskMessage
{
    long m_clientInterfaceHandle;

    public Iv2InitiateTaskMessage(long clientInterfaceHSId,
                        long primaryInitiatorHSId,
                        long txnId,
                        boolean isReadOnly,
                        boolean isSinglePartition,
                        StoredProcedureInvocation invocation,
                        long clientInterfaceHandle)
    {
        super(clientInterfaceHSId, primaryInitiatorHSId, txnId, isReadOnly,
              isSinglePartition, invocation, Long.MAX_VALUE);
        m_clientInterfaceHandle = clientInterfaceHandle;
    }

    // Needed for deserialization
    public Iv2InitiateTaskMessage()
    {
    }

    public long getClientInterfaceHandle()
    {
        return m_clientInterfaceHandle;
    }

    @Override
    public int getSerializedSize()
    {
        int msgsize = super.getSerializedSize();
        msgsize += 8; // m_clientHandle
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        buf.put(VoltDbMessageFactory.IV2_INITIATE_TASK_ID);
        super.flattenToBuffer(buf);

        buf.putLong(m_clientInterfaceHandle);
        assert(buf.capacity() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromBuffer(ByteBuffer buf) throws IOException
    {
        super.initFromBuffer(buf);

        m_clientInterfaceHandle = buf.getLong();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IV2 INITIATE TASK: ");
        sb.append("  CLIENT HANDLE: ").append(m_clientInterfaceHandle).append("\n");
        sb.append(super.toString());

        return sb.toString();
    }
}
