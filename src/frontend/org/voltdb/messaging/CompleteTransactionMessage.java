package org.voltdb.messaging;

import java.io.IOException;

import org.voltdb.utils.DBBPool;

public class CompleteTransactionMessage extends TransactionInfoBaseMessage
{
    boolean m_isRollback;
    boolean m_requiresAck;

    /** Empty constructor for de-serialization */
    CompleteTransactionMessage() {
        super();
    }

    /**
     * These four args needed for base class
     * @param initiatorSiteId
     * @param coordinatorSiteId
     * @param txnId
     * @param isReadOnly
     *
     * @param isRollback  Should the recipient rollback this transaction to complete it?
     * @param requiresAck  Does the recipient need to respond to this message
     *                     with a CompleteTransactionResponseMessage?
     */
    public CompleteTransactionMessage(int initiatorSiteId, int coordinatorSiteId,
                                      long txnId, boolean isReadOnly,
                                      boolean isRollback, boolean requiresAck)
    {
        super(initiatorSiteId, coordinatorSiteId, txnId, isReadOnly);
        m_isRollback = isRollback;
        m_requiresAck = requiresAck;
    }

    public boolean isRollback()
    {
        return m_isRollback;
    }

    public boolean requiresAck()
    {
        return m_requiresAck;
    }

    @Override
    protected void flattenToBuffer(DBBPool pool) throws IOException
    {
        int msgsize = super.getMessageByteCount();
        // Add the bytes for isRollback and requiresAck
        msgsize += 2;

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(COMPLETE_TRANSACTION_ID);

        super.writeToBuffer();

        m_buffer.put(m_isRollback ? (byte) 1 : (byte) 0);
        m_buffer.put(m_requiresAck ? (byte) 1 : (byte) 0);
        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer()
    {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id
        super.readFromBuffer();

        m_isRollback = m_buffer.get() == 1;
        m_requiresAck = m_buffer.get() == 1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("COMPLETE_TRANSACTION (FROM COORD: ");
        sb.append(m_coordinatorSiteId);
        sb.append(") FOR TXN ");
        sb.append(m_txnId);

        if (m_isRollback)
            sb.append("\n  THIS IS AN ROLLBACK REQUEST");

        if (m_requiresAck)
            sb.append("\n  THIS MESSAGE REQUIRES AN ACK");

        return sb.toString();
    }
}
