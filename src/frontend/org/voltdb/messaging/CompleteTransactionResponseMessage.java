package org.voltdb.messaging;

import java.io.IOException;

import org.voltdb.utils.DBBPool;

public class CompleteTransactionResponseMessage extends VoltMessage
{
    int m_executionSiteId;
    long m_txnId;

    /** Empty constructor for de-serialization */
    CompleteTransactionResponseMessage() {
        super();
    }

    CompleteTransactionResponseMessage(CompleteTransactionMessage msg,
                                       int siteId)
    {
        m_executionSiteId = siteId;
        m_txnId = msg.getTxnId();
    }

    @Override
    protected void flattenToBuffer(DBBPool pool) throws IOException
    {
        int msgsize = 4 + 8;

        if (m_buffer == null) {
            m_container = pool.acquire(msgsize + 1 + HEADER_SIZE);
            m_buffer = m_container.b;
        }
        setBufferSize(msgsize + 1, pool);

        m_buffer.position(HEADER_SIZE);
        m_buffer.put(COMPLETE_TRANSACTION_RESPONSE_ID);

        m_buffer.putInt(m_executionSiteId);
        m_buffer.putLong(m_txnId);
        m_buffer.limit(m_buffer.position());
    }

    @Override
    protected void initFromBuffer()
    {
        m_buffer.position(HEADER_SIZE + 1); // skip the msg id

        m_executionSiteId = m_buffer.getInt();
        m_txnId = m_buffer.getLong();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("COMPLETE_TRANSACTION_RESPONSE");
        sb.append(" (FROM EXEC SITE: ");
        sb.append(m_executionSiteId);
        sb.append(") FOR TXN ID: ");
        sb.append(m_txnId);

        return sb.toString();
    }
}
