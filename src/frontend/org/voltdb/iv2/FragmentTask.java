package org.voltdb.iv2;

import org.voltcore.logging.VoltLogger;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.messaging.FragmentTaskMessage;

public class FragmentTask extends SiteTasker
{
    private static final VoltLogger execLog = new VoltLogger("EXEC");
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    final Iv2MPTransactionState m_txn;
    final InitiatorMailbox m_initiator;

    FragmentTask(InitiatorMailbox initiator, FragmentTaskMessage message)
    {
        m_initiator = initiator;
        // IZZY: work out right transaction ID?
        m_txn = new Iv2MPTransactionState(message.getTxnId(), message);
    }

    @Override
    public void run(SiteProcedureConnection siteConnection)
    {
    }

    @Override
    public int priority()
    {
        return 0;
    }

}
