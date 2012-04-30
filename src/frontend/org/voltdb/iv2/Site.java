package org.voltdb.iv2;

import org.voltcore.messaging.HostMessenger;

public class Site
{
    private HostMessenger m_messenger = null;
    private int m_partitionId;

    private InitiatorMailbox m_initiatorMailbox = null;

    public Site(HostMessenger messenger, Integer partition)
    {
        m_messenger = messenger;
        m_partitionId = partition;
        m_initiatorMailbox = new InitiatorMailbox(m_messenger, m_partitionId);
        m_messenger.createMailbox(null, m_initiatorMailbox);
    }

    public long getInitiatorHSId()
    {
        return m_initiatorMailbox.getHSId();
    }
}