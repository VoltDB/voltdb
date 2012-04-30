package org.voltdb.iv2;

import org.voltcore.messaging.HostMessenger;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.Iv2ExecutionSite;
import org.voltdb.dtxn.SiteTracker;

public class Site
{
    // External references/config
    private HostMessenger m_messenger = null;
    private int m_partitionId;

    // Encapsulated objects
    private InitiatorMailbox m_initiatorMailbox = null;
    private Iv2ExecutionSite m_executionSite = null;
    private SiteTaskerScheduler m_scheduler = null;

    private Thread m_siteThread = null;

    public Site(HostMessenger messenger, Integer partition)
    {
        m_messenger = messenger;
        m_partitionId = partition;
        m_scheduler = new SiteTaskerScheduler();
        m_initiatorMailbox = new InitiatorMailbox(m_scheduler, m_messenger, m_partitionId);
        m_messenger.createMailbox(null, m_initiatorMailbox);
    }

    public void configure(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          SiteTracker siteTracker)
    {
        m_executionSite = new Iv2ExecutionSite(m_scheduler,
                                               m_initiatorMailbox.getHSId(),
                                               backend, catalogContext,
                                               serializedCatalog,
                                               catalogContext.m_transactionId,
                                               m_partitionId,
                                               siteTracker.m_numberOfPartitions);
        m_siteThread = new Thread(m_executionSite);
        m_siteThread.start(); // Maybe this moves --izzy
    }

    public long getInitiatorHSId()
    {
        return m_initiatorMailbox.getHSId();
    }
}
