package org.voltdb.iv2;

import org.voltcore.messaging.HostMessenger;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.Iv2ExecutionSite;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ProcedureRunnerFactory;
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
    private LoadedProcedureSet m_procSet = null;

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
        ProcedureRunnerFactory prf = new ProcedureRunnerFactory();
        prf.configure(m_executionSite, null /* wtfhsql!? */);
        m_procSet = new LoadedProcedureSet(m_executionSite,
                                           prf,
                                           m_initiatorMailbox.getHSId(),
                                           0, // this has no meaning
                                           siteTracker.m_numberOfPartitions);
        m_procSet.loadProcedures(catalogContext, backend);
        m_initiatorMailbox.setProcedureSet(m_procSet);


        m_siteThread = new Thread(m_executionSite);
        m_siteThread.start(); // Maybe this moves --izzy
    }

    public long getInitiatorHSId()
    {
        return m_initiatorMailbox.getHSId();
    }
}
