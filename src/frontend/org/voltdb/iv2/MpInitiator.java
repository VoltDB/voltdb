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

package org.voltdb.iv2;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.zk.LeaderElector;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ProcedureRunnerFactory;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.iv2.Site;

public class MpInitiator implements Initiator
{
    // External references/config
    private HostMessenger m_messenger = null;
    private int m_partitionId;

    // Encapsulated objects
    private InitiatorMailbox m_initiatorMailbox = null;
    private Site m_executionSite = null;
    private Scheduler m_scheduler = null;
    private LoadedProcedureSet m_procSet = null;
    private InitiatorMessageHandler m_msgHandler = null;
    private Thread m_siteThread = null;

    public MpInitiator(HostMessenger messenger, PartitionClerk clerk)
    {
        m_messenger = messenger;
        m_partitionId = -1;
        m_scheduler = new MpScheduler(clerk);
        m_msgHandler = new MpInitiatorMessageHandler(m_scheduler);
        m_initiatorMailbox = new InitiatorMailbox(m_msgHandler, m_messenger, m_partitionId, clerk);
    }

    @Override
    public void configure(BackendTarget backend, String serializedCatalog,
                          CatalogContext catalogContext,
                          SiteTracker siteTracker)
    {
        m_executionSite = new Site(m_scheduler.getQueue(),
                                   m_initiatorMailbox.getHSId(),
                                   backend, catalogContext,
                                   serializedCatalog,
                                   catalogContext.m_transactionId,
                                   m_partitionId,
                                   siteTracker.m_numberOfPartitions);
        ProcedureRunnerFactory prf = new ProcedureRunnerFactory();
        prf.configure(m_executionSite,
                m_executionSite.m_sysprocContext);
        m_procSet = new LoadedProcedureSet(m_executionSite,
                                           prf,
                                           m_initiatorMailbox.getHSId(),
                                           0, // this has no meaning
                                           siteTracker.m_numberOfPartitions);
        m_procSet.loadProcedures(catalogContext, backend);
        m_scheduler.setProcedureSet(m_procSet);
        m_executionSite.setLoadedProcedures(m_procSet);


        m_siteThread = new Thread(m_executionSite);
        m_siteThread.start(); // Maybe this moves --izzy
    }

    @Override
    public void shutdown()
    {
        // rtb: better to schedule a shutdown SiteTasker?
        // than to play java interrupt() games?
        if (m_executionSite != null) {
            m_executionSite.startShutdown();
        }
        if (m_siteThread != null) {
            try {
                m_siteThread.interrupt();
                m_siteThread.join();
            }
            catch (InterruptedException giveup) {
            }
        }
    }

    @Override
    public long getInitiatorHSId()
    {
        return m_initiatorMailbox.getHSId();
    }
}
