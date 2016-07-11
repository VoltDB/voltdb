/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ProcedureRunnerFactory;
import org.voltdb.StarvationTracker;

/**
 * Provide a pool of MP sites to do MP work.
 * This should be owned by the MpTransactionTaskQueue and expects all operations
 * to be done while holding its lock.
 */
class MpSitePool {
    final static VoltLogger tmLog = new VoltLogger("TM");

    static int DEFAULT_MAX_POOL_SIZE = 20;
    static int INITIAL_POOL_SIZE = 1;

    class MpSiteContext {
        final private BackendTarget m_backend;
        final private SiteTaskerQueue m_queue;
        final private MpSite m_site;
        final private CatalogContext m_catalogContext;
        final private ProcedureRunnerFactory m_prf;
        final private LoadedProcedureSet m_loadedProcedures;
        final private Thread m_siteThread;

        MpSiteContext(long siteId, BackendTarget backend,
                CatalogContext context, int partitionId,
                InitiatorMailbox initiatorMailbox, CatalogSpecificPlanner csp,
                ThreadFactory threadFactory)
        {
            m_backend = backend;
            m_catalogContext = context;
            m_queue = new SiteTaskerQueue();
            // IZZY: Just need something non-null for now
            m_queue.setStarvationTracker(new StarvationTracker(siteId));
            m_site = new MpSite(m_queue, siteId, backend, m_catalogContext, partitionId);
            m_prf = new ProcedureRunnerFactory();
            m_prf.configure(m_site, m_site.m_sysprocContext);
            m_loadedProcedures = new LoadedProcedureSet(m_site, m_prf,
                    initiatorMailbox.getHSId(), 0); // Stale constructor arg, fill with bleh
            m_loadedProcedures.loadProcedures(m_catalogContext, m_backend, csp);
            m_site.setLoadedProcedures(m_loadedProcedures);
            m_siteThread = threadFactory.newThread(m_site);
            m_siteThread.start();
        }

        boolean offer(SiteTasker task) {
            return m_queue.offer(task);
        }

        long getCatalogCRC() {
            return m_catalogContext.getCatalogCRC();
        }

        long getCatalogVersion() {
            return m_catalogContext.catalogVersion;
        }

        void shutdown() {
            m_site.startShutdown();
            // Need to unblock the site's run() loop on the take() call on the queue
            m_queue.offer(Scheduler.m_nullTask);
        }

        void joinThread() {
            try {
                m_siteThread.join();
            } catch (InterruptedException e) {
            }
        }
    }

    // Stack of idle MpRoSites
    private Deque<MpSiteContext> m_idleSites = new ArrayDeque<MpSiteContext>();
    // Active sites, hashed by the txnID they're working on
    private Map<Long, MpSiteContext> m_busySites = new HashMap<Long, MpSiteContext>();

    // Stuff we need to construct new MpRoSites
    private final long m_siteId;
    private final BackendTarget m_backend;
    private final int m_partitionId;
    private final InitiatorMailbox m_initiatorMailbox;
    private CatalogContext m_catalogContext;
    private CatalogSpecificPlanner m_csp;
    private ThreadFactory m_poolThreadFactory;
    private final int m_poolSize;

    MpSitePool(
            long siteId,
            BackendTarget backend,
            CatalogContext context,
            int partitionId,
            InitiatorMailbox initiatorMailbox,
            CatalogSpecificPlanner csp)
    {
        m_siteId = siteId;
        m_backend = backend;
        m_catalogContext = context;
        m_partitionId = partitionId;
        m_initiatorMailbox = initiatorMailbox;
        m_csp = csp;
        m_poolThreadFactory =
            CoreUtils.getThreadFactory("RO MP Site - " + CoreUtils.hsIdToString(m_siteId),
                    CoreUtils.MEDIUM_STACK_SIZE);

        Integer poolSize = Integer.getInteger("mpiReadPoolSize");
        if (poolSize == null) {
            poolSize = DEFAULT_MAX_POOL_SIZE;
        }
        m_poolSize = poolSize;
        tmLog.info("Setting maximum size of MPI read pool to: " + m_poolSize);

        // Construct the initial pool
        for (int i = 0; i < INITIAL_POOL_SIZE; i++) {
            m_idleSites.push(new MpSiteContext(m_siteId,
                        m_backend,
                        m_catalogContext,
                        m_partitionId,
                        m_initiatorMailbox,
                        m_csp,
                        m_poolThreadFactory));
        }

    }

    /**
     * Update the catalog
     */
    void updateCatalog(String diffCmds, CatalogContext context, CatalogSpecificPlanner csp)
    {
        m_catalogContext = context;
        m_csp = csp;
        // Wipe out all the idle sites with stale catalogs.
        // Non-idle sites will get killed and replaced when they finish
        // whatever they started before the catalog update
        Iterator<MpSiteContext> siterator = m_idleSites.iterator();
        while (siterator.hasNext()) {
            MpSiteContext site = siterator.next();
            if (site.getCatalogCRC() != m_catalogContext.getCatalogCRC()
                    || site.getCatalogVersion() != m_catalogContext.catalogVersion) {
                site.shutdown();
                m_idleSites.remove(site);
            }
        }
    }

    /**
     * Repair: Submit the provided task to the MpRoSite running the transaction associated
     * with txnId.  This occurs when the MPI has survived a node failure and needs to interrupt and
     * re-run the current MP transaction; this task is used to run the repair algorithm in the site thread.
     */
    void repair(long txnId, SiteTasker task)
    {
        if (m_busySites.containsKey(txnId)) {
            MpSiteContext site = m_busySites.get(txnId);
            site.offer(task);
        }
        else {
            // Should be impossible
            throw new RuntimeException("MPI repair attempted to repair transaction: " + txnId);
        }
    }

    /**
     * Is there a RO site available to do MP RO work?
     */
    boolean canAcceptWork()
    {
        boolean retval = (!m_idleSites.isEmpty() || m_busySites.size() < m_poolSize);
        return retval;
    }

    /**
     * Attempt to start the transaction represented by the given task.  Need the txn ID for future reference.
     * @return true if work was started successfully, false if not.
     */
    boolean doWork(long txnId, TransactionTask task)
    {
        boolean retval = canAcceptWork();
        if (!retval) {
            return false;
        }
        MpSiteContext site;
        // Repair case
        if (m_busySites.containsKey(txnId)) {
            site = m_busySites.get(txnId);
        }
        else {
            if (m_idleSites.isEmpty()) {
                m_idleSites.push(new MpSiteContext(m_siteId,
                            m_backend,
                            m_catalogContext,
                            m_partitionId,
                            m_initiatorMailbox,
                            m_csp,
                            m_poolThreadFactory));
            }
            site = m_idleSites.pop();
            m_busySites.put(txnId, site);
        }
        site.offer(task);
        return true;
    }

    /**
     * Inform the pool that the work associated with the given txnID is complete
     */
    void completeWork(long txnId)
    {
        MpSiteContext site = m_busySites.remove(txnId);
        if (site == null) {
            throw new RuntimeException("No busy site for txnID: " + txnId + " found, shouldn't happen.");
        }
        // check the catalog versions, only push back onto idle if the catalog hasn't changed
        // otherwise, just let it get garbage collected and let doWork() construct new ones for the
        // pool with the updated catalog.
        if (site.getCatalogCRC() == m_catalogContext.getCatalogCRC()
                && site.getCatalogVersion() == m_catalogContext.catalogVersion) {
            m_idleSites.push(site);
        }
        else {
            site.shutdown();
        }
    }

    void shutdown()
    {
        // Shutdown all, then join all, hopefully save some shutdown time for tests.
        for (MpSiteContext site : m_idleSites) {
            site.shutdown();
        }
        for (MpSiteContext site : m_busySites.values()) {
            site.shutdown();
        }
        for (MpSiteContext site : m_idleSites) {
            site.joinThread();
        }
        for (MpSiteContext site : m_busySites.values()) {
            site.joinThread();
        }
    }
}
