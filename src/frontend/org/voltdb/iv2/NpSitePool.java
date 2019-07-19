/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.StarvationTracker;

/**
 * Provide a pool of NP sites to do MP or NP work
 * This should be owned by the MpTransactionTaskQueue and expects all operations
 * to be done while holding its lock.
 */
class NpSitePool {
    final static VoltLogger tmLog = new VoltLogger("TM");

    // TODO: adjust size for allow more parallelism
    static final int MAX_POOL_SIZE = Integer.getInteger("MPI_READ_POOL_SIZE", 3);
    static final int INITIAL_POOL_SIZE = 1;

    class NpSiteContext {
        final private SiteTaskerQueue m_queue;
        final private NpSite m_site;
        final private CatalogContext m_catalogContext;
        final private LoadedProcedureSet m_loadedProcedures;
        final private Thread m_siteThread;

        NpSiteContext(long siteId, BackendTarget backend,
                      CatalogContext context, int partitionId,
                      InitiatorMailbox initiatorMailbox,
                      ThreadFactory threadFactory) {
            m_catalogContext = context;
            m_queue = new SiteTaskerQueue(partitionId);
            // IZZY: Just need something non-null for now
            m_queue.setStarvationTracker(new StarvationTracker(siteId));
            m_queue.setupQueueDepthTracker(siteId);
            m_site = new NpSite(m_queue, siteId, backend, m_catalogContext, partitionId);
            m_loadedProcedures = new LoadedProcedureSet(m_site);
            m_loadedProcedures.loadProcedures(m_catalogContext);
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

    // Stack of idle NpSites
    private Deque<NpSiteContext> m_idleSites = new ArrayDeque<>();
    // Active sites, hashed by the txnID they're working on
    private Map<Long, NpSiteContext> m_busySites = new HashMap<>();

    //The reference for all sites, used for shutdown
    private final List<NpSiteContext> m_allSites = Collections.synchronizedList(new ArrayList<>());

    // Stuff we need to construct new NpSites
    private final long m_siteId;
    private final BackendTarget m_backend;
    private final int m_partitionId;
    private final InitiatorMailbox m_initiatorMailbox;
    private CatalogContext m_catalogContext;
    private ThreadFactory m_poolThreadFactory;
    private volatile boolean m_shuttingDown = false;

    NpSitePool(
            long siteId,
            BackendTarget backend,
            CatalogContext context,
            int partitionId,
            InitiatorMailbox initiatorMailbox) {
        m_siteId = siteId;
        m_backend = backend;
        m_catalogContext = context;
        m_partitionId = partitionId;
        m_initiatorMailbox = initiatorMailbox;
        m_poolThreadFactory =
                CoreUtils.getThreadFactory("Np Site - " + CoreUtils.hsIdToString(m_siteId),
                        CoreUtils.MEDIUM_STACK_SIZE);

        tmLog.info("Setting maximum size of MPI read pool to: " + MAX_POOL_SIZE);

        // Construct the initial pool
        for (int i = 0; i < INITIAL_POOL_SIZE; i++) {
            NpSiteContext site = new NpSiteContext(m_siteId,
                    m_backend,
                    m_catalogContext,
                    m_partitionId,
                    m_initiatorMailbox,
                    m_poolThreadFactory);
            m_idleSites.push(site);
            m_allSites.add(site);
        }
    }

    /**
     * Update the catalog
     */
    void updateCatalog(String diffCmds, CatalogContext context) {
        if (m_shuttingDown) {
            return;
        }

        m_catalogContext = context;
        // Wipe out all the idle sites with stale catalogs.
        // Non-idle sites will get killed and replaced when they finish
        // whatever they started before the catalog update
        Iterator<NpSiteContext> siterator = m_idleSites.iterator();
        while (siterator.hasNext()) {
            NpSiteContext site = siterator.next();
            if (site.getCatalogCRC() != m_catalogContext.getCatalogCRC()
                    || site.getCatalogVersion() != m_catalogContext.catalogVersion) {
                site.shutdown();
                m_idleSites.remove(site);
                m_allSites.remove(site);
            }
        }
    }

    /**
     * update cluster settings
     */
    void updateSettings(CatalogContext context) {
        m_catalogContext = context;
    }

    /**
     * Repair: Submit the provided task to the NpSite running the transaction associated
     * with txnId.  This occurs when the MPI has survived a node failure and needs to interrupt and
     * re-run the current MP transaction; this task is used to run the repair algorithm in the site thread.
     */
    void repair(long txnId, SiteTasker task) {
        if (m_busySites.containsKey(txnId)) {
            NpSiteContext site = m_busySites.get(txnId);
            site.offer(task);
        } else {
            // Should be impossible
            // TODO: change to non-fatal
            throw new RuntimeException("MPI repair attempted to repair transaction: " + txnId);
        }
    }

    /**
     * Is there a NP site available to do work?
     */
    boolean canAcceptWork() {
        //lock down the pool and accept no more work upon shutting down.
        if (m_shuttingDown) {
            return false;
        }
        return (!m_idleSites.isEmpty() || m_busySites.size() < MAX_POOL_SIZE);
    }

    /**
     * Attempt to start the transaction represented by the given task.
     * Need the txn ID for future reference.
     *
     * @return true if work was started successfully, false if not.
     */
    boolean doWork(long txnId, TransactionTask task) {
        boolean retval = canAcceptWork();
        if (!retval) {
            return false;
        }
        NpSiteContext site;
        // Repair case
        if (m_busySites.containsKey(txnId)) {
            site = m_busySites.get(txnId);
        } else {
            if (m_idleSites.isEmpty()) {
                NpSiteContext newSite = new NpSiteContext(m_siteId,
                        m_backend,
                        m_catalogContext,
                        m_partitionId,
                        m_initiatorMailbox,
                        m_poolThreadFactory);
                m_idleSites.push(newSite);
                m_allSites.add(newSite);
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
    void completeWork(long txnId) {
        if (m_shuttingDown) {
            return;
        }

        NpSiteContext site = m_busySites.remove(txnId);
        if (site == null) {
            // TODO: change to non-fatal
            throw new RuntimeException("No busy site for txnID: " + txnId + " found, shouldn't happen.");
        }
        // check the catalog versions, only push back onto idle if the catalog hasn't changed
        // otherwise, just let it get garbage collected and let doWork() construct new ones for the
        // pool with the updated catalog.
        if (site.getCatalogCRC() == m_catalogContext.getCatalogCRC()
                && site.getCatalogVersion() == m_catalogContext.catalogVersion) {
            m_idleSites.push(site);
        } else {
            site.shutdown();
            m_allSites.remove(site);
        }
    }

    void shutdown() {
        m_shuttingDown = true;

        // Shutdown all, then join all, hopefully save some shutdown time for tests.
        synchronized (m_allSites) {
            for (NpSiteContext site : m_allSites) {
                site.shutdown();
            }

            for (NpSiteContext site : m_allSites) {
                site.joinThread();
            }
        }
    }
}
