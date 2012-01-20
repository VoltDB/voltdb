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

package org.voltdb.compiler;

import java.util.ArrayDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

public class AsyncCompilerWorkThread extends Thread {

    // if more than this amount of work is queued, backpressure
    static final int MAX_QUEUE_DEPTH = 250;

    LinkedBlockingQueue<AsyncCompilerWork> m_work = new LinkedBlockingQueue<AsyncCompilerWork>();
    final ArrayDeque<AsyncCompilerResult> m_finished = new ArrayDeque<AsyncCompilerResult>();
    PlannerTool m_ptool;
    int counter = 0;
    final int m_siteId;
    boolean m_isLoaded = false;
    CatalogContext m_context;

    public static int m_OOPTimeout = 60000;

    private static final VoltLogger ahpLog = new VoltLogger("ADHOCPLANNERTHREAD");

    /** If this is true, update the catalog */
    private final AtomicBoolean m_shouldUpdateCatalog = new AtomicBoolean(false);

    public AsyncCompilerWorkThread(CatalogContext context, int siteId) {
        m_ptool = null;
        m_siteId = siteId;
        m_context = context;

        setName("Ad Hoc Planner");
    }

    public void shutdown() {
        AdHocPlannerWork work = AdHocPlannerWork.forShutdown();
        m_work.add(work);
    }

    /**
     * Set the flag that tells this thread to update its
     * catalog when it's threadsafe.
     */
    public void notifyOfCatalogUpdate() {
        m_shouldUpdateCatalog.set(true);
    }

    /**
     *
     * @param sql
     * @param partitionInfo Standard partition info string for single-partition procs.
     *                      NULL if multi-partition
     * @param clientHandle Handle provided by the client application (not ClientInterface)
     * @param connectionId
     * @param hostname Hostname of the other end of the connection
     * @param adminConnection Did this invocation arrive on an admin port?
     * @param clientData Data supplied by ClientInterface (typically a VoltPort) that will be in the PlannedStmt produced later.
     */
    public boolean planSQL(
            String sql,
            Object partitionParam,
            long clientHandle,
            long connectionId,
            String hostname,
            boolean adminConnection,
            Object clientData) {

        AdHocPlannerWork work = new AdHocPlannerWork(
                false, // shouldShutdown
                clientHandle,
                connectionId,
                hostname,
                adminConnection,
                clientData,
                sql,
                partitionParam
                );

        if (m_work.size() > MAX_QUEUE_DEPTH) {
            return false;
        }
        m_work.add(work);
        return true;
    }

    public void prepareCatalogUpdate(
            byte[] catalogBytes,
            String deploymentString,
            long clientHandle,
            long connectionId,
            String hostname,
            boolean adminConnection,
            int sequenceNumber,
            Object clientData) {
        CatalogChangeWork work = new CatalogChangeWork(
                clientHandle, connectionId, hostname, adminConnection,
                clientData, catalogBytes, deploymentString);
        m_work.add(work);
    }

    public AsyncCompilerResult getPlannedStmt() {
        synchronized (m_finished) {
            return m_finished.poll();
        }
    }

    @Override
    public void run() {
        AsyncCompilerWork work = null;
        try {
            work = m_work.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (work.shouldShutdown == false) {
            // deal with reloading the global catalog
            if (m_shouldUpdateCatalog.compareAndSet(true, false)) {
                m_context = VoltDB.instance().getCatalogContext();
                // kill the planner process which has an outdated catalog
                // it will get created again for the next stmt
                if (m_ptool != null)
                    m_ptool.shutdown(); // cleans up hsql
                m_ptool = null;
            }

            AsyncCompilerResult result = null;
            if (work instanceof AdHocPlannerWork)
                result = compileAdHocPlan((AdHocPlannerWork) work);
            if (work instanceof CatalogChangeWork)
                result = prepareApplicationCatalogDiff((CatalogChangeWork) work);
            assert(result != null);

            synchronized (m_finished) {
                m_finished.add(result);
            }

            try {
                work = m_work.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (m_ptool != null) {
            m_ptool.shutdown(); // cleans up hsql
            m_ptool = null;
        }
    }

    public void notifyShouldUpdateCatalog() {
        m_shouldUpdateCatalog.set(true);
    }

    private AsyncCompilerResult compileAdHocPlan(AdHocPlannerWork work) {
        AdHocPlannedStmt plannedStmt = new AdHocPlannedStmt();
        plannedStmt.clientHandle = work.clientHandle;
        plannedStmt.connectionId = work.connectionId;
        plannedStmt.hostname = work.hostname;
        plannedStmt.adminConnection = work.adminConnection;
        plannedStmt.clientData = work.clientData;
        // record the catalog version the query is planned against to
        // catch races vs. updateApplicationCatalog.
        plannedStmt.catalogVersion = m_context.catalogVersion;

        try {
            if (m_ptool == null)
                m_ptool = new PlannerTool(m_context);

            PlannerTool.Result result = m_ptool.planSql(work.sql, work.partitionParam != null);

            plannedStmt.aggregatorFragment = result.onePlan;
            plannedStmt.collectorFragment = result.allPlan;

            plannedStmt.isReplicatedTableDML = result.replicatedDML;
            plannedStmt.sql = work.sql;
            plannedStmt.partitionParam = work.partitionParam;
        }
        catch (Exception e) {
            plannedStmt.errorMsg = "Unexpected Ad Hoc Planning Error: " + e.getMessage();
        }

        return plannedStmt;
    }

    private AsyncCompilerResult prepareApplicationCatalogDiff(CatalogChangeWork work) {
        // create the change result and set up all the boiler plate
        CatalogChangeResult retval = new CatalogChangeResult();
        retval.clientData = work.clientData;
        retval.clientHandle = work.clientHandle;
        retval.connectionId = work.connectionId;
        retval.adminConnection = work.adminConnection;
        retval.hostname = work.hostname;

        // catalog change specific boiler plate
        retval.catalogBytes = work.catalogBytes;
        retval.deploymentString = work.deploymentString;

        // get the diff between catalogs
        try {
            // try to get the new catalog from the params
            String newCatalogCommands = CatalogUtil.loadCatalogFromJar(work.catalogBytes, null);
            if (newCatalogCommands == null) {
                retval.errorMsg = "Unable to read from catalog bytes";
                return retval;
            }
            Catalog newCatalog = new Catalog();
            newCatalog.execute(newCatalogCommands);

            // If VoltProjectBuilder was used, work.deploymentURL will be null. No deployment.xml file was
            // given to the server in this case because its deployment info has already been added to the catalog.
            if (work.deploymentString != null) {
                retval.deploymentCRC =
                        CatalogUtil.compileDeploymentStringAndGetCRC(newCatalog, work.deploymentString, false);
                if (retval.deploymentCRC < 0) {
                    retval.errorMsg = "Unable to read from deployment file string";
                    return retval;
                }
            }

            // get the current catalog
            CatalogContext context = VoltDB.instance().getCatalogContext();

            // store the version of the catalog the diffs were created against.
            // verified when / if the update procedure runs in order to verify
            // catalogs only move forward
            retval.expectedCatalogVersion = context.catalogVersion;

            // compute the diff in StringBuilder
            CatalogDiffEngine diff = new CatalogDiffEngine(context.catalog, newCatalog);
            if (!diff.supported()) {
                throw new Exception("The requested catalog change is not a supported change at this time. " + diff.errors());
            }

            // since diff commands can be stupidly big, compress them here
            retval.encodedDiffCommands = Encoder.compressAndBase64Encode(diff.commands());
            // check if the resulting string is small enough to fit in our parameter sets (about 2mb)
            if (retval.encodedDiffCommands.length() > (2 * 1000 * 1000)) {
                throw new Exception("The requested catalog change is too large for this version of VoltDB. " +
                                    "Try a series of smaller updates.");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            retval.encodedDiffCommands = null;
            retval.errorMsg = e.getMessage();
        }

        return retval;
    }

}
