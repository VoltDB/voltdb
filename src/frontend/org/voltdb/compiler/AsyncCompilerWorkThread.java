/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.debugstate.PlannerThreadContext;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.DumpManager;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.VoltLoggerFactory;

public class AsyncCompilerWorkThread extends Thread implements DumpManager.Dumpable {

    LinkedBlockingQueue<AsyncCompilerWork> m_work = new LinkedBlockingQueue<AsyncCompilerWork>();
    final ArrayDeque<AsyncCompilerResult> m_finished = new ArrayDeque<AsyncCompilerResult>();
    //HSQLInterface m_hsql;
    PlannerTool m_ptool;
    int counter = 0;
    final int m_siteId;
    boolean m_isLoaded = false;
    CatalogContext m_context;

    private static final Logger ahpLog = Logger.getLogger("ADHOCPLANNERTHREAD", VoltLoggerFactory.instance());

    /** If this is true, update the catalog */
    private final AtomicBoolean m_shouldUpdateCatalog = new AtomicBoolean(false);

    // store the id used by the DumpManager to identify this execution site
    final String m_dumpId;
    long m_currentDumpTimestamp = 0;

    public AsyncCompilerWorkThread(CatalogContext context, int siteId) {
        m_ptool = null;
        //m_hsql = null;
        m_siteId = siteId;
        m_context = context;

        setName("Ad Hoc Planner");

        m_dumpId = "AdHocPlannerThread." + String.valueOf(m_siteId);
        DumpManager.register(m_dumpId, this);
    }

    public synchronized void ensureLoadedPlanner() {
        // if the process was created but is dead, clear the placeholder
        if ((m_ptool != null) && (m_ptool.expensiveIsRunningCheck() == false)) {
            ahpLog.error("Planner process died on its own. It will be restarted if needed.");
            m_ptool = null;
        }
        // if no placeholder, create a new plannertool
        if (m_ptool == null) {
            m_ptool = PlannerTool.createPlannerToolProcess(m_context.catalog.serialize());
        }
    }

    public void verifyEverthingIsKosher() {
        if (m_ptool != null) {
            // check if the planner process has been blocked for 2 seconds
            if (m_ptool.perhapsIsHung(5000)) {
                ahpLog.error("Was forced to kill the planner process due to a timeout. It will be restarted if needed.");
                m_ptool.kill();
            }
        }
    }

    public void shutdown() {
        AdHocPlannerWork work = new AdHocPlannerWork();
        work.shouldShutdown = true;
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
     * @param clientHandle Handle provided by the client application (not ClientInterface)
     * @param connectionId
     * @param hostname Hostname of the other end of the connection
     * @param sequenceNumber
     * @param clientData Data supplied by ClientInterface (typically a VoltPort) that will be in the PlannedStmt produced later.
     */
    public void planSQL(
            String sql,
            long clientHandle,
            long connectionId,
            String hostname,
            int sequenceNumber,
            Object clientData) {

        AdHocPlannerWork work = new AdHocPlannerWork();
        work.clientHandle = clientHandle;
        work.sql = sql;
        work.connectionId = connectionId;
        work.hostname = hostname;
        work.sequenceNumber = sequenceNumber;
        work.clientData = clientData;
        m_work.add(work);
    }

    public void prepareCatalogUpdate(
            String catalogURL,
            long clientHandle,
            long connectionId,
            String hostname,
            int sequenceNumber,
            Object clientData) {
        CatalogChangeWork work = new CatalogChangeWork();
        work.clientHandle = clientHandle;
        work.connectionId = connectionId;
        work.hostname = hostname;
        work.sequenceNumber = sequenceNumber;
        work.clientData = clientData;
        work.catalogURL = catalogURL;
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
            // handle a dump if needed
            if (work.shouldDump == true) {
                DumpManager.putDump(m_dumpId, m_currentDumpTimestamp, true, getDumpContents());
            }
            else {
                // deal with reloading the global catalog
                if (m_shouldUpdateCatalog.compareAndSet(true, false)) {
                    m_context = VoltDB.instance().getCatalogContext();
                    // kill the planner process which has an outdated catalog
                    // it will get created again for the next stmt
                    if (m_ptool != null) {
                        m_ptool.kill();
                        m_ptool = null;
                    }
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
            }

            try {
                work = m_work.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (m_ptool != null)
            m_ptool.kill();
    }

    public void notifyShouldUpdateCatalog() {
        m_shouldUpdateCatalog.set(true);
    }

    @Override
    public void goDumpYourself(long timestamp) {
        m_currentDumpTimestamp = timestamp;
        AdHocPlannerWork work = new AdHocPlannerWork();
        work.shouldDump = true;
        m_work.add(work);

        DumpManager.putDump(m_dumpId, timestamp, false, getDumpContents());
    }

    /**
     * Get the actual file contents for a dump of state reachable by
     * this thread. Can be called unsafely or safely.
     */
    public PlannerThreadContext getDumpContents() {
        PlannerThreadContext context = new PlannerThreadContext();
        context.siteId = m_siteId;

        // doing this with arraylists before arrays seems more stable
        // if the contents change while iterating

        ArrayList<AsyncCompilerWork> toplan = new ArrayList<AsyncCompilerWork>();
        ArrayList<AsyncCompilerResult> planned = new ArrayList<AsyncCompilerResult>();

        for (AsyncCompilerWork work : m_work)
            toplan.add(work);
        for (AsyncCompilerResult stmt : m_finished)
            planned.add(stmt);

        context.compilerWork = new AsyncCompilerWork[toplan.size()];
        for (int i = 0; i < toplan.size(); i++)
            context.compilerWork[i] = toplan.get(i);

        context.compilerResults = new AsyncCompilerResult[planned.size()];
        for (int i = 0; i < planned.size(); i++)
            context.compilerResults[i] = planned.get(i);

        return context;
    }

    private AsyncCompilerResult compileAdHocPlan(AdHocPlannerWork work) {
        AdHocPlannedStmt plannedStmt = new AdHocPlannedStmt();
        plannedStmt.clientHandle = work.clientHandle;
        plannedStmt.connectionId = work.connectionId;
        plannedStmt.hostname = work.hostname;
        plannedStmt.clientData = work.clientData;

        try {
            ensureLoadedPlanner();

            PlannerTool.Result result = m_ptool.planSql(work.sql);

            plannedStmt.aggregatorFragment = result.onePlan;
            plannedStmt.collectorFragment = result.allPlan;

            plannedStmt.isReplicatedTableDML = result.replicatedDML;
            plannedStmt.sql = work.sql;
            plannedStmt.errorMsg = result.errors;
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
        retval.hostname = work.hostname;

        // catalog change specific boiler plate
        retval.catalogURL = work.catalogURL;

        // get the diff between catalogs
        try {
            // try to get the new catalog from the params
            String newCatalogCommands = CatalogUtil.loadCatalogFromJar(work.catalogURL, null);
            if (newCatalogCommands == null) {
                retval.errorMsg = "Unable to read from catalog at: " + work.catalogURL;
                return retval;
            }
            Catalog newCatalog = new Catalog();
            newCatalog.execute(newCatalogCommands);

            // get the current catalog
            CatalogContext context = VoltDB.instance().getCatalogContext();
            // store the version of the catalog the diffs were created against
            retval.expectedCatalogVersion = context.catalog.getSubTreeVersion();

            // compute the diff
            String diffCommands = CatalogDiffEngine.getCommandsToDiff(context.catalog, newCatalog);

            if (verifyCatalogChangesAreAllowed(diffCommands) == false)
                throw new Exception("The requested catalog change is not a supported change at this time.");

            // since diff commands can be stupidly big, compress them here
            retval.encodedDiffCommands = Encoder.compressAndBase64Encode(diffCommands);
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

    private boolean verifyCatalogChangesAreAllowed(String diffCommands) {
        String[] allowedCommandPrefixes = { "set /clusters[cluster]/databases[database]/users[",
                                            "set /clusters[cluster]/databases[database]/groups[",
                                            "set /clusters[cluster]/databases[database]/procedures[",
                                            "add /clusters[cluster]/databases[database] users ",
                                            "add /clusters[cluster]/databases[database] groups",
                                            "add /clusters[cluster]/databases[database] procedures ",
                                            "delete /clusters[cluster]/databases[database] users ",
                                            "delete /clusters[cluster]/databases[database] groups ",
                                            "delete /clusters[cluster]/databases[database] procedures ",
                                            "add /clusters[cluster]/databases[database]/users[",
                                            "add /clusters[cluster]/databases[database]/groups[",
                                            "add /clusters[cluster]/databases[database]/procedures[",
                                            "delete /clusters[cluster]/databases[database]/users[",
                                            "delete /clusters[cluster]/databases[database]/groups[",
                                            "delete /clusters[cluster]/databases[database]/procedures[" };

        for (String command : diffCommands.split("\r\n|\r|\n")) {
            command = command.trim();
            if (command.length() == 0)
                continue;
            boolean foundMatch = false;

            for (String prefix : allowedCommandPrefixes) {
                if (command.startsWith(prefix))
                    foundMatch = true;
            }

            if (foundMatch == false)
                return false;
        }

        return true;
    }
}
