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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.hsqldb.HSQLInterface;
import org.hsqldb.HSQLInterface.HSQLParseException;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.debugstate.PlannerThreadContext;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.planner.CompiledPlan.Fragment;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.DumpManager;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltLoggerFactory;

public class AsyncCompilerWorkThread extends Thread implements DumpManager.Dumpable {

    LinkedBlockingQueue<AsyncCompilerWork> m_work = new LinkedBlockingQueue<AsyncCompilerWork>();
    final ArrayDeque<AsyncCompilerResult> m_finished = new ArrayDeque<AsyncCompilerResult>();
    HSQLInterface m_hsql;
    int counter = 0;
    final int m_siteId;
    boolean m_isLoaded = false;
    CatalogContext m_context;

    /** If this is true, update the catalog */
    private final AtomicBoolean m_shouldUpdateCatalog = new AtomicBoolean(false);

    // store the id used by the DumpManager to identify this execution site
    final String m_dumpId;
    long m_currentDumpTimestamp = 0;

    private static final Logger ahpLog = Logger.getLogger("ADHOCPLANNERTHREAD", VoltLoggerFactory.instance());

    public AsyncCompilerWorkThread(CatalogContext context, int siteId) {
        m_hsql = null;
        m_siteId = siteId;
        m_context = context;

        setName("Ad Hoc Planner");

        m_dumpId = "AdHocPlannerThread." + String.valueOf(m_siteId);
        DumpManager.register(m_dumpId, this);
    }

    public synchronized void ensureLoadedHSQL() {
        if (m_isLoaded == true)
            return;
        m_hsql = HSQLInterface.loadHsqldb();

        String hexDDL = m_context.database.getSchema();
        String ddl = Encoder.hexDecodeToString(hexDDL);
        String[] commands = ddl.split(";");
        for (String command : commands) {
            command = command.trim();
            if (command.length() == 0)
                continue;
            try {
                m_hsql.runDDLCommand(command);
            } catch (HSQLParseException e) {
                ahpLog.l7dlog(Level.FATAL, LogKeys.host_Initialiazion_InvalidDDL.name(), e);
                VoltDB.crashVoltDB();
            }
        }
        m_isLoaded = true;
    }

    public void shutdown() {
        AdHocPlannerWork work = new AdHocPlannerWork();
        work.shouldShutdown = true;
        m_work.add(work);
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
            int connectionId,
            String hostname,
            int sequenceNumber,
            Object clientData) {
        ensureLoadedHSQL();

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
            int connectionId,
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
        if (m_hsql != null)
            m_hsql.close();
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
        TrivialCostModel costModel = new TrivialCostModel();
        CatalogContext context = VoltDB.instance().getCatalogContext();
        QueryPlanner planner =
            new QueryPlanner(context.cluster, context.database, m_hsql,
                             new DatabaseEstimates(), false,
                             VoltDB.getQuietAdhoc());
        CompiledPlan plan = null;
        AdHocPlannedStmt plannedStmt = new AdHocPlannedStmt();
        plannedStmt.clientHandle = work.clientHandle;
        plannedStmt.connectionId = work.connectionId;
        plannedStmt.hostname = work.hostname;
        plannedStmt.clientData = work.clientData;
        String error_msg = null;
        try {
            plan = planner.compilePlan(costModel, work.sql, "adhocsql-" + String.valueOf(counter++), "adhocproc", false, null);
            error_msg = planner.getErrorMessage();
        } catch (Exception e) {
            plan = null;
            error_msg = e.getMessage();
        }
        if (plan != null)
        {
            plan.sql = work.sql;
        }
        else
        {
            plannedStmt.errorMsg =
                "Failed to ad-hoc-plan for stmt: " + work.sql;
            plannedStmt.errorMsg += " with error: " + error_msg;
        }

        if (plan != null) {
            assert(plan.fragments.size() <= 2);
            for (int i = 0; i < plan.fragments.size(); i++) {

                Fragment frag = plan.fragments.get(i);
                PlanNodeList planList = new PlanNodeList(frag.planGraph);
                String serializedPlan = planList.toJSONString();

                // assume multipartition is the collector fragment
                if (frag.multiPartition) {
                    plannedStmt.collectorFragment = serializedPlan;
                }
                // assume non-multi is the aggregator fragment
                else {
                    plannedStmt.aggregatorFragment = serializedPlan;
                }

            }

            // fill in stuff for the whole sql stmt
            plannedStmt.isReplicatedTableDML = plan.replicatedTableDML;
            plannedStmt.sql = plan.sql;
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

            // compute the diff
            String diffCommands = CatalogDiffEngine.getCommandsToDiff(context.catalog, newCatalog);

            // since diff commands can be stupidly big, compress them here
            retval.encodedDiffCommands = Encoder.compressAndBase64Encode(diffCommands);
            // check if the resulting string is small enough to fit in our parameter sets
            if (retval.encodedDiffCommands.length() > 32000) {
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
