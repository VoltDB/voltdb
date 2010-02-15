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

package org.voltdb.planner;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import org.hsqldb.HSQLInterface;
import org.hsqldb.HSQLInterface.HSQLParseException;
import org.voltdb.VoltDB;
import org.voltdb.catalog.*;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.debugstate.PlannerThreadContext;
import org.voltdb.planner.CompiledPlan.Fragment;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.utils.HexEncoder;
import org.voltdb.utils.DumpManager;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltLoggerFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class AdHocPlannerThread extends Thread implements DumpManager.Dumpable {

    LinkedBlockingQueue<AdHocPlannerWork> m_work = new LinkedBlockingQueue<AdHocPlannerWork>();
    final ArrayDeque<AdHocPlannedStmt> m_finished = new ArrayDeque<AdHocPlannedStmt>();
    Catalog m_catalog;
    HSQLInterface m_hsql;
    int counter = 0;
    final int m_siteId;
    boolean m_isLoaded = false;

    // store the id used by the DumpManager to identify this execution site
    final String m_dumpId;
    long m_currentDumpTimestamp = 0;

    private static final Logger ahpLog = Logger.getLogger("ADHOCPLANNERTHREAD", VoltLoggerFactory.instance());

    public AdHocPlannerThread(Catalog catalog, int siteId) {
        m_catalog = catalog;
        m_hsql = null;
        m_siteId = siteId;

        setName("Ad Hoc Planner");

        m_dumpId = "AdHocPlannerThread." + String.valueOf(m_siteId);
        DumpManager.register(m_dumpId, this);
    }

    public synchronized void ensureLoadedHSQL() {
        if (m_isLoaded == true)
            return;
        m_hsql = HSQLInterface.loadHsqldb();
        Cluster cluster = m_catalog.getClusters().get("cluster");
        Database database = cluster.getDatabases().get("database");
        String hexDDL = database.getSchema();
        String ddl = HexEncoder.hexDecodeToString(hexDDL);
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
     * @param sequenceNumber
     * @param clientData Data supplied by ClientInterface (typically a VoltPort) that will be in the PlannedStmt produced later.
     */
    public void planSQL(String sql, long clientHandle, int connectionId, int sequenceNumber, Object clientData) {
        ensureLoadedHSQL();

        AdHocPlannerWork work = new AdHocPlannerWork();
        work.clientHandle = clientHandle;
        work.sql = sql;
        work.connectionId = connectionId;
        work.sequenceNumber = sequenceNumber;
        work.clientData = clientData;
        m_work.add(work);
    }

    public AdHocPlannedStmt getPlannedStmt() {
        synchronized (m_finished) {
            return m_finished.poll();
        }
    }

    @Override
    public void run() {
        AdHocPlannerWork work = null;
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
                TrivialCostModel costModel = new TrivialCostModel();
                Cluster cluster = m_catalog.getClusters().get("cluster");
                Database database = cluster.getDatabases().get("database");
                QueryPlanner planner =
                    new QueryPlanner(cluster, database, m_hsql,
                                     new DatabaseEstimates(), false,
                                     VoltDB.getQuietAdhoc());
                CompiledPlan plan = null;
                AdHocPlannedStmt plannedStmt = new AdHocPlannedStmt();
                plannedStmt.clientHandle = work.clientHandle;
                plannedStmt.connectionId = work.connectionId;
                plannedStmt.clientData = work.clientData;
                String error_msg = null;
                try {
                    plan = planner.compilePlan(costModel, work.sql, "adhocsql-" + String.valueOf(counter++), "adhocproc", false, null);
                    error_msg = planner.m_recentErrorMsg;
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

                synchronized (m_finished) {
                    m_finished.add(plannedStmt);
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

        ArrayList<AdHocPlannerWork> toplan = new ArrayList<AdHocPlannerWork>();
        ArrayList<AdHocPlannedStmt> planned = new ArrayList<AdHocPlannedStmt>();

        for (AdHocPlannerWork work : m_work)
            toplan.add(work);
        for (AdHocPlannedStmt stmt : m_finished)
            planned.add(stmt);

        context.plannerWork = new AdHocPlannerWork[toplan.size()];
        for (int i = 0; i < toplan.size(); i++)
            context.plannerWork[i] = toplan.get(i);

        context.plannedStmts = new AdHocPlannedStmt[planned.size()];
        for (int i = 0; i < planned.size(); i++)
            context.plannedStmts[i] = planned.get(i);

        return context;
    }
}
