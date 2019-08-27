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
package org.voltdb;

import java.util.ArrayList;
import java.util.Map;

import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashMap;
import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashSet;
import org.json_voltpatches.JSONObject;
import org.voltcore.network.Connection;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.sched.ScheduleStatsSource;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Agent responsible for collecting stats on this host.
 */
public class StatsAgent extends OpsAgent
{
    private final NonBlockingHashMap<StatsSelector, NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>>> m_registeredStatsSources =
            new NonBlockingHashMap<StatsSelector, NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>>>();

    public StatsAgent()
    {
        super("StatsAgent");
        StatsSelector selectors[] = StatsSelector.values();
        for (StatsSelector selector : selectors) {
            m_registeredStatsSources.put(selector, new NonBlockingHashMap<Long,NonBlockingHashSet<StatsSource>>());
        }
    }

    @Override
    protected void dispatchFinalAggregations(PendingOpsRequest request) {
        if (request.aggregateTables == null || request.aggregateTables.length == 0) {
            // Skip aggregation when there are no stats
            return;
        }

        StatsSelector subselector = StatsSelector.valueOf(request.subselector);
        switch (subselector) {
        case PROCEDUREDETAIL:
            request.aggregateTables = sortProcedureDetailStats(request.aggregateTables);
            break;
        // For PROCEDURE-series tables, they are all based on the procedure detail table.
        case PROCEDURE:
            request.aggregateTables =
            aggregateProcedureStats(request.aggregateTables);
            break;
        case PROCEDUREPROFILE:
            request.aggregateTables =
            aggregateProcedureProfileStats(request.aggregateTables);
            break;
        case PROCEDUREINPUT:
            request.aggregateTables =
            aggregateProcedureInputStats(request.aggregateTables);
            break;
        case PROCEDUREOUTPUT:
            request.aggregateTables =
            aggregateProcedureOutputStats(request.aggregateTables);
            break;
        case DRROLE:
            request.aggregateTables = aggregateDRRoleStats(request.aggregateTables);
            break;
        case SCHEDULED_PROCEDURES:
        case SCHEDULERS:
            ScheduleStatsSource.convert(subselector, request.aggregateTables);
            break;
        default:
        }
    }

    private VoltTable[] sortProcedureDetailStats(VoltTable[] baseStats) {
        ProcedureDetailResultTable result = new ProcedureDetailResultTable(baseStats[0]);
        return result.getSortedResultTable();
    }

    private Supplier<Map<String, Boolean>> m_procedureInfo = getProcedureInformationfoSupplier();

    private Supplier<Map<String, Boolean>> getProcedureInformationfoSupplier() {
        return Suppliers.memoize(new Supplier<Map<String, Boolean>>() {
                @Override
                public Map<String, Boolean> get() {
                    ImmutableMap.Builder<String, Boolean> b = ImmutableMap.builder();
                    CatalogContext ctx = VoltDB.instance().getCatalogContext();
                    for (Procedure p : ctx.procedures) {
                        b.put(p.getClassname(), p.getReadonly());
                    }
                    return b.build();
                }
            });
    }

    /**
     * Check if procedure is readonly?
     *
     * @param pname
     * @return
     */
    private boolean isReadOnlyProcedure(String pname) {
        final Boolean b = m_procedureInfo.get().get(pname);
        if (b == null) {
            return false;
        }
        return b;
    }

    /**
     * Produce PROCEDURE aggregation of PROCEDURE subselector
     * Basically it leaves out the rows that were not labeled as "<ALL>".
     */
    private VoltTable[] aggregateProcedureStats(VoltTable[] baseStats)
    {
        VoltTable result = new VoltTable(
            new ColumnInfo("TIMESTAMP", VoltType.BIGINT),
            new ColumnInfo(VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID),
            new ColumnInfo("HOSTNAME", VoltType.STRING),
            new ColumnInfo(VoltSystemProcedure.CNAME_SITE_ID, VoltSystemProcedure.CTYPE_ID),
            new ColumnInfo("PARTITION_ID", VoltType.INTEGER),
            new ColumnInfo("PROCEDURE", VoltType.STRING),
            new ColumnInfo("INVOCATIONS", VoltType.BIGINT),
            new ColumnInfo("TIMED_INVOCATIONS", VoltType.BIGINT),
            new ColumnInfo("MIN_EXECUTION_TIME", VoltType.BIGINT),
            new ColumnInfo("MAX_EXECUTION_TIME", VoltType.BIGINT),
            new ColumnInfo("AVG_EXECUTION_TIME", VoltType.BIGINT),
            new ColumnInfo("MIN_RESULT_SIZE", VoltType.INTEGER),
            new ColumnInfo("MAX_RESULT_SIZE", VoltType.INTEGER),
            new ColumnInfo("AVG_RESULT_SIZE", VoltType.INTEGER),
            new ColumnInfo("MIN_PARAMETER_SET_SIZE", VoltType.INTEGER),
            new ColumnInfo("MAX_PARAMETER_SET_SIZE", VoltType.INTEGER),
            new ColumnInfo("AVG_PARAMETER_SET_SIZE", VoltType.INTEGER),
            new ColumnInfo("ABORTS", VoltType.BIGINT),
            new ColumnInfo("FAILURES", VoltType.BIGINT),
            new ColumnInfo("TRANSACTIONAL", VoltType.TINYINT));
        baseStats[0].resetRowPosition();
        while (baseStats[0].advanceRow()) {
            if (baseStats[0].getString("STATEMENT").equalsIgnoreCase("<ALL>")) {
                result.addRow(
                    baseStats[0].getLong("TIMESTAMP"),
                    baseStats[0].getLong(VoltSystemProcedure.CNAME_HOST_ID),
                    baseStats[0].getString("HOSTNAME"),
                    baseStats[0].getLong(VoltSystemProcedure.CNAME_SITE_ID),
                    baseStats[0].getLong("PARTITION_ID"),
                    baseStats[0].getString("PROCEDURE"),
                    baseStats[0].getLong("INVOCATIONS"),
                    baseStats[0].getLong("TIMED_INVOCATIONS"),
                    baseStats[0].getLong("MIN_EXECUTION_TIME"),
                    baseStats[0].getLong("MAX_EXECUTION_TIME"),
                    baseStats[0].getLong("AVG_EXECUTION_TIME"),
                    baseStats[0].getLong("MIN_RESULT_SIZE"),
                    baseStats[0].getLong("MAX_RESULT_SIZE"),
                    baseStats[0].getLong("AVG_RESULT_SIZE"),
                    baseStats[0].getLong("MIN_PARAMETER_SET_SIZE"),
                    baseStats[0].getLong("MAX_PARAMETER_SET_SIZE"),
                    baseStats[0].getLong("AVG_PARAMETER_SET_SIZE"),
                    baseStats[0].getLong("ABORTS"),
                    baseStats[0].getLong("FAILURES"),
                    (byte) baseStats[0].getLong("TRANSACTIONAL"));
            }
        }
        return new VoltTable[] { result };
    }

    /**
     * Produce PROCEDUREPROFILE aggregation of PROCEDURE subselector
     */
    private VoltTable[] aggregateProcedureProfileStats(VoltTable[] baseStats)
    {
        StatsProcProfTable timeTable = new StatsProcProfTable();
        baseStats[0].resetRowPosition();
        while (baseStats[0].advanceRow()) {
            // Skip non-transactional procedures for some of these rollups until
            // we figure out how to make them less confusing.
            // NB: They still show up in the raw PROCEDURE stata.
            boolean transactional = baseStats[0].getLong("TRANSACTIONAL") == 1;
            if (!transactional) {
                continue;
            }

            if ( ! baseStats[0].getString("STATEMENT").equalsIgnoreCase("<ALL>")) {
                continue;
            }
            String pname = baseStats[0].getString("PROCEDURE");

            timeTable.updateTable(!isReadOnlyProcedure(pname),
                    baseStats[0].getLong("TIMESTAMP"),
                    pname,
                    baseStats[0].getLong("PARTITION_ID"),
                    baseStats[0].getLong("INVOCATIONS"),
                    baseStats[0].getLong("MIN_EXECUTION_TIME"),
                    baseStats[0].getLong("MAX_EXECUTION_TIME"),
                    baseStats[0].getLong("AVG_EXECUTION_TIME"),
                    baseStats[0].getLong("FAILURES"),
                    baseStats[0].getLong("ABORTS"));
        }
        return new VoltTable[] { timeTable.sortByAverage("EXECUTION_TIME") };
    }
    /**
     * Produce PROCEDUREINPUT aggregation of PROCEDURE subselector
     */
    private VoltTable[] aggregateProcedureInputStats(VoltTable[] baseStats)
    {
        StatsProcInputTable timeTable = new StatsProcInputTable();
        baseStats[0].resetRowPosition();
        while (baseStats[0].advanceRow()) {
            // Skip non-transactional procedures for some of these rollups until
            // we figure out how to make them less confusing.
            // NB: They still show up in the raw PROCEDURE stata.
            boolean transactional = baseStats[0].getLong("TRANSACTIONAL") == 1;
            if (!transactional) {
                continue;
            }

            if ( ! baseStats[0].getString("STATEMENT").equalsIgnoreCase("<ALL>")) {
                continue;
            }
            String pname = baseStats[0].getString("PROCEDURE");
            timeTable.updateTable(!isReadOnlyProcedure(pname),
                    pname,
                    baseStats[0].getLong("PARTITION_ID"),
                    baseStats[0].getLong("TIMESTAMP"),
                    baseStats[0].getLong("INVOCATIONS"),
                    baseStats[0].getLong("MIN_PARAMETER_SET_SIZE"),
                    baseStats[0].getLong("MAX_PARAMETER_SET_SIZE"),
                    baseStats[0].getLong("AVG_PARAMETER_SET_SIZE")
                    );
        }
        return new VoltTable[] { timeTable.sortByInput("PROCEDURE_INPUT") };
    }

    /**
     * Produce PROCEDUREOUTPUT aggregation of PROCEDURE subselector
     */

    private VoltTable[] aggregateProcedureOutputStats(VoltTable[] baseStats)
    {
        StatsProcOutputTable timeTable = new StatsProcOutputTable();
        baseStats[0].resetRowPosition();
        while (baseStats[0].advanceRow()) {
            // Skip non-transactional procedures for some of these rollups until
            // we figure out how to make them less confusing.
            // NB: They still show up in the raw PROCEDURE stata.
            boolean transactional = baseStats[0].getLong("TRANSACTIONAL") == 1;
            if (!transactional) {
                continue;
            }

            if ( ! baseStats[0].getString("STATEMENT").equalsIgnoreCase("<ALL>")) {
                continue;
            }
            String pname = baseStats[0].getString("PROCEDURE");
            timeTable.updateTable(!isReadOnlyProcedure(pname),
                    pname,
                    baseStats[0].getLong("PARTITION_ID"),
                    baseStats[0].getLong("TIMESTAMP"),
                    baseStats[0].getLong("INVOCATIONS"),
                    baseStats[0].getLong("MIN_RESULT_SIZE"),
                    baseStats[0].getLong("MAX_RESULT_SIZE"),
                    baseStats[0].getLong("AVG_RESULT_SIZE")
                    );
        }
        return new VoltTable[] { timeTable.sortByOutput("PROCEDURE_OUTPUT") };
    }


    /**
     * Please be noted that this function will be called from Site thread, where
     * most other functions in the class are from StatsAgent thread.
     *
     * Need to release references to catalog related stats sources
     * to avoid hoarding references to the catalog.
     */
    public void notifyOfCatalogUpdate() {
        m_procedureInfo = getProcedureInformationfoSupplier();
        m_registeredStatsSources.put(StatsSelector.PROCEDURE,
                new NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>>());
    }

    @Override
    protected void collectStatsImpl(Connection c, long clientHandle, OpsSelector selector,
            ParameterSet params) throws Exception
    {
        JSONObject obj = new JSONObject();
        obj.put("selector", "STATISTICS");
        // parseParamsForStatistics has a clumsy contract, see definition
        String err = null;
        if (selector == OpsSelector.STATISTICS) {
            err = parseParamsForStatistics(params, obj);
        } else {
            err = "StatsAgent received non-STATISTICS selector: " + selector.name();
        }
        if (err != null) {
            sendErrorResponse(c, ClientResponse.GRACEFUL_FAILURE, err, clientHandle);
            return;
        }
        String subselector = obj.getString("subselector");

        PendingOpsRequest psr = new PendingOpsRequest(selector, subselector, c, clientHandle,
                System.currentTimeMillis(), obj);

        // Some selectors can provide a single answer based on global data.
        // Intercept them and respond before doing the distributed stuff.
        if (subselector.equalsIgnoreCase("TOPO")) {
            // hacky way to support two format of hashconfig using interval value
            // return true if interval == true (delta-flag == 1), indicate sent compressed json
            // otherwise return false, indicate sent original binary format
            boolean jsonConfig = obj.getBoolean("interval");
            collectTopoStats(psr,jsonConfig);
        } else if (subselector.equalsIgnoreCase("PARTITIONCOUNT")) {
            collectPartitionCount(psr);
        } else {
            distributeOpsWork(psr, obj);
        }
    }

    // Parse the provided parameter set object and fill in subselector and interval into
    // the provided JSONObject.  If there's an error, return that in the String, otherwise
    // return null.  Yes, ugly.  Bang it out, then refactor later.
    private String parseParamsForStatistics(ParameterSet params, JSONObject obj) throws Exception
    {
        if ((params.toArray().length < 1) || (params.toArray().length > 2)) {
            return "Incorrect number of arguments to @Statistics (expects 2, received " +
                    params.toArray().length + ")";
        }
        Object first = params.toArray()[0];
        if (!(first instanceof String)) {
            return "First argument to @Statistics must be a valid STRING selector, instead was " +
                    first;
        }
        String subselector = (String)first;
        try {
            StatsSelector s = StatsSelector.valueOf(subselector.toUpperCase());
            subselector = s.name();
        }
        catch (Exception e) {
            return "First argument to @Statistics must be a valid STRING selector, instead was " +
                    first;
        }

        boolean interval = false;
        if (params.toArray().length == 2) {
            interval = ((Number)(params.toArray()[1])).longValue() == 1L;
        }
        obj.put("subselector", subselector);
        obj.put("interval", interval);

        return null;
    }

    @Override
    protected void handleJSONMessage(JSONObject obj) throws Exception {
        VoltTable[] results = null;

        try {
            OpsSelector selector = OpsSelector.valueOf(obj.getString("selector").toUpperCase());
            if (selector == OpsSelector.STATISTICS) {
                results = collectDistributedStats(obj);
            }
            else {
                hostLog.warn("StatsAgent received a non-STATISTICS OPS selector: " + selector);
            }

            sendOpsResponse(results, obj);
        } catch (Exception e) {
            hostLog.warn("Error processing stats request " + obj.toString(4), e);
        } catch (Throwable t) {
            //Handle throwable because otherwise the future swallows up other exceptions
            VoltDB.crashLocalVoltDB("Error processing stats request " + obj.toString(4), true, t);
        }
    }

    private void collectTopoStats(PendingOpsRequest psr, boolean jsonConfig)
    {
        VoltTable[] tables = null;
        VoltTable topoStats = getStatsAggregate(StatsSelector.TOPO, false, psr.startTime);
        if (topoStats != null) {
            tables = new VoltTable[2];
            tables[0] = topoStats;
            VoltTable vt =
                    new VoltTable(
                            new VoltTable.ColumnInfo("HASHTYPE", VoltType.STRING),
                            new VoltTable.ColumnInfo("HASHCONFIG", VoltType.VARBINARY));
            tables[1] = vt;
            HashinatorConfig hashConfig = TheHashinator.getCurrentConfig();
            if (!jsonConfig) {
                vt.addRow("ELASTIC", hashConfig.configBytes);
            } else {
                vt.addRow("ELASTIC", TheHashinator.getCurrentHashinator().getConfigJSONCompressed());
            }

        }
        psr.aggregateTables = tables;

        try {
            sendClientResponse(psr);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to return TOPO results to client.", true, e);
        }
    }

    private void collectPartitionCount(PendingOpsRequest psr)
    {
        VoltTable[] tables = null;
        VoltTable pcStats = getStatsAggregate(StatsSelector.PARTITIONCOUNT, false, psr.startTime);
        if (pcStats != null) {
            tables = new VoltTable[1];
            tables[0] = pcStats;
        }
        psr.aggregateTables = tables;

        try {
            sendClientResponse(psr);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to return PARTITIONCOUNT to client", true, e);
        }
    }

    public VoltTable[] collectDistributedStats(JSONObject obj) throws Exception {
        VoltTable[] stats = null;
        // dispatch to collection
        String subselectorString = obj.getString("subselector");
        boolean interval = obj.getBoolean("interval");
        StatsSelector[] subSelectors = StatsSelector.valueOf(subselectorString).subSelectors();
        stats = new VoltTable[subSelectors.length];
        long now = System.currentTimeMillis();

        for (int i = 0; i < subSelectors.length; ++i) {
            StatsSelector subSelector = subSelectors[i];
            VoltTable stat = getStatsAggregate(subSelector, subSelector.interval(interval), now);
            if (stat == null) {
                return null;
            }
            stats[i] = stat;
        }

        return stats;
    }

    private VoltTable[] aggregateDRRoleStats(VoltTable[] stats) {
        return new VoltTable[] { DRRoleStats.aggregateStats(stats[0]) };
    }

    public void registerStatsSource(StatsSelector selector, long siteId, StatsSource source) {
        assert selector != null;
        assert source != null;

        final NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>> siteIdToStatsSources =
                m_registeredStatsSources.get(selector);
        assert siteIdToStatsSources != null;

        siteIdToStatsSources.computeIfAbsent(siteId, s -> new NonBlockingHashSet<>()).add(source);
    }

    public void deregisterStatsSource(StatsSelector selector, long siteId, StatsSource source) {
        assert selector != null;
        assert source != null;
        final NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>> siteIdToStatsSources =
                m_registeredStatsSources.get(selector);
        assert siteIdToStatsSources != null;

        NonBlockingHashSet<StatsSource> statsSources = siteIdToStatsSources.get(siteId);
        if (statsSources != null) {
            statsSources.remove(source);
        }
    }

    public void deregisterStatsSourcesFor(StatsSelector selector, long siteId) {
        assert selector != null;
        final NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>> siteIdToStatsSources =
                m_registeredStatsSources.get(selector);
        if (siteIdToStatsSources != null) {
            siteIdToStatsSources.remove(siteId);
        }
    }

    /**
     * Get aggregate statistics on this node for the given selector.
     * If you need both site-wise and node-wise stats, register the appropriate StatsSources for that
     * selector with each siteId and then some other value for the node-level stats (PLANNER stats uses -1).
     * This call will automatically aggregate every StatsSource registered for every 'site'ID for that selector.
     *
     * @param selector    @Statistics selector keyword
     * @param interval    true if processing a reporting interval
     * @param now         current timestamp
     * @return  statistics VoltTable results
     */
    public VoltTable getStatsAggregate(
            final StatsSelector selector,
            final boolean interval,
            final Long now) {
        return getStatsAggregateInternal(selector, interval, now);
    }

    private VoltTable getStatsAggregateInternal(
            final StatsSelector selector,
            final boolean interval,
            final Long now)
    {
        assert selector != null;
        NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>> siteIdToStatsSources =
                m_registeredStatsSources.get(selector);

        // There are cases early in rejoin where we can get polled before the server is ready to provide
        // stats.  Just return null for now, which will result in no tables from this node.
        if (siteIdToStatsSources == null || siteIdToStatsSources.isEmpty()) {
            return null;
        }

        VoltTable resultTable = null;

        for (NonBlockingHashSet<StatsSource> statsSources : siteIdToStatsSources.values()) {
            if (statsSources == null || statsSources.isEmpty()) {
                continue;
            }

            for (final StatsSource ss : statsSources) {
                assert ss != null;
                /*
                 * Some sources like TableStats use VoltTable to keep track of
                 * statistics
                 */
                if (ss.isEEStats()) {
                    final VoltTable table = ss.getStatsTable();
                    // this table can be null during recovery, at least
                    if (table != null) {
                        if (resultTable == null) {
                            resultTable = new VoltTable(table.getTableSchema());
                        }
                        resultTable.addTable(table);
                    }
                } else {
                    if (resultTable == null) {
                        ArrayList<ColumnInfo> columns = ss.getColumnSchema();
                        resultTable = new VoltTable(columns.toArray(new ColumnInfo[columns.size()]));
                    }
                    Object statsRows[][] = ss.getStatsRows(interval, now);
                    for (Object[] row : statsRows) {
                        resultTable.addRow(row);
                    }
                }
            }
        }
        return resultTable;
    }
}
