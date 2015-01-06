/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.Map;

import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashMap;
import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashSet;
import org.json_voltpatches.JSONObject;
import org.voltcore.network.Connection;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Agent responsible for collecting stats on this host.
 */
public class StatsAgent extends OpsAgent
{
    private final NonBlockingHashMap<StatsSelector, NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>>> registeredStatsSources =
            new NonBlockingHashMap<StatsSelector, NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>>>();

    public StatsAgent()
    {
        super("StatsAgent");
        StatsSelector selectors[] = StatsSelector.values();
        for (int ii = 0; ii < selectors.length; ii++) {
            registeredStatsSources.put(selectors[ii], new NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>>());
        }
    }

    @Override
    protected void dispatchFinalAggregations(PendingOpsRequest request)
    {
        StatsSelector subselector = StatsSelector.valueOf(request.subselector);
        switch (subselector) {
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

        default:
        }
    }

    private Supplier<Map<String, Boolean>> m_procInfo = getProcInfoSupplier();

    private Supplier<Map<String, Boolean>> getProcInfoSupplier() {
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
     * Check if proc is readonly?
     *
     * @param pname
     * @return
     */
    private boolean isReadOnlyProcedure(String pname) {
        final Boolean b = m_procInfo.get().get(pname);
        if (b == null) {
            return false;
        }
        return b;
    }

    /**
     * Produce PROCEDUREPROFILE aggregation of PROCEDURE subselector
     */
    private VoltTable[] aggregateProcedureProfileStats(VoltTable[] baseStats)
    {
        if (baseStats == null || baseStats.length != 1) {
            return baseStats;
        }

        StatsProcProfTable timeTable = new StatsProcProfTable();
        baseStats[0].resetRowPosition();
        while (baseStats[0].advanceRow()) {
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
        if (baseStats == null || baseStats.length != 1) {
            return baseStats;
        }

        StatsProcInputTable timeTable = new StatsProcInputTable();
        baseStats[0].resetRowPosition();
        while (baseStats[0].advanceRow()) {
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
        if (baseStats == null || baseStats.length != 1) {
            return baseStats;
        }

        StatsProcOutputTable timeTable = new StatsProcOutputTable();
        baseStats[0].resetRowPosition();
        while (baseStats[0].advanceRow()) {
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
     * Need to release references to catalog related stats sources
     * to avoid hoarding references to the catalog.
     */
    public void notifyOfCatalogUpdate() {
        m_procInfo = getProcInfoSupplier();
        registeredStatsSources.put(StatsSelector.PROCEDURE,
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
        }
        else {
            err = "StatsAgent received non-STATISTICS selector: " + selector.name();
        }
        if (err != null) {
            sendErrorResponse(c, ClientResponse.GRACEFUL_FAILURE, err, clientHandle);
            return;
        }
        String subselector = obj.getString("subselector");

        // Some selectors can provide a single answer based on global data.
        // Intercept them and respond before doing the distributed stuff.
        if (subselector.equalsIgnoreCase("TOPO")) {
            PendingOpsRequest psr = new PendingOpsRequest(
                    selector,
                    subselector,
                    c,
                    clientHandle,
                    System.currentTimeMillis(),
                    obj);
            collectTopoStats(psr);
            return;
        }
        else if (subselector.equalsIgnoreCase("PARTITIONCOUNT")) {
            PendingOpsRequest psr = new PendingOpsRequest(
                    selector,
                    subselector,
                    c,
                    clientHandle,
                    System.currentTimeMillis(),
                    obj);
            collectPartitionCount(psr);
            return;
        }

        PendingOpsRequest psr =
                new PendingOpsRequest(
                        selector,
                        subselector,
                        c,
                        clientHandle,
                        System.currentTimeMillis(),
                        obj);
        distributeOpsWork(psr, obj);
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

        OpsSelector selector = OpsSelector.valueOf(obj.getString("selector").toUpperCase());
        if (selector == OpsSelector.STATISTICS) {
            results = collectDistributedStats(obj);
        }
        else {
            hostLog.warn("StatsAgent received a non-STATISTICS OPS selector: " + selector);
        }

        sendOpsResponse(results, obj);
    }

    private void collectTopoStats(PendingOpsRequest psr)
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
            vt.addRow(hashConfig.type.toString(), hashConfig.configBytes);
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

    private VoltTable[] collectDistributedStats(JSONObject obj) throws Exception
    {
        VoltTable[] stats = null;
        // dispatch to collection
        String subselectorString = obj.getString("subselector");
        boolean interval = obj.getBoolean("interval");
        StatsSelector subselector = StatsSelector.valueOf(subselectorString);
        switch (subselector) {
        case DR:
            stats = collectDRStats();
            break;
        case DRNODE:
            stats = collectDRNodeStats();
            break;
        case DRPARTITION:
            stats = collectDRPartitionStats();
            break;
        case SNAPSHOTSTATUS:
            stats = collectSnapshotStatusStats();
            break;
        case MEMORY:
            stats = collectMemoryStats(interval);
            break;
        case CPU:
            stats = collectCpuStats(interval);
            break;
        case IOSTATS:
            stats = collectIOStats(interval);
            break;
        case INITIATOR:
            stats = collectInitiatorStats(interval);
            break;
        case TABLE:
            stats = collectTableStats(interval);
            break;
        case INDEX:
            stats = collectIndexStats(interval);
            break;
        case PROCEDURE:
        case PROCEDUREINPUT:
        case PROCEDUREOUTPUT:
        case PROCEDUREPROFILE:
            stats = collectProcedureStats(interval);
            break;
        case STARVATION:
            stats = collectStarvationStats(interval);
            break;
        case PLANNER:
            stats = collectPlannerStats(interval);
            break;
        case LIVECLIENTS:
            stats = collectLiveClientsStats(interval);
            break;
        case LATENCY:
            stats = collectLatencyStats(interval);
            break;
        case LATENCY_HISTOGRAM:
            stats = collectLatencyHistogramStats(interval);
            break;
        case MANAGEMENT:
            stats = collectManagementStats(interval);
            break;
        case REBALANCE:
            stats = collectRebalanceStats(interval);
            break;
        case KSAFETY:
            stats = collectKSafetyStats(interval);
            break;
        default:
            // Should have been successfully groomed in collectStatsImpl().  Log something
            // for our information but let the null check below return harmlessly
            hostLog.warn("Received unknown stats selector in StatsAgent: " + subselector.name() +
                    ", this should be impossible.");
            stats = null;
        }

        return stats;
    }

    private VoltTable[] collectDRStats()
    {
        VoltTable[] stats = null;

        VoltTable[] partitionStats = collectDRPartitionStats();
        VoltTable[] nodeStats = collectDRNodeStats();
        if (partitionStats != null && nodeStats != null) {
            stats = new VoltTable[2];
            stats[0] = partitionStats[0];
            stats[1] = nodeStats[0];
        }
        return stats;
    }

    private VoltTable[] collectDRNodeStats()
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable nodeStats = getStatsAggregate(StatsSelector.DRNODE, false, now);
        if (nodeStats != null) {
            stats = new VoltTable[1];
            stats[0] = nodeStats;
        }
        return stats;
    }

    private VoltTable[] collectDRPartitionStats()
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable partitionStats = getStatsAggregate(StatsSelector.DRPARTITION, false, now);
        if (partitionStats != null) {
            stats = new VoltTable[1];
            stats[0] = partitionStats;
        }
        return stats;
    }

    private VoltTable[] collectSnapshotStatusStats()
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable ssStats = getStatsAggregate(StatsSelector.SNAPSHOTSTATUS, false, now);
        if (ssStats != null) {
            stats = new VoltTable[1];
            stats[0] = ssStats;
        }
        return stats;
    }

    private VoltTable[] collectMemoryStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable mStats = getStatsAggregate(StatsSelector.MEMORY, interval, now);
        if (mStats != null) {
            stats = new VoltTable[1];
            stats[0] = mStats;
        }
        return stats;
    }

    private VoltTable[] collectCpuStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable cStats = getStatsAggregate(StatsSelector.CPU, interval, now);
        if (cStats != null) {
            stats = new VoltTable[1];
            stats[0] = cStats;
        }
        return stats;
    }

    private VoltTable[] collectIOStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable iStats = getStatsAggregate(StatsSelector.IOSTATS, interval, now);
        if (iStats != null) {
            stats = new VoltTable[1];
            stats[0] = iStats;
        }
        return stats;
    }

    private VoltTable[] collectInitiatorStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable iStats = getStatsAggregate(StatsSelector.INITIATOR, interval, now);
        if (iStats != null) {
            stats = new VoltTable[1];
            stats[0] = iStats;
        }
        return stats;
    }

    private VoltTable[] collectTableStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable tStats = getStatsAggregate(StatsSelector.TABLE, interval, now);
        if (tStats != null) {
            stats = new VoltTable[1];
            stats[0] = tStats;
        }
        return stats;
    }

    private VoltTable[] collectIndexStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable tStats = getStatsAggregate(StatsSelector.INDEX, interval, now);
        if (tStats != null) {
            stats = new VoltTable[1];
            stats[0] = tStats;
        }
        return stats;
    }

    private VoltTable[] collectProcedureStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable pStats = getStatsAggregate(StatsSelector.PROCEDURE, interval, now);
        if (pStats != null) {
            stats = new VoltTable[1];
            stats[0] = pStats;
        }
        return stats;
    }

    private VoltTable[] collectStarvationStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable sStats = getStatsAggregate(StatsSelector.STARVATION, interval, now);
        if (sStats != null) {
            stats = new VoltTable[1];
            stats[0] = sStats;
        }
        return stats;
    }

    private VoltTable[] collectPlannerStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable pStats = getStatsAggregate(StatsSelector.PLANNER, interval, now);
        if (pStats != null) {
            stats = new VoltTable[1];
            stats[0] = pStats;
        }
        return stats;
    }

    private VoltTable[] collectLiveClientsStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable lStats = getStatsAggregate(StatsSelector.LIVECLIENTS, interval, now);
        if (lStats != null) {
            stats = new VoltTable[1];
            stats[0] = lStats;
        }
        return stats;
    }

    // Latency stats have been broken since 3.0.  Putting these hooks
    // in here so that ALL selectors in SysProcSelector go through
    // this path and nothing uses the legacy sysproc
    private VoltTable[] collectLatencyStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable lStats = getStatsAggregate(StatsSelector.LATENCY, interval, now);
        if (lStats != null) {
            stats = new VoltTable[1];
            stats[0] = lStats;
        }
        return stats;
    }

    private VoltTable[] collectLatencyHistogramStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable lStats = getStatsAggregate(StatsSelector.LATENCY_HISTOGRAM, interval, now);
        if (lStats != null) {
            stats = new VoltTable[1];
            stats[0] = lStats;
        }
        return stats;
    }

    // This is just a roll-up of MEMORY, TABLE, INDEX, PROCEDURE, INITIATOR, IO, and
    // STARVATION
    private VoltTable[] collectManagementStats(boolean interval)
    {
        VoltTable[] mStats = collectMemoryStats(interval);
        VoltTable[] iStats = collectInitiatorStats(interval);
        VoltTable[] pStats = collectProcedureStats(interval);
        VoltTable[] ioStats = collectIOStats(interval);
        VoltTable[] tStats = collectTableStats(interval);
        VoltTable[] indStats = collectIndexStats(interval);
        VoltTable[] sStats = collectStarvationStats(interval);
        VoltTable[] cStats = collectCpuStats(interval);
        // Ugh, this is ugly.  Currently need to return null if
        // we're missing any of the tables so that we
        // don't screw up the aggregation in handleStatsResponse (see my rant there)
        if (mStats == null || iStats == null || pStats == null ||
                ioStats == null || tStats == null || indStats == null ||
                sStats == null || cStats == null)
        {
            return null;
        }
        VoltTable[] stats = new VoltTable[8];
        stats[0] = mStats[0];
        stats[1] = iStats[0];
        stats[2] = pStats[0];
        stats[3] = ioStats[0];
        stats[4] = tStats[0];
        stats[5] = indStats[0];
        stats[6] = sStats[0];
        stats[7] = cStats[0];

        return stats;
    }

    private VoltTable[] collectRebalanceStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable mStats = getStatsAggregate(StatsSelector.REBALANCE, interval, now);
        if (mStats != null) {
            stats = new VoltTable[1];
            stats[0] = mStats;
        }
        return stats;
    }

    private VoltTable[] collectKSafetyStats(boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable mStats = getStatsAggregate(StatsSelector.KSAFETY, interval, now);
        if (mStats != null) {
            stats = new VoltTable[1];
            stats[0] = mStats;
        }
        return stats;
    }

    public void registerStatsSource(StatsSelector selector, long siteId, StatsSource source) {
        assert selector != null;
        assert source != null;
        final NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>> siteIdToStatsSources = registeredStatsSources.get(selector);
        assert siteIdToStatsSources != null;

        //Racy putIfAbsent idiom, may return existing map value from another thread http://goo.gl/jptTS7
        NonBlockingHashSet<StatsSource> statsSources = siteIdToStatsSources.get(siteId);
        if (statsSources == null) {
            statsSources = new NonBlockingHashSet<StatsSource>();
            NonBlockingHashSet<StatsSource> oldval = siteIdToStatsSources.putIfAbsent(siteId, statsSources);
            if (oldval != null) statsSources = oldval;
        }
        statsSources.add(source);
    }

    /**
     * Get aggregate statistics on this node for the given selector.
     * If you need both site-wise and node-wise stats, register the appropriate StatsSources for that
     * selector with each siteId and then some other value for the node-level stats (PLANNER stats uses -1).
     * This call will automagically aggregate every StatsSource registered for every 'site'ID for that selector.
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
        return getStatsAggregateInternal(selector, interval, now, null);
    }

    private VoltTable getStatsAggregateInternal(
            final StatsSelector selector,
            final boolean interval,
            final Long now,
            VoltTable prevResults)
    {
        assert selector != null;
        final NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>> siteIdToStatsSources = registeredStatsSources.get(selector);

        // There are cases early in rejoin where we can get polled before the server is ready to provide
        // stats.  Just return null for now, which will result in no tables from this node.
        if (siteIdToStatsSources == null || siteIdToStatsSources.isEmpty()) {
            return null;
        }

        // Just need a random site's list to do some things
        NonBlockingHashSet<StatsSource> sSources = siteIdToStatsSources.entrySet().iterator().next().getValue();

        //There is a window registering the first source where the empty set is visible, don't panic it's coming
        while (sSources.isEmpty()) {
            Thread.yield();
        }

        /*
         * Some sources like TableStats use VoltTable to keep track of
         * statistics. We need to use the table schema the VoltTable has in this
         * case.
         */
        VoltTable.ColumnInfo columns[] = null;
        final StatsSource firstSource = sSources.iterator().next();
        if (!firstSource.isEEStats())
            columns = firstSource.getColumnSchema().toArray(new VoltTable.ColumnInfo[0]);
        else {
            final VoltTable table = firstSource.getStatsTable();
            if (table == null)
                return null;
            columns = new VoltTable.ColumnInfo[table.getColumnCount()];
            for (int i = 0; i < columns.length; i++)
                columns[i] = new VoltTable.ColumnInfo(table.getColumnName(i),
                        table.getColumnType(i));
        }

        // Append to previous results if provided.
        final VoltTable resultTable = prevResults != null ? prevResults : new VoltTable(columns);

        for (NonBlockingHashSet<StatsSource> statsSources : siteIdToStatsSources.values()) {

            //The window where it is empty exists here to
            while (statsSources.isEmpty()) {
                Thread.yield();
            }

            assert statsSources != null;
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
                        while (table.advanceRow()) {
                            resultTable.add(table);
                        }
                        table.resetRowPosition();
                    }
                } else {
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
