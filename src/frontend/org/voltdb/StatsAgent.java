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
            // hacky way to support two format of hashconfig using interval value
            // return true if interval == true (delta-flag == 1), indicate sent compressed json
            // otherwise return false, indicate sent original binary format
            boolean jsonConfig = obj.getBoolean("interval");
            collectTopoStats(psr,jsonConfig);
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
                vt.addRow(hashConfig.type.toString(), hashConfig.configBytes);
            } else {
                vt.addRow(hashConfig.type.toString(), TheHashinator.getCurrentHashinator().getConfigJSONCompressed());
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

    private VoltTable[] collectDistributedStats(JSONObject obj) throws Exception
    {
        VoltTable[] stats = null;
        // dispatch to collection
        String subselectorString = obj.getString("subselector");
        boolean interval = obj.getBoolean("interval");
        StatsSelector subselector = StatsSelector.valueOf(subselectorString);
        switch (subselector) {
        case DRPRODUCER:
        case DR: // synonym of DRPRODUCER
            stats = collectDRProducerStats();
            break;
        case DRPRODUCERNODE:
            stats = collectStats(StatsSelector.DRPRODUCERNODE, false);
            break;
        case DRPRODUCERPARTITION:
            stats = collectStats(StatsSelector.DRPRODUCERPARTITION, false);
            break;
        case SNAPSHOTSTATUS:
            stats = collectStats(StatsSelector.SNAPSHOTSTATUS, false);
            break;
        case MEMORY:
            stats = collectStats(StatsSelector.MEMORY, interval);
            break;
        case CPU:
            stats = collectStats(StatsSelector.CPU, interval);
            break;
        case IOSTATS:
            stats = collectStats(StatsSelector.IOSTATS, interval);
            break;
        case INITIATOR:
            stats = collectStats(StatsSelector.INITIATOR, interval);
            break;
        case TABLE:
            stats = collectStats(StatsSelector.TABLE, interval);
            break;
        case INDEX:
            stats = collectStats(StatsSelector.INDEX, interval);
            break;
        case PROCEDURE:
        case PROCEDUREINPUT:
        case PROCEDUREOUTPUT:
        case PROCEDUREPROFILE:
            stats = collectStats(StatsSelector.PROCEDURE, interval);
            break;
        case STARVATION:
            stats = collectStats(StatsSelector.STARVATION, interval);
            break;
        case PLANNER:
            stats = collectStats(StatsSelector.PLANNER, interval);
            break;
        case LIVECLIENTS:
            stats = collectStats(StatsSelector.LIVECLIENTS, interval);
            break;
        case LATENCY:
            stats = collectStats(StatsSelector.LATENCY, interval);
            break;
        case LATENCY_HISTOGRAM:
            stats = collectStats(StatsSelector.LATENCY_HISTOGRAM, interval);
            break;
        case MANAGEMENT:
            stats = collectManagementStats(interval);
            break;
        case REBALANCE:
            stats = collectStats(StatsSelector.REBALANCE, interval);
            break;
        case KSAFETY:
            stats = collectStats(StatsSelector.KSAFETY, interval);
            break;
        case DRCONSUMER:
            stats = collectDRConsumerStats();
            break;
        case DRCONSUMERNODE:
            stats = collectStats(StatsSelector.DRCONSUMERNODE, false);
            break;
        case DRCONSUMERPARTITION:
            stats = collectStats(StatsSelector.DRCONSUMERPARTITION, false);
            break;
        case COMMANDLOG:
            stats = collectStats(StatsSelector.COMMANDLOG, false);
            break;
        case IMPORTER:
            stats = collectStats(StatsSelector.IMPORTER, interval);
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

    private VoltTable[] collectDRProducerStats()
    {
        VoltTable[] stats = null;

        VoltTable[] partitionStats = collectStats(StatsSelector.DRPRODUCERPARTITION, false);
        VoltTable[] nodeStats = collectStats(StatsSelector.DRPRODUCERNODE, false);
        if (partitionStats != null && nodeStats != null) {
            stats = new VoltTable[2];
            stats[0] = partitionStats[0];
            stats[1] = nodeStats[0];
        }
        return stats;
    }

    private VoltTable[] collectDRConsumerStats() {
        VoltTable[] stats = null;

        VoltTable[] statusStats = collectStats(StatsSelector.DRCONSUMERNODE, false);
        VoltTable[] perfStats = collectStats(StatsSelector.DRCONSUMERPARTITION, false);
        if (statusStats != null && perfStats != null) {
            stats = new VoltTable[2];
            stats[0] = statusStats[0];
            stats[1] = perfStats[0];
        }
        return stats;
    }

    // This is just a roll-up of MEMORY, TABLE, INDEX, PROCEDURE, INITIATOR, IO, and
    // STARVATION
    private VoltTable[] collectManagementStats(boolean interval)
    {
        VoltTable[] mStats = collectStats(StatsSelector.MEMORY, interval);
        VoltTable[] iStats = collectStats(StatsSelector.INITIATOR, interval);
        VoltTable[] pStats = collectStats(StatsSelector.PROCEDURE, interval);
        VoltTable[] ioStats = collectStats(StatsSelector.IOSTATS, interval);
        VoltTable[] tStats = collectStats(StatsSelector.TABLE, interval);
        VoltTable[] indStats = collectStats(StatsSelector.INDEX, interval);
        VoltTable[] sStats = collectStats(StatsSelector.STARVATION, interval);
        VoltTable[] cStats = collectStats(StatsSelector.CPU, interval);
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

    private VoltTable[] collectStats(StatsSelector selector, boolean interval)
    {
        Long now = System.currentTimeMillis();
        VoltTable[] stats = null;

        VoltTable statsAggr = getStatsAggregate(selector, interval, now);
        if (statsAggr != null) {
            stats = new VoltTable[1];
            stats[0] = statsAggr;
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

    public void deregisterStatsSourcesFor(StatsSelector selector, long siteId) {
        assert selector != null;
        final NonBlockingHashMap<Long, NonBlockingHashSet<StatsSource>> siteIdToStatsSources = registeredStatsSources.get(selector);
        if (siteIdToStatsSources != null) {
            siteIdToStatsSources.remove(siteId);
        }
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
