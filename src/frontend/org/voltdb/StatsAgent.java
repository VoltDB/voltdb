/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.Set;

import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashMap;
import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashSet;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.Connection;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.ClientResponse;
import org.voltdb.dr2.DRConsumerStatsBase.DRConsumerClusterStatsBase;
import org.voltdb.dr2.DRProducerClusterStats;
import org.voltdb.stats.procedure.ProcedureDetailAggregator;
import org.voltdb.task.TaskStatsSource;

/**
 * Agent responsible for collecting stats on this host.
 */
public class StatsAgent extends OpsAgent {
    private final Map<StatsSelector, Map<Long, Set<StatsSource>>> m_registeredStatsSources = new NonBlockingHashMap<>();
    private ProcedureDetailAggregator procedureDetailAggregator;

    public StatsAgent() {
        super("StatsAgent");

        for (StatsSelector selector : StatsSelector.values()) {
            m_registeredStatsSources.put(selector, new NonBlockingHashMap<>());
        }

        procedureDetailAggregator = ProcedureDetailAggregator.create();
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
            request.aggregateTables = procedureDetailAggregator.sortProcedureDetailStats(request.aggregateTables);
            break;
        // For PROCEDURE-series tables, they are all based on the procedure detail table.
        case PROCEDURE:
            request.aggregateTables = procedureDetailAggregator.aggregateProcedureStats(request.aggregateTables);
            break;
        case PROCEDUREPROFILE:
            request.aggregateTables = procedureDetailAggregator.aggregateProcedureProfileStats(request.aggregateTables);
            break;
        case PROCEDUREINPUT:
            request.aggregateTables = procedureDetailAggregator.aggregateProcedureInputStats(request.aggregateTables);
            break;
        case PROCEDUREOUTPUT:
            request.aggregateTables = procedureDetailAggregator.aggregateProcedureOutputStats(request.aggregateTables);
            break;
        case COMPOUNDPROCSUMMARY:
            request.aggregateTables = procedureDetailAggregator.aggregateCompoundProcSummary(request.aggregateTables);
            break;
        case COMPOUNDPROC:
            request.aggregateTables = procedureDetailAggregator.aggregateCompoundProcByHost(request.aggregateTables);
            break;
        case DRROLE:
            request.aggregateTables = aggregateDRRoleStats(request.aggregateTables);
            break;
        case TASK_PROCEDURE:
        case TASK_SCHEDULER:
            TaskStatsSource.convert(subselector, request.aggregateTables);
            break;
        case SNAPSHOTSUMMARY:
            request.aggregateTables = SnapshotSummary.summarize(request.aggregateTables[0]);
            break;
        case DRPRODUCER:
            request.aggregateTables = aggregateDRProducerClusterStats(request.aggregateTables);
            break;
        case DRCONSUMER:
            request.aggregateTables = aggregateDRConsumerClusterStats(request.aggregateTables);
        default:
        }
    }

    /**
     * Please be noted that this function will be called from Site thread, where
     * most other functions in the class are from StatsAgent thread.
     * <p>
     * Need to release references to catalog related stats sources
     * to avoid hoarding references to the catalog.
     */
    public void notifyOfCatalogUpdate() {
        procedureDetailAggregator = ProcedureDetailAggregator.create();
        m_registeredStatsSources.put(StatsSelector.PROCEDURE, new NonBlockingHashMap<>());
        m_registeredStatsSources.put(StatsSelector.COMPOUNDPROCCALLS, new NonBlockingHashMap<>());
        CompoundProcCallStats.initStats(this);
    }

    @Override
    protected void collectStatsImpl(Connection c, long clientHandle, OpsSelector selector,
                                    ParameterSet params) throws Exception {
        JSONObject obj = new JSONObject();
        obj.put("selector", "STATISTICS");
        // parseParamsForStatistics has a clumsy contract, see definition
        String err;
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
            collectTopoStats(psr, jsonConfig);
        } else if (subselector.equalsIgnoreCase("PARTITIONCOUNT")) {
            collectPartitionCount(psr);
        } else {
            distributeOpsWork(psr, obj);
        }
    }

    // TODO pk please refactor this
    // Parse the provided parameter set object and fill in subselector and interval into
    // the provided JSONObject.  If there's an error, return that in the String, otherwise
    // return null.  Yes, ugly.  Bang it out, then refactor later.
    private String parseParamsForStatistics(ParameterSet params, JSONObject obj) throws Exception {
        if ((params.toArray().length < 1) || (params.toArray().length > 2)) {
            return "Incorrect number of arguments to @Statistics (expects 2, received " +
                   params.toArray().length + ")";
        }
        Object first = params.toArray()[0];
        if (!(first instanceof String)) {
            return "First argument to @Statistics must be a valid STRING selector, instead was " +
                   first;
        }
        String subselector = (String) first;
        try {
            StatsSelector s = StatsSelector.valueOf(subselector.toUpperCase());
            subselector = s.name();
        } catch (Exception e) {
            return "First argument to @Statistics must be a valid STRING selector, instead was " +
                   first;
        }

        boolean interval = false;
        if (params.toArray().length == 2) {
            interval = ((Number) (params.toArray()[1])).longValue() == 1L;
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
            } else {
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

    private void collectTopoStats(PendingOpsRequest psr, boolean jsonConfig) {
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

    private void collectPartitionCount(PendingOpsRequest psr) {
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
        VoltTable[] stats;
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
        return new VoltTable[]{DRRoleStats.aggregateStats(stats[0])};
    }

    private VoltTable[] aggregateDRProducerClusterStats(VoltTable[] stats) {
        VoltTable clusterStats = DRProducerClusterStats.aggregateStats(stats[2]);
        return new VoltTable[]{stats[0], stats[1], clusterStats};
    }

    private VoltTable[] aggregateDRConsumerClusterStats(VoltTable[] stats) {
        VoltTable clusterStats = DRConsumerClusterStatsBase.aggregateStats(stats[2]);
        return new VoltTable[]{stats[0], stats[1], clusterStats};
    }

    public void registerStatsSource(StatsSelector selector, long siteId, StatsSource source) {
        assert selector != null;
        assert source != null;

        final Map<Long, Set<StatsSource>> siteIdToStatsSources = m_registeredStatsSources.get(selector);
        assert siteIdToStatsSources != null;

        siteIdToStatsSources.computeIfAbsent(siteId, s -> new NonBlockingHashSet<>()).add(source);
    }

    public void deregisterStatsSource(StatsSelector selector, long siteId, StatsSource source) {
        assert selector != null;
        assert source != null;
        final Map<Long, Set<StatsSource>> siteIdToStatsSources = m_registeredStatsSources.get(selector);
        assert siteIdToStatsSources != null;

        Set<StatsSource> statsSources = siteIdToStatsSources.get(siteId);
        if (statsSources != null) {
            statsSources.remove(source);
        }
    }

    public void deregisterStatsSourcesFor(StatsSelector selector, long siteId) {
        assert selector != null;
        final Map<Long, Set<StatsSource>> siteIdToStatsSources = m_registeredStatsSources.get(selector);
        if (siteIdToStatsSources != null) {
            siteIdToStatsSources.remove(siteId);
        }
    }

    public Set<StatsSource> lookupStatsSource(StatsSelector selector, long siteId) {
        assert selector != null;

        final Map<Long, Set<StatsSource>> siteIdToStatsSources = m_registeredStatsSources.get(selector);
        assert siteIdToStatsSources != null;

        return siteIdToStatsSources.get(siteId);
    }

    /**
     * Get aggregate statistics on this node for the given selector.
     * If you need both site-wise and node-wise stats, register the appropriate StatsSources for that
     * selector with each siteId and then some other value for the node-level stats (PLANNER stats uses -1).
     * This call will automatically aggregate every StatsSource registered for every 'site'ID for that selector.
     *
     * @param selector @Statistics selector keyword
     * @param interval true if processing a reporting interval
     * @param now      current timestamp
     * @return statistics VoltTable results
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
            final Long now) {
        assert selector != null;
        Map<Long, Set<StatsSource>> siteIdToStatsSources = m_registeredStatsSources.get(selector);

        // There are cases early in rejoin where we can get polled before the server is ready to provide
        // stats.  Just return null for now, which will result in no tables from this node.
        if (siteIdToStatsSources == null || siteIdToStatsSources.isEmpty()) {
            return null;
        }

        VoltTable resultTable = null;

        for (Set<StatsSource> statsSources : siteIdToStatsSources.values()) {
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
                        resultTable = new VoltTable(columns.toArray(new ColumnInfo[0]));
                    }
                    Object[][] statsRows = ss.getStatsRows(interval, now);
                    for (Object[] row : statsRows) {
                        if (row.length != resultTable.m_colCount) {
                            new VoltLogger("HOST").warn("Dump ss: " + ss + ": " + selector);
                        }
                        resultTable.addRow(row);
                    }
                }
            }
        }
        return resultTable;
    }
}
