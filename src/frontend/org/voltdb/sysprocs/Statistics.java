/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.sysprocs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.BackendTarget;
import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.ClientInterface;
import org.voltdb.HsqlBackend;
import org.voltdb.LiveClientStats;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.Pair;

/**
 * Access the TABLE, PRCOEDURE, INITIATOR, IOSTATS, or PARTITIONCOUNT statistics.
 */
@ProcInfo(
    // partitionInfo = "TABLE.ATTR: 0",
    singlePartition = false
)

public class Statistics extends VoltSystemProcedure {

    private static final VoltLogger HOST_LOG = new VoltLogger("HOST");

    static final int DEP_nodeMemory = (int)
        SysProcFragmentId.PF_nodeMemory | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_nodeMemoryAggregator = (int) SysProcFragmentId.PF_nodeMemoryAggregator;

    static final int DEP_tableData = (int)
        SysProcFragmentId.PF_tableData | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_tableAggregator = (int) SysProcFragmentId.PF_tableAggregator;

    static final int DEP_indexData = (int)
        SysProcFragmentId.PF_indexData | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_indexAggregator = (int) SysProcFragmentId.PF_indexAggregator;

    static final int DEP_procedureData = (int)
        SysProcFragmentId.PF_procedureData | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_procedureAggregator = (int)
        SysProcFragmentId.PF_procedureAggregator;

    static final int DEP_initiatorData = (int)
        SysProcFragmentId.PF_initiatorData | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_initiatorAggregator = (int)
        SysProcFragmentId.PF_initiatorAggregator;

    static final int DEP_ioData = (int)
        SysProcFragmentId.PF_ioData | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_ioDataAggregator = (int)
        SysProcFragmentId.PF_ioDataAggregator;

    static final int DEP_starvationData = (int)
    SysProcFragmentId.PF_starvationData | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_starvationDataAggregator = (int)
        SysProcFragmentId.PF_starvationDataAggregator;

    static final int DEP_liveClientData = (int)
        SysProcFragmentId.PF_liveClientData | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_liveClientDataAggregator = (int)
        SysProcFragmentId.PF_liveClientDataAggregator;

    static final int DEP_partitionCount = (int)
        SysProcFragmentId.PF_partitionCount;
//    static final int DEP_initiatorAggregator = (int)
//        SysProcFragmentId.PF_initiatorAggregator;

    @Override
        public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql, Cluster cluster) {

        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);

        site.registerPlanFragment(SysProcFragmentId.PF_tableData, this);
        site.registerPlanFragment(SysProcFragmentId.PF_tableAggregator, this);
        site.registerPlanFragment(SysProcFragmentId.PF_indexData, this);
        site.registerPlanFragment(SysProcFragmentId.PF_indexAggregator, this);
        site.registerPlanFragment(SysProcFragmentId.PF_nodeMemory, this);
        site.registerPlanFragment(SysProcFragmentId.PF_nodeMemoryAggregator, this);
        site.registerPlanFragment(SysProcFragmentId.PF_procedureData, this);
        site.registerPlanFragment(SysProcFragmentId.PF_procedureAggregator, this);
        site.registerPlanFragment(SysProcFragmentId.PF_initiatorData, this);
        site.registerPlanFragment(SysProcFragmentId.PF_initiatorAggregator, this);
        site.registerPlanFragment(SysProcFragmentId.PF_partitionCount, this);
        site.registerPlanFragment(SysProcFragmentId.PF_ioData, this);
        site.registerPlanFragment(SysProcFragmentId.PF_ioDataAggregator, this);
        site.registerPlanFragment(SysProcFragmentId.PF_starvationData, this);
        site.registerPlanFragment(SysProcFragmentId.PF_starvationDataAggregator, this);
        site.registerPlanFragment(SysProcFragmentId.PF_liveClientData, this);
        site.registerPlanFragment(SysProcFragmentId.PF_liveClientDataAggregator, this);
    }

    @Override
    public DependencyPair executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies,
                                                  long fragmentId,
                                                  ParameterSet params,
                                                  ExecutionSite.SystemProcedureExecutionContext context)
    {
        //  TABLE statistics
        if (fragmentId == SysProcFragmentId.PF_tableData) {
            assert(params.toArray().length == 2);
            final boolean interval =
                ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
            final Long now = (Long)params.toArray()[1];
            // create an array of the table ids for which statistics are required.
            // pass this to EE owned by the execution site running this plan fragment.
            CatalogMap<Table> tables = context.getDatabase().getTables();
            int[] tableGuids = new int[tables.size()];
            int ii = 0;
            for (Table table : tables) {
                tableGuids[ii++] = table.getRelativeIndex();
            }
            VoltTable result =
                context.getExecutionEngine().getStats(
                        SysProcSelector.TABLE,
                        tableGuids,
                        interval,
                        now)[0];
            return new DependencyPair(DEP_tableData, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_tableAggregator) {
            VoltTable result = unionTables(dependencies.get(DEP_tableData));
            return new DependencyPair(DEP_tableAggregator, result);
        }

        //  INDEX statistics
        else if (fragmentId == SysProcFragmentId.PF_indexData) {
            assert(params.toArray().length == 2);
            final boolean interval =
                ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
            final Long now = (Long)params.toArray()[1];
            // create an array of the table ids for which index statistics are required.
            // pass this to EE owned by the execution site running this plan fragment.
            CatalogMap<Table> tables = context.getDatabase().getTables();
            int[] tableGuids = new int[tables.size()];
            int ii = 0;
            for (Table table : tables) {
                tableGuids[ii++] = table.getRelativeIndex();
            }
            VoltTable result =
                context.getExecutionEngine().getStats(
                        SysProcSelector.INDEX,
                        tableGuids,
                        interval,
                        now)[0];
            return new DependencyPair(DEP_indexData, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_indexAggregator) {
            VoltTable result = unionTables(dependencies.get(DEP_indexData));
            return new DependencyPair(DEP_indexAggregator, result);
        }

        //  PROCEDURE statistics
        else if (fragmentId == SysProcFragmentId.PF_procedureData) {
            // procedure stats are registered to VoltDB's statsagent with the site's catalog id.
            // piece this information together and the stats agent returns a table. pretty sweet.
            assert(params.toArray().length == 2);
            final boolean interval =
                ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
            final Long now = (Long)params.toArray()[1];
            ArrayList<Integer> catalogIds = new ArrayList<Integer>();
            catalogIds.add(Integer.parseInt(context.getSite().getTypeName()));
            VoltTable result = VoltDB.instance().
                    getStatsAgent().getStats(
                            SysProcSelector.PROCEDURE,
                            catalogIds,
                            interval,
                            now);
            return new DependencyPair(DEP_procedureData, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_procedureAggregator) {
            VoltTable result = unionTables(dependencies.get(DEP_procedureData));
            return new DependencyPair(DEP_procedureAggregator, result);
        }
        //  STARVATION statistics
        else if (fragmentId == SysProcFragmentId.PF_starvationData) {
            // starvation stats are registered to VoltDB's statsagent with the site's catalog id.
            // piece this information together and the stats agent returns a table. pretty sweet.
            assert(params.toArray().length == 2);
            final boolean interval =
                ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
            final Long now = (Long)params.toArray()[1];
            ArrayList<Integer> catalogIds = new ArrayList<Integer>();
            catalogIds.add(Integer.parseInt(context.getSite().getTypeName()));
            VoltTable result = VoltDB.instance().
                    getStatsAgent().getStats(
                            SysProcSelector.STARVATION,
                            catalogIds,
                            interval,
                            now);
            return new DependencyPair(DEP_starvationData, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_starvationDataAggregator) {
            VoltTable result = unionTables(dependencies.get(DEP_starvationData));
            return new DependencyPair(DEP_starvationDataAggregator, result);
        }
        //INITIATOR statistics
        else if (fragmentId == SysProcFragmentId.PF_initiatorData) {
            // initiator stats are registered to VoltDB's statsagent with the initiators index.
            // piece this information together and the stats agent returns a table. pretty sweet.
            assert(params.toArray().length == 2);
            final boolean interval =
                ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
            final Long now = (Long)params.toArray()[1];
            ArrayList<Integer> catalogIds = new ArrayList<Integer>();
            catalogIds.add(0);
            VoltTable result = VoltDB.instance().
            getStatsAgent().getStats(
                                     SysProcSelector.INITIATOR,
                                     catalogIds,
                                     interval,
                                     now);

            // Choose the lowest site ID on this host to do the scan
            // All other sites should just return empty results tables.
            int host_id = context.getExecutionSite().getCorrespondingHostId();
            Integer lowest_site_id =
                VoltDB.instance().getCatalogContext().siteTracker.
                getLowestLiveExecSiteIdForHost(host_id);
            if (context.getExecutionSite().getSiteId() != lowest_site_id)
            {
                // Hacky way to generate an empty table with the correct schema
                result.clearRowData();
            }
            return new DependencyPair(DEP_initiatorData, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_initiatorAggregator) {
            VoltTable result = unionTables(dependencies.get(DEP_initiatorData));
            return new DependencyPair(DEP_initiatorAggregator, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_nodeMemory) {
            assert(params.toArray().length == 2);
            final boolean interval =
                ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
            final Long now = (Long)params.toArray()[1];
            ArrayList<Integer> catalogIds = new ArrayList<Integer>();
            catalogIds.add(0);
            VoltTable result =
                VoltDB.instance().getStatsAgent().getStats(SysProcSelector.MEMORY,
                                                           catalogIds,
                                                           interval,
                                                           now);

            // Choose the lowest site ID on this host to do the scan
            // All other sites should just return empty results tables.
            int hostId = context.getExecutionSite().getCorrespondingHostId();
            Integer lowestSiteId =
                VoltDB.instance().getCatalogContext().siteTracker.
                getLowestLiveExecSiteIdForHost(hostId);
            if (context.getExecutionSite().getSiteId() == lowestSiteId) {
                assert(result.getRowCount() == 1);
            }
            else {
                // Hacky way to generate an empty table with the correct schema
                result.clearRowData();
                assert(result.getRowCount() == 0);
            }
            return new DependencyPair(DEP_nodeMemory, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_nodeMemoryAggregator) {
            VoltTable result = unionTables(dependencies.get(DEP_nodeMemory));
            return new DependencyPair(DEP_nodeMemoryAggregator, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_partitionCount) {
            VoltTable result = new VoltTable(new VoltTable.ColumnInfo("PARTITION_COUNT", VoltType.INTEGER));
            result.addRow(context.getCluster().getPartitions().size());
            return new DependencyPair(DEP_partitionCount, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_ioData) {
            final VoltTable result = new VoltTable(ioColumnInfo);
            // Choose the lowest site ID on this host to do the scan
            // All other sites should just return empty results tables.
            int host_id = context.getExecutionSite().getCorrespondingHostId();
            Integer lowest_site_id =
                VoltDB.instance().getCatalogContext().siteTracker.
                getLowestLiveExecSiteIdForHost(host_id);
            if (context.getExecutionSite().getSiteId() == lowest_site_id)
            {
                assert(params.toArray() != null);
                assert(params.toArray().length == 2);
                final boolean interval =
                    ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
                final Long now = (Long)params.toArray()[1];
                final Map<Long, Pair<String,long[]>> stats =
                    VoltDB.instance().getNetwork().getIOStats(interval);

                final Integer hostId = VoltDB.instance().getHostMessenger().getHostId();
                final String hostname = VoltDB.instance().getHostMessenger().getHostname();
                for (Map.Entry<Long, Pair<String, long[]>> e : stats.entrySet()) {
                    final Long connectionId = e.getKey();
                    final String remoteHostname = e.getValue().getFirst();
                    final long counters[] = e.getValue().getSecond();
                    result.addRow(
                                  now,
                                  hostId,
                                  hostname,
                                  connectionId,
                                  remoteHostname,
                                  counters[0],
                                  counters[1],
                                  counters[2],
                                  counters[3]);
                }
            }
            return new DependencyPair(DEP_ioData, result);
        } else if (fragmentId == SysProcFragmentId.PF_ioDataAggregator) {
            final VoltTable result = new VoltTable(ioColumnInfo);
            List<VoltTable> dep = dependencies.get(DEP_ioData);
            for (VoltTable t : dep) {
                while (t.advanceRow()) {
                    result.add(t);
                }
            }
            return new DependencyPair(DEP_ioDataAggregator, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_liveClientData) {
            // Choose the lowest site ID on this host to do the scan
            // All other sites should just return empty results tables.
            int hostId = context.getExecutionSite().getCorrespondingHostId();
            Integer lowestSiteId =
                VoltDB.instance().getCatalogContext().siteTracker.
                getLowestLiveExecSiteIdForHost(hostId);
            VoltTable result = new VoltTable(LiveClientStats.liveClientColumnInfo);
            if (context.getExecutionSite().getSiteId() == lowestSiteId) {
                assert(params.toArray().length == 2);
                final Long now = (Long)params.toArray()[1];
                Map<Long, Pair<String,long[]>> stats =
                    new HashMap<Long, Pair<String, long[]>>();
                for (ClientInterface ci : VoltDB.instance().getClientInterfaces())
                {
                    stats.putAll(ci.getLiveClientStats());
                }

                final String hostname = VoltDB.instance().getHostMessenger().getHostname();
                for (Map.Entry<Long, Pair<String, long[]>> e : stats.entrySet()) {
                    final Long connectionId = e.getKey();
                    final String remoteHostname = e.getValue().getFirst();
                    final long counters[] = e.getValue().getSecond();
                    result.addRow(
                                  now,
                                  hostId,
                                  hostname,
                                  connectionId,
                                  remoteHostname,
                                  counters[0],
                                  counters[1],
                                  counters[2],
                                  counters[3]);
                }
            }
            return new DependencyPair(DEP_liveClientData, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_liveClientDataAggregator) {
            VoltTable result = unionTables(dependencies.get(DEP_liveClientData));
            return new DependencyPair(DEP_liveClientDataAggregator, result);
        }
        assert (false);
        return null;
    }

    private static final ColumnInfo ioColumnInfo[] = new ColumnInfo[] {
        new ColumnInfo( "TIMESTAMP", VoltType.BIGINT),
        new ColumnInfo( VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID),
        new ColumnInfo( "HOSTNAME", VoltType.STRING),
        new ColumnInfo( "CONNECTION_ID", VoltType.BIGINT),
        new ColumnInfo( "CONNECTION_HOSTNAME", VoltType.STRING),
        new ColumnInfo( "BYTES_READ", VoltType.BIGINT),
        new ColumnInfo( "MESSAGES_READ", VoltType.BIGINT),
        new ColumnInfo( "BYTES_WRITTEN", VoltType.BIGINT),
        new ColumnInfo( "MESSAGES_WRITTEN", VoltType.BIGINT)
    };


    /**
     * Returns a table stats.
     * requested.
     * @param ctx          Internal. Not exposed to the end-user.
     * @param selector     Selector requested TABLE, PROCEDURE, INITIATOR,
     *                     PARTITIONCOUNT, IOSTATS, MANAGEMENT, INDEX
     * @param interval     1 for interval statistics. 0 for full statistics.
     * @return             The returned schema is specific to the selector.
     * @throws VoltAbortException
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx,
            String selector, long interval) throws VoltAbortException
    {
        VoltTable[] results;
        final long now = System.currentTimeMillis();
        if ((selector.toUpperCase().equals(SysProcSelector.MEMORY.name())) ||
            (selector.toUpperCase().equals("NODEMEMORY"))) {
            results = getMemoryData(interval, now);
            assert(results.length == 1);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.TABLE.name())) {
            results = getTableData(interval, now);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.INDEX.name())) {
            results = getIndexData(interval, now);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.PROCEDURE.name())) {
            results = getProcedureData(interval, now);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.INITIATOR.name())) {
            results = getInitiatorData(interval, now);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.PARTITIONCOUNT.name())) {
            results = getPartitionCountData();
        }
        else if (selector.toUpperCase().equals(SysProcSelector.IOSTATS.name())) {
            results = getIOStatsData(interval, now);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.STARVATION.name())) {
            results = getStarvationData(interval, now);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.LIVECLIENTS.name())) {
            results = getLiveClientData(interval, now);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.MANAGEMENT.name())) {
            VoltTable[] memoryResults = getMemoryData(interval, now);
            VoltTable[] tableResults = getTableData(interval, now);
            VoltTable[] indexResults = getIndexData(interval, now);
            VoltTable[] procedureResults = getProcedureData(interval, now);
            VoltTable[] initiatorResults = getInitiatorData(interval, now);
            VoltTable[] ioResults = getIOStatsData(interval, now);
            VoltTable[] starvationResults = getIOStatsData(interval, now);
            results = new VoltTable[] {
                    memoryResults[0],
                    initiatorResults[0],
                    procedureResults[0],
                    ioResults[0],
                    tableResults[0],
                    indexResults[0],
                    starvationResults[0]
            };
            final long endTime = System.currentTimeMillis();
            final long delta = endTime - now;
            HOST_LOG.debug("Statistics invocation of MANAGEMENT selector took " + delta + " milliseconds");
        } else {
            throw new VoltAbortException(String.format("Invalid Statistics selector %s.", selector));
        }

        return results;
    }

    private VoltTable[] getIOStatsData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather initiator data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_ioData;
        pfs[1].outputDepId = DEP_ioData;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_ioDataAggregator;
        pfs[0].outputDepId = DEP_ioDataAggregator;
        pfs[0].inputDepIds = new int[]{DEP_ioData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results =
            executeSysProcPlanFragments(pfs, DEP_ioDataAggregator);
        return results;
    }

    private VoltTable[] getPartitionCountData() {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[1];
        // create a work fragment to gather the partition count the catalog.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_partitionCount;
        pfs[0].outputDepId = DEP_partitionCount;
        pfs[0].inputDepIds = new int[]{};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        results =
            executeSysProcPlanFragments(pfs, DEP_partitionCount);
        return results;
    }

    private VoltTable[] getInitiatorData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather initiator data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_initiatorData;
        pfs[1].outputDepId = DEP_initiatorData;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_initiatorAggregator;
        pfs[0].outputDepId = DEP_initiatorAggregator;
        pfs[0].inputDepIds = new int[]{DEP_initiatorData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results =
            executeSysProcPlanFragments(pfs, DEP_initiatorAggregator);
        return results;
    }

    private VoltTable[] getMemoryData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather node memory data
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_nodeMemory;
        pfs[1].outputDepId = DEP_nodeMemory;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_nodeMemoryAggregator;
        pfs[0].outputDepId = DEP_nodeMemoryAggregator;
        pfs[0].inputDepIds = new int[]{DEP_nodeMemory};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results =
            executeSysProcPlanFragments(pfs, DEP_nodeMemoryAggregator);
        return results;
    }

    private VoltTable[] getProcedureData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather procedure data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_procedureData;
        pfs[1].outputDepId = DEP_procedureData;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_procedureAggregator;
        pfs[0].outputDepId = DEP_procedureAggregator;
        pfs[0].inputDepIds = new int[]{DEP_procedureData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results =
            executeSysProcPlanFragments(pfs, DEP_procedureAggregator);
        return results;
    }

    private VoltTable[] getStarvationData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather procedure data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_starvationData;
        pfs[1].outputDepId = DEP_starvationData;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_starvationDataAggregator;
        pfs[0].outputDepId = DEP_starvationDataAggregator;
        pfs[0].inputDepIds = new int[]{DEP_starvationData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results =
            executeSysProcPlanFragments(pfs, DEP_starvationDataAggregator);
        return results;
    }

    private VoltTable[] getTableData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather table data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_tableData;
        pfs[1].outputDepId = DEP_tableData;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_tableAggregator;
        pfs[0].outputDepId = DEP_tableAggregator;
        pfs[0].inputDepIds = new int[]{DEP_tableData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results =
            executeSysProcPlanFragments(pfs, DEP_tableAggregator);
        return results;
    }

    private VoltTable[] getIndexData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather table data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_indexData;
        pfs[1].outputDepId = DEP_indexData;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_indexAggregator;
        pfs[0].outputDepId = DEP_indexAggregator;
        pfs[0].inputDepIds = new int[]{DEP_indexData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results = executeSysProcPlanFragments(pfs, DEP_indexAggregator);
        return results;
    }

    private VoltTable[] getLiveClientData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather live client data from each of the nodes
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_liveClientData;
        pfs[1].outputDepId = DEP_liveClientData;
        pfs[1].inputDepIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_liveClientDataAggregator;
        pfs[0].outputDepId = DEP_liveClientDataAggregator;
        pfs[0].inputDepIds = new int[]{DEP_liveClientData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results =
            executeSysProcPlanFragments(pfs, DEP_liveClientDataAggregator);
        return results;
    }
}
