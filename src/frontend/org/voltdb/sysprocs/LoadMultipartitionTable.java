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

import java.util.HashMap;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.DtxnConstants;

/**
 * Given as input a VoltTable with a schema corresponding to a persistent table,
 * partition the rows and insert into the appropriate persistent table
 * partitions(s). This system procedure does not generate undo data. Any
 * intermediate failure, for example a constraint violation, will leave partial
 * and inconsistent data in the persistent store.
 */
@ProcInfo(singlePartition = false)
public class LoadMultipartitionTable extends VoltSystemProcedure
{

    static final int DEP_distribute = (int) SysProcFragmentId.PF_distribute |
                                      DtxnConstants.MULTIPARTITION_DEPENDENCY;

    static final int DEP_aggregate = (int) SysProcFragmentId.PF_aggregate;

    private Cluster m_cluster = null;

    @Override
    public void init(int numberOfPartitions, SiteProcedureConnection site,
            Procedure catProc, BackendTarget eeType, HsqlBackend hsql,
            Cluster cluster) {
        super.init(numberOfPartitions, site, catProc, eeType, hsql, cluster);
        m_cluster = cluster;
        site.registerPlanFragment(SysProcFragmentId.PF_distribute, this);
        site.registerPlanFragment(SysProcFragmentId.PF_aggregate, this);
    }

    @Override
    public DependencyPair executePlanFragment(
            HashMap<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        // Return the standard status schema for sysprocs
        VoltTable result = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        result.addRow(VoltSystemProcedure.STATUS_OK);

        if (fragmentId == SysProcFragmentId.PF_distribute) {
            assert context.getCluster().getTypeName() != null;
            assert context.getDatabase().getTypeName() != null;
            assert params != null;
            assert params.toArray() != null;
            assert params.toArray()[0] != null;
            assert params.toArray()[1] != null;

            try {
                // voltLoadTable is void. Assume success or exception.
                super.voltLoadTable(context.getCluster().getTypeName(),
                                    context.getDatabase().getTypeName(),
                                    (String) (params.toArray()[0]),
                                    (VoltTable) (params.toArray()[1]));
            }
            catch (VoltAbortException e) {
                // must continue and reply with dependency.
                e.printStackTrace();
            }
            return new DependencyPair(DEP_distribute, result);

        } else if (fragmentId == SysProcFragmentId.PF_aggregate) {
            return new DependencyPair(DEP_aggregate, result);
        }
        // must handle every dependency id.
        assert (false);
        return null;
    }

    /**
     * These parameters, with the exception of ctx, map to user provided values.
     *
     * @param ctx
     *            Internal. Not a user-supplied parameter.
     * @param tableName
     *            Name of persistent table receiving data.
     * @param table
     *            A VoltTable with schema matching tableName containing data to
     *            load.
     * @return {@link org.voltdb.VoltSystemProcedure#STATUS_SCHEMA}
     * @throws VoltAbortException
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx,
            String tableName, VoltTable table)
            throws VoltAbortException {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[];

        // if tableName is replicated, just send table everywhere.
        // otherwise, create a VoltTable for each partition and
        // split up the incoming table .. then send those partial
        // tables to the appropriate sites.

        Table catTable = m_cluster.getDatabases().get("database").getTables()
                                  .getIgnoreCase(tableName);
        if (catTable == null) {
            throw new VoltAbortException("Table not present in catalog.");
        }
        if (catTable.getIsreplicated()) {
            pfs = new SynthesizedPlanFragment[2];

            // create a work unit to invoke super.loadTable() on each site.
            pfs[1] = new SynthesizedPlanFragment();
            pfs[1].fragmentId = SysProcFragmentId.PF_distribute;
            pfs[1].outputDepId = DEP_distribute;
            pfs[1].inputDepIds = new int[] {};
            pfs[1].multipartition = true;
            ParameterSet params = new ParameterSet();
            params.setParameters(tableName, table);
            pfs[1].parameters = params;

            // create a work unit to aggregate the results.
            // MULTIPARTION_DEPENDENCY bit set, requiring result from each site
            pfs[0] = new SynthesizedPlanFragment();
            pfs[0].fragmentId = SysProcFragmentId.PF_aggregate;
            pfs[0].outputDepId = DEP_aggregate;
            pfs[0].inputDepIds = new int[] { DEP_distribute };
            pfs[0].multipartition = false;
            pfs[0].parameters = new ParameterSet();

            // distribute and execute the fragments providing pfs and id
            // of the aggregator's output dependency table.
            results = executeSysProcPlanFragments(pfs, DEP_aggregate);
            return results;
        } else {
            // find the index and type of the partitioning attribute
            int partitionCol = catTable.getPartitioncolumn().getIndex();
            int intType = catTable.getPartitioncolumn().getType();
            VoltType partitionType = VoltType.get((byte) intType);

            // create a table for each partition
            int numPartitions = m_cluster.getPartitions().size();
            VoltTable partitionedTables[] = new VoltTable[numPartitions];
            for (int i = 0; i < partitionedTables.length; i++) {
                partitionedTables[i] = table.clone(1024 * 1024);
            }

            // split the input table into per-partition units
            while (table.advanceRow()) {
                int p = 0;
                try {
                    p = TheHashinator.hashToPartition(
                        table.get(partitionCol, partitionType));
                }
                catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e.getMessage());
                }
                // this adds the active row from table
                partitionedTables[p].add(table);
            }

            int num_exec_sites = VoltDB.instance().getCatalogContext().siteTracker.getLiveSiteCount();
            pfs = new SynthesizedPlanFragment[num_exec_sites + 1];
            int site_index = 0;
            for (Site site : VoltDB.instance().getCatalogContext().siteTracker.getUpSites()) {
                if (!site.getIsexec()) {
                    continue;
                }
                ParameterSet params = new ParameterSet();
                int site_id = Integer.valueOf(site.getTypeName());
                int partition = VoltDB.instance().getCatalogContext().siteTracker.getPartitionForSite(site_id);
                params.setParameters(tableName, partitionedTables[partition]);
                pfs[site_index] = new SynthesizedPlanFragment();
                pfs[site_index].fragmentId = SysProcFragmentId.PF_distribute;
                pfs[site_index].outputDepId = DEP_distribute;
                pfs[site_index].inputDepIds = new int[] {};
                pfs[site_index].multipartition = false;
                pfs[site_index].siteId = Integer.valueOf(site.getTypeName());
                pfs[site_index].parameters = params;
                site_index++;
            }
            // a final plan fragment to aggregate the results
            pfs[num_exec_sites] = new SynthesizedPlanFragment();
            pfs[num_exec_sites].fragmentId = SysProcFragmentId.PF_aggregate;
            pfs[num_exec_sites].inputDepIds = new int[] { DEP_distribute };
            pfs[num_exec_sites].outputDepId = DEP_aggregate;
            pfs[num_exec_sites].multipartition = false;
            pfs[num_exec_sites].parameters = new ParameterSet();

            // send these forth in to the world .. and wait
            results = executeSysProcPlanFragments(pfs, DEP_aggregate);
            return results;
        }

    }
}
