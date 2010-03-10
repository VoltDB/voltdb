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

package org.voltdb.sysprocs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.voltdb.HsqlBackend;
import org.voltdb.BackendTarget;
import org.voltdb.DependencyPair;
import org.voltdb.ExecutionSite;
import org.voltdb.VoltDB;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.DtxnConstants;

@ProcInfo(
    // partitionInfo = "TABLE.ATTR: 0",
    singlePartition = false
)

public class Statistics extends VoltSystemProcedure {

    static final int DEP_tableData = (int)
        SysProcFragmentId.PF_tableData | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_tableAggregator = (int) SysProcFragmentId.PF_tableAggregator;

    static final int DEP_procedureData = (int)
        SysProcFragmentId.PF_procedureData | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_procedureAggregator = (int)
        SysProcFragmentId.PF_procedureAggregator;

    static final int DEP_initiatorData = (int)
        SysProcFragmentId.PF_initiatorData | DtxnConstants.MULTINODE_DEPENDENCY;
    static final int DEP_initiatorAggregator = (int)
        SysProcFragmentId.PF_initiatorAggregator;

    static final int DEP_partitionCount = (int)
        SysProcFragmentId.PF_partitionCount;
//    static final int DEP_initiatorAggregator = (int)
//        SysProcFragmentId.PF_initiatorAggregator;

    @Override
    public void init(ExecutionSite site, Procedure catProc,
                     BackendTarget eeType, HsqlBackend hsql, Cluster cluster) {
        super.init(site, catProc, eeType, hsql, cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_tableData, this);
        site.registerPlanFragment(SysProcFragmentId.PF_tableAggregator, this);
        site.registerPlanFragment(SysProcFragmentId.PF_procedureData, this);
        site.registerPlanFragment(SysProcFragmentId.PF_procedureAggregator,
                                  this);
        site.registerPlanFragment(SysProcFragmentId.PF_initiatorData, this);
        site.registerPlanFragment(SysProcFragmentId.PF_initiatorAggregator,
                                  this);
        site.registerPlanFragment(SysProcFragmentId.PF_partitionCount,
                this);
    }

    @Override
    public DependencyPair executePlanFragment(HashMap<Integer, List<VoltTable>> dependencies,
                                                  long fragmentId,
                                                  ParameterSet params,
                                                  ExecutionSite.SystemProcedureExecutionContext context)
    {
        //  TABLE statistics
        if (fragmentId == SysProcFragmentId.PF_tableData) {
            // create an array of the table ids for which statistics are required.
            // pass this to EE owned by the execution site running this plan fragment.
            CatalogMap<Table> tables = context.getDatabase().getTables();
            int[] tableGuids = new int[tables.size()];
            int ii = 0;
            for (Table table : tables) {
                tableGuids[ii++] = table.getRelativeIndex();
            }
            VoltTable result = context.getExecutionEngine().getStats(SysProcSelector.TABLE, tableGuids)[0];
            return new DependencyPair(DEP_tableData, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_tableAggregator) {
            VoltTable result = null;

            // create an output table from the schema of a dependency table
            // (they're all the same). Iterate the dependencies and add
            // all rows from each to the output.
            List<VoltTable> dep = dependencies.get(DEP_tableData);
            VoltTable vt = dep.get(0);
            if (vt != null) {
                VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[vt.getColumnCount()];
                for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                    columns[ii] = new VoltTable.ColumnInfo(vt.getColumnName(ii), vt.getColumnType(ii));
                }
                result = new VoltTable(columns);
                for (Object d : dep) {
                    vt = (VoltTable) (d);
                    while (vt.advanceRow()) {
                        // this adds the active row of vt
                        result.add(vt);
                    }
                }
            }
            return new DependencyPair(DEP_tableAggregator, result);
        }

        //  PROCEDURE statistics
        else if (fragmentId == SysProcFragmentId.PF_procedureData) {
            // procedure stats are registered to VoltDB's statsagent with the site's catalog id.
            // piece this information together and the stats agent returns a table. pretty sweet.
            final boolean resetCounters =
                ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
            ArrayList<Integer> catalogIds = new ArrayList<Integer>();
            catalogIds.add(Integer.parseInt(context.getSite().getTypeName()));
            VoltTable result = VoltDB.instance().
                    getStatsAgent().getStats(
                            SysProcSelector.PROCEDURE,
                            catalogIds,
                            resetCounters);
            return new DependencyPair(DEP_procedureData, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_procedureAggregator) {
            VoltTable result = null;

            // create an output table from the schema of a dependency table
            // (they'll be the same). Then iterate the dependencies and add
            // all rows from each to the output.
            List<VoltTable> dep = dependencies.get(DEP_procedureData);
            VoltTable vt = dep.get(0);
            if (vt != null) {
                VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[vt.getColumnCount()];
                for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                    columns[ii] = new VoltTable.ColumnInfo(vt.getColumnName(ii), vt.getColumnType(ii));
                }
                result = new VoltTable(columns);
                for (Object d : dep) {
                    vt = (VoltTable) (d);
                    while (vt.advanceRow()) {
                        // this adds the active row of vt
                        result.add(vt);
                    }
                }
            }
            return new DependencyPair(DEP_procedureAggregator, result);
        }

        //INITIATOR statistics
        else if (fragmentId == SysProcFragmentId.PF_initiatorData) {
            // initiator stats are registered to VoltDB's statsagent with the initiators index.
            // piece this information together and the stats agent returns a table. pretty sweet.
            final boolean resetCounters =
                ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
            ArrayList<Integer> catalogIds = new ArrayList<Integer>();
            catalogIds.add(0);
            VoltTable result = VoltDB.instance().
                    getStatsAgent().getStats(
                            SysProcSelector.INITIATOR,
                            catalogIds,
                            resetCounters);
            return new DependencyPair(DEP_initiatorData, result);
        }
        else if (fragmentId == SysProcFragmentId.PF_initiatorAggregator) {
            VoltTable result = null;

            // create an output table from the schema of a dependency table
            // (they'll be the same). Then iterate the dependencies and add
            // all rows from each to the output.
            List<VoltTable> dep = dependencies.get(DEP_initiatorData);
            VoltTable vt = dep.get(0);
            if (vt != null) {
                VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[vt.getColumnCount()];
                for (int ii = 0; ii < vt.getColumnCount(); ii++) {
                    columns[ii] = new VoltTable.ColumnInfo(vt.getColumnName(ii), vt.getColumnType(ii));
                }
                result = new VoltTable(columns);
                for (Object d : dep) {
                    vt = (VoltTable) (d);
                    while (vt.advanceRow()) {
                        // this adds the active row of vt
                        result.add(vt);
                    }
                }
            }
            return new DependencyPair(DEP_initiatorAggregator, result);
        } else if (fragmentId == SysProcFragmentId.PF_partitionCount) {
            VoltTable result = new VoltTable(new VoltTable.ColumnInfo("Partition count", VoltType.INTEGER));
            result.addRow(context.getCluster().getPartitions().size());
            return new DependencyPair(DEP_partitionCount, result);
        }

        assert (false);
        return null;
    }

    public VoltTable[] run(String selector, long resetCounters) throws VoltAbortException {
        VoltTable[] results;

        if (selector.toUpperCase().equals(SysProcSelector.TABLE.name())) {
            SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
            // create a work fragment to gather table data from each of the sites.
            pfs[1] = new SynthesizedPlanFragment();
            pfs[1].fragmentId = SysProcFragmentId.PF_tableData;
            pfs[1].outputDepId = DEP_tableData;
            pfs[1].inputDepIds = new int[]{};
            pfs[1].multipartition = true;
            pfs[1].nonExecSites = false;
            pfs[1].parameters = new ParameterSet();

            // create a work fragment to aggregate the results.
            // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
            pfs[0] = new SynthesizedPlanFragment();
            pfs[0].fragmentId = SysProcFragmentId.PF_tableAggregator;
            pfs[0].outputDepId = DEP_tableAggregator;
            pfs[0].inputDepIds = new int[]{DEP_tableData};
            pfs[0].multipartition = false;
            pfs[0].nonExecSites = false;
            pfs[0].parameters = new ParameterSet();

            // distribute and execute these fragments providing pfs and id of the
            // aggregator's output dependency table.
            results =
                executeSysProcPlanFragments(pfs, DEP_tableAggregator);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.PROCEDURE.name())) {
            SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
            // create a work fragment to gather procedure data from each of the sites.
            pfs[1] = new SynthesizedPlanFragment();
            pfs[1].fragmentId = SysProcFragmentId.PF_procedureData;
            pfs[1].outputDepId = DEP_procedureData;
            pfs[1].inputDepIds = new int[]{};
            pfs[1].multipartition = true;
            pfs[1].nonExecSites = false;
            pfs[1].parameters = new ParameterSet();
            pfs[1].parameters.setParameters((byte)resetCounters);

            // create a work fragment to aggregate the results.
            // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
            pfs[0] = new SynthesizedPlanFragment();
            pfs[0].fragmentId = SysProcFragmentId.PF_procedureAggregator;
            pfs[0].outputDepId = DEP_procedureAggregator;
            pfs[0].inputDepIds = new int[]{DEP_procedureData};
            pfs[0].multipartition = false;
            pfs[0].nonExecSites = false;
            pfs[0].parameters = new ParameterSet();

            // distribute and execute these fragments providing pfs and id of the
            // aggregator's output dependency table.
            results =
                executeSysProcPlanFragments(pfs, DEP_procedureAggregator);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.INITIATOR.name())) {
            SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
            // create a work fragment to gather initiator data from each of the sites.
            pfs[1] = new SynthesizedPlanFragment();
            pfs[1].fragmentId = SysProcFragmentId.PF_initiatorData;
            pfs[1].outputDepId = DEP_initiatorData;
            pfs[1].inputDepIds = new int[]{};
            pfs[1].multipartition = false;
            pfs[1].nonExecSites = true;
            pfs[1].parameters = new ParameterSet();
            pfs[1].parameters.setParameters((byte)resetCounters);

            // create a work fragment to aggregate the results.
            // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
            pfs[0] = new SynthesizedPlanFragment();
            pfs[0].fragmentId = SysProcFragmentId.PF_initiatorAggregator;
            pfs[0].outputDepId = DEP_initiatorAggregator;
            pfs[0].inputDepIds = new int[]{DEP_initiatorData};
            pfs[0].multipartition = false;
            pfs[0].nonExecSites = false;
            pfs[0].parameters = new ParameterSet();

            // distribute and execute these fragments providing pfs and id of the
            // aggregator's output dependency table.
            results =
                executeSysProcPlanFragments(pfs, DEP_initiatorAggregator);
        } else if (selector.toUpperCase().equals(SysProcSelector.PARTITIONCOUNT.name())) {
            SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[1];
            // create a work fragment to gather the partition count the catalog.
            pfs[0] = new SynthesizedPlanFragment();
            pfs[0].fragmentId = SysProcFragmentId.PF_partitionCount;
            pfs[0].outputDepId = DEP_partitionCount;
            pfs[0].inputDepIds = new int[]{};
            pfs[0].multipartition = false;
            pfs[0].nonExecSites = false;
            pfs[0].parameters = new ParameterSet();

            results =
                executeSysProcPlanFragments(pfs, DEP_partitionCount);
        }
        else {
            throw new VoltAbortException("Invalid Statistics selector.");
        }

        return results;
    }

}
