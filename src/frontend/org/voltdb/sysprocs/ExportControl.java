/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.sysprocs;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.hsqldb_voltpatches.lib.StringUtil;
import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.RealVoltDB;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.export.ExportManager;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltTableUtil;

public class ExportControl extends VoltSystemProcedure {

    private static final VoltLogger LOG = new VoltLogger("EXPORT");

    // support operations
    public static enum OperationMode{ RELEASE
                       //PAUSE, RESUME, TRUNCATE //for future use
    }

    private static final int DEP_exportalControl = (int)
            SysProcFragmentId.PF_exportControl | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEF_exportControlAggregate = (int)
            SysProcFragmentId.PF_exportControlAggregate;


    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{
                SysProcFragmentId.PF_exportControl,
                SysProcFragmentId.PF_exportControlAggregate
        };
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        if (fragmentId == SysProcFragmentId.PF_exportControl) {
            VoltTable results = new VoltTable(
                    new ColumnInfo("SOURCE", VoltType.STRING),
                    new ColumnInfo("TARGET", VoltType.STRING),
                    new ColumnInfo("PARTITIONID", VoltType.BIGINT),
                    new ColumnInfo("STATUS", VoltType.STRING),
                    new ColumnInfo("MESSAGE", VoltType.STRING));

            if (context.isLowestSiteId()) {
                assert(params.toArray()[0] != null);
                assert(params.toArray()[1] != null);
                assert(params.toArray()[2] != null);
                final String exportSource = (String) params.toArray()[0];
                final String[] targets = (String[]) params.toArray()[1];
                final String operationMode = (String) params.toArray()[2];
                List<String> exportTargets = Arrays.asList(targets).stream().
                        filter(s -> (!StringUtil.isEmpty(s))).collect(Collectors.toList());
                ExportManager.instance().processStreamControl(exportSource, exportTargets,
                        OperationMode.valueOf(operationMode.toUpperCase()), results);
            }

            return new DependencyPair.TableDependencyPair(DEP_exportalControl, results);
        } else if (fragmentId == SysProcFragmentId.PF_exportControlAggregate) {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_exportalControl));
            return new DependencyPair.TableDependencyPair(DEF_exportControlAggregate, result);
        }
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String exportSource, String[] targets, String operationMode) throws Exception {
        VoltTable results = new VoltTable(
                new ColumnInfo("SOURCE", VoltType.STRING),
                new ColumnInfo("TARGET", VoltType.STRING),
                new ColumnInfo("PARTITIONID", VoltType.BIGINT),
                new ColumnInfo("STATUS", VoltType.STRING),
                new ColumnInfo("MESSAGE", VoltType.STRING));
        try {
            OperationMode.valueOf(operationMode.toUpperCase());
        } catch (IllegalArgumentException e){
            results.addRow("", "", -1, "FAILURE", e.getMessage());
            return new VoltTable[] {results};
        }

        exportSource = exportSource == null ? "" : exportSource;
        LOG.info("Export " + operationMode + " source:" + exportSource + " targets:" + Arrays.toString(targets));
        if (!StringUtil.isEmpty(exportSource)) {
            RealVoltDB volt = (RealVoltDB)VoltDB.instance();
            Set<String> exportStreams = CatalogUtil.getExportTableNames( volt.getCatalogContext().database);
            boolean isThere = exportStreams.stream().anyMatch(exportSource::equalsIgnoreCase);
            if (!isThere) {
                results.addRow(exportSource, "", -1,"FAILURE", "Export stream " + exportSource + " does not exist.");
                return new VoltTable[] {results};
            }
        }
        return performExportControl(exportSource, targets, operationMode);
    }

    private final VoltTable[] performExportControl(String exportSource,
            String[] exportTargets, String operationMode) {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_exportControl;
        pfs[0].outputDepId = DEP_exportalControl;
        pfs[0].inputDepIds = new int[] {};
        pfs[0].multipartition = true;
        pfs[0].parameters = ParameterSet.fromArrayNoCopy(
                exportSource, exportTargets, operationMode);

        // This fragment aggregates the results of creating those files
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_exportControlAggregate;
        pfs[1].outputDepId = DEF_exportControlAggregate;
        pfs[1].inputDepIds = new int[] { DEP_exportalControl };
        pfs[1].multipartition = false;
        pfs[1].parameters = ParameterSet.emptyParameterSet();

        return executeSysProcPlanFragments(pfs, DEF_exportControlAggregate);
    }
}
