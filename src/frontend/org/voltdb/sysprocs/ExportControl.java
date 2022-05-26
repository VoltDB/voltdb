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

package org.voltdb.sysprocs;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.hsqldb_voltpatches.lib.StringUtil;
import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.export.StreamControlOperation;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltTableUtil;

public class ExportControl extends VoltSystemProcedure {

    private static final VoltLogger LOG = new VoltLogger("EXPORT");

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
                VoltDB.getExportManager().processExportControl(exportSource, exportTargets,
                        StreamControlOperation.valueOf(operationMode.toUpperCase()), results);
            }

            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_exportControl, results);
        } else if (fragmentId == SysProcFragmentId.PF_exportControlAggregate) {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(SysProcFragmentId.PF_exportControl));
            return new DependencyPair.TableDependencyPair(SysProcFragmentId.PF_exportControlAggregate, result);
        }
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String streamName, String[] targets, String operationMode) throws Exception {
        VoltTable results = new VoltTable(
                new ColumnInfo("SOURCE", VoltType.STRING),
                new ColumnInfo("TARGET", VoltType.STRING),
                new ColumnInfo("PARTITIONID", VoltType.BIGINT),
                new ColumnInfo("STATUS", VoltType.STRING),
                new ColumnInfo("MESSAGE", VoltType.STRING));
        try {
            StreamControlOperation.valueOf(operationMode.toUpperCase());
        } catch (IllegalArgumentException e){
            results.addRow("", "", -1, "FAILURE", e.getMessage());
            return new VoltTable[] {results};
        }

        streamName = streamName == null ? "" : streamName;
        if (!StringUtil.isEmpty(streamName)) {
            if (!CatalogUtil.isExportTable(ctx.getDatabase(), streamName)) {
                results.addRow(streamName, "", -1,"FAILURE", "Export stream " + streamName + " does not exist.");
                return new VoltTable[] {results};
            }
        }

        if (targets.length == 0 || Arrays.stream(targets).allMatch(s -> StringUtil.isEmpty(s))) {
            results.addRow(streamName, "", -1,"FAILURE", "Target list is empty");
            return new VoltTable[] {results};
        }

        LOG.info("Export " + operationMode + " source:" + streamName + " targets:" + Arrays.toString(targets));
        return performExportControl(streamName, targets, operationMode);
    }

    private final VoltTable[] performExportControl(String exportSource,
            String[] exportTargets, String operationMode) {
        return createAndExecuteSysProcPlan(SysProcFragmentId.PF_exportControl,
                SysProcFragmentId.PF_exportControlAggregate, exportSource, exportTargets, operationMode);
    }
}
