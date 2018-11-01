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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
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
            assert(params.toArray()[0] != null);
            assert(params.toArray()[1] != null);
            assert(params.toArray()[2] != null);
            final String exportSource = (String) params.toArray()[0];
            final String[] targets = (String[]) params.toArray()[1];
            final String operationMode = (String) params.toArray()[2];
            List<String> exportTargets = Arrays.asList(targets);
            LOG.info("Export " + operationMode + " source:" + exportSource + " targets:" + exportTargets);
            VoltTable results = new VoltTable(
                    new ColumnInfo("PARTITIONID", VoltType.BIGINT),
                    new ColumnInfo("SOURCE", VoltType.STRING),
                    new ColumnInfo("TARGET", VoltType.STRING),
                    new ColumnInfo("STATUS", VoltType.BIGINT),
                    new ColumnInfo("MESSAGE", VoltType.STRING));
            if (context.isLowestSiteId()) {
                ExportManager.instance().applyExportControl(exportSource, exportTargets, operationMode, results);
                if (results.getRowCount() == 0) {
                    RealVoltDB volt = (RealVoltDB)VoltDB.instance();
                    results.addRow(-1, "", "", VoltSystemProcedure.STATUS_OK,
                            "No control is applied on host " + volt.getHostMessenger().getHostname());
                }
            }

            return new DependencyPair.TableDependencyPair(DEP_exportalControl, results);
        } else if (fragmentId == SysProcFragmentId.PF_exportControlAggregate) {
            VoltTable result = VoltTableUtil.unionTables(dependencies.get(DEP_exportalControl));
            return new DependencyPair.TableDependencyPair(DEF_exportControlAggregate, result);
        }
        return null;
    }

    /**
     *
     * @param ctx
     * @param json  The json string which contains export source, targets and action command
     *              example: "{source:\"source\",targets:['target1','target2','targets3'],command:\"release\"}"
     * @return
     * @throws Exception
     */
    public VoltTable[] run(SystemProcedureExecutionContext ctx, String json) throws Exception {
        VoltTable results = new VoltTable(
                new ColumnInfo("PARTITIONID", VoltType.BIGINT),
                new ColumnInfo("SOURCE", VoltType.STRING),
                new ColumnInfo("TARGET", VoltType.STRING),
                new ColumnInfo("STATUS", VoltType.BIGINT),
                new ColumnInfo("MESSAGE", VoltType.STRING));
        String operationMode = null;
        String exportSource = null;
        List<String> exportTargets = new ArrayList<>();
        try {
            JSONObject jsObj = new JSONObject(json);
            operationMode = jsObj.getString("command");
            OperationMode.valueOf(operationMode.toUpperCase());

            exportSource = jsObj.getString("source");
            JSONArray jsonArray = jsObj.optJSONArray("targets");
            if (jsonArray != null) {
                for(int i=0; i < jsonArray.length(); i++) {
                    String s = jsonArray.getString(i).trim();
                    if(s.length() > 0) {
                        exportTargets.add(s.toString());
                    }
                }
            }
            exportSource = exportSource == null ? "" : exportSource;
        } catch (IllegalArgumentException | JSONException e){
            results.addRow(-1, "", "", VoltSystemProcedure.STATUS_FAILURE, e.getMessage());
            return new VoltTable[] {results};
        }

        if (!"".equals(exportSource)) {
            RealVoltDB volt = (RealVoltDB)VoltDB.instance();
            Set<String> exportStreams = CatalogUtil.getExportTableNames( volt.getCatalogContext().database);
            if (exportStreams.isEmpty()) {
                results.addRow(-1, "", "", VoltSystemProcedure.STATUS_FAILURE, "No export streams defined.");
                return new VoltTable[] {results};
            }
            boolean isThere = exportStreams.stream().anyMatch(exportSource::equalsIgnoreCase);
            if (!isThere) {
                results.addRow(-1, exportSource, "", VoltSystemProcedure.STATUS_FAILURE, "Export stream " + exportSource + " does not exist.");
                return new VoltTable[] {results};
            }
        }
        return performExportControl(exportSource, exportTargets.toArray(new String[exportTargets.size()]), operationMode);
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
