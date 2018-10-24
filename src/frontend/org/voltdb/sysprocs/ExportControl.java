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
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.export.ExportManager;

public class ExportControl extends VoltSystemProcedure {

    private static final VoltLogger LOG = new VoltLogger("EXPORT");
    // support operations
    public static enum OperationMode{ RELEASE
                       //PAUSE, RESUME, TRUNCATE //for future use
    }

    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    @Override
    public DependencyPair executePlanFragment(
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        throw new RuntimeException("ExportControl was given an " +
                                   "invalid fragment id: " + String.valueOf(fragmentId));
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String json) throws JSONException {
        VoltTable t = new VoltTable(
                new ColumnInfo("STATUS", VoltType.BIGINT),
                new ColumnInfo("MESSAGE", VoltType.STRING));
        JSONObject jsObj = new JSONObject(json);
        String operationMode = jsObj.getString("command");
        String exportStream = jsObj.getString("source");
        JSONArray targetList = jsObj.optJSONArray("targets");
        try {
            OperationMode.valueOf(operationMode.toUpperCase());
        } catch (IllegalArgumentException e){
            t.addRow(VoltSystemProcedure.STATUS_FAILURE, "Invalide operation");
            return (new VoltTable[] {t});
        }
        if (!ctx.isLowestSiteId()) {
            t.addRow(VoltSystemProcedure.STATUS_OK, "");
            return (new VoltTable[] {t});
        }

        List<String> exportTargets = new ArrayList<>();
        for(int i=0 ; i<targetList.length(); i++) {
            try {
                String s = targetList.getString(i).trim().toUpperCase();
                if(s.length() > 0 && !"none".equalsIgnoreCase(s)) {
                    exportTargets.add(s.toString());
                }
            } catch (JSONException e) {
                throw new RuntimeException(e.getMessage());
            }
        }
        LOG.info("Export stream:" + exportStream + " targets:" + exportTargets + " action:" + operationMode);
        String error= ExportManager.instance().updateExportFlowControl(exportStream, exportTargets, operationMode);
        if (error != null) {
            t.addRow(VoltSystemProcedure.STATUS_FAILURE, error);
            return (new VoltTable[] {t});
        }

        t.addRow(VoltSystemProcedure.STATUS_OK, "");
        return (new VoltTable[] {t});
    }
}
