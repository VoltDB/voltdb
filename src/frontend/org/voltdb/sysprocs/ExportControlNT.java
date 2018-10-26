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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.client.ClientResponse;
import org.voltdb.export.ExportManager;

public class ExportControlNT extends VoltNTSystemProcedure {

    private static final VoltLogger LOG = new VoltLogger("EXPORT");

    // support operations
    public static enum OperationMode{ RELEASE
                       //PAUSE, RESUME, TRUNCATE //for future use
    }

    public static class ExportControlTaskNT extends VoltNTSystemProcedure {
        public VoltTable run(String json) throws InterruptedException, ExecutionException {
            VoltTable t = new VoltTable(
                    new ColumnInfo("STATUS", VoltType.BIGINT),
                    new ColumnInfo("MESSAGE", VoltType.STRING));
            try {
                JSONObject jsObj = new JSONObject(json);
                String operationMode = jsObj.getString("command");
                OperationMode.valueOf(operationMode.toUpperCase());

                String exportSource = jsObj.getString("source");
                    JSONArray jsonArray = jsObj.optJSONArray("targets");
                    List<String> exportTargets = new ArrayList<>();
                    if (jsonArray != null) {
                        for(int i=0; i < jsonArray.length(); i++) {
                            String s = jsonArray.getString(i).trim();
                            if(s.length() > 0) {
                                exportTargets.add(s.toString());
                            }
                        }
                    }
                    LOG.info("Export " + operationMode + " source:" + exportSource + " targets:" + exportTargets);
                    ExportManager.instance().applyExportControl(exportSource, exportTargets, operationMode);
            } catch (IllegalArgumentException | JSONException e){
                t.addRow(VoltSystemProcedure.STATUS_FAILURE, e.getMessage());
                return t;
            }
            t.addRow(VoltSystemProcedure.STATUS_OK, "");
            return t;
        }
    }

    public VoltTable run(String json) {
        VoltTable t = new VoltTable(
                new ColumnInfo("STATUS", VoltType.BIGINT),
                new ColumnInfo("MESSAGE", VoltType.STRING));
        CompletableFuture<Map<Integer,ClientResponse>> pf = callNTProcedureOnAllHosts("@ExportControlTaskNT", json);
        Map<Integer, ClientResponse> cr;
        try {
            cr = pf.get();
            cr.entrySet().stream()
            .forEach(e -> {
                t.add(e.getValue().getResults()[0].fetchRow(0));
            });
        } catch (InterruptedException | ExecutionException e1) {
        }
        return t;
    }
}
