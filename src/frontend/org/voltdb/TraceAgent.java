/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import org.json_voltpatches.JSONObject;
import org.voltcore.network.Connection;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.VoltTrace;

import java.io.File;
import java.util.Collection;

public class TraceAgent extends OpsAgent {
    public TraceAgent()
    {
        super("TraceAgent");
    }

    @Override
    protected void collectStatsImpl(Connection c, long clientHandle, OpsSelector selector,
                                    ParameterSet params) throws Exception
    {
        JSONObject obj = new JSONObject();
        obj.put("selector", OpsSelector.TRACE);
        String err;
        if (selector == OpsSelector.TRACE) {
            err = parseParamsForSystemInformation(params, obj);
        } else {
            err = "TraceAgent received non-TRACE selector: " + selector.name();
        }
        if (err != null) {
            sendErrorResponse(c, ClientResponse.GRACEFUL_FAILURE, err, clientHandle);
            return;
        }

        PendingOpsRequest psr = new PendingOpsRequest(selector,
                                                      obj.getString("subselector"),
                                                      c,
                                                      clientHandle,
                                                      System.currentTimeMillis(),
                                                      obj);
        distributeOpsWork(psr, obj);
    }

    // Parse the provided parameter set object and fill in subselector and interval into
    // the provided JSONObject.  If there's an error, return that in the String, otherwise
    // return null.  Yes, ugly.  Bang it out, then refactor later.
    private String parseParamsForSystemInformation(ParameterSet params, JSONObject obj) throws Exception
    {
        // Default with no args is OVERVIEW
        String subselector = "status";
        if (params.toArray().length < 1) {
            return "Incorrect number of arguments to @Trace (expects as least 1, received " +
                   params.toArray().length + ")";
        }
        if (params.toArray().length >= 1) {
            Object first = params.toArray()[0];
            if (!(first instanceof String)) {
                return "First argument to @Trace must be a valid STRING selector, instead was " +
                       first;
            }
            subselector = (String)first;
            if (!(subselector.equalsIgnoreCase("enable") ||
                  subselector.equalsIgnoreCase("disable") ||
                  subselector.equalsIgnoreCase("status") ||
                  subselector.equalsIgnoreCase("dump"))) {
                return "Invalid @Trace selector " + subselector;
            }
        }
        // Would be nice to have subselector validation here, maybe.  Maybe later.
        obj.put("subselector", subselector);
        if (params.toArray().length >= 2) {
            obj.put("categories", params.toArray()[1]);
        }
        obj.put("interval", false);

        return null;
    }

    @Override
    protected void handleJSONMessage(JSONObject obj) throws Exception
    {
        VoltTable[] results = new VoltTable[] {new VoltTable(new VoltTable.ColumnInfo("STATUS", VoltType.STRING))};

        OpsSelector selector = OpsSelector.valueOf(obj.getString("selector").toUpperCase());
        if (selector != OpsSelector.TRACE) {
            hostLog.warn("TraceAgent received a non-TRACE OPS selector: " + selector);
        }

        final String subselector = obj.getString("subselector");
        if (subselector.equalsIgnoreCase("dump")) {
            final String filePath = VoltTrace.dump(new File(VoltDB.instance().getVoltDBRootPath(), "trace_logs").getAbsolutePath());
            if (filePath != null) {
                results[0].addRow(filePath);
            } else {
                results[0].addRow("A trace file write request is already in progress or there is no category enabled");
            }
        } else if (subselector.equalsIgnoreCase("enable")) {
            VoltTrace.enableCategories(VoltTrace.Category.valueOf(obj.getString("categories").toUpperCase()));
        } else if (subselector.equalsIgnoreCase("disable")) {
            VoltTrace.disableCategories(VoltTrace.Category.valueOf(obj.getString("categories").toUpperCase()));
        } else if (subselector.equalsIgnoreCase("status")) {
            final Collection<VoltTrace.Category> enabledCategories = VoltTrace.enabledCategories();
            if (enabledCategories.isEmpty()) {
                results[0].addRow("off");
            } else {
                results[0].addRow(enabledCategories.toString());
            }
        }

        sendOpsResponse(results, obj);
    }
}
