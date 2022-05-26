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
    // return null.
    private String parseParamsForSystemInformation(ParameterSet params, JSONObject obj) throws Exception
    {
        // Default with no args is OVERVIEW
        String subselector = "status";
        Object[] paramsArray = params.toArray();
        int numOfParams = paramsArray.length;

        if (numOfParams < 1) {
            return "Incorrect number of arguments to @Trace (expects as least 1, received " +
                   numOfParams + ")";
        }

        Object first = paramsArray[0];
        if (!(first instanceof String)) {
            return "First argument to @Trace must be a valid STRING selector, instead was " +
                   first;
        }

        subselector = (String)first;

        // Usage: "exec @Trace status"
        // Check the status of VoltDB tracing tool and the result is returned.
        // Usage: "exec @Trace dump"
        // Write the trace events recorded in the buffer to file, and the absolute path of the file is returned
        if ("status".equalsIgnoreCase(subselector) || "dump".equalsIgnoreCase(subselector)) {
            if (numOfParams != 1) {
                return "Incorrect number of arguments to @Trace " +
                        subselector + " (expected: 1,  received: " +
                        numOfParams + ")";
            }

            obj.put("subselector", subselector);
            obj.put("interval", false);
            return null;
        }

        // Enable/Disable the category for VoltDB tracing tool
        // The tracing tool is turned on automatically when, at least, one category is enabled.
        // The tracing tool is turned off automatically when there is no enabled category.
        // Usage: "exec @Trace enable [Category]"
        // Usage: "exec @Trace disable [Category]"
        if ("enable".equalsIgnoreCase(subselector) || "disable".equalsIgnoreCase(subselector)) {
            if (numOfParams != 2) {
                return "Incorrect number of arguments to @Trace " +
                        subselector + " (expected: 2,  received: " +
                        numOfParams + ")";
            }

            // check the validity of argument after enable/disable
            String categoryItem = paramsArray[1].toString();
            for (VoltTrace.Category cat : VoltTrace.Category.values()) {
                if (cat.toString().equalsIgnoreCase(categoryItem)) {
                    obj.put("subselector", subselector);
                    obj.put("categories", categoryItem);
                    obj.put("interval", false);
                    return null;
                }
            }

            // Disable all enabled categories and shutdown the VoltDB tracing tool
            // Usage: "exec @Trace disable ALL"
            if ("ALL".equalsIgnoreCase(categoryItem)) {
                obj.put("subselector", subselector);
                obj.put("categories", categoryItem);
                obj.put("interval", false);
                return null;
            }

            return "Second argument to @Trace " +
                    subselector + " must be a valid STRING category or 'ALL', instead was " +
                    categoryItem;
        }

        // Trace event filter of VoltDB Tracing tool
        // Usage: "exec @Trace filter [filterTime]"
        // The filter is turned on automatically when filterTime is positive.
        // Usage: "exec @Trace filter 0"
        // The filter is turned off automatically by setting the filterTime to be zero.
        if ("filter".equalsIgnoreCase(subselector)) {
            if (numOfParams != 2) {
                return "Incorrect number of arguments to @Trace " +
                        subselector + " (expected: 2,  received: " +
                        numOfParams + ")";
            }
            // filterTime is a latency target set by the customers
            // It is used as the threshold to filter the trace events,
            // whose latencies are larger than the filterTime.
            String filterTime = paramsArray[1].toString();
            try {
                double time = Double.parseDouble(filterTime);
                if (time < 0) {
                    return "Second argument of @Trace filter must be a non-negative numeric number";
                }
            } catch (NumberFormatException | NullPointerException e) {
                return "Incorrect type of second argument of @Trace filter " +
                        filterTime + " (It must be a double-precision number)";
            }

            obj.put("subselector", subselector);
            obj.put("filterTime", filterTime);
            obj.put("interval", false);
            return null;
        }

        return "Invalid @Trace selector " + subselector;
    }

    @Override
    protected void handleJSONMessage(JSONObject obj) throws Exception
    {
        VoltTable[] results = new VoltTable[] {new VoltTable(new VoltTable.ColumnInfo("STATUS", VoltType.STRING)),
                                               new VoltTable(new VoltTable.ColumnInfo("FILTER", VoltType.STRING))};

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
            if (obj.getString("categories").equalsIgnoreCase("all")) {
                // disable all categories and close the tracer
                VoltTrace.disableAllCategories();
            } else {
                VoltTrace.disableCategories(VoltTrace.Category.valueOf(obj.getString("categories").toUpperCase()));
            }
        } else if (subselector.equalsIgnoreCase("filter")) {
            double time = Double.parseDouble(obj.getString("filterTime"));
            if (time > 0) {
                VoltTrace.turnOnFilter(time);
            } else {
                VoltTrace.turnOffFilter();
            }
        } else if (subselector.equalsIgnoreCase("status")) {
            final Collection<VoltTrace.Category> enabledCategories = VoltTrace.enabledCategories();
            if (enabledCategories.isEmpty()) {
                results[0].addRow("off");
            } else {
                results[0].addRow(enabledCategories.toString());
            }
            if (VoltTrace.isFilterOn()) {
                results[1].addRow("On");
            } else {
                results[1].addRow("Off");
            }
        }

        sendOpsResponse(results, obj);
    }
}
