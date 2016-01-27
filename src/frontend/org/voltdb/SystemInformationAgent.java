/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.network.Connection;
import org.voltdb.client.ClientResponse;
import org.voltdb.sysprocs.SystemInformation;

/**
 * Agent responsible for collecting SystemInformation on this host.
 *
 */
public class SystemInformationAgent extends OpsAgent
{
    public SystemInformationAgent() {
        super("SystemInformationAgent");
    }

    @Override
    protected void collectStatsImpl(Connection c, long clientHandle, OpsSelector selector,
            ParameterSet params) throws Exception
    {
        JSONObject obj = new JSONObject();
        obj.put("selector", "SYSTEMINFORMATION");
        String err = null;
        if (selector == OpsSelector.SYSTEMINFORMATION) {
            err = parseParamsForSystemInformation(params, obj);
        }
        else {
            err = "SystemInformationAgent received non-SYSTEMINFORMATION selector: " + selector.name();
        }
        if (err != null) {
            sendErrorResponse(c, ClientResponse.GRACEFUL_FAILURE, err, clientHandle);
            return;
        }
        String subselector = obj.getString("subselector");

        PendingOpsRequest psr = new PendingOpsRequest(
                selector,
                subselector,
                c,
                clientHandle,
                System.currentTimeMillis(),
                obj);

        // Some selectors can provide a single answer based on global data.
        // Intercept them and respond before doing the distributed stuff.
        if (subselector.equalsIgnoreCase("DEPLOYMENT")) {
            collectSystemInformationDeployment(psr);
            return;
        }
        distributeOpsWork(psr, obj);
    }

    // Parse the provided parameter set object and fill in subselector and interval into
    // the provided JSONObject.  If there's an error, return that in the String, otherwise
    // return null.  Yes, ugly.  Bang it out, then refactor later.
    private String parseParamsForSystemInformation(ParameterSet params, JSONObject obj) throws Exception
    {
        // Default with no args is OVERVIEW
        String subselector = "OVERVIEW";
        if (params.toArray().length > 1) {
            return "Incorrect number of arguments to @SystemInformation (expects 1, received " +
                    params.toArray().length + ")";
        }
        if (params.toArray().length == 1) {
            Object first = params.toArray()[0];
            if (!(first instanceof String)) {
                return "First argument to @SystemInformation must be a valid STRING selector, instead was " +
                    first;
            }
            subselector = (String)first;
            if (!(subselector.equalsIgnoreCase("OVERVIEW") || subselector.equalsIgnoreCase("DEPLOYMENT"))) {
                return "Invalid @SystemInformation selector " + subselector;
            }
        }
        // Would be nice to have subselector validation here, maybe.  Maybe later.
        obj.put("subselector", subselector);
        obj.put("interval", false);

        return null;
    }

    @Override
    protected void handleJSONMessage(JSONObject obj) throws Exception {
        VoltTable[] results = null;

        OpsSelector selector = OpsSelector.valueOf(obj.getString("selector").toUpperCase());
        if (selector == OpsSelector.SYSTEMINFORMATION) {
            results = collectSystemInformation(obj);
        }
        else {
            hostLog.warn("SystemInformationAgent received a non-SYSTEMINFORMATION OPS selector: " + selector);
        }
        sendOpsResponse(results, obj);
    }

    private VoltTable[] collectSystemInformation(JSONObject obj) throws JSONException
    {
        String subselector = obj.getString("subselector");
        VoltTable[] tables = null;
        VoltTable result = null;
        if (subselector.toUpperCase().equals("OVERVIEW"))
        {
            result = SystemInformation.populateOverviewTable();
        }

        if (result != null)
        {
            tables = new VoltTable[1];
            tables[0] = result;
        }
        return tables;
    }

    private void collectSystemInformationDeployment(PendingOpsRequest psr)
    {
        VoltTable result =
                SystemInformation.populateDeploymentProperties(VoltDB.instance().getCatalogContext().cluster,
                        VoltDB.instance().getCatalogContext().database);
        if (result == null) {
            sendErrorResponse(psr.c, ClientResponse.GRACEFUL_FAILURE,
                    "Unable to collect DEPLOYMENT information for @SystemInformation", psr.clientData);
            return;
        }
        psr.aggregateTables = new VoltTable[1];
        psr.aggregateTables[0] = result;
        try {
            sendClientResponse(psr);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to return PARTITIONCOUNT to client", true, e);
        }
    }
}
