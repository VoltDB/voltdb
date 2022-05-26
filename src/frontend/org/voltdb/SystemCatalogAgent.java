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

/**
 * Agent responsible for collecting SystemCatalog on this host.
 *
 */
public class SystemCatalogAgent extends OpsAgent
{
    public SystemCatalogAgent() {
        super("SystemCatalogAgent");
    }

    @Override
    protected void collectStatsImpl(Connection c, long clientHandle, OpsSelector selector,
            ParameterSet params) throws Exception
    {
        JSONObject obj = new JSONObject();
        obj.put("selector", selector.name());
        String err = null;
        if (selector == OpsSelector.SYSTEMCATALOG) {
            err = parseParamsForSystemCatalog(params, obj);
        }
        else {
            err = "SystemCatalogAgent received a non-SYSTEMCATALOG OPS selector: " + selector.name();
        }
        if (err != null) {
            sendErrorResponse(c, ClientResponse.GRACEFUL_FAILURE, err, clientHandle);
            return;
        }
        String subselector = obj.getString("subselector");

        // All system catalog selectors are currently local, can all get serviced here
        PendingOpsRequest psr = new PendingOpsRequest(
                selector,
                subselector,
                c,
                clientHandle,
                System.currentTimeMillis(),
                obj);
        collectSystemCatalog(psr);
        return;
    }

    // Parse the provided parameter set object and fill in subselector and interval into
    // the provided JSONObject.  If there's an error, return that in the String, otherwise
    // return null.  Yes, ugly.  Bang it out, then refactor later.
    private String parseParamsForSystemCatalog(ParameterSet params, JSONObject obj) throws Exception
    {
        if (params.toArray().length != 1) {
            return "Incorrect number of arguments to @SystemCatalog (expects 1, received " +
                    params.toArray().length + ")";
        }
        Object first = params.toArray()[0];
        if (!(first instanceof String)) {
            return "First argument to @SystemCatalog must be a valid STRING selector, instead was " +
                    first;
        }
        // Would be nice to have subselector validation here, maybe.  Maybe later.
        String subselector = (String)first;
        obj.put("subselector", subselector);
        obj.put("interval", false);

        return null;
    }

    // SystemCatalog shouldn't currently get here, make it so we don't die or do anything
    @Override
    protected void handleJSONMessage(JSONObject obj) throws Exception {
        hostLog.warn("SystemCatalogAgent received a JSON message, which should be impossible.");
        VoltTable[] results = null;
        sendOpsResponse(results, obj);
    }

    private void collectSystemCatalog(PendingOpsRequest psr)
    {
        VoltTable results = VoltDB.instance().getCatalogContext().m_jdbc.getMetaData(psr.subselector);
        if (results == null) {
            sendErrorResponse(psr.c, ClientResponse.GRACEFUL_FAILURE,
                    "Invalid @SystemCatalog selector: " + psr.subselector, psr.clientData);
            return;
        }
        psr.aggregateTables = new VoltTable[1];
        psr.aggregateTables[0] = results;
        try {
            sendClientResponse(psr);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to return PARTITIONCOUNT to client", true, e);
        }
    }
}
