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

package org.voltdb.utils;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;

public class ClientResponseToJsonApiV2 {
    /* Takes a ClientResonse and builds a api/v2.0 string
    Currently, this is only used in HTTPClientInterface. If we want to version the response generally, it will
    require extensive refactoring. This one-off bypasses the implementations of toJSONString()
     */

    // JSON KEYS FOR SERIALIZATION
    static final String JSON_STATUS_KEY = "status";
    static final String JSON_STATUSSTRING_KEY = "statusstring";
    static final String JSON_APPSTATUS_KEY = "appstatus";
    static final String JSON_APPSTATUSSTRING_KEY = "appstatusstring";
    static final String JSON_RESULTS_KEY = "results";

    public static String toJSONStringV2(ClientResponse clientResponse) throws JSONException {
        JSONStringer js = new JSONStringer();
            js.object();
            js.keySymbolValuePair(JSON_STATUS_KEY, clientResponse.getStatus());
            js.keySymbolValuePair(JSON_APPSTATUS_KEY, clientResponse.getAppStatus());
            js.keySymbolValuePair(JSON_STATUSSTRING_KEY, clientResponse.getStatusString());
            js.keySymbolValuePair(JSON_APPSTATUSSTRING_KEY, clientResponse.getAppStatusString());
            js.key(JSON_RESULTS_KEY);
            js.object();
            VoltTable[] results = clientResponse.getResults();
            for (int i=0; i<results.length; i++) {
                js.key(String.valueOf(i));
                VoltTable o = results[i];
                o.toJSONStringerV2(js);
            }
            js.endObject();
            js.endObject();
        return js.toString();
    }
}
