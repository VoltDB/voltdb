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

import static org.voltdb.OpsSelector.SYSTEM_CLOCK_SKEW;

public class ClockSkewCollectorAgent extends OpsAgent {

    public static final String PROCEDURE = "@SystemClockSkew";
    public static final String COLLECT = "COLLECT";
    public static final String CLOCK_SKEW = "CLOCKSKEW";
    public static final String NODE_CURRENT_TIME = "NODE_CURRENT_TIME";
    public static final String HOST_ID = "HOST_ID";

    private static final String REQUEST_TYPE = "RT";

    public ClockSkewCollectorAgent() {
        super("ClockSkewCollector");
    }

    @Override
    protected void collectStatsImpl(Connection connection, long clientHandle, OpsSelector selector, ParameterSet params) throws Exception {
        long now = System.currentTimeMillis();
        JSONObject obj = new JSONObject();
        obj.put(REQUEST_TYPE, COLLECT);

        PendingOpsRequest request = new PendingOpsRequest(SYSTEM_CLOCK_SKEW, COLLECT, connection, clientHandle, now, obj);
        distributeOpsWork(request, obj);
    }

    @Override
    protected void handleJSONMessage(JSONObject obj) throws Exception {
        if (COLLECT.equals(obj.getString(REQUEST_TYPE))) {
            VoltTable response = new VoltTable(
                    new VoltTable.ColumnInfo(NODE_CURRENT_TIME, VoltType.BIGINT),
                    new VoltTable.ColumnInfo(HOST_ID, VoltType.INTEGER));
            response.addRow(
                    System.currentTimeMillis(),
                    VoltDB.instance().getMyHostId()
            );
            sendOpsResponse(obj, response);
        }
    }
}
