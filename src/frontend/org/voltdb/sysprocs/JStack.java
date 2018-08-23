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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.LocalMailbox;

public class JStack extends VoltSystemProcedure {
    private static final VoltLogger JSTACK_LOG = new VoltLogger("JSTACK");
    HostMessenger m_hostMessenger = new HostMessenger(new HostMessenger.Config(false), null, null);
    @Override
    public long[] getPlanFragmentIds() {
        return new long[]{};
    }

    @Override
    public DependencyPair executePlanFragment(Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params, SystemProcedureExecutionContext context) {
        return null;
    }

    public VoltTable[] run(SystemProcedureExecutionContext ctx, String command)
    {
        try {
            System.out.println(command);
            JSONObject jsObj = new JSONObject(command);
            String[] hsIDs = jsObj.getString("hsIds").split(",");
            long[] hsID_arr = new long[hsIDs.length];
            for(int i = 0 ; i < hsIDs.length ; i++) {
                hsID_arr[i] = Long.parseLong(hsIDs[i]);
            }
            m_hostMessenger.sendPoisonPillJStack(hsID_arr, "Send poison pill for JStack command.");
        } catch (Exception e) {
            e.printStackTrace();
        }
        VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        t.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {t});
    }
}
