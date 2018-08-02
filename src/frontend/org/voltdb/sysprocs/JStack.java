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
import org.voltdb.DependencyPair;
import org.voltdb.ParameterSet;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

public class JStack extends VoltSystemProcedure {
    private static final VoltLogger JSTACK_LOG = new VoltLogger("JSTACK");

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
        Process process = null;
        List<String> processList = new ArrayList<String>();
        try {
            JSONObject jsObj = new JSONObject(command);
            String host = jsObj.getString("Host");
            process = Runtime.getRuntime().exec("jps");
            BufferedReader input = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = input.readLine()) != null) {
                processList.add(line);
            }
            input.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        List<String> stackTrace = new ArrayList<>();
        for (String line : processList) {
            String[] ss = line.split(" ");
            if(ss.length > 1 && ss[1].equals("VoltDB")) {
                int pid = Integer.parseInt(ss[0]);
                try {
                    Process pcsStackTrace = Runtime.getRuntime().exec("jstack " + pid);
                    BufferedReader input = new BufferedReader(new InputStreamReader(pcsStackTrace.getInputStream()));
                    stackTrace.add("--------------Stack trace for PID " + pid + "--------------");
                    String s = "";
                    while ((s = input.readLine()) != null) {
                        stackTrace.add(s);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        for(String s : stackTrace) {
            JSTACK_LOG.info(s);
        }

        VoltTable t = new VoltTable(VoltSystemProcedure.STATUS_SCHEMA);
        t.addRow(VoltSystemProcedure.STATUS_OK);
        return (new VoltTable[] {t});
    }
}
