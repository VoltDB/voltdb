/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.compiler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.hsqldb.HSQLInterface;
import org.hsqldb.HSQLInterface.HSQLParseException;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.planner.CompiledPlan.Fragment;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

/**
 * Planner tool accepts an already compiled VoltDB catalog and then
 * interactively accept SQL and outputs plans on standard out.
 */
public class PlannerTool {

    Process m_process;
    OutputStreamWriter m_in;

    PlannerTool(Process process, OutputStreamWriter in) {
        assert(process != null);
        assert(in != null);

        m_process = process;
        m_in = in;
    }

    public synchronized void kill() {
        m_process.destroy();
    }

    public synchronized String[] planSql(String sql) throws Exception {
        String[] retval = null;

        if ((sql == null) || (sql.length() == 0)) {
            retval = new String[1];
            retval[0] = "ERROR: Can't plan empty or null SQL.";
            return retval;
        }
        // remove any spaces or newlines
        sql = sql.trim();
        m_in.write(sql + "\n");
        m_in.flush();

        String line1 = readLineFromProcess(m_process, 3000);
        assert(line1 != null);
        String line2 = null;
        if (line1.startsWith("ERROR: ")) {
            // get rid of "ERROR: "
            line1 = line1.substring(7);
            throw new IOException(line1);
        }
        else if (line1.startsWith("PLAN-ONE: ")) {
            // valid case
        }
        else if (line1.startsWith("PLAN-ALL: ")) {
            line2 = readLineFromProcess(m_process, 500);
            assert(line2 != null);
            if (line2.startsWith("ERROR: ")) {
                // get rid of "ERROR: "
                line2 = line2.substring(7);
                throw new IOException(line2);
            }
            if (line2.startsWith("PLAN-ONE: ") == false) {
                throw new IOException("Unintelligble output from planner process.");
            }
            // trim PLAN-TWO: from the front
            line2 = line1.substring(10);
        }
        else {
            throw new IOException("Unintelligble output from planner process.");
        }
        // trim PLAN-XYZ: from the front
        line1 = line1.substring(10);

        retval = (line2 == null) ? new String[1] : new String[2];
        retval[0] = line1;
        if (line2 != null) retval[1] = line2;

        return retval;
    }

    /**
     * Try to read a line of output from stdout of the process. Wait a specified
     * amount of time before failing. Under timeout or any error scenario, throw
     * an exception.
     *
     * @param p The process to read output from.
     * @param timeoutInMillis How long to wait for the output.
     * @return One line of text from process stdout.
     * @throws InterruptedException
     * @throws IOException
     */
    private static String readLineFromProcess(Process p, long timeoutInMillis) throws IOException, InterruptedException {
        StringBuilder line = new StringBuilder();
        InputStreamReader isr = new InputStreamReader(p.getInputStream());

        long timeout = 100;
        if ((timeout * 2) > timeoutInMillis)
            timeout = timeoutInMillis / 2;

        long start = System.nanoTime();
        long now = start;
        char c = ' ';
        while (c != '\n') {
            if (p.getInputStream().available() > 0) {
                c = (char) isr.read();
                if (c == '\n') break;
                else line.append(c);
            }
            else {
                now = System.nanoTime();
                System.out.printf("Blocked for %d ms\n", (now - start) / 1000000);
                if ((now - start) > (timeoutInMillis * 1000000))
                    throw new InterruptedException("Timeout while reading output from process");
                Thread.sleep(timeout);
            }
        }

        return line.toString();
    }

    public static PlannerTool createPlannerToolProcess(String pathToCatalog) {
        String classpath = System.getProperty("java.class.path");
        assert(classpath != null);

        ArrayList<String> cmd = new ArrayList<String>();

        cmd.add("java");
        cmd.add("-cp");
        cmd.add(classpath);
        cmd.add(PlannerTool.class.getName());

        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectErrorStream(true);
        Process process = null;

        try {
            process = pb.start();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        OutputStreamWriter in = new OutputStreamWriter(process.getOutputStream());

        return new PlannerTool(process, in);
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        //////////////////////
        // PARSE COMMAND LINE ARGS
        //////////////////////

        if (args.length != 1) {
            // need usage here
            return;
        }

        String pathToCatalog = args[0];

        //////////////////////
        // LOAD THE CATALOG
        //////////////////////

        final String serializedCatalog = CatalogUtil.loadCatalogFromJar(pathToCatalog, null);
        if ((serializedCatalog == null) || (serializedCatalog.length() == 0)) {
            // need real error path
            return;
        }
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);
        Cluster cluster = catalog.getClusters().get("cluster");
        Database db = cluster.getDatabases().get("database");

        //////////////////////
        // LOAD HSQL
        //////////////////////

        HSQLInterface hsql = HSQLInterface.loadHsqldb();
        String hexDDL = db.getSchema();
        String ddl = Encoder.hexDecodeToString(hexDDL);
        String[] commands = ddl.split(";");
        for (String command : commands) {
            command = command.trim();
            if (command.length() == 0)
                continue;
            try {
                hsql.runDDLCommand(command);
            } catch (HSQLParseException e) {
                // need a good error message here
                e.printStackTrace();
                return;
            }
        }

        //////////////////////
        // BEGIN THE MAIN INPUT LOOP
        //////////////////////

        String inputLine = "";
        InputStreamReader isr = new InputStreamReader(System.in);
        BufferedReader in = new BufferedReader(isr);

        while (true) {

            //////////////////////
            // READ THE SQL
            //////////////////////

            try {
                inputLine = in.readLine();
            } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            // check the input
            if (inputLine.length() == 0) {
                // need error output here
                continue;
            }
            inputLine = inputLine.trim();

            //////////////////////
            // PLAN THE STMT
            //////////////////////

            TrivialCostModel costModel = new TrivialCostModel();
            QueryPlanner planner = new QueryPlanner(
                    cluster, db, hsql, new DatabaseEstimates(), false, true);
            CompiledPlan plan = null;
            try {
                plan = planner.compilePlan(
                        costModel, inputLine, "PlannerTool", "PlannerToolProc", false, null);
            } catch (Exception e) {
                // need a good error path here
                e.printStackTrace();
                return;
            }
            if (plan == null) {
                String plannerMsg = planner.getErrorMessage();
                if (plannerMsg != null) {
                    System.out.println("ERROR: " + plannerMsg + "\n");
                }
                else {
                    System.out.println("ERROR: UNKNOWN PLANNING ERROR\n");
                }
                continue;
            }

            assert(plan.fragments.size() <= 2);

            //////////////////////
            // OUTPUT THE RESULT
            //////////////////////

            // print out the run-at-every-partition fragment
            boolean found = false;
            for (int i = 0; i < plan.fragments.size(); i++) {
                Fragment frag = plan.fragments.get(i);
                if (frag.multiPartition) {
                    PlanNodeList planList = new PlanNodeList(frag.planGraph);
                    String serializedPlan = planList.toJSONString();
                    System.out.println("PLAN-ALL: " + serializedPlan);
                    found = true;
                    break;
                }
            }
            if (plan.fragments.size() > 1) assert(found == true);

            // print the run-at-coordinator fragment
            // (or only frag if using replicated tables)
            found = false;
            for (int i = 0; i < plan.fragments.size(); i++) {
                Fragment frag = plan.fragments.get(i);
                if (frag.multiPartition == false) {
                    PlanNodeList planList = new PlanNodeList(frag.planGraph);
                    String serializedPlan = planList.toJSONString();
                    System.out.println("PLAN-ONE: " + serializedPlan);
                    found = true;
                    break;
                }
            }
            assert(found == true);
        }
    }

}
