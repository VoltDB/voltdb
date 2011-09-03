/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.CompiledPlan.Fragment;
import org.voltdb.planner.QueryPlanner;
import org.voltdb.planner.TrivialCostModel;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.utils.Encoder;

/**
 * Planner tool accepts an already compiled VoltDB catalog and then
 * interactively accept SQL and outputs plans on standard out.
 */
public class PlannerTool {

    public static final String POISON_SQL = "*kill*";

    Process m_process;
    OutputStreamWriter m_in;
    AtomicLong m_timeOfLastPlannerCall = new AtomicLong(0);
    String m_lastSql = "";

    public static class Result {
        String onePlan = null;
        String allPlan = null;
        String errors = null;
        boolean replicatedDML = false;

        public String getErrors()
        {
            return errors;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("RESULT {\n");
            sb.append("  ONE: ").append(onePlan == null ? "null" : onePlan).append("\n");
            sb.append("  ALL: ").append(allPlan == null ? "null" : allPlan).append("\n");
            sb.append("  ERR: ").append(errors == null ? "null" : errors).append("\n");
            sb.append("  RTD: ").append(replicatedDML ? "true" : "false").append("\n");
            sb.append("}");
            return sb.toString();
        }
    }

    PlannerTool(Process process, OutputStreamWriter in) {
        assert(process != null);
        assert(in != null);

        m_process = process;
        m_in = in;
    }

    public void kill() {
        m_process.destroy();
        try {
            m_process.waitFor();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public boolean expensiveIsRunningCheck() {
        try {
            m_process.exitValue();
        }
        catch (IllegalThreadStateException e) {
            return true;
        }
        return false;
    }

    public int getExitValue()
    {
        return m_process.exitValue();
    }

    public String getLastSql()
    {
        synchronized (m_lastSql)
        {
            return m_lastSql;
        }
    }

    public boolean perhapsIsHung(long msTimeout) {
        long start = m_timeOfLastPlannerCall.get();
        if (start == 0) return false;
        long duration = System.currentTimeMillis() - start;
        if (duration > msTimeout) return true;
        return false;
    }

    public synchronized Result planSql(String sql, boolean singlePartition) {
        Result retval = new Result();
        retval.errors = "";

        // note when this call started / ensure value was previously zero
        if (m_timeOfLastPlannerCall.compareAndSet(0, System.currentTimeMillis()) == false) {
            retval.errors = "Multiple simultanious calls to out of process planner are not allowed";
            return retval;
        }

        if ((sql == null) || (sql.length() == 0)) {
            m_timeOfLastPlannerCall.set(0);
            retval.errors = "Can't plan empty or null SQL.";
            return retval;
        }
        // remove any spaces or newlines
        sql = sql.trim();
        synchronized (m_lastSql) {
            m_lastSql = sql;
        }
        try {
            // format is S#sql for single-partition and M#sql for multi
            m_in.write((singlePartition ? "S" : "M") + "#" + sql + "\n");
            m_in.flush();
        } catch (IOException e) {
            m_timeOfLastPlannerCall.set(0);
            //e.printStackTrace();
            retval.errors = e.getMessage();
            return retval;
        }

        BufferedReader r = new BufferedReader(new InputStreamReader(m_process.getInputStream()));

        ArrayList<String> output = new ArrayList<String>();

        // read all the lines of output until a newline
        while (true) {
            String line = null;
            try {
                line = r.readLine();
            }
            catch (Exception e) {
                m_timeOfLastPlannerCall.set(0);
                //e.printStackTrace();
                retval.errors = e.getMessage();
                return retval;
            }
            if (line == null)
                continue;
            line = line.trim();
            if (line.length() == 0)
                break;
            //System.err.println(line);
            output.add(line);
        }

        // bucket and process the lines into a response
        for (String line : output) {
            if (line.equals("No logging configuration supplied via -Dlog4j.configuration. Supplying default config that logs INFO or higher to STDOUT")) {
                continue;
            }

            if (line.startsWith("PLAN-ONE: ")) {
                // trim PLAN-ONE: from the front
                assert(retval.onePlan == null);
                retval.onePlan = line.substring(10);
            }
            else if (line.startsWith("PLAN-ALL: ")) {
                // trim PLAN-ALL: from the front
                assert(retval.allPlan == null);
                retval.allPlan = line.substring(10);
            }
            else if (line.startsWith("REPLICATED-DML: ")) {
                retval.replicatedDML = true;
            }
            else {
                // assume error output
                retval.errors += line.substring(7) + "\n";
            }
        }

        // post-process errors
        // removes newlines
        retval.errors = retval.errors.trim();
        // no errors => null
        if (retval.errors.length() == 0) retval.errors = null;

        // reset the clock to zero, meaning not currently planning
        m_timeOfLastPlannerCall.set(0);
        synchronized (m_lastSql)
        {
            m_lastSql = "";
        }
        return retval;
    }

    public static PlannerTool createPlannerToolProcess(String serializedCatalog) {
        assert(serializedCatalog != null);

        String classpath = System.getProperty("java.class.path");
        assert(classpath != null);

        ArrayList<String> cmd = new ArrayList<String>();

        cmd.add("java");
        cmd.add("-cp");
        cmd.add(classpath);
        cmd.add("-Xmx256m");
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

        String encodedCatalog = Encoder.compressAndBase64Encode(serializedCatalog);

        try {
            in.write(encodedCatalog + "\n");
            in.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return new PlannerTool(process, in);
    }

    static void log(String str) {
        try {
            FileWriter log = new FileWriter(m_logfile, true);
            log.write(str + "\n");
            log.flush();
            log.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    static File m_logfile;

    /**
     * @param args
     */
    public static void main(String[] args) {

        // name this thread
        Thread.currentThread().setName("VoltDB Planner Process Main");

        //////////////////////
        // PARSE COMMAND LINE ARGS
        //////////////////////

        String logfile_dir = "";
        if (System.getenv().get("TEST_DIR") != null) {
            logfile_dir = System.getenv().get("TEST_DIR") + File.separator;
        }
        m_logfile = new File(logfile_dir + "plannerlog.txt");

        log("\ngetting started at: " + new Date().toString());

        //////////////////////
        // LOAD THE CATALOG
        //////////////////////

        BufferedReader br = null;
        final int TEN_MEGS = 10 * 1024 * 1024;
        br = new BufferedReader(new InputStreamReader(System.in), TEN_MEGS);
        String encodedSerializedCatalog = null;
        try {
            encodedSerializedCatalog = br.readLine();
        } catch (IOException e) {
            log("Couldn't read catalog: " + e.getMessage());
            System.exit(50);
        }

        final String serializedCatalog = Encoder.decodeBase64AndDecompress(encodedSerializedCatalog);
        if ((serializedCatalog == null) || (serializedCatalog.length() == 0)) {
            log("Catalog is null or empty");
            // need real error path
            System.exit(28);
        }
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);
        Cluster cluster = catalog.getClusters().get("cluster");
        Database db = cluster.getDatabases().get("database");

        log("catalog loaded");

        //////////////////////
        // LOAD HSQL
        //////////////////////

        HSQLInterface hsql = HSQLInterface.loadHsqldb();
        String hexDDL = db.getSchema();
        String ddl = Encoder.hexDecodeToString(hexDDL);
        String[] commands = ddl.split("\n");
        for (String command : commands) {
            String decoded_cmd = Encoder.hexDecodeToString(command);
            decoded_cmd = decoded_cmd.trim();
            if (decoded_cmd.length() == 0)
                continue;
            try {
                hsql.runDDLCommand(decoded_cmd);
            } catch (HSQLParseException e) {
                // need a good error message here
                log("Error creating hsql: " + e.getMessage());
                log("   in DDL statement: " + decoded_cmd);
                System.exit(82);
            }
        }

        log("hsql loaded");

        //////////////////////
        // BEGIN THE MAIN INPUT LOOP
        //////////////////////

        String inputLine = "";

        while (true) {

            //////////////////////
            // READ THE SQL
            //////////////////////

            try {
                inputLine = br.readLine();
            } catch (IOException e) {
                log("Exception: " + e.getMessage());
                System.out.println("ERROR: " + e.getMessage() + "\n");
                System.exit(81);
            }
            // check the input
            if (inputLine.length() == 0) {
                log("got a zero-length input to the sql planner");
                continue;
            }
            inputLine = inputLine.trim();

            log("recieved sql stmt: " + inputLine);

            String[] parts = inputLine.split("#");
            log(String.format("Got %d parts", parts.length));
            if (parts.length != 2) {
                log("got a malformed input to the sql planner (bad hash splitting)");
                continue;
            }
            if ((parts[0].charAt(0) != 'S') && (parts[0].charAt(0) != 'M')) {
                log("got a malformed input to the sql planner (bad single/multi hinting)");
                continue;
            }

            boolean isSinglePartition = parts[0].charAt(0) == 'S';
            String sql = parts[1];

            // the poison sql causes the process to exit
            if (sql.equalsIgnoreCase("POISON_SQL")) {
                log("Dying a horrible death (on purpose).");
                System.exit(-1);
            }

            //////////////////////
            // PLAN THE STMT
            //////////////////////

            TrivialCostModel costModel = new TrivialCostModel();
            QueryPlanner planner = new QueryPlanner(
                    cluster, db, hsql, new DatabaseEstimates(), false, true);
            CompiledPlan plan = null;
            try {
                plan = planner.compilePlan(
                        costModel, sql, null, "PlannerTool", "PlannerToolProc", isSinglePartition, null);
            } catch (Exception e) {
                log("Error creating planner: " + e.getMessage());
                String plannerMsg = e.getMessage();
                if (plannerMsg != null) {
                    System.out.println("ERROR: " + plannerMsg + "\n");
                }
                else {
                    System.out.println("ERROR: UNKNOWN PLANNING ERROR\n");
                }
                continue;
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
            if (plan.parameters.size() > 0) {
                System.out.println("ERROR: PARAMETERIZATION IN AD HOC QUERY\n");
                continue;
            }

            log("finished planning stmt:");
            log("SQL: " + plan.sql);
            log("COST: " + Double.toString(plan.cost));
            log("PLAN:\n");
            log(plan.explainedPlan);

            assert(plan.fragments.size() <= 2);

            //////////////////////
            // OUTPUT THE RESULT
            //////////////////////

            // print out the run-at-every-partition fragment
            for (int i = 0; i < plan.fragments.size(); i++) {
                Fragment frag = plan.fragments.get(i);
                PlanNodeList planList = new PlanNodeList(frag.planGraph);
                String serializedPlan = planList.toJSONString();
                String encodedPlan = serializedPlan; //Encoder.compressAndBase64Encode(serializedPlan);
                if (frag.multiPartition) {
                    log("PLAN-ALL GENERATED");
                    System.out.println("PLAN-ALL: " + encodedPlan);
                }
                else {
                    log("PLAN-ONE GENERATED");
                    System.out.println("PLAN-ONE: " + encodedPlan);
                }
            }

            if (plan.replicatedTableDML) {
                System.out.println("REPLICATED-DML: true");
            }

            log("finished loop");

            // print a newline to delimit
            System.out.println();
            System.out.flush();
        }
    }

}
