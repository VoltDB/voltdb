/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.TreeSet;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.utils.CatalogUtil;

public class ProcedureProfiler {

    /**
     * Describes the level of profiling the Profilier will use.
     */
    public enum Level {
        /** No profiling of procedures or statements will be done */
        DISABLED    (1),
        /** Profiling will be done at the procedure level */
        POLITE      (2),
        /** Profiling will be done for each statement, breaking any batching */
        INTRUSIVE   (3);

        public final int value;

        Level(int i) { value = i; }

        public static Level get(int value) {
            for (Level l : Level.values())
                if (l.value == value)
                    return l;
            assert(false);
            return null;
        }
    }

    // for profiling
    static Level profilingLevel = Level.DISABLED;
    private static final String PROFILE_FILENAME = "txnProfile.txt";
    private static long profileStartTime = 0;
    private static long profileEndTime = 0;
    private static long profileOutputNext = 1000;
    private static long allCallCount = 0;
    private static long allStatementCount = 0;
    private static long startTime = 0;
    private static long stmtStartTime = 0;
    private static long totalTime = 0;
    private static long stmtTotalTime = 0;
    /**
     * Used for collecting profiling information using wall-clock time
     * for procedures and SQL statements. Usually not used.
     */
    private static class TimingInfo implements Comparable<TimingInfo>{
        String name = null;
        long callCount = 0;
        long averageTime = 0;
        long sumOfSquares = 0;
        long totalTime = 0;
        long stmtCalls = 0;
        long max = 0;
        long min = Long.MAX_VALUE;
        Procedure proc = null;

        public int compareTo(TimingInfo o) {
            //if ((o.averageTime - averageTime) > 0) return 1;
            //else if ((o.averageTime - averageTime) < 0) return -1;
            //else return 0;
            if ((o.totalTime - totalTime) > 0) return 1;
            else if ((o.totalTime - totalTime) < 0) return -1;
            else return 0;
        }
        HashMap<Long, TimingInfo> stmts = new HashMap<Long, TimingInfo>();
    }
    private static TimingInfo currentTimingInfo = null;
    private static TimingInfo currentStmtTimingInfo = null;
    private static HashMap<String, TimingInfo> times = new HashMap<String, TimingInfo>();

    // Workload Trace
    static WorkloadTrace workloadTrace;

    final void startCounter(final Procedure catProc) {
        String proc = catProc.getTypeName();
        if (proc.charAt(0) < 'a') {
            currentTimingInfo = null;
            return;
        }
        startTime = System.nanoTime();
        if (profileStartTime == 0) profileStartTime = startTime;
        TimingInfo ti = times.get(proc);
        if (ti == null) {
            ti = new TimingInfo();
            ti.name = proc;
            ti.proc = catProc;
            times.put(proc, ti);
        }
        currentTimingInfo = ti;
    }

    final void stopCounter() {
        if (currentTimingInfo == null)
            return;

        allCallCount++;
        profileEndTime = System.nanoTime();
        long time = profileEndTime - startTime;
        currentTimingInfo.callCount++;
        currentTimingInfo.totalTime += time;
        currentTimingInfo.sumOfSquares += time * time / 1000000;
        currentTimingInfo.averageTime = currentTimingInfo.totalTime / currentTimingInfo.callCount;
        totalTime += time;
        if (time > currentTimingInfo.max)
            currentTimingInfo.max = time;
        if (time < currentTimingInfo.min)
            currentTimingInfo.min = time;

        if (allCallCount == profileOutputNext) {
            flushProfile();
            profileOutputNext *= 2;
        }
        currentTimingInfo = null;
    }

    final static void startStatementCounter(long fragId) {
        if (profilingLevel == Level.DISABLED)
            return;
        if (currentTimingInfo == null)
            return;
        stmtStartTime = System.nanoTime();
        TimingInfo ti = currentTimingInfo.stmts.get(fragId);
        if (ti == null) {
            ti = new TimingInfo();
            //ti.name = String.valueOf(fragId);
            currentTimingInfo.stmts.put(fragId, ti);
        }
        if ((fragId == -1) && (ti.name == null))
            ti.name = "Batched Statements";
        currentStmtTimingInfo = ti;
    }

    final static void stopStatementCounter() {
        if (profilingLevel == Level.DISABLED)
            return;
        if (currentStmtTimingInfo == null)
            return;

        allStatementCount++;
        currentTimingInfo.stmtCalls++;
        long time = System.nanoTime() - stmtStartTime;
        stmtTotalTime += time;
        currentStmtTimingInfo.callCount++;
        currentStmtTimingInfo.totalTime += time;
        currentStmtTimingInfo.sumOfSquares += time * time / 1000000;
        currentStmtTimingInfo.averageTime = currentStmtTimingInfo.totalTime / currentStmtTimingInfo.callCount;
        if (time > currentStmtTimingInfo.max)
            currentStmtTimingInfo.max = time;
        if (time < currentStmtTimingInfo.min)
            currentStmtTimingInfo.min = time;
        currentStmtTimingInfo = null;

    }

    public static void flushProfile() {
        if (profilingLevel == Level.DISABLED)
            return;
        if (allCallCount == 0)
            return;
        PrintStream printer = null;

        try {
            try {
                printer = new PrintStream(PROFILE_FILENAME);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                profilingLevel = Level.DISABLED;
                return;
            }
            //printer = System.out;

            printer.println("+========================================== PROCEDURE INFO ===========================================+");
            printer.println("| PROC                  |     Count |  % time | Avg uSecs |      Min |  # Stmts | Std. Dev |      TPS |");
            printer.println("+-----------------------------------------------------------------------------------------------------+");
            TreeSet<TimingInfo> tset = new TreeSet<TimingInfo>();
            for (TimingInfo ti : times.values()) {
                tset.add(ti);
            }
            long totalStmtCalls = 0;
            for (TimingInfo ti : tset) {
                double percent = ti.totalTime / (totalTime / 100.0);
                long uSecs = ti.averageTime / 1000;
                double tps = 1000000000.0 / ti.averageTime;
                long stddev = (long) Math.sqrt(ti.sumOfSquares / ti.callCount - uSecs * uSecs);
                printer.printf("| %-21s | %9d | %6.2f%% | %9d | %8d | %8d | %8d | %8.2f |\n",
                        ti.name, ti.callCount, percent, uSecs, ti.min / 1000, ti.stmtCalls, stddev, tps);
                totalStmtCalls += ti.stmtCalls;
            }

            printer.println("+-----------------------------------------------------------------------------------------------------+");
            printer.printf("| %-21s | %9d | %6.2f%% | %9d |          | %8d |           |%8.2f |\n",
                    "TOTALS", allCallCount, 100.0, totalTime / 1000, totalStmtCalls, 1000000000.0 / (totalTime / allCallCount));
            printer.println("+=====================================================================================================+\n");
            printer.flush();

            printer.println("+====================================== STATEMENT INFO ==========================================+");
            printer.println("| PROC / STMT                 |     Count |  % time | Avg uSecs | #PerProc | %PerCall | %ofTotal |");
            printer.println("+------------------------------------------------------------------------------------------------+");

            // filter by procedure and sort everything
            for (TimingInfo ti : tset) {
                long totalStmtTime = 0;
                // handle sql statements
                TreeSet<TimingInfo> stset = new TreeSet<TimingInfo>();
                for (long fragId : ti.stmts.keySet()) {
                    TimingInfo sti = ti.stmts.get(fragId);

                    // find the name from the catalog
                    if (sti.name == null) {
                        CatalogMap<Statement> stmts = ti.proc.getStatements();
                        for (Statement stmt : stmts) {
                            CatalogMap<PlanFragment> frags = stmt.getFragments();
                            for (PlanFragment frag : frags) {
                                if (CatalogUtil.getUniqueIdForFragment(frag) == fragId)
                                    sti.name = stmt.getTypeName();
                            }
                        }
                    }

                    stset.add(sti);
                    totalStmtTime += sti.totalTime;
                }

                // Print out the java overhead for each procedure
                long javaTime = ti.totalTime - totalStmtTime;
                double percent = javaTime / (ti.totalTime / 100.0);
                long uSecs = (long)(javaTime / (double)ti.callCount / 1000.0);
                printer.printf("| %-27s |           |         |           |          |          |          |\n", ti.name);
                printer.printf("|   (Java Overhead)           |           | %6.2f%% | %9d |          |          | %7.3f%% |\n",
                        percent, uSecs, (javaTime * 100.0) / totalTime);

                // print out the stats for each sql statement
                for (TimingInfo sti : stset) {
                    percent = sti.totalTime / (ti.totalTime / 100.0);
                    uSecs = sti.averageTime / 1000;
                    double percentOfWhole = (sti.totalTime * 100.0) / totalTime;
                    double perProc = (double)sti.callCount / ti.callCount;
                    double perCallPercent = percentOfWhole / perProc;
                    //long stddev = (long) Math.sqrt(sti.sumOfSquares / sti.callCount - uSecs * uSecs);
                    printer.printf("|   %-25s | %9d | %6.2f%% | %9d | %8.3f | %7.3f%% | %7.3f%% |\n",
                            sti.name, sti.callCount, percent, uSecs, perProc, perCallPercent, percentOfWhole);
                }
                printer.println("|                             |           |         |           |          |          |          |");
            }
            printer.println("+================================================================================================+\n");
            printer.flush();

            long fullTime = profileEndTime - profileStartTime;

            printer.printf("\nRan benchmark for %d \n", fullTime / 1000);
            printer.printf("Ran procedures for %d \n", totalTime / 1000);
            printer.printf("Ran statements for %d \n\n", stmtTotalTime / 1000);

            printer.printf("Spend %5.2f%% of run time in procedures\n", ((double)totalTime / fullTime) * 100.0);
            printer.printf("Spend %5.2f%% of run time in stmts\n", ((double)stmtTotalTime / fullTime) * 100.0);
            printer.printf("Spend %5.2f%% of procedure time in stmts\n", ((double)stmtTotalTime / totalTime) * 100.0);

            printer.flush();

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if ((printer != null) && (printer != System.out))
                printer.close();
        }
    }

    public static void initializeWorkloadTrace(Catalog catalog) {
        if (profilingLevel == ProcedureProfiler.Level.INTRUSIVE) {
            String traceClass = System.getenv("workload.trace.class");
            String tracePath = System.getenv("workload.trace.path");
            String traceIgnore = System.getenv("workload.trace.ignore");
            if (traceClass != null && !traceClass.isEmpty()) {

                ClassLoader loader = ClassLoader.getSystemClassLoader();
                try {
                    ProcedureProfiler.workloadTrace = (WorkloadTrace)loader.loadClass(traceClass).newInstance();
                    ProcedureProfiler.workloadTrace.setCatalog(catalog);
                } catch (Exception ex) {
                    //LOG.log(Level.SEVERE, null, ex);
                    throw new RuntimeException(ex);
                }

                // Output Path
                if (tracePath != null && !tracePath.isEmpty()) {
                    ProcedureProfiler.workloadTrace.setOutputPath(tracePath);
                }

                // Ignore certain procedures in the workload trace
                if (traceIgnore != null) {
                    for (String ignore_proc : traceIgnore.split(",")) {
                        if (!ignore_proc.isEmpty()) {
                            //LOG.fine("Ignoring Procedure '" + ignore_proc + "'");
                            ProcedureProfiler.workloadTrace.addIgnoredProcedure(ignore_proc);
                        }
                    }
                }
            }
        }
    }
}
