/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayDeque;
import java.util.HashMap;

import org.voltcore.logging.VoltLogger;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.processtools.ShellTools;

/**
 * SystemStatsCollector stores a history of system memory usage samples.
 * Generating a sample is a manually instigated process that must be done
 * periodically.
 * It stored history in three buckets, each with a fixed size.
 * Each bucket should me more granular than the last.
 *
 */
public class SystemStatsCollector {

    private enum GetRSSMode { MACOSX_NATIVE, PROCFS, PS }

    static long starttime = System.currentTimeMillis();
    static final long javamaxheapmem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    static long memorysize = 256;
    static int pid = 0;
    static boolean initialized = false;
    static GetRSSMode mode = GetRSSMode.PS;
    static Thread thread = null;

    final static ArrayDeque<Datum> historyL = new ArrayDeque<Datum>(); // every hour
    final static ArrayDeque<Datum> historyM = new ArrayDeque<Datum>(); // every minute
    final static ArrayDeque<Datum> historyS = new ArrayDeque<Datum>(); // every 5 seconds
    final static int historySize = 720;

    /**
     * All the code that is needed to read info from "ps" is
     * packaged up here. Should work on MACOSX and LINUX.
     * It's not fast though.
     */
    public static class PSScraper {

        /**
         * Structure to hold the output from "ps"
         */
        public static class PSData {
            final long rss;
            final double pmem;
            final double pcpu;
            long time;
            long etime;

            public PSData(long rss, double pmem, double pcpu, long time, long etime) {
                this.rss = rss;
                this.pmem = pmem;
                this.pcpu = pcpu;
                this.time = time;
                this.etime = etime;
            }
        }

        /**
         * Givent the format "ps" uses for a time duration, parse it into
         * a numbe of milliseconds.
         */
        static long getDurationFromPSString(String duration) {
            String[] parts;

            // split into days and sub-days
            duration = duration.trim();
            parts = duration.split("-");
            assert(parts.length > 0);
            assert(parts.length <= 2);
            String dayString = "0"; if (parts.length == 2) dayString = parts[0];
            String subDayString = parts[parts.length - 1];
            long days = Long.parseLong(dayString);

            // split into > seconds in 00:00:00 time and second fractions
            subDayString = subDayString.trim();
            parts = subDayString.split("\\.");
            assert(parts.length > 0);
            assert(parts.length <= 2);
            String fractionString = "0"; if (parts.length == 2) fractionString = parts[parts.length - 1];
            subDayString = parts[0];
            while (fractionString.length() < 3) fractionString += "0";
            long miliseconds = Long.parseLong(fractionString);

            // split into hours,minutes,seconds
            parts = subDayString.split(":");
            assert(parts.length > 0);
            assert(parts.length <= 3);
            String hoursString = "0"; if (parts.length == 3) hoursString = parts[parts.length - 3];
            String minutesString = "0"; if (parts.length >= 2) minutesString = parts[parts.length - 2];
            String secondsString = parts[parts.length - 1];
            long hours = Long.parseLong(hoursString);
            long minutes = Long.parseLong(minutesString);
            long seconds = Long.parseLong(secondsString);

            // compound down to ms
            hours = hours + (days * 24);
            minutes = minutes + (hours * 60);
            seconds = seconds + (minutes * 60);
            miliseconds = miliseconds + (seconds * 1000);
            return miliseconds;
        }

        /**
         * Call up "ps" in another process and scrape the results
         * to get memory/cpu statistics.
         * @param pid The pid of the process to inquire about.
         * @return Structure containing output of the "ps" call.
         */
        public static PSData getPSData(int pid) {
            // run "ps" to get stats for this pid
            String command = String.format("ps -p %d -o rss,pmem,pcpu,time,etime", pid);
            String results = ShellTools.local_cmd(command);

            // parse ps into value array
            String[] lines = results.split("\n");
            if (lines.length != 2)
                return null;
            results = lines[1];
            results = results.trim();

            // For systems where LANG != en_US.UTF-8.
            // see: http://community.voltdb.com/node/422
            results = results.replace(",", ".");

            String[] values = results.split("\\s+");

            // tease out all the stats
            long rss = Long.valueOf(values[0]) * 1024;
            double pmem = Double.valueOf(values[1]) / 100.0;
            double pcpu = Double.valueOf(values[2]) / 100.0;
            long time = getDurationFromPSString(values[3]);
            long etime = getDurationFromPSString(values[4]);

            // create a new Datum which adds java stats
            return new PSData(rss, pmem, pcpu, time, etime);
        }
    }

    /**
     * Datum class is one sample of memory usage.
     */
    public static class Datum {
        public final long timestamp;
        public final long rss;
        public final long javatotalheapmem;
        public final long javausedheapmem;
        public final long javatotalsysmem;
        public final long javausedsysmem;

        /**
         * Constructor accepts some system values and generates some Java values.
         *
         * @param rss Resident set size.
         */
        Datum(long rss) {
            MemoryMXBean mmxb = ManagementFactory.getMemoryMXBean();
            MemoryUsage muheap = mmxb.getHeapMemoryUsage();
            MemoryUsage musys = mmxb.getNonHeapMemoryUsage();

            timestamp = System.currentTimeMillis();
            this.rss = rss;
            javatotalheapmem = muheap.getCommitted();
            javausedheapmem = muheap.getUsed();
            javatotalsysmem = musys.getCommitted();
            javausedsysmem = musys.getUsed();
        }

        /**
         * @return Print-friendly string for this Datum.
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%dms:\n", timestamp));
            sb.append(String.format("  SYS: %dM RSS, %dM Total\n",
                    rss / 1024 /1024,
                    memorysize));
            sb.append(String.format("  JAVA: HEAP(%d/%d/%dM) SYS(%d/%dM)\n",
                    javausedheapmem / 1024 / 1024,
                    javatotalheapmem / 1024 / 1024,
                    javamaxheapmem / 1024 / 1024,
                    javausedsysmem / 1024 / 1024,
                    javatotalsysmem / 1024 / 1024));
            return sb.toString();
        }

        /**
         * @return A CSV-formatted line for this Datum
         */
        String toLine() {
            return String.format("%d,%d,%d,%d,%d,%d,%d",
                    timestamp,
                    rss,
                    javausedheapmem,
                    javatotalheapmem,
                    javausedsysmem,
                    javatotalsysmem,
                    javamaxheapmem);
        }
    }

    /**
     * Synchronously collect memory stats.
     * @param medium Add result to medium set?
     * @param large Add result to large set?
     * @return The generated Datum instance.
     */
    public static Datum sampleSystemNow(final boolean medium, final boolean large) {
        Datum d = generateCurrentSample();
        if (d == null)
            return null;
        historyS.addLast(d);
        if (historyS.size() > historySize) historyS.removeFirst();
        if (medium) {
            historyM.addLast(d);
            if (historyM.size() > historySize) historyM.removeFirst();
        }
        if (large) {
            historyL.addLast(d);
            if (historyL.size() > historySize) historyL.removeFirst();

        }
        return d;
    }

    /**
     * Fire off a thread to asynchronously collect stats.
     * @param medium Add result to medium set?
     * @param large Add result to large set?
     */
    public static synchronized void asyncSampleSystemNow(final boolean medium, final boolean large) {
        // slow mode starts an async thread
        if (mode == GetRSSMode.PS) {
            if (thread != null) {
                if (thread.isAlive()) return;
                else thread = null;
            }

            thread = new Thread(new Runnable() {
                @Override
                public void run() { sampleSystemNow(medium, large); }
            });
            thread.start();
        }
        // fast mode doesn't spawn a thread
        else {
            sampleSystemNow(medium, large);
        }
    }

    /**
     * @return The most recently generated Datum.
     */
    public static synchronized Datum getRecentSample() {
        if (historyS.isEmpty()) {
            return null;
        }
        return historyS.getLast();
    }

    /**
     * Get the process id, the total memory size and determine the
     * best way to get the RSS on an ongoing basis.
     */
    private static synchronized void initialize() {
        PlatformProperties pp = PlatformProperties.getPlatformProperties();

        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        String pidString = processName.substring(0, processName.indexOf('@'));
        pid = Integer.valueOf(pidString);
        initialized = true;

        // get the RSS and other stats from scraping "ps" from the command line
        PSScraper.PSData psdata = PSScraper.getPSData(pid);
        assert(psdata.rss > 0);

        // figure out how much memory this thing has
        memorysize = pp.ramInMegabytes;
        assert(memorysize > 0);

        // now try to figure out the best way to get the rss size
        long rss = -1;

        // try the mac method
        try {
            rss = ExecutionEngine.nativeGetRSS();
        }
        // This catch is broad to specifically include the UnsatisfiedLinkError that arises when
        // using the hsqldb backend on linux -- along with any other exceptions that might arise.
        // Otherwise, the hsql backend would get an annoying report to stdout
        // as the useless stats thread got needlessly killed.
        catch (Throwable e) { }
        if (rss > 0) mode = GetRSSMode.MACOSX_NATIVE;

        // try procfs
        rss = getRSSFromProcFS();
        if (rss > 0) mode = GetRSSMode.PROCFS;

        // notify users if stats collection might be slow
        if (mode == GetRSSMode.PS) {
            VoltLogger logger = new VoltLogger("HOST");
            logger.warn("System statistics will be collected in a sub-optimal "
                    + "manner because either procfs couldn't be read from or "
                    + "the native library couldn't be loaded.");
        }
    }

    /**
     * Get the RSS using the procfs. If procfs is not
     * around, this will return -1;
     */
    private static long getRSSFromProcFS() {
        try {
            File statFile = new File(String.format("/proc/%d/stat", pid));
            FileInputStream fis = new FileInputStream(statFile);
            try {
                BufferedReader r = new BufferedReader(new InputStreamReader(fis));
                String stats = r.readLine();
                String[] parts = stats.split(" ");
                return Long.parseLong(parts[23]) * 4 * 1024;
            } finally {
                fis.close();
            }
        }
        catch (Exception e) {
            return -1;
        }
    }

    public static synchronized long getRSSMB() {
        Datum d = generateCurrentSample();
        return d.rss;
    }

    /**
     * Poll the operating system and generate a Datum
     * @return A newly created Datum instance.
     */
    private static synchronized Datum generateCurrentSample() {
        // get this info once
        if (!initialized) initialize();

        long rss = -1;
        switch (mode) {
        case MACOSX_NATIVE:
            rss = ExecutionEngine.nativeGetRSS();
            break;
        case PROCFS:
            rss = getRSSFromProcFS();
            break;
        case PS:
            rss = PSScraper.getPSData(pid).rss;
            break;
        }

        // create a new Datum which adds java stats
        Datum d = new Datum(rss);
        return d;
    }

    /**
     * Get a URL that uses the Google Charts API to show a chart of memory usage history.
     *
     * @param minutes The number of minutes the chart should cover. Tested values are 2, 30 and 1440.
     * @param width The width of the chart image in pixels.
     * @param height The height of the chart image in pixels.
     * @param timeLabel The text to put under the left end of the x axis.
     * @return A String containing the URL of the chart.
     */
    public static synchronized String getGoogleChartURL(int minutes, int width, int height, String timeLabel) {

        ArrayDeque<Datum> history = historyS;
        if (minutes > 2) history = historyM;
        if (minutes > 30) history = historyL;

        HTMLChartHelper chart = new HTMLChartHelper();
        chart.width = width;
        chart.height = height;
        chart.timeLabel = timeLabel;

        HTMLChartHelper.DataSet Jds = new HTMLChartHelper.DataSet();
        chart.data.add(Jds);
        Jds.title = "UsedJava";
        Jds.belowcolor = "ff9999";

        HTMLChartHelper.DataSet Rds = new HTMLChartHelper.DataSet();
        chart.data.add(Rds);
        Rds.title = "RSS";
        Rds.belowcolor = "ff0000";

        HTMLChartHelper.DataSet RUds = new HTMLChartHelper.DataSet();
        chart.data.add(RUds);
        RUds.title = "RSS+UnusedJava";
        RUds.dashlength = 6;
        RUds.spacelength = 3;
        RUds.thickness = 2;
        RUds.belowcolor = "ffffff";

        long cropts = System.currentTimeMillis();
        cropts -= (60 * 1000 * minutes);
        long modulo = (60 * 1000 * minutes) / 30;

        double maxmemdatum = 0;

        for (Datum d : history) {
            if (d.timestamp < cropts) continue;

            double javaused = d.javausedheapmem + d.javausedsysmem;
            double javaunused = SystemStatsCollector.javamaxheapmem - d.javausedheapmem;
            javaused /= 1204 * 1024;
            javaunused /= 1204 * 1024;
            double rss = d.rss / 1024 / 1024;

            long ts = (d.timestamp / modulo) * modulo;

            if ((rss + javaunused) > maxmemdatum)
                maxmemdatum = rss + javaunused;

            RUds.append(ts, rss + javaunused);
            Rds.append(ts, rss);
            Jds.append(ts, javaused);
        }

        chart.megsMax = 2;
        while (chart.megsMax < maxmemdatum)
            chart.megsMax *= 2;

        return chart.getURL(minutes);
    }

    public static long getStartTime() {
        return starttime;
    }

    /**
     * Manual performance testing code for getting stats.
     */
    public static void main(String[] args) {
        int repeat = 1000;
        long start, duration, correct;
        double per;

        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        String pidString = processName.substring(0, processName.indexOf('@'));
        pid = Integer.valueOf(pidString);

        org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(false);

        // test the default fallback performance
        start = System.currentTimeMillis();
        correct = 0;
        for (int i = 0; i < repeat; i++) {
            long rss = PSScraper.getPSData(pid).rss;
            if (rss > 0) correct++;
        }
        duration = System.currentTimeMillis() - start;
        per = duration / (double) repeat;
        System.out.printf("%.2f ms per \"ps\" call / %d / %d correct\n",
                per, correct, repeat);

        // test linux procfs performance
        start = System.currentTimeMillis();
        correct = 0;
        for (int i = 0; i < repeat; i++) {
            long rss = getRSSFromProcFS();
            if (rss > 0) correct++;
        }
        duration = System.currentTimeMillis() - start;
        per = duration / (double) repeat;
        System.out.printf("%.2f ms per procfs read / %d / %d correct\n",
                per, correct, repeat);

        // test mac performance
        start = System.currentTimeMillis();
        correct = 0;
        for (int i = 0; i < repeat; i++) {
            long rss = ExecutionEngine.nativeGetRSS();
            if (rss > 0) correct++;
        }
        duration = System.currentTimeMillis() - start;
        per = duration / (double) repeat;
        System.out.printf("%.2f ms per ee.nativeGetRSS call / %d / %d correct\n",
                per, correct, repeat);
    }

}
