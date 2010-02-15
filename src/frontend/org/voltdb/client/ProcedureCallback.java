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

package org.voltdb.client;

import java.io.IOException;

import org.apache.log4j.*;
import java.io.*;
import java.util.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.voltdb.utils.VoltLoggerFactory;

/**
 * Abstract base class for callbacks that are invoked when an asynchronously invoked transaction receives a response.
 * Extend this class and provide an implementation of {@link #clientCallback} to receive a response to a
 * stored procedure invocation.
 */
public abstract class ProcedureCallback {
    final void invokeCallback(ClientResponse clientResponse) {
        clientCallback(clientResponse);
    }

    /**
     * Implementation of callback to be provided by client applications
     * @param clientResponse Response to the stored procedure invocation this callback is associated with
     */
    abstract protected void clientCallback(ClientResponse clientResponse);

    /**
     * Cheasy hack to compile out code in various places that does extra messaging
     * and sampling for measuring latency as a transaction executes.
     */
    public static final boolean measureLatency = false;

    // BEGIN LATENCY CHECKING INFO
    long callTimeInNanos = 0;

    /**
     * A context for storing information about the number of invocations and how logn those invocations
     * took
     *
     */
    static class TimingContext {

        /**
         * Index used to assign increasing numbers to each report
         */
        int timesReportIndex = 0;

        /**
         * Index used to assign increasing numbers to each report
         */
        int countsReportIndex = 0;

        /**
         * Total time taken to execute all transactions
         */
        public long totalCallTime;
        public long sumOfSquaresForCallTime;
        public long totalCallCount;
        public long maxDuration = Long.MIN_VALUE;
        public long minDuration = Long.MAX_VALUE;

        /**
         * Times for a specific interval (number of txns).
         * The key is a specific quantity of latency in milliseconds and the value
         * is a count of the number of transactions that had that latency.
         */
        public final HashMap<Long, Long> times = new HashMap<Long, Long>();

        /**
         * Times for the whole run.
         * The key is a specific quantity of latency in milliseconds and the value
         * is a count of the number of transactions that had that latency
         */
        public final HashMap<Long, Long> totalTimes = new HashMap<Long, Long>();

        /**
         * Invocation counts for a specific interval (number of txns)
         * The key is an Object identifying a specific client connection
         * and the value is the number of transactions successfully invoked by
         * that client.
         */
        public final HashMap<Object, Long> invocationCount = new HashMap<Object, Long>();

        /**
         * Invocation counts for the whole run
         * The key is an Object identifying a specific client connection
         * and the value is the number of transactions successfully invoked by
         * that client.
         */
        public final HashMap<Object, Long> totalInvocationCount = new HashMap<Object, Long>();

        /**
         * Constructs a shutdown hook that will output the totals as graphs
         */
        public TimingContext() {
            if (measureLatency) {
                Thread logTimesThread = new Thread() {
                    @Override
                    public void run() {
                        final File timesFile = new File(hostname + "-latency-distribution-total.png");
                        //final File countsFile = new File(hostname + "-counts-distribution-total.png");
                        final Thread t1 = logTimes(totalTimes, timesFile);
                        //final Thread t2 = logCounts(totalInvocationCount, countsFile);
                        try {
                            t1.join();
                            //t2.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                };
                logTimesThread.setDaemon(false);
                Runtime.getRuntime().addShutdownHook(logTimesThread);
            }
        }
    }

    /**
     * Logger to record timing information to a file
     */
    static final Logger timesLogger = Logger.getLogger("times", VoltLoggerFactory.instance());

    /**
     * Store the hostname of this host for use in filenames
     */
    static final String hostname;

    /**
     * Retrieve the hostname and configure a file appender
     * to record latencies for each interval.
     */
    static {
        String hostnameTemp = null;
        try {
            InetAddress addr = InetAddress.getLocalHost();

            // Get hostname
            hostnameTemp = addr.getHostName();
        } catch (UnknownHostException e) {
        }
        hostname = hostnameTemp;

        if (measureLatency) {
            timesLogger.removeAllAppenders();
            try {
                FileAppender fa = new FileAppender(new TTCCLayout(), hostname + "-latencies.log", false, false, 8192);
                timesLogger.addAppender(fa);
                timesLogger.setLevel(Level.INFO);
                timesLogger.setAdditivity(false);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    /**
     * Take a map of latencies and counts and write them to a temp file and then invoke GNU graph
     * to prettify things.
     * @param times
     * @param file File that the pretty graph should be output to
     * @return A thread that will monitor the progress of GNU graph and exit when graph terminates
     */
    private static Thread logTimes(HashMap<Long, Long> times, final File file) {

        /*
         * Sort the times so that the graph is sensical. Sorted on key (latency)
         * which should create a graph showing the distribution of latency
         * from lowest to highest.
         */
        TreeMap<Long, Long> sortedTimes = new TreeMap<Long, Long>(times);

        /*
         * Output the sorted results to a temp file
         */
        final File tempFile;
        try {
            tempFile = new File(file.getName().substring(0, file.getName().length() - 4));
            final FileWriter fw = new FileWriter(tempFile);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter pwr = new PrintWriter(bw);
            for (Map.Entry<Long, Long> e : sortedTimes.entrySet()) {
                  pwr.append('\n');
                  pwr.append(e.getKey().toString()).append(' ').append(e.getValue().toString());
            }
            pwr.flush();
            pwr.close();
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }

        /*
         * Invoke graph in a separate process and create a thread to forward the output
         * to stderr
         */
        try {
            String args[] = new String[] {
                "/bin/sh",
                "-c",
                "/usr/bin/graph -T png --bitmap-size 1024x768 -L \"Distribution of latency\" " +
                "-Y \"Number of invocations with this latency\" -w .8 -h.8 -u .1 " +
                "-X \"Latency in milliseconds\" " +
                "-r .15 -f.03 " + tempFile.getAbsolutePath() +
                " > " + file.getName(),
            };
            ProcessBuilder pb = new ProcessBuilder(args);
            pb.redirectErrorStream(true);
            final Process p = pb.start();
            Thread t = new Thread() {
                @Override
                public void run() {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    try {
                    while (true) {
                        final String line = reader.readLine();
                        if (line == null) {
                            try {
                                p.waitFor();
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            return;
                        }
                      System.err.println(line);
                    }
                    } catch (IOException e) {

                    }
                }
            };
            t.start();
            return t;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    /**
     * A comparator that compares keys based on their value in a map. Makes it possible
     * to sort a TreeMap by value instead of by key.
     *
     */
    private static class ObjectMapValueComparator implements Comparator<Object> {
        HashMap<Object, Long> baseMap;

        public ObjectMapValueComparator(HashMap<Object, Long> map) {
            baseMap = map;
        }

        @Override
        public int compare(Object o1, Object o2) {
            if (!baseMap.containsKey(o1) || !baseMap.containsKey(o2)) {
                return 0;
            }

            return baseMap.get(o1).compareTo(baseMap.get(o2));
        }
    }

    /**
     * Take a map of invocation counts and write them to a temp file and then invoke GNU graph
     * to prettify things.
     * @param times
     * @param file File that the pretty graph should be output to
     * @return A thread that will monitor the progress of GNU graph and exit when graph terminates
     */
    @SuppressWarnings("unused")
    private static Thread logCounts(HashMap<Object, Long> counts, final File file) {
        /*
         * Sort the counts by value (number of invocations) rather then by key (Client).
         */
        TreeMap<Object, Long> sortedCounts = new TreeMap<Object, Long>(new ObjectMapValueComparator(counts));
        sortedCounts.putAll(counts);

        /*
         * Output the sorted invocation counts to a temp file. Replace the key with integers.
         */
        final File tempFile;
        try {
            tempFile = new File(file.getName().substring(0, file.getName().length() - 4));
            final FileWriter fw = new FileWriter(tempFile);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter pwr = new PrintWriter(bw);
            int ii = 0;
            for (Map.Entry<Object, Long> e : sortedCounts.entrySet()) {
                  pwr.append('\n');
                  pwr.append(Integer.toString(ii++)).append(' ').append(e.getValue().toString());
            }
            pwr.flush();
            pwr.close();
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }

        /*
         * Start graph in a separate process and create a thread to monitor progress and forward
         * output from graph to stderr.
         */
        try {
            String args[] = new String[] {
                "/bin/sh",
                "-c",
                "/usr/bin/graph -T png --bitmap-size 1024x768 -L \"# invocations/client\" " +
                "-Y \"# invocations\" -X \"client\" " +
                "-w .8 -h.8 -u .1 " +
                "-r .15 -f.03 " + tempFile.getAbsolutePath() +
                " > " + file.getName(),
            };
            ProcessBuilder pb = new ProcessBuilder(args);
            pb.redirectErrorStream(true);
            final Process p = pb.start();
            Thread t = new Thread() {
                @Override
                public void run() {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    try {
                    while (true) {
                        final String line = reader.readLine();
                        if (line == null) {
                            try {
                                p.waitFor();
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            return;
                        }
                      System.err.println(line);
                    }
                    } catch (IOException e) {

                    }
                }
            };
            t.start();
            return t;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Do work related to the completion of a transaction. Use the current time to measure latency and increment
     * the invocation counts. If enough transactions have been completed in this context output some latency stats
     * and produce graphs of invocation counts and latency.
     * @param context Context in which times or invocation counts are being recorded. The supplied context could be
     * global to the application or there could be a context per client etc.
     * @param invocationCounterKey A key identifying what generated the invocation for this transaction. Normally this
     * will be an Object identifying the connection that originated this transaction.
     * @param queueTime Time the request was queued at the client
     * @param CIAcceptTime Time the reqest was accepted at a ClientInterface
     * @param FHReceiveTime Time the request was received at a FH and queued in a priority queue
     * @param FHResponseTime Time the request was pulled from the priority queue, executed,
     * and a response was queued to the client
     * @param initiatorReceiveTime TIme the response from the FH was received at the originating CI and then forwarded
     * to the client.
     */
    void closeTimer(
            TimingContext context,
            Object invocationCounterKey,
            long queueTime,
            long CIAcceptTime,
            long FHReceiveTime,
            long FHResponseTime,
            long initiatorReceiveTime) {
        synchronized(context) {
            final long now = System.nanoTime();
            final long durationNano = now - callTimeInNanos;
            final long durationNanoToMillis = (long)(durationNano / 1000000.0);

            final Long time = context.times.get( durationNanoToMillis);
            if (time != null) {
                context.times.put( durationNanoToMillis, time.longValue() + 1);
            } else {
                context.times.put( durationNanoToMillis, 1L);
            }
            final Long totalTime = context.totalTimes.get(durationNanoToMillis);
            if (totalTime != null) {
                context.totalTimes.put( durationNanoToMillis, totalTime.longValue() + 1);
            } else {
                context.totalTimes.put( durationNanoToMillis, 1L);
            }

            final Long invocationCount = context.invocationCount.get(invocationCounterKey);
            if (invocationCount != null) {
                context.invocationCount.put( invocationCounterKey, new Long(invocationCount.longValue() + 1));
            } else {
                context.invocationCount.put( invocationCounterKey, 1L);
            }

            final Long totalInvocationCount = context.totalInvocationCount.get( invocationCounterKey );
            context.totalInvocationCount.put( invocationCounterKey, totalInvocationCount.longValue() + 1);

            context.totalCallTime += durationNano;
            context.sumOfSquaresForCallTime += durationNano * durationNano;
            context.totalCallCount++;
            if (durationNano > context.maxDuration) context.maxDuration = durationNano;
            if (durationNano < context.minDuration) context.minDuration = durationNano;

            if ((context.totalCallCount % 144000000) == 0) {
            //if ((context.totalCallCount % (300000 / 2)) == 0) {
                final CharArrayWriter caw = new CharArrayWriter(8192);
                final PrintWriter pw = new PrintWriter(caw);
                double ave = context.totalCallTime / (double)context.totalCallCount;
                double s2 = context.sumOfSquaresForCallTime / (context.totalCallCount - 1.0) - (ave * ave);
                double s = Math.sqrt(s2);

                final File timesFile = new File(hostname + "-latency-distribution-" + context.timesReportIndex++ + ".png");
                logTimes(context.times, timesFile);
                context.times.clear();
                //final File countsFile = new File(hostname + "-invocation-distribution-" + context.countsReportIndex++ + ".png");
                //logCounts( context.invocationCount, countsFile);
                pw.printf("Latency ave/max/min/stddev = %.3f / %.3f / %.3f / %.3f ms\n",
                        ave / 1000000.0, context.maxDuration / 1000000.0, context.minDuration / 1000000.0, s / 1000000.0);
                pw.flush();
                timesLogger.info(caw.toString());

                context.totalCallTime = 0;
                context.sumOfSquaresForCallTime = 0;
                context.totalCallCount = 0;
                context.maxDuration = Long.MIN_VALUE;
                context.minDuration = Long.MAX_VALUE;
            }
        }
    }
    // END LATENCY CHECKING INFO
}
