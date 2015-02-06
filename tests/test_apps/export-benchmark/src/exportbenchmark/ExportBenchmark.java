/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
/*
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the native VoltDB client
 * library.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */

package exportbenchmark;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NullCallback;

/**
 * Asychronously sends data to an export table to test VoltDB export performance.
 */
public class ExportBenchmark {
    
    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // Client connection to the server
    final Client client;
    // Validated CLI config
    final ExportBenchConfig config;
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;
    // Timer for periodic stats
    Timer timer;
    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);

    
    static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    
    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class ExportBenchConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 10;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 5;
        
        @Option(desc = "Export table to use")
        String tableName = "allValues";

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";
        
        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (tableName == null || tableName.isEmpty()) exitWithMessageAndUsage("Table name cannot be blank");
        }
    }
    
    /**
     * Creates a new instance of the test to be run.
     * Establishes a client connection to a voltdb server, which should already be running
     * @param args The arguments passed to the program
     */
    public ExportBenchmark(ExportBenchConfig config) {
        this.config = config;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);
        
        fullStatsContext = client.createStatsContext();
        periodicStatsContext = client.createStatsContext();
    }
    
    /**
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
     */
    public void schedulePeriodicStats() {
        timer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() { printStatistics(); }
        };
        timer.scheduleAtFixedRate(statsPrinting,
                                  config.displayinterval * 1000,
                                  config.displayinterval * 1000);
    }
    
    /**
     * Checks the export table to make sure that everything has been successfully
     * processed.
     * @throws Exception
     */
    public void waitForStreamedAllocatedMemoryZero() throws Exception {
        boolean passed = false;

        VoltTable stats = null;
        long ftime = 0;
        long st = System.currentTimeMillis();
        //Wait 10 mins only
        long end = System.currentTimeMillis() + (10 * 60 * 1000);
        while (true) {
            stats = client.callProcedure("@Statistics", "table", 0).getResults()[0];
            boolean passedThisTime = true;
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                System.out.println("Waited too long...");
                System.out.println(stats);
                break;
            }
            if (ctime - st > (3 * 60 * 1000)) {
                System.out.println(stats);
                st = System.currentTimeMillis();
            }
            long ts = 0;
            while (stats.advanceRow()) {
                String ttype = stats.getString("TABLE_TYPE");
                Long tts = stats.getLong("TIMESTAMP");
                //Get highest timestamp and watch is change
                if (tts > ts) {
                    ts = tts;
                }
                if (ttype.equals("StreamedTable")) {
                    if (0 != stats.getLong("TUPLE_ALLOCATED_MEMORY")) {
                        passedThisTime = false;
                        System.out.println("Partition Not Zero.");
                        break;
                    }
                }
            }
            if (passedThisTime) {
                if (ftime == 0) {
                    ftime = ts;
                    continue;
                }
                //we got 0 stats 2 times in row with diff highest timestamp.
                if (ftime != ts) {
                    passed = true;
                    break;
                }
                System.out.println("Passed but not ready to declare victory.");
            }
            Thread.sleep(5000);
        }
        System.out.println("Passed is: " + passed);
        System.out.println(stats);
    }
    
    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServerWithRetry(String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            }
            catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    void connect(String servers) throws InterruptedException {
        System.out.println("Connecting to VoltDB...");

        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }
    
    /**
     * While <code>benchmarkComplete</code> is set to false, run as many
     * synchronous procedure calls as possible and record the results.
     *
     */
    class ExportThread implements Runnable {

        @Override
        public void run() {
            while (benchmarkComplete.get() == false) {
                // Insert objects
                try {
                    client.callProcedure(new NullCallback(), "ExportInsert", 532532, 1, 53, 64, 42, 2.452, "String", 48932098, 0x421);
                } catch (Exception e) {
                    System.err.println("Couldn't insert into VoltDB\n");
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }


    /**
     * Runs the export benchmark test
     */
    private void runTest() throws Exception{
        // Connect to servers
        System.out.println("Test initialization");
        connect(config.servers);
        
        // Start the work
        Thread thread = new Thread(new ExportThread());
        thread.start();
        
        // Run the benchmark loop for the requested warmup time
        System.out.println("Warming up...");
        Thread.sleep(1000l * config.warmup);
        
        warmupComplete.set(true);
        
        // reset the stats after initialization
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();
        
        schedulePeriodicStats();
        
        // Wait until the insertion is done
        System.out.println("\nRunning benchmark...");
        Thread.sleep(1000l * config.duration);
        System.out.println("Object insertion complete");
        
        benchmarkComplete.set(true);
        client.drain();
        
        // Wait until export is done
        try {
            waitForStreamedAllocatedMemoryZero();
        } catch (Exception e) {
            System.err.println("Error while waiting for export: ");
            e.printStackTrace();
            System.exit(1);
        }
        
        timer.cancel();
        
        // Print results & close
        printResults();
        client.close();
    }
    
    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();

        // Print an ISO8601 timestamp (of the same kind Python logging uses) to help
        // log merger correlate correctly
        System.out.print(LOG_DF.format(new Date(stats.getEndTimestamp())));
        System.out.printf(" Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/99.999%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.99999));
    }
    
    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
        System.out.printf("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1));
        System.out.printf("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25));
        System.out.printf("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5));
        System.out.printf("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75));
        System.out.printf("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9));
        System.out.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
        System.out.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));
        System.out.printf("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995));
        System.out.printf("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999));
        System.out.printf("99.999th percentile latency:   %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99999));

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" Latency Histogram");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(stats.latencyHistoReport());

        // Write stats to file if requested
        client.writeSummaryCSV(stats, config.statsfile);
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */
    public static void main(String[] args) throws Exception {
        ExportBenchConfig config = new ExportBenchConfig();
        config.parse(ExportBenchmark.class.getName(), args);
        
        ExportBenchmark bench = new ExportBenchmark(config);
        bench.runTest();
    }
}