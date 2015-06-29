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
 * Bogus comment, probably inherited from someplace -- replace!
 *
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.text.SimpleDateFormat;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

/**
 * Asynchronously sends data to an export table to test VoltDB export performance.
 */
public class ExportBenchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // Client connection to the server
    static Client client;
    // Validated CLI config
    final ExportBenchConfig config;
    // Ratio of DB inserts to export inserts
    int ratio;
    // Network variables
    Selector statsSocketSelector;
    Thread statsThread;
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    // Statistics manager objects from the client
    static ClientStatsContext periodicStatsContext;
    static ClientStatsContext fullStatsContext;
    // Timer for periodic stats
    Timer periodicStatsTimer;
    // Test stats variables
    long totalInserts = 0;
    AtomicLong successfulInserts = new AtomicLong(0);
    AtomicLong failedInserts = new AtomicLong(0);
    AtomicBoolean testFinished = new AtomicBoolean(false);

    // collectors for min/max/start/stop statistics
    double min = -1;
    double max = 0;
    double start = 0;
    double end = 0;

    int dbInserts;
    int exportInserts;

    // Test timestamp markers
    long benchmarkStartTS, benchmarkWarmupEndTS, benchmarkEndTS, serverStartTS, serverEndTS, decodeTime, partCount;

    static long samples = 0;
    static long sampleSum = 0;

    static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class ExportBenchConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 1;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 30;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 10;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option (desc = "Port on which to listen for statistics info from export clients")
        int statsPort = 5001;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Filename to write periodic stat infomation in CSV format")
        String csvfile = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
        }
    }

    /**
     * Callback for export insert method. Tracks successes & failures
     */
    class ExportCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) {
            if (response.getStatus() == ClientResponse.SUCCESS) {
                successfulInserts.incrementAndGet();
            } else {
                failedInserts.incrementAndGet();
            }
        }
    }

    /**
     * Clean way of exiting from an exception
     * @param message   Message to accompany the exception
     * @param e         The exception thrown
     */
    private void exitWithException(String message, Exception e) {
        System.err.println(message);
        System.err.println(e.getLocalizedMessage());
        System.exit(1);
    }

    /**
     * Creates a new instance of the test to be run.
     * Establishes a client connection to a voltdb server, which should already be running
     * @param args The arguments passed to the program
     */
    public ExportBenchmark(ExportBenchConfig config, int dbInserts, int exportInserts) {
        this.config = config;
        this.dbInserts = dbInserts;
        this.exportInserts = exportInserts;
        samples = 0;
        sampleSum = 0;
        serverStartTS = serverEndTS = decodeTime = partCount = 0;
    }

    /**
     * Prints a one line update on performance
     * periodically during benchmark.
     */
    public synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);
        long thrup;

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        thrup = stats.getTxnThroughput();
        System.out.printf("Throughput %d/s, ", thrup);
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/95%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.95));
        samples++;
        if (samples > 3) {
            sampleSum += thrup;
        }

        // collect run statistics -- TPS min, max, start, end
        if (min == -1 || thrup < min) min = thrup;
        if (start == 0) start = thrup;
        if (thrup > max) max = thrup;
        end = thrup;
    }

    /**
    * Create a Timer task to display performance data.
    * It calls printStatistics() every displayInterval seconds
    */
    public void schedulePeriodicStats() {
        periodicStatsTimer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() { printStatistics(); }
        };
        periodicStatsTimer.scheduleAtFixedRate(statsPrinting,
                                    config.displayinterval * 1000,
                                    config.displayinterval * 1000);
    }

    /**
     * Inserts values into the export table for the test. First it does warmup
     * inserts, then tracked inserts.
     * @param ratio
     * @throws InterruptedException
     * @throws NoConnectionsException
     */
    public void doInserts(Client client, int ratio) {

        // Make sure DB tables are empty
        System.out.println("Truncating DB tables");
        try {
            client.callProcedure("TruncateTables");
        } catch (IOException | ProcCallException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        // Don't track warmup inserts
        System.out.println("Warming up...");
        long now = System.currentTimeMillis();
        AtomicLong rowId = new AtomicLong(0);
        while (benchmarkWarmupEndTS > now) {
            try {
                client.callProcedure(
                        new NullCallback(),
                        "InsertExport",
                        rowId.getAndIncrement(),
                        dbInserts,
                        exportInserts,
                        0);
                // Check the time every 50 transactions to avoid invoking System.currentTimeMillis() too much
                if (++totalInserts % 50 == 0) {
                    now = System.currentTimeMillis();
                }
            } catch (Exception ignore) {}
        }
        System.out.println("Warmup complete");
        rowId.set(0);

        // reset the stats after warmup is done
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        schedulePeriodicStats();

        // Insert objects until we've run for long enough
        System.out.println("Running benchmark...");
        System.out.println("Calling SP " + "InsertExport");
        now = System.currentTimeMillis();
        while (benchmarkEndTS > now) {
            try {

                client.callProcedure(
                        new ExportCallback(),
                        "InsertExport",
                        rowId.getAndIncrement(),
                        dbInserts,
                        exportInserts,
                        0);
                // Check the time every 50 transactions to avoid invoking System.currentTimeMillis() too much
                if (++totalInserts % 50 == 0) {
                    now = System.currentTimeMillis();
                }
            } catch (Exception e) {
                System.err.println("Couldn't insert into VoltDB\n");
                e.printStackTrace();
                System.exit(1);
            }
        }

        try {
            client.drain();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Benchmark complete: wrote " + successfulInserts.get() + " objects");
        System.out.println("Failed to insert " + failedInserts.get() + " objects");

        testFinished.set(true);
        //statsSocketSelector.wakeup();
    }

    /**
     * Runs the export benchmark test
     * @throws InterruptedException
     * @throws NoConnectionsException
     */
    private void runTest() throws InterruptedException {
        // Figure out how long to run for
        benchmarkStartTS = System.currentTimeMillis();
        benchmarkWarmupEndTS = benchmarkStartTS + (config.warmup * 1000);
        benchmarkEndTS = benchmarkWarmupEndTS + (config.duration * 1000);

        // Do the inserts in a separate thread
        Thread writes = new Thread(new Runnable() {
            @Override
            public void run() {
                doInserts(client, ratio);
            }
        });
        writes.start();

        // Listen for stats until we stop
        Thread.sleep(config.warmup * 1000);
        // setupSocketListener();
        // listenForStats();

        writes.join();
        periodicStatsTimer.cancel();
        System.out.println("Client flushed; waiting for export to finish");

        boolean success = false;
        /***
         * Wait until export is done

        try {
            success = Stats.waitForStreamedAllocatedMemoryZero();
        } catch (IOException e) {
            System.err.println("Error while waiting for export: ");
            e.getLocalizedMessage();
        } catch (ProcCallException e) {
            System.err.println("Error while calling procedures: ");S
            e.getLocalizedMessage();
        }
        ***/
        // Print results & close
        System.out.println("Finished benchmark");
        System.out.println("Throughput");
        System.out.format("Start %6.0f, End %6.0f. Delta %6.2f%%%n" , start, end, (end-start)/start*100.0);
        System.out.format("Min %6.0f, Max %6.0f. Delta %6.2f%%%n", min, max, (max-min)/min*100.0);

        try {
            client.drain();
        } catch (NoConnectionsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    static void connectToOneServerWithRetry(String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            }
            catch (IOException e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (InterruptedException interruted) {}
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
    static void connect(String servers) throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);
        fullStatsContext = client.createStatsContext();
        periodicStatsContext = client.createStatsContext();

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
    /* Main routine creates a benchmark instance and kicks off the
     * run method for each configuration variant.
     *
     * We write to a DB table AND a export table in various ratios
     * so for test 1, there's one write to the DB table, none to the export table
     * and for test 2, again there's one write the DB table and one to the export table
     * and so on...
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */

    public static void main(String[] args) {
        ExportBenchConfig config = new ExportBenchConfig();
        config.parse(ExportBenchmark.class.getName(), args);
        // set up the distinct tests -- (dbInserts, exportInserts) pairs
        final int DBINSERTS = 0;
        final int EXPORTINSERTS = 1;
        int[][] tests = {{5,5}};

     // Connect to servers
        try {
            System.out.println("Test initialization. Servers: " + config.servers);
            connect(config.servers);
        } catch (InterruptedException e) {
            System.err.println("Error connecting to VoltDB");
            e.printStackTrace();
            System.exit(1);
        }

        for (int test = 0; test < tests.length; test++) {
            try {
                 ExportBenchmark bench = new ExportBenchmark(config,tests[test][DBINSERTS], tests[test][EXPORTINSERTS]);
                 System.out.println("Running trial " + test + " -- DB Inserts: " + tests[test][DBINSERTS] + ", Export Inserts: " + tests[test][EXPORTINSERTS]);
                 bench.runTest();
            } catch (InterruptedException e) {
                 e.printStackTrace();
            }

        }
    }
}
