/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
 * This program exercises the socket import capability by writing
 * <key, value> pairs to one or more VoltDB socket importers.
 *
 * The pairs accumulate in a Queue structure. The program removes pairs
 * from the Queue and uses asynchronous database queuries to verify that
 * all the pairs written to the socket interface are present and have
 * matching values.
 *
 * The checking proceeds in parallel as the socket writers write to the
 * socket importers, and continues on until all pairs have been checked and
 * the database has time to complete all socket importer input transactions.
 *
 * The "perftest" option skips the queuing/checking functions to max out and measure
 * import speed.
 *
 * Opton "partitioned" designates the target table and related SP's are partitioned.
 *
 * If this option is omitted, the table is replicated.
 */

package socketimporter.client.socketimporter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.RandomStringUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStatsContext;

import com.google_voltpatches.common.net.HostAndPort;

public class AsyncBenchmark {

    static VoltLogger log = new VoltLogger("Benchmark");

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // queue structure to hold data as it's written, so we can check it all get's into the database
    static Queue<Pair<String,String>> queue = new LinkedBlockingQueue<Pair<String,String>>();
    static Queue<Pair<String,String>> dqueue = new LinkedBlockingQueue<Pair<String,String>>();
    static boolean importerDone = false;
    static DataUtils checkDB = null;

    // validated command line configuration
    static Config config;
    // Timer for periodic stats printing
    static Timer timer;
    // Benchmark start time
    static long benchmarkStartTS;

    static final Map<HostAndPort, OutputStream> haplist = new HashMap<HostAndPort, OutputStream>();
    static Client client;
    // Some thread safe counters for reporting
    AtomicLong linesRead = new AtomicLong(0);
    AtomicLong rowsAdded = new AtomicLong(0);
    static final AtomicLong rowsChecked = new AtomicLong(0);
    static final AtomicLong rowsMismatch = new AtomicLong(0);
    static final AtomicLong writers = new AtomicLong(0);
    static final AtomicLong socketWrites = new AtomicLong(0);
    static final AtomicLong socketWriteExceptions = new AtomicLong(0);
    static final AtomicLong runCount = new AtomicLong(0);
    static final AtomicLong warmupCount = new AtomicLong(0);

    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class Config extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 300;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 20;

        @Option(desc = "Comma separated list of the form server[:port] to connect to database for queuries")
        String servers = "localhost";

        @Option(desc = "Comma separated list of the form server[:port] to connect to socket stream")
        String sockservers = "localhost";

        @Option(desc = "Report latency for async benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Performance test only.")
        boolean perftest = false;

        @Option(desc = "If true, use the partitioned table for the benchmark; replicated otherwise.")
        boolean partitioned = false;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (perftest && statsfile.length() == 0) statsfile = "socketimporter.csv";
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public AsyncBenchmark(Config config) {
        this.config = config;
        //AsyncBenchmark.config = config;
        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    static OutputStream connectToOneServerWithRetry(String server, int port) {
        int sleep = 1000;
        while (true) {
            try {
                Socket pushSocket = new Socket(server, port);
                OutputStream out = pushSocket.getOutputStream();
                System.out.printf("Connected to VoltDB node at: %s.\n", server);
                return out;
            }
            catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @param port
     * @throws InterruptedException if anything bad happens with the threads.
     */
    static void connect(String servers) throws InterruptedException {
        log.info("Connecting to Socket Streaming Interface...");

        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    // default port; assumed in system test so keep sync'd if it's changed
                    HostAndPort hap = HostAndPort.fromString(server).withDefaultPort(7001);
                    OutputStream writer = connectToOneServerWithRetry(hap.getHostText(), hap.getPort());
                    haplist.put(hap, writer);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    /**
     * Connect to one or more VoltDB servers.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional). Assumes 21212 if not specified otherwise.
     * @throws InterruptedException if anything bad happens with the threads.
     */
    static void dbconnect(String servers) throws InterruptedException, Exception {
        log.info("Connecting to VoltDB Interface...");

        String[] serverArray = servers.split(",");
        client = ClientFactory.createClient();
        for (String server : serverArray) {
            log.info("..." + server);
            client.createConnection(server);
        }
    }

    /**
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
     */
    public static void schedulePeriodicStats() {
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
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized static void printStatistics() {
        try {
            long thrup;

            long max_insert_time = checkDB.maxInsertTime();
            thrup = (long) (runCount.get() / ((max_insert_time-benchmarkStartTS)/1000.0));

            if (thrup > 0) { // first time through, calc can be whacky
                log.info(String.format("Import Throughput %d/s, Total Rows %d",
                    thrup, runCount.get()+warmupCount.get()));
            }
        } catch (Exception e) {
            log.info("Exception in printStatistics" + e);
            e.printStackTrace();
        }
        log.info("Import stats: " + UtilQueries.getImportStats(client));
    }

    /**
     * Prints the results to a csv file for charting
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized static void printResults() throws Exception {
        FileWriter fw = null;

        if ((config.statsfile != null) && (config.statsfile.length() != 0)) {
            fw = new FileWriter(config.statsfile);
            fw.append(String.format("%s,%d,-1,%d,0,0,0,0,0,0,0,0,0,0\n",
                    (config.partitioned ? "Partitioned" : "Replicated"),
                    benchmarkStartTS/1000, // back to seconds
                    runCount.get()/((checkDB.maxInsertTime()-benchmarkStartTS)/1000))); // throughput -- TPS
            fw.close();
        }
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark(HostAndPort hap) throws Exception {
        System.out.print(HORIZONTAL_RULE);
        log.info(" Setup & Initialization");
        log.info(HORIZONTAL_RULE);

        System.out.print(HORIZONTAL_RULE);
        log.info(" Starting Benchmark");
        log.info(HORIZONTAL_RULE);

        SecureRandom rnd = new SecureRandom();
        rnd.setSeed(Thread.currentThread().getId());
        log.info("Warming up...");
        final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
        while (warmupEndTime > System.currentTimeMillis()) {
            String key = Long.toString(rnd.nextLong());
            String s;
            if (config.perftest) {
                String valString = RandomStringUtils.randomAlphanumeric(1024);
                s = key + "," + valString + "\n";
            } else {
                String t = Long.toString(System.currentTimeMillis());
                Pair<String,String> p = new Pair<String,String>(key, t);
                queue.offer(p);
                s = key + "," + t + "\n";
            }
            writeFully(s, hap, warmupEndTime);
            warmupCount.getAndIncrement();
        }

        benchmarkStartTS = System.currentTimeMillis();

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        // Save the key/value pairs so they can be verified through the database
        log.info("\nRunning benchmark...");
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {
            String key = Long.toString(rnd.nextLong());
            String s;
            if (config.perftest) {
                String valString = RandomStringUtils.randomAlphanumeric(16);
                s = key + "," + valString + "\n";
            } else {
                String t = Long.toString(System.currentTimeMillis());
                Pair<String,String> p = new Pair<String,String>(key, t);
                queue.offer(p);
                s = key + "," + t + "\n";
            }
            writeFully(s, hap, benchmarkEndTime);
            runCount.getAndIncrement();
        }
        haplist.get(hap).flush();
        log.info("Benchmark loop complete for this thread.");
        if (timer != null) timer.cancel();
    }

    private void writeFully(String data, HostAndPort hap, long endTime) {
        while (System.currentTimeMillis() < endTime) {
            try {
                OutputStream writer = haplist.get(hap);
                writer.write(data.getBytes());
                socketWrites.incrementAndGet();
                return;
            } catch (IOException ex) {
                log.info("Exception: " + ex);
                OutputStream writer = connectToOneServerWithRetry(hap.getHostText(), hap.getPort());
                haplist.put(hap, writer);
                socketWriteExceptions.incrementAndGet();
            }
        }
    }

    public static class BenchmarkRunner extends Thread {
        private final AsyncBenchmark benchmark;
        private final CountDownLatch cdl;
        private final HostAndPort hap;
        public BenchmarkRunner(AsyncBenchmark bm, CountDownLatch c, HostAndPort iidx) {
            benchmark = bm;
            cdl = c;
            hap = iidx;
        }

        @Override
        public void run() {
            try {
                benchmark.runBenchmark(hap);
                writers.incrementAndGet();
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                cdl.countDown();
            }
        }
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link VoterConfig}
     */
    public static void main(String[] args) throws Exception {
        VoltLogger log = new VoltLogger("Benchmark.main");
        final long WAIT_FOR_A_WHILE = 100 * 1000; // 5 minutes in milliseconds
        // create a configuration from the arguments
        Config config = new Config();
        config.parse(AsyncBenchmark.class.getName(), args);
        System.out.print(HORIZONTAL_RULE);
        log.info(" Command Line Configuration");
        log.info(HORIZONTAL_RULE);
        log.info(config.getConfigDumpString());
        if(config.latencyreport) {
            log.info("NOTICE: Not implemented in this benchmark client.\n");
        }

        // connect to one or more servers, loop until success
        dbconnect(config.servers);

        log.info("Setting up DDL");
        checkDB = new DataUtils(queue, dqueue, client, config.partitioned);
        checkDB.ddlSetup(config.partitioned);

        connect(config.sockservers);

        CountDownLatch cdl = new CountDownLatch(haplist.size());
        for (HostAndPort hap : haplist.keySet()) {
            AsyncBenchmark benchmark = new AsyncBenchmark(config);
            BenchmarkRunner runner = new BenchmarkRunner(benchmark, cdl, hap);
            runner.start();
        }
        schedulePeriodicStats();

        if (!config.perftest) {
            // start checking the table that's being populated by the socket injester(s)
            while (queue.size() == 0) {
                try {
                    Thread.sleep(1000);                 // one second.
                } catch(InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
            log.info("Starting CheckData methods. Queue size: " + queue.size());
            checkDB.processQueue();
        }
        log.info("-- waiting for socket writers.");
        // this hangs occasionally, so adding a timeout with a margin
        cdl.await(config.duration+config.warmup+1, TimeUnit.SECONDS);

        // close socket connections...
        for (HostAndPort hap : haplist.keySet()) {
             OutputStream writer = haplist.get(hap);
             writer.flush();
             writer.close();
         }

        // print the summary results
        printResults();

        if (!config.perftest) {
            log.info("...starting timed check looping... " + queue.size());
            final long queueEndTime = System.currentTimeMillis() + WAIT_FOR_A_WHILE;
            log.info("Continue checking for " + (queueEndTime-System.currentTimeMillis())/1000 + " seconds.");
            while (queueEndTime > System.currentTimeMillis()) {
                checkDB.processQueue();
            }
        }

        // final exit criteria -- queue of outstanding importer requests goes to zero
        // but with checking for no-progress so we don't get stuck forever.
        long outstandingRequests = UtilQueries.getImportOutstandingRequests(client);
        long prev_outstandingRequests = outstandingRequests;
        int waitloops = 10; // kinda arbitrary but if outstanding requests is not changing for this interval...
        while (outstandingRequests != 0 && waitloops > 0) {
            log.info("Importer outstanding requests is " + outstandingRequests + ". Waiting for zero.");
            outstandingRequests = UtilQueries.getImportOutstandingRequests(client);
            if (prev_outstandingRequests == outstandingRequests) {
                log.info("Outstanding requests unchanged since last interval.");
                waitloops--;
            }
            prev_outstandingRequests = outstandingRequests;
            Thread.sleep(config.displayinterval*1000);
        }

        client.drain();
        client.close();

        if (!config.perftest) {
            log.info("Queued tuples remaining: " + queue.size());
            log.info("Rows checked against database: " + rowsChecked.get());
            log.info("Mismatch rows (value imported <> value in DB): " + rowsMismatch.get());
        }
        log.info("Total rows added by Socket Injester: " + (warmupCount.get()+runCount.get()));
        log.info("Socket write count: " + socketWrites.get());
        log.info("Socket write exception count: " + socketWriteExceptions.get());

        System.exit(0);
    }
}
