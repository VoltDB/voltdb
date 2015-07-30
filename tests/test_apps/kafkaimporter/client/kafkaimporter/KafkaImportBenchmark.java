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
 * This program exercises the Kafka import capability by inserting
 * <key, value> pairs into both a VoltDB table -- KAFKAMIRRORTABLE1, and
 * a export table -- KAFKAEXPORTTABLE1. The export table links to a
 * topic in a Kafka complex. The deployment file also add an import from
 * that same complex. The checking proceeds in parallel, checking
 * the rows in the KAFKAIMPORTTABLE1 with rows in the mirror table.
 * Matching rows are deleted from both tables. Separate threads check
 * statistics on both the export table and import table to determine when
 * both import and export activity have quiesced. At the end of a
 * successful run, both the import table and the mirror table are empty.
 * If there are rows left in the mirror table, then not all exported
 * rows have made the round trip back to the import table, or there might
 * be data corruption causing the match process to fail.
 */

package kafkaimporter.client.kafkaimporter;

import java.io.OutputStream;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltcore.logging.VoltLogger;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.net.HostAndPort;

public class KafkaImportBenchmark {

    static VoltLogger log = new VoltLogger("Benchmark");

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------";

    // Statistics manager objects from the client
    static ClientStatsContext periodicStatsContext;

    // validated command line configuration
    final Config config;
    // Timer for periodic stats printing
    Timer statsTimer;
    static Timer checkTimer;
    // Benchmark start time
    long benchmarkStartTS;

    static final Map<HostAndPort, OutputStream> haplist = new HashMap<HostAndPort, OutputStream>();
    static Client client;
    // Some thread safe counters for reporting
    AtomicLong linesRead = new AtomicLong(0);
    AtomicLong rowsAdded = new AtomicLong(0);
    static final AtomicLong rowsChecked = new AtomicLong(0);
    static final AtomicLong finalInsertCount = new AtomicLong(0);

    static InsertExport exportProc;
    static TableChangeMonitor exportMon;
    static TableChangeMonitor importMon;
    static MatchChecks matchChecks;

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
        int warmup = 2;

        @Option(desc = "Comma separated list of the form server[:port] to connect to for database queuries")
        String servers = "localhost";

        @Option(desc = "Report latency for kafka benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            log.info("finished arg validate");
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public KafkaImportBenchmark(Config config) {
        this.config = config;
        periodicStatsContext = client.createStatsContext();

        log.info(HORIZONTAL_RULE);
        log.info(" Command Line Configuration");
        log.info(HORIZONTAL_RULE);
        log.info(config.getConfigDumpString());
        if(config.latencyreport) {
            log.warn("Option latencyreport is ON for async run, please set a reasonable ratelimit.\n");
        }
    }

    /**
     * Connect to one or more VoltDB servers.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional). Assumes 21212 if not specified otherwise.
     * @throws InterruptedException if anything bad happens with the threads.
     */
    static void dbconnect(String servers) throws InterruptedException, Exception {
        final Splitter COMMA_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

        log.info("Connecting to VoltDB Interface...");
        client = ClientFactory.createClient();

        for (String server: COMMA_SPLITTER.split(servers)) {
            log.info("..." + server);
            client.createConnection(server);
        }
    }

    /**
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
     */
    public void schedulePeriodicStats() {
        statsTimer = new Timer("periodicStats", true);
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() { printStatistics(); }
        };
        statsTimer.scheduleAtFixedRate(statsPrinting,
                                  config.displayinterval * 1000,
                                  config.displayinterval * 1000);
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);
        long thrup;

        thrup = stats.getTxnThroughput();
        log.info(String.format("Throughput %d/s, Aborts/Failures %d/%d, Avg/95%% Latency %.2f/%.2fms",
                    thrup, stats.getInvocationAborts(), stats.getInvocationErrors(),
                    stats.getAverageLatency(), stats.kPercentileLatencyAsDouble(0.95)));
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        log.info(HORIZONTAL_RULE);
        log.info("Setup & Initialization");
        log.info(HORIZONTAL_RULE);

        log.info(HORIZONTAL_RULE);
        log.info("Starting Benchmark");
        log.info(HORIZONTAL_RULE);

        SecureRandom rnd = new SecureRandom();
        rnd.setSeed(System.identityHashCode(Thread.currentThread()));
        long icnt = 0;
        try {
            // Run the benchmark loop for the requested warmup time
            // The throughput may be throttled depending on client configuration
            log.info("Warming up...");
            final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
            while (warmupEndTime > System.currentTimeMillis()) {
                long value = System.currentTimeMillis();
                long key = rnd.nextLong();
                exportProc.insertExport(key, value);
                icnt++;
            }

            // print periodic statistics to the console
            benchmarkStartTS = System.currentTimeMillis();
            schedulePeriodicStats();

            log.info("Starting data checker...");
            checkTimer = matchChecks.checkTimer(5000, client);

            // Run the benchmark loop for the requested duration
            // The throughput may be throttled depending on client configuration
            // Save the key/value pairs so they can be verified through the database
            log.info("Running benchmark...");
            final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
            while (benchmarkEndTime > System.currentTimeMillis()) {
                long value = System.currentTimeMillis();
                long key = rnd.nextLong();
                exportProc.insertExport(key, value);
                icnt++;
            }
            // check for export completion
            exportMon.waitForStreamedAllocatedMemoryZero();
            importMon.waitForStreamedAllocatedMemoryZero();
            exportProc.insertFinal(-1, -1);
            log.info("Done waiting for import & export tables");
        } finally {
            // cancel periodic stats printing
            log.info("Cancel periodic stats");
            statsTimer.cancel();
            finalInsertCount.addAndGet(icnt);
        }
    }

    public static class BenchmarkRunner extends Thread {
        private final KafkaImportBenchmark benchmark;

        public BenchmarkRunner(KafkaImportBenchmark bm) {
            benchmark = bm;
        }

        @Override
        public void run() {
            try {
                benchmark.runBenchmark();
            } catch (Exception ex) {
                ex.printStackTrace();
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
        // create a configuration from the arguments
        Config config = new Config();
        config.parse(KafkaImportBenchmark.class.getName(), args);

        // connect to one or more servers, loop until success
        dbconnect(config.servers);

        // instance handles inserts to Kafka export table and its mirror DB table
        exportProc = new InsertExport(client);

        // get instances to track track export completion using @Statistics
        exportMon = new TableChangeMonitor(client, "StreamedTable", "KAFKAEXPORTTABLE1");
        importMon = new TableChangeMonitor(client, "PersistentTable", "KAFKAIMPORTTABLE1");

        log.info("starting KafkaImportBenchmark...");
        KafkaImportBenchmark benchmark = new KafkaImportBenchmark(config);
        BenchmarkRunner runner = new BenchmarkRunner(benchmark);
        runner.start();

        // start watcher that compares mirror table which contains all
        // the export data with the import table that's rows back from Kafka.
        // Arg is interval to wait between checks
        // log.info("starting data checker...");
        // @SuppressWarnings("static-access")
        // Timer t = matchChecks.checkTimer(5000, client);

        runner.join(); // writers are done

        // final check time since the import and export tables have quiesced.
        // check that the mirror table is empty. If not, that indicates that
        // not all the rows got to Kafka or not all the rows got imported back.
        boolean testResult = FinalCheck.check(client);

        checkTimer.cancel();
        client.drain();
        client.close();

        if (testResult == true) {
            log.info("Test passed!");
            System.exit(0);
        } else {
            log.info("Test failed!");
            System.exit(1);
        }
    }
}
