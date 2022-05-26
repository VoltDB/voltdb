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
 *
 * 8/31: add option to populate topic externally, called "pounder" since
 * pounder (pounder.groovy) is a simple script commonly used to populate
 * a topic quickly.
 */

package client.kafkaimporter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NoConnectionsException;

import com.google_voltpatches.common.base.Splitter;
import com.google_voltpatches.common.net.HostAndPort;

public class KafkaImportBenchmark {

    static VoltLogger log = new VoltLogger("Benchmark");

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------";
    static final int SUCCESSES = 0;
    static final int FAILURES = 1;
    static final int OUTSTANDING_REQUESTS = 2;
    static final int RETRIES = 3;
    // Statistics manager objects from the client
    static ClientStatsContext periodicStatsContext;

    // validated command line configuration
    static Config config;
    // Timer for periodic stats printing
    static Timer statsTimer;
    static Timer checkTimer;
    // Benchmark start time
    long benchmarkStartTS;

    final ClientStatsContext fullStatsContext;

    static final Map<HostAndPort, OutputStream> haplist = new HashMap<HostAndPort, OutputStream>();
    static Client client;
    // Some thread safe counters for reporting
    AtomicLong linesRead = new AtomicLong(0);
    // count of rows successfully exported
    static AtomicLong rowsAdded = new AtomicLong(0);
    // count of rows queued to export
    static final AtomicLong finalInsertCount = new AtomicLong(0);

    private static final int WAIT = 20 * 1000; // wait at the end for import to settle after export completes
    private static String RUNNING_STATE = "Running";

    static InsertExport exportProc;
    static TableChangeMonitor exportMon;
    static TableChangeMonitor importMon;
    static MatchChecks matchChecks;

    static Map<Integer, AtomicLong> IMPORT_COUNTS = new HashMap<>();
    static volatile long  IMPORT_LAST_PROGRESS_REPORTED = System.currentTimeMillis();

    //get out ot waiting if no progress is made in 5 min
    static final long MAX_TIME_WITHOUT_PROGRESS = 5 * 60 * 1000; //3 min

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class Config extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 20;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 300;

        @Option(desc = "Maximum export TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Comma separated list of the form server[:port] to connect to for database queries")
        String servers = "localhost";

        @Option(desc = "Number of rows to expect to import from the Kafka topic")
        long expected_rows = 10_000_000;

        @Option(desc = "Report latency for kafka benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "Test using all VoltDB datatypes (except varbin).")
        boolean alltypes = false;

        @Option(desc = "Set to true to use voltdb export instead of groovy loader to populate kafka topic(s).")
        boolean useexport = false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Are we running the multi-stream/multi topic test?")
        boolean streamtest = false;

        @Option(desc = "Are we running the multi-stream from one topic test?")
        boolean topictest = false;

        @Option(desc = "Are we importing with kafkaloader, not in-server imports?")
        boolean loadertest = false;

        @Option(desc = "Number of streams and topics we're importing.")
        int streams = 1;

        @Option(desc = "Enable SSL with configuration file.")
        String sslfile = "";

        @Option(desc = "user id.")
        String username = "";

        @Option(desc = "password.")
        String password = "";

        @Option(desc = "Set to autoreconnect (setReconnectOnConnectionLoss) or topoaware (setTopologyChangeAware)")
        // String connectoption = "topoaware";
        String connectoption = "autoreconnect";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
            // 0, means we're not expecting any rows -- part of new offset checking test
            // if (expected_rows <= 0) exitWithMessageAndUsage("row number must be > 0");
            if (!useexport && alltypes) exitWithMessageAndUsage("groovy loader and alltypes are mutually exclusive");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (!connectoption.equalsIgnoreCase("topoaware") && !connectoption.equalsIgnoreCase("autoreconnect"))
                exitWithMessageAndUsage("connect option must be one of topoaware or autoreconnect");
            log.info("finished validating args");
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
        fullStatsContext = client.createStatsContext();

        log.info(HORIZONTAL_RULE);
        log.info(" Command Line Configuration");
        log.info(HORIZONTAL_RULE);
        log.info(config.getConfigDumpString());
        if(config.latencyreport) {
            log.warn("Option latencyreport is ON for async run, please set a reasonable ratelimit.\n");
        }

        if (!config.alltypes) {
            int start = config.loadertest ? 0 : 1;
            int end = config.loadertest ? (config.streams-1) : config.streams;
            for (int i = start; i <= end; i++) {
                IMPORT_COUNTS.put(i, new AtomicLong(0));
            }
        }
    }

    /**
     * Connect to one or more VoltDB servers.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional). Assumes 21212 if not specified otherwise.
     * @throws InterruptedException if anything bad happens with the threads.
     */
    static void dbconnect(Config config) throws InterruptedException, Exception {
        final Splitter COMMA_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

        log.info("Connecting to VoltDB Interface...");
        ClientConfig clientConfig = new ClientConfig(config.username, config.password);

        clientConfig.setMaxTransactionsPerSecond(config.ratelimit);
        if (config.sslfile.trim().length() > 0) {
            clientConfig.setTrustStoreConfigFromPropertyFile(config.sslfile);
            clientConfig.enableSSL();
        }


        // NB: set only one of setReconnectOnConnectionLoss or setTopologyChangeAware.
        // They're mutually exclusive.
        if (config.connectoption.equalsIgnoreCase("autoreconnect")) {
            clientConfig.setReconnectOnConnectionLoss(true);
        }
        else {
            clientConfig.setTopologyChangeAware(true);
        }

        client = ClientFactory.createClient(clientConfig);

        for (String server: COMMA_SPLITTER.split(config.servers)) {
            log.info("..." + server);
            client.createConnection(server);
        }
    }

    /**
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
     */
    public static void schedulePeriodicStats() {
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
    public synchronized static void printStatistics() {
        try {
            ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
            long thrup;

            thrup = stats.getTxnThroughput();
            long rows = MatchChecks.getExportRowCount(config.alltypes,client);
            if (rows == VoltType.NULL_BIGINT)
                rows = 0;
            log.info("Importer stats: " + MatchChecks.getImportStats(client));
            log.info(String.format("Export Throughput %d/s, Total Rows %d, Aborts/Failures %d/%d, Avg/95%% Latency %.2f/%.2fms",
                    thrup, rows, stats.getInvocationAborts(), stats.getInvocationErrors(),
                    stats.getAverageLatency(), stats.kPercentileLatencyAsDouble(0.95)));
        } catch (Exception ex) {
            log.error("Exception in printStatistics", ex);
        }
    }

    protected static void scheduleCheckTimer() {

        final Timer timer = new Timer("checkTimer", true);
        final long period = config.displayinterval;

        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                long currentTotalCount = 0;
                long lastTotalCount = 0;
                if (!config.alltypes) {
                    int start = config.loadertest ? 0 : 1;
                    int end = config.loadertest ? (config.streams-1) : config.streams;
                    for (int i = start; i <= end; i++) {
                        long num = MatchChecks.getImportTableRowCount(i, client); // imported count
                        AtomicLong streamCount = IMPORT_COUNTS.get(i);
                        lastTotalCount += streamCount.longValue();
                        if (num != streamCount.longValue()) {
                            IMPORT_LAST_PROGRESS_REPORTED = System.currentTimeMillis();
                        }
                        streamCount.set(num);
                        currentTotalCount += num;
                    }
                }
                log.info("Imported a total of " + currentTotalCount + " rows from " + config.streams + ":" + IMPORT_COUNTS.values());
                if (config.alltypes) {
                    // for alltypes, if a column in mirror doesn't match import, key will be a row key, and non-zero
                    long key = MatchChecks.checkRowMismatch(client);
                    if (key != 0) {
                        log.error("Import value mismatch at row " + key + ". Exiting.");
                        System.exit(-1);
                    }
                }
                if (!config.alltypes) {
                    log.info("Import Throughput " + (currentTotalCount - lastTotalCount ) / period + "/s, Total Rows: " + currentTotalCount);
                }
                if (!config.loadertest) {
                    log.info("Import stats: " + MatchChecks.getImportStats(client));
                }
            }
        }, period * 1000, period * 1000);
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        log.info(HORIZONTAL_RULE);
        log.info(" Setup & Initialization");
        log.info(HORIZONTAL_RULE);

        log.info(HORIZONTAL_RULE);
        log.info("Starting Benchmark");
        log.info(HORIZONTAL_RULE);

        long icnt = 0;
        try {
            // Run the benchmark loop for the requested duration
            // The throughput may be throttled depending on client configuration
            // Save the key/value pairs so they can be verified through the database
            log.info("Running benchmark...");
            final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
            while (benchmarkEndTime > System.currentTimeMillis()) {
                exportProc.insertExport(icnt, icnt);
                icnt++;
                finalInsertCount.addAndGet(1);
            }
        } catch (Exception ex) {
            log.error("Exception in Benchmark", ex);
        } finally {
            log.info("Benchmark ended, submitted " + icnt + " rows.");
            // cancel periodic stats printing
            statsTimer.cancel();
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
                if (config.useexport) {
                    schedulePeriodicStats();
                    benchmark.runBenchmark();
                }
                scheduleCheckTimer();
            } catch (Exception ex) {
                log.error("Exception in benchmark", ex);
                System.exit(-1);
            }
        }
    }

    public static boolean verifyZero() {
        long count = -1;
        log.info("Checking for zero rows in KAFKAIMPORTTABLE1");
        log.info("Wait for 1 minute for import to settle");
        try {
            Thread.sleep(1 * 1000 * 60);  // wait for 1 minute
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            count = MatchChecks.getImportTableRowCount(1, client); // imported count for KAFKAIMPORTTABLE1
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (count == 0)
            return true;
        else
            return false;
    }

    public static void endTest(boolean testResult, Config config) {
        // Write stats to file if requested
        try {
            if ((config.statsfile != null) && (config.statsfile.length() != 0)) {
                log.info("Stats file: " + config.statsfile);
                FileWriter fw = new FileWriter(config.statsfile);

                // stats: row count, latest time, earliest time
                long[] stats  = MatchChecks.getStats(client);
                log.info("rows: " + stats[0] + ". End timestamp: " + stats[1] + ". Start timestamp: " + stats[2]);
                // Date date = new Date(stats[2]);
                //    LocalDateTime.ofInstant(Instant.ofEpochMilli(stats[2]*1000), ZoneId.systemDefault());
                double tps = stats[0] / ((double)stats[1] - (double)stats[2]);
                log.info("TPS: " + tps);
                log.info("Stats string: " + String.format("%d,%d,%d,%d,%d,%d,%d,0,0,0,0,0,0\n",
                    stats[2], config.duration, 0, 0, 0, 0, (long)tps));
                fw.append(String.format("%d,%d,%d,%d,%d,%d,%d,0,0,0,0,0,0\n",
                            stats[2],
                            (stats[1]-stats[2])*1000,
                            stats[0],
                            0,
                            0,
                            0,
                            0
                            ));
                fw.close();
            }
        } catch (IOException e) {
            System.err.println("Error writing stats file");
            e.printStackTrace();
        }

        try {
            client.drain();
            client.close();
        } catch (NoConnectionsException | InterruptedException e) {
            e.printStackTrace();
        }

        if (testResult == true) {
            log.info("Test passed!");
            System.exit(0);
        } else {
            log.info("Test failed!");
            System.exit(1);
        }
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */
    public static void main(String[] args) throws Exception {

        VoltLogger log = new VoltLogger("Benchmark.main");
        boolean testResult = true;
        // create a configuration from the arguments
        Config config = new Config();
        config.parse(KafkaImportBenchmark.class.getName(), args);

        // connect to one or more servers, method loops until success
        dbconnect(config);

        // special case for second half of offset check test.
        // we expect no rows, and give the import subsystem about a
        // minute to settle
        if (config.expected_rows == 0) {
            testResult = verifyZero();
            endTest(testResult, config);
        }

        // instance handles inserts to Kafka export table and its mirror DB table
        exportProc = new InsertExport(config.alltypes, client, rowsAdded);

        log.info("Starting KafkaImportBenchmark...");
        KafkaImportBenchmark benchmark = new KafkaImportBenchmark(config);
        BenchmarkRunner runner = new BenchmarkRunner(benchmark);
        runner.start();
        runner.join(); // writers are done

        //The number of tuples which should be exported.
        long exportRowCount = 0;
        if (config.useexport) {
            exportRowCount = MatchChecks.getExportRowCount(config.alltypes, client);
            long startTime = System.currentTimeMillis();
            // make sure the export table has drained, wait an extra config.duration and timeout if it doesn't finish by then
            if ( ! MatchChecks.waitForExportToDrain(client) ) {
                log.error("Timeout waiting for export to drain");
                throw new Exception("Timeout waiting for export to drain");
            }
            log.info("Export phase complete, " + exportRowCount + " rows exported, waiting for import to drain...");
        } else {
            log.info("depending on external tool to populate kafka");
        }

        // final check time since the import and export tables have quiesced.
        // check that the mirror table is empty. If not, that indicates that
        // not all the rows got to Kafka or not all the rows got imported back.
        long idle = System.currentTimeMillis() - IMPORT_LAST_PROGRESS_REPORTED;
        // give 10 min. if import is not done in 10 min, the import could be extremely slow
        // then fail the test
        final long totalWait = 600 * 1000;
        long totalStart = System.currentTimeMillis();
        while (idle < MAX_TIME_WITHOUT_PROGRESS) {
            Thread.sleep(WAIT);
            idle = System.currentTimeMillis() - IMPORT_LAST_PROGRESS_REPORTED;
            if ((System.currentTimeMillis() - totalStart) > totalWait) {
                log.warn("Reach a mixmum of 10 min but importer still reports progress!");
                break;
            }
            boolean done = false;
            if (!config.useexport) {
                done = true;
                for (AtomicLong count : IMPORT_COUNTS.values()) {
                    if (count.get() != config.expected_rows) {
                        done = false;
                        break;
                    }
                }
            }
            if (done) {
                break;
            }
        }

        long[] importStatValues = MatchChecks.getImportValues(client);
        long mirrorStreamCounts = 0;
        if (config.useexport) mirrorStreamCounts = MatchChecks.getMirrorTableRowCount(config.alltypes, config.streams, client);
        long importRows = MatchChecks.getImportTableRowCount(config.alltypes?5:1, client);
        long importRowCount = 0;
        if (!config.streamtest) importRowCount = MatchChecks.getImportRowCount(client);

        log.info("Continue checking import progress. Imported tuples for all " + config.streams + ":" + IMPORT_COUNTS.values());

        // in case of pause / resume tweak, let it drain longer
        long expectedRows = config.expected_rows;
        if (config.useexport) {
            expectedRows = exportRowCount;
        }
        //wait for another two min after the import is down for anyone of the streams
        // or till the import is down on all stream
        long startTiming = System.currentTimeMillis();
        boolean importInProgress = true;
        boolean oneStreamCompleted = false;
        long waitingTime = MAX_TIME_WITHOUT_PROGRESS;
        while (importInProgress) {
            int streamWithMissingCount = 0;
            for (AtomicLong count : IMPORT_COUNTS.values()) {
                if (count.get() == expectedRows) {
                    if (!oneStreamCompleted) {
                        waitingTime = 120 * 1000;
                        startTiming = System.currentTimeMillis();
                    }
                    oneStreamCompleted = true;
                } else {
                    streamWithMissingCount++;
                    break;
                }
            }
            importInProgress = (streamWithMissingCount > 0);
            if (!importInProgress || (System.currentTimeMillis() - startTiming) > waitingTime) {
                break;
            }
            Thread.sleep(WAIT);
            log.info("Imported tuples for all " + config.streams + ":" + IMPORT_COUNTS.values());
        }

        if (config.useexport) {
            mirrorStreamCounts = MatchChecks.getMirrorTableRowCount(config.alltypes, config.streams, client);
        }

        log.info("Finish checking import progress. Imported tuples for all " + config.streams + ":" + IMPORT_COUNTS.values());

        // some counts that might help debugging
        if (!config.loadertest) { // if kafkaloader, no import stats!
            log.info("importer outstanding requests: " + importStatValues[OUTSTANDING_REQUESTS]);
        }
        log.info("importRows: " + importRows);
        if (!(config.streamtest || config.loadertest)) {
            log.info("mirrorStreamCounts: " + mirrorStreamCounts);
            log.info("importRowCount: " + importRowCount);
        }

        if (config.useexport) {
            log.info("The number of rows to export stream: " + finalInsertCount);
            log.info("The number of rows exported: " + exportRowCount);
            log.info("Unmatched Rows remaining in the export Mirror Table: " + mirrorStreamCounts);
            log.info("Unmatched Rows received from Kafka to Import Table (duplicate rows): " + importRows);
            if (mirrorStreamCounts != 0) {
                if (config.alltypes) {
                    log.error(mirrorStreamCounts + " Rows are missing from the import stream, failing test");
                } else {
                    log.error(mirrorStreamCounts + " Rows not imported by all streams, failing test");
                }
                testResult = false;
            }
        }

        if (!config.useexport && !config.streamtest) {
            int start = config.loadertest ? 0 : 1;
            int end = config.loadertest ? (config.streams-1) : config.streams;
            for (int i = start; i <= end; i++) {
                testResult = MatchChecks.checkPounderResults(config.expected_rows, client, i);
                if (!testResult) {
                    break;
                }
            }
        }
        log.info("Import results for streams " + config.streams + ":" + IMPORT_COUNTS.values());
        endTest(testResult, config);
    }
}
