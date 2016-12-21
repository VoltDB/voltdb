/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

    static final Map<HostAndPort, OutputStream> haplist = new HashMap<HostAndPort, OutputStream>();
    static Client client;
    // Some thread safe counters for reporting
    AtomicLong linesRead = new AtomicLong(0);
    // count of rows successfully exported
    static AtomicLong rowsAdded = new AtomicLong(0);
    // count of rows queued to export
    static final AtomicLong finalInsertCount = new AtomicLong(0);

    private static final int END_WAIT = 10; // wait at the end for import to settle after export completes

    private static final int PAUSE_WAIT = 10; // wait for server resume from pause mode
    private static String RUNNING_STATE = "Running";

    static List<Integer> importProgress = new ArrayList<Integer>();

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

        @Option(desc = "Are we running the multi-stream/nmulti topic test?")
        boolean streamtest = false;

        @Option(desc = "Number of streams and topics we're importing.")
        int streams = 1;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
            // 0, means we're not expecting any rows -- part of new offset checking test
            // if (expected_rows <= 0) exitWithMessageAndUsage("row number must be > 0");
            if (!useexport && alltypes) exitWithMessageAndUsage("groovy loader and alltypes are mutually exclusive");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
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
    static void dbconnect(String servers, int ratelimit) throws InterruptedException, Exception {
        final Splitter COMMA_SPLITTER = Splitter.on(",").omitEmptyStrings().trimResults();

        log.info("Connecting to VoltDB Interface...");
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setMaxTransactionsPerSecond(ratelimit);
        clientConfig.setReconnectOnConnectionLoss(true);
        client = ClientFactory.createClient(clientConfig);

        for (String server: COMMA_SPLITTER.split(servers)) {
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
            long rows = MatchChecks.getExportRowCount(client);
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
                long count = 0;

                if (!config.useexport) {
                    count = MatchChecks.getImportTableRowCount(config.alltypes, client); // imported count
                } else {
                    count = MatchChecks.getImportRowCount(client); // deleted count
                }
                importProgress.add((int) count);

                if (config.alltypes) {
                    // for alltypes, if a column in mirror doesn't match import, key will be a row key, and non-zero
                    long key = MatchChecks.checkRowMismatch(client);
                    if (key != 0) {
                        log.error("Import value mismatch at row " + key + ". Exiting.");
                        System.exit(-1);
                    }
                }
                int sz = importProgress.size();
                if (sz > 1) {
                    log.info("Import Throughput " + (count - importProgress.get(sz - 2)) / period + "/s, Total Rows: " + count);
                }
                log.info("Import stats: " + MatchChecks.getImportStats(client));
            }
        },
        config.displayinterval * 1000,
        config.displayinterval * 1000);
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
            // print periodic statistics to the console
            schedulePeriodicStats();
            scheduleCheckTimer();

            // Run the benchmark loop for the requested duration
            // The throughput may be throttled depending on client configuration
            // Save the key/value pairs so they can be verified through the database
            log.info("Running benchmark...");
            final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
            while (benchmarkEndTime > System.currentTimeMillis()) {
                long value = System.currentTimeMillis();
                long key = icnt;
                exportProc.insertExport(key, value);
                icnt++;
            }
        } catch (Exception ex) {
            log.error("Exception in Benchmark", ex);
        } finally {
            log.info("Benchmark ended, exported " + icnt + " rows.");
            // cancel periodic stats printing
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            count = MatchChecks.getImportTableRowCount(false, client); // imported count
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (count == 0)
            return true;
        else
            return false;
    }

    public static void endTest(boolean testResult) {
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
        dbconnect(config.servers, config.ratelimit);

        // special case for second half of offset check test.
        // we expect no rows, and give the import subsystem about a
        // minute to settle
        if (config.expected_rows == 0) {
            testResult = verifyZero();
            endTest(testResult);
        }

        // instance handles inserts to Kafka export table and its mirror DB table
        exportProc = new InsertExport(config.alltypes, client, rowsAdded);

        log.info("Starting KafkaImportBenchmark...");
        KafkaImportBenchmark benchmark = new KafkaImportBenchmark(config);
        BenchmarkRunner runner = new BenchmarkRunner(benchmark);
        runner.start();
        runner.join(); // writers are done

        long exportRowCount = 0;
        if (config.useexport) {
            exportRowCount = MatchChecks.getExportRowCount(client);
            log.info("Export phase complete, " + exportRowCount + " rows exported, waiting for import to drain...");
        }
        // final check time since the import and export tables have quiesced.
        // check that the mirror table is empty. If not, that indicates that
        // not all the rows got to Kafka or not all the rows got imported back.
        do {
            Thread.sleep(END_WAIT * 1000);

            //}
            // importProgress is an array of sampled counts of the importedcounts table, showing import progress
            // samples are recorded by the checkTimer thread
        } while (!RUNNING_STATE.equalsIgnoreCase(MatchChecks.getClusterState(client)) ||
                importProgress.size() < 4 || importProgress.get(importProgress.size()-1) > importProgress.get(importProgress.size()-2) ||
                importProgress.get(importProgress.size()-1) > importProgress.get(importProgress.size()-3) ||
                importProgress.get(importProgress.size()-1) > importProgress.get(importProgress.size()-4) );

        long[] importStatValues = MatchChecks.getImportValues(client);
        long mirrorRows = 0;
        if (!config.streamtest) mirrorRows = MatchChecks.getMirrorTableRowCount(config.alltypes, client);
        long importRows = MatchChecks.getImportTableRowCount(config.alltypes, client);
        long importRowCount = 0;
        if (!config.streamtest) importRowCount = MatchChecks.getImportRowCount(client);

        // in case of pause / resume tweak, let it drain longer
        int trial = 3;
        while (!RUNNING_STATE.equalsIgnoreCase(MatchChecks.getClusterState(client)) ||
                ((--trial > 0) && ((importStatValues[OUTSTANDING_REQUESTS] > 0) || (importRows < config.expected_rows)))) {
            Thread.sleep(PAUSE_WAIT * 1000);
            importStatValues = MatchChecks.getImportValues(client);
            if (!config.streamtest) mirrorRows = MatchChecks.getMirrorTableRowCount(config.alltypes, client);
            importRows = MatchChecks.getImportTableRowCount(config.alltypes, client);
            // importRowCount = MatchChecks.getImportRowCount(client);
        }

        // some counts that might help debugging....
        log.info("importer outstanding requests: " + importStatValues[OUTSTANDING_REQUESTS]);
        log.info("importRows: " + importRows);
        if (!config.streamtest) {
            log.info("mirrorRows: " + mirrorRows);
            log.info("importRowCount: " + importRowCount);
        }
        if (config.useexport) {
            log.info("exportRowCount: " + exportRowCount);
        }

        if (config.useexport) {
            log.info("Total rows exported: " + finalInsertCount);
            log.info("Unmatched Rows remaining in the export Mirror Table: " + mirrorRows);
            log.info("Unmatched Rows received from Kafka to Import Table (duplicate rows): " + importRows);

            if (mirrorRows != 0) {
                log.error(mirrorRows + " Rows are missing from the import stream, failing test");
                testResult = false;
            }
        }

        if ((exportRowCount != (importStatValues[SUCCESSES] + importStatValues[FAILURES])) && config.useexport) {
            log.error("Export count '" + exportRowCount +
                "' does not match import stats count '" +
                (importStatValues[SUCCESSES] + importStatValues[FAILURES]) +
                "' test fails.");
            testResult = false;
        }

        if (!config.useexport && !config.streamtest) {
            testResult = MatchChecks.checkPounderResults(config.expected_rows, client);
        }

        endTest(testResult);
    }
}
