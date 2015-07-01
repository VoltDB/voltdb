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

package adhocbenchmark;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.AsyncCompilerAgent;

/**
 * Class providing the entire benchmark implementation and command line main().
 */
public class Benchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final BenchmarkConfig cliConfig;
    // Reference to the database connection we will use
    final Client client;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;

    final QueryTracer tracer;

    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    private final class CallProcedureCallback implements ProcedureCallback {
        final String query;
        final long queryIndex;

        public CallProcedureCallback(long queryIndex, final String query) {
            this.queryIndex = queryIndex;
            this.query = query;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                tracer.logQueryFailedAndExit(queryIndex, query, clientResponse);
            }
        }
    }

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class BenchmarkConfig extends CLIConfig {

        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Input configuration file path.")
        String configfile = "config.xml";

        @Option(desc = "Output file path for raw summary statistics.")
        String statsfile = "";

        @Option(desc = "Output file path for query tracing (disabled by default).")
        String querytracefile = "";

        @Option(desc = "Benchmark duration in seconds (0=infinite).")
        int duration = 60;

        @Option(desc = "Warm-up duration in seconds (0=disable warm-up).")
        int warmup = 5;

        @Option(desc = "Maximum # of outstanding transactions (0=not throttled).")
        int querythrottle = AsyncCompilerAgent.MAX_QUEUE_DEPTH;

        @Option(desc = "Test to run.")
        String test = BenchmarkConfiguration.getDefaultTestName();

        @Override
        public void validate() {
            if (displayinterval <= 0)
                exitWithMessageAndUsage("displayinterval must be > 0");
            if (duration < 0)
                exitWithMessageAndUsage("duration must be 0 or a positive integer");
            if (warmup < 0)
                exitWithMessageAndUsage("warmup must be 0 or a positive integer");
            if (querythrottle < 0)
                exitWithMessageAndUsage("querythrottle must 0 or a positive integer");
            // We don't know the test names here. Ask for them and use them to validate.
            Set<String> tests = BenchmarkConfiguration.getTestNames();
            test = test.toLowerCase();
            if (!tests.contains(test)) {
                StringBuilder validTests = new StringBuilder()
                    .append("test '").append(test).append("' is unknown, use one of: {");
                for (String test : tests) {
                    validTests.append(" ");
                    validTests.append(test);
                }
                validTests.append(" }");
                exitWithMessageAndUsage(validTests.toString());
            }
        }
    }

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            System.out.printf("Connection to %s:%d was closed.\n", hostname, port);
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param cliConfig Parsed & validated CLI options.
     */
    public Benchmark(BenchmarkConfig cliConfig) {

        this.cliConfig = cliConfig;

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
        // Throttle so that ad hoc queries don't get rejected with "planner not available".
        if (cliConfig.querythrottle > 0) {
            System.out.printf("Throttling maximum outstanding transactions to %d\n",
                              cliConfig.querythrottle);
            clientConfig.setMaxOutstandingTxns(cliConfig.querythrottle);
        }
        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        tracer = new QueryTracer(cliConfig.querytracefile);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(cliConfig.getConfigDumpString());


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
     * Create a Timer task to display performance data every displayInterval seconds
     */
    public void schedulePeriodicStats() {
        timer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() { printStatistics(); }
        };
        timer.scheduleAtFixedRate(statsPrinting,
                                  cliConfig.displayinterval * 1000,
                                  cliConfig.displayinterval * 1000);
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Txns Completed %d ", stats.getInvocationsCompleted());
        System.out.printf("Avg/95%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.95));
        System.out.println("");
    }

    /**
     * Prints the results and statistics about performance.
     * @throws IOException
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults(long totalElapsedMS, long queryElapsedMS) throws IOException {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // 1. Results and performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Results");
        System.out.println(HORIZONTAL_RULE);
        System.out.printf("Total queries completed:       %9d\n", stats.getInvocationsCompleted());

        // 2. Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
        System.out.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
        System.out.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" Planner Statistics");
        System.out.println(HORIZONTAL_RULE);

        printPlannerStatistics();

        // 3. Write stats to file if requested
        client.writeSummaryCSV(stats, cliConfig.statsfile);
    }

    /**
     * Print planner and cache statistics.
     * @throws IOException
     * @throws NoConnectionsException
     */
    public void printPlannerStatistics() throws IOException, NoConnectionsException {
        try {
            VoltTable result = client.callProcedure("@Statistics", "PLANNER", 0).getResults()[0];
            while(result.advanceRow()) {
                String hostname  = result.getString("HOSTNAME");
                long siteId      = result.getLong("SITE_ID");
                long partitionId = result.getLong("PARTITION_ID");
                long hits1       = result.getLong("CACHE1_HITS");
                long hits2       = result.getLong("CACHE2_HITS");
                long level1      = result.getLong("CACHE1_LEVEL");
                long level2      = result.getLong("CACHE2_LEVEL");
                long misses      = result.getLong("CACHE_MISSES");
                long total       = hits1 + hits2 + misses;
                double hitpc1    = (100.0 * hits1) / total;
                double hitpc2    = (100.0 * hits2) / total;
                double planTimeMin = result.getLong("PLAN_TIME_MIN")/ 1000000.0;
                double planTimeMax = result.getLong("PLAN_TIME_MAX")/ 1000000.0;
                double planTimeAvg = result.getLong("PLAN_TIME_AVG")/ 1000000.0;
                long failures    = result.getLong("FAILURES");
                // Global stats
                System.out.printf(    "          HOSTNAME: %s\n", hostname);
                if (siteId == -1) {
                    System.out.printf("              SITE: (global)\n");
                }
                else {
                    System.out.printf("              SITE: %d\n", siteId);
                    System.out.printf("         PARTITION: %d\n", partitionId);
                }
                System.out.printf(    "       TOTAL PLANS: %d\n", total);
                System.out.printf(    "      CACHE MISSES: %d\n", misses);
                if (siteId == -1) {
                    System.out.printf("LEVEL 1 CACHE HITS: %d (%.1f%%)\n", hits1, hitpc1);
                    System.out.printf("LEVEL 2 CACHE HITS: %d (%.1f%%)\n", hits2, hitpc2);
                    System.out.printf("LEVEL 1 CACHE SIZE: %d\n", level1);
                    System.out.printf("LEVEL 2 CACHE SIZE: %d\n", level2);
                }
                else {
                    System.out.printf("   PLAN CACHE HITS: %d (%.1f%%)\n", hits1, hitpc1);
                    System.out.printf("   PLAN CACHE SIZE: %d\n", level1);
                }
                System.out.printf(    "     PLAN TIME MIN: %6.2f ms\n", planTimeMin);
                System.out.printf(    "     PLAN TIME MAX: %6.2f ms\n", planTimeMax);
                System.out.printf(    "     PLAN TIME AVG: %6.2f ms\n", planTimeAvg);
                System.out.printf(    "          FAILURES: %d\n\n", failures);
            }
        }
        catch (ProcCallException e) {
            e.printStackTrace();
        }
    }

    /**
     * Implements an iterator that returns generated query strings.
     */
    private static class QueryIterator implements Iterator<String>, QueryTestHelper {

        private final List<QueryTestBase> tests;
        private Iterator<QueryTestBase> testIterator;
        private QueryTestBase currentTest = null;
        private int iGeneratedQueries = 0;
        private final long startTimeMS;
        private final int duration;
        private List<Integer> shuffledNumbers;
        private final Random rnd = new Random(99);

        public QueryIterator(final List<QueryTestBase> tests, final int duration) {
            this.tests = tests;
            this.duration = duration;
            startTimeMS = System.currentTimeMillis();
            testIterator = tests.listIterator();
            // Prepare to run the first test
            nextTest();
        }

        /**
         * Prepare for the next test.
         */
        public void nextTest() {
            currentTest = testIterator.next();
            if (currentTest != null) {
                // Prepare the random numbers.
                shuffledNumbers = new ArrayList<Integer>(currentTest.nRandomNumbers);
                for (int i = 0; i < currentTest.nRandomNumbers; i++) {
                    shuffledNumbers.add(i);
                }
            }
        }

        /**
         *  Advance to the next test.
         */
        private void advance() {
            if (currentTest == null) {
                throw new NoSuchElementException();
            }
            if (duration > 0 && System.currentTimeMillis() > startTimeMS + (duration * 1000)) {
                // Time expired.
                currentTest = null;
            } else {
                iGeneratedQueries++;
                if (testIterator.hasNext()) {
                    // Keep going as long as tests remain.
                    nextTest();
                } else {
                    // Wrap around until time expires.
                    testIterator = tests.listIterator();
                    currentTest = testIterator.next();
                }
            }
        }

        // Generate table name from prefix and zero-based table index.
        @Override
        public String tableName(int iTable) {
            return String.format("%s_%d", currentTest.tablePrefix, iTable + 1);
        }

        // Generate column name from prefix and zero-based table index.
        @Override
        public String columnName(int iColumn) {
            return String.format("%s_%d", currentTest.columnPrefix,
                                          (iColumn % currentTest.nColumns) + 1);
        }

        // Generate table.column name from prefix and zero-based table index.
        @Override
        public String tableColumnName(int iTable, int iColumn) {
            return String.format("%s.%s", tableName(iTable), columnName(iColumn));
        }

        // Get an indexed shuffled number.
        @Override
        public int getShuffledNumber(final int i) {
            return shuffledNumbers.get(i % shuffledNumbers.size());
        }

        // Check if a next query is available. Pre-determined by previous next()/advance() call.
        @Override
        public boolean hasNext() {
            // As soon as tests are exhausted currentTest is set to null in advance().
            return (currentTest != null);
        }

        // Get the next query string.
        @Override
        public String next() {
            // Exhausted all tests?
            if (currentTest == null) {
                throw new NoSuchElementException();
            }
            // Shuffle the numbers used by the query generator.
            java.util.Collections.shuffle(shuffledNumbers, rnd);
            // Ask the derived class to generate the query. Subtract 1 for zero-based query counter.
            String query = currentTest.getQuery(iGeneratedQueries, this);
            // Get ready for the next one (sets currentTest to null when testing is complete).
            advance();
            return query;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     *  Iterable query generator.
     */
    private static class QueryGenerator implements Iterable<String>
    {
        protected final List<QueryTestBase> tests;
        protected final int duration;

        public QueryGenerator(final List<QueryTestBase> tests, final int duration) {
            this.tests = tests;
            this.duration = duration;
        }

        @Override
        public Iterator<String> iterator() {
            return new QueryIterator(tests, duration);
        }
    }

    private static class QueryTracer {
        private OutputStreamWriter writer = null;

        QueryTracer(String path) {
            if (path != null && !path.isEmpty()) {
                try {
                    writer = new FileWriter(path);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }

        private String getQueryIndexStr(long queryIndex) {
            if (queryIndex < 0) {
                return String.format("WARM%9d", queryIndex * -1);
            }
            else {
                return String.format("%9d", queryIndex);
            }
        }

        synchronized void logQueryInvoked(long queryIndex, String query) throws IOException {
            // skip warmups
            if (queryIndex < 0) {
                return;
            }

            String msg = String.format("[QUERY %9s]: INVOKED WITH SQL: %s\n", getQueryIndexStr(queryIndex), query);

            // write the log
            if (writer != null) {
                writer.write(msg);
                // don't flush for invocations // writer.flush();
            }
        }

        synchronized void logQueryFailedAndExit(long queryIndex, String query, ClientResponse response) throws IOException {
            String queryIndexStr = getQueryIndexStr(queryIndex);
            String err =  String.format("[QUERY %s]: **FAILED** WITH STATUS %d\n", queryIndexStr, response.getStatus());
                   err += String.format("[QUERY %s]:   SQL: %s\n", queryIndexStr, query);
                   err += String.format("[QUERY %s]:   STATUS STRING: %s\n\n", queryIndexStr, response.getStatusString());

            // write the err to standard err
            System.err.print(err);
            System.err.flush();

            // write the err to the log if it exists
            if (writer != null) {
                writer.write(err);

                // flush and close in anticipation of impending death
                writer.flush();
                writer.close();
            }

            System.exit(-1);
        }

        synchronized void close() {
            if (writer != null) {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @param configPath  path to configuration file
     *
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws IOException
     * @throws NoConnectionsException
     */
    public void runBenchmark(String configPath)
            throws ConfigurationException, InterruptedException, NoConnectionsException, IOException
    {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        // Parse the XML test configuration.
        System.out.printf("Reading configuration file '%s'...\n", configPath);
        List<QueryTestBase> tests = BenchmarkConfiguration.configureTests(configPath, cliConfig.test);

        // connect to one or more servers, loop until success
        connect(cliConfig.servers);

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        long queryIndex = 1;
        if (cliConfig.warmup > 0) {
            System.out.println("Warming up...");
            for (String query : new QueryGenerator(tests, cliConfig.warmup)) {
                // negative query index means warmup to query tracer
                client.callProcedure(new CallProcedureCallback(queryIndex * -1, query), "@AdHoc", query);
                queryIndex++;
            }
        }

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        long queryElapsedMS = 0;
        schedulePeriodicStats();

        // Run the benchmark for the requested duration.
        System.out.printf("\nRunning '%s' benchmark...\n", cliConfig.test);
        queryIndex = 1;
        for (String query : new QueryGenerator(tests, cliConfig.duration)) {
            client.callProcedure(new CallProcedureCallback(queryIndex, query), "@AdHoc", query);
            tracer.logQueryInvoked(queryIndex, query);
            queryIndex++;
        }

        printStatistics();
        System.out.println();

        long totalElapsedMS = System.currentTimeMillis() - benchmarkStartTS;

        // cancel periodic stats printing
        timer.cancel();

        // block until all outstanding txns return
        client.drain();

        // close the logfile if it exists
        tracer.close();

        // print the summary results
        printResults(totalElapsedMS, queryElapsedMS);

        // close down the client connections
        client.close();
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        BenchmarkConfig cliConfig = new BenchmarkConfig();
        cliConfig.parse(Benchmark.class.getName(), args);

        Benchmark benchmark = new Benchmark(cliConfig);

        benchmark.runBenchmark(cliConfig.configfile);
    }
}
