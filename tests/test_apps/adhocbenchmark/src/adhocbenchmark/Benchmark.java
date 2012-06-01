/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;

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
    // Number of queries generated;
    long queriesProcessed = 0;
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    private static final class CallProcedureCallback implements ProcedureCallback {
        int errorStatus = ClientResponse.SUCCESS;
        String errorString = null;
        public CallProcedureCallback() {
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                this.errorStatus = clientResponse.getStatus();
                this.errorString = clientResponse.getStatusString();
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

        @Option(desc = "Benchmark duration in seconds (0=infinite).")
        int duration = 60;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 5;

        @Option(desc = "Test to run.")
        String test = BenchmarkConfiguration.getDefaultTestName();

        @Override
        public void validate() {
            if (this.displayinterval <= 0)
                exitWithMessageAndUsage("displayinterval must be > 0");
            if (this.duration < 0)
                exitWithMessageAndUsage("duration must be 0 or a positive integer");
            if (this.warmup <= 0)
                exitWithMessageAndUsage("warmup must be a positive integer");
            // We don't know the test names here. Ask for them and use them to validate.
            List<String> tests = Arrays.asList(BenchmarkConfiguration.getTestNames());
            this.test = this.test.toLowerCase();
            if (!tests.contains(this.test)) {
                StringBuilder validTests = new StringBuilder()
                    .append("test '").append(this.test).append("' is unknown");
                boolean first = true;
                for (String test : tests) {
                    if (first) {
                        validTests.append(", use one of: ");
                        first = false;
                    } else {
                        validTests.append(" ");
                    }
                    validTests.append(test);
                }
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
        clientConfig.setMaxOutstandingTxns(200);
        this.client = ClientFactory.createClient(clientConfig);

        this.periodicStatsContext = this.client.createStatsContext();
        this.fullStatsContext = this.client.createStatsContext();

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
                this.client.createConnection(server);
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
        this.timer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() { printStatistics(); }
        };
        this.timer.scheduleAtFixedRate(statsPrinting,
                                       this.cliConfig.displayinterval * 1000,
                                       this.cliConfig.displayinterval * 1000);
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized void printStatistics() {
        ClientStats stats = this.periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - this.benchmarkStartTS) / 1000.0);

        System.out.printf("Count %d ", this.queriesProcessed);
        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/95%% Latency %d/%dms\n", stats.getAverageLatency(),
                stats.kPercentileLatency(0.95));
    }

    /**
     * Prints the results and statistics about performance.
     * @throws IOException
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults(long totalElapsedMS, long queryElapsedMS) throws IOException {
        ClientStats stats = this.fullStatsContext.fetch().getStats();

        // 1. Results and performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Results");
        System.out.println(HORIZONTAL_RULE);
        System.out.printf("Total queries generated:       %9d\n", this.queriesProcessed);
        System.out.printf("Query elapsed time:            %9.2f seconds\n", queryElapsedMS / 1000.0);
        System.out.printf("Test overhead elapsed time:    %9.2f seconds\n", (totalElapsedMS - queryElapsedMS)/ 1000.0);
        System.out.printf("Total elapsed time:            %9.2f seconds\n\n", totalElapsedMS / 1000.0);

        // 2. Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        System.out.printf("Average latency:               %,9d ms\n", stats.getAverageLatency());
        System.out.printf("95th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.95));
        System.out.printf("99th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.99));

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Reported Internal Avg Latency: %,9d ms\n", stats.getAverageInternalLatency());

        // 3. Write stats to file if requested
        this.client.writeSummaryCSV(stats, this.cliConfig.statsfile);
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
            this.startTimeMS = System.currentTimeMillis();
            this.testIterator = this.tests.listIterator();
            // Prepare to run the first test
            this.nextTest();
        }

        /**
         * Prepare for the next test.
         */
        public void nextTest() {
            this.currentTest = this.testIterator.next();
            if (this.currentTest != null) {
                // Prepare the random numbers.
                this.shuffledNumbers = new ArrayList<Integer>(this.currentTest.nRandomNumbers);
                for (int i = 0; i < this.currentTest.nRandomNumbers; i++) {
                    this.shuffledNumbers.add(i);
                }
            }
        }

        /**
         *  Advance to the next test.
         */
        private void advance() {
            if (this.currentTest == null) {
                throw new NoSuchElementException();
            }
            if (this.duration > 0 && System.currentTimeMillis() > this.startTimeMS + (duration * 1000)) {
                // Time expired.
                this.currentTest = null;
            } else {
                this.iGeneratedQueries++;
                if (this.testIterator.hasNext()) {
                    // Keep going as long as tests remain.
                    this.nextTest();
                } else {
                    // Wrap around until time expires.
                    this.testIterator = this.tests.listIterator();
                    this.currentTest = this.testIterator.next();
                }
            }
        }

        // Generate table name from prefix and zero-based table index.
        @Override
        public String tableName(int iTable) {
            return String.format("%s_%d", this.currentTest.tablePrefix, iTable + 1);
        }

        // Generate column name from prefix and zero-based table index.
        @Override
        public String columnName(int iColumn) {
            return String.format("%s_%d", this.currentTest.columnPrefix,
                                          (iColumn % this.currentTest.nColumns) + 1);
        }

        // Generate table.column name from prefix and zero-based table index.
        @Override
        public String tableColumnName(int iTable, int iColumn) {
            return String.format("%s.%s", this.tableName(iTable), this.columnName(iColumn));
        }

        // Get an indexed shuffled number.
        @Override
        public int getShuffledNumber(final int i) {
            return this.shuffledNumbers.get(i % this.shuffledNumbers.size());
        }

        // Check if a next query is available. Pre-determined by previous next()/advance() call.
        @Override
        public boolean hasNext() {
            // As soon as tests are exhausted currentTest is set to null in advance().
            return (this.currentTest != null);
        }

        // Get the next query string.
        @Override
        public String next() {
            // Exhausted all tests?
            if (this.currentTest == null) {
                throw new NoSuchElementException();
            }
            // Shuffle the numbers used by the query generator.
            java.util.Collections.shuffle(this.shuffledNumbers, rnd);
            // Ask the derived class to generate the query. Subtract 1 for zero-based query counter.
            String query = this.currentTest.getQuery(this.iGeneratedQueries, this);
            // Get ready for the next one (sets currentTest to null when testing is complete).
            this.advance();
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
            return new QueryIterator(this.tests, this.duration);
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
            throws ConfigurationException, InterruptedException, NoConnectionsException, IOException {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        // Parse the XML test configuration.
        System.out.printf("Reading configuration file '%s'...\n", configPath);
        List<QueryTestBase> tests = BenchmarkConfiguration.configureTests(configPath, this.cliConfig.test);

        // connect to one or more servers, loop until success
        connect(this.cliConfig.servers);

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        System.out.println("Warming up...");
        for (String query : new QueryGenerator(tests, this.cliConfig.warmup)) {
            this.client.callProcedure(new NullCallback(), "@AdHoc", query);
        }

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        this.benchmarkStartTS = System.currentTimeMillis();
        long queryElapsedMS = 0;
        schedulePeriodicStats();

        // Run the benchmark for the requested duration.
        System.out.printf("\nRunning '%s' benchmark...\n", this.cliConfig.test);
        CallProcedureCallback cb = new CallProcedureCallback();
        for (String query : new QueryGenerator(tests, this.cliConfig.duration)) {
            long startTS = System.currentTimeMillis();
            this.client.callProcedure(cb, "@AdHoc", query);
            if (cb.errorStatus != ClientResponse.SUCCESS) {
                throw new RuntimeException(String.format(
                        "callProcedure() failed with status=%d:\n%s",
                        cb.errorStatus, cb.errorString));
            }
            queryElapsedMS += (System.currentTimeMillis() - startTS);
            this.queriesProcessed++;
        }

        printStatistics();
        System.out.println("");

        long totalElapsedMS = System.currentTimeMillis() - this.benchmarkStartTS;

        // cancel periodic stats printing
        this.timer.cancel();

        // block until all outstanding txns return
        this.client.drain();

        // print the summary results
        printResults(totalElapsedMS, queryElapsedMS);

        // close down the client connections
        this.client.close();
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
