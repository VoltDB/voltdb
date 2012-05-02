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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

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

        @Option(desc = "Number of queries generated per test.")
        int queriespertest = 10000;

        @Option(desc = "Comma-separated tests to run (joins|projections). Runs all tests by default")
        String teststorun = "joins,projections";

        // Set by validate()
        boolean runJoins = false;
        boolean runProjections = false;

        @Override
        public void validate() {
            if (this.displayinterval <= 0)
                exitWithMessageAndUsage("displayinterval must be > 0");
            if (this.queriespertest <= 0)
                exitWithMessageAndUsage("queriespertest must be > 0");
            if (!this.teststorun.isEmpty()) {
                String[] tests = this.teststorun.split(",");
                for (String test : tests) {
                    if (test.equalsIgnoreCase("joins")) {
                        this.runJoins = true;
                    } else if (test.equalsIgnoreCase("projections")) {
                        this.runProjections = true;
                    } else {
                        System.err.printf("WARNING: Ignoring unknown test '%s'\n", test);
                    }
                }
            }
            if (!this.runJoins && !this.runProjections) {
                exitWithMessageAndUsage("No valid tests are specified in teststorun");
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
     * Configuration that determines what the generated join queries look like.
     */
    private static class JoinTest {
        // Table prefix
        public final String tablePrefix;
        // Table count
        public final int nTables;
        // Column prefix
        public final String columnPrefix;
        // Column count
        public final int nColumns;
        // Number of join levels
        public final int nLevels;
        // Number of queries to generate
        public final int nQueries;

        public JoinTest(final String tablePrefix, int nTables,
                                  final String columnPrefix, int nColumns,
                                  int nLevels, int nQueries) {
            this.tablePrefix = tablePrefix;
            this.nTables = nTables;
            this.columnPrefix = columnPrefix;
            this.nColumns = nColumns;
            this.nLevels = nLevels;
            this.nQueries = nQueries;
        }
    }

    /**
     * Implements an iterator that returns generated join query strings.
     */
    private static class JoinIterator implements Iterator<String> {
        private final List<JoinTest> tests;
        private int iGeneratedQueries = 0;
        private final Iterator<JoinTest> testIterator;
        private JoinTest currentTest = null;
        private List<Integer> shuffledNumbers = null;
        private final Random rnd = new Random(99);

        public JoinIterator(List<JoinTest> testParameters) {
            this.tests = testParameters;
            this.testIterator = this.tests.listIterator();
            // Prepare to run the first test
            this.nextTest();
        }

        public void nextTest() {
            this.currentTest = this.testIterator.next();
            this.shuffledNumbers = new ArrayList<Integer>(this.currentTest.nLevels);
            for (int i = 0; i < this.currentTest.nTables; i++) {
                this.shuffledNumbers.add(i);
            }
            java.util.Collections.shuffle(this.shuffledNumbers, rnd);
        }

        // Move the needle, either to the next set of tables within a test or to the next test.
        private void advance() {
            if (this.currentTest == null) {
                throw new NoSuchElementException();
            }
            this.iGeneratedQueries++;
            if (this.iGeneratedQueries == this.currentTest.nQueries) {
                this.iGeneratedQueries = 0;
                if (this.testIterator.hasNext()) {
                    this.nextTest();
                } else {
                    this.currentTest = null;
                }
            }
        }

        // Generate table name from prefix and zero-based table index.
        private String tableName(int iTable) {
            // Use shuffled numbers to randomize the distribution.
            int nTable = this.shuffledNumbers.get(iTable % this.currentTest.nTables) + 1;
            return String.format("%s_%d", this.currentTest.tablePrefix, nTable);
        }

        // Generate column name from prefix and zero-based table index.
        private String columnName(int iColumn) {
            return String.format("%s_%d", this.currentTest.columnPrefix,
                                          (iColumn % this.currentTest.nColumns) + 1);
        }

        // Generate table.column name from prefix and zero-based table index.
        private String tableColumnName(int iTable, int iColumn) {
            return String.format("%s.%s", this.tableName(iTable), this.columnName(iColumn));
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
            // Generate table lists by grabbing n sequential numbers at a time (wrap around).
            int iStart = this.iGeneratedQueries * this.currentTest.nLevels;
            StringBuilder query = new StringBuilder("SELECT * FROM ");
            for (int i = 0; i < this.currentTest.nLevels; i++) {
                if (i > 0) {
                    query.append(", ");
                }
                query.append(this.tableName(iStart + i));
            }
            // The where clause uses a foreign key/primary key pair.
            query.append(" WHERE ");
            for (int i = 0; i < this.currentTest.nLevels - 1; i++) {
                if (i > 0) {
                    query.append(" AND ");
                }
                query.append(this.tableColumnName(iStart + i + 1, 1))
                     .append(" = ")
                     .append(this.tableColumnName(iStart + i, 0));
            }
            this.advance();
            return query.toString();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     *  Iterable Join generator.
     */
    private static class JoinGenerator implements Iterable<String>
    {
        private final List<JoinTest> tests;

        public JoinGenerator(List<JoinTest> tests) {
            this.tests = tests;
        }

        @Override
        public Iterator<String> iterator() {
            return new JoinIterator(this.tests);
        }
    }

    /**
     * Configuration that determines what the projection queries look like.
     */
    private static class ProjectionTest {
        // Full table name
        public final String tableName;
        // Column prefix
        public final String columnPrefix;
        // Column count
        public final int nColumns;
        // Total number of joins to generate
        public int nQueries;

        public ProjectionTest(final String tableName,
                                        final String columnPrefix, int nColumns,
                                        int nQueries) {
            this.tableName = tableName;
            this.columnPrefix = columnPrefix;
            this.nColumns = nColumns;
            this.nQueries = nQueries;
        }
    }

    /**
     * Implements an iterator that returns query strings.
     */
    private static class ProjectionIterator implements Iterator<String> {
        private final List<ProjectionTest> testParameters;
        private int iGeneratedQueries = 0;
        private final Iterator<ProjectionTest> testIterator;
        private ProjectionTest currentTest = null;
        public List<Integer> shuffledNumbers;
        private final Random rnd = new Random(99);

        public ProjectionIterator(List<ProjectionTest> tests) {
            this.testParameters = tests;
            this.testIterator = this.testParameters.listIterator();
            // This column number list gets shuffled each time next() is called.
            this.currentTest = testIterator.next();
            this.shuffledNumbers = new ArrayList<Integer>(this.currentTest.nColumns);
            for (int i = 0; i < this.currentTest.nColumns; i++) {
                this.shuffledNumbers.add(i);
            }
        }

        // Move the needle, either to the next set of tables within a test or to the next test.
        private void advance() {
            if (this.currentTest == null) {
                throw new NoSuchElementException();
            }
            this.iGeneratedQueries++;
            if (this.iGeneratedQueries == this.currentTest.nQueries) {
                this.iGeneratedQueries = 0;
                if (this.testIterator.hasNext()) {
                    this.currentTest = this.testIterator.next();
                } else {
                    this.currentTest = null;
                }
            }
        }

        // Generate column name from prefix and zero-based table index.
        private String columnName(int iColumn) {
            return String.format("%s_%d", this.currentTest.columnPrefix,
                                          (iColumn % this.currentTest.nColumns) + 1);
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
            // Shuffle the column number mappings to (deterministically) randomize the ordering.
            java.util.Collections.shuffle(this.shuffledNumbers, rnd);
            // Build the query
            StringBuilder query = new StringBuilder("SELECT ");
            for (Integer iColumn : this.shuffledNumbers) {
                if (iColumn > 0) {
                    query.append(", ");
                }
                query.append(this.columnName(iColumn));
            }
            query.append(" FROM ")
                 .append(this.currentTest.tableName)
                 .append(" WHERE ")
                 .append(this.columnName(this.shuffledNumbers.get(0)))
                 .append(" = 'abc'");
            this.advance();
            return query.toString();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Iterable projection query generator.
     */
    private static class ProjectionGenerator implements Iterable<String>
    {
        private final List<ProjectionTest> tests;

        public ProjectionGenerator(List<ProjectionTest> tests) {
            this.tests = tests;
        }

        @Override
        public Iterator<String> iterator() {
            return new ProjectionIterator(this.tests);
        }
    }

    /**
     * Provides the lists of join and projection tests to run
     */
    private static class TestConfiguration {
        public List<JoinTest> joinTests;
        public List<ProjectionTest> projectionTests;

        public TestConfiguration(List<JoinTest> joinTests,
                                 List<ProjectionTest> projectionTests) {
            this.joinTests = joinTests;
            this.projectionTests = projectionTests;
        }
    }

    /**
     * Configuration parsing or data exception
     */
    private static class ConfigurationException extends Exception {
        private static final long serialVersionUID = 5634572284073087115L;

        public ConfigurationException(String message) {
            super(message);
        }

        public ConfigurationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Table configuration data from config.xml
     */
    private static class ConfigurationTable {
        public final String name;
        public final String columnPrefix;
        public final int nColumns;
        public final int nVariations;

        public ConfigurationTable(final String name, final String columnPrefix, int nColumns, int nVariations) {
            this.name = name;
            this.columnPrefix = columnPrefix;
            this.nColumns = nColumns;
            this.nVariations = nVariations;
        }

        public static ConfigurationTable fromElement(Element elem) throws ConfigurationException {
            if (!elem.hasAttribute("name")) {
                throw new ConfigurationException("<table> element is missing the 'name' attribute");
            }
            if (!elem.hasAttribute("prefix")) {
                throw new ConfigurationException("<table> element is missing the 'prefix' attribute");
            }
            if (!elem.hasAttribute("columns")) {
                throw new ConfigurationException("<table> element is missing the 'columns' attribute");
            }
            String name = elem.getAttribute("name");
            String columnPrefix = elem.getAttribute("prefix");
            int nColumns = Integer.parseInt(elem.getAttribute("columns"));
            int nVariations = (elem.hasAttribute("variations")
                                    ? Integer.parseInt(elem.getAttribute("variations"))
                                    : 1);
            return new ConfigurationTable(name, columnPrefix, nColumns, nVariations);
        }
    }

    /**
     * Test configuration data from config.xml
     */
    private static class ConfigurationTest {
        public final String type;
        public final String table;
        public final int nLevels;

        public ConfigurationTest(final String type, final String table, int nLevels) {
            this.type = type;
            this.table = table;
            this.nLevels = nLevels;
        }

        public static ConfigurationTest fromElement(Element elem) throws ConfigurationException {
            if (!elem.hasAttribute("type")) {
                throw new ConfigurationException("<test> element is missing the 'type' attribute");
            }
            if (!elem.hasAttribute("table")) {
                throw new ConfigurationException("<test> element is missing the 'table' attribute");
            }
            String type = elem.getAttribute("type");
            String table = elem.getAttribute("table");
            int nLevels = (elem.hasAttribute("levels") ? Integer.parseInt(elem.getAttribute("levels")) : 0);
            return new ConfigurationTest(type, table, nLevels);
        }
    }

    /**
     * Parse config.xml and produce test configuration.
     *
     * @param path path to configuration file
     * @param nQueries number of queries to generate per test
     * @return test configuration
     * @throws ConfigurationException
     */
    public TestConfiguration configureTests(final String path, int nQueries) throws ConfigurationException {
        List<JoinTest> joinTests = new ArrayList<JoinTest>();
        List<ProjectionTest> projectionTests = new ArrayList<ProjectionTest>();

        try {
            DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder docBuilder = docBuilderFactory.newDocumentBuilder();
            Document doc = docBuilder.parse(new File(path));
            doc.getDocumentElement().normalize();

            // Read schema tables (provides variation count for tests, etc.)
            Map<String, ConfigurationTable> tables = new HashMap<String, ConfigurationTable>();
            Element schemaElement = (Element)doc.getElementsByTagName("schema").item(0);
            NodeList tableNodes = schemaElement.getElementsByTagName("table");
            for (int iTable = 0; iTable < tableNodes.getLength(); iTable++) {
                ConfigurationTable table = ConfigurationTable.fromElement((Element)tableNodes.item(iTable));
                tables.put(table.name, table);
            }

            // Read tests and build test lists mixing in data from schema read above.
            Element testElement = (Element)doc.getElementsByTagName("tests").item(0);
            NodeList testNodes = testElement.getElementsByTagName("test");
            for (int iTest = 0; iTest < testNodes.getLength(); iTest++) {
                ConfigurationTest test = ConfigurationTest.fromElement((Element)testNodes.item(iTest));
                if (tables.containsKey(test.table)) {
                    ConfigurationTable table = tables.get(test.table);
                    if (test.type.equalsIgnoreCase("join")) {
                        joinTests.add(new JoinTest(test.table, table.nVariations, table.columnPrefix,
                                                   table.nColumns, test.nLevels, nQueries));
                    }
                    else if (test.type.equalsIgnoreCase("projection")) {
                        projectionTests.add(new ProjectionTest(test.table, table.columnPrefix,
                                                               table.nColumns, nQueries));
                    }
                } else {
                    throw new ConfigurationException(
                            String.format("Configuration includes test for unknown table '%s'", test.table));
                }
            }
        } catch (SAXException e) {
            throw new ConfigurationException("XML parser SAX exception", e);
        } catch (ParserConfigurationException e) {
            throw new ConfigurationException("XML parser configuration exception", e);
        } catch (IOException e) {
            throw new ConfigurationException("XML parser I/O exception", e);
        }

        return new TestConfiguration(joinTests, projectionTests);
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @param configPath  path to configuration file
     * @param nQueriesPerTest  number of queries to generate per test
     *
     * @throws ConfigurationException
     * @throws InterruptedException
     * @throws IOException
     * @throws NoConnectionsException
     */
    public void runBenchmark(String configPath, int nQueriesPerTest)
            throws ConfigurationException, InterruptedException, NoConnectionsException, IOException {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        // Hard-code the tests to run for now.
        System.out.printf("Reading configuration file '%s'...\n", configPath);
        TestConfiguration testConfig = configureTests(configPath, nQueriesPerTest);

        // connect to one or more servers, loop until success
        connect(this.cliConfig.servers);

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // print periodic statistics to the console
        this.benchmarkStartTS = System.currentTimeMillis();
        long queryElapsedMS = 0;
        schedulePeriodicStats();

        // Run the benchmark loop for the requested number of generated queries.
        // Show intermediate results if more than one test is enabled.
        if (this.cliConfig.runJoins) {
            System.out.println("\nRunning join benchmark...");
            for (String joinQuery : new JoinGenerator(testConfig.joinTests)) {
                long startTS = System.currentTimeMillis();
                this.client.callProcedure(new NullCallback(), "@AdHoc", joinQuery);
                queryElapsedMS += (System.currentTimeMillis() - startTS);
                this.queriesProcessed++;
            }
        }
        if (this.cliConfig.runProjections) {
            System.out.println("\nRunning projection benchmark...");
            for (String projectionQuery : new ProjectionGenerator(testConfig.projectionTests)) {
                long startTS = System.currentTimeMillis();
                this.client.callProcedure(new NullCallback(), "@AdHoc", projectionQuery);
                queryElapsedMS += (System.currentTimeMillis() - startTS);
                this.queriesProcessed++;
            }
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

        benchmark.runBenchmark(cliConfig.configfile, cliConfig.queriespertest);
    }
}
