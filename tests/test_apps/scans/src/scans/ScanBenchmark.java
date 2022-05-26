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
 * This samples uses the native asynchronous request processing protocol
 * to post requests to the VoltDB server, thus leveraging to the maximum
 * VoltDB's ability to run requests in parallel on multiple database
 * partitions, and multiple servers.
 *
 * While asynchronous processing is (marginally) more convoluted to work
 * with and not adapted to all workloads, it is the preferred interaction
 * model to VoltDB as it guarantees blazing performance.
 *
 * Because there is a risk of 'firehosing' a database cluster (if the
 * cluster is too slow (slow or too few CPUs), this sample performs
 * auto-tuning to target a specific latency (5ms by default).
 * This tuning process, as demonstrated here, is important and should be
 * part of your pre-launch evaluation so you can adequately provision your
 * VoltDB cluster with the number of servers required for your needs.
 */

package scans;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NullCallback;

public class ScanBenchmark {

    static VoltLogger log = new VoltLogger("HOST");

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final ScanConfig config;
    // Reference to the database connection we will use
    final Client client;
    // Benchmark start time
    long benchmarkStartTS;
    long benchmarkEndTS;

    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    final Random rand = new Random();

    static final List<String> tests = Arrays.asList(new String[]{"sequential", "index"});

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class ScanConfig extends CLIConfig {
        @Option(desc = "Number of times to run the benchmark (averages runs).")
        long runs = 5;

        @Option(desc = "Number of loaded rows.")
        long rows = 1000000;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";

        @Option(desc = "Test to run.")
        String test = "";

        @Override
        public void validate() {
            boolean testIsValid = false;
            for (String t : tests) {
              if (test.equals(t))
                  testIsValid = true;
            }
            if (!testIsValid) exitWithMessageAndUsage("test is invalid");
            if (runs <= 0) exitWithMessageAndUsage("runs must be > 0");
            if (rows < 0) exitWithMessageAndUsage("rows must be >= 0");
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public ScanBenchmark(ScanConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig(config.user, config.password);
        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
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
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // 3. Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        double averageTimePerScan = (benchmarkEndTS - benchmarkStartTS) / (double) config.runs;
        double tuplesPerSecond = (config.rows / averageTimePerScan) * 1000.0;

        System.out.printf("Each of %d %s scans of %d tuples took %.2fms for a throughput of %.2f tuples/second.\n",
                config.runs, config.test, config.rows, averageTimePerScan, tuplesPerSecond);

        PrintWriter outputStream = null;

        if (config.statsfile != "") {
            try {
                outputStream = new PrintWriter(new FileWriter(config.statsfile));
                // for stats: duration in milliseconds, # iterations (# rows in this case)
                outputStream.printf("0,%f,%d,0,0,0,0,0,0,0,0,0,0\n", averageTimePerScan, config.rows);
            } catch (Exception e) {
                System.err.println("ERROR unable to write stats file");
                System.err.println(e);
                System.exit(1);
            } finally {
                outputStream.close();
            }
        }
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        // connect to one or more servers, loop until success
        connect(config.servers);

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Loading Tuples");
        System.out.println(HORIZONTAL_RULE);

        String loadproc = null;
        String tablename = null;
        String scanproc = null;

        if (config.test.equals("sequential")) {

            loadproc = "NARROW_P.insert";
            tablename = "narrow_p";
            scanproc = "MinSeqScan";

        } else if (config.test.equals("index")) {

            loadproc = "NARROW_INDEX_P.insert";
            tablename = "narrow_index_p";
            scanproc = "MinIndexScan";
        }

        for (long i = 0; i < config.rows; i++) {
            client.callProcedure(new NullCallback(), loadproc, i % 509 /* radom prime */, i);
            if ((i % 100000) == 0) {
                System.out.printf("Loading row at index %d.\n", i);
            }
        }
        client.drain();

        ClientResponse cr = client.callProcedure("@AdHoc", "select count(*) from " + tablename + ";");
        long rows = cr.getResults()[0].asScalarLong();

        System.out.printf("Loaded %d rows.\n", rows);

        assert (rows == config.rows);

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        benchmarkStartTS = System.currentTimeMillis();

        System.out.printf("\nRunning %s scan benchmark...\n", config.test);

        for (int i = 0; i < config.runs; i++) {
            client.callProcedure(scanproc);
        }

        benchmarkEndTS = System.currentTimeMillis();

        // print the summary results
        printResults();

        // close down the client connections
        client.close();
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link VoterConfig}
     */
    public static void main(String[] args) throws Exception {
        /*VoltDB.Configuration vconfig = new VoltDB.Configuration();

        CatalogBuilder cb = new CatalogBuilder();
        cb.addSchema("/Users/jhugg/Documents/workspace/voltdb/tests/test_apps/scans/ddl.sql");
        vconfig.m_pathToCatalog = Configuration.getPathToCatalogForTest("scans.jar");
        boolean success = cb.compile(vconfig.m_pathToCatalog);
        assert(success);

        DeploymentBuilder db = new DeploymentBuilder(1, 1, 0);
        vconfig.m_pathToDeployment = Configuration.getPathToCatalogForTest("scans.xml");
        db.writeXML(vconfig.m_pathToDeployment);

        ServerThread server = new ServerThread(vconfig);

        server.start();
        server.waitForInitialization();*/

        // create a configuration from the arguments
        ScanConfig config = new ScanConfig();
        //config.test = "sequential";
        config.parse(ScanBenchmark.class.getName(), args);

        ScanBenchmark benchmark = new ScanBenchmark(config);
        benchmark.runBenchmark();

        //server.shutdown();
        //server.join();
    }
}
