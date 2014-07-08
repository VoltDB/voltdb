/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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



import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;

public class AdHocDDLBenchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final BenchmarkConfig config;
    // Reference to the database connection we will use
    final Client client;
    // DDL generator
    DDLGenerator DDLGen;
    // Flags to tell the worker threads to stop or go
    boolean benchmarkComplete = false;
    // Statistics manager objects from the client
    final ClientStatsContext fullStatsContext;

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class BenchmarkConfig extends CLIConfig {
        @Option(desc = "Number of tests sent to server")
        int numOfTests = 20;

        @Option(desc = "Table name prefix for test")
        String prefix = "TEST";

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "0: create tables test; 1: drop table test; 2: average latency test")
        int testMode = 2;

        @Option(desc = "Median number of Columns in randomly generated tables")
        int numOfCols = 10;

        @Option(desc = "Percentage of indexed columns in the ramdonly generated table")
        double idxPercent = 0.1;

        @Override
        public void validate() {
            if (numOfTests <= 0) exitWithMessageAndUsage("numOfTests must be > 0");
            if (numOfCols <= 0) exitWithMessageAndUsage("numOfCols must be > 0");
            if (idxPercent <= 0 || idxPercent >= 1) exitWithMessageAndUsage("idxPercent must be > 0 and < 1");
        }
    }

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            // if the benchmark is still active
            if (!benchmarkComplete) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     * @throws IOException
     * @throws UnknownHostException
     */
    public AdHocDDLBenchmark(BenchmarkConfig config) throws UnknownHostException, IOException {
        DDLGen = new DDLGenerator(config.numOfCols, config.idxPercent);
        this.config = config;

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());

        client = ClientFactory.createClient(clientConfig);

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

    public long runTest(String sqlstmt) {
        // synchronously call the "AdHoc" procedure
        try
        {
            long start = System.currentTimeMillis();
            ClientResponse response = client.callProcedure("@AdHoc", sqlstmt);
            if(response.getStatus() != ClientResponse.SUCCESS)
            {
                System.out.println("AdHoc call failed");
                return 0;
            }
            else
            {
                long end = System.currentTimeMillis();
                System.out.println(end - start);
                return end - start;
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        return 0;
    }

    public void createTableTest()
    {
        fullStatsContext.fetchAndResetBaseline();

        String sqlstmt;
        for(int i = 0; i < config.numOfTests; i++)
        {
            sqlstmt = DDLGen.CreateTable(i, config.prefix);
            runTest(sqlstmt);
        }

        ClientStats stats = fullStatsContext.fetch().getStats();
        System.out.println();
        System.out.println("Average Latency: " + stats.getAverageLatency());
        System.out.println("Average Internal Latency: " + stats.getAverageInternalLatency());
    }

    public void dropTableTest()
    {
        String sqlstmt;
        for(int i = 0; i < config.numOfTests; i++)
        {
            sqlstmt = DDLGen.CreateTable(i, config.prefix);
            runTest(sqlstmt);
        }

        fullStatsContext.fetchAndResetBaseline();

        for(int i = 0; i < config.numOfTests; i++)
        {
            sqlstmt = DDLGen.DropTable(i, config.prefix);
            runTest(sqlstmt);
        }

        ClientStats stats = fullStatsContext.fetch().getStats();
        System.out.println();
        System.out.println("Average Latency: " + stats.getAverageLatency());
        System.out.println("Average Internal Latency: " + stats.getAverageInternalLatency());
    }

    public void averageLatencyTest()
    {
        String sqlstmt;
        long createSum = 0, dropSum = 0;
        for(int i = 0; i < config.numOfTests; i++)
        {
            sqlstmt = DDLGen.CreateTable(i, config.prefix);
            createSum += runTest(sqlstmt);
            sqlstmt = DDLGen.DropTable(i, config.prefix);
            dropSum += runTest(sqlstmt);
        }

        System.out.println();
        System.out.println("Average Latency for CREATE: " + (createSum / config.numOfTests));
        System.out.println("Average Latency for DROP: " + (dropSum / config.numOfTests));
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
        System.out.println(" Statistics ");
        System.out.println(HORIZONTAL_RULE);

        switch(config.testMode)
        {
        case 0:
            createTableTest();
            break;
        case 1:
            dropTableTest();
            break;
        case 2:
            averageLatencyTest();
            break;
        default:
            System.out.println("No such test!");
            break;
        }

        // block until all outstanding txns return
        client.drain();

        benchmarkComplete = true;
        // close down the client connections
        client.close();
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link BenchmarkConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        BenchmarkConfig config = new BenchmarkConfig();
        config.parse(AdHocDDLBenchmark.class.getName(), args);

        AdHocDDLBenchmark benchmark = new AdHocDDLBenchmark(config);
        benchmark.runBenchmark();
    }
}
