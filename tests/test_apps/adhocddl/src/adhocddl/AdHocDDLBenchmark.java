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

package adhocddl;

import java.io.FileWriter;
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
        @Option(desc = "Number of warmup tests sent to server")
        int numOfWarmup = 5;

        @Option(desc = "Number of tests sent to server")
        int numOfTests = 5;

        @Option(desc = "Number of SPs per table in server")
        int numOfSPs = 4;

        @Option(desc = "Table name prefix")
        String prefix = "TABLE";

        @Option(desc = "Table name for CREATE and DROP")
        String table = "TEST";

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Number of Columns in each tables")
        int numOfCols = 5;

        @Option(desc = "Percentage of indexed columns in the ramdonly generated table")
        double idxPercent = 0.1;

        @Override
        public void validate() {
            if (numOfTests <= 0) exitWithMessageAndUsage("numOfTests must be > 0");
            if (numOfCols <= 0) exitWithMessageAndUsage("numOfCols must be > 0");
            if (idxPercent < 0 || idxPercent > 1) exitWithMessageAndUsage("idxPercent must be >= 0 and <= 1");
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

    /**
     * Test the exec time for AdHoc sqlstmt
     * @param sqlstmt
     * @return exec time
     */
    public long runTest(String sqlstmt) throws Exception {
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
            throw e;
        }
    }

    /**
     * Run the config.numOfTests times CREATE and Drop
     * @return avg exec time for CREATE and DROP in total
     */
    public float averageLatencyTest() throws Exception
    {
        String createStmt = DDLGen.CreateTable(0, config.table);
        String dropStmt = DDLGen.DropTable(0, config.table);

        // Warmup
        for(int i = 0; i < config.numOfWarmup; i++)
        {
            runTest(createStmt);
            runTest(dropStmt);
        }

        long sum = 0;
        for(int i = 0; i < config.numOfTests; i++)
        {
            sum += runTest(createStmt);
            sum += runTest(dropStmt);
        }

        System.out.println();
        System.out.println("Average Latency: " + (sum / config.numOfTests));
        return sum / config.numOfTests;
    }

    /**
     * Update catalog from the client side
     * Exec AdHoc DDL in bunch
     * @param startNo
     * @param endNo
     */
    public void updateServer(int startNo, int endNo) throws Exception
    {
        StringBuffer sqlstmt = new StringBuffer();
        for(int i = startNo; i < endNo; i++)
        {
            sqlstmt.append(DDLGen.CreateTable(i, config.prefix) + "\n\n");
            for(int j = 0; j < config.numOfSPs; j++)
            {
                sqlstmt.append(DDLGen.CreateProcedure(j, i, config.prefix) + "\n\n");
            }
        }
        runTest(sqlstmt.toString());
    }

    /**
     * Clear all the tables and procedures in catalog from client
     * Exec AdHoc DDL in bunch
     * @param startNo
     * @param endNo
     */
    public void clearServer(int startNo, int endNo) throws Exception
    {
        StringBuffer sqlstmt = new StringBuffer();
        for(int i = startNo; i < endNo; i++)
        {
            sqlstmt.append(DDLGen.DropTable(i, config.prefix) + "\n\n");
            for(int j = 0; j < config.numOfSPs; j++)
            {
                sqlstmt.append(DDLGen.DropProcedure(j, i, config.prefix) + "\n\n");
            }
        }
        runTest(sqlstmt.toString());
    }

    /**
     * Run test in bunch to generate stats for charts
     * @param col
     * @param fw
     * @param label
     * @throws IOException
     */
    public void bunchTest(int col, FileWriter fw, String label) throws Exception
    {
        fullStatsContext.fetchAndResetBaseline();
        DDLGen = new DDLGenerator(col, config.idxPercent);

        float series1 = averageLatencyTest();
        updateServer(0, 100);
        float series2 = averageLatencyTest();
        updateServer(100, 1000);
        float series3 = averageLatencyTest();

        ClientStats stats = fullStatsContext.fetchAndResetBaseline().getStats();

        if(fw != null)
        {
            fw.append(String.format("%s,%d,-1,%.2f,0,0,%.2f,%.2f\n",
                    label,
                    stats.getStartTimestamp(),
                    series3,
                    series1,
                    series2));
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
        System.out.println(" Statistics ");
        System.out.println(HORIZONTAL_RULE);

        FileWriter fw = null;
        if((config.statsfile != null) && (config.statsfile.length() != 0))
        {
            fw = new FileWriter(config.statsfile);
        }

        bunchTest(5, fw, "5 columns table");
        clearServer(0, 1000);
        bunchTest(50, fw, "50 columns table");
        clearServer(0, 1000);

        if((config.statsfile != null) && (config.statsfile.length() != 0))
        {
            fw.close();
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
