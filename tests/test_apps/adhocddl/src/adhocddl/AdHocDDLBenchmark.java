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

package adhocddl;

import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStatusListenerExt;

public class AdHocDDLBenchmark {

    // handy, rather than typing this out several times
    private static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    private final BenchmarkConfig config;
    // Reference to the database connection we will use
    private final Client client;
    // DDL generator
    private DDLGenerator DDLGen;
    // Flags to tell the worker threads to stop or go
    private boolean benchmarkComplete = false;

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    private static class BenchmarkConfig extends CLIConfig {
        @Option(desc = "Number of measured CREATE and DELETE statements per round")
        int numCreateDeleteCalls = 5;

        @Option(desc = "Number of rounds of tests averaged together")
        int numRounds = 5;

        @Option(desc = "Number of extra procedures per table in server")
        int numProcedures = 2;

        @Option(desc = "Number of tables in benchmarked DDL")
        int numTables = 40;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "results.txt";

        @Option(desc = "Number of Columns in each tables")
        int numCols = 20;

        @Option(desc = "Percentage of indexed columns in the ramdonly generated table")
        double idxPercent = 0.1;

        @Override
        public void validate() {
            if (numRounds <= 0) exitWithMessageAndUsage("numRounds must be > 0");
            if (numCreateDeleteCalls <= 0) exitWithMessageAndUsage("numCreateDeleteCalls must be > 0");
            if (numProcedures <= 0) exitWithMessageAndUsage("numProcedures must be > 0");
            if (numCols <= 0) exitWithMessageAndUsage("numCols must be > 0");
            if (numTables <= 0) exitWithMessageAndUsage("numTables must be > 0");
            if (idxPercent < 0 || idxPercent > 1) exitWithMessageAndUsage("idxPercent must be >= 0 and <= 1");
        }
    }

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    private class StatusListener extends ClientStatusListenerExt {
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
    private AdHocDDLBenchmark(BenchmarkConfig config) throws UnknownHostException, IOException {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());

        client = ClientFactory.createClient(clientConfig);

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
    private void connectToOneServerWithRetry(String server) {
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
    private void connect(String servers) throws InterruptedException {
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

    private double averageLatencyTest() throws Exception
    {
        String createStmt = DDLGen.CreateTable(0, "TEST");
        String dropStmt = DDLGen.DropTable(0, "TEST");

        System.out.printf("Running %d CREATEs and DROPs for test.\n", config.numCreateDeleteCalls);
        System.out.flush();

        long sum = 0;
        for(int i = 0; i < config.numCreateDeleteCalls; i++)
        {
            long start = System.currentTimeMillis();
            client.callProcedure("@AdHoc", createStmt);
            sum += System.currentTimeMillis() - start;
            client.callProcedure("@AdHoc", dropStmt);
        }

        return sum / 1000.0 / config.numCreateDeleteCalls;
    }

    private long createSchema(boolean batch) throws Exception
    {
        System.out.printf("Creating schema in %sbatch mode.\n", batch ? "" : "non-");
        System.out.flush();

        StringBuffer sqlstmt = new StringBuffer();
        long start = System.currentTimeMillis();
        for(int i = 0; i < config.numTables; i++)
        {
            sqlstmt.append(DDLGen.CreateTable(i, "TABLE") + "\n\n");
            if (!batch) {
                System.out.printf("Creating table and %d procedures for table %d of %d.\n",
                        config.numProcedures, i, config.numTables);
                System.out.flush();
                client.callProcedure("@AdHoc", sqlstmt.toString());
                sqlstmt = new StringBuffer();
            }

            for(int j = 0; j < config.numProcedures; j++)
            {
                sqlstmt.append(DDLGen.CreateProcedure(j, i, "TABLE") + "\n\n");
                if (!batch) {
                    client.callProcedure("@AdHoc", sqlstmt.toString());
                    sqlstmt = new StringBuffer();
                }
            }
        }

        if (batch) {
            System.out.printf("Calling a single batch insert @AdHoc for the whole schema.\n");
            client.callProcedure("@AdHoc", sqlstmt.toString());
        }

        return System.currentTimeMillis() - start;
    }

    private void clearServer() throws Exception
    {
        System.out.println("Clearing tables and procedures.");
        System.out.flush();

        StringBuffer sqlstmt = new StringBuffer();
        for(int i = 0; i < config.numTables; i++)
        {
            sqlstmt.append(DDLGen.DropTable(i, "TABLE") + "\n\n");
            for(int j = 0; j < config.numProcedures; j++)
            {
                sqlstmt.append(DDLGen.DropProcedure(j, i, "TABLE") + "\n\n");
            }
        }

        client.callProcedure("@AdHoc", sqlstmt.toString());
    }

    private void runBenchmark() throws Exception {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        // connect to one or more servers, loop until success
        connect(config.servers);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Benchmark ");
        System.out.println(HORIZONTAL_RULE);

        double sumBatched = 0, sumScripted = 0, sumIndividual = 0;

        for (int i = 0; i < config.numRounds; i++) {

            System.out.printf("Running round %d of %d.\n", i, config.numRounds);
            System.out.print(HORIZONTAL_RULE);

            DDLGen = new DDLGenerator(config.numCols, config.idxPercent);

            clearServer();

            long scriptTime = createSchema(false);

            double perStmtLatencySec = averageLatencyTest();

            clearServer();

            long batchTime = createSchema(true);

            clearServer();

            System.out.print(HORIZONTAL_RULE);
            System.out.printf("Batched %.3f seconds, Scripted %.3f seconds, Single create %.3f seconds\n",
                    batchTime / 1000.0d, scriptTime / 1000d, perStmtLatencySec);
            System.out.println(HORIZONTAL_RULE);

            sumBatched += batchTime / 1000.0d;
            sumScripted += scriptTime / 1000.0d;
            sumIndividual += perStmtLatencySec;
        }

        double avgBatched = sumBatched / config.numRounds;
        double avgScripted = sumScripted / config.numRounds;
        double avgIndividual = sumIndividual / config.numRounds;

        System.out.println();
        System.out.print(HORIZONTAL_RULE);
        System.out.printf("FINAL AVERAGE STATS OVER %d RUNS:\n", config.numRounds);
        System.out.printf("Batched %f seconds, Scripted %f seconds, Single create %f seconds\n",
                avgBatched, avgScripted, avgIndividual);
        System.out.println(HORIZONTAL_RULE);

        if((config.statsfile != null) && (config.statsfile.length() != 0))
        {
            FileWriter fw = new FileWriter(config.statsfile);

            // write config
            // tables, columns, procs per table, index percentage, rounds, create delete calls
            fw.append(String.format("%d,%d,%d,%.3f,%d,%d\n",
                    config.numTables, config.numCols, config.numProcedures, config.idxPercent,
                    config.numRounds, config.numCreateDeleteCalls));

            // write results
            fw.append(String.format("%.3f,%.3f,%.3f\n", avgBatched, avgScripted, avgIndividual));
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
