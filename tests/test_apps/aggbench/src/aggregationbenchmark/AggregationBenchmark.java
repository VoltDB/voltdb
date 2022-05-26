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

package aggregationbenchmark;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;

public class AggregationBenchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final AggConfig config;
    // Reference to the database connection we will use
    final Client client;

    AtomicInteger total = new AtomicInteger();

    // Statistics manager objects from the client
    final ClientStatsContext fullStatsContext;

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class AggConfig extends CLIConfig {
        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of invocations.")
        int invocations = 6;

        @Option(desc = "Restore the data from snapshot or not.")
        int restore = 0;

        @Option(desc = "Snapshot path.")
        String snapshotpath = "";

        @Option(desc = "Stored procedure number ( an integer from 1 to 20 )")
        int proc = 1;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Override
        public void validate() {
            if (proc <= 0 || proc > 20) exitWithMessageAndUsage("procedure number must be in range [1, 20]");
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
            System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public AggregationBenchmark(AggConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
        //snapshot restore needs > default 2 minute timeout
        clientConfig.setProcedureCallTimeout(0);

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

    void restoreDatabase() throws Exception {
        ClientResponse resp = null;
        try {
            resp = client.callProcedure("@SnapshotRestore", config.snapshotpath, "TestBackupAggBench");
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        // connect to one or more servers, loop until success
        connect(config.servers);

        if (config.restore > 0) {
            System.out.println("\nLoading data from snapshot...");
            restoreDatabase();
        }

        FileWriter fw = null;
        if ((config.statsfile != null) && (config.statsfile.length() != 0)) {
            fw = new FileWriter(config.statsfile);
        }

        System.out.print(HORIZONTAL_RULE);
        System.out.println("\nRunning Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // Benchmark start time
        long queryStartTS, queryElapse;

        int counter = config.invocations;
        String procName = "Q" + config.proc;
        List<Long> m = new ArrayList<Long>();
        System.out.println(String.format("Running procedure %s for the %d times...", procName, counter));
        queryStartTS = System.nanoTime();
        VoltTable vt = null;
        for (int i = 1; i <= counter; i++) {

            vt = client.callProcedure(procName).getResults()[0];

            if (vt.getRowCount() <= 0) {
                System.err.println("ERROR Query %d empty result set");
                System.exit(-1);
            }

        }

        double avg =  (double)(System.nanoTime() - queryStartTS) / counter;
        System.out.printf("\n\n(Returned %d rows in average %f us)\n",
                vt.getRowCount(), avg);
        // block until all outstanding txns return
        client.drain();

        //retrieve stats
        ClientStats stats = fullStatsContext.fetch().getStats();
        // write stats to file
        //client.writeSummaryCSV(stats, config.statsfile);

        fw.append(String.format("%s,%d,-1,0,0,0,0,%f,0,0,0,0,0,0\n",
                                "Q" + String.format("%02d", config.proc),
                                stats.getStartTimestamp(),
                                avg/1000.0));

        // close down the client connections
        client.close();

        if (fw != null)
            fw.close();
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link AggConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        AggConfig config = new AggConfig();
        config.parse(AggregationBenchmark.class.getName(), args);

        AggregationBenchmark benchmark = new AggregationBenchmark(config);
        benchmark.runBenchmark();
    }
}
