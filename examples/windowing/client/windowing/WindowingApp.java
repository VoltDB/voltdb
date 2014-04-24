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

package windowing;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStatusListenerExt;

public class WindowingApp {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    final GlobalState state;

    final ContinuousDeleter deleter;
    final PartitionDataTracker partitionTracker;
    final RandomDataInserter inserter;
    final MaxTracker maxTracker;
    final Reporter reporter;

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            // if the benchmark is still active
            if (inserter.isDone() == false) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public WindowingApp(WindowingConfig config) {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);

        System.out.println(config.getConfigDumpString());

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        ClientConfig clientConfig = new ClientConfig(config.user, config.password, new StatusListener());
        clientConfig.setMaxTransactionsPerSecond(config.ratelimit);

        Client insertsClient = ClientFactory.createClient(clientConfig);
        Client nonInsertsClient = ClientFactory.createClient(clientConfig);

        // connect to one or more servers, loop until success
        try {
            connect(insertsClient, config.servers);
            connect(nonInsertsClient, config.servers);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        state = new GlobalState(config, nonInsertsClient);

        partitionTracker = new PartitionDataTracker(state);
        deleter = new ContinuousDeleter(state);
        inserter = new RandomDataInserter(state, insertsClient);
        maxTracker = new MaxTracker(state);
        reporter = new Reporter(state, inserter);
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    static void connectToOneServerWithRetry(final Client client, String server) {
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
    static void connect(final Client client, String servers) throws InterruptedException {
        System.out.println("Connecting to VoltDB...");

        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(client, server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    /*public synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);

        long nowTotalDeletes = totalDeletes.get();
        long nowDeletesSinceLastChecked = deletesSinceLastChecked.getAndSet(0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        if(this.config.latencyreport) {
            System.out.printf("Avg/95%% Latency %.2f/%dms, ", stats.getAverageLatency(),
                stats.kPercentileLatency(0.95));
        }

        System.out.printf("Total Deletes/Deletes %d/%d", nowTotalDeletes, nowDeletesSinceLastChecked);
        System.out.printf("\n");
    }*/

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // Print periodic stats/analysis to the console
        state.scheduler.scheduleWithFixedDelay(reporter,
                                               state.config.displayinterval,
                                               state.config.displayinterval,
                                               TimeUnit.SECONDS);

        // Update the partition key set, row counts and redundancy level once per second
        state.scheduler.scheduleWithFixedDelay(partitionTracker,
                                               1,
                                               1,
                                               TimeUnit.SECONDS);

        // Delete data as often as need be
        //  -- This will resubmit itself at varying rates according to insert load
        state.scheduler.execute(deleter);

        // Start tracking changes to the maximum value after a 5 second delay to
        // let things settle a bit. Then check up to 100 times per second.
        state.scheduler.scheduleWithFixedDelay(maxTracker,
                                               5000,
                                               10,
                                               TimeUnit.MILLISECONDS);

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        inserter.run();

        state.shutdown();
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link VoterConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        WindowingConfig config = new WindowingConfig();
        config.parse(WindowingApp.class.getName(), args);

        WindowingApp app = new WindowingApp(config);
        app.runBenchmark();
    }
}
