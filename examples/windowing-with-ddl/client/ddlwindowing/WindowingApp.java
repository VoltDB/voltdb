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

package ddlwindowing;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStatusListenerExt;

/**
 * <p>App that does three simultaneous things on a single-table schema:</p>
 *
 * <ol>
 * <li>Insert random, timestamped tuples at a high rate.</li>
 * <li>Check for changes in the maximum value stored in the table.</li>
 * <li>Periodically compute an average of values over various time windows.</li>
 * </ol>
 *
 * <p>It does this by creating task-focused classes that implement Runnable.
 * Each class has a specific job and is scheduled to run periodically in a
 * threadpool. All inter-task communication is done via the main instance of
 * this class.</p>
 *
 * <p>Note there is no need to explicitly invoke a procedure to delete old data
 * anywhere in application code.  Deletion of old data is taken care of by the
 * DELETE statement embedded in the LIMIT PARTITION ROWS constraint of the
 * table definition.</p>
 */
public class WindowingApp {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // these values are updated by the UpdatePartitionData class each time it is run
    private final AtomicReference<Map<Long, PartitionInfo>> partitionData =
            new AtomicReference<Map<Long, PartitionInfo>>(new HashMap<Long, PartitionInfo>());

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class WindowingConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Duration, in seconds.")
        int duration = 120;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";

        @Option(desc = "Maximum TPS rate for inserts.")
        int ratelimit = 15000;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
        }
    }

    /////
    // PACKAGE VISIBLE SHARED STATE ACCESS BELOW
    /////

    static final class PartitionInfo {
        String partitionKey;
        long tupleCount;
        long oldestTupleAge;
        long youngestTupleAge;
    }

    // Reference to the database connection we will use
    final Client client;

    // validated command line configuration
    final WindowingConfig config;

    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    final long startTS = System.currentTimeMillis();

    final PartitionDataTracker partitionTracker;
    final RandomDataInserter inserter;
    final MaxTracker maxTracker;
    final Reporter reporter;

    Map<Long, PartitionInfo> getPartitionData() {
        return partitionData.get();
    }

    void updatePartitionInfo(Map<Long, PartitionInfo> partitionData) {
        this.partitionData.set(partitionData);
   }


    void shutdown() {
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            // block until all outstanding txns return
            client.drain();
            // close down the client connections
            client.close();
        }
        catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            // if the benchmark is still active, note if a server disconnects
            final long benchmarkEndTime = startTS + (1000l * config.duration);
            if (System.currentTimeMillis() < benchmarkEndTime) {
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
        this.config = config;

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
        client = ClientFactory.createClient(clientConfig);

        // connect to one or more servers, loop until success
        try {
            connect(insertsClient, config.servers);
            connect(client, config.servers);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        partitionTracker = new PartitionDataTracker(this);
        inserter = new RandomDataInserter(this, insertsClient);
        maxTracker = new MaxTracker(this);
        reporter = new Reporter(this);
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
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void run() {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Processing");
        System.out.println(HORIZONTAL_RULE);

        // Print periodic stats/analysis to the console
        scheduler.scheduleWithFixedDelay(reporter,
                config.displayinterval,
                config.displayinterval,
                TimeUnit.SECONDS);

        // Update the partition key set, row counts and redundancy level 4 times per second
        scheduler.scheduleWithFixedDelay(partitionTracker,
                0,
                250,
                TimeUnit.MILLISECONDS);

        // Start tracking changes to the maximum value after a 5 second delay to
        // let things settle a bit. Then check up to 100 times per second.
        scheduler.scheduleWithFixedDelay(maxTracker,
                5000,
                10,
                TimeUnit.MILLISECONDS);

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        inserter.run();

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Processing Complete");
        System.out.println(HORIZONTAL_RULE);

        shutdown();
    }

    /**
     * Main routine creates an instance of this app and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link WindowingConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        WindowingConfig config = new WindowingConfig();
        config.parse(WindowingApp.class.getName(), args);

        WindowingApp app = new WindowingApp(config);
        app.run();
    }
}
