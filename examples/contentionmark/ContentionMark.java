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

import java.io.IOException;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;

/**
 * ContentionMark is a small VoltDB application that measures VoltDB
 * throughput under extreme contention.
 *
 * By default, ContentionMark creates one row, then updates it as fast
 * as VoltDB will let it, one transaction per update.
 *
 * The "Increment" procedure that is called repeatedly adds a random
 * value between 0 and 4 (inclusive) to the "value" field of a specific
 * tuple.
 */
public class ContentionMark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final ContentionMarkConfig config;

    // Reference to the database connection we will use
    final Client client;

    // Instance of Random for generating randomized data.
    Random rand;

    // Benchmark start time
    long benchmarkStartTS;

    long lastStatsReportTS;

    // Statistics
    AtomicLong incrementCount = new AtomicLong(0);
    AtomicLong lastPeriod = new AtomicLong(0);
    AtomicLong failureCount = new AtomicLong(0);

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class ContentionMarkConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 60;

        @Option(desc = "Warmup duration, in seconds.")
        int warmup = 5;

        @Option(desc = "Number of tuples to fight over.")
        int tuples = 1;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";

        @Override
        public void validate() {
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (tuples <= 0) exitWithMessageAndUsage("tuples must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
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
            if ((System.currentTimeMillis() - benchmarkStartTS) < (config.duration * 1000)) {
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
    public ContentionMark(ContentionMarkConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig(config.user, config.password, new StatusListener());
        clientConfig.setMaxTransactionsPerSecond(config.ratelimit);

        client = ClientFactory.createClient(clientConfig);

        rand = new Random(777);

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
    static void connectToOneServerWithRetry(Client client, String server) {
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
     * Print stats for the last displayinterval seconds to the console.
     */
    public synchronized void printStatistics() {
        // quickly memoize and update counters, get the time, record update time, etc...
        long nowSuccesses = incrementCount.get();
        long nowFailures = failureCount.get();
        long prevSuccesss = lastPeriod.getAndSet(nowSuccesses);
        long endTime = System.currentTimeMillis();
        long startTime = lastStatsReportTS;
        lastStatsReportTS = endTime;

        // do math with as much safety as possible
        long invocationsCompleted = Math.max(nowSuccesses - prevSuccesss, 0);
        long periodDurationMs = Math.max(endTime - startTime, 1);
        long cumulativeDurationMs = Math.max(endTime - benchmarkStartTS, 1);
        long periodThroughput = (long) (invocationsCompleted / (periodDurationMs / 1000.0));
        long cumulativeThroughput = (long) (nowSuccesses / (cumulativeDurationMs / 1000.0));
        long time = (long) (Math.round(endTime - benchmarkStartTS) / 1000.0);

        // print out periodic report
        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Period Throughput %10d/s, ", periodThroughput);
        System.out.printf("Cumulative Throughput %10d/s, ", cumulativeThroughput);
        System.out.printf("Total Failures %10d", nowFailures);
        System.out.printf("\n");
    }

    /**
     * ProcedureCallback implementation that tracks successes and failures.
     */
    class CMCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                incrementCount.incrementAndGet();
            }
            else {
                failureCount.incrementAndGet();
            }
        }
    };

    /**
     * Run the "Increment" procedure on the server asynchronously.
     */
    void increment() {
        long id = rand.nextInt(config.tuples);
        long toIncrement = rand.nextInt(5); // 0 - 4

        try {
            client.callProcedure(new CMCallback(), "Increment", toIncrement, id);
        }
        catch (IOException e) {
            // This is not ideal error handling for production, but should be
            // harmless in a benchmark like this
            try { Thread.sleep(50); } catch (Exception e2) {}
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
        connect(client, config.servers);

        for (long i = 0; i < config.tuples; i++) {
            ClientResponse response = client.callProcedure("Init", i, i);
            if (response.getStatus() != ClientResponse.SUCCESS) {
                System.exit(-1);
            }
        }

        System.out.print("\n\n" + HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        System.out.println("\nWarming up...");
        final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
        while (warmupEndTime > System.currentTimeMillis()) {
            increment();
        }

        // reset counters before the real benchmark starts post-warmup
        incrementCount.set(0);
        lastPeriod.set(0);
        failureCount.set(0);
        lastStatsReportTS = System.currentTimeMillis();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() { printStatistics(); }
        };
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(statsPrinting,
                                  config.displayinterval * 1000,
                                  config.displayinterval * 1000);

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        System.out.println("\nRunning benchmark...");
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {
            increment();
        }

        // cancel periodic stats printing
        timer.cancel();
        // block until all outstanding txns return
        client.drain();
        // close down the client connections
        client.close();
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link ContentionMarkConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        ContentionMarkConfig config = new ContentionMarkConfig();
        config.parse(ContentionMarkConfig.class.getName(), args);

        ContentionMark benchmark = new ContentionMark(config);
        benchmark.runBenchmark();
    }
}
