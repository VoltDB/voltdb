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
/*
 * This samples uses the native asynchronous request processing protocol
 * to post requests to the VoltDB server, thus leveraging to the maximum
 * VoltDB's ability to run requests in parallel on multiple database
 * partitions, and multiple servers.
 *
 * While asynchronous processing is (marginally) more convoluted to work
 * with and not adapted to all workloads, it is the preferred interaction
 * model to VoltDB as it allows a single client with a small amount of
 * threads to flood VoltDB with requests, guaranteeing blazing throughput
 * performance.
 *
 * Note that this benchmark focuses on throughput performance and
 * not low latency performance.  This benchmark will likely 'firehose'
 * the database cluster (if the cluster is too slow or has too few CPUs)
 * and as a result, queue a significant amount of requests on the server
 * to maximize throughput measurement. To test VoltDB latency, run the
 * SyncBenchmark client, also found in the voter sample directory.
 */

package windowing;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;

public class WindowingApp {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final WindowingConfig config;
    // Reference to the database connection we will use
    final Client insertsClient;
    final Client nonInsertsClient;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    private final AtomicLong totalDeletes = new AtomicLong(0);
    private final AtomicLong deletesSinceLastChecked = new AtomicLong(0);

    final ContinuousDeleter ctb;
    final PartitionDataTracker partitionData;

    static Random rand = new Random(0);

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class WindowingConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 2000;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 2;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "")
        long maxrows = 0;

        @Option(desc = "")
        long historyseconds = 30;

        @Option(desc = "")
        long maxrss = 0;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Report latency for async benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (maxrows < 0) exitWithMessageAndUsage("maxrows must be >= 0");
            if (historyseconds < 0) exitWithMessageAndUsage("historyseconds must be >= 0");
            if (historyseconds < 0) exitWithMessageAndUsage("historyseconds must be >= 0");
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
    public WindowingApp(WindowingConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig(config.user, config.password, new StatusListener());
        clientConfig.setMaxTransactionsPerSecond(config.ratelimit);

        insertsClient = ClientFactory.createClient(clientConfig);
        nonInsertsClient = ClientFactory.createClient(clientConfig);

        periodicStatsContext = insertsClient.createStatsContext();
        fullStatsContext = insertsClient.createStatsContext();

        partitionData = new PartitionDataTracker(nonInsertsClient);
        ctb = new ContinuousDeleter();

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
        if(config.latencyreport) {
            System.out.println("NOTICE: Option latencyreport is ON for async run, please set a reasonable ratelimit.\n");
        }
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
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
     */
    public void schedulePeriodicStats() {
        timer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() { printStatistics(); }
        };
        timer.scheduleAtFixedRate(statsPrinting,
                                  config.displayinterval * 1000,
                                  config.displayinterval * 1000);
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized void printStatistics() {
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
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // 1. Voting Board statistics, Voting results and performance statistics
        String display = "\n" +
                         HORIZONTAL_RULE +
                         " Voting Results\n" +
                         HORIZONTAL_RULE +
                         "\nA total of %d votes were received...\n" +
                         " - %,9d Accepted\n" +
                         " - %,9d Rejected (Invalid Contestant)\n" +
                         " - %,9d Rejected (Maximum Vote Count Reached)\n" +
                         " - %,9d Failed (Transaction Error)\n\n";
        /*System.out.printf(display, stats.getInvocationsCompleted(),
                acceptedVotes.get(), badContestantVotes.get(),
                badVoteCountVotes.get(), failedVotes.get());*/

        // 2. Voting results
        /*VoltTable result = client.callProcedure("Results").getResults()[0];

        System.out.println("Contestant Name\t\tVotes Received");
        while(result.advanceRow()) {
            System.out.printf("%s\t\t%,14d\n", result.getString(0), result.getLong(2));
        }
        System.out.printf("\nThe Winner is: %s\n\n", result.fetchRow(0).getString(0));

        // 3. Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        if(this.config.latencyreport) {
            System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
            System.out.printf("10th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.1));
            System.out.printf("25th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.25));
            System.out.printf("50th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.5));
            System.out.printf("75th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.75));
            System.out.printf("90th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.9));
            System.out.printf("95th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.95));
            System.out.printf("99th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.99));
            System.out.printf("99.5th percentile latency:     %,9d ms\n", stats.kPercentileLatency(.995));
            System.out.printf("99.9th percentile latency:     %,9d ms\n", stats.kPercentileLatency(.999));

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" System Server Statistics");
            System.out.println(HORIZONTAL_RULE);
            System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" Latency Histogram");
            System.out.println(HORIZONTAL_RULE);
            System.out.println(stats.latencyHistoReport());
        }*/

    }

    public Runnable getDeleterRunnable() {
        return new Runnable() {
            @Override
            public void run() {
                long[] updateData = ctb.deleteSomeTuples(nonInsertsClient, partitionData);
                long nowTuplesDeleted = updateData[0];
                long partitionsThatStoppedEarly = updateData[1];

                totalDeletes.addAndGet(nowTuplesDeleted);
                deletesSinceLastChecked.addAndGet(nowTuplesDeleted);

                try {
                    if (partitionsThatStoppedEarly > 0) {
                        scheduler.execute(getDeleterRunnable());
                    }
                    else {
                        scheduler.schedule(getDeleterRunnable(), 100, TimeUnit.MILLISECONDS);
                    }
                }
                catch (RejectedExecutionException e) {
                    // ignore this... presumably the executor service has shutdown
                }
            }
        };
    }

    class InsertCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            assert(clientResponse.getStatus() == ClientResponse.SUCCESS);
        }
    }

    void insertRandomRow() {
        // unique identifier and partition key
        String uuid = UUID.randomUUID().toString();

        // millisecond timestamp
        Date now = new Date();

        // for some odd reason, this will give LONG_MAX if the
        // computed value is > LONG_MAX.
        long val = (long) (rand.nextGaussian() * 1000.0);

        try {
            insertsClient.callProcedure(new InsertCallback(),
                                 "TIMEDATA.insert",
                                 uuid,
                                 val,
                                 now);
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
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
        connect(insertsClient, config.servers);
        connect(nonInsertsClient, config.servers);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // reset the stats
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // record the start of the benchmark
        benchmarkStartTS = System.currentTimeMillis();

        // print periodic statistics to the console
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                printStatistics();
            }
        }, config.displayinterval, config.displayinterval, TimeUnit.SECONDS);

        // update the partition key set, row counts and redundancy level once per second
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                partitionData.update();
            }
        }, 1, 1, TimeUnit.SECONDS);

        // delete data as often as need be
        //  -- this will resubmit at varying rates according to insert load
        scheduler.execute(getDeleterRunnable());

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        System.out.println("\nRunning benchmark...");
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {
            insertRandomRow();
        }

        scheduler.shutdown();
        try {
            scheduler.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // block until all outstanding txns return
        insertsClient.drain();
        nonInsertsClient.drain();

        // print the summary results
        printResults();

        // close down the client connections
        insertsClient.close();
        nonInsertsClient.drain();
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
