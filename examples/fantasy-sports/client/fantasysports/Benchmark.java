/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package fantasysports;

import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NullCallback;

import bankfraud.FraudBenchmark.BenchmarkCallback;

public class Benchmark {

    // handy, rather than typing this out several times
    public static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final FantasySportsConfig config;
    // Reference to the database connection we will use
    final Client client;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    // CONSTANTS
    private int footballPlayers = 300;
    private int userCount = 1000000;
    private int largeContests = 1;
    private int smallContests = 0;
    private int threads = 1;

    private Random rand = new Random();

    /**
     * Prints headings
     */
    public static void printHeading(String heading) {
        System.out.print("\n"+HORIZONTAL_RULE);
        System.out.println(" " + heading);
        System.out.println(HORIZONTAL_RULE);
    }

    /**
     * Uses CLIConfig class to declaratively state command line options
     * with defaults and validation.
     */
    public static class FantasySportsConfig extends CLIConfig {

        // STANDARD BENCHMARK OPTIONS
        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Volt user name")
        public String user = "";

        @Option(desc = "Volt password")
        public String password = "";

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 120;

        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 5;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        // CUSTOM OPTIONS
        @Option(desc = "Number of customers to generate")
        int custcount = 500000;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
        }
    }

    public Benchmark(FantasySportsConfig config) throws Exception {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig(config.user, config.password, new StatusListener());
        clientConfig.setMaxTransactionsPerSecond(config.ratelimit);
        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        printHeading("Command Line Configuration");
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

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());

        // cast to stats.getAverageLatency from long to double
        System.out.printf("Avg/95%% Latency %.2f/%dms\n",
                          stats.getAverageLatency(),
                          stats.kPercentileLatency(0.95));

    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        printHeading("Transaction Results");
        BenchmarkCallback.printAllResults();

        ClientStats stats = fullStatsContext.fetch().getStats();

        // 3. Performance statistics
        printHeading("Client Workload Statistics");

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
        System.out.printf("95th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.95));
        System.out.printf("99th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.99));

        printHeading("System Server Statistics");

        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

        // 4. Write stats to file if requested
        client.writeSummaryCSV(stats, config.statsfile);
    }

    public void init() throws Exception {

        // generate large contests
        System.out.println("Generating " + largeContests + " large contests...");
        for (int i = 0; i < largeContests; i++) {
            client.callProcedure(new NullCallback(), "FOOTBALL_CONTEST_LARGE.upsert", i, 1);
        }

        // generate small contests
        for (int i=0; i<smallContests; i++) {
            client.callProcedure(new NullCallback(), "FOOTBALL_CONTEST_SMALL.upsert", i, 1);
        }

        // generate users
        System.out.println("Generating " + userCount + " users...");
        for (int i = 0; i < userCount; i++) {
            client.callProcedure(new BenchmarkCallback("FANTASY_USER.upsert"),
                                 "FANTASY_USER.upsert",
                                 i,
                                 "User " + i
                                 );

            // TODO: roster may need to be a different table
            /*int smallContest = rand.nextInt(smallContests);
            // each user has 9 (randomly selected) players on their roster
            for (int j = 0; j < 9; j++) {
                client.callProcedure(new NullCallback(), "USER_CONTEST_ROSTER.upsert",
                                     smallContest, i, rand.nextInt(footballPlayers));
            }*/

            int largeContest = rand.nextInt(largeContests);
            // each user has 9 (randomly selected) players on their roster
            for (int j = 0; j < 9; j++) {
                client.callProcedure(new NullCallback(), "USER_CONTEST_ROSTER.upsert",
                                     largeContest, i, rand.nextInt(footballPlayers), 0);
            }
        }


    }

    public void runBenchmark() throws Exception {
        printHeading("Setup & Initialization");

        // connect to one or more servers, loop until success
        connect(config.servers);

        // initialize using synchronous call
        System.out.println("\nPre-loading Tables...\n");
        init();

        // for Run Everywhere procedures
        ArrayList<Integer> partitionKeys = new ArrayList<Integer>();
        VoltTable partitions = client.callProcedure("@GetPartitionKeys","INTEGER").getResults()[0];
        while (partitions.advanceRow()) {
            int p = (int)partitions.getLong(1);
            partitionKeys.add(p);
            System.out.println("Partition " + partitions.getLong(0) + " has key " + p);
        }

        // Run Ranker threads (see below) in a pool
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        for (int i=0; i<1; i++) {

            // generate player stats
            System.out.println("Updating football player stats...");
            for (int p=0; p<footballPlayers; p++) {
                client.callProcedure(new BenchmarkCallback("FOOTBALL_PLAYER_GAME_SCORE.upsert"),
                                     "FOOTBALL_PLAYER_GAME_SCORE.upsert",
                                     p,
                                     1,
                                     rand.nextInt(50)
                                     );
            }


            // run everywhere
            // for (int partVal : partitionKeys) {
            //     client.callProcedure(new BenchmarkCallback("SelectAllScoresInPartition"),
            //                          "SelectAllScoresInPartition",
            //                          partVal);
            // }
            // for (int partVal : partitionKeys) {
            //     for (int c=0; c<largeContests; c++) {

            //         client.callProcedure(new BenchmarkCallback("SelectContestScoresInPartition"),
            //                              "SelectContestScoresInPartition",
            //                              partVal,
            //                              c);

                    // client.callProcedure(new BenchmarkCallback("UpsertUserScores"),
                    //                      "UpsertUserScores",
                    //                      partVal,
                    //                      c);

            //     }
            // }
            client.drain();

            System.out.println("Updating scores and rankings...");

            for (int c = 0; c < largeContests; c++) {
                // run a Ranker
                Runnable r = new Ranker(partitionKeys, c, client);
                //r.run();
                executor.execute(r);
            }
        }

        // stop the executor & wait for any threads to finish
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        client.drain();

        BenchmarkCallback.printAllResults();

        client.close();
    }


    public static void main(String[] args) throws Exception {

        FantasySportsConfig config = new FantasySportsConfig();
        config.parse(Benchmark.class.getName(), args);

        Benchmark benchmark = new Benchmark(config);
        benchmark.runBenchmark();
    }
}
