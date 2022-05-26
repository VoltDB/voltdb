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
 * This is a version of the SyncBenchmark code,
 * modified to use the in-development Client2 API.
 */

package voter;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.CLIConfig.Option;
import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatsUtil;

public class Client2SyncBenchmark {

    // Initialize some common constants and variables
    static final String CONTESTANT_NAMES_CSV =
            "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway," +
            "Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster," +
            "Kurt Walser,Ericka Dieter,Loraine Nygren,Tania Mattioli";

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // potential return codes (synced with Vote procedure)
    static final long VOTE_SUCCESSFUL = 0;
    static final long ERR_INVALID_CONTESTANT = 1;
    static final long ERR_VOTER_OVER_VOTE_LIMIT = 2;

    // validated command line configuration
    final VoterConfig config;
    // Reference to the database connection we will use
    final Client2 client;
    // Phone number generator
    PhoneCallGenerator switchboard;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    // voter benchmark state
    AtomicLong totalVotes = new AtomicLong(0);
    AtomicLong acceptedVotes = new AtomicLong(0);
    AtomicLong badContestantVotes = new AtomicLong(0);
    AtomicLong badVoteCountVotes = new AtomicLong(0);
    AtomicLong failedVotes = new AtomicLong(0);

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class VoterConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 120;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 5;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of contestants in the voting contest (from 1 to 10).")
        int contestants = 6;

        @Option(desc = "Maximum number of votes cast per voter.")
        int maxvotes = 2;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Number of concurrent threads synchronously calling procedures.")
        int threads = 40;

        @Option(desc = "SSL Configuration file")
        String sslfile = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (duration < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (contestants <= 0) exitWithMessageAndUsage("contestants must be > 0");
            if (maxvotes <= 0) exitWithMessageAndUsage("maxvotes must be > 0");
            if (threads <= 0) exitWithMessageAndUsage("threads must be > 0");
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public Client2SyncBenchmark(VoterConfig config) {
        this.config = config;

        Client2Config clientConfig = new Client2Config()
            .connectionUpHandler((h,p) -> System.out.printf("[up: %s %d]%n", h, p))
            .connectionDownHandler((h,p) -> System.out.printf("[down: %s %d]%n", h, p))
            .connectFailureHandler((h,p) -> System.out.printf("[fail: %s %d]%n", h, p));

        if (!config.sslfile.trim().isEmpty()) {
            clientConfig.trustStoreFromPropertyFile(config.sslfile)
                        .enableSSL();
        }

        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        switchboard = new PhoneCallGenerator(config.contestants);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
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
                    try {
                        client.connectSync(server, 120, 5, TimeUnit.SECONDS);
                    }
                    catch (IOException ex) {
                        System.err.printf("Connection to %s failed: %s\n", server, ex.getMessage());
                    }
                    finally {
                        connections.countDown();
                    }
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

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/95%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.95));
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
                         "\nA total of %,9d votes were received during the benchmark...\n" +
                         " - %,9d Accepted\n" +
                         " - %,9d Rejected (Invalid Contestant)\n" +
                         " - %,9d Rejected (Maximum Vote Count Reached)\n" +
                         " - %,9d Failed (Transaction Error)\n\n";
        System.out.printf(display, totalVotes.get(),
                acceptedVotes.get(), badContestantVotes.get(),
                badVoteCountVotes.get(), failedVotes.get());

        // 2. Voting results
        VoltTable result = client.callProcedureSync("Results").getResults()[0];

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
        System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
        System.out.printf("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1));
        System.out.printf("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25));
        System.out.printf("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5));
        System.out.printf("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75));
        System.out.printf("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9));
        System.out.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
        System.out.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));
        System.out.printf("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995));
        System.out.printf("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999));

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" Latency Histogram");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(stats.latencyHistoReport());

        // 4. Write stats to file if requested
        if (!config.statsfile.isEmpty()) {
            ClientStatsUtil.writeSummaryCSV(stats, config.statsfile);
        }
    }

    /**
     * While <code>benchmarkComplete</code> is set to false, run as many
     * synchronous procedure calls as possible and record the results.
     *
     */
    class VoterThread implements Runnable {

        @Override
        public void run() {
            while (warmupComplete.get() == false) {
                // Get the next phone call
                PhoneCallGenerator.PhoneCall call = switchboard.receive();

                // synchronously call the "Vote" procedure
                try {
                    client.callProcedureSync("Vote", call.phoneNumber,
                            call.contestantNumber, config.maxvotes);
                }
                catch (Exception e) {}
            }

            while (benchmarkComplete.get() == false) {
                // Get the next phone call
                PhoneCallGenerator.PhoneCall call = switchboard.receive();

                // synchronously call the "Vote" procedure
                try {
                    ClientResponse response = client.callProcedureSync("Vote",
                                                                       call.phoneNumber,
                                                                       call.contestantNumber,
                                                                       config.maxvotes);

                    long resultCode = response.getResults()[0].asScalarLong();
                    totalVotes.incrementAndGet();
                    if (resultCode == ERR_INVALID_CONTESTANT) {
                        badContestantVotes.incrementAndGet();
                    }
                    else if (resultCode == ERR_VOTER_OVER_VOTE_LIMIT) {
                        badVoteCountVotes.incrementAndGet();
                    }
                    else {
                        assert(resultCode == VOTE_SUCCESSFUL);
                        acceptedVotes.incrementAndGet();
                    }
                }
                catch (Exception e) {
                    failedVotes.incrementAndGet();
                }
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

        // initialize using synchronous call
        System.out.println("\nPopulating Static Tables\n");
        client.callProcedureSync("Initialize", config.contestants, CONTESTANT_NAMES_CSV);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // create/start the requested number of threads
        Thread[] voterThreads = new Thread[config.threads];
        for (int i = 0; i < config.threads; ++i) {
            voterThreads[i] = new Thread(new VoterThread());
            voterThreads[i].start();
        }

        // Run the benchmark loop for the requested warmup time
        System.out.println("Warming up...");
        Thread.sleep(1000l * config.warmup);

        // signal to threads to end the warmup phase
        warmupComplete.set(true);

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        // Run the benchmark loop for the requested warmup time
        System.out.println("\nRunning benchmark...");
        Thread.sleep(1000l * config.duration);

        // stop the threads
        benchmarkComplete.set(true);

        // cancel periodic stats printing
        timer.cancel();

        // block until all outstanding txns return
        client.drain();

        // join on the threads
        for (Thread t : voterThreads) {
            t.join();
        }

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
        // create a configuration from the arguments
        VoterConfig config = new VoterConfig();
        config.parse(Client2SyncBenchmark.class.getName(), args);

        Client2SyncBenchmark benchmark = new Client2SyncBenchmark(config);
        benchmark.runBenchmark();
    }
}
