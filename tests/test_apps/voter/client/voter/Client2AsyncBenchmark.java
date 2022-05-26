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
 * This is a version of the AsyncBenchmark code,
 * modified to use the in-development Client2 API.
 *
 * Client2 provides a more functional asynchronous
 * API. Queueing is incorporated into the API, so
 * that transport level backpressure can be handled
 * internally. The application is not notified
 * until and unless a specified queue length is
 * exceeded. The notification is a warning to slow
 * down; requests are not rejected until a second,
 * larger limit is breached.
 */

package voter;

import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientAffinityStats;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsUtil;
import org.voltdb.client.ClientStatsContext;

public class Client2AsyncBenchmark {

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
        int duration = 20;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 2;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of contestants in the voting contest (from 1 to 10).")
        int contestants = 6;

        @Option(desc = "Maximum number of votes cast per voter.")
        int maxvotes = 2;

        @Option(desc = "Maximum TPS rate for benchmark (default is no rate limiting).")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Report latency for async benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "Report client affinity stats for async benchmark run.")
        boolean affinityreport = false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";

        @Option(desc = "Enable SSL, optionally provide configuration file.")
        String sslfile = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (contestants <= 0) exitWithMessageAndUsage("contestants must be > 0");
            if (maxvotes <= 0) exitWithMessageAndUsage("maxvotes must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
        }
    }

    /**
     * Flow control. Gate opens and shuts in response
     * to backpressure notifications from the VoltDB
     * client.
     */
    static class Gate {
        // state
        boolean closed;
        long closedAt;

        // stats
        int closeCount;
        long totalWait;
        long minWait = Integer.MAX_VALUE;
        long maxWait = Integer.MIN_VALUE;

        synchronized void waitOpen() throws InterruptedException {
            if (closed) {
                long wt = System.nanoTime();
                while (closed)
                    wait();
                wt = System.nanoTime() - wt;
                totalWait += wt;
                if (wt > maxWait) maxWait = wt;
                if (wt < minWait) minWait = wt;
            }
        }

        synchronized void operate(boolean closing) {
            if (closing ^ closed) {
                closed = closing;
                if (closed) {
                    closedAt = System.nanoTime();
                    closeCount++;
                }
                else {
                    notifyAll();
                }
            }
        }
    }

    Gate gate;

    /*
     * Client operational parameters. See Client2Config
     * for full descriptions.
     */
    static final int OUTSTANDING_TXN_LIMIT = 1500;
    static final int YELLOW_REQ_LIMIT = 3000;
    static final int RED_REQ_LIMIT = YELLOW_REQ_LIMIT + 1000;
    static final int REQ_RESUME_LEVEL = 2500;

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public Client2AsyncBenchmark(VoterConfig config) {
        this.config = config;


        Client2Config clientConfig = new Client2Config()
            .username(config.user)
            .password(config.password)
            .outstandingTransactionLimit(OUTSTANDING_TXN_LIMIT)
            .clientRequestBackpressureLevel(YELLOW_REQ_LIMIT, REQ_RESUME_LEVEL)
            .clientRequestLimit(RED_REQ_LIMIT)
            .requestBackpressureHandler((s) -> gate.operate(s))
            .connectionUpHandler((h,p) -> System.out.printf("[up: %s %d]%n", h, p))
            .connectionDownHandler((h,p) -> System.out.printf("[down: %s %d]%n", h, p))
            .connectFailureHandler((h,p) -> System.out.printf("[fail: %s %d]%n", h, p));

        if (config.ratelimit != Integer.MAX_VALUE) {
            clientConfig.transactionRateLimit(config.ratelimit);
        }

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
        //if(config.latencyreport) {
        //    System.out.println("NOTICE: Option latencyreport is ON for async run, please set a reasonable ratelimit.\n");
        //}
    }

    /**
     * Connect to any one of a list of servers. We return when one connection is
     * established; the topology-aware feature will take it from there.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     */
    void connect(String servers) throws IOException {
        System.out.println("Connecting to VoltDB ...");
        client.connectSync(servers, 300, 5, TimeUnit.SECONDS);
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
        System.out.printf("Throughput %d/s", stats.getTxnThroughput());
        int rc = client.currentRequestCount();
        if (rc != 0) {
            System.out.printf(", %d req", rc);
        }
        int ot = client.outstandingTxnCount();
        if (ot != 0) {
            System.out.printf(", %d out", ot);
        }
        boolean cl = gate.closed;
        if (cl) {
            long wt = System.nanoTime() - gate.closedAt;
            if (wt >= 1000000) // at least 1ms
                System.out.printf(", closed %dms", Math.round(wt / 1000000.0));
            else
                System.out.printf(", closed %d\u00b5s", Math.round(wt / 1000.0));
        }

        System.out.printf("\n");
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStatsContext statsCtx = fullStatsContext.fetch();
        ClientStats stats = statsCtx.getStats();

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
        if (gate.closeCount != 0) {
            System.out.printf("Wait count:                    %,9d\n", gate.closeCount);
            System.out.printf("Average wait:                  %,9d \u00b5sec\n", Math.round((gate.totalWait / gate.closeCount) / 1000.0));
            System.out.printf("Minimum wait:                  %,9d \u00b5sec\n", Math.round(gate.minWait / 1000.0));
            System.out.printf("Maximum wait:                  %,9d \u00b5sec\n", Math.round(gate.maxWait / 1000.0));
        }

        if (this.config.latencyreport) {
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
        }

        // 4. Write stats to file if requested
        if (!config.statsfile.isEmpty()) {
            ClientStatsUtil.writeSummaryCSV(stats, config.statsfile);
        }

        // 5. Affinity stats
        Map<Integer,ClientAffinityStats> casMap = statsCtx.getAffinityStats();
        if (config.affinityreport && !casMap.isEmpty()) {
            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" Client Affinity Statistics");
            System.out.println(HORIZONTAL_RULE);
            System.out.printf("%10s %21s %21s%n", "",
                              "Affinity    ", "Round Robin   ");
            System.out.printf("%10s %10s %10s %10s %10s%n", "Partition",
                              "Reads", "Writes", "Reads", "Writes");
            for (Map.Entry<Integer,ClientAffinityStats>  ent : casMap.entrySet()) {
                int partitionId = ent.getKey();
                ClientAffinityStats cas = ent.getValue();
                System.out.printf("%10d %,10d %,10d %,10d %,10d%n", partitionId,
                                  cas.getAffinityReads(), cas.getAffinityWrites(),
                                  cas.getRrReads(), cas.getRrWrites());
            }
        }

        // That's all
        System.out.println();
    }

    /**
     * Handle the response to a stored procedure call.
     * Tracks response types.
     */
    public Void callComplete(ClientResponse response) {
        totalVotes.incrementAndGet();
        if (response.getStatus() == ClientResponse.SUCCESS) {
            long resultCode = response.getResults()[0].asScalarLong();
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
        else {
            failedVotes.incrementAndGet();
        }
        return null;
    }

    /**
     * Likewise, but in the warmup period. We don't
     * track anything here.
     */
    public Void warmupComplete(ClientResponse response) {
        return null;
    }

    /**
     * Any procedure call failure (exceptional completion case).
     * Handling is primitive just write a message and forget it.
     *
     * This does not include the cases where a CallResponse is
     * received with a non-success status.
     */
    public Void callFailed(Throwable th) {
        System.err.printf("Procedure call failed: %s%n", th);
        return null;
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

        // connect to first available server, loop until success
        connect(config.servers);

        // initialize using synchronous call
        System.out.println("\nPopulating Static Tables\n");
        client.callProcedureSync("Initialize", config.contestants, CONTESTANT_NAMES_CSV);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        System.out.println("Warming up...");
        gate = new Gate();

        final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
        while (warmupEndTime > System.currentTimeMillis()) {
            // Get the next phone call
            PhoneCallGenerator.PhoneCall call = switchboard.receive();

            // if backpressured, wait
            gate.waitOpen();

            // asynchronously call the "Vote" procedure
            client.callProcedureAsync("Vote",
                                      call.phoneNumber,
                                      call.contestantNumber,
                                      config.maxvotes)
                .thenAccept(this::warmupComplete)
                .exceptionally(this::callFailed);
        }

        // block until all warmup txns return
        client.drain();

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        System.out.println("\nRunning benchmark...");
        gate = new Gate();

        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {
            // Get the next phone call
            PhoneCallGenerator.PhoneCall call = switchboard.receive();

            // if backpressured, wait
            gate.waitOpen();

            // asynchronously call the "Vote" procedure
            client.callProcedureAsync("Vote",
                                      call.phoneNumber,
                                      call.contestantNumber,
                                      config.maxvotes)
                .thenAccept(this::callComplete)
                .exceptionally(this::callFailed);
        }

        // cancel periodic stats printing
        timer.cancel();
        System.out.println();

        // block until all outstanding txns return
        client.drain();

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
        config.parse(Client2AsyncBenchmark.class.getName(), args);

        Client2AsyncBenchmark benchmark = new Client2AsyncBenchmark(config);
        benchmark.runBenchmark();
    }
}
