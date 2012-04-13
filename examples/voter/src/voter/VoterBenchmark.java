/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
 * model to VoltDB as it guarantees blazing performance.
 *
 * Because there is a risk of 'firehosing' a database cluster (if the
 * cluster is too slow (slow or too few CPUs), this sample performs
 * auto-tuning to target a specific latency (5ms by default).
 * This tuning process, as demonstrated here, is important and should be
 * part of your pre-launch evaluation so you can adequately provision your
 * VoltDB cluster with the number of servers required for your needs.
 */

package voter;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.utils.MiscUtils;

import voter.procedures.Vote;

public class VoterBenchmark {

    // Initialize some common constants and variables
    static final String CONTESTANT_NAMES_CSV =
            "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway," +
            "Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster," +
            "Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli";

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "-------------------------------------------" +
            "------------------------------------------\n";

    // validated command line configuration
    final VoterConfig config;
    // Reference to the database connection we will use
    final Client client;
    // Phone number generator
    PhoneCallGenerator switchboard;
    // Callback for asynchronous "Vote" procedure calls
    VoterCallback callback = new VoterCallback();
    // Timer for periodic stats printing
    Timer timer;

    // voter benchmark state
    AtomicLong acceptedVotes = new AtomicLong(0);
    AtomicLong badContestantVotes = new AtomicLong(0);
    AtomicLong badVoteCountVotes = new AtomicLong(0);
    AtomicLong failedVotes = new AtomicLong(0);

    /**
     * Uses included {@link Configuration} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class VoterConfig extends Configuration {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 30;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of contestants in the voting contest (from 1 to 10).")
        int contestants = 6;

        @Option(desc = "Interval for performance feedback, in seconds.")
        int maxvotes = 10;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = 100000;

        @Option(desc = "Determine transaction rate dynamically based on latency.")
        boolean autotune = true;

        @Option(desc = "Server-side latency target for auto-tuning.")
        int latencytarget = 5;

        @Option(desc = "Filename to write raw summary statistics to.")
        String stats = null;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("display-interval must be > 0");
            if (contestants <= 0) exitWithMessageAndUsage("contestants must be > 0");
            if (maxvotes <= 0) exitWithMessageAndUsage("max-votes must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("rate-limit must be > 0");
            if (latencytarget <= 0) exitWithMessageAndUsage("latency-target must be > 0");
        }
    }

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            System.out.printf("Connection to %s:%d was lost.\n", hostname, port);
        }
    }

    /**
     * Callback to handle the response to a stored procedure call.
     * Tracks response types.
     *
     */
    class VoterCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() == ClientResponse.SUCCESS) {
                long resultCode = response.getResults()[0].asScalarLong();
                if (resultCode == Vote.ERR_INVALID_CONTESTANT) {
                    badContestantVotes.incrementAndGet();
                }
                else if (resultCode == Vote.ERR_VOTER_OVER_VOTE_LIMIT) {
                    badVoteCountVotes.incrementAndGet();
                }
                else {
                    assert(resultCode == Vote.VOTE_SUCCESSFUL);
                    acceptedVotes.incrementAndGet();
                }
            }
            else {
                failedVotes.incrementAndGet();
            }
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public VoterBenchmark(VoterConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
        if (config.autotune) {
            clientConfig.enableAutoTune();
            clientConfig.setAutoTuneTargetInternalLatency(config.latencytarget);
        }
        else {
            clientConfig.setMaxTransactionsPerSecond(config.ratelimit);
        }
        client = ClientFactory.createClient(clientConfig);
        switchboard = new PhoneCallGenerator(config.contestants);

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
        int port = MiscUtils.getPortFromHostnameColonPort(server, Client.VOLTDB_SERVER_PORT);
        server = MiscUtils.getHostnameFromHostnameColonPort(server);

        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server, port);
                break;
            }
            catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
        System.out.printf("Connected to %s on port %d.\n", server, port);
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
        ClientStats stats = client.getStats(true, true, true)[0];
        System.err.println(stats.benchmarkUpdate(System.currentTimeMillis()));
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = client.getStats(false, true, true)[0];

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
        System.out.printf(display, stats.invocationsCompleted,
                acceptedVotes.get(), badContestantVotes.get(),
                badVoteCountVotes.get(), failedVotes.get());

        // 2. Voting results
        VoltTable result = client.callProcedure("Results").getResults()[0];

        System.out.println("Contestant Name\t\tVotes Received");
        while(result.advanceRow()) {
            System.out.printf("%s\t\t%,14d\n", result.getString(0), result.getLong(2));
        }
        System.out.printf("\nThe Winner is: %s\n\n", result.fetchRow(0).getString(0));

        // 3. Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" System Statistics");
        System.out.println(HORIZONTAL_RULE);

        long now = System.currentTimeMillis();
        System.out.printf("For %.1f seconds, an average throughput of %d txns/sec was sustained.\n",
                (now - stats.since) / 1000.0, stats.throughput(now));
        System.out.printf("Average latency was %d ms per procedure.\n", stats.averageLatency());
        System.out.printf("Average internal latency, as reported by the server(s) was %d ms.\n", stats.averageInternalLatency());
        System.out.printf("Measured 95th and 99th percentile latencies were %d and %d ms respectively\n",
                stats.kPercentileLatency(.95), stats.kPercentileLatency(.99));

        // 4. Write stats to file if requested
        if ((config.stats != null) && (config.stats.length() > 0)) {
            client.writeSummaryCSV(config.stats);
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
        client.callProcedure("Initialize", config.contestants, CONTESTANT_NAMES_CSV);

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // print periodic statistics to the console
        schedulePeriodicStats();

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        final long endTime = System.currentTimeMillis() + (1000l * config.duration);
        while (endTime > System.currentTimeMillis()) {
            // Get the next phone call
            PhoneCallGenerator.PhoneCall call = switchboard.receive();

            // asynchronously call the "Vote" procedure
            client.callProcedure(callback,
                                 "Vote",
                                 call.phoneNumber,
                                 call.contestantNumber,
                                 config.maxvotes);
        }

        // cancel periodic stats printing
        timer.cancel();

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
        config.parse(args);

        VoterBenchmark benchmark = new VoterBenchmark(config);
        benchmark.runBenchmark();
    }
}
