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
/*
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the VoltDB JDBC client
 * library.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */

package voter;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.jdbc.IVoltDBConnection;

public class JDBCBenchmark {

    // Initialize some common constants and variables
    static final String CONTESTANT_NAMES_CSV = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway,"
            + "Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster,"
            + "Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli";

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE = "----------" + "----------"
            + "----------" + "----------" + "----------" + "----------"
            + "----------" + "----------" + "\n";

    // validated command line configuration
    final VoterConfig config;
    // Reference to the database connection we will use
    Connection client;
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
    ClientStatsContext periodicStatsContext;
    ClientStatsContext fullStatsContext;

    // voter benchmark state
    AtomicLong acceptedVotes = new AtomicLong(0);
    AtomicLong badContestantVotes = new AtomicLong(0);
    AtomicLong badVoteCountVotes = new AtomicLong(0);
    AtomicLong failedVotes = new AtomicLong(0);

    /**
     * Uses included {@link CLIConfig} class to declaratively state command line
     * options with defaults and validation.
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

        @Override
        public void validate() {
            if (duration <= 0)
                exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0)
                exitWithMessageAndUsage("warmup must be >= 0");
            if (duration < 0)
                exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0)
                exitWithMessageAndUsage("displayinterval must be > 0");
            if (contestants <= 0)
                exitWithMessageAndUsage("contestants must be > 0");
            if (maxvotes <= 0)
                exitWithMessageAndUsage("maxvotes must be > 0");
            if (threads <= 0)
                exitWithMessageAndUsage("threads must be > 0");
        }
    }

    /**
     * Provides a callback to be notified on node failure. This example only
     * logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port,
                int connectionsLeft, DisconnectCause cause) {
            // if the benchmark is still active
            if (benchmarkComplete.get() == false) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname,
                        port);
            }
        }
    }

    /**
     * Constructor for benchmark instance. Configures VoltDB client and prints
     * configuration.
     *
     * @param config
     *            Parsed & validated CLI options.
     */
    public JDBCBenchmark(VoterConfig config) {
        this.config = config;

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
     * @param servers
     *            A comma separated list of servers using the hostname:port
     *            syntax (where :port is optional).
     * @throws InterruptedException
     *             if anything bad happens with the threads.
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    void connect(String servers) throws InterruptedException,
            ClassNotFoundException, SQLException {
        System.out.println("Connecting to VoltDB...");

        // We need only do this once, to "hot cache" the JDBC driver reference
        // so the JVM may realize it's there.
        Class.forName("org.voltdb.jdbc.Driver");

        // Prepare the JDBC URL for the VoltDB driver
        String url = "jdbc:voltdb://" + config.servers;

        client = DriverManager.getConnection(url, "", "");

        periodicStatsContext = ((IVoltDBConnection) client)
                .createStatsContext();
        fullStatsContext = ((IVoltDBConnection) client).createStatsContext();
    }

    /**
     * Create a Timer task to display performance data on the Vote procedure It
     * calls printStatistics() every displayInterval seconds
     */
    public void schedulePeriodicStats() {
        timer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() {
                printStatistics();
            }
        };
        timer.scheduleAtFixedRate(statsPrinting, config.displayinterval * 1000,
                config.displayinterval * 1000);
    }

    /**
     * Prints a one line update on performance that can be printed periodically
     * during a benchmark.
     */
    public synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline()
                .getStats();
        long time = Math
                .round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60,
                time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/95%% Latency %.2f/%.2fms\n",
                stats.getAverageLatency(), stats.kPercentileLatencyAsDouble(0.95));
    }

    /**
     * Prints the results of the voting simulation and statistics about
     * performance.
     *
     * @throws Exception
     *             if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // 1. Voting Board statistics, Voting results and performance statistics
        String display = "\n" + HORIZONTAL_RULE + " Voting Results\n"
                + HORIZONTAL_RULE + "\nA total of %d votes were received...\n"
                + " - %,9d Accepted\n"
                + " - %,9d Rejected (Invalid Contestant)\n"
                + " - %,9d Rejected (Maximum Vote Count Reached)\n"
                + " - %,9d Failed (Transaction Error)\n\n";
        System.out.printf(display, stats.getInvocationsCompleted(),
                acceptedVotes.get(), badContestantVotes.get(),
                badVoteCountVotes.get(), failedVotes.get());

        // 2. Voting results
        final CallableStatement resultsCS = client
                .prepareCall("{call Results}");
        ResultSet result = resultsCS.executeQuery();
        String winner = "";
        long winnerVoteCount = 0;

        System.out.println("Contestant Name\t\tVotes Received");
        while (result.next()) {
            if (result.getLong(3) > winnerVoteCount) {
                winnerVoteCount = result.getLong(3);
                winner = result.getString(1);
            }
            System.out.printf("%s\t\t%,14d\n", result.getString(1),
                    result.getLong(3));
        }
        System.out.printf("\nThe Winner is: %s\n\n", winner);

        // 3. Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n",
                stats.getTxnThroughput());
        System.out.printf("Average latency:               %,9.2f ms\n",
                stats.getAverageLatency());
        System.out.printf("95th percentile latency:       %,9.2f ms\n",
                stats.kPercentileLatencyAsDouble(.95));
        System.out.printf("99th percentile latency:       %,9.2f ms\n",
                stats.kPercentileLatencyAsDouble(.99));

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n",
        stats.getAverageInternalLatency());

        // 4. Write stats to file if requested
        ((IVoltDBConnection)client).writeSummaryCSV(stats, config.statsfile);
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
                    final CallableStatement voteCS = client
                            .prepareCall("{call Vote(?,?,?)}");
                    voteCS.setLong(1, call.phoneNumber);
                    voteCS.setInt(2, call.contestantNumber);
                    voteCS.setLong(3, config.maxvotes);
                } catch (Exception e) {
                }
            }

            while (benchmarkComplete.get() == false) {
                // Get the next phone call
                PhoneCallGenerator.PhoneCall call = switchboard.receive();

                // synchronously call the "Vote" procedure
                try {

                    final CallableStatement voteCS = client
                            .prepareCall("{call Vote(?,?,?)}");
                    voteCS.setLong(1, call.phoneNumber);
                    voteCS.setInt(2, call.contestantNumber);
                    voteCS.setLong(3, config.maxvotes);

                    try {
                        voteCS.executeUpdate();
                        acceptedVotes.incrementAndGet();
                    } catch (Exception x) {
                        badVoteCountVotes.incrementAndGet();
                    }
                } catch (Exception e) {
                    failedVotes.incrementAndGet();
                }
            }

        }

    }

    /**
     * Core benchmark code. Connect. Initialize. Run the loop. Cleanup. Print
     * Results.
     *
     * @throws Exception
     *             if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        // connect to one or more servers, loop until success
        connect(config.servers);

        // initialize using synchronous call
        // Initialize the application
        System.out.println("\nPopulating Static Tables\n");
        final CallableStatement initializeCS = client
                .prepareCall("{call Initialize(?,?)}");
        initializeCS.setInt(1, config.contestants);
        initializeCS.setString(2, CONTESTANT_NAMES_CSV);
        initializeCS.executeUpdate();

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Starting Benchmark");
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
        // client.drain();

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
     * @param args
     *            Command line arguments.
     * @throws Exception
     *             if anything goes wrong.
     * @see {@link VoterConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        VoterConfig config = new VoterConfig();
        config.parse(JDBCBenchmark.class.getName(), args);

        JDBCBenchmark benchmark = new JDBCBenchmark(config);
        benchmark.runBenchmark();
    }
}
