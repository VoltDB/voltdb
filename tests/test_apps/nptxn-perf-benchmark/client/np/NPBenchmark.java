/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package np;

import java.util.Date;
import java.util.Random;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.TimerTask;

import org.voltdb.*;
import org.voltdb.client.*;

import com.google_voltpatches.common.collect.ConcurrentHashMultiset;
import com.google_voltpatches.common.collect.Multiset;

// Ported and modified from https://github.com/VoltDB/voltdb/pull/3822
class NPBenchmark {

    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final NPBenchmarkConfig config;
    // Reference to the database connection we will use
    final Client client;

    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    Timer timer;
    long benchmarkStartTS;

    private Random rand = new Random();

    static class NPBenchmarkConfig extends CLIConfig {
        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 20;

        @Option(desc = "name of the benchmark to run")
        String name = "NP Benchmark";

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 2;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "stats";

        // New options
        @Option(desc = "Percentage of SP transactions compared to NP txns")
        double sprate = 0.5;

        @Option(desc = "Number of Cards")
        int cardcount = 500000;

        @Option(desc = "Rate of MP txns")
        double mprate = 0.05;

        @Option(desc = "Data skew ratio")
        double skew = 0.2;

        @Override
        public void validate() {
            if (sprate > 1 || sprate < 0) {
                exitWithMessageAndUsage("Invalid sprate...");
            }

            if (cardcount <= 0)
                exitWithMessageAndUsage("Invalid cardcount...");

            if (mprate > 1 || mprate < 0) {
                exitWithMessageAndUsage("Invalid mprate...");
            }

            if (skew < 0 || skew >= 1) {
                exitWithMessageAndUsage("Invalid skew...");
            }

            if (duration <= 0) {
                exitWithMessageAndUsage("Invalid duration...");
            }
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

    public NPBenchmark(NPBenchmarkConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());

        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
    }

    static class ProcCallback implements ProcedureCallback {
        private static Multiset<String> stats =
                                ConcurrentHashMultiset.create();
        String procedureName;
        long maxErrors;

        public static int count( String procedureName, String event ){
            return stats.add(procedureName + event, 1);
        }

        public static int getCount( String procedureName, String event ){
            return stats.count(procedureName + event);
        }

        public static void printProcedureResults(String procedureName) {
            System.out.println("  " + procedureName);
            System.out.println("        calls: " + getCount(procedureName,"call"));
            System.out.println("      commits: " + getCount(procedureName,"commit"));
            System.out.println("    rollbacks: " + getCount(procedureName,"rollback"));
        }

        public ProcCallback(String procedure, long maxErrors) {
            super();
            this.procedureName = procedure;
            this.maxErrors = maxErrors;
        }

        public ProcCallback(String procedure) {
            this(procedure, 25l);
    }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            count(procedureName,"call");

            if (response.getStatus() == ClientResponse.SUCCESS) {
                count(procedureName,"commit");
            } else {
                long totalErrors = count(procedureName,"rollback");

                if (totalErrors > maxErrors) {
                    System.err.println("exceeded " + maxErrors + " maximum database errors - exiting client");
                    System.exit(-1);
                }

                System.err.println("DATABASE ERROR: " + response.getStatusString());
            }
        }
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
                          (double)stats.getAverageLatency(),
                          stats.kPercentileLatency(0.95));

    }

    /**
     * Prints statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        // cast stats.getAverateLatency from long to double
        System.out.printf("Average latency:               %,9.2f ms\n", (double)stats.getAverageLatency());
        //System.out.printf("Average latency:               %,9d ms\n", stats.getAverageLatency());
        System.out.printf("95th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.95));
        System.out.printf("99th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.99));

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        // cast stats.getAverageInternalLatency from long to double
        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", (double)stats.getAverageInternalLatency());
        //System.out.printf("Reported Internal Avg Latency: %,9d ms\n", stats.getAverageInternalLatency());

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" Transaction Results");
        System.out.println(HORIZONTAL_RULE);

        ProcCallback.printProcedureResults("CARD_ACCOUNT.insert");
        ProcCallback.printProcedureResults("Authorize");
        ProcCallback.printProcedureResults("Redeem");
        ProcCallback.printProcedureResults("Transfer");
        ProcCallback.printProcedureResults("Select");

        // Write stats to file if requested
        // client.writeSummaryCSV(stats, config.statsfile);

        FileWriter fw = null;
        if ((config.statsfile != null) && (config.statsfile.length() != 0)) {
            fw = new FileWriter(config.statsfile);

            fw.append(String.format("%d,%d,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%d,%d\n",
                stats.getStartTimestamp(),
                stats.getDuration(),
                stats.getInvocationsCompleted(),
                stats.getTxnThroughput(),
                stats.getAverageLatency(),
                stats.getAverageInternalLatency(),
                stats.kPercentileLatencyAsDouble(0.95),
                stats.kPercentileLatencyAsDouble(0.99),
                stats.kPercentileLatencyAsDouble(0.9999),
                stats.kPercentileLatencyAsDouble(0.99999),
                stats.getInvocationErrors(),
                stats.getInvocationAborts(),
                stats.getInvocationTimeouts()));

            fw.flush();
            fw.close();
        }
    }

    public void initialize() throws Exception {
        System.out.println("Generating " + config.cardcount + " cards...");
        for (int i = 0; i < config.cardcount; i++) {
            // generate a card
            String pan = generate16DString(i);
            Date now = new Date();

            // insert the card
            client.callProcedure(new ProcCallback("CARD_ACCOUNT.insert"),
                                 "CARD_ACCOUNT.insert",
                                 pan,
                                 1, // ACTIVE
                                 "ACTIVATED",
                                 500,
                                 500,
                                 "USD",
                                 now
                                 );
            if (i % 50000 == 0)
                System.out.println("  " + i);

        }
        System.out.println("  " + config.cardcount);
    }

    /**
     * Performs one iteration of the benchmark
     */
    public void iterate() throws Exception {
        double count = ((double) config.cardcount) * (1.0 - config.skew);
        int offset = (int) (((double) config.cardcount) / 2.0 - count / 2.0);

        // SP transaction
        if (rand.nextDouble() < config.sprate) {
            String pan1 = generate16DString(rand.nextInt(config.cardcount));
            String pan2 = generate16DString(rand.nextInt(config.cardcount));

            client.callProcedure(new ProcCallback("Authorize"),
                                 "Authorize",
                                 pan1,
                                 1,
                                 "USD"
                                 );
            client.callProcedure(new ProcCallback("Redeem"),
                                 "Redeem",
                                 pan1,
                                 1,
                                 "USD",
                                 1
                                 );
        } else {
            for (int i = 0; i < 2; i++) {
                // 2P transaction
                String pan1 = generate16DString(rand.nextInt(config.cardcount));
                String pan2 = generate16DString(rand.nextInt((int) count) + offset);    // a smaller range of entities

                client.callProcedure(new ProcCallback("Transfer",10000),
                                        "Transfer",
                                        pan1,
                                        pan2,
                                        rand.nextDouble() < 0.5 ? -1 : 1,   // random transfer direction
                                        "USD"
                                        );

                if (rand.nextDouble() < config.mprate) {
                    // MP transaction
                    int id1 = rand.nextInt(config.cardcount);
                    int id2 = id1 + 2000 < config.cardcount ?
                            id1 + 2000 : config.cardcount - 1;

                    pan1 = generate16DString(id1);
                    pan2 = generate16DString(id2);

                    client.callProcedure(new ProcCallback("Select"),
                                        "Select",
                                        pan1,
                                        pan2);
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
        initialize();

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        System.out.println("Warming up for "+ config.warmup +" seconds...");
        final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
        while (warmupEndTime > System.currentTimeMillis()) {
            iterate();
        }

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        System.out.println("\nRunning benchmark...");
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {
            iterate();
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

    // Generate a string containing 16 digits from the given integer
    private static String generate16DString(int num) {
        String str = Integer.toString(num);
        int num_0 = 16 - str.length();

        char[] zeros = new char[num_0];
        Arrays.fill(zeros, '0');

        return new String(zeros) + str;
    }

    public static void main(String[] args) throws Exception {
        NPBenchmarkConfig config = new NPBenchmarkConfig();
        config.parse(NPBenchmark.class.getName(), args);

        NPBenchmark benchmark = new NPBenchmark(config);
        benchmark.runBenchmark();
    }
}
