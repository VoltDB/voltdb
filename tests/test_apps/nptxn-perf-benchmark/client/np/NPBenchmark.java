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

package np;

import java.util.Date;
import java.util.Random;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;

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
    // Each client is backed by a single thread for execution
    final Client[] clients; // the 1st client is for sending sp only

    // Statistics manager objects from the client
    final ClientStatsContext[] periodicStatsContexts;
    final ClientStatsContext[] fullStatsContexts;

    Timer timer;
    long benchmarkStartTS;

    // Count the sp / np procs that have been sent so far, use volatile variable for
    // optimal performance
    volatile long spCount = 0;
    volatile long npCount = 0;

    static class NPBenchmarkConfig extends CLIConfig {
        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 60;

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
        double mprate = 0.005;

        @Option(desc = "Data skew ratio")
        double skew = 0.2;

        @Option(desc = "Number of clients for the test")
        int clientscount = 2;

        @Override
        public void validate() {
            if (sprate < 0) {
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

            if (clientscount < 2) {
                exitWithMessageAndUsage("Invalid client number...");
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

        clients = new Client[config.clientscount];
        periodicStatsContexts = new ClientStatsContext[config.clientscount];
        fullStatsContexts = new ClientStatsContext[config.clientscount];

        for (int i = 0; i < config.clientscount; i++) {
            ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());

            clients[i] = ClientFactory.createClient(clientConfig);
            periodicStatsContexts[i] = clients[i].createStatsContext();
            fullStatsContexts[i] = clients[i].createStatsContext();
        }

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
    void connectToOneServerWithRetry(Client client, String server, int index) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            }
            catch (Exception e) {
                System.err.printf("Client %d connection failed - retrying in %d second(s).\n", index, sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
        System.out.printf("Client %d connected to VoltDB node at: %s.\n", index, server);
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
        final CountDownLatch connections = new CountDownLatch(serverArray.length * config.clientscount);

        for (int i = 0; i < config.clientscount; i++) {
            final int index = i;
            // use a new thread to connect to each server
            for (final String server : serverArray) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        connectToOneServerWithRetry(clients[index], server, index);
                        connections.countDown();
                    }
                }).start();
            }
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
        long time = Math.round((periodicStatsContexts[0].fetch().getStats().getEndTimestamp() - benchmarkStartTS) / 1000.0);

        long thruput = 0;
        long invocErrs = 0, invocAbrts = 0, invocTimeOuts = 0;
        double avgLatcy = 0.0, kpLatcy = 0.0;

        long totalInvoc = 0;

        for (int i = 0; i < config.clientscount; i++) {
            ClientStats stats = periodicStatsContexts[i].fetchAndResetBaseline().getStats();

            thruput += stats.getTxnThroughput();
            invocErrs += stats.getInvocationErrors();
            invocAbrts += stats.getInvocationAborts();
            invocTimeOuts += stats.getInvocationTimeouts();

            long temp = stats.getInvocationsCompleted() + stats.getInvocationAborts() + stats.getInvocationErrors() + stats.getInvocationTimeouts();
            totalInvoc += temp;

            avgLatcy += stats.getAverageLatency() * (double) temp;
            kpLatcy += stats.kPercentileLatency(0.95) * (double) temp;
        }
        avgLatcy = avgLatcy / (double) totalInvoc;
        kpLatcy = kpLatcy / (double) totalInvoc;

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", thruput);
        System.out.printf("Aborts/Failures/Aborts %d/%d/%d, ",
                invocAbrts, invocErrs, invocTimeOuts);

        // cast to stats.getAverageLatency from long to double
        System.out.printf("Avg/95%% Latency %.2f/%.2fms\n",
                          avgLatcy,
                          kpLatcy);

    }

    /**
     * Prints statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        long thruput = 0;
        long invocErrs = 0, invocAbrts = 0, invocTimeOuts = 0;
        double avgLatcy = 0.0, k95pLatcy = 0.0, k99pLatcy = 0.0, internalLatcy = 0.0;

        long totalInvoc = 0;

        for (int i = 0; i < config.clientscount; i++) {
            ClientStats stats = fullStatsContexts[i].fetchAndResetBaseline().getStats();

            thruput += stats.getTxnThroughput();
            invocErrs += stats.getInvocationErrors();
            invocAbrts += stats.getInvocationAborts();
            invocTimeOuts += stats.getInvocationTimeouts();

            long temp = stats.getInvocationsCompleted() + stats.getInvocationAborts() + stats.getInvocationErrors() + stats.getInvocationTimeouts();
            totalInvoc += temp;

            avgLatcy += stats.getAverageLatency() * (double) temp;
            k95pLatcy += stats.kPercentileLatency(0.95) * (double) temp;
            k99pLatcy += stats.kPercentileLatency(0.99) * (double) temp;
            internalLatcy += stats.getAverageInternalLatency() * (double) temp;
        }
        avgLatcy = avgLatcy / (double) totalInvoc;
        k95pLatcy = k95pLatcy / (double) totalInvoc;
        k99pLatcy = k99pLatcy / (double) totalInvoc;
        internalLatcy = internalLatcy / (double) totalInvoc;


        // Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", thruput);
        // cast stats.getAverateLatency from long to double
        System.out.printf("Average latency:               %,9.2f ms\n", avgLatcy);
        //System.out.printf("Average latency:               %,9d ms\n", stats.getAverageLatency());
        System.out.printf("95th percentile latency:       %,9.2f ms\n", k95pLatcy);
        System.out.printf("99th percentile latency:       %,9.2f ms\n", k99pLatcy);

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        // cast stats.getAverageInternalLatency from long to double
        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", internalLatcy);
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
                0,
                config.duration * 1000,
                totalInvoc,
                thruput,
                avgLatcy,
                k95pLatcy,
                k99pLatcy,
                internalLatcy,
                0.0,
                0.0,
                invocErrs,
                invocAbrts,
                invocTimeOuts));

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
            clients[0].callProcedure(new ProcCallback("CARD_ACCOUNT.insert"),
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

    public void iterateNP(double count, int offset, int num) throws Exception {
         // 2P transaction
        String pan1 = generate16DString(ThreadLocalRandom.current().nextInt(config.cardcount));
        String pan2 = generate16DString(ThreadLocalRandom.current().nextInt((int) count) + offset);    // a smaller range of entities

        clients[num].callProcedure(
                                "Transfer",
                                pan1,
                                pan2,
                                ThreadLocalRandom.current().nextDouble() < 0.5 ? -1 : 1,   // random transfer direction
                                "USD"
                                );

        npCount += 1;

        if (ThreadLocalRandom.current().nextDouble() < config.mprate) {
            // MP transaction
            int id1 = ThreadLocalRandom.current().nextInt(config.cardcount);
            int id2 = id1 + 2000 < config.cardcount ?
                    id1 + 2000 : config.cardcount - 1;

            pan1 = generate16DString(id1);
            pan2 = generate16DString(id2);

            clients[num].callProcedure(
                                "Select",
                                pan1,
                                pan2);
        }
    }

    /**
     * Performs one iteration of the benchmark
     */
    public void iterateSP(double count, int offset) throws Exception {

        if ((double) spCount < (double) npCount * config.sprate) {
            String pan1 = generate16DString(ThreadLocalRandom.current().nextInt(config.cardcount));
            String pan2 = generate16DString(ThreadLocalRandom.current().nextInt(config.cardcount));

            clients[0].callProcedure(new ProcCallback("Authorize"),
                                "Authorize",
                                pan1,
                                1,
                                "USD"
                                );
            clients[0].callProcedure(new ProcCallback("Redeem"),
                                "Redeem",
                                pan2,
                                1,
                                "USD",
                                1
                                );
            spCount += 2;
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

        final double count = ((double) config.cardcount) * (1.0 - config.skew);
        final int offset = (int) (((double) config.cardcount) / 2.0 - count / 2.0);

        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        System.out.println("Warming up for "+ config.warmup +" seconds...");
        final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
        while (warmupEndTime > System.currentTimeMillis()) {
            iterateSP(count, offset);
            iterateNP(count, offset, 1);
        }

        // reset the stats after warmup
        for (int i = 0; i < config.clientscount; i++) {
            fullStatsContexts[i].fetchAndResetBaseline();
            periodicStatsContexts[i].fetchAndResetBaseline();
        }

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        System.out.println("\nRunning benchmark...");
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);

        Thread[] threads = new Thread[config.clientscount];

        // First thread for running SP proc only, using only the first client
        // SP txns are very fast to execute and usually do not trigger thrashing issue
        threads[0] = new Thread(() -> {
            try {
                while (benchmarkEndTime > System.currentTimeMillis()) {
                    iterateSP(count, offset);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Other threads for running NP proc only
        // use more clients and round-robin strategy to avoid thrashing (with max tps set)
        for (int i = 1; i < config.clientscount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                try {
                    while (benchmarkEndTime > System.currentTimeMillis()) {
                        iterateNP(count, offset, index);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        // cancel periodic stats printing
        timer.cancel();

        // block until all outstanding txns return
        for (int i = 0; i < config.clientscount; i++) {
            clients[i].drain();
        }

        // print the summary results
        printResults();

        // close down the client connections
        for (int i = 0; i < config.clientscount; i++) {
            clients[i].close();
        }
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
