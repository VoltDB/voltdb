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

package positionkeeper;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;

import com.google_voltpatches.common.collect.ConcurrentHashMultiset;
import com.google_voltpatches.common.collect.Multiset;

public class PositionsBenchmark {

    // handy, rather than typing this out several times
    public static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final PositionsConfig config;
    // Reference to the database connection we will use
    final Client client;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    private Random rand = new Random();
    private int order_id = 1;
    private int trade_id = 1;
    private int price_id = 1;
    private final int numSecs = 3000;
    private double[] prices;
    private int[][] positions;

    public void printHeading(String heading) {
        System.out.print("\n"+HORIZONTAL_RULE);
        System.out.println(" " + heading);
        System.out.println(HORIZONTAL_RULE);
    }

    /**
     * Uses CLIConfig class to declaratively state command line options
     * with defaults and validation.
     */
    static class PositionsConfig extends CLIConfig {
        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "User name")
        public String user = "";

        @Option(desc = "Password")
        public String password = "";

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 20;

        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 2;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = 100000;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Number of traders")
        int traders = 500;

        @Option(desc = "Number of positions per traders")
        int secpercnt = 50;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
        }
    }

    // constructor
    public PositionsBenchmark(PositionsConfig config) {
        this.config = config;

        prices = new double[numSecs];
        positions = new int[config.traders][config.secpercnt];

        ClientConfig clientConfig = new ClientConfig(config.user, config.password, new StatusListener());
        clientConfig.setMaxTransactionsPerSecond(config.ratelimit);
        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        printHeading("Command Line Configuration");
        System.out.println(config.getConfigDumpString());
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
        // cast stats.getAverateLatency from long to double
        System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
        //System.out.printf("Average latency:               %,9d ms\n", stats.getAverageLatency());
        System.out.printf("95th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.95));
        System.out.printf("99th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.99));

        printHeading("System Server Statistics");

        // cast stats.getAverageInternalLatency from long to double
        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());
        //System.out.printf("Reported Internal Avg Latency: %,9d ms\n", stats.getAverageInternalLatency());

        // 4. Write stats to file if requested
        client.writeSummaryCSV(stats, config.statsfile);
    }

    public void initialize() throws Exception {

        // EXC
        for (int e = 0; e < 5; e++) {
            client.callProcedure(new BenchmarkCallback("EXC.insert"), "EXC.insert", e);
        }
        System.out.println("  loaded 5 records into EXC");

        // SEC
        for (int s = 0; s < numSecs; s++) {
            client.callProcedure(new BenchmarkCallback("SEC.insert"), "SEC.insert", s, rand.nextInt(5));
            prices[s] = rand.nextInt(100);
        }
        System.out.println("  loaded "+numSecs+" records into SEC");

        // CNT
        for (int t = 0; t < config.traders; t++) {
            client.callProcedure(new BenchmarkCallback("CNT.insert"), "CNT.insert", t);
        }
        System.out.println("  loaded "+config.traders+" records into CNT");

        // positions (client side only)
        for (int t = 0; t < config.traders; t++) {

            // make a list of random securities with no duplicates
            List<Integer> secs = new ArrayList<Integer>();
            while(secs.size() < config.secpercnt) {
                int sec = rand.nextInt(numSecs);
                if(!secs.contains(sec))
                    secs.add(sec);
            }

            // put securities into an two-dimension array: positions[trader][index] = sec_id
            for (int i = 0; i < config.secpercnt; i++) {
                positions[t][i] = secs.get(i);
            }
        }
        System.out.println("  initialized client with array of "+ config.traders * config.secpercnt +" possible positions");

    }

    static class BenchmarkCallback implements ProcedureCallback {

        private static Multiset<String> calls = ConcurrentHashMultiset.create();
        private static Multiset<String> commits = ConcurrentHashMultiset.create();
        private static Multiset<String> rollbacks = ConcurrentHashMultiset.create();

        String procedureName;
        long maxErrors;

        public static void printProcedureResults(String procedureName) {
            System.out.println("  " + procedureName);
            System.out.println("        calls: " + calls.count(procedureName));
            System.out.println("      commits: " + commits.count(procedureName));
            System.out.println("    rollbacks: " + rollbacks.count(procedureName));
            System.out.println();
        }

        public static void printAllResults() {
            for (String e : calls.elementSet()) {
                printProcedureResults(e);
            }
        }

        public BenchmarkCallback(String procedure, long maxErrors) {
            super();
            this.procedureName = procedure;
            this.maxErrors = maxErrors;
        }

        public BenchmarkCallback(String procedure) {
            this(procedure, 5l);
        }

        @Override
        public void clientCallback(ClientResponse cr) {
            calls.add(procedureName,1);

            if (cr.getStatus() == ClientResponse.SUCCESS) {
                commits.add(procedureName,1);
            }
            else {
                long totalErrors = rollbacks.add(procedureName,1);

                System.err.println("DATABASE ERROR: " + cr.getStatusString());

                if (totalErrors > maxErrors) {
                    System.err.println("exceeded " + maxErrors + " maximum database errors - exiting client");
                    System.exit(-1);
                }
            }
        }
    }

    public void iterate() throws Exception {

        // always do a new price
        newPrice();

        // 1/10th of the time
        if (rand.nextInt(10) == 0) {
            newOrder();
        }

        // 1/50th of the time
        if (rand.nextInt(50) == 0) {
            newTrade();
        }
    }


    private void newOrder() throws Exception {
        // get a random trader
        int cnt = rand.nextInt(config.traders);

        // retrieve the sec of one of the positions for that trader
        // (so the total # of positions isn't growing beyond what was intended)
        int sec = positions[cnt][rand.nextInt(config.secpercnt)];

        // get the last price, modify it randomly
        double price = prices[sec] * (1+rand.nextGaussian()/100);
        // store the new price
        prices[sec] = price;

        // insert the order with a random qty
        client.callProcedure(new BenchmarkCallback("OrderInsert"),
                             "OrderInsert",
                             order_id++,
                             cnt,
                             sec,
                             (rand.nextInt(9)+1) * 10,
                             price);
    }

    private void newTrade() throws Exception {

        // get a random trader
        int cnt = rand.nextInt(config.traders);

        // retrieve the sec of one of the positions for that trader
        // (so the total # of positions is as planned)
        int sec = positions[cnt][rand.nextInt(config.secpercnt)];

        // get the last price, modify it randomly
        double price = prices[sec] * (1+rand.nextGaussian()/100);
        // store the new price
        prices[sec] = price;

        // insert the trade, with a random qty
        client.callProcedure(new BenchmarkCallback("TradeInsert"),
                             "TradeInsert",
                             trade_id++,
                             cnt,
                             sec,
                             (rand.nextInt(9)+1) * 10,
                             price);
    }

    private void newPrice() throws Exception {
        int sec = rand.nextInt(numSecs);
        double price = prices[sec] * (1+rand.nextGaussian()/100); // modify price randomly

        client.callProcedure(new BenchmarkCallback("PriceInsert"),
                             "PriceInsert",
                             price_id++,
                             sec,
                             price,
                             new Date());
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        printHeading("Setup & Initialization");

        // connect to one or more servers, loop until success
        connect(config.servers);

        // initialize using synchronous call
        System.out.println("\nPre-loading Tables...\n");
        //client.callProcedure("Initialize", config.contestants, CONTESTANT_NAMES_CSV);
        initialize();

        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        System.out.println("Warming up for the specified "+ config.warmup +" seconds...");
        final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
        while (warmupEndTime > System.currentTimeMillis()) {
            iterate();
        }

        printHeading("Starting Benchmark");

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

    public static void main(String[] args) throws Exception {
        PositionsConfig config = new PositionsConfig();
        config.parse(PositionsBenchmark.class.getName(), args);

        PositionsBenchmark benchmark = new PositionsBenchmark(config);
        benchmark.runBenchmark();
    }
}
