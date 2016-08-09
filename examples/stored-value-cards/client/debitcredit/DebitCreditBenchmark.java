/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 * 
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
package debitcredit;

import java.util.Date;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;

import com.google_voltpatches.common.collect.ConcurrentHashMultiset;
import com.google_voltpatches.common.collect.Multiset;

//import voter.procedures.Vote;

public class DebitCreditBenchmark {

    // handy, rather than typing this out several times
    public static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final CardsConfig config;
    // Reference to the database connection we will use
    final Client client;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;
    // Random number generator
    private Random rand = new Random();

    static class CardsConfig extends CLIConfig {
        // STANDARD BENCHMARK OPTIONS
        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 20;

        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 2;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = 100000;

        @Option(desc = "Determine transaction rate dynamically based on latency.")
        boolean autotune = true;

        @Option(desc = "Server-side latency target for auto-tuning.")
        int latencytarget = 6;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        // CUSTOM OPTIONS
        @Option(desc = "Number of Cards")
        int cardcount = 500000;

        @Option(desc = "Percentage of transactions that are transfers")
        int transferpct = 1;
        
        @Option(desc = "Type of transfer transaction to run (stored procedure SP, Ad Hoc MP, or Ad Hoc NP)")
        String type = "SP";

        public CardsConfig() {
        }

        public static CardsConfig getConfig(String classname, String[] args) {
        	CardsConfig config = new CardsConfig();
            config.parse(classname, args);
            return config;
        }
        
        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
            if (latencytarget <= 0) exitWithMessageAndUsage("latencytarget must be > 0");
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
     * Callback to handle the response to a stored procedure call.
     *
     */
    static class CardCallback implements ProcedureCallback {
        private static Multiset<String> stats = ConcurrentHashMultiset.create();
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

        public CardCallback(String procedure, long maxErrors) { 
            super();
            this.procedureName = procedure;
            this.maxErrors = maxErrors;
        }

        public CardCallback(String procedure) {
            this(procedure, 5l);
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
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public DebitCreditBenchmark(CardsConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
        clientConfig.setMaxTransactionsPerSecond(config.ratelimit);
        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

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
        
        CardCallback.printProcedureResults("CARD_ACCOUNT.insert");
        CardCallback.printProcedureResults("Authorize");
        CardCallback.printProcedureResults("Redeem");
        CardCallback.printProcedureResults("Transfer");

        // Write stats to file if requested
        client.writeSummaryCSV(stats, config.statsfile);
    }


    /**
     * Initializes any data that needs to be loaded into VoltDB before running the benchmark
     * Override this method for customization
     * @throws Exception
     */
    public void initialize() throws Exception {
        System.out.println("Generating " + config.cardcount + " cards...");
        for (int i = 0; i < config.cardcount; i++) {

            // generate a card
            String pan = Integer.toString(i); // TODO - pad with zeros for 16-digits
            Date now = new Date();

            // insert the card
            client.callProcedure(new CardCallback("CARD_ACCOUNT.insert"),
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
        int id = rand.nextInt(config.cardcount-1);
        String pan = Integer.toString(id);

        client.callProcedure(new CardCallback("Authorize"),
                             "Authorize",
                             pan,
                             25,
                             "USD"
                             );

        client.callProcedure(new CardCallback("Redeem"),
                             "Redeem",
                             pan,
                             25,
                             "USD",
                             1
                             );

        if (rand.nextInt(100) < config.transferpct) {
            int id1 = rand.nextInt(config.cardcount-1);
            int id2 = rand.nextInt(config.cardcount-1);

            String pan1 = Integer.toString(id1);
            String pan2 = Integer.toString(id2);

            if (config.type.equals("SP")) {
            	client.callProcedure(new CardCallback("Transfer",10000),
                                 "Transfer",
                                 pan1,
                                 pan2,
                                 5,
                                 "USD"
                                 );
            }
            else {
                                 
            	// An approximation of the transfer transaction using an adhoc NP transaction
            	double amount = 5;
            	Date txnTime = new Date();
            	String sql = "";
            	sql += String.format("SELECT * FROM card_account WHERE pan = '%s';",pan1);
            	sql += String.format("SELECT * FROM card_account WHERE pan = '%s';",pan2);
            	sql += String.format("UPDATE card_account SET " +
                    " balance = balance + %f," +
                    " available_balance = available_balance + %f," +
                    " last_activity = NOW" +
                    " WHERE pan = '%s';", -amount, -amount, pan1);
            	sql += String.format("UPDATE card_account SET " +
                    " balance = balance + %f," +
                    " available_balance = available_balance + %f," +
                    " last_activity = NOW" +
                    " WHERE pan = '%s';", amount, amount, pan2);
            	sql += String.format("INSERT INTO card_activity VALUES ('%s',NOW,'%s','%s',%f);",
            		pan1,
                    "TRANSFER",
                    "D",
                    -amount);
            	sql += String.format("INSERT INTO card_activity VALUES ('%s',NOW,'%s','%s',%f);",
            		pan2,
                    "TRANSFER",
                    "C",
                    amount);
            	if (config.type.equals("MP")) {
                    client.callProcedure(new CardCallback("Transfer",10000),
                            "@AdHoc",
                            sql
                            );
            	}
            	else {
            		assert (config.type.equals("NP"));
            		String jsonKeys = String.format("{\"Type\":\"VARCHAR\",\"Keys\":[{\"Key\":\"'%s'\"},{\"Key\":\"'%s'\"}]}", pan1, pan2);
                    client.callProcedure(new CardCallback("Transfer",10000),
                            "@AdHoc_NP",
                            sql,
                            jsonKeys
                            );
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
        //client.callProcedure("Initialize", config.contestants, CONTESTANT_NAMES_CSV);
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

    public static void main(String[] args) throws Exception {
        CardsConfig config = new CardsConfig();
        config.parse(DebitCreditBenchmark.class.getName(), args);
        DebitCreditBenchmark benchmark = new DebitCreditBenchmark(config);
        benchmark.runBenchmark();
    }

}

