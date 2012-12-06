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

package txnIdSelfCheck;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.utils.MiscUtils;

import txnIdSelfCheck.procedures.updateReplicated;

public class AsyncBenchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final Config config;
    // create a client for each server node
    final List<Client> clients = Collections.synchronizedList(new ArrayList<Client>());
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Statistics manager objects from the client
//    final ClientStatsContext periodicStatsContext;
//    final ClientStatsContext fullStatsContext;
    final PayloadProcessor processor;
    // For retry connections
    final ExecutorService es = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable arg0) {
            Thread thread = new Thread(arg0, "Retry Connection");
            thread.setDaemon(true);
            return thread;
        }
    });

    final AtomicLong previousReplicated = new AtomicLong(0);
    final AtomicBoolean shutdown = new AtomicBoolean(false);

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    private static class Config extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 20;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";
        String[] parsedServers = null;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = 100000;

        @Option(desc = "Determine transaction rate dynamically based on latency.")
        boolean autotune = false;

        @Option(desc = "Server-side latency target for auto-tuning.")
        int latencytarget = 5;

        @Option(desc = "Multi/single ratio.")
        double multisingleratio = 0.1;

        @Option(desc = "Number of rows to keep in the database as a moving window")
        long windowsize = 100000;

        @Option(desc = "Minimum value size in bytes.")
        int minvaluesize = 1024;

        @Option(desc = "Maximum value size in bytes.")
        int maxvaluesize = 1024;

        @Option(desc = "Number of values considered for each value byte.")
        int entropy = 127;

        @Option(desc = "Compress values on the client side.")
        boolean usecompression= false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
            if (latencytarget <= 0) exitWithMessageAndUsage("latencytarget must be > 0");
            if (multisingleratio < 0) exitWithMessageAndUsage("multisingleratio must be within [0, 1]");
            if (multisingleratio > 1) exitWithMessageAndUsage("multisingleratio must be within [0, 1]");
            if (windowsize <= 0) exitWithMessageAndUsage("windowsize must be > 0");

            if (minvaluesize <= 0) exitWithMessageAndUsage("minvaluesize must be > 0");
            if (maxvaluesize <= 0) exitWithMessageAndUsage("maxvaluesize must be > 0");
            if (entropy <= 0) exitWithMessageAndUsage("entropy must be > 0");
            if (entropy > 127) exitWithMessageAndUsage("entropy must be <= 127");
        }

        @Override
        public void parse(String cmdName, String[] args) {
            super.parse(cmdName, args);

            // parse servers
            parsedServers = servers.split(",");
        }
    }

    /**
     * Remove the client from the list if connection is broken.
     */
    class StatusListener extends ClientStatusListenerExt {
        private Client client;

        public void setClient(Client client) {
            this.client = client;
        }

        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            if (shutdown.get()) {
                return;
            }

            // if the benchmark is still active
            if ((System.currentTimeMillis() - benchmarkStartTS) < (config.duration * 1000)) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }
            clients.remove(this.client);

            // setup for retry
            final String server = MiscUtils.getHostnameColonPortString(hostname, port);
            es.execute(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(client, server);
                }
            });
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public AsyncBenchmark(Config config) {
        this.config = config;

        processor = new PayloadProcessor(config.minvaluesize, config.maxvaluesize,
                                         config.entropy, config.usecompression);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
    }

    Client createClient(int serverCount) {
        StatusListener statusListener = new StatusListener();
        ClientConfig clientConfig = new ClientConfig("", "", statusListener);
        if (config.autotune) {
            clientConfig.enableAutoTune();
            clientConfig.setAutoTuneTargetInternalLatency(config.latencytarget);
        }
        else {
            clientConfig.setMaxTransactionsPerSecond(config.ratelimit / serverCount);
        }
        Client client = ClientFactory.createClient(clientConfig);
        statusListener.setClient(client);

//        periodicStatsContext = client.createStatsContext();
//        fullStatsContext = client.createStatsContext();
        return client;
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param client The client to use for this server
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServerWithRetry(Client client, String server) {
        int sleep = 1000;
        while (!shutdown.get()) {
            try {
                client.createConnection(server);
                clients.add(client);
                System.out.printf("Connected to VoltDB node at: %s.\n", server);
                break;
            }
            catch (Exception e) {
                System.err.printf("Connection to " + server + " failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @throws InterruptedException if anything bad happens with the threads.
     */
    void connect() throws InterruptedException {
        System.out.println("Connecting to VoltDB...");

        final CountDownLatch connections = new CountDownLatch(config.parsedServers.length);

        // use a new thread to connect to each server
        for (final String server : config.parsedServers) {
            final Client client = createClient(config.parsedServers.length);
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
//        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
//        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);
//
//        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
//        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
//        System.out.printf("Aborts/Failures %d/%d, ",
//                stats.getInvocationAborts(), stats.getInvocationErrors());
//        System.out.printf("Avg/95%% Latency %.2f/%dms\n", stats.getAverageLatency(),
//                stats.kPercentileLatency(0.95));
        System.out.printf("Executed %d\n", c.get());
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        //ClientStats stats = fullStatsContext.fetch().getStats();

        // 1. Voting Board statistics, Voting results and performance statistics
        String display = "\n" +
                         HORIZONTAL_RULE +
                         " Voting Results\n" +
                         HORIZONTAL_RULE +
                         "\nA total of %d votes were received...\n\n";
        //System.out.printf(display, stats.getInvocationsCompleted());

        // 3. Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

//        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
//        System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
//        System.out.printf("95th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.95));
//        System.out.printf("99th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.99));

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        if (config.autotune) {
            System.out.printf("Targeted Internal Avg Latency: %,9d ms\n", config.latencytarget);
        }
        //System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

        // 4. Write stats to file if requested
        //client.writeSummaryCSV(stats, config.statsfile);
    }

    private static void crash(String msg) {
        System.err.println(msg);
        System.exit(-1);
    }

    /**
     * Callback to handle the response to a stored procedure call.
     * Tracks response types.
     *
     */
    class updateReplicatedCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            c.incrementAndGet();

            if (response.getStatus() == ClientResponse.SUCCESS) {
                // pass
            } else if (response.getStatus() == ClientResponse.USER_ABORT) {
                if (response.getAppStatus() != updateReplicated.AbortStatus.NORMAL.ordinal()) {
                    crash(response.getStatusString());
                }
            } else if (response.getStatus() == ClientResponse.UNEXPECTED_FAILURE) {
                crash(response.getStatusString());
            } else {
                // Could be server connection lost
                //System.err.println("updateReplicated failed: " + response.getStatusString());
            }
        }
    }

    /**
     * Callback to handle the response to a stored procedure call.
     * Tracks response types.
     *
     */
    class doTxnCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            c.incrementAndGet();

            if (response.getStatus() == ClientResponse.UNEXPECTED_FAILURE) {
                crash(response.getStatusString());
            }
        }
    }

    private final AtomicLong c = new AtomicLong();
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
        connect();

        VoltTable[] rowResults = null;
        VoltTable[] replicatedResults = null;
        boolean succeeded = false;
        // If server fails during initialization, try again
        while (!succeeded) {
            try {
                // initialize using synchronous call
                Client initClient = clients.get(0);
                initClient.callProcedure("Initialize");
                ClientResponse rowResp = initClient.callProcedure("getLastRow");
                rowResults = rowResp.getResults();
                assert (rowResp.getStatus() == ClientResponse.SUCCESS);
                System.err.println("start");
                Thread.sleep(3000);
                System.err.println("end");
                ClientResponse replicatedRowResp = initClient.callProcedure("getLastReplicatedRow");
                replicatedResults = replicatedRowResp.getResults();
                assert (replicatedRowResp.getStatus() == ClientResponse.SUCCESS);
                succeeded = true;
            } catch (ProcCallException e) {
                System.err.println(e.getMessage());
            } catch (NoConnectionsException e) {
                System.err.println("Failed to initialize, will retry: " + e.getMessage());
            }
        }

        // total of 127 cids
        final int cidCount = 127;
        final long windowPerCid = config.windowsize / cidCount;
        // rids per cid
        final Map<Integer, Long> rids = new HashMap<Integer, Long>();
        // reset all cids to 0 and initialize inFlight queues
        for (int i = -1; i < cidCount; i++) {
            rids.put(i, 0l);
        }
        // populate the rids with values from previous run
        while (rowResults[0].advanceRow()) {
            long cid = rowResults[0].getLong("cid");
            long last_rid = rowResults[0].getLong("last_rid");
            rids.put((int) cid, last_rid + 1);
        }
        rids.put(-1, replicatedResults[0].asScalarLong() + 1);

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        java.util.Random r = new java.util.Random(2);

        // reset the stats after warmup
//        fullStatsContext.fetchAndResetBaseline();
//        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        System.out.println("\nRunning benchmark...");
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {
            int cid;
            if (r.nextDouble() < config.multisingleratio) {
                // MP uses cid -1
                cid = -1;
            } else {
                cid = (byte)r.nextInt(cidCount);
            }

            long rid = rids.get(cid);
            // txns for the same cid go to the same client
            Client client = null;
            try {
                if (clients.isEmpty()) {
                    crash("No connection to any server");
                }

                if (cid == -1) {
                    client = clients.get(0);
                    // update the replicated table
                    client.callProcedure(new updateReplicatedCallback(),
                                         "updateReplicated",
                                         rid);
                } else {
                    client = clients.get(cid % clients.size());
                    // asynchronously call the "doTxn" procedure
                    client.callProcedure(new doTxnCallback(),
                                         "doTxn",
                                         cid,
                                         rid,
                                         rid > windowPerCid ? rid - windowPerCid : 0,
                                         processor.generateForStore().getStoreValue());
                }
            } catch (IndexOutOfBoundsException e) {
                Thread.sleep(1000);
                continue;
            } catch (IOException e) {
                Thread.sleep(1000);
                continue;
            }

            rids.put(cid, rid + 1);
        }

        // cancel periodic stats printing
        timer.cancel();

        shutdown.set(true);
        es.shutdownNow();

        // block until all outstanding txns return
        for (Client client : clients) {
            client.drain();
            client.close();
        }
        clients.clear();

        // print the summary results
        printResults();
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link Config}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        Config config = new Config();
        config.parse(AsyncBenchmark.class.getName(), args);

        AsyncBenchmark benchmark = new AsyncBenchmark(config);
        benchmark.runBenchmark();
    }
}
