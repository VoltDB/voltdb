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

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.utils.MiscUtils;

import txnIdSelfCheck.procedures.updateReplicated;

public class AsyncBenchmark {

    static VoltLogger log = new VoltLogger("HOST");

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------";

    // validated command line configuration
    final Config config;
    // create a client for each server node
    final List<Client> clients = Collections.synchronizedList(new ArrayList<Client>());
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;

    final TxnIdPayloadProcessor processor;

    final AtomicLong previousReplicated = new AtomicLong(0);
    final AtomicBoolean shutdown = new AtomicBoolean(false);

    // for reporting and detecting progress
    private final AtomicLong txnCount = new AtomicLong();
    private long txnCountAtLastCheck;
    private long lastProgressTimestamp = System.currentTimeMillis();

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

        @Option(desc = "Timeout that kills the client if progress is not made.")
        int progresstimeout = 360;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Enable/Disable topology awareness, default=true")
        boolean topologyaware = true;


        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
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
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public AsyncBenchmark(Config config) {
        this.config = config;

        processor = new TxnIdPayloadProcessor(4, config.minvaluesize, config.maxvaluesize,
                                         config.entropy, Integer.MAX_VALUE, config.usecompression);

        log.info(HORIZONTAL_RULE);
        log.info(" Command Line Configuration");
        log.info(HORIZONTAL_RULE);
        log.info(config.getConfigDumpString());
    }

    Client createClient(int serverCount) {
        ClientConfig clientConfig = new ClientConfig("", "");
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setMaxTransactionsPerSecond(config.ratelimit / serverCount);

        if (config.topologyaware) {
              clientConfig.setTopologyChangeAware(true);
        }

        Client client = ClientFactory.createClient(clientConfig);

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
                log.info(String.format("Connected to VoltDB node at: %s.", server));
                break;
            }
            catch (Exception e) {
                log.warn(String.format("Connection to " + server + " failed - retrying in %d second(s).", sleep / 1000));
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 1000) sleep += sleep;
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
        log.info("Connecting to VoltDB...");

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
        long txnCountNow = txnCount.get();
        long now = System.currentTimeMillis();
        boolean madeProgress = txnCountNow > txnCountAtLastCheck;

        if (madeProgress) {
            lastProgressTimestamp = now;
        }
        txnCountAtLastCheck = txnCountNow;
        long diffInSeconds = (now - lastProgressTimestamp) / 1000;

        log.info(String.format("Executed %d%s", txnCount.get(),
                madeProgress ? "" : " (no progress made in " + diffInSeconds + " seconds)"));

        if (diffInSeconds > config.progresstimeout) {
            log.error("No progress was made in over " + diffInSeconds + " seconds while connected to a cluster. Exiting.");
            System.exit(-1);
        }
    }

    private static void crash(String msg) {
        log.error(msg);
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
            if (response.getStatus() == ClientResponse.SUCCESS) {
                txnCount.incrementAndGet();
                return; // pass
            }

            if (response.getStatus() == ClientResponse.USER_ABORT) {
                if (response.getAppStatus() != updateReplicated.AbortStatus.NORMAL.ordinal()) {
                    log.warn("Non-success in updateReplicatedCallback");
                    log.warn(((ClientResponseImpl) response).toJSONString());
                    crash(response.getStatusString());
                }
                return; // this might happen and it's ok
            }

            log.warn("Non-success in updateReplicatedCallback");
            log.warn(((ClientResponseImpl) response).toJSONString());

            if (response.getStatus() == ClientResponse.UNEXPECTED_FAILURE) {
                crash(response.getStatusString());
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
            if (response.getStatus() == ClientResponse.SUCCESS) {
                txnCount.incrementAndGet();
                return; // pass
            }

            log.warn("Non-success in doTxnCallback");
            log.warn(((ClientResponseImpl) response).toJSONString());

            if (response.getStatus() == ClientResponse.UNEXPECTED_FAILURE) {
                crash(response.getStatusString());
            }
        }
    }

    public void sleepUntilConnected(Client client, long benchmarkEndTime) {
        // take a breather to avoid slamming the log (stay paused if no connections)
        do {
            try { Thread.sleep(1000); } catch (Exception e) {} // sleep for 1s
        }
        while ((client.getConnectedHostList().size() == 0) && (benchmarkEndTime > System.currentTimeMillis()));
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        log.info(HORIZONTAL_RULE);
        log.info(" Setup & Initialization");
        log.info(HORIZONTAL_RULE);

        // connect to one or more servers, loop until success
        connect();

        VoltTable[] rowResults = null;
        VoltTable[] replicatedResults = null;
        boolean succeeded = false;
        // If server fails during initialization, try again
        while (!succeeded) {
            Client initClient = clients.get(0);

            try {
                // initialize using synchronous call
                initClient.callProcedure("Initialize");
                ClientResponse rowResp = initClient.callProcedure("getLastRow");
                rowResults = rowResp.getResults();
                assert (rowResp.getStatus() == ClientResponse.SUCCESS);

                ClientResponse replicatedRowResp = initClient.callProcedure("getLastReplicatedRow");
                replicatedResults = replicatedRowResp.getResults();
                assert (replicatedRowResp.getStatus() == ClientResponse.SUCCESS);
                succeeded = true;
            }
            catch (Exception e) {
                log.error("Exception on init. Will sleep.", e);
                sleepUntilConnected(initClient, System.currentTimeMillis() + config.duration * 1000);
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

        log.info(HORIZONTAL_RULE);
        log.info("Starting Benchmark");
        log.info(HORIZONTAL_RULE);

        java.util.Random r = new java.util.Random(2);

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
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
                if (cid == -1) {
                    client = clients.get(0);
                    // skip this work if the client is not connected
                    if (client.getConnectedHostList().size() == 0) {
                        continue;
                    }

                    // update the replicated table
                    try {
                        client.callProcedure(new updateReplicatedCallback(),
                                             "updateReplicated",
                                             rid);
                    }
                    catch (NoConnectionsException e) {
                        log.error("ClientThread got NoConnectionsException on updateReplicated proc call.");
                    }
                }
                else {
                    client = clients.get(cid % clients.size());

                    // skip this work if the client is not connected
                    if (client.getConnectedHostList().size() == 0) {
                        continue;
                    }

                    // asynchronously call the "doTxn" procedure
                    try {
                        client.callProcedure(new doTxnCallback(),
                                             "doTxn",
                                             cid,
                                             rid,
                                             rid > windowPerCid ? rid - windowPerCid : 0,
                                             processor.generateForStore().getStoreValue());
                    }
                    catch (NoConnectionsException e) {
                        log.error("ClientThread got NoConnectionsException on doTxn proc call.");
                    }
                }
            }
            catch (Exception e) {
                log.error("Benchark had a unexpected exception", e);
                System.exit(-1);
            }

            rids.put(cid, rid + 1);
        }

        // cancel periodic stats printing
        timer.cancel();

        shutdown.set(true);
        /* don't wait for a proper cleanup just go away the system may not be healthy -pr
        // block until all outstanding txns return
        for (Client client : clients) {
            client.drain();
            client.close();
        }*/

        log.info(HORIZONTAL_RULE);
        log.info("Benchmark Complete");
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
