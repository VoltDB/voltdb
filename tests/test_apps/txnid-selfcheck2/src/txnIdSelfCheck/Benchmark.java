/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package txnIdSelfCheck;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.io.FileWriter;
import java.io.IOException;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientImpl;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.MiscUtils;

public class Benchmark {

    static VoltLogger log = new VoltLogger("HOST");

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------";

    // validated command line configuration
    final Config config;
    // create a client for each server node
    Client client;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Timer for writing the checkpoint count for apprunner
    Timer checkpointTimer;

    final TxnId2RateLimiter rateLimiter;

    final TxnId2PayloadProcessor processor;

    final AtomicInteger activeConnections = new AtomicInteger(0);
    final AtomicBoolean shutdown = new AtomicBoolean(false);

    // for reporting and detecting progress
    private final AtomicLong txnCount = new AtomicLong();
    private long txnCountAtLastCheck;
    private long lastProgressTimestamp = System.currentTimeMillis();

    // For retry connections
    private final ExecutorService es = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable arg0) {
            Thread thread = new Thread(arg0, "Retry Connection");
            thread.setDaemon(true);
            return thread;
        }
    });

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

        @Option(desc = "Number of parallel syncrhonous threads.")
        int threads = 100;

        @Option(desc = "Id of the first thread (useful for running multiple clients).")
        int threadoffset = 0;

        @Option(desc = "Minimum value size in bytes.")
        int minvaluesize = 1024;

        @Option(desc = "Maximum value size in bytes.")
        int maxvaluesize = 1024;

        @Option(desc = "Number of values considered for each value byte.")
        int entropy = 127;

        @Option(desc = "Compress values on the client side.")
        boolean usecompression = false;

        @Option(desc = "Filler table blob size.")
        int fillerrowsize = 5128;

        @Option(desc = "Target data size for the filler replicated table (at each site).")
        int replfillerrowmb = 32;

        @Option(desc = "Target data size for the partitioned filler table.")
        int partfillerrowmb = 128;

        @Option(desc = "Timeout that kills the client if progress is not made.")
        int progresstimeout = 120;

        @Option(desc = "Whether or not to disable adhoc writes.")
        boolean disableadhoc = false;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = 100000;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Allow experimental in-procedure adhoc statments.")
        boolean allowinprocadhoc = false;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (threadoffset < 0) exitWithMessageAndUsage("threadoffset must be >= 0");
            if (threads <= 0) exitWithMessageAndUsage("threads must be > 0");
            if (threadoffset > 127) exitWithMessageAndUsage("threadoffset must be within [0, 127]");
            if (threadoffset + threads > 128) exitWithMessageAndUsage("max thread offset must be <= 127");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");

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
     * Fake an internal jstack to the log
     */
    static public void printJStack() {
        log.info(new Date().toString() + " Full thread dump");

        Map<String, List<String>> deduped = new HashMap<String, List<String>>();

        // collect all the output, but dedup the identical stack traces
        for (Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
            Thread t = e.getKey();
            String header = String.format("\"%s\" %sprio=%d tid=%d %s",
                    t.getName(),
                    t.isDaemon() ? "daemon " : "",
                    t.getPriority(),
                    t.getId(),
                    t.getState().toString());

            String stack = "";
            for (StackTraceElement ste : e.getValue()) {
                stack += "    at " + ste.toString() + "\n";
            }

            if (deduped.containsKey(stack)) {
                deduped.get(stack).add(header);
            }
            else {
                ArrayList<String> headers = new ArrayList<String>();
                headers.add(header);
                deduped.put(stack, headers);
            }
        }

        for (Entry<String, List<String>> e : deduped.entrySet()) {
            String logline = "";
            for (String header : e.getValue()) {
                logline += header + "\n";
            }
            logline += e.getKey();
            log.info(logline);
        }
    }

    /**
     * Remove the client from the list if connection is broken.
     */
    private class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            if (shutdown.get()) {
                return;
            }

            activeConnections.decrementAndGet();

            // reset the connection id so the client will connect to a recovered cluster
            // this is a bit of a hack
            if (connectionsLeft == 0) {
                ((ClientImpl) client).resetInstanceId();
            }

            // if the benchmark is still active
            if ((System.currentTimeMillis() - benchmarkStartTS) < (config.duration * 1000)) {
                log.warn(String.format("Connection to %s:%d was lost.", hostname, port));
            }

            // setup for retry
            final String server = MiscUtils.getHostnameColonPortString(hostname, port);
            es.execute(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(server);
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
    Benchmark(Config config) {
        this.config = config;

        rateLimiter = new TxnId2RateLimiter(config.ratelimit);
        processor = new TxnId2PayloadProcessor(4, config.minvaluesize, config.maxvaluesize,
                                         config.entropy, Integer.MAX_VALUE, config.usecompression);

        log.info(HORIZONTAL_RULE);
        log.info(" Command Line Configuration");
        log.info(HORIZONTAL_RULE);
        log.info(config.getConfigDumpString());

        StatusListener statusListener = new StatusListener();
        ClientConfig clientConfig = new ClientConfig("", "", statusListener);
        client = ClientFactory.createClient(clientConfig);
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    private void connectToOneServerWithRetry(String server) {
        int sleep = 1000;
        while (!shutdown.get()) {
            try {
                client.createConnection(server);
                activeConnections.incrementAndGet();
                log.info(String.format("Connected to VoltDB node at: %s.", server));
                break;
            }
            catch (Exception e) {
                log.warn(String.format("Connection to " + server + " failed - retrying in %d second(s).", sleep / 1000));
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
    private void connect() throws InterruptedException {
        log.info("Connecting to VoltDB...");

        final CountDownLatch connections = new CountDownLatch(1);

        // use a new thread to connect to each server
        for (final String server : config.parsedServers) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(server);
                    connections.countDown();
                }
            }).start();
        }
        // block until at least one connection is established
        connections.await();
    }

    /**
     * Create a Timer task to write the value of the txnCount to
     * disk to make it available to apprunner
     */
    private void schedulePeriodicCheckpoint() throws IOException {
        checkpointTimer = new Timer();
        TimerTask checkpointTask = new TimerTask() {
            @Override
            public void run() {
                String count = String.valueOf(txnCount.get()) + "\n";
                try {
                    FileWriter writer = new FileWriter(".checkpoint", false);
                    writer.write(count);
                    writer.close();
                }
                catch (Exception e) {
                    System.err.println("Caught exception writing checkpoint file.");
               }
            }
        };
        checkpointTimer.scheduleAtFixedRate(checkpointTask,
                                  1 * 1000,
                                  1 * 1000);
    }

    /**
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
     */
    private void schedulePeriodicStats() {
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
    private synchronized void printStatistics() {

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
            printJStack();
            System.exit(-1);
        }
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

        // Only rate limit the ClientThread for now. Share the same permits for all type of invocations.
        Semaphore permits = rateLimiter.addType(0, 1);

        final int cidCount = 128;
        final long[] lastRid = new long[cidCount];
        for (int i = 0; i < lastRid.length; i++) {
            lastRid[i] = 0;
        }

        // connect to one or more servers, loop until success
        connect();

        // get stats
        try {
            ClientResponse cr = client.callProcedure("Summarize");
            if (cr.getStatus() != ClientResponse.SUCCESS) {
                log.error("Failed to call Summarize proc at startup. Exiting.");
                log.error(((ClientResponseImpl) cr).toJSONString());
                printJStack();
                System.exit(-1);
            }

            // successfully called summarize
            VoltTable t = cr.getResults()[0];
            long ts = t.fetchRow(0).getLong("ts");
            String tsStr = ts == 0 ? "NO TIMESTAMPS" : String.valueOf(ts) + " / " + new Date(ts).toString();
            long count = t.fetchRow(0).getLong("count");

            log.info("STARTUP TIMESTAMP OF LAST UPDATE (GMT): " + tsStr);
            log.info("UPDATES RUN AGAINST THIS DB TO DATE: " + count);
        }
        catch (ProcCallException e) {
            log.error("Failed to call Summarize proc at startup. Exiting.", e);
            log.error(((ClientResponseImpl) e.getClientResponse()).toJSONString());
            printJStack();
            System.exit(-1);
        }

        log.info(HORIZONTAL_RULE);
        log.info("Starting Benchmark");
        log.info(HORIZONTAL_RULE);

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        // reset progress tracker
        lastProgressTimestamp = System.currentTimeMillis();
        schedulePeriodicStats();
        schedulePeriodicCheckpoint();

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        log.info("Running benchmark...");

        BigTableLoader partitionedLoader = new BigTableLoader(client, "bigp",
                (config.partfillerrowmb * 1024 * 1024) / config.fillerrowsize, config.fillerrowsize);
        partitionedLoader.start();
        BigTableLoader replicatedLoader = new BigTableLoader(client, "bigr",
                (config.replfillerrowmb * 1024 * 1024) / config.fillerrowsize, config.fillerrowsize);
        replicatedLoader.start();

        ReadThread readThread = new ReadThread(client, config.threads, config.threadoffset,
                config.allowinprocadhoc);
        readThread.start();

        AdHocMayhemThread adHocMayhemThread = new AdHocMayhemThread(client);
        if (!config.disableadhoc) {
            adHocMayhemThread.start();
        }

        InvokeDroppedProcedureThread idpt = new InvokeDroppedProcedureThread(client);
        idpt.start();

        List<ClientThread> clientThreads = new ArrayList<ClientThread>();
        for (byte cid = (byte) config.threadoffset; cid < config.threadoffset + config.threads; cid++) {
            ClientThread clientThread = new ClientThread(cid, txnCount, client, processor, permits,
                    config.allowinprocadhoc);
            clientThread.start();
            clientThreads.add(clientThread);
        }

        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);

        long currentTs = System.currentTimeMillis();
        while (benchmarkEndTime > currentTs) {
            rateLimiter.updateActivePermits(currentTs);
            Thread.yield();
            currentTs = System.currentTimeMillis();
        }

        replicatedLoader.shutdown();
        partitionedLoader.shutdown();
        readThread.shutdown();
        adHocMayhemThread.shutdown();
        idpt.shutdown();
        for (ClientThread clientThread : clientThreads) {
            clientThread.shutdown();
        }
        replicatedLoader.join();
        partitionedLoader.join();
        readThread.join();
        adHocMayhemThread.join();
        idpt.join();
        for (ClientThread clientThread : clientThreads) {
            clientThread.join();
        }

        // cancel periodic stats printing
        timer.cancel();
        checkpointTimer.cancel();

        shutdown.set(true);
        es.shutdownNow();

        // block until all outstanding txns return
        client.drain();
        client.close();

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
        config.parse(Benchmark.class.getName(), args);

        Benchmark benchmark = new Benchmark(config);
        benchmark.runBenchmark();
    }
}
