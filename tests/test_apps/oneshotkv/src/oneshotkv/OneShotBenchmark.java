/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
 * This sample uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the native VoltDB client
 * library.
 *
 * It also has the additional option to add a number of threads submitting
 * one-shot multipartition reads to the cluster, which might reflect
 * an infrequently performed query submitted against a high-velocity intake
 * stream.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */

package oneshotkv;

import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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

public class OneShotBenchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final KVConfig config;
    // Reference to the database connection we will use
    final Client client;
    final Client mpClient;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Get a payload generator to create random Key-Value pairs to store in the database
    //  and process (uncompress) pairs retrieved from the database.
    final PayloadProcessor processor;
    // random number generator with constant seed
    final Random rand = new Random(0);
    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;
    final ClientStatsContext mpFullStatsContext;
    final ClientStatsContext mpPeriodicStatsContext;

    // kv benchmark state
    final AtomicLong successfulGets = new AtomicLong(0);
    final AtomicLong missedGets = new AtomicLong(0);
    final AtomicLong failedGets = new AtomicLong(0);
    final AtomicLong rawGetData = new AtomicLong(0);
    final AtomicLong networkGetData = new AtomicLong(0);

    final AtomicLong successfulPuts = new AtomicLong(0);
    final AtomicLong failedPuts = new AtomicLong(0);
    final AtomicLong rawPutData = new AtomicLong(0);
    final AtomicLong networkPutData = new AtomicLong(0);

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class KVConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 10;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 5;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of keys to preload.")
        int poolsize = 100000;

        @Option(desc = "Whether to preload a specified number of keys and values.")
        boolean preload = true;

        @Option(desc = "Fraction of SP ops that are gets (vs puts).")
        double getputratio = 0.90;

        @Option(desc = "Fraction of MP ops that are gets (vs puts).")
        double mpgetputratio = 1.00;

        @Option(desc = "Size of keys in bytes.")
        int keysize = 32;

        @Option(desc = "Minimum value size in bytes.")
        int minvaluesize = 1024;

        @Option(desc = "Maximum value size in bytes.")
        int maxvaluesize = 1024;

        @Option(desc = "Number of values considered for each value byte.")
        int entropy = 127;

        @Option(desc = "Compress values on the client side.")
        boolean usecompression= false;

        @Option(desc = "Number of concurrent threads synchronously calling SP procedures.")
        int threads = 40;

        @Option(desc = "Number of concurrent threads synchronously calling MP procedures.")
        int mpthreads = 3;

        @Option(desc = "Number of partitions the multi partitition put writes two.")
        int mpputpartitions = 1;

        @Option(desc = "Use optimistic update for multi partition put. (enables preload)")
        boolean mpputoptimistic = true;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Choose which stats to export.")
        boolean exportStatsfalseSPtrueMP=false;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (poolsize <= 0) exitWithMessageAndUsage("poolsize must be > 0");
            if (getputratio < 0) exitWithMessageAndUsage("getputratio must be >= 0");
            if (getputratio > 1) exitWithMessageAndUsage("getputratio must be <= 1");

            if (keysize <= 0) exitWithMessageAndUsage("keysize must be > 0");
            if (keysize > 250) exitWithMessageAndUsage("keysize must be <= 250");
            if (minvaluesize <= 0) exitWithMessageAndUsage("minvaluesize must be > 0");
            if (maxvaluesize <= 0) exitWithMessageAndUsage("maxvaluesize must be > 0");
            if (entropy <= 0) exitWithMessageAndUsage("entropy must be > 0");
            if (entropy > 127) exitWithMessageAndUsage("entropy must be <= 127");

            if (threads < 0) exitWithMessageAndUsage("threads must be => 0");
            if (mpthreads < 0) exitWithMessageAndUsage("mpthreads must be => 0");
            if (mpgetputratio < 0) exitWithMessageAndUsage("mpgetputratio must be >= 0");
            if (mpgetputratio > 1) exitWithMessageAndUsage("mpgetputratio must be <= 1");
            if (mpputpartitions < 0) exitWithMessageAndUsage("mpputpartitions must be => 0");
            if (mpthreads > 0 && mpgetputratio < 1.0 && mpputoptimistic) {
                preload = true;
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
            if (benchmarkComplete.get() == false) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public OneShotBenchmark(KVConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        ClientConfig mpClientConfig = new ClientConfig("", "", new StatusListener());
        mpClientConfig.setClientAffinity(true);
        mpClient = ClientFactory.createClient(mpClientConfig);
        mpPeriodicStatsContext = mpClient.createStatsContext();
        mpFullStatsContext = mpClient.createStatsContext();

        processor = new PayloadProcessor(config.keysize, config.minvaluesize,
                config.maxvaluesize, config.entropy, config.poolsize, config.usecompression);

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
    static void connectToOneServerWithRetry(Client client, String server) {
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
    static void connect(final Client client, String servers) throws InterruptedException {
        System.out.println("Connecting to VoltDB...");

        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
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
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/95%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.95));
        stats = mpPeriodicStatsContext.fetchAndResetBaseline().getStats();
        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("MP Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/95%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.95));
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // 1. Get/Put performance results
        String display = "\n" +
                         HORIZONTAL_RULE +
                         " KV Store Results\n" +
                         HORIZONTAL_RULE +
                         "\nA total of %,d operations were posted...\n" +
                         " - GETs: %,9d Operations (%,d Misses and %,d Failures)\n" +
                         "         %,9d MB in compressed store data\n" +
                         "         %,9d MB in uncompressed application data\n" +
                         "         Network Throughput: %6.3f Gbps*\n" +
                         " - PUTs: %,9d Operations (%,d Failures)\n" +
                         "         %,9d MB in compressed store data\n" +
                         "         %,9d MB in uncompressed application data\n" +
                         "         Network Throughput: %6.3f Gbps*\n" +
                         " - Total Network Throughput: %6.3f Gbps*\n\n" +
                         "* Figure includes key & value traffic but not database protocol overhead.\n\n";

        double oneGigabit = (1024 * 1024 * 1024) / 8;
        long oneMB = (1024 * 1024);
        double getThroughput = networkGetData.get() + (successfulGets.get() * config.keysize);
               getThroughput /= (oneGigabit * config.duration);
        long totalPuts = successfulPuts.get() + failedPuts.get();
        double putThroughput = networkGetData.get() + (totalPuts * config.keysize);
               putThroughput /= (oneGigabit * config.duration);

        System.out.printf(display,
                stats.getInvocationsCompleted(),
                successfulGets.get(), missedGets.get(), failedGets.get(),
                networkGetData.get() / oneMB,
                rawGetData.get() / oneMB,
                getThroughput,
                successfulPuts.get(), failedPuts.get(),
                networkPutData.get() / oneMB,
                rawPutData.get() / oneMB,
                putThroughput,
                getThroughput + putThroughput);

        // 2. Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
        System.out.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
        System.out.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));

        for (Entry<String, ClientStats> e : fullStatsContext.getStatsByProc().entrySet()) {
            System.out.println("\nPROC: " + e.getKey());
            System.out.printf("Average throughput:            %,9d txns/sec\n", e.getValue().getTxnThroughput());
            System.out.printf("Average latency:               %,9.2f ms\n", e.getValue().getAverageLatency());
            System.out.printf("95th percentile latency:       %,9.2f ms\n", e.getValue().kPercentileLatencyAsDouble(.95));
            System.out.printf("99th percentile latency:       %,9.2f ms\n", e.getValue().kPercentileLatencyAsDouble(.99));
        }

        ClientStats mpstats = mpFullStatsContext.fetch().getStats();

        for (Entry<String, ClientStats> e : mpFullStatsContext.getStatsByProc().entrySet()) {
            System.out.println("\nMP PROC: " + e.getKey());
            System.out.printf("Average throughput:            %,9d txns/sec\n", e.getValue().getTxnThroughput());
            System.out.printf("Average latency:               %,9.2f ms\n", e.getValue().getAverageLatency());
            System.out.printf("95th percentile latency:       %,9.2f ms\n", e.getValue().kPercentileLatencyAsDouble(.95));
            System.out.printf("99th percentile latency:       %,9.2f ms\n", e.getValue().kPercentileLatencyAsDouble(.99));
        }

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());
        System.out.printf("Reported Internal Avg MP Latency: %,9.2f ms\n",
                mpFullStatsContext.getStats().getAverageInternalLatency());

        // 3. Write stats to file if requested
        if (!config.exportStatsfalseSPtrueMP) {
            System.out.println("Exporting SP stats context");
            client.writeSummaryCSV(stats, config.statsfile);
        } else {
            System.out.println("Exporting MP stats context");
            mpClient.writeSummaryCSV(mpstats, config.statsfile);
        }
    }

    /**
     * While <code>benchmarkComplete</code> is set to false, run as many
     * synchronous procedure calls as possible and record the results.
     *
     */
    class KVThread implements Runnable {

        final boolean m_sp;
        final Client m_client;

        KVThread(Client client, boolean sp)
        {
            m_sp = sp;
            m_client = client;
        }

        @Override
        public void run() {
            while (warmupComplete.get() == false) {
                if (m_sp) {
                    // Decide whether to perform a GET or PUT operation
                    if (rand.nextDouble() < config.getputratio) {
                        // Get a key/value pair, synchronously
                        try {
                            m_client.callProcedure("Get", processor.generateRandomKeyForRetrieval());
                        }
                        catch (Exception e) {}
                    }
                    else {
                        // Put a key/value pair, synchronously
                        final PayloadProcessor.Pair pair = processor.generateForStore();
                        try {
                            m_client.callProcedure("Put", pair.Key, pair.getStoreValue());
                        }
                        catch (Exception e) {}
                    }
                }
                else {
                    if (rand.nextDouble() < config.mpgetputratio) {
                        try {
                            m_client.callProcedure("GetMP", processor.generateRandomKeyForRetrieval());
                        }
                        catch (Exception e) {}
                    }
                    else {
                        try {
                            String [] keys = processor.generateAdjecentKeys(config.mpputpartitions);
                            final PayloadProcessor.Pair pair = processor.generateForStore();
                            m_client.callProcedure(
                                    (config.mpputoptimistic ? "OptimisticPutsMP" : "PutsMP"),
                                    pair.getStoreValue(),
                                    keys
                                    );
                        }
                        catch (Exception e) {}
                    }
                }
            }

            while (benchmarkComplete.get() == false) {
                if (m_sp) {
                    // Decide whether to perform a GET or PUT operation
                    if (rand.nextDouble() < config.getputratio) {
                        // Get a key/value pair, synchronously
                        try {
                            ClientResponse response = m_client.callProcedure("Get",
                                    processor.generateRandomKeyForRetrieval());

                            final VoltTable pairData = response.getResults()[0];
                            // Cache miss (Key does not exist)
                            if (pairData.getRowCount() == 0)
                                missedGets.incrementAndGet();
                            else {
                                final PayloadProcessor.Pair pair =
                                    processor.retrieveFromStore(pairData.fetchRow(0).getString(0),
                                            pairData.fetchRow(0).getVarbinary(1));
                                successfulGets.incrementAndGet();
                                networkGetData.addAndGet(pair.getStoreValueLength());
                                rawGetData.addAndGet(pair.getRawValueLength());
                            }
                        }
                        catch (Exception e) {
                            failedGets.incrementAndGet();
                        }
                    }
                    else {
                        // Put a key/value pair, synchronously
                        final PayloadProcessor.Pair pair = processor.generateForStore();
                        try {
                            m_client.callProcedure("Put", pair.Key, pair.getStoreValue());
                            successfulPuts.incrementAndGet();
                        }
                        catch (Exception e) {
                            failedPuts.incrementAndGet();
                        }
                        networkPutData.addAndGet(pair.getStoreValueLength());
                        rawPutData.addAndGet(pair.getRawValueLength());
                    }
                }
                else {
                    if (rand.nextDouble() < config.mpgetputratio) {
                        try {
                            ClientResponse response = m_client.callProcedure("GetMP",
                                    processor.generateRandomKeyForRetrieval());

                            final VoltTable pairData = response.getResults()[0];
                            // Cache miss (Key does not exist)
                            if (pairData.getRowCount() == 0)
                                missedGets.incrementAndGet();
                            else {
                                final PayloadProcessor.Pair pair =
                                        processor.retrieveFromStore(pairData.fetchRow(0).getString(0),
                                                pairData.fetchRow(0).getVarbinary(1));
                                successfulGets.incrementAndGet();
                                networkGetData.addAndGet(pair.getStoreValueLength());
                                rawGetData.addAndGet(pair.getRawValueLength());
                            }
                        }
                        catch (Exception e) {
                            failedGets.incrementAndGet();
                        }
                    }
                    else {
                        final String [] keys = processor.generateAdjecentKeys(config.mpputpartitions);
                        final PayloadProcessor.Pair pair = processor.generateForStore();

                        try {
                            m_client.callProcedure(
                                    (config.mpputoptimistic ? "OptimisticPutsMP" : "PutsMP"),
                                    pair.getStoreValue(),
                                    keys
                                    );
                            successfulPuts.incrementAndGet();
                        }
                        catch (Exception e) {
                            failedPuts.incrementAndGet();
                        }
                        networkPutData.addAndGet(pair.getStoreValueLength());
                        rawPutData.addAndGet(pair.getRawValueLength());
                    }
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
        connect(client, config.servers);
        connect(mpClient, config.servers);

        // preload keys if requested
        System.out.println();
        if (config.preload) {
            System.out.println("Preloading data store...");
            for(int i=0; i < config.poolsize; i++) {
                client.callProcedure(new NullCallback(),
                                     "Put",
                                     String.format(processor.KeyFormat, i),
                                     processor.generateForStore().getStoreValue());
            }
            client.drain();
            System.out.println("Preloading complete.\n");
        }

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // create/start the requested number of threads
        ArrayList<Thread> kvThreads = new ArrayList<Thread>();
        for (int i = 0; i < config.threads; ++i) {
            kvThreads.add(new Thread(new KVThread(client, true)));
        }
        for (int i = 0; i < config.mpthreads; ++i) {
            kvThreads.add(new Thread(new KVThread(mpClient, false)));
        }

        for (Thread thread : kvThreads) {
            thread.start();
        }

        // Run the benchmark loop for the requested warmup time
        System.out.println("Warming up...");
        Thread.sleep(1000l * config.warmup);

        // signal to threads to end the warmup phase
        warmupComplete.set(true);

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();
        mpFullStatsContext.fetchAndResetBaseline();

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
        client.drain();
        mpClient.drain();

        // join on the threads
        for (Thread t : kvThreads) {
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
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link KVConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        KVConfig config = new KVConfig();
        config.parse(OneShotBenchmark.class.getName(), args);

        OneShotBenchmark benchmark = new OneShotBenchmark(config);
        benchmark.runBenchmark();
    }
}
