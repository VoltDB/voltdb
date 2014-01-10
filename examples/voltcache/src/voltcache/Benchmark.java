/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
 * VoltCache provides a Memcache-like interface on a VoltDB database.
 *
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the cache, using the VoltCache interface.
 *
 * While this example demonstrates simple Get/Set operations, the VoltCache
 * interface exposes many more operations (Add, Replace, Prepend, Append,
 * Check-and-Set Increment/Decrement, etc.) and also provides asynchronous
 * cache interaction.
 *
 * You can review the IVoltCache interface for full details of the API.
 *
 * To use VoltCache in your application, simply import voltcache.api.*
 */

package voltcache;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.CLIConfig;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;

import voltcache.api.VoltCache;
import voltcache.procedures.VoltCacheProcBase;

public class Benchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final VoltCacheConfig config;
    // Reference to the database connection we will use in them main thread
    private VoltCache cache;
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
    ClientStatsContext periodicStatsContext;
    ClientStatsContext fullStatsContext;

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
    static class VoltCacheConfig extends CLIConfig {
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

        @Option(desc = "Fraction of ops that are gets (vs puts).")
        double getputratio = 0.90;

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

        @Option(desc = "Number of concurrent threads synchronously calling procedures.")
        int threads = 40;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

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

            if (threads <= 0) exitWithMessageAndUsage("threads must be > 0");
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
    public Benchmark(VoltCacheConfig config) {
        this.config = config;

        processor = new PayloadProcessor(config.keysize, config.minvaluesize,
                config.maxvaluesize, config.entropy, config.poolsize, config.usecompression);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
    }

    /*void connect(String servers) throws InterruptedException {
        System.out.println("Connecting to VoltDB...");

        cache = new VoltCache(config.servers);
        fullStatsContext = cache.getStatistics();
    }*/

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
        System.out.printf("Avg/95%% Latency %.2f/%dms\n", stats.getAverageLatency(),
                stats.kPercentileLatency(0.95));
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
        System.out.printf("10th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.1));
        System.out.printf("25th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.25));
        System.out.printf("50th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.5));
        System.out.printf("75th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.75));
        System.out.printf("90th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.9));
        System.out.printf("95th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.95));
        System.out.printf("99th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.99));
        System.out.printf("99.5th percentile latency:     %,9d ms\n", stats.kPercentileLatency(.995));
        System.out.printf("99.9th percentile latency:     %,9d ms\n", stats.kPercentileLatency(.999));

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" Latency Histogram");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(stats.latencyHistoReport());

        // 3. Write stats to file if requested
        cache.saveStatistics(stats, config.statsfile);
    }

    /**
     * While <code>benchmarkComplete</code> is set to false, run as many
     * synchronous procedure calls as possible and record the results.
     *
     */
    class VoltCacheThread implements Runnable {

        @Override
        public void run() {
            while (warmupComplete.get() == false) {
                // Decide whether to perform a GET or PUT operation
                if (rand.nextDouble() < config.getputratio) {
                    // Get a key/value pair, synchronously
                    try {
                        cache.get(processor.generateRandomKeyForRetrieval());
                    }
                    catch (Exception e) {}
                }
                else {
                    // Put a key/value pair, synchronously
                    final PayloadProcessor.Pair pair = processor.generateForStore();
                    try {
                        cache.set(pair.Key, 0, 0, pair.getStoreValue(), false);
                    }
                    catch (Exception e) {}
                }
            }

            while (benchmarkComplete.get() == false) {
                // Decide whether to perform a GET or PUT operation
                if (rand.nextDouble() < config.getputratio) {
                    // Get a key/value pair, synchronously
                    try {
                        String key = processor.generateRandomKeyForRetrieval();
                        VoltCacheProcBase.Result result = cache.get(key);
                        // Cache miss (Key does not exist)
                        if (result.data.size() == 0)
                            missedGets.incrementAndGet();
                        else {
                            final PayloadProcessor.Pair pair =
                                    processor.retrieveFromStore(key, result.data.get(key).Value);
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
                        cache.set(pair.Key, 0, 0, pair.getStoreValue(), false);
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
        System.out.println("Connecting to VoltDB...");
        cache = new VoltCache(config.servers);

        fullStatsContext = cache.getStatistics();
        periodicStatsContext = cache.getStatistics();

        // preload keys if requested
        System.out.println();
        if (config.preload) {
            System.out.println("Preloading data store...");
            for(int i=0; i < config.poolsize; i++) {
                cache.set(String.format(processor.KeyFormat, i),
                          0,
                          0,
                          processor.generateForStore().getStoreValue(),
                          true);
            }
            System.out.println("Preloading complete.\n");
        }

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // create/start the requested number of threads
        Thread[] cacheThreads = new Thread[config.threads];
        for (int i = 0; i < config.threads; ++i) {
            cacheThreads[i] = new Thread(new VoltCacheThread());
            cacheThreads[i].start();
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

        // join on the threads
        for (Thread t : cacheThreads) {
            t.join();
        }

        // print the summary results
        printResults();

        // close down the client connections
        cache.close();
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link VoltCacheConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        VoltCacheConfig config = new VoltCacheConfig();
        config.parse(Benchmark.class.getName(), args);

        Benchmark benchmark = new Benchmark(config);
        benchmark.runBenchmark();
    }
}
