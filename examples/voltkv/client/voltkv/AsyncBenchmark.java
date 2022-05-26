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
 * self-tuning to target a specific latency (10ms by default).
 * This tuning process, as demonstrated here, is important and should be
 * part of your pre-launch evalution so you can adequately provision your
 * VoltDB cluster with the number of servers required for your needs.
 */

package voltkv;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
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
import org.voltdb.client.ProcedureCallback;

public class AsyncBenchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final KVConfig config;
    // Reference to the database connection we will use
    final Client client;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Get a payload generator to create random Key-Value pairs to store in the database
    //  and process (uncompress) pairs retrieved from the database.
    final PayloadProcessor processor;
    // random number generator with constant seed
    final Random rand = new Random(0);
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

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
        int duration = 120;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 5;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of keys to preload.")
        int poolsize = 100000;

        // Allows separating keys when multiple clients are running, e.g against
        // different clusters in XDCR configurations. Combine with poolSize to separate
        // key values entirely, or allow partial or complete overlap.
        @Option(desc = "Key offset, default = 0")
        int offset = 0;

        @Option(desc = "Whether to preload a specified number of keys and values.")
        boolean preload = true;

        @Option(desc = "Fraction of ops that are to STORE (vs STORER), default is SP.")
        double multisingleratio = 0.00;

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

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Report latency for async benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Enable topology awareness")
        boolean topologyaware = false;

        @Option(desc = "Enable SSL, Optionally provide configuration file.")
        String sslfile = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (poolsize <= 0) exitWithMessageAndUsage("poolsize must be > 0");
            if (offset < 0) exitWithMessageAndUsage("offset must be >= 0");
            if (getputratio < 0) exitWithMessageAndUsage("getputratio must be >= 0");
            if (getputratio > 1) exitWithMessageAndUsage("getputratio must be <= 1");

            if (keysize <= 0) exitWithMessageAndUsage("keysize must be > 0");
            if (keysize > 250) exitWithMessageAndUsage("keysize must be <= 250");
            if (minvaluesize <= 0) exitWithMessageAndUsage("minvaluesize must be > 0");
            if (maxvaluesize <= 0) exitWithMessageAndUsage("maxvaluesize must be > 0");
            if (entropy <= 0) exitWithMessageAndUsage("entropy must be > 0");
            if (entropy > 127) exitWithMessageAndUsage("entropy must be <= 127");

            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
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
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public AsyncBenchmark(KVConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
        if (config.sslfile.trim().length() > 0) {
            clientConfig.setTrustStoreConfigFromPropertyFile(config.sslfile);
            clientConfig.enableSSL();
        }
        clientConfig.setMaxTransactionsPerSecond(config.ratelimit);

        if (config.topologyaware) {
            clientConfig.setTopologyChangeAware(true);
        }

        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        processor = new PayloadProcessor(config.offset, config.keysize, config.minvaluesize,
                config.maxvaluesize, config.entropy, config.poolsize, config.usecompression);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
        if(config.latencyreport) {
            System.out.println("NOTICE: Option latencyreport is ON for async run, please set a reasonable ratelimit.\n");
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
        if (config.topologyaware) {
            connectToOneServerWithRetry(serverArray[0]);
        } else {
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
        System.out.printf("Aborts/Failures %d/%d",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        if(this.config.latencyreport) {
            System.out.printf(", Avg/95%% Latency %.2f/%.2fms", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.95));
        }
        System.out.printf("\n");
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
        if(this.config.latencyreport) {
            System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
            System.out.printf("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1));
            System.out.printf("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25));
            System.out.printf("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5));
            System.out.printf("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75));
            System.out.printf("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9));
            System.out.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
            System.out.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));
            System.out.printf("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995));
            System.out.printf("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999));

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" System Server Statistics");
            System.out.println(HORIZONTAL_RULE);
            System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" Latency Histogram");
            System.out.println(HORIZONTAL_RULE);
            System.out.println(stats.latencyHistoReport());
        }

        // 3. Write stats to file if requested
        client.writeSummaryCSV(stats, config.statsfile);
    }

    /**
     * Callback to handle the response to a stored procedure call.
     * Tracks response types.
     *
     */
    class GetCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            // Track the result of the operation (Success, Failure, Payload traffic...)
            if (response.getStatus() == ClientResponse.SUCCESS) {
                final VoltTable pairData = response.getResults()[0];
                // Cache miss (Key does not exist)
                if (pairData.getRowCount() == 0) {
                    missedGets.incrementAndGet();
                }
                else {
                    final PayloadProcessor.Pair pair =
                            processor.retrieveFromStore(pairData.fetchRow(0).getString(0),
                                                        pairData.fetchRow(0).getVarbinary(1));
                    successfulGets.incrementAndGet();
                    networkGetData.addAndGet(pair.getStoreValueLength());
                    rawGetData.addAndGet(pair.getRawValueLength());
                }
            }
            else {
                failedGets.incrementAndGet();
            }
        }
    }

    class PutCallback implements ProcedureCallback {
        final long storeValueLength;
        final long rawValueLength;

        PutCallback(PayloadProcessor.Pair pair) {
            storeValueLength = pair.getStoreValueLength();
            rawValueLength = pair.getRawValueLength();
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            // Track the result of the operation (Success, Failure, Payload traffic...)
            if (response.getStatus() == ClientResponse.SUCCESS) {
                successfulPuts.incrementAndGet();
            }
            else {
                failedPuts.incrementAndGet();
            }
            networkPutData.addAndGet(storeValueLength);
            rawPutData.addAndGet(rawValueLength);
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

        // preload keys if requested
        System.out.println();
        if (config.preload) {
            System.out.println("Preloading data store...");
            for(int i=0; i < config.poolsize; i++) {
                String keyStr = String.format(processor.KeyFormat, i + config.offset);
                byte[] valArr = processor.generateForStore().getStoreValue();
                if (config.multisingleratio != 0.00) {
                    client.callProcedure(new NullCallback(),
                                         "STORER.upsert",
                                         keyStr,
                                         valArr);
                }
                client.callProcedure(new NullCallback(),
                                     "STORE.upsert",
                                     keyStr,
                                     valArr);
            }
            client.drain();
            System.out.println("Preloading complete.\n");
        }

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        System.out.println("Warming up...");
        final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
        while (warmupEndTime > System.currentTimeMillis()) {
            // Decide whether to perform a GET or PUT operation
            if (rand.nextDouble() < config.getputratio) {
                // Get a key/value pair using inbuilt select procedure, asynchronously
                if (rand.nextDouble() < config.multisingleratio) {
                    client.callProcedure(new NullCallback(), "selectR", processor.generateRandomKeyForRetrieval());
                }
                else {
                    client.callProcedure(new NullCallback(), "STORE.select", processor.generateRandomKeyForRetrieval());
                }
            }
            else {
                String table = rand.nextDouble() < config.multisingleratio ? "STORER" : "STORE";
                // Put a key/value pair using inbuilt upsert procedure, asynchronously
                final PayloadProcessor.Pair pair = processor.generateForStore();
                client.callProcedure(new NullCallback(), table+".upsert", pair.Key, pair.getStoreValue());
            }
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
            // Decide whether to perform a GET or PUT operation
            if (rand.nextDouble() < config.getputratio) {
                // Get a key/value pair using inbuilt select procedure, asynchronously
                if (rand.nextDouble() < config.multisingleratio) {
                    client.callProcedure(new GetCallback(), "selectR", processor.generateRandomKeyForRetrieval());
                }
                else {
                    client.callProcedure(new GetCallback(), "STORE.select", processor.generateRandomKeyForRetrieval());
                }
            }
            else {
                String table = rand.nextDouble() < config.multisingleratio ? "STORER" : "STORE";
                // Put a key/value pair using inbuilt upsert procedure, asynchronously
                final PayloadProcessor.Pair pair = processor.generateForStore();
                client.callProcedure(new PutCallback(pair), table+".upsert", pair.Key, pair.getStoreValue());
            }
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
        config.parse(AsyncBenchmark.class.getName(), args);

        AsyncBenchmark benchmark = new AsyncBenchmark(config);
        benchmark.runBenchmark();
    }
}
