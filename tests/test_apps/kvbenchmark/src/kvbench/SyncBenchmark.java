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
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the database, using the native VoltDB client
 * library.
 *
 * While synchronous processing can cause performance bottlenecks (each
 * caller waits for a transaction answer before calling another
 * transaction), the VoltDB cluster at large is still able to perform at
 * blazing speeds when many clients are connected to it.
 */

package kvbench;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
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
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NullCallback;

import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Throwables;

public class SyncBenchmark {

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
    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;
    // Graphite logger
    GraphiteLogger graphite = null;
    // CSV logger
    CsvLogger csvlogger = null;

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

    static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

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

        @Option(desc = "Graphite server hostname")
        String graphitehost = "";

        @Option(desc = "Filename to write periodic stat infomation in CSV format")
        String csvfile = "";

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

    static class GraphiteLogger implements AutoCloseable {
        final static String METRIC_PREFIX = "volt.kv.";
        final Socket m_socket;
        final PrintWriter m_writer;

        public GraphiteLogger(final String host) {
            Preconditions.checkArgument(host != null && !host.trim().isEmpty(), "host is null or emtpy");

            InetSocketAddress addr = new InetSocketAddress(host, 2003);
            m_socket = new Socket();

            PrintWriter pw = null;
            try {
                m_socket.connect(addr,2000);
                pw = new PrintWriter(m_socket.getOutputStream(),true);
            } catch (IOException ioex) {
                Throwables.propagate(ioex);
            }
            m_writer = pw;
        }

        @Override
        public void close() throws IOException {
            m_writer.close();
            m_socket.close();
        }

        public void log(final ClientStats stats) {
            if (stats == null) return;

            double now = stats.getEndTimestamp() / 1000.0;

            m_writer.printf("volt.kv.aborts %d %.3f\n", stats.getInvocationAborts(), now);
            m_writer.printf("volt.kv.errors %d %.3f\n", stats.getInvocationErrors(), now);
            m_writer.printf("volt.kv.latency.average %f %.3f\n", stats.getAverageLatency(), now);
            m_writer.printf("volt.kv.latency.five9s %.2f %.3f\n", stats.kPercentileLatencyAsDouble(0.99999), now);
            m_writer.printf("volt.kv.completed %d %.3f\n", stats.getInvocationsCompleted(), now);
            m_writer.printf("volt.kv.throughput %d %.3f\n", stats.getTxnThroughput(), now);
        }
    }

    static class CsvLogger implements AutoCloseable {
        final PrintWriter m_writer;
        final SimpleDateFormat m_df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        public CsvLogger(String csvFN) {
            Preconditions.checkArgument(csvFN != null && !csvFN.trim().isEmpty(),"file name is null or empty");
            File fh = new File(csvFN);
            PrintWriter pw = null;
            try {
                // 'true' to flush the buffer every println/printf
                pw = new PrintWriter(new FileWriter(fh), true);
            } catch (IOException ioex) {
                Throwables.propagate(ioex);
            }
            m_writer = pw;
            pw.println("TIMESTAMP,TSMILLIS,COMPLETED,ABORTS,ERRORS,TIMEOUTS,THROUGHPUT,AVERAGE_LATENCY,TWO9S_LATENCY,THREE9S_LATENCY,FOUR9S_LATENCY,FIVE9S_LATENCY");
        }

        @Override
        public void close() throws IOException {
            m_writer.close();
        }

        public void log(final ClientStats stats) {
            String ts = m_df.format(new Date(stats.getEndTimestamp()));
            m_writer.printf("%s,%d,%d,%d,%d,%d,%d,%.4f,%.4f,%.4f,%.4f,%.4f\n",
                    ts,                                        // col 00 string timestamp
                    stats.getEndTimestamp(),                   // col 01 long   timestamp millis
                    stats.getInvocationsCompleted(),           // col 02 long   invocations completed
                    stats.getInvocationAborts(),               // col 03 long   invocation aborts
                    stats.getInvocationErrors(),               // col 04 long   invocation errors
                    stats.getInvocationTimeouts(),             // col 05 long   invocation timeouts
                    stats.getTxnThroughput(),                  // col 06 long   transaction throughput
                    stats.getAverageLatency(),                 // col 07 double average latency
                    stats.kPercentileLatencyAsDouble(0.99),    // col 08 double two nines latency
                    stats.kPercentileLatencyAsDouble(0.999),   // col 09 double three nines latency
                    stats.kPercentileLatencyAsDouble(0.9999),  // col 10 double four nines latency
                    stats.kPercentileLatencyAsDouble(0.99999)  // col 11 double five nines latency
                    );
        }
    }

    void logMetric(final ClientStats stats) {
        if (graphite != null) {
            graphite.log(stats);
        }
        if (csvlogger != null) {
            csvlogger.log(stats);
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public SyncBenchmark(KVConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig("", "");
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        if (config.graphitehost != null && !config.graphitehost.trim().isEmpty()) {
            graphite = new GraphiteLogger(config.graphitehost);
        }

        if (config.csvfile != null && !config.csvfile.trim().isEmpty()) {
            csvlogger = new CsvLogger(config.csvfile);
        }

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

        // Print an ISO8601 timestamp (of the same kind Python logging uses) to help
        // log merger correlate correctly
        System.out.print(LOG_DF.format(new Date(stats.getEndTimestamp())));
        System.out.printf(" Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/99.999%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.99999));

        logMetric(stats);
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
        System.out.printf("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1));
        System.out.printf("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25));
        System.out.printf("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5));
        System.out.printf("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75));
        System.out.printf("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9));
        System.out.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
        System.out.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));
        System.out.printf("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995));
        System.out.printf("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999));
        System.out.printf("99.999th percentile latency:   %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99999));

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" Latency Histogram");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(stats.latencyHistoReport());

        // 3. Write stats to file if requested
        client.writeSummaryCSV(stats, config.statsfile);
    }

    /**
     * While <code>benchmarkComplete</code> is set to false, run as many
     * synchronous procedure calls as possible and record the results.
     *
     */
    class KVThread implements Runnable {

        @Override
        public void run() {
            while (warmupComplete.get() == false) {
                // Decide whether to perform a GET or PUT operation
                if (rand.nextDouble() < config.getputratio) {
                    // Get a key/value pair, synchronously
                    try {
                        client.callProcedure("Get", processor.generateRandomKeyForRetrieval());
                    }
                    catch (Exception e) {}
                }
                else {
                    // Put a key/value pair, synchronously
                    final PayloadProcessor.Pair pair = processor.generateForStore();
                    try {
                        client.callProcedure("Put", pair.Key, pair.getStoreValue());
                    }
                    catch (Exception e) {}
                }
            }

            while (benchmarkComplete.get() == false) {
                // Decide whether to perform a GET or PUT operation
                if (rand.nextDouble() < config.getputratio) {
                    // Get a key/value pair, synchronously
                    try {
                        ClientResponse response = client.callProcedure("Get",
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
                        client.callProcedure("Put", pair.Key, pair.getStoreValue());
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
        connect(config.servers);

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
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // create/start the requested number of threads
        Thread[] kvThreads = new Thread[config.threads];
        for (int i = 0; i < config.threads; ++i) {
            kvThreads[i] = new Thread(new KVThread());
            kvThreads[i].start();
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

        // block until all outstanding txns return
        client.drain();

        // join on the threads
        for (Thread t : kvThreads) {
            t.join();
        }

        // print the summary results
        printResults();

        // close down the client connections
        client.close();

        // if enabled close the graphite logger
        if (graphite != null) {
            graphite.close();
        }

        // if enabled close the csv logger
        if (csvlogger != null) {
            csvlogger.close();
        }
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
        config.parse(SyncBenchmark.class.getName(), args);

        SyncBenchmark benchmark = new SyncBenchmark(config);
        benchmark.runBenchmark();
    }
}
