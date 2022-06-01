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
package voltkvqa;

import java.util.Random;
import java.util.Timer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.voltdb.CLIConfig;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NullCallback;

public class HTTPBenchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE
            = "----------" + "----------" + "----------" + "----------"
            + "----------" + "----------" + "----------" + "----------" + "\n";

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
     * Uses included {@link CLIConfig} class to declaratively state command line options with defaults and validation.
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
        boolean usecompression = false;

        @Option(desc = "Number of concurrent threads synchronously calling procedures.")
        int threads = 40;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "disable client affinity (as of V11, this has no effect).")
        boolean noclientaffinity = false;

        @Option(desc = "user id.")
        String username = "";

        @Option(desc = "password.")
        String password = "";

        @Option(desc = "kerberos")
        boolean kerberos = false;

        @Override
        public void validate() {
            if (duration <= 0) {
                exitWithMessageAndUsage("duration must be > 0");
            }
            if (warmup < 0) {
                exitWithMessageAndUsage("warmup must be >= 0");
            }
            if (displayinterval <= 0) {
                exitWithMessageAndUsage("displayinterval must be > 0");
            }
            if (poolsize <= 0) {
                exitWithMessageAndUsage("poolsize must be > 0");
            }
            if (getputratio < 0) {
                exitWithMessageAndUsage("getputratio must be >= 0");
            }
            if (getputratio > 1) {
                exitWithMessageAndUsage("getputratio must be <= 1");
            }

            if (keysize <= 0) {
                exitWithMessageAndUsage("keysize must be > 0");
            }
            if (keysize > 250) {
                exitWithMessageAndUsage("keysize must be <= 250");
            }
            if (minvaluesize <= 0) {
                exitWithMessageAndUsage("minvaluesize must be > 0");
            }
            if (maxvaluesize <= 0) {
                exitWithMessageAndUsage("maxvaluesize must be > 0");
            }
            if (entropy <= 0) {
                exitWithMessageAndUsage("entropy must be > 0");
            }
            if (entropy > 127) {
                exitWithMessageAndUsage("entropy must be <= 127");
            }

            if (threads <= 0) {
                exitWithMessageAndUsage("threads must be > 0");
            }
        }
    }

    /**
     * Constructor for benchmark instance. Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public HTTPBenchmark(KVConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig(config.username, config.password);
        clientConfig.setReconnectOnConnectionLoss(true);
        if (config.kerberos) {
            clientConfig.enableKerberosAuthentication("VoltDBClient");
        }

        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        processor = new PayloadProcessor(config.keysize, config.minvaluesize,
                config.maxvaluesize, config.entropy, config.poolsize, config.usecompression);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff. No timeout. This will run until the process
     * is killed if it's not able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServerWithRetry(String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            } catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try {
                    Thread.sleep(sleep);
                } catch (Exception interruted) {
                }
                if (sleep < 8000) {
                    sleep += sleep;
                }
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until connection. This call will block until all have
     * connected.
     *
     * @param servers A comma separated list of servers using the hostname:port syntax (where :port is optional).
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
     * Prints the results of the test and statistics about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // 1. Get/Put performance results
        String display = "\n"
                + HORIZONTAL_RULE
                + " KV Store Results\n"
                + HORIZONTAL_RULE
                + "\nA total of %,d operations were posted...\n"
                + " - GETs: %,9d Operations (%,d Misses and %,d Failures)\n"
                + "         %,9d MB in compressed store data\n"
                + "         %,9d MB in uncompressed application data\n"
                + "         Network Throughput: %6.3f Gbps*\n"
                + " - PUTs: %,9d Operations (%,d Failures)\n"
                + "         %,9d MB in compressed store data\n"
                + "         %,9d MB in uncompressed application data\n"
                + "         Network Throughput: %6.3f Gbps*\n"
                + " - Total Network Throughput: %6.3f Gbps*\n\n"
                + " - Transactions/second (GET+PUT): %6.3f TPS*\n\n"
                + "* Figure includes key & value traffic but not database protocol overhead.\n\n";

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
                getThroughput + putThroughput,
                (successfulGets.get() + successfulPuts.get()) / (double) config.duration);

        client.writeSummaryCSV(stats, config.statsfile);
    }

    /**
     * While <code>benchmarkComplete</code> is set to false, run as many synchronous procedure calls as possible and
     * record the results.
     *
     */
    class KVThread implements Runnable {

        private final CloseableHttpClient m_httpClient;
        private final HttpPost m_httpPost;

        // constructor  --  each thread gets its http connection
        public KVThread(CloseableHttpClient httpClient, HttpPost httpPost) {
            m_httpClient = httpClient;
            m_httpPost = httpPost;
        }

        @Override
        public void run() {
            while (warmupComplete.get() == false) {
                // Decide whether to perform a GET or PUT operation
                if (rand.nextDouble() < config.getputratio) {
                    // Get a key/value pair, synchronously
                    try {
                        HTTPUtils.Response response = HTTPUtils.callProcedure("Get", processor.generateRandomKeyForRetrieval(), m_httpClient, m_httpPost);
                        if (response.status != ClientResponse.SUCCESS) {
                            System.err.println(response.statusString);
                            System.out.println("ERROR: Bad Client response from HTTPBenchmark Get");
                            System.exit(1);
                        }
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                        System.exit(1);
                    }
                } else {
                    // Put a key/value pair, synchronously
                    final PayloadProcessor.Pair pair = processor.generateForStore();
                    try {
                        HTTPUtils.Response response = HTTPUtils.callProcedure("Put", pair.Key, pair.getStoreValue(), m_httpClient, m_httpPost);
                        if (response.status != ClientResponse.SUCCESS) {
                            System.err.println(response.statusString);
                            System.out.println("ERROR: Bad Client response from HTTPBenchmark Put");
                            System.exit(1);
                        }
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                        System.exit(1);
                    }
                }
            }

            while (benchmarkComplete.get() == false) {
                // Decide whether to perform a GET or PUT operation
                if (rand.nextDouble() < config.getputratio) {
                    // Get a key/value pair, synchronously
                    try {
                        HTTPUtils.Response response = HTTPUtils.callProcedure("Get",
                                processor.generateRandomKeyForRetrieval(), m_httpClient, m_httpPost);

                        if (response.status != ClientResponse.SUCCESS) {
                            System.err.println(response.statusString);
                            System.out.println("ERROR: Bad Client response from HTTPBenchmark Get");
                            System.exit(1);
                        }

                        if (response.results[0].advanceRow()) {

                            final VoltTable pairData = response.results[0];
                            // Cache miss (Key does not exist)
                            if (pairData.getRowCount() == 0) {
                                missedGets.incrementAndGet();
                            } else {
                                final PayloadProcessor.Pair pair
                                        = processor.retrieveFromStore(pairData.fetchRow(0).getString(0),
                                                pairData.fetchRow(0).getVarbinary(1));
                                successfulGets.incrementAndGet();
                                networkGetData.addAndGet(pair.getStoreValueLength());
                                rawGetData.addAndGet(pair.getRawValueLength());
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                        System.exit(1);
                        failedGets.incrementAndGet();
                    }
                } else {
                    // Put a key/value pair, synchronously
                    final PayloadProcessor.Pair pair = processor.generateForStore();
                    try {
                        HTTPUtils.Response response = HTTPUtils.callProcedure("Put", pair.Key, pair.getStoreValue(), m_httpClient, m_httpPost);
                        if (response.status != ClientResponse.SUCCESS) {
                            System.err.println(response.statusString);
                            System.out.println("ERROR: Bad Client response from HTTPBenchmark Get");
                            System.exit(1);
                        }
                        successfulPuts.incrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace(System.out);
                        System.exit(1);
                        failedPuts.incrementAndGet();
                    }
                    networkPutData.addAndGet(pair.getStoreValueLength());
                    rawPutData.addAndGet(pair.getRawValueLength());
                }
            }
        }
    }

    /**
     * Core benchmark code. Connect. Initialize. Run the loop. Cleanup. Print Results.
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
            for (int i = 0; i < config.poolsize; i++) {
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

        // setup the HTTP connection pool that will be used by the threads
        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        cm.setMaxTotal(config.threads * 2);
        cm.setDefaultMaxPerRoute(config.threads);
        CloseableHttpClient httpclient = HttpClients.custom().setConnectionManager(cm).build();
        String[] servers = config.servers.split(",");

        // create/start the requested number of threads
        Thread[] kvThreads = new Thread[config.threads];
        for (int i = 0; i < config.threads; ++i) {
            HttpPost httppost = new HttpPost("http://" + servers[i % servers.length] + ":8080/api/1.0/");
            kvThreads[i] = new Thread(new KVThread(httpclient, httppost));
            kvThreads[i].start();
        }

        // Run the benchmark loop for the requested warmup time
        System.out.println("Warming up...");
        Thread.sleep(1000l * config.warmup);

        // signal to threads to end the warmup phase
        warmupComplete.set(true);

        // Run the benchmark loop for the requested warmup time
        System.out.println("\nRunning benchmark...");
        Thread.sleep(1000l * config.duration);

        // stop the threads
        benchmarkComplete.set(true);

        // cancel periodic stats printing
        // timer.cancel();
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
        config.parse(HTTPBenchmark.class.getName(), args);

        HTTPBenchmark benchmark = new HTTPBenchmark(config);
        benchmark.runBenchmark();
    }
}
