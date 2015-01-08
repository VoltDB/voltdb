/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package voltkvqa;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;

public class AsyncBenchmark {
    boolean slow = false;
    boolean debug = false;

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
    private AtomicInteger totalConnections = new AtomicInteger(0);
    private static AtomicInteger fatalLevel = new AtomicInteger(0);

    private static final AtomicLong successfulPutCount = new AtomicLong(0);
    private static final AtomicLong missingPutCount = new AtomicLong(0);
    private static final AtomicLong incorrectPutCount = new AtomicLong(0);
    private static Map<String, Long> hashMap = Collections.synchronizedMap(new HashMap<String, Long>());

    final AtomicLong successfulGetsMPT = new AtomicLong(0);
    final AtomicLong successfulGetsMPF = new AtomicLong(0);
    final AtomicLong successfulPutsMPT = new AtomicLong(0);
    final AtomicLong successfulPutsMPF = new AtomicLong(0);

    long lastSuccessfulResponse = 0;


    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class KVConfig extends CLIConfig {
        @Option(desc="If it's set false, it'll quit right away when lost all connections.")
        boolean recover = true;

        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 120;

        @Option(desc = "Max number of puts. This does not count any preloading or warmup and may not be reached if duration is too short.")
        long maxputs = Long.MAX_VALUE;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 0;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of keys to preload.")
        int poolsize = 100000;

        @Option(desc = "Whether to preload a specified number of keys and values.")
        boolean preload = true;

        @Option(desc = "Preload starting key value.")
        int preloadLowKey = 0;

        @Option(desc = "Whether to run the benchmark (permits exiting after the preload.")
        boolean runbenchmark = true;

        @Option(desc = "Controls if counts are checked at end of run.")
        boolean checkdata = true;

        @Option(desc = "Fraction of ops that are gets (vs puts).")
        double getputratio = 0.90;

        @Option(desc = "Fraction of ops that are gets (singlePartition vs multiPartition) " +
                "and puts (singlePartition vs multiPartition).")
        double multisingleratio = 0; // By default, don't run multi-partition

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

        @Option(desc = "Determine transaction rate dynamically based on latency.")
        boolean autotune = false;

        @Option(desc = "Server-side latency target for auto-tuning.")
        int latencytarget = 5;

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

            if (multisingleratio < 0) exitWithMessageAndUsage("multisingleratio must be >= 0");
            if (multisingleratio > 1) exitWithMessageAndUsage("multisingleratio must be <= 1");

            if (keysize <= 0) exitWithMessageAndUsage("keysize must be > 0");
            if (keysize > 250) exitWithMessageAndUsage("keysize must be <= 250");
            if (minvaluesize <= 0) exitWithMessageAndUsage("minvaluesize must be > 0");
            if (maxvaluesize <= 0) exitWithMessageAndUsage("maxvaluesize must be > 0");
            if (entropy <= 0) exitWithMessageAndUsage("entropy must be > 0");
            if (entropy > 127) exitWithMessageAndUsage("entropy must be <= 127");

            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
            if (latencytarget <= 0) exitWithMessageAndUsage("latencytarget must be > 0");
            if (preloadLowKey < 0) exitWithMessageAndUsage("preloadlowkey must be >= 0");
        }
    }

    /**
     * Fake an internal jstack to the log
     */
    static public void printJStack() {
        prt(new Date().toString() + " Full thread dump");

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
            prt(logline);
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
            long currentTime = System.currentTimeMillis();
            if ((currentTime - benchmarkStartTS) < (config.duration * 1000)) {
                if(debug) {
                    long diff = currentTime - benchmarkStartTS;
                    String msg = "Time remaining in seconds: " + diff/1000l +
                        ", connectionsLeft: " + connectionsLeft +
                        ", Total Connections detected by this client: " + totalConnections.get();
                    prt(msg);
                }
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
                totalConnections.decrementAndGet();
                if (!config.recover) {
                    if (totalConnections.get() == 0) {
                        //totalConnections.set(-1);
                        System.exit(1);
                    }
                }
            } // if ((currentTime - benchmarkStartTS) < (config.duration * 1000))
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
        clientConfig.setReconnectOnConnectionLoss(config.recover);

        if (config.autotune) {
            clientConfig.enableAutoTune();
            clientConfig.setAutoTuneTargetInternalLatency(config.latencytarget);
        }
        else {
            clientConfig.setMaxTransactionsPerSecond(config.ratelimit);
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
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServerWithRetry(String server, String info) {
        /*
         * Possible exceptions are:
         * 1) Exception: java.net.ConnectException: Connection refused
         * 2) Exception: java.io.IOException: Failed to authenticate to rejoining node
         * 3) Exception: java.io.IOException: Cluster instance id mismatch.
         * The third one could indicate a bug.
         */
        int sleep = 1000;
        boolean flag = true;
        String msg;
        if(fatalLevel.get() > 0) {
            System.err.printf("In connectToOneServerWithRetry, don't bother to try reconnecting to this host: %s\n", server);
            flag = false;
        }
        while (flag) {
            try {
                client.createConnection(server);
                totalConnections.incrementAndGet();
                msg = "Connected to VoltDB node at: " + server + ", IDs: " +
                       client.getInstanceId()[0] + " - " + client.getInstanceId()[1] +
                       ", totalConnections = " + totalConnections.get();
                prt(msg);
                break;
            }
            catch (Exception e) {
                if(debug) {
                    msg = "\n---------------\n" + info + "\n---------------\n" +
                        "Failed to connect server: " + server +
                        ", remaining totalConnections = '" + totalConnections.get() +
                        "'\nReason for connection failed: '" + e.getMessage() + "'";
                    prt(msg);
                }
                msg = "Connection to " + server + " failed - retrying in " + sleep/1000 + " second(s)";
                prt(msg);
                try {
                    Thread.sleep(sleep);
                } catch (Exception interruted) {
                    msg = "A possible hiccup, seriously?? Exception: '" + interruted.getMessage() +
                          "'.\nStop trying to reconnect to this host: " + server;
                    prt(msg);
                    fatalLevel.incrementAndGet();
                    flag = false;
                }

                if (sleep < 8000) sleep += sleep;
                if((e.getMessage().contains("Cluster instance id mismatch"))) {
                    msg = "A possible bug? Could be a serious exception:" + e.getMessage() +
                        "\nStop trying to reconnect to this host: " + server;
                    prt(msg);
                    fatalLevel.incrementAndGet();
                    flag = false;
                }
                else if((e.getMessage().contains("Client instance is shutdown"))) {
                    msg = "Dangling client is detected. Stop trying to connect to this host: " + server;
                    prt(msg);
                    flag = false;
                }
            }
        }
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    void connect(String servers, final String info) throws InterruptedException {
        String[] serverArray = servers.split(",");
        String msg;
        final CountDownLatch connections = new CountDownLatch(1);
        if(debug) {
            msg = "\n=========\n" + info + "\nIn connect Server counts: " + serverArray.length +
                    ", Server Names: " + servers;
            prt(msg);
        }

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(server, info);
                    connections.countDown();
                }
            }).start();
        }
        connections.await();
    }

    /**
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
     */
    public void schedulePeriodicStats() {
        timer = new Timer(true);
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
        try {
            ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();

            System.out.printf("%s ", dateformat(getTime()));
            System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
            System.out.printf("Aborts/Failures %d/%d, ",
                    stats.getInvocationAborts(), stats.getInvocationErrors());
            System.out.printf("Avg/95%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                    stats.kPercentileLatencyAsDouble(0.95));
            if(totalConnections.get() == -1 && stats.getTxnThroughput() == 0) {
                if(!config.recover) {
                    String errMsg = "Lost all connections. Exit...";
                    exitOnError(errMsg);
                }
            }
        } catch (Exception e) {
            String msg = "In printStatistics. We got an exception: '" + e.getMessage() + "'!!";
            prt(msg);
        }
        if (lastSuccessfulResponse > 0  && (System.currentTimeMillis() - lastSuccessfulResponse) > 6*60*1000) {
            prt("Not making any progress, last at " +
                    (new SimpleDateFormat("yyyy-MM-DD HH:mm:ss.S")).format(new Date(lastSuccessfulResponse)) + ", exiting");
            printJStack();
            System.exit(1);
        }
    }

    public void exitOnError(String err) {
        prt(err);

        // cancel periodic stats printing
        timer.cancel();

        // block until all outstanding txns return
        try {
            client.drain();
        } catch (NoConnectionsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // print the summary results
        try {
            printResults();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // close down the client connections
        try {
            client.close();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.exit(1);
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
                         "         %,9d  Multi-partition Operations (%.2f%%)\n" +
                         "         %,9d Single-partition Operations (%.2f%%)\n" +
                         "         %,9d MB in compressed store data\n" +
                         "         %,9d MB in uncompressed application data\n" +
                         "         Network Throughput: %6.3f Gbps*\n" +
                         " - PUTs: %,9d Operations (%,d Failures)\n" +
                         "         %,9d  Multi-partition Operations (%.2f%%)\n" +
                         "         %,9d Single-partition Operations (%.2f%%)\n" +
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

        long gtt = successfulGetsMPT.get() + successfulGetsMPF.get();
        long gst = successfulGetsMPT.get();
        long gsf = successfulGetsMPF.get();
        double getMptR = (double)(100.00*gst/gtt);
        double getMpfR = (double)(100.00*gsf/gtt);
        //System.out.printf("st = %d, sf = %d, tt = %d, getMptR = '%.2f%%', getMpfR = '%.2f%%',",
        //        gst, gsf, gtt, getMptR, getMpfR);
        long ptt = successfulPutsMPT.get() + successfulPutsMPF.get();
        long pst = successfulPutsMPT.get();
        long psf = successfulPutsMPF.get();
        double putMptR = (double)(100.00*pst/ptt);
        double putMpfR = (double)(100.00*psf/ptt);

        System.out.printf(display,
                stats.getInvocationsCompleted(),
                successfulGets.get(), missedGets.get(), failedGets.get(),
                successfulGetsMPT.get(), getMptR, successfulGetsMPF.get(), getMpfR,
                networkGetData.get() / oneMB,
                rawGetData.get() / oneMB,
                getThroughput,
                successfulPuts.get(), failedPuts.get(),
                successfulPutsMPT.get(), putMptR, successfulPutsMPF.get(), putMpfR,
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

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);

        if (config.autotune) {
            System.out.printf("Targeted Internal Avg Latency: %,9d ms\n", config.latencytarget);
        }
        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

        // 3. Write stats to file if requested
        client.writeSummaryCSV(stats, config.statsfile);
    }

    /**
     * Callback to handle the response to a stored procedure call.
     * Tracks response types.
     *
     */
    class GetCallback implements ProcedureCallback {
        double rand;
        GetCallback(double rand) {
            this.rand = rand;
        }
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
                    lastSuccessfulResponse = System.currentTimeMillis();
                    if(rand < config.multisingleratio)
                        successfulGetsMPT.incrementAndGet();
                    else
                        successfulGetsMPF.incrementAndGet();

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
        PayloadProcessor.Pair thisPair;
        double rand;

        PutCallback(PayloadProcessor.Pair pair, double rand) {
            storeValueLength = pair.getStoreValueLength();
            rawValueLength = pair.getRawValueLength();
            thisPair = pair;
            this.rand = rand;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            // Track the result of the operation (Success, Failure, Payload traffic...)
            if (response.getStatus() == ClientResponse.SUCCESS) {
                successfulPuts.incrementAndGet();
                if(rand < config.multisingleratio)
                    successfulPutsMPT.incrementAndGet();
                else
                    successfulPutsMPF.incrementAndGet();

                final VoltTable pairData = response.getResults()[0];
                final VoltTableRow tablerow = pairData.fetchRow(0);
                final long counter = tablerow.getLong(0);
                hashMap.put(thisPair.Key, counter);
                lastSuccessfulResponse = System.currentTimeMillis();
            }
            else {
                failedPuts.incrementAndGet();
            }
            networkPutData.addAndGet(storeValueLength);
            rawPutData.addAndGet(rawValueLength);
        }
    }

    class SumCallback implements ProcedureCallback {
        String key = null;
        int c;
        int i;
        public SumCallback(String key, int c, int i) {
            this.c = c; // line count
            this.i = i; // print interval
            this.key = key;
        }

        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() == ClientResponse.SUCCESS) {
                final VoltTable pairData = response.getResults()[0];
                if (pairData.getRowCount() != 0) {
                    successfulPutCount.incrementAndGet();
                    final long hashMapCount = hashMap.get(key);
                    final long dbCount = ByteBuffer.wrap(pairData.fetchRow(0).getVarbinary(1)).getLong(0);
                    if (dbCount < hashMapCount) {
                        missingPutCount.incrementAndGet();
                        incorrectPutCount.addAndGet(hashMapCount - dbCount);
                        System.out.printf("ERROR: Key %s: count in db '%d' is less than client expected '%d'\n",
                                          key.replaceAll("\\s", ""), dbCount, hashMapCount);
                    }
                }
            }
            else {
                System.out.print("ERROR: Bad Client response from Volt");
                System.exit(1);
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

        // preload keys if requested
        System.out.println();
        if (config.preload) {
            System.out.println("Preloading data store...");
            for(int i=config.preloadLowKey; i < config.poolsize; i++) {
                client.callProcedure(new NullCallback(),
                                     "Put",
                                     String.format(processor.KeyFormat, i),
                                     processor.generateForStore().getStoreValue());
                lastSuccessfulResponse = System.currentTimeMillis();
            }
            client.drain();
            System.out.println("Preloading complete.\n");
        }

        if (!config.runbenchmark) {
            System.out.println("Benchmark run disabled by --runbenchmark option, exiting now");
            System.exit(0);
        }

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        if (config.warmup > 0) {
            System.out.println("Warming up...");
            final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
            while (warmupEndTime > System.currentTimeMillis()) {
                // Decide whether to perform a GET or PUT operation
                if (rand.nextDouble() < config.getputratio) {
                    // Get a key/value pair, asynchronously
                    client.callProcedure(new NullCallback(), "Get", processor.generateRandomKeyForRetrieval());
                }
                else {
                    // Put a key/value pair, asynchronously
                    final PayloadProcessor.Pair pair = processor.generateForStore();
                    client.callProcedure(new NullCallback(), "Put", pair.Key, pair.getStoreValue());
                }
                lastSuccessfulResponse = System.currentTimeMillis();
            }
        }

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        if(totalConnections.get() == 1)
            // If Volt is running on one node only, no need to run this test on multi-partition
            config.multisingleratio = 0;

        // Run the benchmark loop for the requested duration or txn count
        // The throughput may be throttled depending on client configuration
        System.out.println("\nRunning benchmark...");
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        long currentTime = System.currentTimeMillis();
        long diff = benchmarkEndTime - currentTime;
        int i = 1;
        long putCount = 0;

        double mpRand;
        String msg = "";
        while (benchmarkEndTime > currentTime && putCount < config.maxputs) {
            if(debug && diff != 0 && diff%5000.00 == 0 && i%5 == 0) {
                msg = "i = " + i + ", Time remaining in seconds: " + diff/1000l +
                      ", totalConnections = " + totalConnections.get();
                prt(msg);
                i++;
            }
            if(totalConnections.get() < 1) {
                if(debug) {
                    msg = "i = " + i + ", diff = '" + diff +
                           ", totalConnections = " + totalConnections.get() + "\n";
                }
                msg += "All connections are lost! VoltDB could be down!!";
                prt(msg);
                System.exit(1);
            }

            // Decide whether to perform a GET or PUT operation
            if (rand.nextDouble() < config.getputratio) {
                // Get a key/value pair, asynchronously
                mpRand = rand.nextDouble();
                if(mpRand < config.multisingleratio) {
                    if(totalConnections.get() > 1 && config.poolsize > 10000) {
                        slow = true;
                        debug = true;
                    }
                    else
                        debug = false;
                    client.callProcedure(new GetCallback(mpRand), "GetMp", processor.generateRandomKeyForRetrieval());
                }
                else {
                    client.callProcedure(new GetCallback(mpRand), "Get", processor.generateRandomKeyForRetrieval());
                }
            }
            else {
                // Put a key/value pair, asynchronously
                putCount++;
                final PayloadProcessor.Pair pair = processor.generateForStore();
                mpRand = rand.nextDouble();
                if(rand.nextDouble() < config.multisingleratio) {
                    if(totalConnections.get() > 1 && config.poolsize > 10000) {
                        slow = true;
                        debug = true;
                    }
                    else
                        debug = false;
                    client.callProcedure(new PutCallback(pair, mpRand), "PutMp", pair.Key, pair.getStoreValue());
                }
                else {
                    client.callProcedure(new PutCallback(pair, mpRand), "Put", pair.Key, pair.getStoreValue());
                }
            }
            currentTime = System.currentTimeMillis();
            diff = benchmarkEndTime - currentTime;
        }
        timer.cancel();
    }

    public void closeClient() {
        // cancel periodic stats printing
        try {
            timer.cancel();
        } catch (Exception e) {
            String msg = "In closeOldClient exception = '" + e.getMessage() + "'.";
            prt(msg);
        }
        try {
            client.drain();
        } catch (NoConnectionsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // close down the client connections
        try {
            client.close();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void readyToExit() {
        // block until all outstanding txns return
        try {
            client.drain();
        } catch (NoConnectionsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        if (config.checkdata)
            summary4qa();

        try {
            client.drain();
        } catch (NoConnectionsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // close down the client connections
        try {
            client.close();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.exit(0);
    }

    public void summary4qa() {
        // print the summary results
        try {
            printResults();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.printf("\n\nChecking put count in the database against expected data");
        Iterator<String> it = hashMap.keySet().iterator();
        if(debug) {
            System.out.printf("HashMap Size: %10d\n", hashMap.size());
            System.out.println("poolsize = " + config.poolsize);
        }

        try {
            int printInterval = (int)hashMap.size()/5;
            if(hashMap.size() > 10000)
                if(slow)
                    printInterval = (int)config.poolsize/200;

            int c = 0;
            String lastkey = "error";
            while (it.hasNext()) {
                String key = it.next().toString();

                if(debug && c%printInterval == 0)
                    System.out.printf("in summary4qa() count = %d, key = '%s'\n", c, key);
                client.callProcedure(new SumCallback(key, c, printInterval), "Get", key);
                lastkey = key;
                c++;
            }
            if(debug)
                System.out.printf("In summary4qa, count = %d, Last Key = '%s'\n", c, lastkey);

        }
        catch (Exception x)    {
            System.out.println("This is not working: " + x + "\n");
            x.printStackTrace();
            System.exit(1);
        }

        try {
            client.drain();
        } catch (NoConnectionsException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("\n-------------------\nTest Results:\n-------------------\n");
        System.out.printf("\n\t%10d\tKV Rows Checked\n", hashMap.size());
        System.out.printf("\t%10d\tKV Rows Found in database\n", successfulPutCount.get());
        System.out.printf("\t%10d\tKV Rows Missing in database\n", hashMap.size() - successfulPutCount.get());
        System.out.printf("\n\t%10d\tKV Rows are Correct\n", successfulPutCount.get() - missingPutCount.get());
        System.out.printf("\t%10d\tKV Rows are Incorrect (off by %d Puts)\n", missingPutCount.get(), incorrectPutCount.get());

        if (successfulPutCount.get() == hashMap.size() && missingPutCount.get() == 0){
            System.out.println("\n-------------------\nGood News:  Database Put counts match\n");
        }
        else {
            System.out.println("\n-------------------\nError! Database Put counts don't match!!\n");
            System.exit(1);
        }
    }

    public AtomicInteger get_totalConnections() {
        return totalConnections;
    }

    public static String dateformat(long ctime) {
        return DateFormatUtils.format(ctime, "yyyy-MM-dd HH:mm:ss,SSS");
    }

    public static long getTime() {
        return System.currentTimeMillis();
    }

    public static void prt(String str) {
        System.out.println(dateformat(getTime()) + "\t" + str + "\n");
    }
    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link KVConfig}
     */
    public static void main(String[] args) throws Exception {
        prt("\n\n");
        String msg = "Test Starts!!!";
        prt(msg);
        // create a configuration from the arguments
        KVConfig config = new KVConfig();
        config.parse(AsyncBenchmark.class.getName(), args);

        AsyncBenchmark benchmark = new AsyncBenchmark(config);

        int numOfHosts = config.servers.split(",").length;
        benchmark.connect(config.servers, "In Main First Time");
        AtomicInteger totalConnections;
        try {
            totalConnections = benchmark.get_totalConnections();
            if(benchmark.debug) {
                msg = "In main..try block before runBenchmark. totalConnections = '" +
                    totalConnections.get() + "'\n";
                prt(msg);
            }

            if(numOfHosts != totalConnections.get() && fatalLevel.get() > 0) {
                msg = "Error!! We expect to connect " + numOfHosts + " hosts, which are " + config.servers + "\n";
                msg += "However, the actual total connections are " + totalConnections.get() + "\n";
                prt(msg);
            }
            else
                benchmark.runBenchmark();

            if(benchmark.debug) {
                msg = "In main..try block after runBenchmark. totalConnections = '" + totalConnections.get() + "'\n";
                prt(msg);
            }
            benchmark.readyToExit();
        }
        catch(org.voltdb.client.NoConnectionsException x) {
            benchmark.closeClient();
            if(benchmark.debug) {
                totalConnections = benchmark.get_totalConnections();
                msg = "In main..catch block before instanciate benchmark\n" +
                    "totalConnections = " + totalConnections.get() +
                    ", hashMap size: " + AsyncBenchmark.hashMap.size();
                prt(msg);
            }

            if(config.recover) {
                benchmark = new AsyncBenchmark(config);
                totalConnections = benchmark.get_totalConnections();
                long currentTime = System.currentTimeMillis();
                if(benchmark.debug) {
                    msg = "In main..catch block after instanciate benchmark\n" +
                        "currentTime = " + currentTime +
                        ", totalConnections = " + totalConnections.get() +
                        ",hashMap size: " + AsyncBenchmark.hashMap.size();
                    prt(msg);
                }

                Thread.sleep(5000); // sleep 5 seconds

                msg = "Reconnecting to these severs: " + config.servers;
                prt(msg);

                benchmark.connect(config.servers, "In catch Second Time");
                totalConnections = benchmark.get_totalConnections();
                msg = "Reconnected to these severs: '" + config.servers +
                      "', totalConnections = " + totalConnections.get();
                prt(msg);
                if(numOfHosts != totalConnections.get() && fatalLevel.get() > 0) {
                    msg = "Error!! We expect to connect " + numOfHosts + " hosts, which are " + config.servers + "\n";
                    msg += "However, the actual total connections are " + totalConnections.get() + "\n";
                    prt(msg);
                    benchmark.client.close();
                }
                else if(fatalLevel.get() > 0) {
                    msg = "There could be an error during this run which could be caused by a hick-up.";
                    msg += "\nPlease check the logs for detailed infomation.";
                    prt(msg);
                }
                if(benchmark.debug) {
                    msg = "In main..catch block, these severs '" + config.servers + "' should be connected.\n";
                    msg += "totalConnections = '" + totalConnections.get() + "'\n";
                    prt(msg);
                }
                //Thread.sleep(120000);
                if(benchmark.debug) {
                    totalConnections = benchmark.get_totalConnections();
                    msg = "In main..catch block Ready To Exit, totalConnections = " + totalConnections.get();
                    prt(msg);
                }
                benchmark.readyToExit();
            } // if(config.recover)
            else {
                msg = "Lost all connections without recover. Exit...";
                prt(msg);
                benchmark.timer.cancel();
                System.exit(1);
            }
        } // catch(org.voltdb.client.NoConnectionsException x)
    }
}
