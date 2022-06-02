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
 * This client inserts a specified number of rows into a Volt stream as topic.
 *
 * Another client or clients consume rows from the stream topic.
 */

package topicbenchmark;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

/**
 * Asychronously sends data to topics to test VoltDB export performance.
 */
public class TopicBenchmark {

    static VoltLogger log = new VoltLogger("TopicBenchmark");
    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // Client connection to the server
    final Client client;
    // Validated CLI config
    final TopicBenchConfig config;
    // Network variables
    Selector statsSocketSelector;
    Thread statsThread;
    ByteBuffer buffer = ByteBuffer.allocate(1024);
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;
    // Timer for periodic stats
    Timer periodicStatsTimer;
    // Test stats variables
    long totalInserts = 0;
    AtomicLong successfulInserts = new AtomicLong(0);
    AtomicLong failedInserts = new AtomicLong(0);
    AtomicBoolean testFinished = new AtomicBoolean(false);
    public int target = 0;

    // Test timestamp markers
    long benchmarkStartTS, benchmarkWarmupEndTS, benchmarkEndTS, serverStartTS, serverEndTS, partCount;

    class StatClass {
        public int m_partition;
        public long m_transactions;
        public long m_startTime;
        public long m_endTime;

        StatClass (int partition, long transactions, long startTime, long endTime) {
            m_partition = partition;
            m_transactions = transactions;
            m_startTime = startTime;
            m_endTime = endTime;
        }
    }

    static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class TopicBenchConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 25;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 10;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Filename to write periodic stat infomation in CSV format")
        String csvfile = "";

        @Option(desc = "How many tuples to push includes priming count.")
        int count = 0; // 10000000+40000

        @Option(desc="How many tuples to insert for each procedure call (default = 1)")
        int multiply = 1;

        @Option(desc="How many topics to divide the multiplier into (default = 1)")
        int topics = 1;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (count < 0) exitWithMessageAndUsage("count must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (multiply <= 0) exitWithMessageAndUsage("multiply must be >= 0");
            //If count is specified turn warmup off.
            if (count > 0) {
                warmup = 0;
                duration = 0;
            }
        }
    }

    /**
     * Callback for export insert method. Tracks successes & failures
     */
    class TopicCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) {
            if (response.getStatus() == ClientResponse.SUCCESS) {
                successfulInserts.incrementAndGet();
            } else {
                failedInserts.incrementAndGet();
                log.info(response.getStatusString());
            }
        }
    }

    /**
     * Clean way of exiting from an exception
     * @param message   Message to accompany the exception
     * @param e         The exception thrown
     */
    private void exitWithException(String message, Exception e) {
        log.error(message);
        log.info(e.getLocalizedMessage());
        System.exit(-1);
    }

    /**
     * Creates a new instance of the test to be run.
     * Establishes a client connection to a voltdb server, which should already be running
     * @param args The arguments passed to the program
     */
    public TopicBenchmark(TopicBenchConfig config) {
        this.config = config;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setTopologyChangeAware(true);
        client = ClientFactory.createClient(clientConfig);

        fullStatsContext = client.createStatsContext();
        periodicStatsContext = client.createStatsContext();

        serverStartTS = serverEndTS = partCount = 0;
    }

    /**
    * Create a Timer task to display performance data.
    * It calls printStatistics() every displayInterval seconds
    */
    public void schedulePeriodicStats() {
        periodicStatsTimer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() { printStatistics(); }
        };
        periodicStatsTimer.scheduleAtFixedRate(statsPrinting,
                                    config.displayinterval * 1000,
                                    config.displayinterval * 1000);
    }

    public VoltTable getTopicStats() throws IOException,InterruptedException{
        long retryStats = 5;
        VoltTable stats = null;
        while (retryStats-- > 0) {
            try {
                stats = client.callProcedure("@Statistics", "topic", 0).getResults()[0];
                break;
            } catch (ProcCallException e) {
                log.warn("Error while calling procedures: ");
                e.printStackTrace();
            }
            Thread.sleep(5000);
        }
        return stats;
    }

    public boolean waitTilTupleCountSettles(long insertCount) throws IOException, InterruptedException {
        /*
         * There might be a delay between the completion of the last insert transaction and when
         * stats register all tuples, that is TUPLE_COUNT = insert count
         *
         * Since at this point, all the stream/partitions are disabled, we have to be careful
         * to count each partition for each table once.
         */
        long st = System.currentTimeMillis();
        //Wait 10 mins only
        long end = st + (10 * 60 * 1000);
        while (true) {
            Map<String, Long> partitionMap = new HashMap<String, Long>();
            VoltTable stats = getTopicStats();
            long totalTupleCount = 0;
            while (stats.advanceRow()) {
                long partitionid = stats.getLong("PARTITION_ID");
                String source = stats.getString("TOPIC");
                Long tupleCount = stats.getLong("LAST_OFFSET");
                String tablePart = source + "_" + partitionid;
                if (! partitionMap.containsKey(tablePart)) {
                    // only put this table+partition count in the map once
                    partitionMap.put(tablePart, tupleCount);
                    totalTupleCount += tupleCount;
                }
            }
            if (totalTupleCount == insertCount) {
                long settleTimeMillis = System.currentTimeMillis() - st;
                log.info("LAST_OFFSET settled in " + settleTimeMillis/1000.0 + " seconds");
                return true;
            }
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                log.info("Waited too long...");
                return false;
            }
            Thread.sleep(1000);
        }
    }

    /**
     * Connect to a single server.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServer(String server) {
        try {
            client.createConnection(server);
        }
        catch (IOException e) {
            log.info("Connection to " + server + " failed");
            return;
        }
        log.info("Connected to VoltDB node at: " + server);
    }

    /**
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    void connect(String servers) throws InterruptedException {
        log.info("Connecting to VoltDB...");

        String[] serverArray = servers.split(",");
        connectToOneServer(serverArray[0]);
    }

    /**
     * Inserts values into the export table for the test. First it does warmup
     * inserts, then tracked inserts.
     * @throws InterruptedException
     * @throws NoConnectionsException
     */
    public void doInserts(Client client) {

        long now;
        AtomicLong rowId = new AtomicLong(0);
        if (benchmarkWarmupEndTS > 0) {
            log.info("Warming up...");
            now = System.currentTimeMillis();
            rowId = new AtomicLong(0);
            while (benchmarkWarmupEndTS > now) {
              // Fixme - only 1 target
                for (int t = 1; t <= config.topics; t++) {
                try {
                    client.callProcedure(
                            new NullCallback(),
                            "InsertTopic"+t,
                            rowId.getAndIncrement(),
                            config.multiply,
                            1);
                    // Check the time every 50 transactions to avoid invoking System.currentTimeMillis() too much
                    if (++totalInserts % 50 == 0) {
                        now = System.currentTimeMillis();
                    }
                } catch (Exception ignore) {}
                }
            }
            log.info("Warmup complete");
            rowId.set(0);
        }

        // reset the stats after warmup is done
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        schedulePeriodicStats();

        // Insert objects until we've run for long enough
        log.info("Running benchmark...");
        long benchmarkStart = 0;
        benchmarkStart = now = System.currentTimeMillis();
        while (true) {
            if ((benchmarkEndTS != 0) && (now > benchmarkEndTS)) {
                break;
            }
            //If we are count based use count.
            if ( (config.count > 0) && (totalInserts >= config.count) ) {
                break;
            }

            // Fixme - only 1 target
            for (int t = 1; t <= config.topics; t++) {
            try {
                client.callProcedure(
                        new TopicCallback(),
                        "InsertTopic"+t,
                        rowId.getAndIncrement(),
                        config.multiply,
                        config.topics);
                // Check the time every 50 transactions to avoid invoking System.currentTimeMillis() too much
                if (++totalInserts % 50 == 0) {
                    now = System.currentTimeMillis();
                }
            } catch (Exception e) {
                log.error("Couldn't insert into VoltDB\n");
                e.printStackTrace();
                System.exit(1);
            }
            }
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - benchmarkStart;

        try {
            client.drain();
        } catch (Exception e) {
            e.printStackTrace();
        }

        log.info("Benchmark complete: " + successfulInserts.get() + " successful procedure calls" +
            " (excludes warmup)");
        log.info("Failed " + failedInserts.get() + " procedure calls");
        // Use this to correlate the total rows exported
        log.info("Total inserts: (" + totalInserts + " * " + config.multiply + ") = "
                + (totalInserts * config.multiply) + " (includes warmup)");
        try {
            if (waitTilTupleCountSettles(totalInserts * config.multiply)) {
                log.info("TUPLE_COUNT matches tuples inserted!");
            } else {
                log.info("TUPLE_COUNT did not settle in 10 minutes");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Runs the topic benchmark test
     * @throws InterruptedException
     * @throws NoConnectionsException
     */
    private void runTest() throws InterruptedException {
        boolean success = true;
        // int t = config.targets;
        // Connect to servers
        try {
            log.info("Test initialization");
            connect(config.servers);
        } catch (InterruptedException e) {
            log.error("ERROR: Error connecting to VoltDB");
            e.printStackTrace();
            System.exit(1);
        }

        Thread writes = null;

        // Figure out how long to run for
        benchmarkStartTS = System.currentTimeMillis();
        if (config.warmup == 0) {
            benchmarkWarmupEndTS = 0;
        } else {
            benchmarkWarmupEndTS = benchmarkStartTS + (config.warmup * 1000);
        }
        //If we are going by count turn off end by timestamp.
        if (config.count > 0) {
            benchmarkEndTS = 0;
        } else {
            benchmarkEndTS = benchmarkWarmupEndTS + (config.duration * 1000);
        }

        // Do the inserts in a separate thread
        long xyz = System.currentTimeMillis();
        log.info("Creating thread ");
        writes = new Thread(new Runnable() {
          @Override
          public void run() {
                log.info("Creating thread target " + target);
                doInserts(client);
            }
        });
        writes.start();
        writes.join();
        long duration = System.currentTimeMillis() - xyz;

        log.info("Finished benchmark");

        // Print results & close
        printResults(duration);
        client.close();

        if (!success) {
            log.error("Test client failed");
            System.exit(-1);
        } else {
            log.info("Test client finished successfully");
            System.exit(0);
        }

    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();

        // Print an ISO8601 timestamp (of the same kind Python logging uses) to help
        // log merger correlate correctly
        // System.out.print(LOG_DF.format(new Date(stats.getEndTimestamp())));
        log.info(String.format(" Throughput %d/s, ", stats.getTxnThroughput()));
        log.info(String.format("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors()));
        log.info(String.format("Avg/99.999%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.99999)));
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     * @param duration   How long the test by count ran
     * @param count      How many objects the test by duration inserted
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults(long duration) {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // Calculate "server tps" i.e. export performance: this is only valid for
        // socket tests as it is based on stats messages received via UDP. It is
        // also only valid for the first test execution after voltdb startup,
        // due to the way the timestamps are managed in SocketExporter.

        long serverTxn = 0L;
        long serverTps = 0L;
        long elapsedMs = 0L;

        // Performance statistics
        log.info(HORIZONTAL_RULE);
        log.info(" Client Workload Statistics");
        log.info(HORIZONTAL_RULE);

        log.info(String.format("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput()));
        log.info(String.format("Average latency:               %,9.2f ms\n", stats.getAverageLatency()));
        log.info(String.format("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1)));
        log.info(String.format("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25)));
        log.info(String.format("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5)));
        log.info(String.format("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75)));
        log.info(String.format("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9)));
        log.info(String.format("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95)));
        log.info(String.format("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99)));
        log.info(String.format("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995)));
        log.info(String.format("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999)));
        log.info(String.format("99.999th percentile latency:   %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99999)));

        log.info("\n" + HORIZONTAL_RULE);
        log.info(" System Server Statistics (note: only valid on socket tests for first test execution after voltdb startup)");
        log.info(String.format("Exported rows collected:       %,9d \n", serverTxn));
        log.info(String.format("Average throughput:            %,9d txns/sec\n", serverTps));

        log.info(HORIZONTAL_RULE);

        log.info(String.format("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency()));

        log.info("\n" + HORIZONTAL_RULE);
        log.info(" Latency Histogram");
        log.info(HORIZONTAL_RULE);
        log.info(stats.latencyHistoReport());

        // Write stats to file if requested
        try {
            if ((config.statsfile != null) && (config.statsfile.length() != 0)) {
                FileWriter fw = new FileWriter(config.statsfile);
                fw.append(String.format("%d,%d,%d,%d,%d,0,0,0,0,0,0,0,0\n",
                                    stats.getStartTimestamp(),
                                    duration,
                                    successfulInserts.get(),
                                    elapsedMs,
                                    serverTps));
                fw.close();
            }
        } catch (IOException e) {
            log.error("Error writing stats file");
            e.printStackTrace();
        }
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */
    public static void main(String[] args) {
        TopicBenchConfig config = new TopicBenchConfig();
        config.parse(TopicBenchmark.class.getName(), args);

        try {
            TopicBenchmark bench = new TopicBenchmark(config);
            bench.runTest();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
