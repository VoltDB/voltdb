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

package exportbenchmark;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.utils.CoreUtils;
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
import org.voltcore.logging.VoltLogger;

/**
 * Asychronously sends data to an export table to test VoltDB export performance.
 */
public class ExportBenchmark {

    static VoltLogger log = new VoltLogger("ExportBenchmark");
    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // Client connection to the server
    final Client client;
    // Validated CLI config
    final ExportBenchConfig config;
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

    // Server-side stats - Note: access synchronized on serverStats
    ArrayList<StatClass> serverStats = new ArrayList<StatClass>();
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
    static class ExportBenchConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 25;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 10;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option (desc = "Port on which to listen for statistics info from export clients")
        int statsPort = 5001;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Filename to write periodic stat infomation in CSV format")
        String csvfile = "";

        @Option(desc = "Export to socket or export to Kafka cluster or discarding (socket|kafka|discarding|other)")
        String target = "socket";

        @Option(desc = "if a socket target, act as a client only 'client', socket 'receiver', or default 'both' ")
        String socketmode = "both";

        @Option(desc = "How many tuples to push includes priming count.")
        int count = 0; // 10000000+40000

        @Option(desc="How many tuples to insert for each procedure call (default = 1)")
        int multiply = 1;

        @Option(desc="How many streams to divide the procedure calls to (default = 1)")
        int streams = 1;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (count < 0) exitWithMessageAndUsage("count must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (!target.equals("socket") && !target.equals("kafka") && !target.equals("other") && !target.equals("discarding")) {
                exitWithMessageAndUsage("target must be either \"socket\" or \"kafka\" or \"other\" or \"discarding\"");
            }
            if (target.equals("socket")) {
                if ( !socketmode.equals("client") && !socketmode.equals("receiver") && !socketmode.equals("both")) {
                    exitWithMessageAndUsage("socketmode must be either \"client\" or \"receiver\" or \"both\"");
                }
            }
            if (multiply <= 0) exitWithMessageAndUsage("multiply must be >= 0");
            if (target.equals("other") && count == 0 ) {
               count = 10000000+40000;
               log.info("Using count mode with count: " + count);
            }
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
    class ExportCallback implements ProcedureCallback {
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
    public ExportBenchmark(ExportBenchConfig config) {
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

    public VoltTable getExportStats() throws IOException,InterruptedException{
        long retryStats = 5;
        VoltTable stats = null;
        while (retryStats-- > 0) {
            try {
                stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
                break;
            } catch (ProcCallException e) {
                log.warn("Error while calling procedures: ");
                e.printStackTrace();
            }
            Thread.sleep(5000);
        }
        return stats;
    }

    /**
     * Checks the export table to make sure that everything has been successfully
     * processed.
     * @throws ProcCallException
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean waitForStreamedAllocatedMemoryZero() throws ProcCallException,IOException,InterruptedException {
        boolean passed = false;

        VoltTable stats = null;
        long ftime = 0;
        long st = System.currentTimeMillis();
        //Wait 10 mins only
        long end = st + (10 * 60 * 1000);
        while (true) {
            stats = getExportStats();

            boolean passedThisTime = true;
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                log.info("Waited too long...");
                break;
            }
            if (ctime - st > (3 * 60 * 1000)) {
                st = System.currentTimeMillis();
            }
            long ts = 0;
            while (stats.advanceRow()) {
                Long tts = stats.getLong("TIMESTAMP");
                //Get highest timestamp and watch it change
                if (tts > ts) {
                    ts = tts;
                }
                if (stats.getLong("TUPLE_PENDING") != 0) {
                    passedThisTime = false;
                    break;
                }
            }
            if (passedThisTime) {
                if (ftime == 0) {
                    ftime = ts;
                    continue;
                }
                //we got 0 stats 2 times in row with diff highest timestamp.
                if (ftime != ts) {
                    passed = true;
                    break;
                }
                log.info("Passed but not ready to declare victory.");
            }
            Thread.sleep(5000);
        }
        log.info("Passed is: " + passed);
        return passed;
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
            VoltTable stats = getExportStats();
            long totalTupleCount = 0;
            while (stats.advanceRow()) {
                long partitionid = stats.getLong("PARTITION_ID");
                String source = stats.getString("SOURCE");
                Long tupleCount = stats.getLong("TUPLE_COUNT");
                String tablePart = source + "_" + partitionid;
                if (! partitionMap.containsKey(tablePart)) {
                    // only put this table+partition count in the map once
                    partitionMap.put(tablePart, tupleCount);
                    totalTupleCount += tupleCount;
                }
            }
            if (totalTupleCount == insertCount) {
                long settleTimeMillis = System.currentTimeMillis() - st;
                log.info("TUPLE_COUNT settled in " + settleTimeMillis/1000.0 + " seconds");
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

        // Don't track warmup inserts
        long now;
        AtomicLong rowId = new AtomicLong(0);
        if (benchmarkWarmupEndTS > 0) {
            log.info("Warming up...");
            now = System.currentTimeMillis();
            rowId = new AtomicLong(0);
            while (benchmarkWarmupEndTS > now) {
                for (int t = 1; t <= config.streams; t++) {
                try {
                    client.callProcedure(
                            new NullCallback(),
                            "InsertExport"+t,
                            rowId.getAndIncrement(),
                            config.multiply);
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
        now = System.currentTimeMillis();
        while (true) {
            if ((benchmarkEndTS != 0) && (now > benchmarkEndTS)) {
                break;
            }
            //If we are count based use count.
            if ( (config.count > 0) && (totalInserts > config.count) ) {
                break;
            }

            for (int t = 1; t <= config.streams; t++) {
            try {
                client.callProcedure(
                        new ExportCallback(),
                        "InsertExport"+t,
                        rowId.getAndIncrement(),
                        config.multiply);
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
     * Listens on a UDP socket for incoming statistics packets, until the
     * test is finished.
     */
    private void listenForStats(CountDownLatch latch) {

        latch.countDown();
        while (true) {
            // Wait for an event...
            try {
                statsSocketSelector.select();
            } catch (IOException e) {
                exitWithException("Can't select a new socket", e);
            }

            // See if we're done
            if (testFinished.get() == true) {
                return;
            }

            // We have events. Process each one.
            for (SelectionKey key : statsSocketSelector.selectedKeys()) {
                if (!key.isValid()) {
                    continue;           // Ignore invalid keys
                }

                if (key.isReadable()) {
                    getStatsMessage((DatagramChannel)key.channel());
                }
            }
        }
    }

    /**
     * Parses a received statistics message & logs the information
     * @param channel   The channel with the incoming packet
     */
    private void getStatsMessage(DatagramChannel channel) {
        String message = null;

        // Read the data
        try {
            buffer.clear();
            channel.receive(buffer);

            buffer.flip();
            int messageLength = buffer.get();

            if (messageLength > buffer.capacity()) {
                log.info("WARN: packet exceeds allocate size; message truncated");
            }

            byte[] localBuf = new byte[messageLength];
            buffer.get(localBuf, 0, messageLength);
            message = new String(localBuf);
        } catch (IOException e) {
            exitWithException("Couldn't read from socket", e);
        }

        // Parse the stats message
        JSONObject json;
        try {
            json = new JSONObject(message);
        } catch (JSONException e) {
            log.error("Received invalid JSON: " + e.getLocalizedMessage());
            return;
        }

        int  partitionId;
        long transactions;
        long startTime;
        long endTime;
        try {
            partitionId = new Integer(json.getInt("partitionId"));
            transactions = new Long(json.getLong("transactions"));
            startTime = new Long(json.getLong("startTime"));
            endTime = new Long(json.getLong("endTime"));
        } catch (JSONException e) {
            log.error("Unable to parse JSON " + e.getLocalizedMessage());
            return;
        }
        // Round up elapsed time to 1 ms to avoid invalid data when startTime == endTime
        if (startTime > 0 && endTime == startTime) {
            endTime += 1;
        }
        // This should always be true
        if (transactions > 0 && startTime > 0 && endTime > startTime) {
            synchronized(serverStats) {
                serverStats.add(new StatClass(partitionId, transactions, startTime, endTime));
                if (startTime < serverStartTS || serverStartTS == 0) {
                    serverStartTS = startTime;
                }
                if (endTime > serverEndTS) {
                    serverEndTS = endTime;
                }
                if (partitionId > partCount) {
                    partCount = partitionId;
                }
            }
        }
        // If the else is called it means we received invalid data from the export client
        else {
            log.info("WARN: invalid data received - partitionId: " + partitionId + " | transactions: " + transactions +
                    " | startTime: " + startTime + " | endTime: " + endTime);
        }
    }

    /**
     * Sets up a UDP socket on a certain port to listen for connections.
     */
    private void setupSocketListener() {
        DatagramChannel channel = null;

        // Setup Listener
        try {
            statsSocketSelector = SelectorProvider.provider().openSelector();
            channel = DatagramChannel.open();
            channel.configureBlocking(false);
        } catch (IOException e) {
            exitWithException("Couldn't set up network channels", e);
        }

        // Bind to port & register with a channel
        try {
            InetSocketAddress isa = new InetSocketAddress(
                                            CoreUtils.getLocalAddress(),
                                            config.statsPort);
            channel.socket().setReuseAddress(true);
            channel.socket().bind(isa);
            channel.register(statsSocketSelector, SelectionKey.OP_READ);
            log.info("socket setup completed " + CoreUtils.getLocalAddress().toString() +":"+ config.statsPort);
        } catch (IOException e) {
            exitWithException("Couldn't bind to socket", e);
        }
    }

    /**
     * Runs the export benchmark test
     * @throws InterruptedException
     * @throws NoConnectionsException
     */
    private void runTest() throws InterruptedException {

        boolean isSocketTest = config.target.equals("socket") && (config.socketmode.equals("both") || config.socketmode.equals("receiver"));
        boolean success = true;
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
        if ( !config.socketmode.equals("receiver")) {
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
            log.info("Creating thread ");
            writes = new Thread(new Runnable() {
            @Override
            public void run() {
                log.info("Creating thread target " + target);
                doInserts(client);
                }
            });
            writes.start();
            Thread.sleep(config.warmup * 1000);
        }

        Thread statsListener = null;
        if (isSocketTest) {
            // On a socket test, listen for stats until the exports are drained
            // don't do this for other export types
            final CountDownLatch listenerRunning = new CountDownLatch(1);
            statsListener = new Thread(new Runnable() {
                @Override
                public void run() {
                    log.info("Running statsListener ...");
                    setupSocketListener();
                    listenForStats(listenerRunning);
                }
            });
            statsListener.start();
            listenerRunning.await();
        }

        if (!config.socketmode.equals("receiver")) {
            writes.join();
            periodicStatsTimer.cancel();
            log.info("Client finished; ready for export to finish");
        }

        // wait for export to finish draining if we are receiver..
        if (isSocketTest) {
            try {
                success = waitForStreamedAllocatedMemoryZero();
            } catch (IOException e) {
                log.error("Error while waiting for export: ");
                e.printStackTrace();
            } catch (ProcCallException e) {
                log.error("Error while calling procedures: ");
                e.printStackTrace();
            }
        }

        log.info("Finished benchmark");

        // On a socket test, stop the stats listener
        testFinished.set(true);
        if (isSocketTest) {
            statsSocketSelector.wakeup();
            statsListener.join();
            log.info("Finished statsListener ...");
        }

        // Print results & close
        printResults(benchmarkEndTS-benchmarkWarmupEndTS);
        if (!config.socketmode.equals("receiver")) {
            client.close();
        }

        // Make sure we got serverside stats if we are acting as a receiver
        if (isSocketTest) {
            if (serverStats.size() == 0 ) {
                log.error("ERROR: Never received stats from export clients");
                success = false;
            }
        }


        if (!success) {
            log.error("Export client failed");
            System.exit(-1);
        } else {
            log.info("Export client finished successfully");
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

        // Note:normally the serverStats should be stopped but synchronizing nonetheless
        synchronized(serverStats) {
            elapsedMs = serverEndTS - serverStartTS;
            if (elapsedMs > 0) {
                for (StatClass index : serverStats) {
                    serverTxn += index.m_transactions;
                }
                serverTps = serverTxn * 1000 / (serverEndTS - serverStartTS);
            }
        }

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
        VoltLogger log = new VoltLogger("ExportBenchmark.main");
        ExportBenchConfig config = new ExportBenchConfig();
        config.parse(ExportBenchmark.class.getName(), args);

        try {
            ExportBenchmark bench = new ExportBenchmark(config);
            bench.runTest();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
