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

/**
 * Asychronously sends data to an export table to test VoltDB export performance.
 */
public class ExportBenchmark {

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
    // Server-side stats
    ArrayList<StatClass> serverStats = new ArrayList<StatClass>();
    // Test timestamp markers
    long benchmarkStartTS, benchmarkWarmupEndTS, benchmarkEndTS, serverStartTS, serverEndTS, decodeTime, partCount;

    class StatClass {
        public Integer m_partition;
        public Long m_transactions;
        public Long m_decode;
        public Long m_startTime;
        public Long m_endTime;

        StatClass (Integer partition, Long transactions, Long decode, Long startTime, Long endTime) {
            m_partition = partition;
            m_transactions = transactions;
            m_decode = decode;
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

        @Option(desc = "Export to socket or export to Kafka cluster (socket|kafka)")
        String target = "socket";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (!target.equals("socket") && !target.equals("kafka")) {
                exitWithMessageAndUsage("target must be either \"socket\" or \"kafka\"");
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
            }
        }
    }

    /**
     * Clean way of exiting from an exception
     * @param message   Message to accompany the exception
     * @param e         The exception thrown
     */
    private void exitWithException(String message, Exception e) {
        System.err.println(message);
        System.err.println(e.getLocalizedMessage());
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
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);

        fullStatsContext = client.createStatsContext();
        periodicStatsContext = client.createStatsContext();

        serverStartTS = serverEndTS = decodeTime = partCount = 0;
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
            stats = client.callProcedure("@Statistics", "table", 0).getResults()[0];
            boolean passedThisTime = true;
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                System.out.println("Waited too long...");
                System.out.println(stats);
                break;
            }
            if (ctime - st > (3 * 60 * 1000)) {
                System.out.println(stats);
                st = System.currentTimeMillis();
            }
            long ts = 0;
            while (stats.advanceRow()) {
                String ttype = stats.getString("TABLE_TYPE");
                Long tts = stats.getLong("TIMESTAMP");
                //Get highest timestamp and watch it change
                if (tts > ts) {
                    ts = tts;
                }
                if ("StreamedTable".equals(ttype)) {
                    if (0 != stats.getLong("TUPLE_ALLOCATED_MEMORY")) {
                        passedThisTime = false;
                        System.out.println("Partition Not Zero.");
                        break;
                    }
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
                System.out.println("Passed but not ready to declare victory.");
            }
            Thread.sleep(5000);
        }
        System.out.println("Passed is: " + passed);
        System.out.println(stats);
        return passed;
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
            catch (IOException e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (InterruptedException interruted) {}
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
     * Inserts values into the export table for the test. First it does warmup
     * inserts, then tracked inserts.
     * @throws InterruptedException
     * @throws NoConnectionsException
     */
    public void doInserts(Client client) {

        // Don't track warmup inserts
        System.out.println("Warming up...");
        long now = System.currentTimeMillis();
        AtomicLong rowId = new AtomicLong(0);
        while (benchmarkWarmupEndTS > now) {
            try {
                client.callProcedure(
                        new NullCallback(),
                        "InsertExport",
                        rowId.getAndIncrement(),
                        0);
                // Check the time every 50 transactions to avoid invoking System.currentTimeMillis() too much
                if (++totalInserts % 50 == 0) {
                    now = System.currentTimeMillis();
                }
            } catch (Exception ignore) {}
        }
        System.out.println("Warmup complete");
        rowId.set(0);

        // reset the stats after warmup is done
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        schedulePeriodicStats();

        // Insert objects until we've run for long enough
        System.out.println("Running benchmark...");
        now = System.currentTimeMillis();
        while (benchmarkEndTS > now) {
            try {
                client.callProcedure(
                        new ExportCallback(),
                        "InsertExport",
                        rowId.getAndIncrement(),
                        0);
                // Check the time every 50 transactions to avoid invoking System.currentTimeMillis() too much
                if (++totalInserts % 50 == 0) {
                    now = System.currentTimeMillis();
                }
            } catch (Exception e) {
                System.err.println("Couldn't insert into VoltDB\n");
                e.printStackTrace();
                System.exit(1);
            }
        }

        try {
            client.drain();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Benchmark complete: wrote " + successfulInserts.get() + " objects");
        System.out.println("Failed to insert " + failedInserts.get() + " objects");

        testFinished.set(true);
        if (config.target.equals("socket")) {
            statsSocketSelector.wakeup();
        }
    }

    /**
     * Listens on a UDP socket for incoming statistics packets, until the
     * test is finished.
     */
    private void listenForStats() {

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
                System.out.println("WARN: packet exceeds allocate size; message truncated");
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
            System.err.println("Received invalid JSON: " + e.getLocalizedMessage());
            return;
        }

        final Integer partitionId;
        final Long transactions;
        final Long decode;
        final Long startTime;
        final Long endTime;
        try {
            partitionId = new Integer(json.getInt("partitionId"));
            transactions = new Long(json.getLong("transactions"));
            decode = new Long(json.getLong("decodeTime"));
            startTime = new Long(json.getLong("startTime"));
            endTime = new Long(json.getLong("endTime"));
        } catch (JSONException e) {
            System.err.println("Unable to parse JSON " + e.getLocalizedMessage());
            return;
        }
        // This should always be true
        if (transactions > 0 && decode > 0 && startTime > 0 && endTime > startTime) {
            serverStats.add(new StatClass(partitionId, transactions, decode, startTime, endTime));
            if (startTime < serverStartTS || serverStartTS == 0) {
                serverStartTS = startTime;
            }
            if (endTime > serverEndTS) {
                serverEndTS = endTime;
            }
            if (partitionId > partCount) {
                partCount = partitionId;
            }
            decodeTime += decode;
        }
        // If the else is called it means we received invalid data from the export client
        else {
            System.out.println("WARN: invalid data received - partitionId: " + partitionId + " | transactions: " + transactions + " | decode: " + decode +
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
        // Connect to servers
        try {
            System.out.println("Test initialization");
            connect(config.servers);
        } catch (InterruptedException e) {
            System.err.println("Error connecting to VoltDB");
            e.printStackTrace();
            System.exit(1);
        }

        // Figure out how long to run for
        benchmarkStartTS = System.currentTimeMillis();
        benchmarkWarmupEndTS = benchmarkStartTS + (config.warmup * 1000);
        benchmarkEndTS = benchmarkWarmupEndTS + (config.duration * 1000);

        // Do the inserts in a separate thread
        Thread writes = new Thread(new Runnable() {
            @Override
            public void run() {
                doInserts(client);
            }
        });
        writes.start();

        // Listen for stats until we stop
        Thread.sleep(config.warmup * 1000);
        // don't do this for Kafka -- nothing to listen to
        if (config.target.equals("socket")) {
            setupSocketListener();
            listenForStats();
        }

        writes.join();
        periodicStatsTimer.cancel();
        System.out.println("Client flushed; waiting for export to finish");

        // Wait until export is done -- socket target only
        boolean success = false;
        if (config.target.equals("socket")) {
            try {
                success = waitForStreamedAllocatedMemoryZero();
            } catch (IOException e) {
                System.err.println("Error while waiting for export: ");
                e.getLocalizedMessage();
            } catch (ProcCallException e) {
                System.err.println("Error while calling procedures: ");
                e.getLocalizedMessage();
            }
        } else {
            success = true; // kafka case -- no waiting
        }

        System.out.println("Finished benchmark");

        // Print results & close
        printResults(benchmarkEndTS-benchmarkWarmupEndTS);
        client.close();

        // Make sure we got serverside stats
        if (config.target.equals("socket") && serverStats.size() == 0) {
            System.err.println("ERROR: Never received stats from export clients");
            success = false;
        }

        if (!success) {
            System.exit(-1);
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
        System.out.print(LOG_DF.format(new Date(stats.getEndTimestamp())));
        System.out.printf(" Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/99.999%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.99999));
    }

    public synchronized Double calcRatio(StatClass index, StatClass indexPrime) {
        Double ratio = new Double(0);
        // Check for overlap format:
        //      |-----index window-----|
        //   |-----indexPrime window-----|
        if (indexPrime.m_endTime >= index.m_endTime && indexPrime.m_startTime <= index.m_startTime) {
            ratio = new Double(index.m_endTime - index.m_startTime) / (indexPrime.m_endTime - indexPrime.m_startTime);
            if (ratio <= 0 || ratio > 1) {
                System.out.println("Bad Ratio 1 - ratio: " + ratio + " || index.endTime: " + index.m_endTime +
                        " || index.startTime: " + index.m_startTime + " || indexPrime.endTime: " + indexPrime.m_endTime +
                        " || indexPrime.startTime: " + indexPrime.m_startTime);
                System.exit(-1);
            }
        }
        // Check for overlap format:
        //      |-----index window-----|
        //        |-indexPrime window-|
        else if (indexPrime.m_endTime <= index.m_endTime && indexPrime.m_startTime >= index.m_startTime) {
            ratio = new Double(1);
        }
        // Check for overlap format:
        //      |-----index window-----|
        //            |--indexPrime window--|
        else if (indexPrime.m_startTime >= index.m_startTime && indexPrime.m_startTime < index.m_endTime) {
            ratio = new Double(index.m_endTime - indexPrime.m_startTime) / (indexPrime.m_endTime - indexPrime.m_startTime);
            if (ratio <= 0 || ratio > 1) {
                System.out.println("Bad Ratio 2 - ratio: " + ratio + " || index.endTime: " + index.m_endTime +
                        " || index.startTime: " + index.m_startTime + " || indexPrime.endTime: " + indexPrime.m_endTime +
                        " || indexPrime.startTime: " + indexPrime.m_startTime);
                System.exit(-1);
            }
        }
        // Check for overlap format:
        //      |-----index window-----|
        // |--indexPrime window--|
        else if (indexPrime.m_endTime <= index.m_endTime && indexPrime.m_endTime > index.m_startTime) {
            ratio = new Double(indexPrime.m_endTime - index.m_startTime) / (indexPrime.m_endTime - indexPrime.m_startTime);
            if (ratio <= 0 || ratio > 1) {
                System.out.println("Bad Ratio 3 - ratio: " + ratio + " || index.endTime: " + index.m_endTime +
                        " || index.startTime: " + index.m_startTime + " || indexPrime.endTime: " + indexPrime.m_endTime +
                        " || indexPrime.startTime: " + indexPrime.m_startTime);
                System.exit(-1);
            }
        }
        return ratio;
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

        ArrayList<StatClass> indexStats = new ArrayList<StatClass>();
        for (StatClass index : serverStats) {
            if (index.m_partition.equals(0)) {
                Double transactions = new Double(index.m_transactions);
                Double decode = new Double(index.m_decode);
                for (StatClass indexPrime : serverStats) {
                    // If indexPrime is not partition 0 check for window overlap
                    if (!indexPrime.m_partition.equals(0)) {
                        Double ratio = calcRatio(index, indexPrime);
                        transactions +=  ratio * indexPrime.m_transactions;
                        decode += ratio * indexPrime.m_transactions;
                    }
                }
                indexStats.add(new StatClass(index.m_partition, transactions.longValue(), decode.longValue(), index.m_startTime, index.m_endTime));
            }
        }

        Double tpsSum = new Double(0);
        Double decodeSum = new Double(0);
        for (StatClass index : indexStats) {
            tpsSum += (new Double(index.m_transactions * 1000) / (index.m_endTime - index.m_startTime));
            decodeSum += (new Double(index.m_decode) / index.m_transactions);
        }
        tpsSum = (tpsSum / indexStats.size());
        decodeSum = (decodeSum / indexStats.size());

        // Performance statistics
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
        System.out.printf("Average throughput:            %,9d txns/sec\n", tpsSum.longValue());
        System.out.printf("Average decode time:           %,9.2f ns\n", decodeSum);
        Double decodePerc = (new Double(decodeTime) / (((serverEndTS - serverStartTS) * (partCount + 1)) * 1000000)) * 100;
        System.out.printf("Percent decode row time:       %,9.2f %%\n", decodePerc);

        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" Latency Histogram");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(stats.latencyHistoReport());

        // Write stats to file if requested
        try {
            if ((config.statsfile != null) && (config.statsfile.length() != 0)) {
                FileWriter fw = new FileWriter(config.statsfile);
                fw.append(String.format("%d,%d,%d,%d,%d,%d,%d,0,0,0,0,0,0\n",
                                    stats.getStartTimestamp(),
                                    duration,
                                    successfulInserts.get(),
                                    serverEndTS - serverStartTS,
                                    decodeTime,
                                    decodeSum.longValue(),
                                    tpsSum.longValue()));
                fw.close();
            }
        } catch (IOException e) {
            System.err.println("Error writing stats file");
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
