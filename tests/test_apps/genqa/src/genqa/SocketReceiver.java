/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package genqa;

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
    Receive stats packets from the socketExporter
 */
public class SocketReceiver {

    static VoltLogger log = new VoltLogger("SocketReceiver");
    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // Client connection to the server
    final Client client;
    // Validated CLI config
    final SocketReceiverConfig config;
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
        public long m_metaCodesInsert;          // change code 1
        public long m_metaCodesDelete;          // change code 2
        public long m_metaCodesUpdateBefore;    // change code 3
        public long m_metaCodesUpdateAfter;     // change code 4
        public long m_metaCodesMigrate;         // change code 5

        StatClass (int partition, long transactions, long startTime, long endTime,
             long inserts, long deletes, long updatesBefore, long updatesAfter, long migrates;
            m_partition = partition;
            m_transactions = transactions;
            m_startTime = startTime;
            m_endTime = endTime;
            m_metaCodesInsert = inserts;
            m_metaCodesDelete = deletes;
            m_metaCodesUpdateBefore = updatesBefore;
            m_metaCodesUpdateAfter = updatesAfter;
            m_metaCodesMigrate = migrates;
        }
    }

    static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class SocketReceiverConfig extends CLIConfig {
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

        @Option(desc="How many targets to divide the multiplier into (default = 1)")
        int targets = 1;

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
     * Creates socket receiver thread
     */
    public SocketReceiver(SocketReceiverConfig config) {
        this.config = config;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setTopologyChangeAware(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);

        fullStatsContext = client.createStatsContext();
        periodicStatsContext = client.createStatsContext();

        serverStartTS = serverEndTS = partCount = 0;
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
        long inserts;
        long deletes;
        long updatesBefore;
        long updatesAfter;
        long migrates;

        try {
            partitionId = new Integer(json.getInt("partitionId"));
            transactions = new Long(json.getLong("transactions"));
            startTime = new Long(json.getLong("startTime"));
            endTime = new Long(json.getLong("endTime"));
            inserts = new Long(json.getLong("inserts"));
            deletes = new Long(json.getLong("deletes"));
            updatesBefore = new Long(json.getLong("updatesBefore"));
            updatesAfter = new Long(json.getLong("updatesAfter"));
            migrates = new Long(json.getLong("migrates"));
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
                serverStats.add(new StatClass(partitionId, transactions, startTime, endTime, metaCodes));
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


        // Make sure we got serverside stats if we are acting as a receiver
        if (isSocketTest) {
            if (serverStats.size() == 0 ) {
                log.error("ERROR: Never received stats from export clients");
                success = false;
            }
        }


        if (!success) {
            log.error("client failed");
            System.exit(-1);
        } else {
            log.info("client finished successfully");
            System.exit(0);
        }
    }
}
