/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package genqa;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.exampleutils.AppHelper;

public class AsyncExportClient
{
    // Transactions between catalog swaps.
    public static long CATALOG_SWAP_INTERVAL = 500000;
    // Number of txn ids per client log file.
    public static long CLIENT_TXNID_FILE_SIZE = 250000;

    static class TxnIdWriter
    {
        String m_nonce;
        String m_txnLogPath;
        FileOutputStream m_curFile = null;
        OutputStreamWriter m_outs = null;
        AtomicLong m_count = new AtomicLong(0);

        public TxnIdWriter(String nonce, String txnLogPath)
        {
            m_nonce = nonce;
            m_txnLogPath = txnLogPath;

            File logPath = new File(m_txnLogPath);
            if (!logPath.exists()) {
                if (!logPath.mkdir()) {
                    System.err.println("Problem creating log directory");
                }
            }
        }

        public void createNewFile() throws IOException
        {
            if (m_curFile != null)
            {
                m_outs.close();
                m_curFile.flush();
                m_curFile.close();
            }
            File blah = new File(m_txnLogPath, m_count + "-" + m_nonce + "-txns");
            m_curFile = new FileOutputStream(blah);
            m_outs = new OutputStreamWriter(m_curFile);
        }

        public void write(String txnId) throws IOException
        {
            if ((m_count.get() % CLIENT_TXNID_FILE_SIZE) == 0)
            {
                createNewFile();
            }
            m_outs.write(txnId);
            m_count.incrementAndGet();
        }
    }

    static class AsyncCallback implements ProcedureCallback
    {
        private final TxnIdWriter m_writer;
        private final long m_rowid;
        public AsyncCallback(TxnIdWriter writer, long rowid)
        {
            super();
            m_rowid = rowid;
            m_writer = writer;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Track the result of the request (Success, Failure)
            if (clientResponse.getStatus() == ClientResponse.SUCCESS)
            {
                TrackingResults.incrementAndGet(0);
                long txid = clientResponse.getResults()[0].asScalarLong();
                final String trace = String.format("%016d:%d\n", m_rowid, txid);
                try
                {
                    m_writer.write(trace);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
            else
            {
                TrackingResults.incrementAndGet(1);
                final String trace = String.format("%016d:-1\n", m_rowid);
                try
                {
                    m_writer.write(trace);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    // Connection configuration
    private final static class ConnectionConfig {

        final long displayInterval;
        final long duration;
        final String servers;
        final int port;
        final int poolSize;
        final int rateLimit;
        final boolean autoTune;
        final int latencyTarget;
        final String [] parsedServers;
        final String procedure;

        ConnectionConfig( AppHelper apph) {
            displayInterval = apph.longValue("displayinterval");
            duration        = apph.longValue("duration");
            servers         = apph.stringValue("servers");
            port            = apph.intValue("port");
            poolSize        = apph.intValue("poolsize");
            rateLimit       = apph.intValue("ratelimit");
            autoTune        = apph.booleanValue("autotune");
            latencyTarget   = apph.intValue("latencytarget");
            procedure       = apph.stringValue("procedure");
            parsedServers   = servers.split(",");
        }
    }

    // Initialize some common constants and variables
    private static final AtomicLongArray TrackingResults = new AtomicLongArray(2);

    private static File[] catalogs = {new File("genqa.jar"), new File("genqa2.jar")};
    private static File deployment = new File("deployment.xml");

    // Connection reference
    private static final AtomicReference<Client> clientRef = new AtomicReference<Client>();

    // Shutdown flag
    private static final AtomicBoolean shutdown = new AtomicBoolean(false);

    // Connection Configuration
    private static ConnectionConfig config;

    // Test startup time
    private static long benchmarkStartTS;

    private static final ExecutorService es = Executors.newCachedThreadPool(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable arg0) {
            Thread thread = new Thread(arg0, "Retry Connection");
            thread.setDaemon(true);
            return thread;
        }
    });

    // Statistics manager objects from the client
    private static ClientStatsContext periodicStatsContext;
    private static ClientStatsContext fullStatsContext;

    // Application entry point
    public static void main(String[] args)
    {
        try
        {

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Use the AppHelper utility class to retrieve command line application parameters

            // Define parameters and pull from command line
            AppHelper apph = new AppHelper(AsyncBenchmark.class.getCanonicalName())
                .add("displayinterval", "display_interval_in_seconds", "Interval for performance feedback, in seconds.", 10)
                .add("duration", "run_duration_in_seconds", "Benchmark duration, in seconds.", 120)
                .add("servers", "comma_separated_server_list", "List of VoltDB servers to connect to.", "localhost")
                .add("port", "port_number", "Client port to connect to on cluster nodes.", 21212)
                .add("poolsize", "pool_size", "Size of the record pool to operate on - larger sizes will cause a higher insert/update-delete rate.", 100000)
                .add("procedure", "procedure_name", "Procedure to call.", "JiggleExportSinglePartition")
                .add("ratelimit", "rate_limit", "Rate limit to start from (number of transactions per second).", 100000)
                .add("autotune", "auto_tune", "Flag indicating whether the benchmark should self-tune the transaction rate for a target execution latency (true|false).", "true")
                .add("latencytarget", "latency_target", "Execution latency to target to tune transaction rate (in milliseconds).", 10)
                .add("catalogswap", "Swap catalogs from the client", "true")
                .setArguments(args)
            ;

            config = new ConnectionConfig(apph);

            // Retrieve parameters
            final boolean catalogSwap  = apph.booleanValue("catalogswap");
            final String csv           = apph.stringValue("statsfile");

            TxnIdWriter writer = new TxnIdWriter("dude", "clientlog");

            // Validate parameters
            apph.validate("duration", (config.duration > 0))
                .validate("poolsize", (config.poolSize > 0))
                .validate("ratelimit", (config.rateLimit > 0))
                .validate("latencytarget", (config.latencyTarget > 0))
            ;

            // Display actual parameters, for reference
            apph.printActualUsage();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Get a client connection - we retry for a while in case the server hasn't started yet
            createClient();
            connect();


// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // Create a Timer task to display performance data on the procedure
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask()
            {
                @Override
                public void run()
                {
                    printStatistics(periodicStatsContext,true);
                }
            }
            , config.displayInterval*1000l
            , config.displayInterval*1000l
            );

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            benchmarkStartTS = System.currentTimeMillis();
            AtomicLong rowId = new AtomicLong(0);

            // Run the benchmark loop for the requested duration
            final long endTime = benchmarkStartTS + (1000l * config.duration);
            int swap_count = 0;
            boolean first_cat = false;
            while (endTime > System.currentTimeMillis())
            {
                long currentRowId = rowId.incrementAndGet();
                // Post the request, asynchronously
                clientRef.get().callProcedure(
                        new AsyncCallback(writer, currentRowId),
                        config.procedure,
                        currentRowId,
                        0);

                swap_count++;
                if (((swap_count % CATALOG_SWAP_INTERVAL) == 0) && catalogSwap)
                {
                    System.out.println("Changing catalogs...");
                    clientRef.get().updateApplicationCatalog(catalogs[first_cat ? 0 : 1], deployment);
                    first_cat = !first_cat;
                }
            }
            shutdown.compareAndSet(false, true);

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // We're done - stop the performance statistics display task
            timer.cancel();

// ---------------------------------------------------------------------------------------------------------------------------------------------------
            clientRef.get().drain();

            Thread.sleep(10000);

            // Now print application results:

            // 1. Tracking statistics
            System.out.printf(
              "-------------------------------------------------------------------------------------\n"
            + " Benchmark Results\n"
            + "-------------------------------------------------------------------------------------\n\n"
            + "A total of %d calls was received...\n"
            + " - %,9d Succeeded\n"
            + " - %,9d Failed (Transaction Error)\n"
            + "\n\n"
            + "-------------------------------------------------------------------------------------\n"
            , TrackingResults.get(0)+TrackingResults.get(1)
            , TrackingResults.get(0)
            , TrackingResults.get(1)
            );

            // 3. Performance statistics (we only care about the procedure that we're benchmarking)
            System.out.println(
              "\n\n-------------------------------------------------------------------------------------\n"
            + " System Statistics\n"
            + "-------------------------------------------------------------------------------------\n\n");
            printStatistics(fullStatsContext,false);

            // Dump statistics to a CSV file
            clientRef.get().writeSummaryCSV(
                    fullStatsContext.getStatsByProc().get(config.procedure),
                    csv
                    );

            clientRef.get().close();

// ---------------------------------------------------------------------------------------------------------------------------------------------------

        }
        catch(Exception x)
        {
            System.out.println("Exception: " + x);
            x.printStackTrace();
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
    static void connect() throws InterruptedException {
        System.out.println("Connecting to VoltDB...");

        String[] serverArray = config.parsedServers;
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(server, config.port);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param client The client to use for this server
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    static void connectToOneServerWithRetry(String server, int port) {
        Client client = clientRef.get();
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server, port);
                break;
            }
            catch (Exception e) {
                System.err.printf("Connection to " + server + " failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }

    static Client createClient() {
        StatusListener statusListener = new StatusListener();
        ClientConfig clientConfig = new ClientConfig("", "", statusListener);
        if (config.autoTune) {
            clientConfig.enableAutoTune();
            clientConfig.setAutoTuneTargetInternalLatency(config.latencyTarget);
        }
        else {
            clientConfig.setMaxTransactionsPerSecond(config.rateLimit);
        }
        Client client = ClientFactory.createClient(clientConfig);
        clientRef.set(client);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        return client;
    }

    /**
     * Remove the client from the list if connection is broken.
     */
    static class StatusListener extends ClientStatusListenerExt {

        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            if (shutdown.get()) {
                return;
            }

            // if the benchmark is still active
            if ((System.currentTimeMillis() - benchmarkStartTS) < (config.duration * 1000)) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }

            // setup for retry
            final String server = hostname;
            final int finalPort = port;
            es.execute(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(server, finalPort);
                }
            });
        }
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    static private synchronized void printStatistics(ClientStatsContext context, boolean resetBaseline) {
        if (resetBaseline) {
            context = context.fetchAndResetBaseline();
        } else {
            context = context.fetch();
        }

        ClientStats stats = context
                .getStatsByProc()
                .get(config.procedure);

        if (stats == null) return;

        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/95%% Latency %.2f/%dms\n", stats.getAverageLatency(),
                stats.kPercentileLatency(0.95));
    }
}
