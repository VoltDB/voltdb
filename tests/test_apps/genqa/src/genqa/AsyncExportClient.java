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
import java.time.Instant;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.ClientResponseImpl;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.exampleutils.AppHelper;
import org.voltdb.iv2.TxnEgo;

public class AsyncExportClient
{
    static VoltLogger log = new VoltLogger("ExportClient");
// Transactions between catalog swaps.
    public static long CATALOG_SWAP_INTERVAL = 500000;
    // Number of txn ids per client log file.
    public static long CLIENT_TXNID_FILE_SIZE = 250000;

    static class TxnIdWriter
    {
        String m_nonce;
        String m_txnLogPath;
        AtomicLong m_count = new AtomicLong(0);

        private Map<Integer,File> m_curFiles = new TreeMap<>();
        private Map<Integer,File> m_baseDirs = new TreeMap<>();
        private Map<Integer,OutputStreamWriter> m_osws = new TreeMap<>();

        public TxnIdWriter(String nonce, String txnLogPath)
        {
            m_nonce = nonce;
            m_txnLogPath = txnLogPath;

            File logPath = new File(m_txnLogPath);
            if (!logPath.exists()) {
                if (!logPath.mkdir()) {
                    log.warn("Problem creating log directory " + logPath);
                }
            }
        }

        public void createNewFile(int partId) throws IOException {
            File dh = m_baseDirs.get(partId);
            if (dh == null) {
                dh = new File(m_txnLogPath, Integer.toString(partId));
                if (!dh.mkdir()) {
                    log.warn("Problem creating log directory " + dh);
                }
                m_baseDirs.put(partId, dh);
            }
            long count = m_count.get();
            count = count - count % CLIENT_TXNID_FILE_SIZE;
            File logFH = new File(dh, "active-" + count + "-" + m_nonce + "-txns");
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(logFH));

            m_curFiles.put(partId,logFH);
            m_osws.put(partId,osw);
        }

        public void write(int partId, String rec) throws IOException {

            if ((m_count.get() % CLIENT_TXNID_FILE_SIZE) == 0) {
                close(false);
            }
            OutputStreamWriter osw = m_osws.get(partId);
            if (osw == null) {
                createNewFile(partId);
                osw = m_osws.get(partId);
            }
            osw.write(rec);
            m_count.incrementAndGet();
        }

        public void close(boolean isLast) throws IOException
        {
            for (Map.Entry<Integer,OutputStreamWriter> e: m_osws.entrySet()) {

                int partId = e.getKey();
                OutputStreamWriter osw = e.getValue();

                if (osw != null) {
                    osw.close();
                    File logFH = m_curFiles.get(partId);
                    File renamed = new File(
                            m_baseDirs.get(partId),
                            logFH.getName().substring("active-".length()) + (isLast ? "-last" : "")
                            );
                    logFH.renameTo(renamed);

                    e.setValue(null);
                }
                m_curFiles.put(partId, null);
            }

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
            long now = System.currentTimeMillis();
            if (clientResponse.getStatus() == ClientResponse.SUCCESS)
            {
                TrackingResults.incrementAndGet(0);
                TransactionCounts.incrementAndGet(INSERT);
                long txid = clientResponse.getResults()[0].asScalarLong();
                final String trace = String.format("%d:%d:%d\n", m_rowid, txid, now);
                // log.info("Success " + trace);
                try
                {
                    m_writer.write(TxnEgo.getPartitionId(txid),trace);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
            else
            {
                TrackingResults.incrementAndGet(1);
                final String trace = String.format("%d:-1:%d:%s\n", m_rowid, now,((ClientResponseImpl)clientResponse).toJSONString());
                // log.info("Fail " + trace);
                try
                {
                    m_writer.write(-1,trace);
                }
                catch (IOException e)
                {
                    log.error("Exception: " + e);
                    e.printStackTrace();
                }
            }
        }
    }

    static class TableExportCallback implements ProcedureCallback
    {
        private final TxnIdWriter m_writer;
        private final long m_type;
        public TableExportCallback(TxnIdWriter writer, long type)
        {
            super();
            m_type = type;
            m_writer = writer;
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Track the result of the request (Success, Failure)
            long now = System.currentTimeMillis();
            int transType = clientResponse.getAppStatus(); // get INSERT, DELETE, or UPDATE)
            if (clientResponse.getStatus() == ClientResponse.SUCCESS)
            {

                TrackingResults.incrementAndGet(0);
                TransactionCounts.incrementAndGet(transType);
            }
            else
            {
                TrackingResults.incrementAndGet(1);
                final String trace = String.format("%d:%s\n", now,((ClientResponseImpl)clientResponse).toJSONString());
                log.info("TableExport failed: " + trace);
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
        final int exportTimeout;
        final boolean migrateWithTTL;
        final boolean usetableexport;
        final boolean migrateWithoutTTL;
        final long migrateNoTTLInterval;

        ConnectionConfig( AppHelper apph) {
            displayInterval      = apph.longValue("displayinterval");
            duration             = apph.longValue("duration");
            servers              = apph.stringValue("servers");
            port                 = apph.intValue("port");
            poolSize             = apph.intValue("poolsize");
            rateLimit            = apph.intValue("ratelimit");
            autoTune             = apph.booleanValue("autotune");
            latencyTarget        = apph.intValue("latencytarget");
            procedure            = apph.stringValue("procedure");
            parsedServers        = servers.split(",");
            exportTimeout        = apph.intValue("timeout");
            migrateWithTTL       = apph.booleanValue("migrate-ttl");
            usetableexport       = apph.booleanValue("usetableexport");
            migrateWithoutTTL    = apph.booleanValue("migrate-nottl");
            migrateNoTTLInterval = apph.longValue("nottl-interval");

        }
    }

    // Initialize some common constants and variables
    private static final AtomicLongArray TrackingResults = new AtomicLongArray(2);

    // If testing Table/Export, count inserts, deletes, update fore & aft

    private static int INSERT = 1;
    private static int DELETE = 2;
    private static int UPDATE_OLD = 3;
    private static int UPDATE_NEW = 4;
    private static final AtomicLongArray TransactionCounts = new AtomicLongArray(4);

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

    // Statistics manager objects from the client
    private static ClientStatsContext periodicStatsContext;
    private static ClientStatsContext fullStatsContext;

    static {
        VoltDB.setDefaultTimezone();
    }

    // Application entry point
    public static void main(String[] args)
    {
        VoltLogger log = new VoltLogger("ExportClient.main");
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
                .add("catalogswap", "catalog_swap", "Swap catalogs from the client", "false")
                .add("timeout","export_timeout","max seconds to wait for export to complete",300)
                .add("migrate-ttl","false","use DDL that includes TTL MIGRATE action","false")
                .add("usetableexport", "usetableexport","use DDL that includes CREATE TABLE with EXPORT ON ... action","false")
                .add("migrate-nottl", "false","use DDL that includes MIGRATE without TTL","false")
                .add("nottl-interval", "milliseconds", "approximate migrate command invocation interval (in milliseconds)", 2500)
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
            Timer timer = new Timer(true);
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

            // If migrate without TTL is enabled, set things up so a migrate is triggered
            // roughly every 2.5 seconds, with the first one happening 3 seconds from now
            // Use a separate Timer object to get a dedicated manual migration thread
            Timer migrateTimer = new Timer(true);
            Random migrateInterval = new Random();
            if (config.migrateWithoutTTL) {
                migrateTimer.scheduleAtFixedRate(new TimerTask()
                {
                    @Override
                    public void run()
                    {
                        trigger_migrate(migrateInterval.nextInt(10)); // vary the migrate/delete interval a little
                    }
                }
                , 3000l
                , config.migrateNoTTLInterval
                );
            }

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            benchmarkStartTS = System.currentTimeMillis();
            AtomicLong rowId = new AtomicLong(0);

            // Run the benchmark loop for the requested duration
            final long endTime = benchmarkStartTS + (1000l * config.duration);
            int swap_count = 0;
            Random r = new Random();
            boolean first_cat = false;
            while (endTime > System.currentTimeMillis())
            {
                long currentRowId = rowId.incrementAndGet();

                // Table with Export
                if (config.usetableexport) {
                    // call TableExport twice, once will insert, the second will randomly update or delete
                    for (int i = 0; i < 2; i++) {
                        try {
                            clientRef.get().callProcedure(
                                    new TableExportCallback(writer, r.nextInt(3)+1),
                                    "TableExport",
                                    currentRowId,
                                    0);
                        }
                        catch (Exception e) {
                            log.fatal("Exception: " + e);
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    }
                }
               else {
                // Post the request, asynchronously
                    try {
                        clientRef.get().callProcedure(
                                                      new AsyncCallback(writer, currentRowId),
                                                      config.procedure,
                                                      currentRowId,
                                                      0);
                    }
                    catch (Exception e) {
                        log.fatal("Exception: " + e);
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }

                swap_count++;
                if (((swap_count % CATALOG_SWAP_INTERVAL) == 0) && catalogSwap)
                {
                    log.info("Changing catalogs...");
                    clientRef.get().updateApplicationCatalog(catalogs[first_cat ? 0 : 1], deployment);
                    first_cat = !first_cat;
                }
            }

// ---------------------------------------------------------------------------------------------------------------------------------------------------

            // We're done - stop the performance statistics display task
            timer.cancel();
            // likewise for the migrate task
            migrateTimer.cancel();
            if (config.migrateWithoutTTL) {
                log_migrating_counts("EXPORT_PARTITIONED_TABLE_JDBC");
                log_migrating_counts("EXPORT_REPLICATED_TABLE_JDBC");
                log_migrating_counts("EXPORT_PARTITIONED_TABLE_KAFKA");
                log_migrating_counts("EXPORT_REPLICATED_TABLE_KAFKA");

                // trigger last "migrate from" cycle and wait a little bit for table to empty, assuming all is working.
                // otherwise, we'll check the table row count at a higher level and fail the test if the table is not empty.
                log.info("triggering final migrate");
                trigger_migrate(0);
                Thread.sleep(7500);

                log_migrating_counts("EXPORT_PARTITIONED_TABLE_JDBC");
                log_migrating_counts("EXPORT_REPLICATED_TABLE_JDBC");
                log_migrating_counts("EXPORT_PARTITIONED_TABLE_KAFKA");
                log_migrating_counts("EXPORT_REPLICATED_TABLE_KAFKA");
            }

            shutdown.compareAndSet(false, true);


// ---------------------------------------------------------------------------------------------------------------------------------------------------
            clientRef.get().drain();

            Thread.sleep(10000);
            // Might need lots of waiting but we'll do that in the runapp driver.
            waitForStreamedAllocatedMemoryZero(clientRef.get(),config.exportTimeout);

            //Write to export table to get count to be expected on other side.
            log.info("Writing export count as: " + TrackingResults.get(0) + " final rowid:" + rowId);
            clientRef.get().callProcedure("InsertExportDoneDetails", TrackingResults.get(0));

            writer.close(true);

            // Now print application results:

            // 1. Tracking statistics
            log.info(
                String.format(
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
            , TrackingResults.get(1))
            );
            if ( TrackingResults.get(0) + TrackingResults.get(1) != rowId.longValue() ) {
                log.info("WARNING Tracking results total doesn't match find rowId sequence number " + (TrackingResults.get(0) + TrackingResults.get(1)) + "!=" + rowId );
            }

            // 2. Print TABLE EXPORT stats if that's configured
            if (config.usetableexport) {
                log.info(
                    String.format(
                        "-------------------------------------------------------------------------------------\n"
                      + " Table/Export Results\n"
                      + "-------------------------------------------------------------------------------------\n\n"
                      + "A total of %d calls were received...\n"
                      + " - %,9d INSERT\n"
                      + " - %,9d DELETE\n"
                      + " - %,9d UPDATE (OLD)"
                      + "\n\n"
                      + "-------------------------------------------------------------------------------------\n"
                      , TrackingResults.get(0)+TrackingResults.get(1)
                      , TransactionCounts.get(INSERT)
                      , TransactionCounts.get(DELETE)
                      , TransactionCounts.get(UPDATE_OLD))
                      // old & new on each update so either = total updates, not the sum of the 2
                      // +TransactionCounts.get(UPDATE_NEW)
                      );

                long export_table_count = get_table_count("EXPORT_PARTITIONED_TABLE_LOOPBACK");
                log.info("\nEXPORT_PARTITIONED_TABLE_LOOPBACK count: " + export_table_count);
                long table_with_metadata_count = get_table_count("PARTITIONED_TABLE_WITH_METADATA");
                log.info("PARTITIONED_TABLE_WITH_METADATA count:" + table_with_metadata_count);

                // do some sanity checks on the counts...
                long meta_data_expected = TransactionCounts.get(INSERT) + TransactionCounts.get(DELETE) + TransactionCounts.get(UPDATE_OLD) * 2;
                if (table_with_metadata_count != meta_data_expected) {
                    System.err.println("ERROR: Metadata expected " + meta_data_expected +
                        " count does not match with table count: " + table_with_metadata_count + "\n");
                }
                long export_table_expected = TransactionCounts.get(INSERT) - TransactionCounts.get(DELETE);
                if (export_table_count != export_table_expected) {
                    System.err.println("Insert and delete count " + export_table_expected +
                        " does not match export table count: " + export_table_count + "\n");
                }

            }
            // 3. Performance statistics (we only care about the procedure that we're benchmarking)
            log.info(
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
            log.fatal("Exception: " + x);
            x.printStackTrace();
        }
        // if we didn't get any successes we need to fail
        if ( TrackingResults.get(0) == 0 ) {
            log.error("No successful transactions");
            System.exit(-1);
        }
    }

    private static void log_migrating_counts(String table) {
        try {
            VoltTable[] results = clientRef.get().callProcedure("@AdHoc",
                                                                "SELECT COUNT(*) FROM " + table + " WHERE MIGRATING; " +
                                                                "SELECT COUNT(*) FROM " + table + " WHERE NOT MIGRATING; " +
                                                                "SELECT COUNT(*) FROM " + table
                                                                ).getResults();
            long migrating = results[0].asScalarLong();
            long not_migrating = results[1].asScalarLong();
            long total = results[2].asScalarLong();

            log.info("row counts for " + table +
                     ": total: " + total +
                     ", migrating: " + migrating +
                     ", not migrating: " + not_migrating);
        }
        catch (Exception e) {
            // log it and otherwise ignore it.  it's not fatal to fail if the
            // SELECTS due to a migrate or some other exception
            log.fatal("log_migrating_counts exception: " + e);
            e.printStackTrace();
        }
    }

    private static void trigger_migrate(int time_window) {
        try {
            VoltTable[] results;
            if (config.procedure.equals("JiggleExportGroupSinglePartition")) {
                results = clientRef.get().callProcedure("MigratePartitionedExport", time_window).getResults();
                log.info("Partitioned Migrate - window: " + time_window + " seconds" +
                         ", kafka: " + results[0].asScalarLong() +
                         ", rabbit: " + results[1].asScalarLong() +
                         ", file: " + results[2].asScalarLong() +
                         ", jdbc: " + results[3].asScalarLong()
                         );
            } else {
                results = clientRef.get().callProcedure("MigrateReplicatedExport", time_window).getResults();
                log.info("Replicated Migrate - window: " + time_window + " seconds" +
                         ", kafka: " + results[0].asScalarLong() +
                         ", rabbit: " + results[1].asScalarLong() +
                         ", file: " + results[2].asScalarLong() +
                         ", jdbc: " + results[3].asScalarLong()
                         );
            }
        }
        catch (Exception e) {
            log.fatal("Exception: " + e);
            e.printStackTrace();
            System.exit(-1);
        }
    }


    private static long get_table_count(String sqlTable) {
        long count = 0;
        try {
            count = clientRef.get().callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + sqlTable + ";").getResults()[0].asScalarLong();
        }
        catch (Exception e) {
            log.error("Exception in get_table_count: " + e);
            log.error("SELECT COUNT from table " + sqlTable + " failed");
        }
        return count;
    }

    /**
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    static void connect() throws InterruptedException {
        log.info("Connecting to VoltDB...");

        String[] serverArray = config.parsedServers;
        Client client = clientRef.get();
        for (final String server : serverArray) {
        // connect to the first server in list; with TopologyChangeAware set, no need for more
            try {
                client.createConnection(server, config.port);
                break;
            }catch (Exception e) {
                log.error("Connection to " + server + " failed.\n");
            }
        }
    }

    static Client createClient() {
        ClientConfig clientConfig = new ClientConfig("", "");
        // clientConfig.setReconnectOnConnectionLoss(true); **obsolete**
        clientConfig.setClientAffinity(true);
        clientConfig.setTopologyChangeAware(true);

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
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     **
     * @return
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
        // switch from app's runtime to VoltLogger clock time so results line up
        // with apprunner if running in that framework

        String stats_out = String.format(" Throughput %d/s, ", stats.getTxnThroughput());
        stats_out += String.format("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        stats_out += String.format("Avg/95%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.95));
        log.info(stats_out);
    }

    /**
     * Wait for export processor to catch up and have nothing to be exported.
     *
     * @param client
     * @throws Exception
     */
    public static void waitForStreamedAllocatedMemoryZero(Client client) throws Exception {
        waitForStreamedAllocatedMemoryZero(client,300);
    }

    public static void waitForStreamedAllocatedMemoryZero(Client client,Integer timeout) throws Exception {
        boolean passed = false;
        Instant maxTime = Instant.now().plusSeconds(timeout);
        Instant maxStatsTime = Instant.now().plusSeconds(60);
        long lastPending = 0;
        VoltTable stats = null;

        // this is a problem -- Quiesce forces queuing but does NOT mean export is done
        try {
            log.info(client.callProcedure("@Quiesce").getResults()[0]);
        }
        catch (Exception ex) {
        }
        while (true) {

            if ( Instant.now().isAfter(maxStatsTime) ) {
                throw new Exception("Test Timeout waiting for non-null @Statistics call");
            }
            try {
                stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
                maxStatsTime = Instant.now().plusSeconds(60);
            }
            catch (Exception ex) {
                // Export Statistics are updated asynchronously and may not be up to date immediately on all hosts
                // retry a few times if we don't get an answer
                log.error("Problem getting @Statistics export: "+ex.getMessage());
            }
            if (stats == null) {
                Thread.sleep(5000);
                continue;
            }
            boolean passedThisTime = true;
            while (stats.advanceRow()) {
                if ( Instant.now().isAfter(maxTime) ) {
                    throw new Exception("Test Timeout waiting for export to drain, expecting non-zero TUPLE_PENDING Statistic, "
                    + "increase --timeout arg for slower clients" );
                }
                Long pending = stats.getLong("TUPLE_PENDING");
                String stream = stats.getString("SOURCE");
                String target = stats.getString("TARGET");
                String active = stats.getString("ACTIVE");
                Long partition = stats.getLong("PARTITION_ID");

                // don't wait for inactive partitions.
                // there are cases where ACTIVE==FALSE is a bug that won't get caught here anyway
                if (active.equalsIgnoreCase("FALSE"))
                    continue;
                if ( pending != lastPending) {
                    // reset the timer if we are making progress
                    maxTime = Instant.now().plusSeconds(timeout);
                    lastPending = pending;
                }
                // switch this message to debug out?
                log.info("DEBUG: Partition "+partition+" for stream "+stream+", target "+target+" TUPLE_PENDING is "+pending);
                if (pending != 0) {
                    passedThisTime = false;
                    log.info("Partition "+partition+" for stream "+stream+" TUPLE_PENDING is not zero, got "+pending);
                    break;
                }
            }
            if (passedThisTime) {
                passed = true;
                break;
            }
            Thread.sleep(5000);
        }
        log.info("Passed is: " + passed);
        log.info(stats);
    }

}
