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

package genqa;

import java.io.File;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;
import org.voltdb.ClientResponseImpl;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.exampleutils.AppHelper;

public class AsyncExportClient {
    static VoltLogger log = new VoltLogger("ExportClient");

    // Operations matched with operations defined in ExportTupleStream::STREAM_ROW_TYPE
    private static enum OperationType {
        INSERT(0),
        DELETE(1),
        UPDATE(2);
        final int op;
        OperationType(int type) {
            this.op = type;
        }
        public int get() {
            return op;
        }
    }

    static class ExportCallback implements ProcedureCallback {
        private final OperationType m_op;
        private final AtomicLongArray m_transactionCounts;
        private final AtomicLongArray m_committedCounts;
        private final AtomicLong m_failedCounts;
        public ExportCallback(OperationType op, AtomicLongArray trancationCounts, AtomicLongArray committedCounts, AtomicLong failedCounts) {
            m_op = op;
            m_transactionCounts = trancationCounts;
            m_committedCounts = committedCounts;
            m_failedCounts = failedCounts;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == ClientResponse.SUCCESS) {
                VoltTable v = clientResponse.getResults()[0];
                // Increase the count only when the sql statement affects rows.
                // DELETE/UPDATE  may not affect any rows if INSERT with the same row id fails.
                // See proc TableExport
                if (m_op == OperationType.INSERT || v.getRowCount() > 0) {
                    m_committedCounts.incrementAndGet(m_op.get());
                }
            } else {
                log.info("Transaction failed: " + ((ClientResponseImpl)clientResponse).toJSONString());
                m_failedCounts.incrementAndGet();
            }
            m_transactionCounts.incrementAndGet(m_op.get());
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
        final String [] parsedServers;
        final String procedure;
        final int exportTimeout;
        final boolean migrateWithTTL;
        final boolean usetableexport;
        final boolean usecdc;
        final boolean migrateWithoutTTL;
        final long migrateNoTTLInterval;

        ConnectionConfig( AppHelper apph) {
            displayInterval      = apph.longValue("displayinterval");
            duration             = apph.longValue("duration");
            servers              = apph.stringValue("servers");
            port                 = apph.intValue("port");
            poolSize             = apph.intValue("poolsize");
            rateLimit            = apph.intValue("ratelimit");
            procedure            = apph.stringValue("procedure");
            parsedServers        = servers.split(",");
            exportTimeout        = apph.intValue("timeout");
            migrateWithTTL       = apph.booleanValue("migrate-ttl");
            usetableexport       = apph.booleanValue("usetableexport");
            usecdc               = apph.booleanValue("usecdc");
            migrateWithoutTTL    = apph.booleanValue("migrate-nottl");
            migrateNoTTLInterval = apph.longValue("nottl-interval");

        }
    }

    // Connection Configuration
    private static ConnectionConfig config;

    private static String[] TABLES = { "EXPORT_PARTITIONED_TABLE_JDBC",
                                       "EXPORT_REPLICATED_TABLE_JDBC",
                                       "EXPORT_PARTITIONED_TABLE_KAFKA",
                                       "EXPORT_REPLICATED_TABLE_KAFKA"};

    static {
        VoltDB.setDefaultTimezone();
    }

    // Application entry point
    public static void main(String[] args) {
        VoltLogger log = new VoltLogger("ExportClient.main");
        Client clientRef = null;
        long export_table_expected = 0;
        try {
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
                .add("timeout","export_timeout", "max seconds to wait for export to complete",300)
                .add("migrate-ttl","false", "use DDL that includes TTL MIGRATE action","false")
                .add("usetableexport", "usetableexport","use DDL that includes CREATE TABLE with EXPORT ON ... action","false")
                .add("usecdc", "usecdc", "Report inserts, deletes, updates", "false")
                .add("migrate-nottl", "false", "use DDL that includes MIGRATE without TTL","false")
                .add("nottl-interval", "milliseconds", "approximate migrate command invocation interval (in milliseconds)", 2500)
                .setArguments(args);

            config = new ConnectionConfig(apph);

            // Retrieve parameters
            final String csv           = apph.stringValue("statsfile");

            // Validate parameters
            apph.validate("duration", (config.duration > 0))
                .validate("poolsize", (config.poolSize > 0))
                .validate("ratelimit", (config.rateLimit > 0));

            // Display actual parameters, for reference
            apph.printActualUsage();

            // Get a client connection - we retry for a while in case the server hasn't started yet
            final Client client = createClient();
            clientRef = client;
            // Statistics manager objects from the client
            ClientStatsContext periodicStatsContext = client.createStatsContext();
            ClientStatsContext fullStatsContext = client.createStatsContext();

            // Create a Timer task to display performance data on the procedure
            Timer timer = new Timer(true);
            timer.scheduleAtFixedRate(new TimerTask() {
                      @Override
                      public void run() {
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
                migrateTimer.scheduleAtFixedRate(new TimerTask() {
                        @Override
                        public void run() {
                            trigger_migrate(migrateInterval.nextInt(10), client); // vary the migrate/delete interval a little
                        }
                    }
                    , 3000l
                    , config.migrateNoTTLInterval
                );
            }

            // Keep track of various counts for INSERT (0), UPDATE (3) and DEELTE (2)
            AtomicLongArray transactionCounts = new AtomicLongArray(3);
            AtomicLongArray committedCounts = new AtomicLongArray(3);
            AtomicLongArray queuedCounts = new AtomicLongArray(3);
            AtomicLong failedCounts = new AtomicLong(0);

            AtomicLong rowId = new AtomicLong(0);
            // Run the benchmark loop for the requested duration
            final long endTime = System.currentTimeMillis() + (1000l * config.duration);
            OperationType [] ops = {OperationType.INSERT, OperationType.UPDATE, OperationType.DELETE};
            while (endTime > System.currentTimeMillis()) {
                long currentRowId = rowId.incrementAndGet();

                // Table with Export, do insert, update and delete
                if (config.usetableexport || config.usecdc) {
                    String sqlTable = config.usecdc ? config.procedure : "TableExport";
                    for (OperationType op : ops) {
                        try {
                            client.callProcedure(
                                    new ExportCallback(op, transactionCounts, committedCounts, failedCounts),
                                    sqlTable,
                                    currentRowId,
                                    op.get());
                            queuedCounts.incrementAndGet(op.get());
                        } catch (Exception e) {
                            log.info("Exception: " + e);
                            e.printStackTrace();
                        }
                    }
                } else {
                   try {
                       client.callProcedure(
                               new ExportCallback(OperationType.INSERT, transactionCounts, committedCounts, failedCounts),
                               config.procedure,
                               currentRowId,
                               0);
                   } catch (Exception e) {
                        log.info("Exception: " + e);
                        e.printStackTrace();
                    }
                }
            }

            // We're done - stop the performance statistics display task
            timer.cancel();
            migrateTimer.cancel();

            if (config.migrateWithoutTTL) {
                for (String t : TABLES) {
                    log_migrating_counts(t, client);
                }
                // trigger last "migrate from" cycle and wait a little bit for table to empty, assuming all is working.
                // otherwise, we'll check the table row count at a higher level and fail the test if the table is not empty.
                log.info("triggering final migrate");
                trigger_migrate(0, client);
                Thread.sleep(7500);
                for (String t : TABLES) {
                    log_migrating_counts(t, client);
                }
            }

            client.drain();

            Thread.sleep(10000);

            long totalCount = 0, totalQueued = 0;
            for (OperationType op : ops) {
                totalCount += transactionCounts.get(op.get());
                totalQueued += totalQueued;
                export_table_expected += committedCounts.get(op.get());
            }
            // Opeation UPDATE will produce 2 export rows from both old and new tuples.
            export_table_expected += committedCounts.get(OperationType.UPDATE.get());

            if (totalCount != totalQueued) {
                log.info("The transaction count " + totalCount + " does not match with the queued count " + totalQueued);
            }

            //Write to export table to get count to be expected on other side.
            log.info("Writing export count as: " + export_table_expected + " final rowid:" + rowId);
            if (config.migrateWithTTL) {
                client.callProcedure("InsertMigrateDoneDetails", export_table_expected);
            } else {
                client.callProcedure("InsertExportDoneDetails", export_table_expected);
            }

            // 1. Tracking statistics
            log.info(
                    String.format(
                            "---------------------------------- Benchmark Results ------------------------------------------\n"
                                    + "A total of %d calls was received...\n"
                                    + " - %,9d Succeeded\n"
                                    + " - %,9d Failed (Transaction Error)\n\n\n"
                                    , totalCount, export_table_expected, failedCounts.get()
                    ));

            // 2. Print TABLE EXPORT stats if that's configured
            if (config.usetableexport || config.usecdc) {
                String msg =  String.format(
                        "---------------------------------Export Committed and Queued Counts --------------------------------------\n"
                                + "A total of %d calls were committed, a total of %d calls were queued\n"
                                + " - %,9d Committed-Inserts\n"
                                + " - %,9d Committed-Deletes\n"
                                + " - %,9d Committed-Updates\n"
                                + " - %,9d Transaction-Inserts\n"
                                + " - %,9d Transaction-Deletes\n"
                                + " - %,9d Transaction-Updates\n"
                                + " - %,9d Queued-Inserts\n"
                                + " - %,9d Queued-Deletes\n"
                                + " - %,9d Queued-Updates"
                                + "\n\n"
                                , totalCount, totalQueued
                                , committedCounts.get(OperationType.INSERT.get())
                                , committedCounts.get(OperationType.DELETE.get())
                                , committedCounts.get(OperationType.UPDATE.get())
                                , transactionCounts.get(OperationType.INSERT.get())
                                , transactionCounts.get(OperationType.DELETE.get())
                                , transactionCounts.get(OperationType.UPDATE.get())
                                , queuedCounts.get(OperationType.INSERT.get())
                                , queuedCounts.get(OperationType.DELETE.get())
                                , queuedCounts.get(OperationType.UPDATE.get()));
                log.info(msg);

                String sqlTable = config.usecdc ? "EXPORT_PARTITIONED_TOPIC_CDC" : "EXPORT_PARTITIONED_TABLE_CDC";
                long export_table_count = get_table_count(sqlTable, client);
                log.info("\n" + sqlTable + " count: " + export_table_count);
                if (export_table_count != export_table_expected) {
                    log.info("Insert and delete count " + export_table_expected +
                        " does not match export table count: " + export_table_count + "\n");
                }
            }
            // 3. Performance statistics (we only care about the procedure that we're benchmarking)
            log.info("\n\n-------------------------------- System Statistics ---------------------------------------\n");
            printStatistics(fullStatsContext,false);

            // Dump statistics to a CSV file
            client.writeSummaryCSV(
                    fullStatsContext.getStatsByProc().get(config.procedure),
                    csv
                    );
        } catch(Exception ex) {
            log.fatal("Exception: " + ex);
            ex.printStackTrace();
        } finally {
            if (clientRef != null) {
                try {
                    clientRef.close();
                } catch (Exception e) {}
            }
        }
        // if we didn't get any successes we need to fail
        if ( export_table_expected == 0 ) {
            log.error("No successful transactions");
            System.exit(-1);
        }
    }

    private static void log_migrating_counts(String table, Client client) {
        try {
            VoltTable[] results = client.callProcedure("@AdHoc",
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
        catch (ProcCallException e) {
            // Proc call failed. OK if connection went down, tests expect that.
            // In any case, no action taken other than logging.
            byte st = e.getClientResponse().getStatus();
            String err = String.format("Procedure call failed in log_migrating_counts: %s (status %d)",
                                       e.getClientResponse().getStatusString(), st);
            if (st == ClientResponse.CONNECTION_LOST || st == ClientResponse.CONNECTION_TIMEOUT) {
                log.info(err);
            } else {
                log.error(err);
            }
        }
        catch (Exception e) {
            // log it and otherwise ignore it.  it's not fatal to fail if the
            // SELECTS due to a migrate or some other exception
            log.fatal("log_migrating_counts exception: " + e);
            e.printStackTrace();
        }
    }

    private static void trigger_migrate(int time_window, Client client) {
        try {
            VoltTable[] results;
            if (config.procedure.equals("JiggleExportGroupSinglePartition")) {
                ClientResponseWithPartitionKey[] responses  = client.callAllPartitionProcedure("MigratePartitionedExport",
                        time_window);
                for (ClientResponseWithPartitionKey resp : responses) {
                    if (ClientResponse.SUCCESS == resp.response.getStatus()){
                        VoltTable res = resp.response.getResults()[0];
                        log.info("Partitioned Migrate - window: " + time_window + " seconds" +
                                ", kafka: " + res.asScalarLong() +
                                ", file: " + res.asScalarLong() +
                                ", jdbc: " + res.asScalarLong() +
                                ", on partition " + resp.partitionKey
                                );
                    } else {
                        log.info("WARNING: fail to migrate on partition:" + resp.partitionKey);
                    }
                }
            } else {
                results = client.callProcedure("MigrateReplicatedExport", time_window).getResults();
                log.info("Replicated Migrate - window: " + time_window + " seconds" +
                         ", kafka: " + results[0].asScalarLong() +
                         ", file: " + results[1].asScalarLong() +
                         ", jdbc: " + results[2].asScalarLong()
                         );
            }
        }
        catch (ProcCallException e1) {
            if (e1.getMessage().contains("was lost before a response was received")) {
                log.warn("Possible problem executing " + config.procedure + ", procedure may not have completed");
            } else {
                log.fatal("Exception: " + e1);
                e1.printStackTrace();
                System.exit(-1);
            }
        }
        catch (Exception e) {
            log.fatal("Exception: " + e);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static long get_table_count(String sqlTable, Client client) {
        long count = 0;
        try {
            count = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + sqlTable + ";").getResults()[0].asScalarLong();
        }
        catch (Exception e) {
            log.error("Exception in get_table_count: " + e);
            log.error("SELECT COUNT from table " + sqlTable + " failed");
        }
        return count;
    }

    static Client createClient() {
        ClientConfig clientConfig = new ClientConfig("", "");
        clientConfig.setTopologyChangeAware(true);
        clientConfig.setMaxTransactionsPerSecond(config.rateLimit);

        Client client = ClientFactory.createClient(clientConfig);
        String[] serverArray = config.parsedServers;
        for (final String server : serverArray) {
        // connect to the first server in list; with TopologyChangeAware set, no need for more
            try {
                client.createConnection(server, config.port);
                break;
            }catch (Exception e) {
                log.error("Connection to " + server + " failed.\n");
            }
        }
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
}
