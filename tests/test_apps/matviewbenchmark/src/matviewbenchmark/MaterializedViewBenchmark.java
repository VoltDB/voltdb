/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package matviewbenchmark;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.Timer;
import java.util.TimerTask;

import org.voltdb.*;
import org.voltdb.client.*;

public class MaterializedViewBenchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    final MatViewConfig config;
    long benchmarkStartTS;
    boolean benchmarkActive;
    final Client client;
    Timer timer;
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    double insert_throughput;
    double insert_execute;

    double delete_throughput;
    double delete_execute;

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class MatViewConfig extends CLIConfig {

        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of transactions to perform for benchmark.")
        int txn = 10000000;

        @Option(desc = "Number of transactions to perform during warmup.")
        int warmup = 100000;

        @Option(desc = "Number of groupings for materialized view (0 for 1:1 group to id ratio).")
        int group = 0;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Override
        public void validate() {
            if (displayinterval <= 0)
                exitWithMessageAndUsage("displayinterval must be > 0");
            if (txn < 0)
                exitWithMessageAndUsage("txn must be 0 or a positive integer");
            if (warmup < 0)
                exitWithMessageAndUsage("warmup must be 0 or a positive integer");
            if (group < 0)
                exitWithMessageAndUsage("group must be 0 or a positive integer");
        }
    }

    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            // if the benchmark is still active
            if (benchmarkActive) {
                System.exit(-1);
            }
        }
    }

    public MaterializedViewBenchmark(MatViewConfig config) {
        this.config = config;

        benchmarkActive = false;

        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());

        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        insert_throughput = 0;
        insert_execute = 0;
        delete_throughput = 0;
        delete_execute = 0;
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
     * Create a Timer task to display performance data every displayInterval seconds
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
        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Txns Completed %d ", stats.getInvocationsCompleted());
        System.out.printf("Avg/95%% Latency %.2f/%.2fms\n", stats.getAverageLatency(),
                          stats.kPercentileLatencyAsDouble(0.95));
        System.out.println("");
    }


    /**
     * Prints the results and statistics about performance.
     * @param procedure The name of the stored procedure that was tested.
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults(String procedure) throws Exception {
        ClientStats stats = fullStatsContext.fetchAndResetBaseline().getStats();
        double execTimeInMicroSec = 0.0;

        // 1. Results and performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Results");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput: %,9d txns/sec\n", stats.getTxnThroughput());

        VoltTable procStats = client.callProcedure("@Statistics",
                                                   "procedureprofile",
                                                   0).getResults()[0];

        while (procStats.advanceRow()) {
            String procName = procStats.getString("PROCEDURE");
            if (procName.equals(procedure)) {
                execTimeInMicroSec = (procStats.getLong("AVG") / 1000.0);
                System.out.printf("Average execution time: %,9f usec\n", execTimeInMicroSec);
                break;
            }
        }
    }

    /**
     * Prints the results and statistics about performance.
     * @param procedure The name of the stored procedure that was tested.
     *        fw        File writer object to write stats to.
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults(String procedure, FileWriter fw, String suffix) throws Exception {
        ClientStats stats = fullStatsContext.fetchAndResetBaseline().getStats();
        double execTimeInMicroSec = 0.0;

        // 1. Results and performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Results");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput: %,9d txns/sec\n", stats.getTxnThroughput());

        VoltTable procStats = client.callProcedure("@Statistics",
                                                   "procedureprofile",
                                                   0).getResults()[0];

        while (procStats.advanceRow()) {
            String procName = procStats.getString("PROCEDURE");
            if (procName.equals(procedure)) {
                execTimeInMicroSec = (procStats.getLong("AVG") / 1000.0);
                System.out.printf("Average execution time: %,9f usec\n", execTimeInMicroSec);
                break;
            }
        }

        // 3. Write stats to file if requested
        fw.append(String.format("%s,%d,-1,%d,0,0,0,%.2f,0,0,0,0,0,0\n",
                                suffix,
                                stats.getStartTimestamp(),
                                stats.getTxnThroughput(),
                                execTimeInMicroSec));

        // Expecting the custom insert/delete procedure names ex. ids_insert
        if (procedure.split("_")[1].equals("insert")) {
            if (insert_throughput > 0) {
                insert_throughput = (((stats.getTxnThroughput() - insert_throughput) /
                                      stats.getTxnThroughput()) * 100);
                insert_execute = (((insert_execute - execTimeInMicroSec) /
                                   execTimeInMicroSec) * 100);
                fw.append(String.format("%s,%d,-1,%.2f,0,0,0,%.2f,0,0,0,0,0,0\n",
                                        "Insert Diff",
                                        stats.getStartTimestamp(),
                                        insert_throughput,
                                        insert_execute));
            } else {
                insert_throughput = (double)stats.getTxnThroughput();
                insert_execute = execTimeInMicroSec;
            }
        } else {
            if (delete_throughput > 0) {
                delete_throughput = (((stats.getTxnThroughput() - delete_throughput) /
                                      stats.getTxnThroughput()) * 100.0);
                delete_execute = (((delete_execute - execTimeInMicroSec) /
                                   execTimeInMicroSec) * 100);
                fw.append(String.format("%s,%d,-1,%.2f,0,0,0,%.2f,0,0,0,0,0,0\n",
                                        "Delete Diff",
                                        stats.getStartTimestamp(),
                                        delete_throughput,
                                        delete_execute));
            } else {
                delete_throughput = (double)stats.getTxnThroughput();
                delete_execute = execTimeInMicroSec;
            }
        }
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @param configPath  path to configuration file
     *
     * @throws Exception
     */
    public void runBenchmark() throws Exception {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        // connect to one or more servers, loop until success
        connect(config.servers);

        System.out.print(HORIZONTAL_RULE);
        System.out.println("Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        benchmarkActive = true;

        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        if (config.warmup > 0) {
            System.out.println("Warming up...");
            for (int i=0; i<config.warmup; i++){
                client.callProcedure(new NullCallback(),
                                     "ids_insert",
                                     i,
                                     i);
                client.callProcedure(new NullCallback(),
                                     "idsWithMatView_insert",
                                     i,
                                     i);
            }
            client.drain();
            for (int i=0; i<config.warmup; i++){
                client.callProcedure(new NullCallback(),
                                     "ids_delete",
                                     i);
                client.callProcedure(new NullCallback(),
                                     "idsWithMatView_delete",
                                     i);
            }
            client.drain();
        }

        int grp = 1;
        FileWriter fw = null;
        if ((config.statsfile != null) && (config.statsfile.length() != 0)) {
            fw = new FileWriter(config.statsfile);
        }

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        // Run the benchmark for the requested duration.
        System.out.println("\nRunning benchmark...\n");
        System.out.println("\n\nInserting into table w/ materialized view...\n");

        if (config.group > 0) {
            for (int i=0; i<config.txn; i++){
                client.callProcedure(new NullCallback(),
                                     "idsWithMatView_insert",
                                     i,
                                     grp);
                if (grp == config.group) {
                    grp = 1;
                } else {
                    grp++;
                }
            }
        } else {
            for (int i=0; i<config.txn; i++){
                client.callProcedure(new NullCallback(),
                                     "idsWithMatView_insert",
                                     i,
                                     i);
            }
        }
        timer.cancel();
        client.drain();

        grp = 1;

        if ((config.statsfile == null) || (config.statsfile.length() == 0)) {
            printResults("idsWithMatView_insert");
        } else {
            printResults("idsWithMatView_insert", fw, "Insert w MV");
        }
        System.out.print(HORIZONTAL_RULE);

        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        System.out.println("\n\nDeleting from table w/ materialized view...\n");
        for (int i=0; i<config.txn; i++){
            client.callProcedure(new NullCallback(),
                                 "idsWithMatView_delete",
                                 i);
        }
        timer.cancel();
        client.drain();

        if ((config.statsfile == null) || (config.statsfile.length() == 0)) {
            printResults("idsWithMatView_delete");
        } else {
            printResults("idsWithMatView_delete", fw, "Delete w MV");
        }
        System.out.print(HORIZONTAL_RULE);

        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();
        System.out.println("\n\nInserting into table w/o materialized view...\n");
        if (config.group > 0) {
            for (int i=0; i<config.txn; i++){
                client.callProcedure(new NullCallback(),
                                     "ids_insert",
                                     i,
                                     grp);
                if (grp == config.group) {
                    grp = 1;
                } else {
                    grp++;
                }
            }
        } else {
            for (int i=0; i<config.txn; i++){
                client.callProcedure(new NullCallback(),
                                     "ids_insert",
                                     i,
                                     i);
            }
        }
        timer.cancel();
        client.drain();

        if ((config.statsfile == null) || (config.statsfile.length() == 0)) {
            printResults("ids_insert");
        } else {
            printResults("ids_insert", fw, "Insert wo MV");
        }
        System.out.print(HORIZONTAL_RULE);

        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();
        System.out.println("\n\nDeleting from table w/o materialized view...\n");
        for (int i=0; i<config.txn; i++){
            client.callProcedure(new NullCallback(),
                                 "ids_delete",
                                 i);
        }
        timer.cancel();
        client.drain();

        if ((config.statsfile == null) || (config.statsfile.length() == 0)) {
            printResults("ids_delete");
        } else {
            printResults("ids_delete", fw, "Delete wo MV");
            fw.close();
        }
        benchmarkActive = false;

        // close down the client connections
        client.close();
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */
    public static void main(String[] args) throws Exception{
        MatViewConfig config = new MatViewConfig();
        config.parse(MaterializedViewBenchmark.class.getName(), args);

        MaterializedViewBenchmark benchmark = new MaterializedViewBenchmark(config);
        benchmark.runBenchmark();
    }
}
