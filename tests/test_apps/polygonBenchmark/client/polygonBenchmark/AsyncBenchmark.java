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
 * model to VoltDB as it allows a single client with a small amount of
 * threads to flood VoltDB with requests, guaranteeing blazing throughput
 * performance.
 *
 * Note that this benchmark focuses on throughput performance and
 * not low latency performance.  This benchmark will likely 'firehose'
 * the database cluster (if the cluster is too slow or has too few CPUs)
 * and as a result, queue a significant amount of requests on the server
 * to maximize throughput measurement. To test VoltDB latency, run the
 * SyncBenchmark client, also found in the voter sample directory.
 */

package polygonBenchmark;

import java.io.File;
import java.io.IOException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;

public class AsyncBenchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // potential return codes (synced with Vote procedure)
    final PolygonConfig config;
    // Reference to the database connection we will use
    final Client client;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    // Number polygons.
    long m_numberPolygons;
    // Number polygons needing repair.
    long m_numberRepairedPolygons;
    // Good and bad polygons
    String m_goodPolygonWKT;
    String m_badPolygonWKT;
    GeographyValue m_goodPolygon;
    GeographyValue m_badPolygon;
    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    public enum HowToRun {
        StoredProcedureWKT,
        StoredProcedureSerialized,
        BuiltInWKT,
        BuiltInSerialized
    };

    static class PolygonConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 20;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 2;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Number of vertices per polygon.")
        int vertices = 10000;

        @Option(desc = "Fraction of polygons needing repair.")
        double repairFrac = 0.5;

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Report latency for async benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";

        @Option(desc = "Synchronous calls (for testing.)")
        boolean synchronous = false;

        @Option(desc = "What function should we use to insert?")
        String insertFunction = "undefined";

        @Option(desc = "Name of the CSV file.  The default is polygonBenchmark.csv.")
        String csvFileName = "polygonBenchmark.csv";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
            if ((repairFrac < 0) || (repairFrac > 1.0)) {
                exitWithMessageAndUsage("repairFrac must be in the interval [0, 1]");
            }
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
            if ((System.currentTimeMillis() - benchmarkStartTS) < (config.duration * 1000)) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }
        }
    }

    /**
     * Create a regular convex polygon, with an optional hole.
     *
     * Note that the resulting polygon will be symmetric around any line
     * through the center and a vertex.  Consequently, the centroid of such
     * a polygon must be the center of the polygon.
     *
     * This is stolen from the testing class PolygonFactory.
     *
     * @param center The center of the polygon.
     * @param firstVertex The coordinates of the first vertex.
     * @param numVertices The number of vertices.
     * @param sizeOfHole If this is positive, we also create a hole whose vertices are
     *                   at the same angle from the polygon's center, but whose distance
     *                   is scaled by sizeOfHole.  This value must be in the range [0,1).
     * @return
     */
    private static GeographyValue createRegularConvex(
            GeographyPointValue center,
            GeographyPointValue firstVertex,
            int numVertices,
            double sizeOfHole) {
        assert(0 <= sizeOfHole && sizeOfHole < 1.0);
        double phi = 360.0/numVertices;
        GeographyPointValue holeFirstVertex = null;
        if (sizeOfHole > 0) {
            holeFirstVertex = firstVertex.scale(center, sizeOfHole);
        }
        List<GeographyPointValue> oneLoop = new ArrayList<>();
        List<GeographyPointValue> hole = (sizeOfHole < 0 ? null : new ArrayList<>());
        // We will add the nth point at angle n*phi.  We want to
        // add points in a counter clockwise order, so phi must be
        // a positive angle.  We will have twice as many vertices
        // as points.
        for (int idx = 0; idx < numVertices; idx += 1) {
            oneLoop.add(firstVertex.rotate(idx*phi, center));
            if (sizeOfHole > 0) {
                hole.add(holeFirstVertex.rotate(-(idx*phi), center));
            }
        }
        // Add the closing vertices.
        oneLoop.add(firstVertex);
        if (sizeOfHole > 0) {
            hole.add(holeFirstVertex);
        }
        List<List<GeographyPointValue>> loops = new ArrayList<>();
        loops.add(oneLoop);
        if (sizeOfHole > 0) {
            loops.add(hole);
        }
        return new GeographyValue(loops);
    }

    /**
     * Reverse all the loops in a polygon.  Don't change the
     * order of the loops, just reverse each loop.
     *
     * This is useful for testing a malformed polygon.
     *
     * This is stolen from the testing class PolygonFactory.
     *
     * @param goodPolygon
     * @return
     */
    public static GeographyValue reverseLoops(GeographyValue goodPolygon) {
        List<List<GeographyPointValue>> newLoops = new ArrayList<>();
        List<List<GeographyPointValue>> oldLoops = goodPolygon.getRings();
        for (List<GeographyPointValue> loop : oldLoops) {
            // Copy loop, but reverse the points.
            List<GeographyPointValue> newLoop = new ArrayList<>();
            // Leave the first and last one fixed, but copy
            // all the others from the end.
            newLoop.add(loop.get(0));
            for (int idx = loop.size() - 2; idx > 1; idx -= 1) {
                newLoop.add(loop.get(idx));
            }
            newLoop.add(loop.get(0));
            newLoops.add(newLoop);
        }
        return new GeographyValue(newLoops);
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public AsyncBenchmark(PolygonConfig config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig(config.user, config.password, new StatusListener());
        clientConfig.setMaxTransactionsPerSecond(config.ratelimit);

        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
        if(config.latencyreport) {
            System.out.println("NOTICE: Option latencyreport is ON for async run, please set a reasonable ratelimit.\n");
        }
        //
        // Create a 100 point regular polygon centered around <50.0, 50.0> with
        // radius 10.0.
        //
        m_goodPolygon = createRegularConvex(new GeographyPointValue(50.0, 50.0),
                                            new GeographyPointValue(50.0, 60.0),
                                            config.vertices,
                                            0.0);
        m_badPolygon = reverseLoops(m_goodPolygon);
        m_goodPolygonWKT = m_goodPolygon.toWKT();
        m_badPolygonWKT  = m_badPolygon.toWKT();
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
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
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
        System.out.printf("Aborts/Failures %d/%d",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        if(this.config.latencyreport) {
            System.out.printf(", Avg/95%% Latency %.2f/%.2fms", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.95));
        }
        System.out.printf("\n");
    }

    private void logToCSV(String CSVFileName,
                          String insertFunctionName,
                          String howToRun,
                          int    vertices,
                          int    repairFrac,
                          double txnThroughput) {
        File csvFile = new File(CSVFileName);
        boolean initialize = ( ! csvFile.exists() );
        FileWriter write = null;
        PrintWriter print = null;
        try {
            write = new FileWriter( csvFile , true);
            print = new PrintWriter( write , true);
            if (initialize) {
                print.printf("InsertFunctionName, HowToRun, NumberVertices, RepairFraction, TransactionThroughPut\n");
            }
            print.printf("%s, %s, %d, %d, %f\n",
                     insertFunctionName,
                     howToRun,
                     vertices,
                     repairFrac,
                     txnThroughput);
        } catch (IOException ex) {
            System.out.printf("IOException: %s\n", ex.getMessage());
            System.exit(100);
        } finally {
            if (print != null) {
                print.close();
            }
        }
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults(HowToRun htr) throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        System.out.print(HORIZONTAL_RULE);
        System.out.printf("Test %s\n", htr.name());
        System.out.printf(" %d inserted polygons, %d needed repairs.\n",
                          m_numberPolygons,
                          m_numberRepairedPolygons);
        System.out.println(HORIZONTAL_RULE);
        // Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);
        System.out.printf("AVG: %s %s %d %d %9d\n",
                          config.insertFunction,
                          htr.name(),
                          config.vertices,
                          (int)(config.repairFrac*100),
                          stats.getTxnThroughput());
        logToCSV(config.csvFileName,
                 config.insertFunction,
                 htr.name(),
                 config.vertices,
                 (int)(config.repairFrac*100),
                 stats.getTxnThroughput());
        if(this.config.latencyreport) {
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

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" System Server Statistics");
            System.out.println(HORIZONTAL_RULE);
            System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" Latency Histogram");
            System.out.println(HORIZONTAL_RULE);
            System.out.println(stats.latencyHistoReport());
        }
        // 4. Write stats to file if requested
        client.writeSummaryCSV(stats, config.statsfile);
    }

    String getPolygonWKT(boolean needsRepair) {
        return (needsRepair ? m_badPolygonWKT : m_goodPolygonWKT);
    }

    GeographyValue getPolygon(boolean needsRepair) {
        return needsRepair ? m_badPolygon : m_goodPolygon;
    }
    /**
     * Callback to handle the response to a stored procedure call.
     * Tracks response types.
     *
     */
    class PolygonCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse response) throws Exception {
            if (response.getStatus() != ClientResponse.SUCCESS) {
                System.out.printf("Failure: %d: %s\n",
                                  response.getStatus(),
                                  response.getStatusString());
            }
        }
    }

    private void callOperation(Client                client,
                               HowToRun              howToRun,
                               long                  id,
                               boolean               needsRepair,
                               String                insertFunction)
                                       throws NoConnectionsException,
                                              IOException,
                                              ProcCallException,
                                              InterruptedException {
        String wkt = getPolygonWKT(needsRepair);
        GeographyValue polygon = getPolygon(needsRepair);
        PolygonCallback cb = new PolygonCallback();
        switch (howToRun) {
        case StoredProcedureWKT:
            client.callProcedure(cb,
                                 "InsertPolygonAsString",
                                 id,
                                 wkt);

            break;
        case StoredProcedureSerialized:
            client.callProcedure(cb,
                                 "InsertPolygonAsObject",
                                 id,
                                 polygon);
            break;
        case BuiltInWKT:
            client.callProcedure(cb,
                                 "@AdHoc",
                                 String.format("insert into polygons values (?, %s(?))",
                                               insertFunction),
                                 id,
                                 wkt);
            break;
        case BuiltInSerialized:
            client.callProcedure(cb,
                                 "POLYGONS.insert",
                                 id,
                                 polygon);
            break;
        default:
            System.err.printf("Undefined howToRun value %s\n", howToRun);
            System.exit(100);
        }
        if (config.synchronous) {
            cb.wait();
        }
    }
    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark(HowToRun htr, String insertFunction) throws Exception {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        // connect to one or more servers, loop until success
        connect(config.servers);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        client.callProcedure("@AdHoc", "truncate table polygons;");
        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        System.out.println("Warming up...");
        final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
        long id = 0;
        while (warmupEndTime > System.currentTimeMillis()) {
            // asynchronously call the "Vote" procedure
            boolean needsRepair = Math.random() < config.repairFrac;
            callOperation(client, htr, id++, needsRepair, insertFunction);
        }

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();


        client.callProcedure("@AdHoc", "truncate table polygons;");

        id = m_numberPolygons = m_numberRepairedPolygons = 0;

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        System.out.printf("\nRunning %s benchmark...\n", htr.name());
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
        while (benchmarkEndTime > System.currentTimeMillis()) {
            boolean needsRepair = Math.random() < config.repairFrac;
            m_numberPolygons++;
            if (needsRepair) {
                m_numberRepairedPolygons++;
            }
            callOperation(client, htr, id++, needsRepair, insertFunction);
        }

        // cancel periodic stats printing
        timer.cancel();

        // block until all outstanding txns return
        client.drain();
        // print the summary results
        printResults(htr);
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link PolygonConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        PolygonConfig config = new PolygonConfig();
        config.parse(AsyncBenchmark.class.getName(), args);

        AsyncBenchmark benchmark = new AsyncBenchmark(config);
        for (HowToRun htr : HowToRun.values()) {
            benchmark.runBenchmark(htr, config.insertFunction);
        }
        benchmark.client.close();
    }
}
