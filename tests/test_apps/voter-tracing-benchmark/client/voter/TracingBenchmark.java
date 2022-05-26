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

package voter;

import java.io.IOException;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.voltdb.CLIConfig;
import org.voltdb.client.*;


/**
 * A case study example to measure the performance impact
 * of voltdb tracing system
 *
 * @author xiang Gao
 */
public class TracingBenchmark {
    // Initialize some common constants and variables
    private final static int TXNS = 10000;

    static final String CONTESTANT_NAMES_CSV =
            "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway," +
            "Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster," +
            "Kurt Walser,Ericka Dieter,Loraine Nygren,Tania Mattioli";

    static final String HORIZONTAL_RULE =
        "----------" + "----------" + "----------" + "----------" +
        "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final TracingConfig config;
    // Reference to the database connection we will use
    final Client client;
    // Statistics manager objects from the client
    final ClientStatsContext fullStatsContext;

    final Random rng;

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class TracingConfig extends CLIConfig {
        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Report latency for async benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";

        @Option(desc = "Number of contestants in the voting contest (from 1 to 10).")
        int contestants = 6;

        // 1 - show results on console with detailed latency report
        // 2 - show result  on console with main statisical parameters of performance listed in table
        // 3 - plot the statistical data in figures
        @Option(desc = "Choose the data output format, on console or in figures")
        int outputFormat = 1;

        @Option(desc = "Enable topology awareness")
        boolean topologyaware = false;

        @Option(desc = "Choose stored procedure for benchmark: true for VOTES.INSERT and false for DistinctCount")
        boolean doInsert = true;

        @Override
        public void validate() {
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
            if (contestants <= 0) exitWithMessageAndUsage("contestants must be > 0");
            if (outputFormat <= 0 || outputFormat > 3) exitWithMessageAndUsage("outputFormat must be 1, 2, or 3");
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public TracingBenchmark(TracingConfig config) {
        this.config = config;

        client = ClientFactory.createClient();
        fullStatsContext = client.createStatsContext();
        rng = new Random();
        /*
        if(config.latencyreport) {
            System.out.println("NOTICE: Option latencyreport is ON for tracing benchmark run, please set a reasonable ratelimit.\n");
        }
        */
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
        String[] serverArray = servers.split(",");
        if (config.topologyaware) {
            connectToOneServerWithRetry(serverArray[0]);
        } else {
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
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {
        // connect to one or more servers, until success
        connect(config.servers);

        // begin to run benchmark
        if (this.config.doInsert) {
            // reset the stats
            fullStatsContext.fetchAndResetBaseline();
            // run benchmark
            for (int i = 0; i < TracingBenchmark.TXNS; i++) {
                ClientResponse response =
                    client.callProcedure("VOTES.insert", rng.nextLong(), "MA", Integer.valueOf(i));

                if (response.getStatus() != ClientResponse.SUCCESS) {
                    throw new RuntimeException(response.getStatusString());
                }
            }
        } else {
            // initialize detabase
            client.callProcedure("Initialize", config.contestants, CONTESTANT_NAMES_CSV);
            // reset the stats after initialization
            fullStatsContext.fetchAndResetBaseline();
            // benchmark on select count
            for (int i = 0; i < TracingBenchmark.TXNS; i++) {
                ClientResponse response = client.callProcedure("DistinctCount");
                if (response.getStatus() != ClientResponse.SUCCESS) {
                    System.out.println("Response unsuccessful");
                    throw new RuntimeException(response.getStatusString());
                }
            }
        }

        // block until all outstanding txns return
        client.drain();
        // print the summary results
        showStatsResults();
        // close down the client connections
        client.close();
    }


    /**
     * Print the benchmark results for figure plot
     * @throws Exception
     */
    public synchronized void printForFigPlot(ClientStats stats) throws Exception {
        String format = "%d %.5f %.5f %.5f\n";
        System.out.printf(format,
            stats.getTxnThroughput(),
            stats.getAverageLatency(),
            stats.kPercentileLatencyAsDouble(0.99),
            stats.kPercentileLatencyAsDouble(1.0));
    }

    /**
     * Print the throughput, avgerage latency, 99.9% latency and max latency
     * for benchmarking.
     * @throws Exception if anything unexpected happens
     */
    public synchronized void showBenchmarkResults(ClientStats stats) throws Exception {
        String format = "%,9d                   %,9.5f                 %,9.5f                 %,9.5f\n";
        System.out.printf(format,
            stats.getTxnThroughput(),
            stats.getAverageLatency(),
            stats.kPercentileLatencyAsDouble(0.99),
            stats.kPercentileLatencyAsDouble(1.0));
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void showLatencyResults(ClientStats stats) throws Exception {
        System.out.printf("Average latency:               %,9.5f ms\n", stats.getAverageLatency());
        System.out.printf("10th percentile latency:       %,9.5f ms\n", stats.kPercentileLatencyAsDouble(.1));
        System.out.printf("25th percentile latency:       %,9.5f ms\n", stats.kPercentileLatencyAsDouble(.25));
        System.out.printf("50th percentile latency:       %,9.5f ms\n", stats.kPercentileLatencyAsDouble(.5));
        System.out.printf("75th percentile latency:       %,9.5f ms\n", stats.kPercentileLatencyAsDouble(.75));
        System.out.printf("90th percentile latency:       %,9.5f ms\n", stats.kPercentileLatencyAsDouble(.9));
        System.out.printf("95th percentile latency:       %,9.5f ms\n", stats.kPercentileLatencyAsDouble(.95));
        System.out.printf("99th percentile latency:       %,9.5f ms\n", stats.kPercentileLatencyAsDouble(.99));
        System.out.printf("99.5th percentile latency:     %,9.5f ms\n", stats.kPercentileLatencyAsDouble(.995));
        System.out.printf("99.9th percentile latency:     %,9.5f ms\n", stats.kPercentileLatencyAsDouble(.999));

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);
        System.out.printf("Reported Internal Avg Latency: %,9.5f ms\n", stats.getAverageInternalLatency());

        System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" Latency Histogram");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(stats.latencyHistoReport());
    }
    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void showStatsResults() throws Exception {
        // performance statistics
        ClientStats stats = fullStatsContext.fetch().getStats();

        if (this.config.outputFormat == 1) {
            System.out.print(HORIZONTAL_RULE);
            System.out.println(" Command Line Configuration");
            System.out.println(HORIZONTAL_RULE);
            System.out.println(config.getConfigDumpString());

            System.out.print(HORIZONTAL_RULE);
            System.out.println(" Client Workload Statistics");
            System.out.println(HORIZONTAL_RULE);
            System.out.printf("\nAverage throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
            if (this.config.latencyreport) {
                showLatencyResults(stats);
            }
        } else if (this.config.outputFormat == 2) {
            showBenchmarkResults(stats);
        } else if (this.config.outputFormat == 3) {
            printForFigPlot(stats);
        }
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link VoterConfig}
     */
    public static void main (String[] args) throws Exception {
        TracingConfig config = new TracingConfig();
        config.parse(TracingBenchmark.class.getName(), args);

        TracingBenchmark benchmark = new TracingBenchmark(config);
        try {
            benchmark.runBenchmark();
        } catch (Exception e) {}
    }
}
