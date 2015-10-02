/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
 * PUT NICE COMMENT HERE
 */

package socketimporter;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.utils.Pair;
import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;

import com.google_voltpatches.common.net.HostAndPort;


public class AsyncBenchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // potential return codes (synced with Vote procedure)
    static final long VOTE_SUCCESSFUL = 0;
    static final long ERR_INVALID_CONTESTANT = 1;
    static final long ERR_VOTER_OVER_VOTE_LIMIT = 2;

    // queue structure to hold data as it's written, so we can check it all get's into the database
    static Queue<Pair<Long,Long>> queue = new LinkedBlockingQueue<Pair<Long,Long>>();
    static boolean importerDone = false;

    // validated command line configuration
    final Config config;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;

    static final Map<HostAndPort, OutputStream> haplist = new HashMap<HostAndPort, OutputStream>();
    static Client client;
    // Some thread safe counters for reporting
    AtomicLong linesRead = new AtomicLong(0);
    AtomicLong rowsAdded = new AtomicLong(0);
    static final AtomicLong rowsChecked = new AtomicLong(0);
    static final AtomicLong rowsMismatch = new AtomicLong(0);
    static final AtomicLong finalInsertCount = new AtomicLong(0);
    static final AtomicLong writers = new AtomicLong(0);

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class Config extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 2;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 30;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 2;

        @Option(desc = "Comma separated list of the form server[:port] to connect to for streaming data")
        String servers = "volt3e,volt3f,volt3g";

        @Option(desc = "Comma separated list of the form server[:port] to connect to for database queries")
        String dbservers = "volt3e,volt3f,volt3g";

        @Option(desc = "Report latency for async benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public AsyncBenchmark(Config config) {
        this.config = config;

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
        if(config.latencyreport) {
            System.out.println("NOTICE: Option latencyreport is ON for async run, please set a reasonable ratelimit.\n");
        }
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    static OutputStream connectToOneServerWithRetry(String server, int port) {
        int sleep = 1000;
        while (true) {
            try {
                Socket pushSocket = new Socket(server, port);
                OutputStream out = pushSocket.getOutputStream();
                System.out.printf("Connected to VoltDB node at: %s.\n", server);
                return out;
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
    static void connect(String servers) throws InterruptedException {
        System.out.println("Connecting to Socket Streaming Interface...");

        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    int port = 7001;
                    HostAndPort hap = HostAndPort.fromString(server);
                    if (hap.hasPort()) {
                        port = hap.getPort();
                    }
                    OutputStream writer = connectToOneServerWithRetry(hap.getHostText(), port);
                    haplist.put(hap, writer);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    /**
     * Connect to one or more VoltDB servers.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional). Assumes 21212 if not specified otherwise.
     * @throws InterruptedException if anything bad happens with the threads.
     */
    static void dbconnect(String dbservers) throws InterruptedException, Exception {
    	System.out.println("Connecting to VoltDB Interface...");

        String[] serverArray = dbservers.split(",");
        client = ClientFactory.createClient();
        for (String server : serverArray) {
        	System.out.println("..." + server);
        	client.createConnection(server);
        }
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
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark(HostAndPort hap) throws Exception {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);
        AtomicLong icnt = new AtomicLong(0);
        try {
            // Run the benchmark loop for the requested warmup time
            // The throughput may be throttled depending on client configuration
            System.out.println("Warming up...");
            final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);
            while (warmupEndTime > System.currentTimeMillis()) {
            	long t = System.currentTimeMillis();
            	long cnt = icnt.getAndIncrement();
            	Pair<Long,Long> p = new Pair<Long,Long>(cnt, t);
                queue.offer(p);
                String s = String.valueOf(cnt) + "," + t + "\n";

                writeFully(s, hap, warmupEndTime);
                //icnt++;
            }

            // print periodic statistics to the console
            benchmarkStartTS = System.currentTimeMillis();
            schedulePeriodicStats();


            // Run the benchmark loop for the requested duration
            // The throughput may be throttled depending on client configuration
            // Save the key/value pairs so they can be verified through the database
            System.out.println("\nRunning benchmark...");
            final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);
            while (benchmarkEndTime > System.currentTimeMillis()) {
            	long t = System.currentTimeMillis();
            	long cnt = icnt.getAndIncrement();
            	Pair<Long,Long> p = new Pair<Long,Long>(cnt, t);
                queue.offer(p);
                String s = cnt + "," + t + "\n";
                writeFully(s, hap, benchmarkEndTime);
                //icnt++;
            }
            haplist.get(hap).flush();
        } finally {
            // cancel periodic stats printing
            timer.cancel();
            finalInsertCount.addAndGet(icnt.get());
            // print the summary results
            printResults();
        }
    }

    private void writeFully(String data, HostAndPort hap, long endTime) {
        while (System.currentTimeMillis() < endTime) {
            try {
                OutputStream writer = haplist.get(hap);
                writer.write(data.getBytes());
                return;
            } catch (IOException ex) {
                OutputStream writer = connectToOneServerWithRetry(hap.getHostText(), hap.getPort());
                haplist.put(hap, writer);
            }
        }
    }

    public static class BenchmarkRunner extends Thread {
        private final AsyncBenchmark benchmark;
        private final CountDownLatch cdl;
        private final HostAndPort hap;
        public BenchmarkRunner(AsyncBenchmark bm, CountDownLatch c, HostAndPort iidx) {
            benchmark = bm;
            cdl = c;
            hap = iidx;
        }

        @Override
        public void run() {
            try {
                benchmark.runBenchmark(hap);
                writers.incrementAndGet();
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                cdl.countDown();
            }
        }
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link VoterConfig}
     */
    public static void main(String[] args) throws Exception {
    	final long WAIT_FOR_A_WHILE = 100 * 1000; // 5 minutes in milliseconds
        // create a configuration from the arguments
        Config config = new Config();
        config.parse(AsyncBenchmark.class.getName(), args);

        // connect to one or more servers, loop until success
        connect(config.servers);
        dbconnect(config.dbservers);

        CountDownLatch cdl = new CountDownLatch(haplist.size());
        for (HostAndPort hap : haplist.keySet()) {
            AsyncBenchmark benchmark = new AsyncBenchmark(config);
            BenchmarkRunner runner = new BenchmarkRunner(benchmark, cdl, hap);
            runner.start();
        }

        // TODO: start checking the table that's being populated by the socket injester(s)

        System.out.println("Starting CheckData methods. Queue size: " + queue.size());
        CheckData checkDB = new CheckData(queue, client);
        while (queue.size() == 0) {
        	try {
        		Thread.sleep(1000);                 //1000 milliseconds is one second.
        	} catch(InterruptedException ex) {
        		Thread.currentThread().interrupt();
        	}
        }
        System.out.println("Starting CheckData methods. Queue size: " + queue.size());
        checkDB.processQueue();
        cdl.await();

        System.out.println("...starting timed check looping... " + queue.size());
        // final long queueEndTime = System.currentTimeMillis() + ((config.duration > WAIT_FOR_A_WHILE) ? WAIT_FOR_A_WHILE : config.duration);
        final long queueEndTime = System.currentTimeMillis() + WAIT_FOR_A_WHILE;
        System.out.println("Continue checking for " + (queueEndTime-System.currentTimeMillis()) + " seconds.");

        while (queueEndTime > System.currentTimeMillis()) {
        	if ((queueEndTime - System.currentTimeMillis())/1000 % 15 == 0) {
         		System.out.println("...still looping... Queue length: " + queue.size());
        	}
        	checkDB.processQueue();
        }
        client.drain();

        System.out.println("Queued tuples remaining: " + queue.size());
        System.out.println("Total rows added by Socket Injester: " + finalInsertCount.get());
        System.out.println("Rows checked against database: " + rowsChecked.get());
        System.out.println("Mismatch rows (value added <> value in DB): " + rowsMismatch.get());
    }
}

