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

package callcenter;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;

import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;

/**
 * <p>Process begin and end call events from a call center. Pair/join
 * events in VoltDB to create a definitive record of completed calls.</p>
 *
 * <p>Use VoltDB's strong consistency and stored procedure logic to
 * compute a running standard deviation on call length by agent.
 * This is not a trivial thing to compute without strong consistency.
 * The provided HTML dashboard shows a top-N list of agents by standard
 * deviation. It can be found in the "web" folder.</p>
 *
 */
public class CallCenterApp {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class CallCenterConfig extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Interval for performance feedback, in seconds.")
        int warmup = 5;

        @Option(desc = "Duration, in seconds.")
        int duration = 120;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";

        @Option(desc = "Stats file.")
        String statsfile = "";

        @Option(desc = "Number of call center agents.")
        long agents = 3000;

        @Option(desc = "Total count of phone numbers.")
        long numbers = 1500000;

        @Option(desc = "Mean call duration target in seconds.")
        long meancalldurationseconds = 5;

        @Option(desc = "Maximum call duration in seconds.")
        long maxcalldurationseconds = 60;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (agents <= 0) exitWithMessageAndUsage("agents must be > 0");
            if (numbers < agents) exitWithMessageAndUsage("numbers must be >= agents");
            if (meancalldurationseconds <= 0) exitWithMessageAndUsage("meancalldurationseconds must be > 0");
            if (maxcalldurationseconds <= meancalldurationseconds) {
                exitWithMessageAndUsage("maxcalldurationseconds must be > meancalldurationseconds");
            }
        }
    }

    // Reference to the database connection we will use
    final Client client;

    // validated command line configuration
    final CallCenterConfig config;

    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS = System.currentTimeMillis();

    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    // generator for fake call data
    final CallSimulator callSimulator;
    // perturbation filter for fake call data
    final NetworkSadnessTransformer<CallEvent> networkTransformer;

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            // if the benchmark is still active, note if a server disconnects
            final long benchmarkEndTime = benchmarkStartTS + (1000l * config.duration);
            if (System.currentTimeMillis() < benchmarkEndTime) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public CallCenterApp(CallCenterConfig config) {
        this.config = config;

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);

        System.out.println(config.getConfigDumpString());

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        ClientConfig clientConfig = new ClientConfig(config.user, config.password, new StatusListener());
        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        callSimulator = new CallSimulator(config);
        networkTransformer = new NetworkSadnessTransformer<>(callSimulator);
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
        System.out.printf("\n");
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     * @throws IOException
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws IOException {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());

        // 4. Write stats to file if requested
        client.writeSummaryCSV(stats, config.statsfile);
    }

    /**
     * Send a call event to either BeginCall or EndCall based on the event.
     *
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    void sendEvent(CallEvent call) throws NoConnectionsException, IOException, ProcCallException {
        if (call.endTS == null) {
            assert(call.startTS != null);
            // null callback isn't ideal for production code, but errors are tracked
            // here through client stats so we'll let it slide
            client.callProcedure(new NullCallback(), "BeginCall",
                    call.agentId, call.phoneNoStr(), call.callId, call.startTS);
        }
        else {
            assert(call.startTS == null);
            client.callProcedure(new NullCallback(), "EndCall",
                    call.agentId, call.phoneNoStr(), call.callId, call.endTS);
        }
    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    public void run() throws InterruptedException, NoConnectionsException, IOException, ProcCallException {
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);

        // connect to one or more servers, loop until success
        connect(config.servers);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);

        // Run the benchmark loop for the requested warmup time
        // The throughput may be throttled depending on client configuration
        System.out.println("Warming up...");
        final long warmupEndTime = System.currentTimeMillis() + (1000l * config.warmup);

        long now = System.currentTimeMillis();
        while (warmupEndTime > now) {
            CallEvent call = networkTransformer.next(now);
            if (call == null) {
                try { Thread.sleep(1); } catch (InterruptedException e) {}
            }
            else {
                sendEvent(call);
            }
            now = System.currentTimeMillis();
        }

        // reset the stats after warmup
        fullStatsContext.fetchAndResetBaseline();
        periodicStatsContext.fetchAndResetBaseline();

        // print periodic statistics to the console
        benchmarkStartTS = System.currentTimeMillis();
        schedulePeriodicStats();

        // Run the benchmark loop for the requested duration
        // The throughput may be throttled depending on client configuration
        System.out.println("\nRunning benchmark...");
        final long benchmarkEndTime = System.currentTimeMillis() + (1000l * config.duration);

        now = System.currentTimeMillis();
        while (benchmarkEndTime > now) {
            CallEvent call = networkTransformer.next(now);
            if (call == null) {
                try { Thread.sleep(1); } catch (InterruptedException e) {}
            }
            else {
                sendEvent(call);
            }
            now = System.currentTimeMillis();
        }

        // cancel periodic stats printing
        timer.cancel();

        // drain any messages that are waiting to be sent immediately
        CallEvent call = null;
        while ((call = networkTransformer.drain()) != null) {
            sendEvent(call);
        }

        // block until all outstanding txns return
        client.drain();

        // print out some debugging stats
        callSimulator.printSummary();

        // print the summary results
        printResults();

        // close down the client connections
        client.close();
    }

    /**
     * Main routine creates an instance of this app and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link WindowingConfig}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        CallCenterConfig config = new CallCenterConfig();
        config.parse(CallCenterApp.class.getName(), args);

        CallCenterApp app = new CallCenterApp(config);
        app.run();
    }
}
