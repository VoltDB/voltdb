/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package client;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.Level;
import org.voltcore.utils.Pair;
import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.Client2;
import org.voltdb.client.Client2CallOptions;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import com.google_voltpatches.common.util.concurrent.RateLimiter;

public class PriorityClient {
    // Replace VoltLogger to log on console
    static class MyLogger {
        static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

        // Ad-hoc rate limiting
        private ConcurrentHashMap<Level, Long> m_lastLogs = new ConcurrentHashMap<>();

        void error(String msg) {
            log(Level.ERROR, msg);
        }

        void warn(String msg) {
            log(Level.WARN, msg);
        }

        void info(String msg) {
            log(Level.INFO, msg);
        }

        void rateLimitedLog(long suppressInterval, Level level, String format, Object... args) {
            long now = System.nanoTime();
            long last = m_lastLogs.getOrDefault(level, 0L);
            if (TimeUnit.NANOSECONDS.toSeconds(now - last) > suppressInterval) {
                m_lastLogs.put(level, now);
                log(level, String.format(format, args));
            }
        }

        private void log(Level level, String msg) {
            System.out.print(LOG_DF.format(new Date()));
            System.out.println(String.format(" %s: %s", level, msg));
        }
    }
    static final MyLogger log = new MyLogger();

    static class Client2Wrapper {
        Client2 client;
        AtomicBoolean backPressured = new AtomicBoolean(false);

        void backPress(boolean slowdown) {
            backPressured.compareAndSet(!slowdown, slowdown);
        }
    }

    // This value must match the number of procedures and tables in the schema
    static final int PRIORITY_COUNT = 8;
    static final int HIGH_PRIO = 1;
    static final int LOW_PRIO = HIGH_PRIO + PRIORITY_COUNT - 1;
    static final int REQUEST_LIMIT = 300_000;
    static final int REQUEST_LIMIT_FACTOR = REQUEST_LIMIT / 10;
    static final int NO_RATE = -1;

    static final String SP_PROC = "TestSpInsert";
    static final String MP_PROC = "TestMpUpdate";

    // Updated by callback or listener
    AtomicLong m_successfulInvocations = new AtomicLong(0);
    AtomicLong m_failedInvocations = new AtomicLong(0);
    AtomicLong m_delayedInvocations = new AtomicLong(0);
    AtomicLong m_skippedInvocations = new AtomicLong(0);

    // Detect all expected callbacks invoked
    AtomicLong m_totalInvocations = new AtomicLong(0);
    AtomicLong m_checkedInvocations = new AtomicLong(0);
    CountDownLatch m_latch = new CountDownLatch(1);

    // Single clients
    final PriorityClientConfig m_config;
    final Client m_client1;
    final Client2Wrapper m_client2;
    final ClientStatsContext m_fullStatsContext;

    // Per-thread clients
    // Ugly maps to keep thread-specific clients around until end of test
    final ConcurrentHashMap<Integer, Object> m_spThreadClients = new ConcurrentHashMap<>();
    final ConcurrentHashMap<Integer, Object> m_mpThreadClients = new ConcurrentHashMap<>();
    final ConcurrentHashMap<Integer, ClientStatsContext> m_spThreadStatsContexts = new ConcurrentHashMap<>();
    final ConcurrentHashMap<Integer, ClientStatsContext> m_mpThreadStatsContexts = new ConcurrentHashMap<>();

    // Test timestamps
    volatile long startTs, warmupEndTS, checkpointTS, testEndTS;

    // INITIATOR stats for verification
    volatile VoltTable m_initiatorStats = null;

    // Checkpoint data
    final StringBuilder sbClientStats = new StringBuilder();
    final StringBuilder sbServerStats = new StringBuilder();

    final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    static class PriorityClientConfig extends CLIConfig {

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Warmup duration in seconds (default = 5s, if 0, no warmup)")
        int warmup = 5;

        // A checkpoint < duration allows collecting stats that ignore the tests's trailing transactions
        // by default we set it to 10s below the test duration
        @Option(desc = "Checkpoint duration after warmup, in seconds (default = 50s, if 0 checkpoint at test duration)")
        int checkpoint = 50;

        @Option(desc = "Test duration after warmup, in seconds (default = 60s)")
        int duration = 60;

        @Option(desc = "How many microseconds to delay the site threads (0 == no delay)")
        long delay = 0;

        @Option(desc = "Client version = 1 | 2 (default 2)")
        int clientversion = 2;

        @Option(desc = "If true use priorities in procedure invocations (default true).")
        boolean prioritize = true;

        @Option(desc = "If true use a single client for procedure invocations (default true). If false, use a client per priority level")
        boolean singleclient = true;

        @Option(desc = "If true use SP procedure invocations (default true)")
        boolean usesps = true;

        @Option(desc = "If true use MP procedure invocations (default true)")
        boolean usemps = true;

        @Option(desc = "If true use async procedure invocations (default true)")
        boolean async = true;

        @Option(desc = "If true, verify INITIATOR execution times (default true)")
        boolean verify = true;

        @Option(desc = "If verification enabled, percentage variation to use in verificatiopn (default 10)")
        int variation = 10;

        @Option(desc = "If true print stats at the end (default false)")
        boolean printstats = false;

        // By default we test priorities 2 and 3, skipping priorities 0 (system, unusable) and 1
        @Option(desc = "List of per-priority SP invocation rates/second (default [-1,1,3000], -1 dont test, 0 no rate, invoke as fast as possible).")
        String sprates = "-1,1,3000";

        // By default we test priorities 2 and 3, skipping priorities 0 (system, unusable) and 1
        @Option(desc = "List of per-priority MP invocation rates/second (default [-1,1,1000], -1 dont test, 0 no rate, invoke as fast as possible).")
        String mprates = "-1,1,1000";

        @Option(desc = "File with SSL properties")
        String sslfile = "";

        @Option(desc = "username")
        String username = "";

        @Option(desc = "password")
        String password = "";

        @Override
        public void validate() {
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (checkpoint < 0) exitWithMessageAndUsage("checkpoint must be >= 0");
            if (checkpoint > duration) exitWithMessageAndUsage("checkpoint must be <= duration");
            if (clientversion < 1 || clientversion > 2) exitWithMessageAndUsage("clientversion must be 1 or 2");
            if (clientversion == 1 && prioritize && singleclient)
                exitWithMessageAndUsage("Client version 1 and prioritize requires 1 client per priority (singleclient = false)");
            if (variation <= 0) exitWithMessageAndUsage("variation must be > 0");
        }
    }

    // Check if all expected callbacks were invoked and signal end of test
    void checkDone() {
        long checkThis = m_checkedInvocations.get();
        if (checkThis == 0) {
            // too early
            return;
        }

        long checkIt = m_successfulInvocations.get() + m_failedInvocations.get() + m_delayedInvocations.get();
        if (checkIt == checkThis) {
            // Done: signal end of test
            m_latch.countDown();
            m_checkedInvocations.set(0);
        }
    }

    void lateResponseHandler(ClientResponse resp, String host, int port) {
        log.rateLimitedLog(10, Level.INFO,
            "lateProcedureResponse, status= %d, client roundtrip= %d, cluster roundtrip= %d",
                resp.getStatus(), resp.getClientRoundtrip(), resp.getClusterRoundtrip());
        m_delayedInvocations.incrementAndGet();
        checkDone();
    }

    void handleConnectionDown(String host, int port) {
        log.info(String.format("Connection down: %s:%d", host, port));
    }

    void processResponse(ClientResponse response) {
        if (response.getStatus() == ClientResponse.SUCCESS) {
            m_successfulInvocations.incrementAndGet();
        } else {
            m_failedInvocations.incrementAndGet();
            log.rateLimitedLog(10, Level.WARN,
                "Failed call: status = "+ response.getStatus() + " %s", response.getStatusString());
        }
        checkDone();
    }

    // Listener for V1 client
    private class StatusListener extends ClientStatusListenerExt {

        @Override
        public void uncaughtException(ProcedureCallback callback, ClientResponse resp, Throwable e) {
            exitWithException("Uncaught exception in procedure callback ", new Exception(e));

        }

        @Override
        public void connectionCreated(String hostname, int port, AutoConnectionStatus status) {
            log.info(String.format("Connection to %s:%d created: %s",
                    hostname, port, status));
        }

        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            log.warn(String.format("Connection to %s:%d lost (%d connections left): %s",
                    hostname, port, connectionsLeft, cause));
        }
    }

    // Callback for V1 client
    ProcedureCallback m_callback = new ProcedureCallback() {

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            processResponse(clientResponse);
        }};

    Client getClient1() {
        return getClient1(-1);
    }

    Client getClient1(int priority) {
        ClientConfig clientConfig = new ClientConfig(this.m_config.username, this.m_config.password, new StatusListener());
        clientConfig.setTopologyChangeAware(true);
        if (priority != -1) {
            clientConfig.setRequestPriority(priority);
        }
        if (this.m_config.sslfile.trim().length() > 0) {
            clientConfig.setTrustStoreConfigFromPropertyFile(this.m_config.sslfile);
            clientConfig.enableSSL();
        }
        return ClientFactory.createClient(clientConfig);
    }

    Client2Wrapper getClient2() {
        Client2Wrapper wrapper = new Client2Wrapper();
        Client2Config clientConfig = new Client2Config()
                .clientRequestLimit(REQUEST_LIMIT)
                .outstandingTransactionLimit(REQUEST_LIMIT_FACTOR)
                .clientRequestBackpressureLevel(REQUEST_LIMIT_FACTOR * 9, REQUEST_LIMIT_FACTOR * 6)
                .lateResponseHandler(this::lateResponseHandler)
                .connectionDownHandler(this::handleConnectionDown)
                .requestBackpressureHandler(wrapper::backPress)
                .username(this.m_config.username)
                .password(this.m_config.password);

        if (this.m_config.sslfile.trim().length() > 0) {
            clientConfig.trustStoreFromPropertyFile(this.m_config.sslfile);
            clientConfig.enableSSL();
        }

        wrapper.client = ClientFactory.createClient(clientConfig);
        return wrapper;
    }

    public PriorityClient(PriorityClientConfig config ) {
        this.m_config = config;
        if (m_config.clientversion == 2) {
            m_client1 = null;
            m_client2 = getClient2();
            m_fullStatsContext = m_client2.client.createStatsContext();
        }
        else {
            m_client1 = getClient1();
            m_fullStatsContext = m_client1.createStatsContext();
            m_client2 = null;
        }
    }

    private void runTest() throws Exception {
        // Connect to servers
        try {
            log.info(String.format("Test initialization, use SPs: %b, use MPs: %b, clientversion %d, prioritize: %b, singleclient: %b",
                    m_config.usesps, m_config.usemps, m_config.clientversion, m_config.prioritize, m_config.singleclient));
            connect(m_config.servers, m_client1, m_client2, true);
        }
        catch (InterruptedException e) {
            exitWithException("Failed connecting to VoltDB", e);
        }

        // Rate for priority HIGH_PRIO is at index 0, etc.
        ArrayList<Integer> spRates = getInvocationRates(m_config.sprates);
        ArrayList<Integer> mpRates = getInvocationRates(m_config.mprates);

        // Fill MP_TABLE and start invocation threads
        if (m_config.usemps) {
            fillMpTable();
        }

        ArrayList<Thread> invocationThreads = new ArrayList<>();
        Map<Integer, Integer> spRatesMap = new HashMap<>();
        Map<Integer, Integer> mpRatesMap = new HashMap<>();
        try {
            for (int i = HIGH_PRIO; i <= LOW_PRIO; i++) {
                final int priority = i;
                int spRate = spRates.get(priority - HIGH_PRIO);
                int mpRate = mpRates.get(priority - HIGH_PRIO);

                if (m_config.usesps && spRate != NO_RATE) {
                    // Add SP invocation thread for priority i
                    Pair<Client, Client2Wrapper> clients = getClientsForPriority(priority, false);

                    invocationThreads.add(new Thread(new Runnable() {
                        @Override
                        public void run() {
                            doInvocations(clients.getFirst(), clients.getSecond(), priority, spRate, false);
                        }}));
                    spRatesMap.put(priority, spRate);
                }

                if (m_config.usemps && mpRate != NO_RATE) {
                    // Add MP invocation thread for priority i
                    Pair<Client, Client2Wrapper> clients = getClientsForPriority(priority, false);

                    invocationThreads.add(new Thread(new Runnable() {
                        @Override
                        public void run() {
                            doInvocations(clients.getFirst(), clients.getSecond(), priority, mpRate, true);
                        }}));
                    mpRatesMap.put(priority, mpRate);
                }
            }

            // Add the checkpoint thread
            invocationThreads.add(new Thread(new Runnable() {
                @Override
                public void run() {
                    doCheckpoint();
                }}));

            // Compute timestamps
            startTs = System.nanoTime();
            warmupEndTS = startTs;
            if (m_config.warmup > 0) {
                warmupEndTS += TimeUnit.SECONDS.toNanos(m_config.warmup);
            }
            testEndTS = warmupEndTS + TimeUnit.SECONDS.toNanos(m_config.duration);
            checkpointTS = m_config.checkpoint == 0 ? testEndTS : warmupEndTS + TimeUnit.SECONDS.toNanos(m_config.checkpoint);

            log.info("Starting test ...");
            if (!spRatesMap.isEmpty()) {
                log.info("SP invocation rates per priority: " + spRatesMap);
            }
            if (!mpRatesMap.isEmpty()) {
                log.info("MP invocation rates per priority: " + mpRatesMap);
            }
            invocationThreads.forEach(t -> t.start());

            // Wait for all threads to finish, and all expected callbacks to be invoked
            invocationThreads.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    // ignore
                }
            });

            // Threads don't invoke any more so ok to check the callbacks
            m_checkedInvocations.set(m_totalInvocations.get());
            checkDone();

            if (m_latch.await(2 * m_config.duration, TimeUnit.SECONDS)) {
                log.info(String.format("Finished: inserted = %d, failed = %d, delayed = %d, skipped = %d",
                        m_successfulInvocations.get(), m_failedInvocations.get(),
                        m_delayedInvocations.get(), m_skippedInvocations.get()));
            }
            else {
                log.error(String.format("FAILED: Timed out waiting: expected = %d, inserted = %d, failed = %d, delayed = %d, skipped = %d",
                        m_checkedInvocations.get(), m_successfulInvocations.get(), m_failedInvocations.get(),
                        m_delayedInvocations.get(), m_skippedInvocations.get()));
            }

            if (m_config.printstats) {
                System.out.print(sbClientStats.toString());
                System.out.print(sbServerStats);
            }

            if (m_config.verify) {
                verify(m_initiatorStats);
            }
        }
        finally {
            closeClients();
        }
    }

    // Fill contents of MP_TABLE with a set of rows in order to have constant MP execution times
    private void fillMpTable() throws NoConnectionsException, IOException, ProcCallException {
        for (int i = 0; i < 1000; i++) {
            if (m_client1 != null) {
                m_client1.callProcedure("@AdHoc", String.format("insert into MP_TABLE(rowid, bigint) values (%d, %d);", i, i));
            }
            else {
                m_client2.client.callProcedureSync("@AdHoc", String.format("insert into MP_TABLE(rowid, bigint) values (%d, %d);", i, i));
            }
        }
    }

    // Rate for priority HIGH_PRIO is at index 0, etc.
    ArrayList<Integer> getInvocationRates(String configRates) {
        ArrayList<Integer> rates = new ArrayList<>(PRIORITY_COUNT);
        String[] split = configRates.split(",", -1);

        for (int i = 0; i < PRIORITY_COUNT; i++) {
            if (i >= split.length) {
                rates.add(i, NO_RATE);
                continue;
            }
            String rate = split[i].trim();
            if (rate.isEmpty()) {
                rates.add(i, NO_RATE);
                continue;
            }
            rates.add(i, Integer.parseInt(rate));
        }
        return rates;
    }

    private Pair<Client, Client2Wrapper> getClientsForPriority(int priority, boolean isMp) {
        // Use single client or per-thread client
        Client client1 = null;
        if (m_config.clientversion == 1) {
            if (m_config.singleclient)      client1 = m_client1;
            else if (m_config.prioritize)   client1 = getClient1(priority);
            else                            client1 = getClient1();
        }
        Client2Wrapper client2 = null;
        if (m_config.clientversion == 2) {
            if (m_config.singleclient)      client2 = m_client2;
            else                            client2 = getClient2();
        }

        // If using per-thread client do some more stuff
        if (!m_config.singleclient) {
            try {
                // Store reference to client and stats context
                Map<Integer, Object> clientMap = isMp ? m_mpThreadClients : m_spThreadClients;
                Map<Integer, ClientStatsContext> statsMap = isMp ? m_mpThreadStatsContexts : m_spThreadStatsContexts;
                if (client1 != null) {
                    clientMap.put(priority, client1);
                    statsMap.put(priority, client1.createStatsContext());
                }
                else {
                    clientMap.put(priority, client2);
                    statsMap.put(priority, client2.client.createStatsContext());
                }

                // Connect this client to server
                connect(m_config.servers, client1, client2, true);
            } catch (InterruptedException e) {
                exitWithException("Interrupted trying to connect client", e, false);
            }
        }
        return Pair.of(client1, client2);
    }

    private void closeClients() throws InterruptedException {
        if (m_client1 != null) m_client1.close();
        if (m_client2 != null) m_client2.client.close();
        closeClients(m_spThreadClients.values());
        closeClients(m_mpThreadClients.values());
    }

    private void closeClients(Collection<Object> clients) throws InterruptedException {
        for (Object o : clients) {
            if (m_config.clientversion == 2) {
                Client2Wrapper client2 = (Client2Wrapper) o;
                client2.client.close();
            }
            else {
                Client client1 = (Client) o;
                client1.close();
            }
        }
    }

    // Any exceptions will exit the process
    private void doInvocations(Client client1, Client2Wrapper client2, int priority, int insertrate, boolean isMp) {
        long thId = Thread.currentThread().getId();

        Client2CallOptions options = null;
        if (m_config.clientversion == 2) {
            options = new Client2CallOptions();
            if (m_config.prioritize) options.requestPriority(priority);
        }

        String procName = String.format("%s%02d", isMp ? MP_PROC : SP_PROC, priority);
        RateLimiter rateLimiter = insertrate > 0 ? RateLimiter.create(insertrate) : null;

        long rowid = 0;
        long increment = 2;
        long delay = m_config.delay;

        while (System.nanoTime() < testEndTS) {
            if (rateLimiter != null) {
                rateLimiter.acquire();
            }
            try {
                // MP invocation uses constant increment value, SP invocation uses unique row identifier
                long parameter = isMp ? increment : rowid++;
                if (m_config.async) {
                    if (m_config.clientversion == 2) {
                        if (client2.backPressured.get()) {
                            log.rateLimitedLog(10, Level.WARN,
                                    "%s invocation thread %d priority %d backpressured",
                                     isMp ? "MP" : "SP", thId, priority);
                            m_skippedInvocations.incrementAndGet();
                            continue;
                        }
                        client2.client.callProcedureAsync(options, procName, parameter, delay)
                        .thenAcceptAsync(resp -> processResponse(resp))
                        .exceptionally(th -> {
                            exitWithException(String.format("Failed async invocation: priority = %d, parameter = %d, isMp = %b", priority, parameter, isMp), th);
                            return null;
                        });
                    }
                    else {
                        client1.callProcedure(m_callback, procName, parameter, delay);
                    }
                }
                else {
                    try {
                        ClientResponse resp = null;
                        if (m_config.clientversion == 2) {
                            if (client2.backPressured.get()) {
                                m_skippedInvocations.incrementAndGet();
                                continue;
                            }
                            resp = client2.client.callProcedureSync(options, procName, parameter, delay);
                        }
                        else {
                            resp = client1.callProcedure(procName, parameter, delay);
                        }
                        processResponse(resp);
                    }
                    catch (Exception e) {
                        exitWithException(String.format("Failed sync invocation: priority = %d, parameter = %d, isMp = %b", priority, parameter, isMp), e);
                    }
                }
                m_totalInvocations.incrementAndGet();
            } catch (Exception e) {
                exitWithException(String.format("%s invocation thread %d failed", isMp ? "MP" : "SP", thId), e, true);
            }
        }
    }

    private void doCheckpoint() {
        long thId = Thread.currentThread().getId();
        try {
            while (System.nanoTime() < warmupEndTS) {
                Thread.sleep(500);
            }
            if (warmupEndTS > startTs) {
                log.info(String.format("Checkpoint finished warmup after %d seconds, reset stats baseline ...",
                        TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTs)));

                m_fullStatsContext.fetchAndResetBaseline();
                m_spThreadStatsContexts.values().forEach(v -> v.fetchAndResetBaseline());
                m_mpThreadStatsContexts.values().forEach(v -> v.fetchAndResetBaseline());
            }

            while (System.nanoTime() < checkpointTS) {
                Thread.sleep(500);
            }
            log.info(String.format("Checkpoint after %d seconds, take stats ...",
                    TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTs)));

            getClientStats(sbClientStats);
            getServerStats(sbServerStats);

        } catch (Exception e) {
            exitWithException(String.format("Checkpoint thread %d failed", thId), e, true);
        }
    }

    void connectToOneServer(String server, Client client1, Client2Wrapper client2, boolean logIt) {
        long thId = Thread.currentThread().getId();
        try {
            if (m_config.clientversion == 2) client2.client.connectSync(server);
            else client1.createConnection(server);
        }
        catch (IOException e) {
            log.info(String.format("Thread %d connection to %s failed: %s", thId, server, e));
            return;
        }
        if (logIt) {
            log.info(String.format("Thread %d connected to %s", thId, server));
        }
    }

    // FIXME: is this the right way to connect to the VoltDB servers
    // --> it's not necessary with topo-change-awareness.
    void connect(String servers, Client client1, Client2Wrapper client2, boolean logIt) throws InterruptedException {

        String[] serverArray = servers.split(",");
        for (int i = 0; i < serverArray.length; i++) {
            connectToOneServer(serverArray[i].trim(), client1, client2, logIt);
        }
    }

    void exitWithException(String message, Throwable ex) {
        exitWithException(message, ex, false);
    }

    void exitWithException(String message, Throwable e, boolean stackTrace) {
        log.error(message);
        log.info(e.getLocalizedMessage());
        if (stackTrace) {
            e.printStackTrace();
        }
        System.exit(1);
    }

    void exitWithMessage(String format, Object... args) {
        log.error(String.format(format, args));
        System.exit(1);
    }

    void getClientStats(StringBuilder sb) {
        if (m_config.singleclient) {
            getClientStats("Single client", m_fullStatsContext.fetch().getStats(), sb);
        }
        else {
            getThreadStats("SP", m_spThreadStatsContexts, sb);
            getThreadStats("MP", m_mpThreadStatsContexts, sb);
        }
    }

    void getClientStats(String what, ClientStats stats, StringBuilder sb) {
        sb
        .append("\n")
        .append(HORIZONTAL_RULE).append("Statistics for").append(what).append("\n")
        .append("\n")
        .append(String.format("Invocations completed:         %,9d txns/sec\n", stats.getInvocationsCompleted()))
        .append(String.format("Invocations in error:          %,9d txns/sec\n", stats.getInvocationErrors()))
        .append(String.format("Invocations aborted:           %,9d txns/sec\n", stats.getInvocationAborts()))
        .append(String.format("Invocations timed out:         %,9d txns/sec\n", stats.getInvocationTimeouts()))
        .append("\n")
        .append(String.format("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput()))
        .append(String.format("Average latency:               %,9.2f ms\n", stats.getAverageLatency()))
        .append(String.format("Average Internal Latency:      %,9.2f ms\n", stats.getAverageInternalLatency()))
        .append("\n")
        .append(String.format("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1)))
        .append(String.format("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25)))
        .append(String.format("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5)))
        .append(String.format("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75)))
        .append(String.format("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9)))
        .append(String.format("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95)))
        .append(String.format("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99)))
        .append(String.format("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995)))
        .append(String.format("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999)))
        .append("\n")
        .append(" Latency Histogram")
        .append("\n")
        .append(stats.latencyHistoReport())
        .append("\n");
    }

    void getThreadStats(String what, Map<Integer, ClientStatsContext> threadStatsContexts, StringBuilder sb) {
        if (threadStatsContexts.isEmpty()) {
            return;
        }

        TreeSet<Integer> sortedKeys = new TreeSet<>(threadStatsContexts.keySet());
        for (Integer k : sortedKeys) {
            String whatStats = String.format("%s thread stats priority %d", what, k);
            getClientStats(whatStats, threadStatsContexts.get(k).fetch().getStats(), sb);
        }
    }

    // get the server stats, store initiator stats for verification
    void getServerStats(StringBuilder sb) throws Exception {
        sb
        .append(HORIZONTAL_RULE)
        .append("Server statistics:\n\n");

        sb.append(" Latency Statistics:\n\n");
        VoltTable latencyStats = getServerStats("LATENCY", m_client1, m_client2);
        sb.append(latencyStats.toFormattedString()).append("\n\n");

        sb.append(" Queue Statistics:\n\n");
        VoltTable queueStats = getServerStats("QUEUEPRIORITY", m_client1, m_client2);
        sb.append(queueStats.toFormattedString()).append("\n\n");

        sb.append(" Initiator Statistics:\n\n");
        m_initiatorStats = getServerStats("INITIATOR", m_client1, m_client2);
        sb.append(m_initiatorStats.toFormattedString()).append("\n\n");

        sb.append(" Procedure Statistics:\n\n");
        VoltTable procStats = getServerStats("PROCEDURE", m_client1, m_client2);
        sb.append(procStats.toFormattedString()).append("\n\n");
    }

    VoltTable getServerStats(String what, Client client1, Client2Wrapper client2) throws IOException,InterruptedException{
        long retryStats = 5;
        VoltTable stats = null;
        while (retryStats-- > 0) {
            try {
                stats = m_config.clientversion == 2 ?
                        client2.client.callProcedureSync("@Statistics", what, 0).getResults()[0]
                        : client1.callProcedure("@Statistics", what, 0).getResults()[0];
                break;
            } catch (ProcCallException e) {
                log.warn("Error while calling stats for " + what);
                e.printStackTrace();
            }
            Thread.sleep(5000);
        }
        return stats;
    }

    void verify(VoltTable initiatorStats) throws IOException, InterruptedException {
        boolean isPriorityEnabled = getPriorityEnabled(m_client1, m_client2);
        boolean isTestPrioritized = isPriorityEnabled && m_config.prioritize;

        if (m_config.usesps) {
            verify(isTestPrioritized, SP_PROC, initiatorStats);
        }
        if (m_config.usemps) {
            verify(isTestPrioritized, MP_PROC, initiatorStats);
        }
    }

    void verify(boolean prioritized, String procNameTemplate, VoltTable initiatorStats) {

        // Build a map of average execution times.
        // The procedure names are ordered per priority e.g. TestSpInsert01, TestSpInsert02, etc..
        TreeMap<String, Long> perfMap = new TreeMap<>();
        initiatorStats.resetRowPosition();
        while (initiatorStats.advanceRow()) {
            String procName = (String) initiatorStats.get("PROCEDURE_NAME", VoltType.STRING);
            if (!procName.startsWith(procNameTemplate)) continue;

            long exeTime = (long) initiatorStats.get("AVG_EXECUTION_TIME", VoltType.BIGINT);
            perfMap.put(procName, exeTime);
        }

        // Now verify, using the configured % variation: if test not prioritized, execution times must be within variation,
        // if test prioritized, execution times must be above variation.

        log.info("Verifying: \n" + perfMap);
        for (Map.Entry<String, Long> e : perfMap.entrySet()) {
            long thisTime = e.getValue();
            Map.Entry<String, Long> next = perfMap.higherEntry(e.getKey());
            if (next == null) break;

            long thatTime = next.getValue();
            long variation = (Math.abs(thatTime - thisTime) * 100) / Math.min(thatTime, thisTime);

            if (prioritized && (thatTime <= thisTime || variation < m_config.variation)) {
                exitWithMessage("Execution times between %s and %s do not differ enough (%d)", e.getKey(), next.getKey(), variation);
            }
            if (!prioritized && variation > m_config.variation) {
                exitWithMessage("Execution times between %s and %s differ too much (%d)", e.getKey(), next.getKey(), variation);
            }
            log.info(String.format("Execution times between %s and %s differ ok (%d)", e.getKey(), next.getKey(), variation));
        }
    }

    boolean getPriorityEnabled(Client client1, Client2Wrapper client2) throws IOException,InterruptedException{
        long retry = 5;
        VoltTable sysinfo = null;
        while (retry-- > 0) {
            try {
                sysinfo = m_config.clientversion == 2 ?
                        client2.client.callProcedureSync("@SystemInformation", "DEPLOYMENT").getResults()[0]
                        : client1.callProcedure("@SystemInformation", "DEPLOYMENT").getResults()[0];
                break;
            } catch (ProcCallException e) {
                log.warn("Error while calling system info");
                e.printStackTrace();
            }
            Thread.sleep(5000);
        }

        boolean isPriorityEnabled = false;
        while (sysinfo.advanceRow()) {
            String name = (String) sysinfo.get(0, VoltType.STRING);
            if ("prioritiesenabled".equalsIgnoreCase(name)) {
                String value = (String) sysinfo.get(1, VoltType.STRING);
                isPriorityEnabled = Boolean.parseBoolean(value);
                break;
            }
        }
        return isPriorityEnabled;
    }

    public static void main(String[] args) {
        PriorityClientConfig config = new PriorityClientConfig();
        config.parse(PriorityClient.class.getName(), args);

        try {
            PriorityClient testClient = new PriorityClient(config);
            testClient.runTest();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
