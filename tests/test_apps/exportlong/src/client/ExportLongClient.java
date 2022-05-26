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
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

import com.google_voltpatches.common.util.concurrent.RateLimiter;

public class ExportLongClient {
    // Updated by callback or listener
    AtomicLong m_successfulInvocations = new AtomicLong(0);
    AtomicLong m_failedInvocations = new AtomicLong(0);
    AtomicLong m_delayedInvocations = new AtomicLong(0);

    // Detect that all expected callbacks were invoked
    AtomicLong m_totalInvocations = new AtomicLong(0);
    AtomicLong m_checkedInvocations = new AtomicLong(0);
    CountDownLatch m_latch = new CountDownLatch(1);

    final ExportLongClientConfig m_config;
    final Client m_client;
    final ClientStatsContext m_fullStatsContext;

    volatile long testStartTS;
    volatile long lastLogTS = 0;

    final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    final String STREAM_TEMPLATE =
            "CREATE STREAM SOURCE%03d PARTITION ON COLUMN id EXPORT TO TARGET TARGET%02d (\n"
            + "  id               BIGINT        NOT NULL\n"
            + ", type_tinyint     TINYINT       NOT NULL\n"
            + ", type_smallint    SMALLINT      NOT NULL\n"
            + ", type_integer     INTEGER       NOT NULL\n"
            + ", type_bigint      BIGINT        NOT NULL\n"
            + ", type_timestamp   TIMESTAMP     DEFAULT NOW NOT NULL\n"
            + ", type_float       FLOAT         NOT NULL\n"
            + ", type_decimal     DECIMAL       NOT NULL\n"
            + ", type_varchar1024 VARCHAR(1024) NOT NULL\n"
            + ");\n"
            + "";

    static class ExportLongClientConfig extends CLIConfig {

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Test duration, in seconds, 0 = infinite, default = 120s)")
        long duration = 120;

        @Option(desc = "Logging interval, in seconds, 0 = no logging, default = 60s)")
        long loginterval = 60;

        @Option(desc = "Number of source streams, default 10, up to 1000, will be named SOURCE000, SOURCE001, etc..")
        int sources = 10;

        @Option(desc = "Number of export targets, default 2, up to , will be named TARGET00, TARGET01, etc..")
        int targets = 2;

        @Option(desc = "If true, create the source streams, default true")
        boolean create = true;

        @Option(desc = "Number of invocations per second, per source, default = 1000")
        int rate = 1000;

        @Option(desc = "Number of row identifiers, per source, default = 1000")
        int idcount = 1000;

        @Override
        public void validate() {
            if (duration < 0) exitWithMessageAndUsage("duration must be >= 0");
            if (loginterval < 0) exitWithMessageAndUsage("loginterval must be >= 0");
            if (sources <= 0 || sources > 1000) exitWithMessageAndUsage("sources must be > 0 and <= 1000");
            if (targets <= 0 || targets > 100) exitWithMessageAndUsage("targets must be > 0 and <= 100");
            if (rate <= 0) exitWithMessageAndUsage("rate must be > 0");
            if (idcount <= 0) exitWithMessageAndUsage("idcount must be > 0");
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
        System.out.println(String.format("lateProcedureResponse, status= %d, client roundtrip= %d, cluster roundtrip= %d",
                resp.getStatus(), resp.getClientRoundtrip(), resp.getClusterRoundtrip()));
        m_delayedInvocations.incrementAndGet();
        checkDone();
    }

    void handleConnectionDown(String host, int port) {
        System.out.println(String.format("Connection down: %s:%d", host, port));
    }

    void processResponse(ClientResponse response) {
        if (response.getStatus() == ClientResponse.SUCCESS) {
            m_successfulInvocations.incrementAndGet();
        } else {
            m_failedInvocations.incrementAndGet();
            System.out.println(String.format("Failed call: status = %d, %s", response.getStatus(), response.getStatusString()));
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
            System.out.println(String.format("Connection to %s:%d created: %s",
                    hostname, port, status));
        }

        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            System.out.println(String.format("Connection to %s:%d lost (%d connections left): %s",
                    hostname, port, connectionsLeft, cause));
        }
    }

    // Callback for V1 client
    ProcedureCallback m_callback = new ProcedureCallback() {

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            processResponse(clientResponse);
        }};

    Client getClient() {
        ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
        clientConfig.setTopologyChangeAware(true);
        return ClientFactory.createClient(clientConfig);
    }

    public ExportLongClient(ExportLongClientConfig config ) {
        this.m_config = config;
        m_client = getClient();
        m_fullStatsContext = m_client.createStatsContext();
    }

    private void runTest() throws Exception {
        // Connect to servers
        try {
            System.out.println(String.format("Test initialization, duration: %d, sources: %d, targets: %d, create: %b",
                    m_config.duration, m_config.sources, m_config.targets, m_config.create));
            connect(m_config.servers, m_client, true);
        }
        catch (InterruptedException e) {
            exitWithException("Failed connecting to VoltDB", e);
        }

        if (m_config.create) {
            createStreams();
        }

        ArrayList<Thread> invocationThreads = new ArrayList<>();
        long durationNs = m_config.duration == 0 ? Long.MAX_VALUE : TimeUnit.SECONDS.toNanos(m_config.duration);
        try {
            for (int i = 0; i < m_config.sources; i++) {
                int sourceIdx = i;
                invocationThreads.add(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        doInvocations(durationNs, sourceIdx, m_config.rate, m_config.idcount);
                    }}));
            }

            System.out.println("Starting test ...");
            testStartTS = System.nanoTime();
            m_fullStatsContext.fetchAndResetBaseline();
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
                System.out.println(String.format("Finished: inserted = %d, failed = %d, delayed = %d",
                        m_successfulInvocations.get(), m_failedInvocations.get(), m_delayedInvocations.get() ));
            }
            else {
                System.out.println(String.format("FAILED: Timed out waiting: expected = %d, inserted = %d, failed = %d, delayed = %d",
                        m_checkedInvocations.get(), m_successfulInvocations.get(), m_failedInvocations.get(), m_delayedInvocations.get() ));
            }
        }
        finally {
            if (m_client != null) m_client.close();
        }

    }

    // Create source streams, round-robining through targets
    private void createStreams() throws NoConnectionsException, IOException, ProcCallException {
        for (int i = 0; i < m_config.sources; i++) {
            String ddl = String.format(STREAM_TEMPLATE, i, i % m_config.targets);
            m_client.callProcedure("@AdHoc", ddl);
        }
        System.out.println(String.format("Created %d streams exporting to %d targets", m_config.sources, m_config.targets));
    }

    // Any exceptions will exit the process
    private void doInvocations(long durationNs, int sourceIdx, int insertrate, int idcount) {
        long thId = Thread.currentThread().getId();

        String procName = String.format("SOURCE%03d.insert", sourceIdx);
        RateLimiter rateLimiter = insertrate > 0 ? RateLimiter.create(insertrate) : null;
        Random r = new Random();

        // To speed up invocations, each thread uses the same record contents
        SampleRecord record = new SampleRecord(0, r);

        long now = 0;
        do {
            if (rateLimiter != null) {
                rateLimiter.acquire();
            }
            now = System.nanoTime();
            try {
                // Call procedure
                m_client.callProcedure(m_callback, procName,
                        r.nextInt(idcount),
                        record.type_tinyint,
                        record.type_smallint,
                        record.type_integer,
                        record.type_bigint,
                        record.type_timestamp,
                        record.type_float,
                        record.type_decimal,
                        record.type_varchar1024);

                // Update total - thread at idx 0 does the logging
                m_totalInvocations.incrementAndGet();
                if (sourceIdx == 0 && m_config.loginterval > 0
                        && now - lastLogTS > TimeUnit.SECONDS.toNanos(m_config.loginterval)) {
                    ClientStats stats = m_fullStatsContext.fetchAndResetBaseline().getStats();
                    String durationStr = m_config.duration == 0 ? "running for infinite time" :
                        String.format("running for %d seconds", m_config.duration);

                    System.out.println(String.format(
                            "%d invocations after %d seconds, latency %,9.2f ms, internal latency %,9.2f ms, %s",
                            m_totalInvocations.get(), TimeUnit.NANOSECONDS.toSeconds(now - testStartTS),
                            stats.getAverageLatency(), stats.getAverageInternalLatency(), durationStr));
                    lastLogTS = now;
                }
            } catch (Exception e) {
                exitWithException(String.format("Thread %d invoking %s failed", thId, procName), e, true);
            }
        } while (now - testStartTS < durationNs);
    }

    void connectToOneServer(String server, Client client1, boolean logIt) {
        long thId = Thread.currentThread().getId();
        try {
            client1.createConnection(server);
        }
        catch (IOException e) {
            System.out.println(String.format("Thread %d connection to %s failed: %s", thId, server, e));
            return;
        }
        if (logIt) {
            System.out.println(String.format("Thread %d connected to %s", thId, server));
        }
    }

    void connect(String servers, Client client1, boolean logIt) throws InterruptedException {

        String[] serverArray = servers.split(",");
        for (int i = 0; i < serverArray.length; i++) {
            connectToOneServer(serverArray[i].trim(), client1, logIt);
        }
    }

    void exitWithException(String message, Throwable ex) {
        exitWithException(message, ex, false);
    }

    void exitWithException(String message, Throwable e, boolean stackTrace) {
        System.out.println(message);
        System.out.println(e.getLocalizedMessage());
        if (stackTrace) {
            e.printStackTrace();
        }
        System.exit(1);
    }
    public static void main(String[] args) {
        ExportLongClientConfig config = new ExportLongClientConfig();
        config.parse(ExportLongClientConfig.class.getName(), args);

        try {
            ExportLongClient testClient = new ExportLongClient(config);
            testClient.runTest();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
