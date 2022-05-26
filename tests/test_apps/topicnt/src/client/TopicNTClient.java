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
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.voltcore.logging.Level;
import org.voltdb.CLIConfig;
import org.voltdb.client.Client2;
import org.voltdb.client.Client2Config;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.topics.VoltDBKafkaPartitioner;

import com.google_voltpatches.common.util.concurrent.RateLimiter;

public class TopicNTClient {

    static final MyLogger log = new MyLogger();

    final TopicNTClientConfig m_config;
    final Client2Wrapper m_client;
    final ArrayList<String> m_urls = new ArrayList<>();

    AtomicLong m_totalInvocations = new AtomicLong(0);
    AtomicLong m_checkedInvocations = new AtomicLong(0);
    CountDownLatch m_latch = new CountDownLatch(1);

    AtomicLong m_successfulInvocations = new AtomicLong(0);
    AtomicLong m_failedInvocations = new AtomicLong(0);

    // Test timestamps
    volatile long startTs, testEndTS;

    // A record we produce, potentially from a retry list
    class Record {
        long cookieId;
        String url;

        public Record(long id, String str) {
            cookieId = id;
            url = str;
        }
    }

    static class TopicNTClientConfig extends CLIConfig {

        @Option(desc = "Comma separated list of VoltDB servers (<server[:port], ...>) to connect to: default = localhost")
        String servers = "localhost";

        @Option(desc = "Comma separated list of Kafka brokers (<broker:port, ...>) to connect to: default = localhost:9092")
        String brokers = "localhost:9092";

        @Option(desc = "If true, use Kafka producer (default true)")
        boolean produce = true;

        @Option(desc = "If true, trigger aborts and failures (default false)")
        boolean fail = false;

        @Option(desc = "If true, use Volt partitioner (default true)")
        boolean voltpartitioner = true;

        @Option(desc = "Test duration, in seconds (default 120s)")
        int duration = 120;

        @Option(desc = "If true, only produce a single record (used to test failures, default false)")
        boolean singletest = false;

        @Option(desc = "If true, initialize tables (default true)")
        boolean initialize = true;

        @Option(desc = "User count (default 100000)")
        int users = 100_000;

        @Option(desc = "Cookies count, per user (default 4)")
        int cookies = 4;

        @Option(desc = "Domains count (default 1000)")
        int domains = 1000;

        @Option(desc = "URL count (default 10000)")
        int urls = 10_000;

        @Option(desc = "Producer count (default 10)")
        int producers = 10;

        @Option(desc = "Producer rate, per second (default 1000, 0 = unlimited)")
        int rate = 1000;

        @Option(desc = "Log error suppression interval, in seconds (default 10).")
        int logsuppression = 10;

        @Option(desc = "Client requests hard limit (default 1 million).")
        int hardlimit = 1_000_000;

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (users <= 0) exitWithMessageAndUsage("users must be > 0");
            if (cookies <= 0) exitWithMessageAndUsage("cookies must be > 0");
            if (domains <= 0) exitWithMessageAndUsage("domains must be > 0");
            if (urls <= 0) exitWithMessageAndUsage("urls must be > 0");
            if (producers <= 0) exitWithMessageAndUsage("producers must be > 0");
            if (rate < 0) exitWithMessageAndUsage("rate must be >= 0");
            if (logsuppression <= 0) exitWithMessageAndUsage("logsuppression must be > 0");
            if (hardlimit < 100) exitWithMessageAndUsage("hardlimit must be >= 100");

            if (singletest) {
                producers = 1;
            }
        }
    }

    public TopicNTClient(TopicNTClientConfig config) {
        this.m_config = config;
        m_client = getClient2();
    }

    void runTest() throws Exception {
        try {
            // Connect and initialize tables
            connect(m_client);
            if (m_config.initialize) {
                initialize();
            }
            else {
                cleanup();
            }

            // URLs always test-specific
            log.infoFmt("Creating %d URLs...", m_config.urls);
            Random r = new Random();
            for (long i = 0; i < m_config.urls; i++) {
                m_urls.add(String.format("https://%s/%s",
                        String.format("www.%09d.com", r.nextInt(m_config.domains)),
                        UUID.randomUUID().toString().replace('-', '/')));
            }
            log.infoFmt("Test using brokers: %s", m_config.brokers);

            // Create producer threads
            ArrayList<Thread> invocationThreads = new ArrayList<>();
            for (int i = 0; i< m_config.producers; i++) {
                invocationThreads.add(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        if (m_config.produce) {
                            produce();
                        }
                        else {
                            runClient(m_client);
                        }
                    }}));
            }

            // Compute timestamps
            startTs = System.nanoTime();
            testEndTS = startTs + TimeUnit.SECONDS.toNanos(m_config.duration);

            // Run producer threads
            log.info("Starting test ...");
            invocationThreads.forEach(t -> t.start());
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
                log.infoFmt("Finished: inserted = %d, failed = %d",
                        m_successfulInvocations.get(), m_failedInvocations.get());
            }
            else {
                log.errorFmt("FAILED: Timed out waiting: expected = %d, inserted = %d, failed = %d",
                        m_checkedInvocations.get(), m_successfulInvocations.get(), m_failedInvocations.get());
            }
        }
        catch (Exception e) {
            exitWithException("FAILED: ", e, true);
        }
        finally {
            m_client.client.close();
        }
    }

    void initialize() throws NoConnectionsException, IOException, ProcCallException, InterruptedException, ExecutionException {
        ClientResponse resp = m_client.client.callProcedureSync("@AdHoc", "delete from cookies; delete from accounts; delete from users; delete from user_hits;");
        if (resp.getStatus() != ClientResponse.SUCCESS) {
            exitWithMessage(String.format("Cleanup returned %d: %s", resp.getStatus(), resp.getStatusString()));
        }

        int batch_size = m_config.hardlimit / 100;
        log.infoFmt("Creating %d users, %d cookies per user, by batches of %d...", m_config.users, m_config.cookies, batch_size);

        long cookieid = 1;
        ArrayList<CompletableFuture<?>> batch = new ArrayList<>();

        String nextAction = "none";
        for (long i = 0; i < m_config.users; i++) {
            String userName = String.format("user%09d", i);
            String email = String.format("%s@foo.com", userName);

            batch.add(m_client.client.callProcedureAsync("USERS.insert", userName, email, nextAction)
                    .thenAcceptAsync(this::processInsertResponse));

            for (int j = 0; j < m_config.cookies; j++) {
                batch.add(m_client.client.callProcedureAsync("COOKIES.insert", cookieid++, userName)
                        .thenAcceptAsync(this::processInsertResponse));
            }

            if (batch.size() >= batch_size) {
                CompletableFuture.allOf(batch.toArray(new CompletableFuture<?>[0]))
                    .exceptionally(th -> { exitWithException("Failed creating user", th, false); return null; })
                    .get();
                batch.clear();
                log.infoFmt("%d users created...", i);
            }
        }
        CompletableFuture.allOf(batch.toArray(new CompletableFuture<?>[0]))
        .exceptionally(th -> { exitWithException("Failed creating user", th, false); return null; })
        .get();
        batch.clear();

        log.infoFmt("Creating accounts for %d domains...", m_config.domains);
        String lastUser = "nobody";
        for (long i = 0; i < m_config.domains; i++) {
            String domain = String.format("www.%09d.com", i);
            batch.add(m_client.client.callProcedureAsync("ACCOUNTS.insert", domain, i, 0, lastUser)
                    .thenAcceptAsync(this::processInsertResponse));

            if (batch.size() >= batch_size) {
                CompletableFuture.allOf(batch.toArray(new CompletableFuture<?>[0]))
                    .exceptionally(th -> { exitWithException("Failed creating account", th, false); return null; })
                    .get();
                batch.clear();
                log.infoFmt("%d accounts created...", i);
            }
        }
        CompletableFuture.allOf(batch.toArray(new CompletableFuture<?>[0]))
        .exceptionally(th -> { exitWithException("Failed creating account", th, false); return null; })
        .get();
        batch.clear();
    }

    private void processInsertResponse(ClientResponse resp) {
        long rowCount = 0;
        if (resp.getStatus() != ClientResponse.SUCCESS) {
            exitWithMessage("Insertion returned %d: %s", resp.getStatus(), resp.getStatusString());
        }
        else if (resp.getResults()[0].advanceRow()) {
            rowCount = resp.getResults()[0].getLong(0);
        }
        if (rowCount != 1) {
            exitWithMessage("Insertion updated %d rows", rowCount);
        }
    }

    // Keep users, cookies, accounts, clean up any user_hits still remaining
    void cleanup() throws IOException, ProcCallException {
        ClientResponse resp = m_client.client.callProcedureSync("@AdHoc", "delete from user_hits;");
        if (resp.getStatus() != ClientResponse.SUCCESS) {
            exitWithMessage(String.format("Cleanup returned %d: %s", resp.getStatus(), resp.getStatusString()));
        }
    }

    // Check if all expected callbacks were invoked and signal end of test
    void checkDone() {
        long checkThis = m_checkedInvocations.get();
        if (checkThis == 0) {
            // too early
            return;
        }

        long checkIt = m_successfulInvocations.get() + m_failedInvocations.get();
        if (checkIt == checkThis) {
            // Done: signal end of test
            m_latch.countDown();
            m_checkedInvocations.set(0);
        }
    }

    void produce() {
        long thId = Thread.currentThread().getId();

        RateLimiter rateLimiter = m_config.rate > 0 ? RateLimiter.create(m_config.rate) : null;
        RateLimiter errorLimiter = RateLimiter.create(1);

        AtomicBoolean inError = new AtomicBoolean(false);
        List<Record> retryList = Collections.synchronizedList(new ArrayList<Record>());

        Random r = new Random();
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", m_config.brokers);
            props.put("bootstrap.servers.voltdb", m_config.servers);
            props.put("acks", "all");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.LongSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
            if (m_config.voltpartitioner) {
                props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VoltDBKafkaPartitioner.class.getName());
            }
            KafkaProducer<Long, Object> producer = new KafkaProducer<>(props);

            while (System.nanoTime() < testEndTS) {
                // If we are in error, limit to 1 record/s
                if (inError.get()) {
                    errorLimiter.acquire();
                }
                else if (rateLimiter != null) {
                    rateLimiter.acquire();
                }

                // Get the record from the retry list, or generate a new one
                Record record = null;
                synchronized(retryList) {
                    if (!retryList.isEmpty()) {
                        log.infoFmt("Producer thread %d retrying record (inError %b)...", thId, inError.get());
                        record = retryList.remove(0);
                    }
                }
                if (record == null) {
                    record = new Record(
                            (Math.abs(r.nextLong()) % (m_config.users * m_config.cookies)) + 1,
                            m_urls.get(r.nextInt(m_urls.size())));
                }

                final Record selectedRecord = record;
                final String value = String.format("%s,%d",
                        selectedRecord.url,
                        m_config.fail ? 1 : 0);

                ProducerRecord<Long, Object> producedRecord = new ProducerRecord<Long, Object>(
                        "topicnt", selectedRecord.cookieId, value);

                producer.send(producedRecord,
                        new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(e != null) {
                            inError.compareAndSet(false, true);
                            retryList.add(selectedRecord);
                            m_failedInvocations.incrementAndGet();
                            log.errorFmt("Producer thread %d failed: %s, %s",
                                    thId, e.getClass().getName(), e.getMessage());
                            /*
                            log.rateLimitedLog(m_config.logsuppression, Level.ERROR,
                                    "Producer thread %d failed inserting: %s, %s",
                                    thId, e.getClass().getName(), e.getMessage());
                            */
                        }
                        else {
                            inError.compareAndSet(true, false);
                            m_successfulInvocations.incrementAndGet();
                            log.rateLimitedLog(30, Level.INFO, "Produced %d records, %d successful, %d failed after %d seconds...",
                                    m_totalInvocations.get(), m_successfulInvocations.get(), m_failedInvocations.get(),
                                    TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTs));
                        }
                        checkDone();
                    }
                });

                m_totalInvocations.incrementAndGet();
                if (m_config.singletest) {
                    break;
                }
            }
            producer.close();
        }
        catch (Exception e) {
            exitWithException("Producer thread " + thId + " failed inserting into topic", e, true);
        }

    }

    void runClient(Client2Wrapper client2) {
        long thId = Thread.currentThread().getId();
        RateLimiter rateLimiter = m_config.rate > 0 ? RateLimiter.create(m_config.rate) : null;
        Random r = new Random();

        try {
            while (System.nanoTime() < testEndTS) {
                // Note: checking backpressure only here, subsequent procedure calls won't check
                if (client2.backPressured.get()) {
                    log.rateLimitedLog(10, Level.WARN, "Client thread %d backpressured", thId);
                    continue;
                }

                if (rateLimiter != null) {
                    rateLimiter.acquire();
                }

                HitHandler h = new HitHandler(
                            client2.client,
                            log,
                            (Math.abs(r.nextLong()) % (m_config.users * m_config.cookies)) + 1,
                            m_urls.get(r.nextInt(m_urls.size())));

                h.run()
                        .exceptionally(th -> {
                            log.errorFmt("Client thread %d failed: %s", thId, th.getMessage());
                            m_failedInvocations.incrementAndGet();
                            checkDone();
                            return null;
                        })
                        .thenAccept(b -> {
                            if (b) m_successfulInvocations.incrementAndGet();
                            else m_failedInvocations.incrementAndGet();
                            checkDone();
                            log.rateLimitedLog(30, Level.INFO, "Clients sent %d records, %d successful, %d failed after %d seconds...",
                                    m_totalInvocations.get(), m_successfulInvocations.get(), m_failedInvocations.get(),
                                    TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTs));
                        });

                m_totalInvocations.incrementAndGet();
                if (m_config.singletest) {
                    break;
                }
            }
        }
        catch (Exception e) {
            exitWithException("Producer thread " + thId + " failed inserting into topic", e, true);
        }
    }

    Client2Wrapper getClient2() {
        Client2Wrapper wrapper = new Client2Wrapper();
        Client2Config clientConfig = new Client2Config()
                .lateResponseHandler(this::lateResponseHandler)
                .connectionDownHandler(this::handleConnectionDown)
                .requestBackpressureHandler(wrapper::backPress);
        wrapper.client = ClientFactory.createClient(clientConfig);
        wrapper.client.setRequestLimits(m_config.hardlimit,
                (m_config.hardlimit / 10) * 8,
                (m_config.hardlimit / 10) * 4);
        wrapper.client.setOutstandingTxnLimit(m_config.hardlimit);
        return wrapper;
    }

    static class Client2Wrapper {
        Client2 client;
        AtomicBoolean backPressured = new AtomicBoolean(false);

        void backPress(boolean slowdown) {
            backPressured.compareAndSet(!slowdown, slowdown);
        }
    }

    void lateResponseHandler(ClientResponse resp, String host, int port) {
        log.rateLimitedLog(10, Level.INFO,
            "lateProcedureResponse, status= %d, client roundtrip= %d, cluster roundtrip= %d",
                resp.getStatus(), resp.getClientRoundtrip(), resp.getClusterRoundtrip());
        checkDone();
    }

    void handleConnectionDown(String host, int port) {
        log.info(String.format("Connection down: %s:%d", host, port));
    }

    void connect(Client2Wrapper client2) throws InterruptedException, IOException {

        String[] serverArray = m_config.servers.split(",");
        for (int i = 0; i < serverArray.length; i++) {
            client2.client.connectSync(serverArray[i].trim());
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

    public static void main(String[] args) {
        TopicNTClientConfig config = new TopicNTClientConfig();
        config.parse(TopicNTClient.class.getName(), args);
        try {
          TopicNTClient testClient = new TopicNTClient(config);
          testClient.runTest();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
