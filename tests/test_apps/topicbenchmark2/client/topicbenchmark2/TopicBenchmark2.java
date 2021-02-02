/* This file is part of VoltDB.
 * Copyright (C) 2020-2021 VoltDB Inc.
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
package topicbenchmark2;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.topics.VoltDBKafkaPartitioner;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.RateLimiter;

/**
 * This client inserts a specified number of rows into a Volt stream as topic.
 * This client or other client instances can consume the expected number of rows from the stream topic.
 */
public class TopicBenchmark2 {

    static VoltLogger log = new VoltLogger("TOPICS");
    static final String TEST_TOPIC = "TEST_TOPIC";
    static final String GROUP_PREFIX = "Group";

    static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    // CSV pre-formatted row template: only produce the string field as value
    static final String ROW_FMT = "\"The quick brown fox jumps over the lazy dog; "
            + "Portez ce vieux whisky au juge blond, qui l'aime fort\"";

    // Validated CLI config
    final TopicBenchConfig m_config;

    Client m_client;
    ArrayList<String> m_brokers = new ArrayList<>();
    Set<String> m_createdGroups = ConcurrentHashMap.newKeySet();

    AtomicLong m_rowId = new AtomicLong(0);
    AtomicLong m_successfulInserts = new AtomicLong(0);
    AtomicLong m_failedInserts = new AtomicLong(0);

    enum Verification {
        NONE,
        RANDOM,
        ALL
    };

    enum Verifier {
        FAST,
        LARGE,
    }

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class TopicBenchConfig extends CLIConfig {
        public Verification m_verification = Verification.ALL;
        public Verifier m_verifier = Verifier.FAST;

        @Option(desc = "Topic (default " + TEST_TOPIC + ").")
        String topic = TEST_TOPIC;

        @Option(desc = "Comma separated list of servers to connect to (using default volt port).")
        String servers = "localhost";

        @Option(desc="Topic port on servers (default 9095)")
        int topicPort = 9095;

        @Option(desc = "How many produce invocations per producer, or how many to verify per subscriber group if no producers.")
        long count = 0;

        @Option(desc = "How many producers (default 1, may be 0 for subscribers only).")
        int producers = 1;

        @Option(desc = "How records per second to insert per producer (default 0 = no rate).")
        int insertrate = 0;

        @Option(desc = "Threshold in seconds to warn of long insertion times (default 90).")
        int insertwarnthreshold = 90;

        @Option(desc = "How many subscribers (default 0): creates groups with 1 subscriber per group. DEPRECATED, use groups/groupmembers")
        int subscribers = 0;

        @Option(desc = "How many subscriber groups (default 0).")
        int groups = 0;

        @Option(desc = "How many members per subscriber groups (default 0).")
        int groupmembers = 0;

        /*
         * The prefix should be changed for each repetition of a subscriber-only test that polls a pre-existing
         * topic. This ensures that polling will start from the beginning of the topic.
         */
        @Option(desc = "Subscriber group name prefix: use a different one for each subscriber test (default " + GROUP_PREFIX + ").")
        String groupprefix = GROUP_PREFIX;

        /*
         * If this value is too low the test client will complain about inability to commit offsets because not
         * being member of the group. This is an indication that the group was emptied because of this timeout.
         */
        @Option(desc = "Suscriber session timeout in seconds (default 30s).")
        int sessiontimeout = 30;

        /*
         * This value may need to be increased when a polling and verifying thread hits this silence timeout without
         * showing the session timeout errors. This may happen towards the end of a large scale polling test.
         */
        @Option(desc = "Max polling silence per thread (default 30s).")
        int maxpollsilence = 30;

        @Option(desc = "Log polling progress every X rows (default 0, no logging).")
        int pollprogress = 0;

        @Option(desc = "Verification (none, random, all) defines how many groups verify polling (default all)")
        String verification = "ALL";

        @Option(desc = "Verifier (fast, large) defines the type of verifier to use to verify polling (default fast)")
        String verifier = "FAST";

        @Override
        public void validate() {
            if (StringUtils.isBlank(topic)) exitWithMessageAndUsage("topic must not be empty or blank");
            if (StringUtils.isBlank(groupprefix)) exitWithMessageAndUsage("groupprefix must not be empty or blank");
            if (count < 0) exitWithMessageAndUsage("count must be >= 0");
            if (producers < 0) exitWithMessageAndUsage("producers must be >= 0");
            if (insertrate < 0) exitWithMessageAndUsage("insertrate must be >= 0");
            if (insertwarnthreshold <= 0) exitWithMessageAndUsage("insertwarnthreshold must be > 0");
            if (subscribers < 0) exitWithMessageAndUsage("subscribers must be >= 0");
            if (topicPort <= 0) exitWithMessageAndUsage("topicPort must be > 0");
            if (groups < 0) exitWithMessageAndUsage("groups must be >= 0");
            if (groups > 0 && groupmembers <= 0) exitWithMessageAndUsage("groupmembers must be > 0 when groups are defined");
            if (sessiontimeout <= 0) exitWithMessageAndUsage("sessiontimeout must be > 0");
            if (maxpollsilence <= 0) exitWithMessageAndUsage("maxpollsilence must be > 0");
            if (pollprogress < 0) exitWithMessageAndUsage("pollprogress must be >= 0");

            // Fold deprecated subscribers parameter into groups and groupmembers
            if (subscribers > 0) {
                if (groups > 0) exitWithMessageAndUsage("subscribers and groups cannot be > 0; using groups is preferred");
                groups = subscribers;
                groupmembers = 1;
            }

            try {
                m_verification = Verification.valueOf(verification.toUpperCase());
            }
            catch (Exception e) {
                exitWithMessageAndUsage(verification + " is an invalid verification value: " + e.getMessage());
            }
            try {
                m_verifier = Verifier.valueOf(verifier.toUpperCase());
            }
            catch (Exception e) {
                exitWithMessageAndUsage(verifier + " is an invalid verifier value: " + e.getMessage());
            }
        }
    }

    /**
     * Clean way of exiting from an exception
     * @param message   Message to accompany the exception
     * @param e         The exception thrown
     */
    void exitWithException(String message, Exception e) {
        log.error(message);
        log.info(e.getLocalizedMessage());
        System.exit(-1);
    }

    /**
     * Creates a new instance of the test to be run.
     * Establishes a client connection to a voltdb server, which should already be running
     * @param args The arguments passed to the program
     */
    TopicBenchmark2(TopicBenchConfig config) {
        this.m_config = config;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setTopologyChangeAware(true);
        clientConfig.setClientAffinity(true);
        m_client = ClientFactory.createClient(clientConfig);

        // Build boker string
        String[] serverArray = config.servers.split(",");
        for (String server : serverArray) {
            if (!StringUtils.isBlank(server)) {
                m_brokers.add(server + ":" + config.topicPort);
            }
        }
        log.info("Test using brokers: " + m_brokers);
    }


    VoltTable getTopicStats() throws IOException,InterruptedException{
        long retryStats = 5;
        VoltTable stats = null;
        while (retryStats-- > 0) {
            try {
                stats = m_client.callProcedure("@Statistics", "topic", 0).getResults()[0];
                break;
            } catch (ProcCallException e) {
                log.warn("Error while calling procedures: ");
                e.printStackTrace();
            }
            Thread.sleep(5000);
        }
        return stats;
    }

    /**
     * Wait for the topics tuple count to settle to the insertion count after insertions
     * <p>
     * Try to account for client resending records by waiting for stable count > expected count
     * for a certain amount of time (fixed 10s but could be made a parameter).
     *
     * @param insertCount
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    boolean waitTilTupleCountSettles(long insertCount) throws IOException, InterruptedException {
        /*
         * There might be a delay between the completion of the last insert transaction and when
         * stats register all tuples, that is TUPLE_COUNT = insert count
         *
         * Since at this point, all the stream/partitions are disabled, we have to be careful
         * to count each partition for each table once.
         */
        long st = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        // Max wait 10 mins only
        long end = st + (10 * 60 * 1000);

        long lastTotalCheck = 0;
        long lastTimeCheck = 0;

        while (true) {
            Map<String, Long> partitionMap = new HashMap<String, Long>();
            VoltTable stats = getTopicStats();
            long totalTupleCount = 0;

            while (stats.advanceRow()) {
                long partitionid = stats.getLong("PARTITION_ID");
                String source = stats.getString("TOPIC");
                if (!m_config.topic.equalsIgnoreCase(source)) {
                    continue;
                }
                Long tupleCount = stats.getLong("LAST_OFFSET");
                String tablePart = source + "_" + partitionid;
                if (! partitionMap.containsKey(tablePart)) {
                    // only put this table+partition count in the map once
                    partitionMap.put(tablePart, tupleCount);
                    totalTupleCount += tupleCount;
                }
            }
            long ctime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            if (ctime > end) {
                log.info("Waited too long...");
                return false;
            }
            else if (totalTupleCount >= insertCount) {
                long stableTimeMillis = ctime - lastTimeCheck;
                long settleTimeMillis = lastTimeCheck - st;
                // We want stable stats for 10s
                if (totalTupleCount == lastTotalCheck && stableTimeMillis > 10_000) {
                    log.info("LAST_OFFSET settled in " + TimeUnit.MILLISECONDS.toSeconds(settleTimeMillis)
                            + " seconds for " + totalTupleCount + " records");
                    return true;
                }
                else if (totalTupleCount > lastTotalCheck) {
                    lastTotalCheck = totalTupleCount;
                    lastTimeCheck = ctime;
                }
            }
            Thread.sleep(1000);
        }
    }

    /**
     * Connect to a single server.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServer(String server) {
        try {
            m_client.createConnection(server);
        }
        catch (IOException e) {
            log.info("Connection to " + server + " failed");
            return;
        }
        log.info("Connected to VoltDB node at: " + server);
    }

    /**
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    void connect(String servers) throws InterruptedException {
        String[] serverArray = servers.split(",");
        log.info("Connecting to VoltDB Server..." + serverArray[0]);
        connectToOneServer(serverArray[0]);
    }

    /**
     * Get a kafka consumer
     *
     * @param groupName
     * @return
     */
    KafkaConsumer<Long, String> getConsumer(String groupName) {
        String sessionTimeout = Long.toString(TimeUnit.SECONDS.toMillis(m_config.sessiontimeout));
        Map<String, Object> consumerConfig = ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, groupName,
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_brokers,
                // Use volt max buffer size of 2MB
                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "2097152",
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);

        if (m_createdGroups.add(groupName)) {
            log.info("Joining group " + groupName + ", config = " + consumerConfig);
        }
        return new KafkaConsumer<Long, String>(consumerConfig, new LongDeserializer(), new StringDeserializer());
    }

    /**
     * Reads values from topic, optionally verifying we are getting all the records
     * <p>
     * Verifying groups verify the polled record ids and report errors: this verification is expensive in memory resources.
     * Non-verifying groups just poll and don't report errors when polling nothing (but report other types of errors);
     * instead, the non-verifying groups will exit polling as soon as maxCount is reached regardless of duplicates.
     *
     * @param id                group identifier
     * @param servers           list of servers
     * @param topicPort         topic port
     * @param maxCount          the max count to poll
     * @param groupCount        the group count of polled records
     * @param groupVerifier     group verifier or {@code null}
     * @param duplicateCount    duplicate counter of {@code null}
     */
    void doReads(int id, String servers, int topicPort, long maxCount, AtomicInteger groupCount,
            BaseVerifier groupVerifier) {
        String groupId = m_config.groupprefix + "-" + id;
        long thId = Thread.currentThread().getId();

        long maxSilence = TimeUnit.SECONDS.toMillis(m_config.maxpollsilence);

        long lastPollTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        long lastGroupCount = 0;

        long polledCount = 0;
        long lastLogged = 0;
        try {
            try (KafkaConsumer<Long, String> consumer = getConsumer(groupId)) {
                consumer.subscribe(ImmutableList.of(m_config.topic));
                while (true) {
                    // Poll records
                    ConsumerRecords<Long, String> records = consumer.poll(Duration.ofSeconds(1));

                    if (groupVerifier != null && groupVerifier.cardinality() == maxCount) {
                        // Another verifying thread polled the last records
                        return;
                    }

                    long thisPollTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                    long elapsed = thisPollTime - lastPollTime;

                    // Decide if polling continues
                    if (m_failedInserts.get() != 0) {
                        return;
                    }
                    else if (records.count() > 0) {
                        lastPollTime = thisPollTime;
                        polledCount += records.count();
                        lastGroupCount = groupCount.addAndGet(records.count());

                        if (groupVerifier != null) {
                            records.forEach(record -> {
                                try {
                                    Long rowId = record.key();
                                    groupVerifier.addKey(rowId);
                                }
                                catch (Exception e) {
                                    exitWithException("Group " + groupId + " thread " + thId + " failed parsing CSV record", e);
                                }
                            });
                            long curSize = groupVerifier.cardinality();
                            if (curSize == maxCount) {
                                // We are the one who got the last row for the group (records,count()  > 0)
                                long startGaps = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                                boolean hasGaps = groupVerifier.hasGaps();
                                long elapsedGaps = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - startGaps;
                                log.info("Group " + groupId + " thread " + thId + " checked gaps in "
                                        + TimeUnit.MILLISECONDS.toSeconds(elapsedGaps) + " seconds");

                                if (hasGaps) {
                                    throw new RuntimeException("Group " + groupId + " thread " + thId
                                            + " verified, " + curSize + " records polled, "
                                            + groupVerifier.duplicates() + " duplicates, failed with gaps");
                                }
                                log.info("Group " + groupId + " thread " + thId + " verified, " + curSize + " records polled, "
                                        + groupVerifier.duplicates() + " duplicates");
                                // Got all the expected rows
                                return;
                            }
                            else if (m_config.pollprogress != 0 && (curSize - lastLogged) > m_config.pollprogress) {
                                lastLogged = curSize ;
                                log.info("Group " + groupId + " thread " + thId + ", verified = " + curSize
                                + ", dups = " + groupVerifier.duplicates());
                            }
                        }
                        else if (lastGroupCount >= maxCount) {
                            // Non-verifying group is done
                            log.info("Group " + groupId + " thread " + thId + ", polled = " + lastGroupCount);
                            return;
                        }
                    }
                    else {
                        // No records
                        int gc = groupCount.get();
                        if (groupVerifier == null && gc >= maxCount) {
                            // Non-verifying group is done
                            return;
                        }

                        // Check for unnatural silence, i.e. no poll nor any group progress
                        if (elapsed > maxSilence && lastGroupCount == gc) {
                            throw new RuntimeException("Group " + groupId + " thread " + thId + " polled no records for "
                                    + TimeUnit.MILLISECONDS.toSeconds(elapsed) + " seconds, thread polled = " + polledCount
                                    + ", group polled = " + gc);
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            exitWithException("Group " + groupId + " thread " + thId + " failed polling from topic\n", e);
        }
    }

    BaseVerifier getVerifier(long maxCount) {
        switch (m_config.m_verifier) {
        case FAST:
            return new FastVerifier(maxCount);
        case LARGE:
            return new LargeVerifier(maxCount);
        default:
            return null;
        }
    }

    /**
     * Inserts values into the topic: all values have identical sizes
     *
     * @throws InterruptedException
     * @throws NoConnectionsException
     */
    void doInserts(int id, String servers, int topicPort) {

        RateLimiter rateLimiter = m_config.insertrate > 0 ? RateLimiter.create(m_config.insertrate) : null;
        long inserts = 0;
        AtomicLong completed = new AtomicLong(0);
        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", m_brokers);
            props.put("acks", "all");
            props.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VoltDBKafkaPartitioner.class.getName());

            Producer<Long, String> producer = new KafkaProducer<>(props);

            // Insert records until we've reached the configured count
            while (inserts < m_config.count) {
                if (rateLimiter != null) {
                    rateLimiter.acquire();
                }
                final long start = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

                producer.send(new ProducerRecord<Long, String>(m_config.topic, m_rowId.getAndIncrement(), ROW_FMT),
                        new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        long completedNow = completed.incrementAndGet();
                        long end = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                        long elapsedSecs = TimeUnit.MILLISECONDS.toSeconds(end - start);
                        if(e != null) {
                            log.error("Thread " + id + " failed insert after " + elapsedSecs + " seconds: " + e.getMessage());
                            m_failedInserts.incrementAndGet();
                        } else {
                            m_successfulInserts.incrementAndGet();
                            if (elapsedSecs > m_config.insertwarnthreshold) {
                                log.warn("Thread " + id + " completed insert after " + elapsedSecs + " seconds");
                            }
                        }
                        if (completedNow == m_config.count) {
                            log.info("Thread " + id + " completed " + completedNow + " records");
                        }
                    }
                });
                ++inserts;
            }
            producer.close();
        }
        catch (Exception e) {
            exitWithException("Thread " + id + " failed inserting into topic\n", e);
        }
    }

    /**
     * Start subscriber threads
     * @return {@code ArrayList<Thread>} of subscribers, or {@code null} if no subscribers
     */
    ArrayList<Thread> startReaders() {
        int nReaders = m_config.groups * m_config.groupmembers;
        if (nReaders == 0) {
            return null;
        }

        // The max count to check depends if we are running a combined producer/subscriber test
        final long maxCount = m_config.producers != 0 ? m_config.count * m_config.producers : m_config.count;

        int verifier = -1;
        if (m_config.m_verification == Verification.RANDOM) {
            Random r = new Random();
            verifier = r.nextInt(m_config.groups);
            log.info("Creating " + nReaders + " subscriber threads in " + m_config.groups
                    + " groups to topic " + m_config.topic + ", group " + verifier + " verifying");
        }
        else if (m_config.m_verification == Verification.ALL){
            log.info("Creating " + nReaders + " subscriber threads in " + m_config.groups
                    + " groups to topic " + m_config.topic + ", all groups verifying");
        }
        else {
            log.info("Creating " + nReaders + " subscriber threads in " + m_config.groups
                    + " groups to topic " + m_config.topic + ", no groups verifying");
        }

        ArrayList<Thread> readers = new ArrayList<>();
        for (int i = 0; i < m_config.groups; i++) {
            boolean verifies = (m_config.m_verification == Verification.RANDOM && verifier == i) ||
                    m_config.m_verification == Verification.ALL;
            final int groupId = i;

            final BaseVerifier groupVerifier = verifies ? getVerifier(maxCount) : null;
            final AtomicInteger groupCount = new AtomicInteger(0);

            for (int j = 0; j <  m_config.groupmembers; j++) {
                readers.add(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        doReads(groupId, m_config.servers, m_config.topicPort, maxCount,
                                groupCount, groupVerifier);
                    }
                }));
            }
        }
        readers.forEach(t -> t.start());
        return readers;
    }

    /**
     * Start producer threads
     * @return {@code ArrayList<Thread>} of producers, or {@code null} if no producers
     */
    ArrayList<Thread> startWriters() {
        if (m_config.producers == 0) {
            return null;
        }

        log.info("Creating " + m_config.producers + " producer threads to topic " + m_config.topic);
        ArrayList<Thread> writers = new ArrayList<>();
        for (int i = 0; i < m_config.producers; i++) {
            final int id = i;
            writers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                      doInserts(id, m_config.servers, m_config.topicPort);
                  }
              }));
        }
        writers.forEach(t -> t.start());
        return writers;
    }

    /**
     * Runs the topic benchmark test
     * @throws InterruptedException
     * @throws IOException
     * @throws NoConnectionsException
     */
    void runTest() throws InterruptedException, IOException {
        try {
            log.info("Test initialization");
            connect(m_config.servers);
        } catch (InterruptedException e) {
            exitWithException("ERROR: Error connecting to VoltDB", e);
        }

        long start = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        ArrayList<Thread> readers = startReaders();
        ArrayList<Thread> writers = startWriters();

        if (writers != null) {
            writers.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    // ignore
                }
            });
            long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - start;

            log.info("Finished benchmark insertion in " + TimeUnit.MILLISECONDS.toSeconds(duration) + " seconds"
                    + ", last rowId = " + (m_rowId.get() - 1));
            if (m_failedInserts.get() == 0) {
                if (!waitTilTupleCountSettles(m_successfulInserts.get())) {
                    log.info("TUPLE_COUNT did not settle in 10 minutes");
                }
            }
        }
        m_client.close();

        if (readers != null && m_failedInserts.get() == 0) {
            readers.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    // ignore
                }
            });
        }

        long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime()) - start;
        log.info("Benchmark complete: " + m_successfulInserts.get() + " successful, " + m_failedInserts.get() + " failed records"
                + " in " + TimeUnit.MILLISECONDS.toSeconds(duration) + " seconds");

        if (m_failedInserts.get() != 0) {
            log.error("Test client failed");
            System.exit(-1);
        } else {
            log.info("Test client finished successfully");
            System.exit(0);
        }
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     */
    public static void main(String[] args) {
        TopicBenchConfig config = new TopicBenchConfig();
        config.parse(TopicBenchmark2.class.getName(), args);

        try {
            TopicBenchmark2 bench = new TopicBenchmark2(config);
            bench.runTest();
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }
    }
}
