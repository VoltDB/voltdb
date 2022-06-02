/* This file is part of VoltDB.
 * Copyright (C) 2020-2022 Volt Active Data Inc.
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

import static org.voltdb.e3.topics.TopicsGateway.LOG;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.avro.SchemaBuilder.FieldBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.ClientStatusListenerExt.AutoConnectionStatus;
import org.voltdb.client.ClientStatusListenerExt.DisconnectCause;
import org.voltdb.client.topics.VoltDBKafkaPartitioner;
import org.voltdb.serdes.FieldDescription;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.util.concurrent.RateLimiter;

/**
 * This client inserts a specified number of rows into a Volt stream as topic.
 * This client or other client instances can consume the expected number of rows from the stream topic.
 */
public class TopicBenchmark2 {

    static VoltLogger log = new VoltLogger("TOPICS");
    static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    static final String TEST_TOPIC = "TEST_TOPIC";

    Random m_random = new Random();

    // This will be overwritten from the program arguments
    String m_varChar = new String();

    // Only set up if useavro
    GenericRecord m_avroRecord;

    // Validated CLI config
    final TopicBenchConfig m_config;

    Client m_client;
    ArrayList<String> m_brokers = new ArrayList<>();
    Set<String> m_createdGroups = ConcurrentHashMap.newKeySet();

    AtomicLong m_rowId = new AtomicLong(0);
    AtomicLong m_successfulInserts = new AtomicLong(0);

    // This variable to tell non-verifying groups to stop polling
    AtomicLong m_maxInserts = new AtomicLong(Long.MAX_VALUE);

    /*
         guard against permanent errors i.e. we define a number of consecutive errors after which we'd bail out of the producer
         m_failedInserts                    Accumulated failures over the entire test
         m_failedConsecutiveInserts  Increment at each failure, reset to 0 if successful insert occurs
     */
    AtomicLong m_failedInserts = new AtomicLong(0);
    AtomicLong m_failedConsecutiveInserts = new AtomicLong(0);

    enum Verification {
        NONE,
        RANDOM,
        ALL
    };

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class TopicBenchConfig extends CLIConfig {
        public Verification m_verification = Verification.ALL;

        @Option(desc = "Topic (default " + TEST_TOPIC + ").")
        String topic = TEST_TOPIC;

        @Option(desc = "Subscribe to groups (default true, if false, use topic/partition assignments")
        boolean subscribe = true;

        // usegroupids == true means use explicit group ids, otherwise create groupless consumers with explicit assignments.
        // Transient group members are not supported because the group does not commit its offsets and any member resuming
        // will read the same rows over and again. We coud refine this mode by maintaining the offsets in memory for this case.
        @Option(desc = "Use group ids (default true, if false, subscribe must be false, groups must be 1, and no transient members")
        boolean usegroupids = true;

        @Option(desc = "Comma separated list of servers to connect to (using default volt port).")
        String servers = "localhost";

        @Option(desc="Topic port on servers (default 9092)")
        int topicPort = 9092;

        @Option(desc="Size of varchar in record, in characters, min = 10, max = 32*1024, (default 512)")
        int varcharsize = 512;

        @Option(desc = "Use ASCII characters only (default = true, each char == 1 byte).")
        boolean asciionly = true;

        @Option(desc = "Use avro (requires schemaregistry, default false")
        boolean useavro = false;

        @Option(desc = "Use Kafka topics instead of Volt topic (default false")
        boolean usekafka = false;

        @Option(desc = "Schema registry URL (default http://localhost:8081). MUST match the value in deployment file.")
        String schemaregistry = "http://localhost:8081";

        @Option(desc = "How many produce invocations per producer, or how many to verify per subscriber group if no producers.")
        long count = 0;

        @Option(desc = "How many producers (default 1, may be 0 for subscribers only).")
        int producers = 1;

        @Option(desc = "How records per second to insert per producer (default 0 = no rate).")
        int insertrate = 0;

        @Option(desc = "Threshold in seconds to warn of long insertion times (default 90).")
        int insertwarnthreshold = 90;

        @Option(desc = "How many subscriber groups (default 0).")
        int groups = 0;

        @Option(desc = "How many members per subscriber groups (default 0).")
        int groupmembers = 0;

        @Option(desc = "Use static group members (default false).")
        boolean staticmembers = false;

        @Option(desc = "How many members in subscriber groups are transient (keep number low, default 0).")
        int transientmembers = 0;

        @Option(desc = "How many seconds max should a transient member be running (default 10).")
        int transientmaxduration = 10;

        /*
         * The prefix should be changed for each repetition of a subscriber-only test that polls a pre-existing
         * topic. This ensures that polling will start from the beginning of the topic.
         */
        @Option(desc = "Subscriber group name prefix: use a different one for each subscriber test (default random-generated).")
        String groupprefix = UUID.randomUUID().toString();

        /*
         * If this value is too low the test client will complain about inability to commit offsets because not
         * being member of the group. This is an indication that the group was emptied because of this timeout.
         */
        @Option(desc = "Suscriber session timeout in seconds (default 30s).")
        int sessiontimeout = 30;

        /*
         * This value may need to be increased when a polling and verifying thread hits this silence timeout without
         * showing the session timeout errors. This may happen towards the end of a large scale polling test.
         * The default value is set to > 30s which is the value of the typical timeout in rebalances.
         *
         * NOTE: this parameter may be increased as the varcharsize is greater, because of the additional time
         * spent handling larger record sizes. The default value is set to 120s which should be large enough
         * for the cases tested on an 8-core MacBook pro 16 with 32Gb RAM.
         */
        @Option(desc = "Max polling silence per thread (default 120s).")
        int maxpollsilence = 120;

        @Option(desc = "Log polling progress every X rows (default 0, no logging).")
        int pollprogress = 0;

        @Option(desc = "Verification (none, random, all) defines how many groups verify polling (default all)")
        String verification = "ALL";

        @Option(desc = "Maximum number of consecutive failed producer calls (inserts) before bailing out (default 1000)")
        long maxfailedinserts = Long.MAX_VALUE;

        @Option(desc = "Log error suppression interval, in seconds (default 10).")
        int logsuppression = 10;

        @Override
        public void validate() {
            if (StringUtils.isBlank(topic)) exitWithMessageAndUsage("topic must not be empty or blank");
            if (StringUtils.isBlank(groupprefix)) exitWithMessageAndUsage("groupprefix must not be empty or blank");
            if (count < 0) exitWithMessageAndUsage("count must be >= 0");
            if (producers < 0) exitWithMessageAndUsage("producers must be >= 0");
            if (insertrate < 0) exitWithMessageAndUsage("insertrate must be >= 0");
            if (insertwarnthreshold <= 0) exitWithMessageAndUsage("insertwarnthreshold must be > 0");
            if (topicPort <= 0) exitWithMessageAndUsage("topicPort must be > 0");
            if (groups < 0) exitWithMessageAndUsage("groups must be >= 0");
            if (groups > 0 && groupmembers <= 0) exitWithMessageAndUsage("groupmembers must be > 0 when groups are defined");
            if (sessiontimeout <= 0) exitWithMessageAndUsage("sessiontimeout must be > 0");
            if (maxpollsilence <= 0) exitWithMessageAndUsage("maxpollsilence must be > 0");
            if (pollprogress < 0) exitWithMessageAndUsage("pollprogress must be >= 0");
            if (transientmembers < 0) exitWithMessageAndUsage("transientmembers must be >= 0");
            if (groups > 0 && transientmembers >= groupmembers)
                exitWithMessageAndUsage("transientmembers must be < groupmembers, there must be at least 1 permanent member");
            if (transientmaxduration <= 0) exitWithMessageAndUsage("transientmaxduration must be > 0");
            if (transientmembers > 1 && maxpollsilence <= 30 )
                exitWithMessageAndUsage("use a maxpollsilence value > 30s when using transient members (minimum recommended 45s");
            if (varcharsize < 10 || varcharsize > 32768) exitWithMessageAndUsage("varcharsize must be > 10 and < 32768");
            if (!usegroupids && (subscribe || groups > 1 || transientmembers > 0))
                exitWithMessageAndUsage("when not using group ids, subscribe must be false, groups must be 1 and no transient members");
            if (logsuppression <= 0) exitWithMessageAndUsage("logsuppression must be > 0");

            try {
                m_verification = Verification.valueOf(verification.toUpperCase());
            }
            catch (Exception e) {
                exitWithMessageAndUsage(verification + " is an invalid verification value: " + e.getMessage());
            }
        }
    }

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

    /**
     * Clean way of exiting from an exception
     * @param message   Message to accompany the exception
     * @param e         The exception thrown
     */
    void exitWithException(String message, Exception e) {
        exitWithException(message, e, false);
    }

    void exitWithException(String message, Exception e, boolean stackTrace) {
        log.error(message);
        log.info(String.format("Exit with exception: %s", e));
        if (stackTrace) {
            e.printStackTrace();
        }
        System.exit(1);
    }

    /**
     * Creates a new instance of the test to be run.
     * Establishes a client connection to a voltdb server, which should already be running
     * @param args The arguments passed to the program
     */
    TopicBenchmark2(TopicBenchConfig config) {
        this.m_config = config;
        if (!m_config.usekafka) {
            ClientConfig clientConfig = new ClientConfig("", "", new StatusListener());
            clientConfig.setTopologyChangeAware(true);
            m_client = ClientFactory.createClient(clientConfig);
        }

        // Build broker string
        String[] serverArray = config.servers.split(",");
        for (String server : serverArray) {
            if (!StringUtils.isBlank(server)) {
                String[] split = server.split(":");
                m_brokers.add(split[0] + ":" + config.topicPort);
            }
        }
        log.info("Test using brokers: " + m_brokers);

        if (m_config.producers > 0) {
            // Generate a String to insert: all inserts use same value.
            if (m_config.asciionly) {
                m_varChar = RandomStringUtils.randomAlphanumeric(m_config.varcharsize);
                log.info("Test producers using ASCII String (size in bytes = " + m_varChar.getBytes().length + "):\n" + m_varChar);
            }
            else {
                m_varChar = RandomStringUtils.random(m_config.varcharsize, 0, 0, false, false, null, m_random);
                log.info("Test producers using UNICODE String (size in bytes = " + m_varChar.getBytes().length + "):\n" + m_varChar);
            }
        }

        if (m_config.useavro) {
            setupAvro(m_varChar);
        }
    }

    /**
     * Set up the one avro record used by all the producers
     * <p>
     * Note: the schema wil conflict with the schemas declared by Volt when loading the DDL,
     * because the Volt schemas are the full record whereas here we only register 1 String colum.
     * Therefore the schema registry must be put in compatibility NONE:
     *
     * curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
     *    --data '{"compatibility": "NONE"}' \
     *     http://localhost:8081/config
     *
     * @param value the record value
     */
    void setupAvro(String value) {
        FieldAssembler<Schema> schemaFields = SchemaBuilder.record("TOPIC_BENCH_PRODUCER").namespace("").fields();
        schemaFields.name("TYPE_VARCHAR32K").type().stringType().noDefault();
        Schema schema = schemaFields.endRecord();

        m_avroRecord = new GenericData.Record(schema);
        m_avroRecord.put("TYPE_VARCHAR32K", value);
        log.info("Test producers using avro record: " + m_avroRecord.toString());
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
     * @return {@code true} if connected
     */
    boolean connectToOneServer(String server) {
        try {
            m_client.createConnection(server);
        }
        catch (IOException e) {
            log.info("Connection to " + server + " failed: " + e);
            return false;
        }
        log.info("Connected to VoltDB node at: " + server);
        return true;
    }

    /**
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional). Exits if unable to connect to any host.
     *
     * @throws InterruptedException if anything bad happens with the threads.
     */
    void connect(String servers) throws InterruptedException {
        String[] serverArray = servers.split(",");
        ArrayList<String> voltServers = new ArrayList<>();
        for (String server : serverArray) {
            if (!StringUtils.isBlank(server)) {
                voltServers.add(server);
            }
        }

        boolean connected = false;
        for (String voltServer : voltServers) {
            log.info("Connecting to VoltDB node at: " + voltServer);
            connected |= connectToOneServer(voltServer);
        }
        if (!connected) {
            log.error("Failed to connect to any VoltDB host in " + voltServers);
            System.exit(1);
        }
    }

    /**
     * Get a kafka consumer
     *
     * @param groupName
     * @return
     */
    KafkaConsumer<Long, Object> getConsumer(String groupName) {
        String sessionTimeout = Long.toString(TimeUnit.SECONDS.toMillis(m_config.sessiontimeout));
        Properties props = new Properties();
        if (groupName != null) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupName);
            // Use a short rebalance timeout (default is 5 minutes)
            // Rebalance timeout only configurable via poll interval?
            props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000");
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, m_brokers);
        // Ensure polling from beginning
        // Use volt max buffer size of 2MB
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "2097152");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);

        if (m_config.staticmembers) {
            String memberName = groupName + Thread.currentThread().getId();
            props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, memberName);
        }

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.LongDeserializer.class);
        if (m_config.useavro) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
            props.put("schema.registry.url", m_config.schemaregistry);
        }
        else {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringDeserializer.class);
        }

        if (m_config.usekafka) {
            // This is not necessary with Volt which starts at the beginning
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }

        if (groupName != null && m_createdGroups.add(groupName)) {
            log.info("Joining group " + groupName + ", config = " + props);
        }
        return new KafkaConsumer<Long, Object>(props);
    }

    /**
     * Reads values from topic, optionally verifying we are getting all the records
     * <p>
     * Verifying groups verify the polled record ids and report errors: this verification is expensive in memory resources.
     * Non-verifying groups just poll and don't report errors when polling nothing (but report other types of errors);
     * instead, the non-verifying groups will exit polling as soon as maxCount is reached regardless of duplicates.
     *
     * @param id                group identifier
     * @param maxCount          the max count to poll
     * @param groupCount        the group count of polled records
     * @param groupVerifier     group verifier or {@code null} if no verification
     * @param maxDurationMs     max duration of this reader in milliseconds or {@code null} if unlimited
     * @param assigmnents       list of {@link TopicPartition} assigned to this group member
     *                          or {@code null} if using subscribe
     *
     * @return {@code true} if polling completed (or in error), {@code false} if polling should be resumed
     */
    boolean doReads(int id, long maxCount, AtomicLong groupCount, BaseVerifier groupVerifier, Long maxDurationMs,
            List<TopicPartition> assignments) {

        String groupId = m_config.groupprefix + "-" + id;
        long thId = Thread.currentThread().getId();

        if (maxDurationMs != null) {
            String staticMember = m_config.staticmembers ? " (static)" : "";
            log.info("Group " + groupId + " thread " + thId + staticMember
                    + " is transient for  " + maxDurationMs + " milliseconds");
        }

        long maxSilence = TimeUnit.SECONDS.toMillis(m_config.maxpollsilence);
        long startTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        long lastPollTime = startTime;

        long lastGroupCount = groupCount.get();
        long polledCount = 0;
        long lastLogged = 0;
        try {
            try (KafkaConsumer<Long, Object> consumer = getConsumer(m_config.usegroupids ? groupId : null)) {
                if (assignments == null) {
                    // Subscribe for automatic assignments
                    consumer.subscribe(ImmutableList.of(m_config.topic));
                }
                else {
                    // Assign topic/partitions
                    consumer.assign(assignments);
                }

                while (true) {
                    // Poll records
                    ConsumerRecords<Long, Object> records = consumer.poll(Duration.ofSeconds(1));

                    if (groupVerifier != null && groupVerifier.cardinality() == maxCount) {
                        // Another verifying thread polled the last records
                        return true;
                    }

                    long thisPollTime = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                    long elapsed = thisPollTime - lastPollTime;

                    // Decide if polling continues
                    if (records.count() > 0) {
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
                                return true;
                            }
                            else if (m_config.pollprogress != 0 && (curSize - lastLogged) > m_config.pollprogress) {
                                lastLogged = curSize ;
                                log.info("Group " + groupId + " thread " + thId + ", verified = " + curSize
                                + ", dups = " + groupVerifier.duplicates());
                            }
                        }
                        else if (lastGroupCount >= maxCount || lastGroupCount >= m_maxInserts.get()) {
                            // Non-verifying group is done - last thread that polled data reports
                            log.info("Non-verifying group " + groupId + " exiting, polled = " + lastGroupCount);
                            return true;
                        }
                    }
                    else {
                        // No records
                        long gc = groupCount.get();
                        if (groupVerifier == null && (gc >= maxCount || gc >= m_maxInserts.get())) {
                            // Non-verifying group is done
                            return true;
                        }

                        // Check for unnatural silence, i.e. no poll nor any group progress
                        if (elapsed > maxSilence && lastGroupCount == gc) {
                            throw new RuntimeException("Group " + groupId + " thread " + thId + " polled no records for "
                                    + TimeUnit.MILLISECONDS.toSeconds(elapsed) + " seconds, thread polled = " + polledCount
                                    + ", group polled = " + gc);
                        }
                        else if (gc > lastGroupCount) {
                            // Other members may have polled something since last time we did
                            lastGroupCount = gc;
                        }
                    }

                    // Transient member decides to bail here....
                    if (maxDurationMs != null) {
                        boolean bail = (thisPollTime - startTime) > maxDurationMs;
                        // Make sure static members can make progress
                        if (m_config.staticmembers && bail) bail = polledCount > 0;
                        if (bail) {
                            log.info("Group " + groupId + " thread " + thId + ", leaving group after  "
                                    + maxDurationMs + " milliseconds, and " + polledCount + " records polled");
                            return false;
                        }
                    }
                }
            }
        }
        catch (FencedInstanceIdException e) {
            // Static members can get fenced exception in the window where the server didn't
            // persist the latest membership updates before going down. Ignore this error as the
            // group should continue polling and the members should eventually reconnect.
            if (m_config.staticmembers) {
                log.warn(String.format(
                        "Group %s thread %d ignoring fenced exception for static member, retrying ...",
                        groupId, thId));
            }
            else {
                exitWithException("Group " + groupId + " thread " + thId + " fenced polling from topic", e, true);
            }
        }
        catch (Exception e) {
            exitWithException("Group " + groupId + " thread " + thId + " failed polling from topic", e, true);
        }
        return false;
    }

    /**
     * Inserts values into the topic: all values have identical sizes
     */
    @SuppressWarnings("unchecked")
    void doInserts() {
        long thId = Thread.currentThread().getId();
        RateLimiter rateLimiter = m_config.insertrate > 0 ? RateLimiter.create(m_config.insertrate) : null;

        AtomicLong completed = new AtomicLong(0);
        List<Long> failedRowIds = Collections.synchronizedList(new LinkedList<Long>());
        long rowIdCount = 0;

        try {
            Properties props = new Properties();
            props.put("bootstrap.servers", m_brokers);
            props.put("bootstrap.servers.voltdb", m_config.servers);
            props.put("acks", "all");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.LongSerializer.class);
            if (m_config.useavro) {
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        io.confluent.kafka.serializers.KafkaAvroSerializer.class);
                props.put("schema.registry.url", m_config.schemaregistry);
            }
            else {
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
            }
            if (!m_config.usekafka) {
                props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VoltDBKafkaPartitioner.class.getName());
            }
            KafkaProducer producer = new KafkaProducer(props);

            // Insert records until we've reached the configured count of SUCCESSFUL insertions
            while (true) {
                if (rateLimiter != null) {
                    rateLimiter.acquire();
                }
                final long start = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

                // Choose the next rowId to produce (may retry failed rowId), or bail, or wait for more completions
                Long nextRowId = failedRowIds.isEmpty() ? null : failedRowIds.remove(0);
                if (nextRowId == null) {
                    long completedNow = completed.get();
                    if (completedNow == m_config.count) {
                        log.info("Producer thread " + thId + " completed " + completedNow + " records");
                        break;
                    }
                    else if (rowIdCount == m_config.count) {
                        // Done, don't produce more than configured, wait for next completions
                        continue;
                    }
                    nextRowId = m_rowId.getAndIncrement();
                    rowIdCount++;
                }
                else {
                    m_failedInserts.decrementAndGet();
                }

                // Produce record with selected rowId (may re-send a record that previously failed)
                ProducerRecord<Long, Object> record = null;
                if (m_config.useavro) {
                    record = new ProducerRecord<Long, Object>(m_config.topic, nextRowId, m_avroRecord);
                }
                else {
                    record = new ProducerRecord<Long, Object>(m_config.topic, nextRowId, m_varChar);
                }
                final Long recordKey = nextRowId;
                producer.send(record,
                        new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        long end = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                        long elapsedSecs = TimeUnit.MILLISECONDS.toSeconds(end - start);
                        if(e != null) {
                            // Failed record, add rowId for retry
                            log.rateLimitedLog(m_config.logsuppression, Level.ERROR, null,
                                    String.format("Producer thread %d failed inserting: %s",
                                            thId, e.getMessage()));

                            failedRowIds.add(recordKey);
                            m_failedInserts.incrementAndGet();
                            m_failedConsecutiveInserts.incrementAndGet(); // accumlate failure and check if over the bail out limit
                            if (m_failedConsecutiveInserts.longValue() > m_config.maxfailedinserts) {
                                log.error("Producer thread exceeds consecutive failure limit: " + m_config.maxfailedinserts);
                                exitWithException("Producer thread " + thId + " failed inserting into topic\n", e, false);
                            }
                        }
                        else {
                            completed.incrementAndGet();
                            m_successfulInserts.incrementAndGet();
                            m_failedConsecutiveInserts.set(0); // had a successful insert, reset error counter
                            if (elapsedSecs > m_config.insertwarnthreshold) {
                                log.warn("Producer thread " + thId + " completed insert after " + elapsedSecs + " seconds");
                            }
                        }
                    }
                });
            }
            producer.close();
        }
        catch (Exception e) {
            exitWithException("Producer thread " + thId + " failed inserting into topic", e, true);
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
            verifier = m_random.nextInt(m_config.groups);
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

        List<TopicPartition> topicPartitions = getTopicPartitions();
        ArrayList<Thread> readers = new ArrayList<>();
        for (int i = 0; i < m_config.groups; i++) {

            boolean verifies = (m_config.m_verification == Verification.RANDOM && verifier == i) ||
                    m_config.m_verification == Verification.ALL;

            final int groupId = i;
            Map<Integer, Long> transients = getTransients();
            if (!transients.isEmpty()) {
                log.info("Subscriber group: " + groupId + " using transient members: " + transients);
            }

            if (topicPartitions != null) {
                // Shuffle manual assignments for each group
                Collections.shuffle(topicPartitions);
            }

            final BaseVerifier groupVerifier = verifies ? new LargeVerifier(maxCount) : null;
            final AtomicLong groupCount = new AtomicLong(0);

            for (int j = 0; j <  m_config.groupmembers; j++) {
                final Long transientDurationMs = transients.get(j);
                final List<TopicPartition> assignments = getTopicPartitionAssignments(topicPartitions, i, j, m_config.groupmembers);
                if (assignments != null && assignments.isEmpty()) {
                    // No partitions assigned to this member
                    continue;
                }

                readers.add(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        // Permanent group members will do one loop, transient group
                        // members may iterate
                        while (!doReads(groupId, maxCount, groupCount, groupVerifier,
                                transientDurationMs, assignments)) {

                            if (!isPollResumed(groupId, maxCount, groupCount)) {
                                break;
                            }
                        }
                    }
                }));
            }
        }
        readers.forEach(t -> t.start());
        return readers;
    }

    /**
     * @return the list of {@link TopicPartition} for the tested topic, or {@code null} if using subscribe
     */
    List<TopicPartition> getTopicPartitions() {
        if (m_config.subscribe) {
            return null;
        }
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        try {
            try (KafkaConsumer<Long, Object> consumer = getConsumer(null)) {
                List<PartitionInfo> partitionInfos = consumer.partitionsFor(m_config.topic);

                if (partitionInfos.isEmpty()) {
                    throw new RuntimeException("No partitions for topic " + m_config.topic);
                }
                partitionInfos.forEach(pi -> topicPartitions.add(new TopicPartition(pi.topic(), pi.partition())));
                log.info("Client assigning " + topicPartitions.size() + " partitions: " + topicPartitions);
            }
        }
        catch (Exception e) {
            exitWithException("Failed to read partitions from topic\n", e, true);
        }
        return topicPartitions;
    }

    /**
     * Get the list of topic/partitions assigned to this group member, or {@code null} if using subscribe
     *
     * @param topicPartitions   list of all {@link TopicPartition}
     * @param groupId           group identifier
     * @param memberId          member identifier (index)
     * @param memberCount       member count in group
     * @return                  list of assigned {@link TopicPartition} for this group member, or {@code null}
     */
    List<TopicPartition> getTopicPartitionAssignments(List<TopicPartition> topicPartitions,
            int groupId, int memberId, int memberCount) {
        if (topicPartitions == null) {
            return null;
        }

        List<TopicPartition> topicPartitionAssignments = new ArrayList<>();
        try {
            // Calculate the sublist, spreading remainder among members
            int span = topicPartitions.size() / memberCount;
            int remainder = topicPartitions.size() % memberCount;
            int fromIndex = 0;
            for (int i = 0; i < memberId; i++) {
                fromIndex += span;
                if (remainder > 0 && i < remainder) {
                    fromIndex += 1;
                }
            }
            int toIndex = fromIndex + span;
            if (remainder > 0 && memberId < remainder) {
                toIndex += 1;
            }

            if (fromIndex == toIndex) {
                log.info("Group " + groupId + " member " + memberId + " has no partitions assigned ");
            }
            else {
                topicPartitionAssignments = topicPartitions.subList(fromIndex,
                    Math.min(toIndex, topicPartitions.size()));
                log.info("Group " + groupId + " member " + memberId + " is assigned "
                        + topicPartitionAssignments.size() + " partitions: " + topicPartitionAssignments);
            }
        }
        catch (Exception e) {
            exitWithException("Failed to read partitions from topic\n", e, true);
        }
        return topicPartitionAssignments;
    }

    /**
     * @return a map of transient members and their durations
     */
    Map<Integer, Long> getTransients() {
        Map<Integer, Long> transients = new HashMap<>();
        if (m_config.transientmembers == 0) {
            return transients;
        }
        while(transients.keySet().size() < m_config.transientmembers) {
            int transientMember = m_random.nextInt(m_config.groupmembers);
            if (transients.keySet().contains(transientMember)) {
                // Same player shoots again
                continue;
            }
            int transientDuration = m_random.nextInt(m_config.transientmaxduration) + 1;
            transients.put(transientMember, TimeUnit.SECONDS.toMillis(transientDuration));
        }
        return transients;
    }

    /**
     * Wait for polling to resume on the group
     *
     * @param id
     * @param maxCount
     * @param groupCount
     * @return
     */
    boolean isPollResumed(int id, long maxCount, AtomicLong groupCount) {
        long thId = Thread.currentThread().getId();
        long lastGroupCount = groupCount.get();

        for (long thisGroupCount = groupCount.get();
                thisGroupCount == lastGroupCount; thisGroupCount = groupCount.get()) {

            if (thisGroupCount >= maxCount) {
                log.info("Group " + id + " thread " + thId + " will not resume polling.");
                return false;
            }

            log.info("Group " + id + " thread " + thId + " waiting for 1 seconds...");
            try { Thread.sleep(1000); } catch (Exception e) {}
        }
        log.info("Group " + id + " thread " + thId + " resumes polling");
        return true;
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
            writers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                      doInserts();
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
        log.info("Test initialization");
        if (m_client != null) {
            try {
                connect(m_config.servers);
            } catch (InterruptedException e) {
                exitWithException("ERROR: Error connecting to VoltDB", e);
            }
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

            if (m_client != null && m_failedInserts.get() == 0) {
                if (!waitTilTupleCountSettles(m_successfulInserts.get())) {
                    log.info("TUPLE_COUNT did not settle in 10 minutes");
                }
            }

            // Set the max inserts to limit non-verifier polling
            m_maxInserts.set(m_successfulInserts.get());
        }
        if (m_client != null) {
            m_client.close();
        }

        if (readers != null) {
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
            System.exit(1);
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
