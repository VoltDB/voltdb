/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
 * This client inserts a specified number of rows into a Volt stream as topic.
 *
 * Another client or clients consume rows from the stream topic.
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
import java.util.TreeMap;
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

/**
 * Asychronously sends data to topics to test VoltDB export performance.
 */
public class TopicBenchmark2 {

    static VoltLogger log = new VoltLogger("TOPICS");
    static final String TEST_TOPIC = "TEST_TOPIC";
    static final int MAX_SILENCE_MS = 10_000;

    // CSV pre-formatted row template
    static final String ROW_FMT = "\"%010d\",\"The quick brown fox jumps over the lazy dog; "
            + "Portez ce vieux whisky au juge blond, qui l'aime fort\"";

    // Validated CLI config
    final TopicBenchConfig config;

    Client client;

    AtomicLong rowId = new AtomicLong(0);
    AtomicLong successfulInserts = new AtomicLong(0);
    AtomicLong failedInserts = new AtomicLong(0);

    static final SimpleDateFormat LOG_DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class TopicBenchConfig extends CLIConfig {

        @Option(desc = "Topic (default " + TEST_TOPIC + ").")
        String topic = TEST_TOPIC;

        @Option(desc = "Comma separated list of servers to connect to (using default volt port).")
        String servers = "localhost";

        @Option(desc="Topic port on servers (default 9095)")
        int topicPort = 9095;

        @Option(desc = "How many produce invocations per producer.")
        int count = 0;

        @Option(desc = "How many producers (default 1, may be 0 for subscribers only).")
        int producers = 1;

        @Option(desc = "How many subscribers (default 0).")
        int subscribers = 0;

        @Override
        public void validate() {
            if (StringUtils.isBlank(topic)) exitWithMessageAndUsage("topic must not be empty or blank");
            if (count < 0) exitWithMessageAndUsage("count must be >= 0");
            if (producers < 0) exitWithMessageAndUsage("producers must be >= 0");
            if (subscribers < 0) exitWithMessageAndUsage("subscribers must be >= 0");
            if (topicPort <= 0) exitWithMessageAndUsage("topicPort must be > 0");
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
        this.config = config;
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setReconnectOnConnectionLoss(true);
        clientConfig.setTopologyChangeAware(true);
        clientConfig.setClientAffinity(true);
        client = ClientFactory.createClient(clientConfig);
    }


    VoltTable getTopicStats() throws IOException,InterruptedException{
        long retryStats = 5;
        VoltTable stats = null;
        while (retryStats-- > 0) {
            try {
                stats = client.callProcedure("@Statistics", "topic", 0).getResults()[0];
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
        long st = System.currentTimeMillis();
        //Wait 10 mins only
        long end = st + (10 * 60 * 1000);
        while (true) {
            Map<String, Long> partitionMap = new HashMap<String, Long>();
            VoltTable stats = getTopicStats();
            long totalTupleCount = 0;
            while (stats.advanceRow()) {
                long partitionid = stats.getLong("PARTITION_ID");
                String source = stats.getString("TOPIC");
                if (!config.topic.equalsIgnoreCase(source)) {
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
            if (totalTupleCount == insertCount) {
                long settleTimeMillis = System.currentTimeMillis() - st;
                log.info("LAST_OFFSET settled in " + settleTimeMillis/1000.0 + " seconds");
                return true;
            }
            long ctime = System.currentTimeMillis();
            if (ctime > end) {
                log.info("Waited too long...");
                return false;
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
            client.createConnection(server);
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
     * @param broker
     * @return
     */
    KafkaConsumer<String, String> getConsumer(String groupName, String broker) {
        return new KafkaConsumer<>(
                ImmutableMap.of(ConsumerConfig.GROUP_ID_CONFIG, groupName,
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker,
                        ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "6000"),
                new StringDeserializer(), new StringDeserializer());
    }

    /**
     * Reads values from topic, optionally verifying we are getting all the records
     * <p>
     * Each consumer belongs to a different group; we stop reading at the first insertion error.
     */
    void doReads(int id, String servers, int topicPort, boolean verify) {

        String[] serverArray = servers.split(",");
        String kafkaBroker = serverArray[0] + ":" + topicPort;

        // Those must be effectively final
        TreeMap<String, Integer> rows = new TreeMap<>();
        long lastPoll = System.currentTimeMillis();

        try {
            // FIXME: we could format a list of hosts
            try (KafkaConsumer<String, String> consumer = getConsumer("Group-" + id, kafkaBroker)) {
                consumer.subscribe(ImmutableList.of(config.topic));
                int polledCount = 0;
                int maxCount = config.producers != 0 ? config.count * config.producers : config.count;

                boolean doPoll = true;
                while (doPoll) {
                    // Poll records
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                    // Need an effective final for logging
                    int thisPoll = polledCount += records.count();

                    // Check for unnatural silence
                    if (records.count() == 0 && (System.currentTimeMillis() - lastPoll) > MAX_SILENCE_MS) {
                        throw new RuntimeException("Subscriber Thread " + id + " polled no records for " + (MAX_SILENCE_MS/1000) + " seconds");
                    }

                    // Decide if polling continues
                    if (failedInserts.get() != 0) {
                        doPoll = false;
                    }
                    else if (verify) {
                        records.forEach(record -> {
                            try {
                                // Note - we don't CSV-parse but just store the quoted rowId value
                                Object[] params = record.value().split(",");
                                if (params == null || params.length == 0) {
                                    throw new RuntimeException("Subscriber Thread " + id + " parsed empty record, " + thisPoll + " records polled");
                                }

                                // FIXME: we may want to detect duplicates in the future
                                Integer count = rows.get((String) params[0]);
                                if (count == null) {
                                    rows.put((String) params[0], 1);
                                }
                                else {
                                    rows.put((String) params[0], count.intValue() + 1);
                                }
                            }
                            catch (Exception e) {
                                exitWithException("Subscriber Thread " + id + " failed parsing CSV record\n", e);
                            }
                        });
                        if (rows.size() == maxCount) {
                            // Got all the expected rows
                            doPoll = false;
                        }
                    }
                    else if (polledCount >= maxCount) {
                        doPoll = false;
                    }
                }
            }

            if (verify) {
                log.info("Subscriber Thread " + id + " verified, " + rows.size() + " records polled");
            }
        }
        catch (Exception e) {
            exitWithException("Subscriber Thread " + id + " failed polling from topic\n", e);
        }
    }

    /**
     * Inserts values into the topic: all values have identical sizes
     *
     * @throws InterruptedException
     * @throws NoConnectionsException
     */
    void doInserts(int id, String servers, int topicPort) {

        int inserts = 0;
        String[] serverArray = servers.split(",");
        String kafkaBroker = serverArray[0] + ":" + topicPort;
        try {
            Properties props = new Properties();
            // FIXME: we could format a list of hosts
            props.put("bootstrap.servers", kafkaBroker);
            props.put("acks", "all");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, VoltDBKafkaPartitioner.class.getName());

            Producer<String, String> producer = new KafkaProducer<>(props);

            // Insert records until we've reached the configured count
            while (inserts < config.count) {
                // Note - create records with no key for now
                producer.send(new ProducerRecord<String, String>(config.topic, String.format(ROW_FMT, rowId.incrementAndGet())),
                        new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(e != null) {
                            failedInserts.incrementAndGet();
                        } else {
                            successfulInserts.incrementAndGet();
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
        if (config.subscribers == 0) {
            return null;
        }

        Random r = new Random();
        int verifier = r.nextInt(config.subscribers);
        log.info("Creating " + config.subscribers + " subscriber threads to topic " + config.topic + ", thread " + verifier + " verifying");

        ArrayList<Thread> readers = new ArrayList<>();
        for (int i = 0; i < config.subscribers; i++) {
            final int id = i;
            readers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                      doReads(id, config.servers, config.topicPort, id == verifier);
                  }
              }));
        }
        readers.forEach(t -> t.start());
        return readers;
    }

    /**
     * Start producer threads
     * @return {@code ArrayList<Thread>} of producers, or {@code null} if no producers
     */
    ArrayList<Thread> startWriters() {
        if (config.producers == 0) {
            return null;
        }

        log.info("Creating " + config.producers + " producer threads to topic " + config.topic);
        ArrayList<Thread> writers = new ArrayList<>();
        for (int i = 0; i < config.producers; i++) {
            final int id = i;
            writers.add(new Thread(new Runnable() {
                @Override
                public void run() {
                      doInserts(id, config.servers, config.topicPort);
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
            connect(config.servers);
        } catch (InterruptedException e) {
            exitWithException("ERROR: Error connecting to VoltDB", e);
        }

        long start = System.currentTimeMillis();
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
            long duration = System.currentTimeMillis() - start;

            log.info("Finished benchmark insertion in " + (duration / 1000) + " seconds.");
            if (failedInserts.get() == 0) {
                if (!waitTilTupleCountSettles(successfulInserts.get())) {
                    log.info("TUPLE_COUNT did not settle in 10 minutes");
                }
            }
        }
        client.close();

        if (readers != null && failedInserts.get() == 0) {
            readers.forEach(t -> {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    // ignore
                }
            });
        }

        long duration = System.currentTimeMillis() - start;
        log.info("Benchmark complete: " + successfulInserts.get() + " successful, " + failedInserts.get() + " failed records"
                + " in " + (duration / 1000) + " seconds");

        if (failedInserts.get() != 0) {
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
