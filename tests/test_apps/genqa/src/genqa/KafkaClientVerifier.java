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

package genqa;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.voltcore.logging.VoltLogger;
import org.voltdb.CLIConfig;
import org.voltdb.VoltDB;
import org.voltdb.CLIConfig.Option;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStats;
import org.voltdb.iv2.TxnEgo;

// import TopicBenchmark.TopicBenchConfig;

// import TopicBenchmark.TopicBenchConfig;

/**
 * This verifier connects to kafka and consumes messages from a topic, it then
 * looks for inconsistencies in the data.
 *
 * It requires a field with a sequence number and another field with an
 * unique index. It will compute gaps in the sequence and check for
 * duplicates based on the index.  It expects that the max sequence number should
 * match the expected number of unique records.
 *
 * Partition information will also be verified.  It expects that the voltdb partition in the
 * metadata fields matches the computed partition from org.voltdb.iv2.TxnEgo.
 *
 * it will also check that their are not too many duplicates.
 *
 */
public class KafkaClientVerifier {

    static VoltLogger log = new VoltLogger("KafkaClientVerifier");

    public static long VALIDATION_REPORT_INTERVAL = 1000;
    private final int consumerTimeoutSecs;
    private String groupId;
    private List<KafkaConsumer<String, String>> consumerGroup = new ArrayList<KafkaConsumer<String, String>>();
    private final AtomicLong expectedRows = new AtomicLong(0);
    private final AtomicLong consumedRows = new AtomicLong(0);
    private final AtomicLong verifiedRows = new AtomicLong(0);
    private final AtomicLong duplifiedRows = new AtomicLong(0);
    private final AtomicBoolean testGood = new AtomicBoolean(true);
    final VerifierCliConfig config;
    private final AtomicInteger maxRecordSize = new AtomicInteger(0); // use
                                                                      // this to
                                                                      // calculate
                                                                      // the
                                                                      // allowable
                                                                      // duplicates
    private final ConcurrentLinkedQueue<Long> foundRowIds = new ConcurrentLinkedQueue<Long>();
    private final Map<Long, RowData> rowData = new ConcurrentHashMap<>();

    static class VerifierCliConfig extends CLIConfig {

        @Option(desc = "Kafka zookeeper host:port")
        String zookeeper = "localhost";

        @Option(desc = "Kafka brokers host:port")
        String brokers = "localhost";

        // Topic prefixes may be used when two or more instances of a tests share the same kafka cluster
        // as a means to distinquish each tests data.
        @Option(desc = "topic prefix")
        String topicprefix = "";

        @Option(desc = "use table export (CDC)")
        Boolean usetableexport = false;

        @Option(desc = "topic .  NOTE topicprefix+topic == the topic used when querying kafka ")
        String topic = "";

        @Option(desc = " the record field position for tracking sequences")
        Integer sequencefield;

        @Option(desc = " the record field position to use for tracking uniqueness")
        Integer uniquenessfield;

        @Option(desc = " topic record position for tracking the source partition")
        Integer partitionfield;

        @Option(desc = "does each data row include 6 metadata fields? Default: false")
        Boolean metadata = false;

        @Option(desc = "does each data row include 6 metadata fields? Default: false")
        Integer count = 0;

        @Option(desc = " the number of consumers to create when reading a topic")
        Integer consumers = 1;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "Filename to write periodic stat infomation in CSV format")
        String csvfile = "";

        @Option(desc = "Number of times to loop over the Kafka topic")
        Integer loops = 1;

        @Option(desc = " max amount of seconds to wait before not receiving another kafka record")
        Integer timeout = 60;
        // Integer timeout = 300;

        @Override
        public void validate() {
            // it's ok to have a empty prefix
            // if ("".equals(topicprefix)) {
            //     exitWithMessageAndUsage("topicprefix must not be empty");
            // }
            if ("".equals(topic)) {
                exitWithMessageAndUsage("topic must not be empty");
            }
            if (sequencefield == null) {
                exitWithMessageAndUsage("sequencefield must not be empty");
            }
            if (uniquenessfield == null) {
                exitWithMessageAndUsage("uniquenessfield must not be empty, txnid field is usually 0");
            }
            if (partitionfield == null) {
                exitWithMessageAndUsage("partitionfield must not be empty, partition id field is usually 3");
            }

        }
    }

    /**
     *
     * @author pshaw
     * Simple data only class, one instance per consumed row
     *
     * Assumes row data includes metadata (skipinternals=False), or things won't go well
     *
     */
    public class RowData {
        Long m_partitionId;
        Long m_sequenceNum;
        Long m_rowId;
        byte m_exportOp;

        public RowData(Long partitionId, Long sequenceNum, Long rowId, byte exportOp) {
            m_partitionId = partitionId;
            m_sequenceNum = sequenceNum;
            m_rowId = rowId;
            m_exportOp = exportOp;
        }
    }

    public class TopicReader implements Runnable {

        private final KafkaConsumer<String, String> consumer;
        private final CountDownLatch m_cdl;
        private final Integer m_uniqueFieldNum;
        private final Integer m_sequenceFieldNum;
        private final Integer m_partitionFieldNum;
        private final Boolean m_usetableexport;
        private final Boolean m_metadata;
        private final Integer timeoutSec;
        private final Integer m_count;

        public TopicReader(KafkaConsumer<String, String> consumer, CountDownLatch cdl, Integer uniqueFieldNum,
                Integer sequenceFieldNum, Integer partitionFieldNum, int timeout,
                AtomicBoolean testGood, Boolean usetableexport, Boolean metadata, Integer count) {
            this.consumer = consumer;
            m_cdl = cdl;
            m_uniqueFieldNum = uniqueFieldNum;
            m_sequenceFieldNum = sequenceFieldNum;
            m_partitionFieldNum = partitionFieldNum;
            m_usetableexport = usetableexport;
            m_metadata = metadata; // no metadata if stream as topic
            m_count = count;
            timeoutSec = timeout;

        }

        @Override
        public void run() {
            log.info("Consumer waiting count: " + m_cdl.getCount());
            int retries = timeoutSec;
            long recordCnt = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                consumedRows.addAndGet(records.count());
                recordCnt = +records.count();
                if (records.count() == 0) {
                    // if we don't get any records for "timeout" seconds or we reach the
                    // max number of
                    // expected
                    // records, break;
                    retries--;

                } else {
                    retries = timeoutSec;
                }
                if (retries == 0) {
                    log.info("No more records in the stream, this consumer found " + recordCnt + " records");
                    break;
                }

                for (ConsumerRecord<String, String> record : records) {

                    String smsg = record.value();
                    Integer mrs = Math.max(maxRecordSize.get(), smsg.getBytes().length);
                    maxRecordSize.set(mrs);
                    String row[] = RoughCSVTokenizer.tokenize(smsg);
                    Long rowTxnId = new Long(0);
                    Long sequenceNum = new Long(0);
                    // log.info("Row: " + smsg);
                    if (m_metadata) {
                        rowTxnId = Long.parseLong(row[m_uniqueFieldNum].trim());
                        sequenceNum = Long.parseLong(row[m_sequenceFieldNum]);
                    }

                    // if count is passed in, run til "count" rows consumed
                    // otherwise the number of expected rows should match the max of the
                    // field that contains the sequence field
                    if (m_count.intValue() > 0) {
                        expectedRows.set(m_count);
                    } else {
                        long maxRow = Math.max(expectedRows.get(), sequenceNum); // TODO: rework based on partitionid + sequence number
                        expectedRows.set(maxRow);
                    }

                    // new code to check for dupes using the combination of partition id and sequence number
                    // since sequence number by itself is zero-based per partition
                    if (m_usetableexport) {
                        Long partitionId = Long.parseLong(row[3]);
                        RowData rowObj = new RowData(
                            Long.parseLong(row[3]), // partition id
                            sequenceNum, // sequence number
                            rowTxnId, // row id
                            Byte.parseByte(row[5])  // export operation
                        );
                        long mapID = (partitionId<<32) | sequenceNum; // assumes sequence number doesn't exceed 4 billion unsigned
                        if (!rowData.containsKey(mapID)) {
                            rowData.put(mapID, rowObj);
                        } else {
                            duplifiedRows.incrementAndGet();
                            log.info("Duplicate row found: " + smsg);
                        }
                    }
                    if (m_metadata) {
                        foundRowIds.add(sequenceNum);
                        if (verifiedRows.incrementAndGet() % VALIDATION_REPORT_INTERVAL == 0) {
                            log.info("Verified " + verifiedRows.get() + " rows. Consumed: " + consumedRows.get()
                            + " Last export sequence num: " + row[m_sequenceFieldNum] + ", txnid " + row[m_uniqueFieldNum] + ","
                            + row[7] + "," + row[8] + "," + row[9] + " foundsize:" + foundRowIds.size());
                        }

                        if (m_partitionFieldNum != null) {
                            Integer partition = row[m_partitionFieldNum].trim() != "NULL" ? Integer.parseInt(row[m_partitionFieldNum].trim()) : 0;

                            if (TxnEgo.getPartitionId(rowTxnId) != partition) {
                                log.error("ERROR mismatched exported partition for txid " + rowTxnId
                                        + ", tx says it belongs to " + TxnEgo.getPartitionId(rowTxnId)
                                        + ", while export record says " + partition);

                                testGood.set(false);
                            }
                        }
                    }
                }
            }

            if (m_cdl != null) {
                m_cdl.countDown();
                log.info("Consumers still remaining: " + m_cdl.getCount());
            }
        }
    }

    public KafkaClientVerifier(VerifierCliConfig config) {
        // Use a random groupId for this consumer group so we don't conflict
        // with any other consumers.

        this.config = config;
        groupId = String.valueOf(System.currentTimeMillis());
        consumerTimeoutSecs = config.timeout;
        Properties props = new Properties();
        props.put("group.id", groupId);
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.commit.enable", "false"); // don't expire any messages,
                                                  // always start from the first
                                                  // offset if
                                                  // the client is restarted.
        props.put("auto.offset.reset", "earliest");
        props.put("queuedchunks.max", "1000");
        props.put("backoff.increment.ms", "1500");
        props.put("consumer.timeout.ms", "600000");
        props.put("rebalance.backoff.ms", "5000");
        props.put("zookeeper.session.timeout.ms", "5000");

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        props.put("bootstrap.servers", config.brokers);

        for (int x = 0; x < config.consumers; x++) {
            consumerGroup.add(new KafkaConsumer<String, String>(props));
        }

    }

    /**
     * Verify that the topic contains an expected number of rows, no records are
     * skipped based on the sequence field and no records are duplicated based on
     * the unique field
     *
     * Command line options typically passed in Python via runapp*.py
     *
     * "genqa.KafkaClientVerifier":
     * [
     *         ('--brokers', self.kafkacluster.get_kfka_conn_str()),
     *         ('--topicprefix', tableprefix),
     *         ('--topic',"EXPORT_PARTITIONED_TABLE_KAFKA"),
     *         ('--sequencefield',7),
     *         ('--uniquenessfield',0),
     *         ('--partitionfield',3),
     *         ('--consumers',3),
     * ]
     *
     *
     * @throws Exception
     */
    public void verifyTopic(String topic, Integer uniqueIndexFieldNum, Integer sequenceFieldNum,
            Integer partitionFieldNum, Boolean usetableexport, Boolean metadata, Integer count) throws Exception {

        List<String> topics = new ArrayList<String>();
        consumedRows.set(0);
        topics.add(topic);

        ExecutorService executor = Executors.newFixedThreadPool(consumerGroup.size());
        CountDownLatch consumersLatch = new CountDownLatch(consumerGroup.size());

        for (KafkaConsumer<String, String> consumer : consumerGroup) {

            consumer.subscribe(Arrays.asList(topic));
            log.info("Creating consumer for " + topic);
            TopicReader reader = new TopicReader(consumer, consumersLatch, uniqueIndexFieldNum, sequenceFieldNum,
                    partitionFieldNum, consumerTimeoutSecs, testGood, usetableexport, metadata, count);
            executor.execute(reader);
        }

        log.info("All Consumer Creation Done...Waiting for EOS");

        // Wait for all consumers to consume and timeout.
        log.info("Wait for drain of consumers.");
        long cnt = 0;
        long wtime = System.currentTimeMillis();
        while (true) {
            cnt = consumedRows.get();
            log.info("Consumed cnt rows: " + cnt);
            Thread.sleep(5000);
            if (cnt != consumedRows.get()) {
                long delta = consumedRows.get() - cnt;
                wtime = System.currentTimeMillis();
                log.info("Train is still running, got " + delta + " more records");
                continue;
            }
            if ((System.currentTimeMillis() - wtime) > 30000) {
                log.info("Waited long enough looks like train has stopped.");
                break;
            }
            if (consumersLatch.getCount() == 0) {
                break;
            }
        }

        consumersLatch.await();
        log.info("Seen Rows: " + consumedRows.get() + " Expected: " + expectedRows.get());

        if (consumedRows.get() == 0) {
            log.error("No rows were consumed.");
            testGood.set(false);
        } else if (consumedRows.get() < expectedRows.get()) {
            // we will calculate the details of this below.
            log.info("WARNING Consumed row count: '" + consumedRows.get()
                    + "' is less then the expected number given by the client: '" + expectedRows.get() + "'");
        }

        log.info("Checking for missing rows");

        Long[] sortedIds = foundRowIds.toArray(new Long[0]);
        Arrays.sort(sortedIds);
        List<Long> ids = Arrays.asList(sortedIds);
        long missingCnt = 0;
        long duplicateCnt = 0;
        long lastId = 0;
        int print_cnt = 0;
        for (Long id : ids) {
            if (id == lastId) {
                duplicateCnt++;
                continue;
            }

            // We may expect to have missing values in the sequence
            // because of failed txn's in the client, if we do make sure the
            // number of missing adds up to the expected row count.
            if (lastId < id - 1) {
                while (lastId < id) {
                    missingCnt++;
                    print_cnt++;
                    System.out.print(lastId + ",");
                    if (print_cnt % 50 == 0) {
                        System.out.print("\n");
                    }
                    lastId++;
                }
            } else {
                lastId = id;
            }
        }
        // # received - duplicates + missing rows = LastId
        // long realMissing = lastId - ids.size() - duplicateCnt + missingCnt;
        log.info("");
        log.info("Total messages in Kafka = " + ids.size());
        log.info("Total missing client row IDs in Kafka = " + missingCnt);
        // log.info("Total duplicates discovered in Kafka = " + duplicateCnt); TODO: rework dupe counting using partition id & sequence number
        log.info("Total attempted rows submitted from client (max client row ID) = " + lastId);
        log.info("max row ID number should = received count - duplicates + missing row ids");
        log.info(lastId + " should = " + ids.size() + " - " + duplicateCnt + " + " + missingCnt);

        // duplicates may be expected in some situations where only part of a buffer was transferred before
        // a failure and volt re-submits the entire buffer
        if (duplicateCnt > 0) {
            // if we have more then 60 MB worth of duplicates, this is an error
            long duplicate_size = duplicateCnt * maxRecordSize.get();
            // if volt was restarted we will see a valid gap in the sequence from when the client had failed transactions,
            // the amount of duplicates shouldn't
            // exceed 60 MB ( max size of a .pbd) from retransmitting the entire .pbd .
            if ( missingCnt > 0 && duplicateCnt > 60 * 1024 * 1024) {
                log.error("there were " + duplicateCnt + " duplicate ids using "+duplicate_size+ " bytes of space, it shouldn't exceed 60MB");
                testGood.set(false);
            } else {
                log.warn("there were " + duplicateCnt + " duplicate ids");
            }
        }

        // process metadata
        if (usetableexport) {
            long metaOps[] = {0, 0, 0, 0, 0};
            final int INSERTS = 1;
            final int DELETES = 2;
            final int UPDATES_BEFORE = 3;
            final int UPDATES_AFTER = 4;

            // total up metadata for all the rows, by metadata operation type
            for (Long rowObj : rowData.keySet()) {
                metaOps[rowData.get(rowObj).m_exportOp]++;
            }
            log.info("\nTable Export (change data capture) Statistics\n");
            log.info("Duplicates " + duplifiedRows.get());
            log.info("Inserts " + metaOps[INSERTS]);
            log.info("Deletes " + metaOps[DELETES]);
            log.info("Updates/Before " + metaOps[UPDATES_BEFORE]);
            log.info("Updates/After " + metaOps[UPDATES_AFTER]);
            log.info("Total-rows " + ids.size());
        }
    }

    public void stop() {
        for ( KafkaConsumer<String,String> consumer : consumerGroup) {
            consumer.close();
        }
    }

    static {
        VoltDB.setDefaultTimezone();
    }

    public static void main(String[] args) throws Exception {

        VoltLogger log = new VoltLogger("KafkaClientVerifier.main");
        VerifierCliConfig config = new VerifierCliConfig();
        config.parse(KafkaClientVerifier.class.getName(), args);
        KafkaClientVerifier verifier = null;
        for (int i = config.loops; i > 0; i--) {
            verifier = new KafkaClientVerifier(config);
            String fulltopic = config.topicprefix + config.topic;
            Boolean metadata = config.metadata;
            log.info("+++ Loops: " + i);
            try {
                verifier.verifyTopic(fulltopic, config.uniquenessfield, config.sequencefield,
                        config.partitionfield, config.usetableexport, metadata, config.count);
            } catch (IOException e) {
                log.error(e.toString());
                e.printStackTrace(System.err);
                System.exit(-1);
            } catch (ValidationErr e) {
                log.error("in Validation: " + e.toString());
                e.printStackTrace(System.err);
                System.exit(-1);
            } catch (Exception e) {
                log.error("in Application: " + e.toString());
                e.printStackTrace(System.err);
                System.exit(-1);
            }
        }

        if (verifier.testGood.get()) {
            log.info("SUCCESS -- exit 0");
            System.exit(0);
        } else {
            log.error("There were missing records");
        }

        log.info("FAILURE -- exit -1");
        System.exit(-1);

    }

}
