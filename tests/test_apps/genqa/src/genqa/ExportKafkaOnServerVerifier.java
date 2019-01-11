/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import genqa.ExportOnServerVerifier.ValidationErr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.Throwable;
import java.util.Arrays;

import org.voltdb.VoltDB;

import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.I0Itec.zkclient.ZkClient;
import org.voltdb.iv2.TxnEgo;

/**
 * This verifier connects to kafka zk and consumes messsages from 2 topics
 * 1. Table data related
 * 2. End of Data topic to which client write when all export data is pushed.
 *
 * Each row is verified and row count is matched at the end.
 *
 */
public class ExportKafkaOnServerVerifier {

    public static long VALIDATION_REPORT_INTERVAL = 1000;

    private VoltKafkaConsumerConfig m_kafkaConfig;
    private final AtomicLong expectedRows = new AtomicLong(0);
    private final AtomicLong consumedRows = new AtomicLong(0);
    private final AtomicLong verifiedRows = new AtomicLong(0);
    private final AtomicBoolean testGood = new AtomicBoolean(true);
    private final ConcurrentLinkedQueue<Long> foundRowIds = new ConcurrentLinkedQueue<Long>();

    private static class VoltKafkaConsumerConfig {
        final String m_zkhost;
        final ConsumerConfig consumerConfig;
        final ConsumerConfig doneConsumerConfig;
        final ConsumerConnector consumer;
        final ConsumerConnector consumer2;
        final ConsumerConnector doneConsumer;
        private final String m_groupId;

        VoltKafkaConsumerConfig(String zkhost) {
            m_zkhost = zkhost;
            //Use random groupId and we clean it up from zk at the end.
            m_groupId = String.valueOf(System.currentTimeMillis());
            Properties props = new Properties();
            props.put("zookeeper.connect", m_zkhost);
            props.put("group.id", m_groupId);
            props.put("auto.commit.interval.ms", "1000");
            props.put("auto.commit.enable", "true");
            props.put("fetch.size", "10240"); // Use smaller size than default.
            props.put("auto.offset.reset", "smallest");
            props.put("queuedchunks.max", "1000");
            props.put("backoff.increment.ms", "1500");
            props.put("consumer.timeout.ms", "600000");

            consumerConfig = new ConsumerConfig(props);
            consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
            consumer2 = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

            //Certain properties in done consumer are different.
            props.remove("consumer.timeout.ms");
            props.put("group.id", m_groupId + "-done");
            //Use higher autocommit interval as we read only 1 row and then real consumer follows for long time.
            props.put("auto.commit.interval.ms", "10000");
            doneConsumerConfig = new ConsumerConfig(props);
            doneConsumer = kafka.consumer.Consumer.createJavaConsumerConnector(doneConsumerConfig);
        }

        public void stop() {
            doneConsumer.commitOffsets();
            doneConsumer.shutdown();
            consumer2.commitOffsets();
            consumer2.shutdown();
            consumer.commitOffsets();
            consumer.shutdown();
            tryCleanupZookeeper();
        }

        void tryCleanupZookeeper() {
            try {
                ZkClient zk = new ZkClient(m_zkhost);
                String dir = "/consumers/" + m_groupId;
                zk.deleteRecursive(dir);
                dir = "/consumers/" + m_groupId + "-done";
                zk.deleteRecursive(dir);
                zk.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

    }

    ExportKafkaOnServerVerifier() {
    }

    boolean verifySetup(String[] args) throws Exception {

        //Zookeeper
        m_zookeeper = args[0];
        System.out.println("Zookeeper is: " + m_zookeeper);
        //Topic Prefix
        m_topicPrefix = args[1]; //"voltdbexport";

        boolean skinny = false;
        if (args.length > 3 && args[3] != null && !args[3].trim().isEmpty()) {
            skinny = Boolean.parseBoolean(args[3].trim().toLowerCase());
        }

        m_kafkaConfig = new VoltKafkaConsumerConfig(m_zookeeper);

        return skinny;
    }

    /**
     * Verifies the fat version of the exported table. By fat it means that it contains many
     * columns of multiple types
     *
     * @throws Exception
     */
    void verifyFat() throws Exception
    {
        createAndConsumeKafkaStreams(m_topicPrefix, false);
    }

    /**
     * Verifies the skinny version of the exported table. By skinny it means that it contains the
     * bare minimum of columns (just enough for the purpose of transaction verification)
     *
     * @throws Exception
     */
    void verifySkinny() throws Exception
    {
        createAndConsumeKafkaStreams(m_topicPrefix, true);
    }

    public class ExportConsumer implements Runnable {

        private final KafkaStream m_stream;
        private final boolean m_doneStream;
        private final CountDownLatch m_cdl;
        private final boolean m_skinny;

        public ExportConsumer(KafkaStream a_stream, boolean doneStream, boolean skinny, CountDownLatch cdl) {
            m_stream = a_stream;
            m_doneStream = doneStream;
            m_skinny = skinny;
            m_cdl = cdl;
        }

        @Override
        public void run() {
            System.out.println("Consumer waiting count: " + m_cdl.getCount());
            try {
                ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
                while (it.hasNext()) {
                    byte msg[] = it.next().message();
                    String smsg = new String(msg);
                    String row[] = ExportOnServerVerifier.RoughCSVTokenizer.tokenize(smsg);
                    try {
                        if (m_doneStream) {
                            System.out.println("EOS Consumed: " + smsg + " Expected Rows: " + row[6]);
                            expectedRows.set(Long.parseLong(row[6]));
                            break;
                        }
                        consumedRows.incrementAndGet();
                        if (m_skinny) {
                            if (expectedRows.get() != 0 && consumedRows.get() >= expectedRows.get()) {
                                break;
                            }
                        }
                        ExportOnServerVerifier.ValidationErr err = ExportOnServerVerifier.verifyRow(row);
                        if (err != null) {
                            System.out.println("ERROR in validation: " + err.toString());
                        }

                        Long rowTxnId = Long.parseLong(row[6].trim());
                        Long rowNum = Long.parseLong(row[7]);
                        foundRowIds.add(rowNum);
                        if (verifiedRows.incrementAndGet() % VALIDATION_REPORT_INTERVAL == 0) {
                            System.out.println("Verified " + verifiedRows.get() + " rows. Consumed: " + consumedRows.get() + " Last row num: " + row[5] + "," + row[6] + "," + row[7] + "," + row[8]+","+ row[9] +" foundsize:"+foundRowIds.size());
                        }

                        Integer partition = Integer.parseInt(row[3].trim());

                        if (TxnEgo.getPartitionId(rowTxnId) != partition) {
                            System.err.println("ERROR mismatched exported partition for txid " + rowTxnId +
                                    ", tx says it belongs to " + TxnEgo.getPartitionId(rowTxnId) +
                                    ", while export record says " + partition);
                        }
                        //if (expectedRows != 0 && consumedRows.get() >= expectedRows) {
                        //    break;
                        //}
                    } catch (ExportOnServerVerifier.ValidationErr ex) {
                        System.out.println("Validation ERROR " + ex);
                    }
                }

            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                if (m_cdl != null) {
                    m_cdl.countDown();
                    System.out.println("Consumer waiting count: " + m_cdl.getCount());
                }
            }
        }
    }

    //Submit consumer tasks to executor and wait for EOS message then continue on.
    void createAndConsumeKafkaStreams(String topicPrefix, boolean skinny) throws Exception {
        final String topic = topicPrefix + "EXPORT_PARTITIONED_TABLE";
        final String topic2 = topicPrefix + "EXPORT_PARTITIONED_TABLE2";
        final String doneTopic = topicPrefix + "EXPORT_DONE_TABLE";

        List<Future<Long>> doneFutures = new ArrayList<>();

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = m_kafkaConfig.consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        ExecutorService executor = Executors.newFixedThreadPool(streams.size());

        // now launch all the threads
        CountDownLatch consumersLatch = new CountDownLatch(streams.size());
        for (final KafkaStream stream : streams) {
            System.out.println("Creating consumer for " + topic);
            ExportConsumer consumer = new ExportConsumer(stream, false, skinny, consumersLatch);
            executor.submit(consumer);
        }

        Map<String, Integer> topicCountMap2 = new HashMap<>();
        topicCountMap2.put(topic2, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap2 = m_kafkaConfig.consumer2.createMessageStreams(topicCountMap2);
        List<KafkaStream<byte[], byte[]>> streams2 = consumerMap2.get(topic2);

        ExecutorService executor2 = Executors.newFixedThreadPool(streams2.size());

        // now launch all the threads
        CountDownLatch consumersLatch2 = new CountDownLatch(streams2.size());
        for (final KafkaStream stream : streams2) {
            System.out.println("Creating consumer for " + topic2);
            ExportConsumer consumer = new ExportConsumer(stream, false, skinny, consumersLatch2);
            executor2.submit(consumer);
        }

        Map<String, Integer> topicDoneCountMap = new HashMap<String, Integer>();
        topicDoneCountMap.put(doneTopic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> doneConsumerMap = m_kafkaConfig.doneConsumer.createMessageStreams(topicDoneCountMap);

        List<KafkaStream<byte[], byte[]>> doneStreams = doneConsumerMap.get(doneTopic);
        ExecutorService executord2 = Executors.newFixedThreadPool(doneStreams.size());
        CompletionService<Long> ecs
                 = new ExecutorCompletionService<>(executord2);
        CountDownLatch doneLatch = new CountDownLatch(doneStreams.size());

        // now launch all the threads
        for (final KafkaStream stream : doneStreams) {
            System.out.println("Creating consumer for " + doneTopic);
            ExportConsumer consumer = new ExportConsumer(stream, true, true, doneLatch);
            Future<Long> f = ecs.submit(consumer, new Long(0));
            doneFutures.add(f);
        }

        System.out.println("All Consumer Creation Done...Waiting for EOS");
        // Now wait for any executorservice2 completion.
        ecs.take().get();
        System.out.println("Done Consumer Saw EOS...Cancelling rest of the done consumers.");
        for (Future<Long> f : doneFutures) {
            f.cancel(true);
        }
        //Wait for all consumers to consume and timeout.
        System.out.println("Wait for drain of consumers.");
        long cnt = consumedRows.get();
        long wtime = System.currentTimeMillis();
        while (true) {
            Thread.sleep(5000);
            if (cnt != consumedRows.get()) {
                wtime = System.currentTimeMillis();
                System.out.println("Train is still running.");
                continue;
            }
            if ( (System.currentTimeMillis() - wtime) > 60000 ) {
                System.out.println("Waited long enough looks like train has stopped.");
                break;
            }
        }
        m_kafkaConfig.stop();
        consumersLatch.await();
        consumersLatch2.await();
        System.out.println("Seen Rows: " + consumedRows.get() + " Expected: " + expectedRows.get());
        if ( consumedRows.get() == 0 ) {
            System.err.println("ERROR No rows were consumed.");
            testGood.set(false);
        } else if (consumedRows.get() < expectedRows.get() ) {
            // we will calculate the details of this below.
            System.out.println("WARNING Consumed row count: '" + consumedRows.get() + "' is less then the expected number given by the client: '" +
                expectedRows.get() + "'");
        }

        System.out.println("Checking for missing rows");
        // the total number of rows should equal the value in the EXPORT_DONE_TABLE
        // there may be missing rows because TXN's will fail if their was server shutdown
        List<Long> missingRowIds = new ArrayList<Long>();
        Long[] sortedIds = foundRowIds.toArray(new Long[0]);
        Arrays.sort(sortedIds);
        List<Long> ids = Arrays.asList(sortedIds);
        long currVal = 1;
        long missingCnt = 0;
        long duplicateCnt = 0;
        long lastId = 0;
        for (Long id : ids) {
            if ( lastId > id ) {
                // double check the sequence.
                System.err.println("out of sequence id: "+id+ " > " + lastId);
            }
            if ( id == lastId ) {
                duplicateCnt++;
                continue;
            }

            // We expect to have missing values in the sequence
            // beause of failed txn's in the client
            if ( lastId < id -1 ) {
                while ( lastId < id ) {
                    missingCnt++;
                    System.out.print(lastId+",");
                    lastId++;
                }
            } else {
                lastId = id;
            }
        }
        // # received - duplicates + missing rows = LastId
        long realMissing = lastId - (ids.size() - duplicateCnt + missingCnt);
        System.out.println("");
        System.out.println("Total messages in Kafka = " + ids.size());
        System.out.println("Total missing sequence numbers in Kafka = " + missingCnt);
        System.out.println("Total duplicates discovered in Kafka = " + duplicateCnt);
        System.out.println("Total attempted txnid from client = "+lastId);

        if ( realMissing > 0 ) {
            System.err.println("\nERROR There are '" + realMissing + "' missing rows");
            testGood.set(false);
        } else {
            System.out.println("There were no missing rows");
        }

        // duplicates may be expected in some situations
        if ( duplicateCnt > 0 ) {
            System.out.println("WARN there were '" + duplicateCnt + "' duplicate ids");
        }
        /*
        for ( long x = 0; x < expectedRows.get() ; x++ ) {
            if ( x % 10000 == 0) {
                System.out.println("Checking for missing rows 2.1 while " + x + " < " + expectedRows.get());
            }
            if (! foundRowIds.contains(x) ) {
                missingRowIds.add(x);
            }
       }
       System.out.println("Checking for missing rows 3 ");
       if (missingRowIds.size() > 0) {
            System.err.println("ERROR missing " + missingRowIds.size() + " rows, found "+foundRowIds.size()+ " rows" );
            for (Long rowid : missingRowIds ) {
                System.err.print(String.valueOf(rowid)+",");
            }
       } else {
            System.out.println("There are no missing rows");
       }
        */

        //For shutdown hook to not stop twice.
        m_kafkaConfig = null;

    }

    String m_topicPrefix = null;
    String m_zookeeper = null;

    static {
        VoltDB.setDefaultTimezone();
    }

    public void stopConsumer() {
        if (m_kafkaConfig != null) {
            m_kafkaConfig.stop();
        }
    }

    public static void main(String[] args) throws Exception {
        final ExportKafkaOnServerVerifier verifier = new ExportKafkaOnServerVerifier();
        try
        {
            /*
            Runtime.getRuntime().addShutdownHook(
                    new Thread() {
                        @Override
                        public void run() {
                            System.out.println("Shutting Down...");
                            verifier.stopConsumer();
                        }
                    });

            */
            boolean skinny = verifier.verifySetup(args);

            if (skinny) {
                verifier.verifySkinny();
            } else {
                verifier.verifyFat();
            }
        } catch(IOException e) {
            System.err.println("ERROR " + e.toString());
            e.printStackTrace(System.err);
            System.exit(-1);
        } catch (ValidationErr e ) {
            System.err.println("ERROR in Validation: " + e.toString());
            System.exit(-1);
        } catch (Exception e) {
            System.err.println("ERROR in Application: " + e.toString());
            System.exit(-1);
        }

        if (verifier.testGood.get())
            System.exit(0);
        else
            System.err.println("ERROR There were missing records");
            System.exit(-1);
    }

}
