/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import static genqa.ExportOnServerVerifier.VALIDATION_REPORT_INTERVAL;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.VoltDB;

import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.voltdb.iv2.TxnEgo;

public class ExportKafkaOnServerVerifier {

    public static long VALIDATION_REPORT_INTERVAL = 50000;

    private final List<RemoteHost> m_hosts = new ArrayList<RemoteHost>();

    private static class RemoteHost {
        @SuppressWarnings("unused")
        String host;
        String port;
        ConsumerConfig consumerConfig;
        ConsumerConnector consumer;
        ConsumerConnector doneConsumer;
        boolean activeSeen = false;
        boolean fileSeen = false;

        public void buildConfig(String a_zookeeper) {
            Properties props = new Properties();
            props.put("zookeeper.connect", a_zookeeper);
            props.put("group.id", "exportverifier");
            //Set 1 min timeout on recieving messages.
            props.put("consumer.timeout.ms", "60000");
            props.put("auto.commit.interval.ms", "1000");
            props.put("auto.commit.enable", "true");
            props.put("auto.offset.reset", "smallest");

            consumerConfig = new ConsumerConfig(props);

            consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
            doneConsumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);
        }

        public void stop() {
            doneConsumer.commitOffsets();
            doneConsumer.shutdown();
            
            consumer.commitOffsets();
            consumer.shutdown();
        }
    }

    public static class ValidationErr extends Exception {
        private static final long serialVersionUID = 1L;
        final String msg;
        final Object value;
        final Object expected;

        ValidationErr(String msg, Object value, Object expected) {
            this.msg = msg;
            this.value = value;
            this.expected = expected;
        }

        public ValidationErr(String string) {
            this.msg = string;
            this.value = "[not provided]";
            this.expected = "[not provided]";
        }
        @Override
        public String toString() {
            return msg + " Value: " + value + " Expected: " + expected;
        }
    }

    ExportKafkaOnServerVerifier()
    {
    }

    boolean verifySetup(String[] args) throws Exception    {
        String remoteHosts[] = args[0].split(",");

        for (String hostString : remoteHosts) {
            String split[] = hostString.split(":");
            RemoteHost rh = new RemoteHost();
            String host = split[0];
            String port = split[1];
            rh.host = host;
            rh.port = port;

            m_hosts.add(rh);
        }

        //Zookeeper
        m_zookeeper = args[1];
        System.out.println("Zookeeper is: " + m_zookeeper);
        //Topic
        m_topic = args[2]; //"voltdbexportEXPORT_PARTITIONED_TABLE";
        
        boolean skinny = false;
        if (args.length > 3 && args[3] != null && !args[3].trim().isEmpty()) {
            skinny = Boolean.parseBoolean(args[3].trim().toLowerCase());
        }
        for (RemoteHost rh : m_hosts) {
            rh.buildConfig(m_zookeeper);
        }
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
        createConsumerKafkaStreams(m_topic, m_doneTopic);
    }

    /**
     * Verifies the skinny version of the exported table. By skinny it means that it contains the
     * bare minimum of columns (just enough for the purpose of transaction verification)
     *
     * @throws Exception
     */
    void verifySkinny() throws Exception
    {
        createConsumerKafkaStreams(m_topic, m_doneTopic);
    }
    static public List<Long> seenTxnIds = new ArrayList<>();
    static public long expectedRows = 0;
    
    public class ExportConsumer implements Runnable {

        private KafkaStream m_stream;
        private int m_threadNumber;
        private final boolean m_doneStream;
        private final ConsumerConnector m_consumer;

        public ExportConsumer(KafkaStream a_stream, int a_threadNumber, boolean doneStream, ConsumerConnector consumer) {
            m_threadNumber = a_threadNumber;
            m_stream = a_stream;
            m_doneStream = doneStream;
            m_consumer = consumer;
        }

        public void run() {
            try {
                ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
                int ttlVerified = 0;
                while (it.hasNext()) {
                    byte msg[] = it.next().message();
                    String smsg = new String(msg);
                    String row[] = ExportOnServerVerifier.RoughCSVTokenizer.tokenize(smsg);
                    try {
                        if (m_doneStream) {
                            System.out.println("EOS Consumed: " + smsg + " Expected Rows: " + row[6]);
                            expectedRows = Long.parseLong(row[6]);
                            try {
                                m_consumer.commitOffsets();
                                Thread.sleep(2000);
                            } catch (InterruptedException ex) {                            
                            }
                            break;
                        } else {
                            long rowTxnId = Long.parseLong(row[6]);
                            seenTxnIds.add(rowTxnId);
                        }
                        ExportOnServerVerifier.ValidationErr err = ExportOnServerVerifier.verifyRow(row);
                        if (err != null) {
                            System.out.println("ERROR in validation: " + err.toString());
                        }
                        if (++ttlVerified % VALIDATION_REPORT_INTERVAL == 0) {
                            System.out.println("Verified " + ttlVerified + " rows.");
                        }

                        Integer partition = Integer.parseInt(row[3].trim());
                        Long rowTxnId = Long.parseLong(row[6].trim());

                        if (TxnEgo.getPartitionId(rowTxnId) != partition) {
                            System.err.println("ERROR: mismatched exported partition for txid " + rowTxnId +
                                    ", tx says it belongs to " + TxnEgo.getPartitionId(rowTxnId) +
                                    ", while export record says " + partition);
                        }                        
                    } catch (ExportOnServerVerifier.ValidationErr ex) {
                        Logger.getLogger(ExportKafkaOnServerVerifier.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
                System.out.println("Shutting down Thread: " + m_threadNumber);
            } catch (Exception ex) {
                m_consumer.commitOffsets();
                m_consumer.shutdown();
                ex.printStackTrace();
            }
        }
    }

    //Submit consumer tasks to executor and wait for EOS message then continue on.
    void createConsumerKafkaStreams(String topic, String doneTopic) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(m_hosts.size() * 4);
        ExecutorService executor2 = Executors.newFixedThreadPool(m_hosts.size() * 4);
        CompletionService<Long> ecs
                 = new ExecutorCompletionService<>(executor2);
        List<Future<Long>> doneFutures = new ArrayList<>();
        
        ExportConsumer bconsumer = null;
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        for (RemoteHost rh : m_hosts) {
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = rh.consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

            // now launch all the threads
            int threadNumber = 0;
            for (final KafkaStream stream : streams) {
                System.out.println("Creating consumer for " + topic);
                bconsumer = new ExportConsumer(stream, threadNumber++, false, rh.consumer);
                executor.submit(bconsumer);
            }
        }

        Map<String, Integer> topicDoneCountMap = new HashMap<String, Integer>();
        topicDoneCountMap.put(doneTopic, 1);
        for (RemoteHost rh : m_hosts) {
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = rh.doneConsumer.createMessageStreams(topicDoneCountMap);

            List<KafkaStream<byte[], byte[]>> doneStreams = consumerMap.get(doneTopic);

            // now launch all the threads
            int threadNumber = 0;
            for (final KafkaStream stream : doneStreams) {
                System.out.println("Creating consumer for " + doneTopic);
                bconsumer = new ExportConsumer(stream, threadNumber, true, rh.doneConsumer);
                Future<Long> f = ecs.submit(bconsumer, new Long(threadNumber++));
                doneFutures.add(f);
            }
        }
                
        System.out.println("All Consumer Creation Done...Waiting for EOS");
        //Now wait for any executorservice2 completion.
        Long l = ecs.take().get();
        System.out.println("Consumer " + l + " Saw EOS...Cancelling rest of the done consumers.");
        for (Future<Long> f : doneFutures) {
            f.cancel(true);
        }
        //Since client waits for export buffers to flush we are giving 1 min for kafka to catch up.
        executor.awaitTermination(2, TimeUnit.MINUTES);
    }

    String m_topic = null;
    String m_doneTopic = "voltdbexportEXPORT_DONE_TABLE";
    String m_zookeeper = null;

    static {
        VoltDB.setDefaultTimezone();
    }

    public void stopConsumer() {
        for (RemoteHost rh : m_hosts) {
            if (rh != null) {
                if (rh.consumer != null) {
                    try {
                        System.out.println("Committing offsets to zookeeper.");
                        rh.consumer.commitOffsets();
                        //We use 1000 ms for autocommit so let it kick in.
                        Thread.sleep(1100);
                    } catch (InterruptedException ex) {                        
                    }
                }
                rh.stop();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final ExportKafkaOnServerVerifier verifier = new ExportKafkaOnServerVerifier();
        try
        {
            Runtime.getRuntime().addShutdownHook(
                    new Thread() {
                        @Override
                        public void run() {
                            System.out.println("Shuttind Down...");
                            verifier.stopConsumer();
                        }
                    });

            boolean skinny = verifier.verifySetup(args);

            if (skinny) {
                verifier.verifySkinny();
            } else {
                verifier.verifyFat();
            }
            System.out.println("Seen Rows: " + seenTxnIds.size() + " Expected: " + expectedRows);
            if (seenTxnIds.size() != expectedRows) {
                System.out.println("ERROR: Exported row count does not match consumed rows.");
            }
        }
        catch(IOException e) {
            e.printStackTrace(System.err);
            System.exit(-1);
        }
        catch (ValidationErr e ) {
            System.err.println("Validation error: " + e.toString());
            System.exit(-1);
        }
        System.exit(0);
    }

}
