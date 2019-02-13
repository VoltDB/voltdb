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

package testutils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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
import org.voltdb.CLIConfig;
import org.voltdb.VoltDB;
import org.voltdb.iv2.TxnEgo;


/**
 * This verifier connects to kafka zk and consumes messsages from one or more topics
 *
 * It expects a field to contain a seqeuence number and another to contain a unique
 * index.  It will compute gaps in the sequence and and check for duplicates based
 * on the index.
 *
 */
public class KafkaClientVerifier {

    public static long VALIDATION_REPORT_INTERVAL = 1000;

    private KafkaConsumer<String,String> consumer;
    private String groupId;
    private List<KafkaConsumer<String,String>> consumerGroup = new ArrayList<KafkaConsumer<String,String>>();
    private final AtomicLong expectedRows = new AtomicLong(0);
    private final AtomicLong consumedRows = new AtomicLong(0);
    private final AtomicLong verifiedRows = new AtomicLong(0);
    private final AtomicBoolean testGood = new AtomicBoolean(true);
    private final AtomicInteger maxRecordSize = new AtomicInteger(0); // use this to calculate the allowable duplicates
    private final ConcurrentLinkedQueue<Long> foundRowIds = new ConcurrentLinkedQueue<Long>();

    static class VerifierCliConfig extends CLIConfig {
    
    		@Option(desc = "Kafka zookeeper host:port")
        String zookeeper = "localhost";
        
    		@Option(desc = "Kafka brokers host:port")
        String brokers = "localhost";

        @Option(desc = "topic prefix")
        String topicprefix  = "";
        
        @Option(desc = "topic .  NOTE topicprofix+topic == the topic used when quering kafka ")
        String topic = "";
        
        @Option(desc = " the topic field position for tracking sequences")
        Integer sequencefield; 
        
        @Option(desc = " topic field position to use for tracking uniqueness")
        Integer uniquenessfield;
        
        @Option(desc = " topic field position for tracking the source partition")
        Integer partitionfield;
        
        @Option(desc = " the number of consumers to create when reading a topic")
        Integer consumers = 1;
        
        @Override
        public void validate() {
        		if ( "".equals(topicprefix) ) {
        			exitWithMessageAndUsage("topicprefix must not be empty");
        		}
        		if ( "".equals(topic) ) {
        			exitWithMessageAndUsage("topic must not be empty");
        		}
        		if (  sequencefield == null) {
        			exitWithMessageAndUsage("sequencefield must not be empty");
        		}
        		if ( uniquenessfield == null  ) {
        			exitWithMessageAndUsage("uniquenessfield must not be empty, txnid field is usually 0");
        		}
        		if ( partitionfield == null  ) {
        			exitWithMessageAndUsage("partitionfield must not be empty, partition id field :qis usually 3");
        		}
        		
        }
    }
    
    public class TopicReader implements Runnable {

        private final KafkaConsumer<String,String> consumer;
        private final CountDownLatch m_cdl;
        private final Integer m_uniqueFieldNum;
        private final Integer m_sequenceFieldNum;
        private final Integer m_partitionFieldNum;
        private final Integer timeoutSec = 30;

        public TopicReader(KafkaConsumer<String,String> consumer, CountDownLatch cdl, Integer uniqueFieldNum, Integer sequenceFieldNum, Integer partitionFieldNum) {
            this.consumer = consumer;
            m_cdl = cdl;
            m_uniqueFieldNum = uniqueFieldNum;
            m_sequenceFieldNum = sequenceFieldNum;
            m_partitionFieldNum = partitionFieldNum;
            
        }

        @Override
        public void run() {
            System.out.println("Consumer waiting count: " + m_cdl.getCount());
            int retries = timeoutSec;  
            long recordCnt = 0;
                while (true) {
                	    ConsumerRecords<String,String> records = consumer.poll(1000);
                	    consumedRows.addAndGet(records.count());
                	    recordCnt =+ records.count();
                	    if ( records.count() == 0 ) {
                	    		// if we don't get any records for 30seconds or we reach the max number of expected
                	    		// records, break;
                	    		retries--;
                	    		
                	    } else {
                	    		retries = timeoutSec;
                	    }
                	    if ( retries == 0 ) {
                	    		System.out.println("No more records in the stream, this consumer found "+recordCnt + " records");
                	    		break;
                	    }
                	    
                	    for (ConsumerRecord<String, String> record : records) {
                	    		
                	        String smsg = record.value();
                	        Integer mrs = Math.max(maxRecordSize.get(), smsg.getBytes().length);
                	        maxRecordSize.set(mrs);
                	        String row[] = RoughCSVTokenizer.tokenize(smsg);
                        Long rowTxnId = Long.parseLong(row[m_uniqueFieldNum].trim());
                        Long rowNum = Long.parseLong(row[m_sequenceFieldNum]);
                    	    // the number of expected rows should match the max of the
                        // field that contains the seqeuence field
                        long maxRow = Math.max(expectedRows.get(), rowNum);
                        expectedRows.set(maxRow);
                        foundRowIds.add(rowNum);
                        if (verifiedRows.incrementAndGet() % VALIDATION_REPORT_INTERVAL == 0) {
                            System.out.println("Verified " + verifiedRows.get() + " rows. Consumed: " + consumedRows.get() + " Last row num: " + row[m_sequenceFieldNum] + ", txnid" + row[m_uniqueFieldNum] + "," + row[7] + "," + row[8]+","+ row[9] +" foundsize:"+foundRowIds.size());
                        }

                        if ( m_partitionFieldNum != null ) {
                            Integer partition = Integer.parseInt(row[m_partitionFieldNum].trim());

                            if (TxnEgo.getPartitionId(rowTxnId) != partition) {
                                System.err.println("ERROR mismatched exported partition for txid " + rowTxnId +
                                    ", tx says it belongs to " + TxnEgo.getPartitionId(rowTxnId) +
                                    ", while export record says " + partition);
                            }
                        }
                	    }
                }
                
                if (m_cdl != null) {
                    m_cdl.countDown();
                    System.out.println("Consumers still remaining: " + m_cdl.getCount());
                }
        }
    }
    
    public KafkaClientVerifier(VerifierCliConfig config) {
        //Use a random groupId for this consumer group so we don't conflict
    		// with any other consumers.
        groupId = String.valueOf(System.currentTimeMillis());
        Properties props = new Properties();
        props.put("group.id", groupId);
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.commit.enable", "false");  // don't expire any messages, always start from the first offset if the client is restarted.
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
        
        for ( int x = 0; x < config.consumers; x++ ) {
        		consumerGroup.add(new KafkaConsumer<String,String>(props));
        }
        
        
    }

    /**
     * Verify that the topic contains an expected number of rows, no records are skipped based on the sequence field and
     * no records are duplicated based on the unique field
     * @throws Exception
     */
    public void verifyTopic(String topic,Integer uniqueIndexFieldNum, Integer sequenceFieldNum, Integer partitionFieldNum) throws Exception
    {
       
    		List<String> topics = new ArrayList<String>();
    		topics.add(topic);
    		
    		ExecutorService executor = Executors.newFixedThreadPool(consumerGroup.size());
    		CountDownLatch consumersLatch = new CountDownLatch(consumerGroup.size());
    		
		for (KafkaConsumer<String, String> consumer : consumerGroup) {

			consumer.subscribe(Arrays.asList(topic));
			System.out.println("Creating consumer for " + topic);
			TopicReader reader = new TopicReader(consumer, consumersLatch, uniqueIndexFieldNum, sequenceFieldNum,
					partitionFieldNum);
			executor.execute(reader);
		}
    		
    		


        System.out.println("All Consumer Creation Done...Waiting for EOS");

        //Wait for all consumers to consume and timeout.
        System.out.println("Wait for drain of consumers.");
        long cnt = 0;
        long wtime = System.currentTimeMillis();
        while (true) {
        		cnt = consumedRows.get();
            Thread.sleep(5000);
            if (cnt != consumedRows.get()) {
            		long delta = consumedRows.get() - cnt;
                wtime = System.currentTimeMillis();
                System.out.println("Train is still running, got " + delta + " more records");
                continue;
            }
            if ( (System.currentTimeMillis() - wtime) > 60000 ) {
                System.out.println("Waited long enough looks like train has stopped.");
                break;
            }
            if ( consumersLatch.getCount() == 0 ) {
            		break;
            }
        }
        
        consumersLatch.await();
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
        Long[] sortedIds = foundRowIds.toArray(new Long[0]);
        Arrays.sort(sortedIds);
        List<Long> ids = Arrays.asList(sortedIds);
        long missingCnt = 0;
        long duplicateCnt = 0;
        long lastId = 0;
        int print_cnt = 0;
        for (Long id : ids) {
            if ( lastId > id ) {
                // double check the sequence.
                System.err.println("out of sequence id: "+id+ " > " + lastId);
            }
            if ( id == lastId ) {
                duplicateCnt++;
                continue;
            }

            // We may expect to have missing values in the sequence
            // because of failed txn's in the client, if we do make sure the
            // number of missing add's up to the expected row count.
            if ( lastId < id -1 ) {
                while ( lastId < id ) {
                    missingCnt++;
                    print_cnt++;
                    System.out.print(lastId+",");
                    if ( print_cnt % 50 == 0 ) {
                    		System.out.print("\n");
                    }
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
            // if we have more then 60 MB worth of duplicates, this is an error
            if ( duplicateCnt * maxRecordSize.get() > 60 * 1024 * 1024 ) {
            		System.out.println("ERROR there were '" + duplicateCnt + "' duplicate ids");
            		testGood.set(false);
            } else {
            		System.out.println("WARN there were '" + duplicateCnt + "' duplicate ids");
            }
        }
    }

    public void stop() {
		consumer.close();
    }

    static {
        VoltDB.setDefaultTimezone();
    }

    

    public static void main(String[] args) throws Exception {
        
        VerifierCliConfig config = new VerifierCliConfig();
        config.parse(KafkaClientVerifier.class.getName(), args);
		final KafkaClientVerifier verifier = new KafkaClientVerifier(config);
		String fulltopic = config.topicprefix + config.topic;
		try {
			verifier.verifyTopic(fulltopic, config.uniquenessfield, config.sequencefield, config.partitionfield);
		} catch (IOException e) {
			System.err.println("ERROR " + e.toString());
			e.printStackTrace(System.err);
			
			System.exit(-1);
		} catch (ValidationErr e) {
			System.err.println("ERROR in Validation: " + e.toString());
			e.printStackTrace(System.err);
			System.exit(-1);
		} catch (Exception e) {
			System.err.println("ERROR in Application: " + e.toString());
			e.printStackTrace(System.err);
			System.exit(-1);
		}

		if (verifier.testGood.get())
			System.exit(0);
		else
			System.err.println("ERROR There were missing records");
		
		System.exit(-1);
    		
    }

}
