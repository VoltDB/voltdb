#!/usr/bin/env groovy

/*
 * This file is part of VoltDB.
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

@Grapes([
    @Grab('com.google.guava:guava:19.0'),
    @Grab('log4j:log4j:1.2.17'),
    @Grab('org.apache.kafka:kafka-clients:2.3.0'),
])

import com.google.common.util.concurrent.SettableFuture
import com.google.common.util.concurrent.ListenableFuture

import static org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.ConsumerRecord

def cli = new CliBuilder(usage: 'groovy kafka10-message-prober.groovy [options]')
cli.with {
    b(longOpt: 'brokers','kafka comma delimited broker list (format - broker:port)', required:true, args:1)
    g(longOpt: 'group', 'consumer group', required:true, args:1)
    t(longOpt: 'topic', 'kafka topic', required:true, args:1)
    h(longOpt: 'help', 'usage information', required: false)
    m(longOpt: 'messages', 'number of messages to read', args:1, required:false)
    a(longOpt: 'auto-assign', 'use kafka consumer\'s partition auto assignment', args:0, required:false)
}

def opts = cli.parse(args)
if (!opts) return

if (opts.h) {
   cli.usage()
   return
}

TIMEOUT = 60 * 1000
clientId = 'voltdb-msg-prober'
topic = opts.t
group = opts.g
msgsToRead  = (opts.m ?: '1_000').replaceAll('_','') as int
boolean autoAssignPartition = false
if (opts.a) {
    autoAssignPartition = true
}

def consumerConf = [
    (CLIENT_ID_CONFIG): clientId,
    (GROUP_ID_CONFIG): group,
    (AUTO_OFFSET_RESET_CONFIG): 'earliest',
    (KEY_DESERIALIZER_CLASS_CONFIG): ByteArrayDeserializer.class.name,
    (VALUE_DESERIALIZER_CLASS_CONFIG): ByteArrayDeserializer.class.name,
    (BOOTSTRAP_SERVERS_CONFIG): opts.b,
    (ENABLE_AUTO_COMMIT_CONFIG): false,
    (REQUEST_TIMEOUT_MS_CONFIG): (TIMEOUT + 30),
    (SESSION_TIMEOUT_MS_CONFIG): TIMEOUT,
    (FETCH_MAX_WAIT_MS_CONFIG): TIMEOUT,
]

@groovy.transform.Canonical
class ProbeResults {
    final TopicPartition topicPartition
    final int msgsRead
    final long bytesRead
    final long startOffset
    final double bytesPerSec
    final double msgsPerSec
    ProbeResults(TopicPartition tp, int msgsToRead, long bytesRead, long startOffset, long durationInMillis) {
        topicPartition = tp
        this.msgsRead = msgsToRead
        this.bytesRead = bytesRead
        this.startOffset = startOffset
        this.bytesPerSec = (this.bytesRead*1000/durationInMillis)
        this.msgsPerSec = (this.msgsRead*1000/durationInMillis)
    }
}

@groovy.transform.Canonical
class Fetcher {
    final TopicPartition topicPartition
    final long startOffset
    final int minRcdToRead
    final Map<String, Object> consumerConf
    boolean kafkaManagedPartitions = true
    final SettableFuture<ProbeResults> fut = SettableFuture.create()

    ListenableFuture<ProbeResults> fetch() {
        Thread.startDaemon("Kafka message prober for ${topicPartition}") {
            String clientId = (consumerConf.get(CLIENT_ID_CONFIG) + '-' + topicPartition.topic() + '-' + topicPartition.partition())
            consumerConf.put(CLIENT_ID_CONFIG, clientId)
            KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(consumerConf)
            try {
                if (kafkaManagedPartitions) {
                    consumer.subscribe(Arrays.asList(topicPartition.topic()))
                } else {
                    consumer.assign(Arrays.asList(topicPartition))
                }
                long bytesRead = 0L
                int recordSize = 0;
                int recordsRead = 0;
                def startTime = System.currentTimeMillis() 
                while (recordsRead < minRcdToRead) {
                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    records.each { record ->
                        recordSize = record.serializedValueSize()
                        bytesRead += recordSize;
                        ++recordsRead;
                    }
                }
                def endTime = System.currentTimeMillis()
                fut.set(new ProbeResults(topicPartition, recordsRead, bytesRead, startOffset, endTime - startTime))
            } finally {
                consumer.close()
            }
        }
        fut
    }
}

KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(consumerConf)
def tpOffsets = [:]
List<TopicPartition> tpart = new ArrayList<TopicPartition>()

try {
    Map<String, List<PartitionInfo>> topics = consumer.listTopics()
    List<PartitionInfo> partitionInfos = topics.get(topic)
    if (!partitionInfos || partitionInfos.isEmpty()) {
        throw new IllegalArgumentException("Topic ${topic} does not exist")
    }

    partitionInfos.each {p ->  tpart.add(new TopicPartition(topic, p.partition()))}
    consumer.assign(tpart)
    
    Map<TopicPartition,Long> startOffsets = consumer.beginningOffsets(tpart)
    Map<TopicPartition,Long> endOffsets = consumer.endOffsets(tpart)
    
    printf("\n%-32s %4s %16s %16s %16s %12s \n",'TOPIC','PRTN','EARLIEST','LATEST','COMMITTED','LAG')
    tpart.each {tp ->
        startOffset = startOffsets.get(tp) 
        endOffset = endOffsets.get(tp)
        OffsetAndMetadata commitOffset = consumer.committed(tp)
        committed = 0
        if (commitOffset != null) {
            committed = commitOffset.offset() 
        }
        long lag = (committed == 0 || committed < startOffset) ? (endOffset - startOffset) :
            (committed > endOffset) ? 0L : (endOffset - committed)
        long nextOffset = consumer.position(tp)
        tpOffsets[tp] = nextOffset
        printf("%-32s %4d %,16d %,16d %,16d %,12d\n",tp.topic(), tp.partition(), startOffset, endOffset, committed, lag)
    }
} finally {
    consumer.close()
}

Long defaultMaxPoll = consumerConf.get(MAX_POLL_RECORDS_CONFIG);
if (defaultMaxPoll == null || defaultMaxPoll > msgsToRead) {
    consumerConf.put(MAX_POLL_RECORDS_CONFIG, msgsToRead)
}

tpart.collect {tp ->
    int startOffset = tpOffsets[tp];
    assert startOffset != null
    new Fetcher(tp, startOffset, msgsToRead, consumerConf, autoAssignPartition)
}.collect {
    it.fetch()
}.collect {
    it.get()
}.eachWithIndex { it, i ->
    if (i==0) println ""
    it.with {
        if (opts.a) {
            printf("Read %,7d messages totaling %,d bytes from %s starting at offset %,d\n",
                msgsRead, bytesRead, topicPartition.topic(), startOffset)
        } else {
            printf("Read %,7d messages totaling %,d bytes from %s starting at offset %,d\n",
                msgsRead, bytesRead, topicPartition, startOffset)
        }
    }
}
