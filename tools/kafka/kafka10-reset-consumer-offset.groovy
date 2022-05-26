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

import groovy.json.JsonSlurper

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata

def cli = new CliBuilder(usage: 'groovy kafka10-reset-consumer-offset.groovy [options]')
cli.with {
    b(longOpt: 'brokers','kafka comma delimited broker list (format - broker:port)', required:true, args:1)
    g(longOpt: 'group', 'consumer group', required:true, args:1)
    t(longOpt: 'topic', 'kafka topic', required:true, args:1)
    h(longOpt: 'help', 'usage information', required: false)
    l(longOpt: 'latest', 'reset commit point to lastest')
    r(longOpt: 'restore', 'restore offsets recorded in [file-name]', args:1)
}

def opts = cli.parse(args)
if (!opts) return

if (opts.h || (opts.l && opts.r)) {
   cli.usage()
   return
}

group = opts.g
topic = opts.t

TIMEOUT = 100 * 1000
clientId = 'voltdb-offset-positioner'

def consumerConf = [
    (CLIENT_ID_CONFIG):clientId,
	(GROUP_ID_CONFIG):group,	    
    (AUTO_OFFSET_RESET_CONFIG):'none',
    (KEY_DESERIALIZER_CLASS_CONFIG):ByteArrayDeserializer.class.name,
    (VALUE_DESERIALIZER_CLASS_CONFIG):ByteArrayDeserializer.class.name,
    (BOOTSTRAP_SERVERS_CONFIG):opts.b,
    (ENABLE_AUTO_COMMIT_CONFIG):false,
	(REQUEST_TIMEOUT_MS_CONFIG):(TIMEOUT + 30),
	(SESSION_TIMEOUT_MS_CONFIG):TIMEOUT,
	(FETCH_MAX_WAIT_MS_CONFIG):TIMEOUT,
]

def resetOffset(KafkaConsumer<byte[], byte[]> consumer, Map<TopicPartition,Long> offsets) {
}

KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(consumerConf);
try {
    Map<String, List<PartitionInfo>> topics = consumer.listTopics()
    
    List<TopicPartition> tpart = new ArrayList<TopicPartition>()
    List<PartitionInfo> partitionInfos = []
    jsn = [:]
    if (opts.r) {        
        jprsr = new JsonSlurper()
        def file = new FileReader(opts.r)
        jsn = jprsr.parse(file)
        if (jsn.topic != topic) {
            throw new IllegalArgumentException(opts.r + "Topic mistmatch - provided topic ${topic}, found in file ${jsn.topic}")
        }
        partitionInfos = topics.get(jsn.topic)
        if (!partitionInfos || partitionInfos.isEmpty()) {
            throw new IllegalArgumentException(opts.r + " does not match match the topic")
        }
        if (jsn.offsets.size() != partitionInfos.size()) {
            throw new IllegalArgumentException(opts.r + " does not match number of offsets")
        }
    } else {
        partitionInfos = topics.get(topic)
        if (!partitionInfos || partitionInfos.isEmpty()) {        
            throw new IllegalArgumentException("Topic ${topic} does not exist")
        }
    }    
    partitionInfos.each {p ->  tpart.add(new TopicPartition(topic, p.partition()))}    

    Map<TopicPartition, OffsetAndMetadata> offsetPositions = [:]
    if (opts.r) {
        tpart.each { tp -> offsetPositions[tp] = new OffsetAndMetadata(jsn.offsets[tp.partition().toString()]) }
    } else if (opts.l) { 
        endOffsets = consumer.endOffsets(tpart) 
        endOffsets.each { partitionOffsets -> offsetPositions[partitionOffsets.key] = new OffsetAndMetadata(partitionOffsets.value) }
    } else { 
        startOffsets = consumer.beginningOffsets(tpart)
        startOffsets.each { partitionOffsets -> offsetPositions[partitionOffsets.key] = new OffsetAndMetadata(partitionOffsets.value) }
    }
    consumer.assign(tpart)
    consumer.commitSync(offsetPositions)    
} finally {
    consumer.close()
}
