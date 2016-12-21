#!/usr/bin/env groovy

/*
 * This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
    @Grab('org.apache.kafka:kafka_2.11:0.8.2.2'),
    @GrabExclude('javax.mail:mail'),
    @GrabExclude('javax.jms:jms'),
    @GrabExclude('com.sun.jdmk:jmxtools')
])

import com.google.common.net.HostAndPort
import com.google.common.util.concurrent.SettableFuture
import com.google.common.util.concurrent.ListenableFuture
import static com.google.common.base.Throwables.getStackTraceAsString as stackTraceFor

import kafka.api.ConsumerMetadataRequest
import kafka.api.FetchRequest
import kafka.api.FetchRequestBuilder
import kafka.api.PartitionOffsetRequestInfo
import kafka.cluster.Broker
import kafka.common.ErrorMapping
import kafka.common.OffsetAndMetadata
import kafka.common.TopicAndPartition
import kafka.javaapi.ConsumerMetadataResponse
import kafka.javaapi.FetchResponse
import kafka.javaapi.OffsetCommitRequest
import kafka.javaapi.OffsetCommitResponse
import kafka.javaapi.OffsetFetchRequest
import kafka.javaapi.OffsetFetchResponse
import kafka.javaapi.OffsetRequest
import kafka.javaapi.OffsetResponse
import kafka.javaapi.PartitionMetadata
import kafka.javaapi.TopicMetadata
import kafka.javaapi.TopicMetadataRequest
import kafka.javaapi.consumer.SimpleConsumer
import kafka.message.MessageAndOffset
import kafka.network.BlockingChannel

@groovy.transform.Canonical
class Attempter<P,R> {
    List<P> targets
    R attempt( Closure<R> clsr) {
        R result
        List<Exception> faults = []

        for (P t: targets) {
            try {
                return clsr(t)
            } catch (Exception e) {
                faults << new RuntimeException("target ${t} attempt failed", e)
            }
        }
        if (faults) {
            throw new RuntimeException(faults.collect { stackTraceFor(it) }.join("\n"))
        }
        null
    }
}

@groovy.transform.Canonical
class ProbeResults {
    final TopicAndPartition tnp
    final int msgs
    final long bytes
    final long startOffset
}

@groovy.transform.Canonical
class Fetcher {
    final TopicAndPartition tnp
    final String clientId
    final long startOffset
    final SimpleConsumer cnsmr
    final int msglmt
    final int fetchWait
    final SettableFuture<ProbeResults> fut = SettableFuture.create()

    ListenableFuture<ProbeResults> fetch() {
        Thread.startDaemon("Kafka message prober for ${tnp}") {
            try {
                FetchRequestBuilder frb =
                    new FetchRequestBuilder().clientId(clientId)
                if (fetchWait > 0) {
                    frb.maxWait(fetchWait).minBytes(64)
                }
                int msgcnt = 0
                long bytes = 0L
                long offset = startOffset
                final int buffsize = 64 << 10
                while (msgcnt < msglmt) {
                    try {
                        FetchRequest frq = frb.addFetch(
                            tnp.topic(), tnp.partition(), offset, buffsize).build()
                        FetchResponse frs = cnsmr.fetch(frq)
                        if (frs == null) break;
                        if (frs.hasError()) {
                            short code = frs.errorCode(tnp.topic(), tnp.partition())
                            fut.exception = ErrorMapping.exceptionFor(code)
                            return
                        }
                        def msgs = frs.messageSet(tnp.topic(),tnp.partition())
                        if (msgs.size() == 0) break
                        msgcnt += msgs.size()
                        msgs.each {
                            bytes += it.message().payload().limit()
                            offset = it.nextOffset()
                        }
                    } catch (Exception e) {
                        fut.exception = e
                        return
                    }
                }
                fut.set(new ProbeResults(tnp,msgcnt,bytes,startOffset))
            } finally {
                cnsmr.close()
            }
        }
        fut
    }
}

def cli = new CliBuilder(usage: 'groovy kafka-offset-info.groovy [options]')
cli.with {
    b(longOpt: 'brokers','kafka comma delimited broker list', required:true, args:1)
    g(longOpt: 'group', 'consumenr group', required:true, args:1)
    t(longOpt: 'topic', 'kafka topic', required:true, args:1)
    h(longOpt: 'help', 'usage information', required: false)
    w(longOpt: 'wait', 'fetch request wait time', args:1, required:false)
    m(longOpt: 'messages', 'number of messages to read', args:1, required:false)
}

def opts = cli.parse(args)
if (!opts) return

if (opts.h) {
   cli.usage()
   return
}

topic = opts.t
group = opts.g
fwait = (opts.w ?: '-1').replaceAll('_','') as int
msgs  = (opts.m ?: '1_000').replaceAll('_','') as int

SO_TIMEOUT = 100 * 1000
SO_BUFFSIZE = 64 * 1024
FETCH_BUFFSIZE = 256 * 1024
clientid = 'voltdb-importer'

LATEST_OFFSET = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.LatestTime(), 1);
EARLIEST_OFFSET = new PartitionOffsetRequestInfo(kafka.api.OffsetRequest.EarliestTime(), 1);

brokers = opts.b.split(',')
.collect { it.trim() }
.findAll { it }
.collect { HostAndPort.fromString(it).withDefaultPort(9092) }

prtldrs = new Attempter(brokers).attempt {
    prober = new SimpleConsumer(it.host,it.port,SO_TIMEOUT, SO_BUFFSIZE, clientid)
    tpmdrq = new TopicMetadataRequest([topic])
    tpmdrs = prober.send(tpmdrq)
    tpmd = tpmdrs.topicsMetadata().find { it.topic() == topic }
    ldrs = tpmd.partitionsMetadata().collectEntries { [it.partitionId(),it.leader()] }
    prober.close()
    ldrs
}

if (!prtldrs) {
    println "Topic ${topic} does not exist"; System.exit(0)
}

ofstldr = new Attempter(brokers).attempt {
    chnl = new BlockingChannel(it.host, it.port,
                               BlockingChannel.UseDefaultBufferSize(),
                               BlockingChannel.UseDefaultBufferSize(),
                               SO_TIMEOUT)
    chnl.connect()
    cmdrq = new ConsumerMetadataRequest(group,ConsumerMetadataRequest.CurrentVersion(),
                                        1, clientid)
    chnl.send(cmdrq)
    cmdrp = ConsumerMetadataResponse.readFrom(chnl.receive().buffer())
    if (cmdrp.errorCode() != ErrorMapping.NoError()) {
        throw ErrorMapping.exceptionFor(cmdrp.errorCode())
    }
    oldr = cmdrp.coordinator()
    chnl.disconnect()
    chnl = new BlockingChannel(oldr.host(),oldr.port(),
                               BlockingChannel.UseDefaultBufferSize(),
                               BlockingChannel.UseDefaultBufferSize(),
                               SO_TIMEOUT)
    chnl.connect()
    chnl
}

printf("%-36s %4s %16s %16s %16s %12s\n",'TOPIC','PRTN','EARLIEST','LATEST','COMMITTED','LAG')

prtldrs.collect { int p, Broker b ->
    cnsmr = new SimpleConsumer(b.host(), b.port(), SO_TIMEOUT, SO_BUFFSIZE, clientid)
    tnp = new TopicAndPartition(topic,p)

    orqst = new OffsetRequest(
        [(tnp):EARLIEST_OFFSET],
        kafka.api.OffsetRequest.CurrentVersion(), clientid)
    rsp = cnsmr.getOffsetsBefore(orqst)
    if (rsp.hasError()) {
        short code = rsp.errorCode(topic, p)
        throw ErrorMapping.exceptionFor(code)
    }
    long earliest = rsp.offsets(topic,p)[0]

    orqst = new OffsetRequest(
        [(tnp):LATEST_OFFSET],
        kafka.api.OffsetRequest.CurrentVersion(), clientid)
    rsp = cnsmr.getOffsetsBefore(orqst)
    if (rsp.hasError()) {
        short code = rsp.errorCode(topic, p)
        throw ErrorMapping.exceptionFor(code)
    }
    long latest = rsp.offsets(topic,p)[0]

    short version = 1
    long committed = -1L
    ofrq = new OffsetFetchRequest(group, [tnp], version, p, clientid)
    ofstldr.send(ofrq.underlying())
    ofrsp = OffsetFetchResponse.readFrom(ofstldr.receive().buffer())
    short code = ofrsp.offsets().get(tnp).error()
    if (code == ErrorMapping.NoError()) {
        committed = ofrsp.offsets().get(tnp).offset()
    } else if (code == ErrorMapping.UnknownTopicOrPartitionCode()) {
    } else {
        throw ErrorMapping.exceptionFor(code)
    }

    long lag = (committed <= 0 || committed < earliest) ? (latest-earliest) :
        (committed > latest) ? 0L : (latest-committed)

    printf("%-36s %4d %,16d %,16d %,16d %,12d\n",topic,p,earliest,latest,committed,lag)

    long offset = committed < earliest ? earliest : committed < latest ? committed : latest

    new Fetcher(tnp,clientid,offset,cnsmr,msgs,fwait)
}.collect {
    it.fetch()
}.collect {
    it.get()
}.eachWithIndex { it, i ->
    if (i==0) println ""
    it.with {
        printf("Read %,7d messages totaling %,d bytes from %s starting from offset %,d\n",
               msgs, bytes, tnp, startOffset)
    }
}

ofstldr.disconnect()
