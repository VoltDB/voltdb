#!/usr/bin/env groovy

@Grapes([
    @Grab('com.google.guava:guava:19.0'),
    @Grab('log4j:log4j:1.2.17'),
    @Grab('org.apache.kafka:kafka_2.10:0.8.2.1'),
    @GrabExclude('javax.mail:mail'),
    @GrabExclude('javax.jms:jms'),
    @GrabExclude('com.sun.jdmk:jmxtools')
])

import com.google.common.net.HostAndPort
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

def cli = new CliBuilder(usage: 'groovy kafka-offset-info.groovy [options]')
cli.with {
    b(longOpt: 'brokers','kafka comma delimited broker list', required:true, args:1)
    g(longOpt: 'group', 'consumenr group', required:true, args:1)
    t(longOpt: 'topic', 'kafka topic', required:true, args:1)
    h(longOpt: 'help', 'usage information', required: false)
}

def opts = cli.parse(args)
if (!opts) return

if (opts.h) {
   cli.usage()
   return
}

topic = opts.t
group = opts.g

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

printf("%-36s %4s %16s %16s %16s\n",'TOPIC','PRTN','EARLIEST','LATEST','COMMITTED')

prtldrs.each { int p, Broker b ->
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

    orqst = new kafka.javaapi.OffsetRequest(
        [(tnp):LATEST_OFFSET],
        kafka.api.OffsetRequest.CurrentVersion(), clientid)
    rsp = cnsmr.getOffsetsBefore(orqst)
    if (rsp.hasError()) {
        short code = rsp.errorCode(topic, p)
        throw ErrorMapping.exceptionFor(code)
    }
    long latest = rsp.offsets(topic,p)[0]

    cnsmr.close()

    long committed = -1L
    short version = 1
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

    printf("%-36s %4d %,16d %,16d %,16d\n",topic,p,earliest,latest,committed)
}

ofstldr.disconnect()
