#!/usr/bin/env groovy

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

@Grab('org.apache.kafka:kafka-clients:0.8.2.1')

import org.voltdb.client.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.*
import static org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.common.serialization.StringSerializer

@groovy.transform.Canonical
class OnNth {
    int limit
    Closure onNth
    final List<Future<?>> batch = []

    OnNth leftShift(final Future<?> fut) {
        batch << fut
        if (batch.size >= limit) flush()
        this
    }

    def flush() {
        onNth(batch.clone())
        batch.clear()
    }
}

def cli = new CliBuilder(usage: 'groovy kafka-pounder.groovy [options]')
cli.with {
    r(longOpt: 'rows', 'number of rows to insert', required:false, args:1)
    t(longOpt: 'topic', 'kafka topic', required:true, args:1)
    b(longOpt: 'borkers','kafka comma delimited broker list', required:true, args:1)
    c(longOpt: 'compression','one of none, gzip, snappy, lz4', required:true, args:1)
    h(longOpt: 'help', 'usage information', required: false)
}

def opts = cli.parse(args)
if (!opts) return

rows = (opts.r ?: '1000000') as int
topic = opts.t

if (opts.h) {
   cli.usage()
   return
}

def kconf = [
    (CLIENT_ID_CONFIG):'kafka-pounder',
    (BUFFER_MEMORY_CONFIG):2097152,
    (BATCH_SIZE_CONFIG):1024,
    (KEY_SERIALIZER_CLASS_CONFIG):StringSerializer.class.name,
    (VALUE_SERIALIZER_CLASS_CONFIG):StringSerializer.class.name,
    (BOOTSTRAP_SERVERS_CONFIG):opts.b,
    (COMPRESSION_TYPE_CONFIG):opts.c,
    (BLOCK_ON_BUFFER_FULL_CONFIG):true,
    (ACKS_CONFIG):'all',
    (RETRIES_CONFIG):4
]

def rlimit = new AtomicInteger()

ProducerRecord<String,String> rec(String t, int k, int v) {
    new ProducerRecord<String,String>(t, k as String, "${k},${v}".toString())
}

def cl = new CountDownLatch(10)

(1..10).collect {
    Thread.startDaemon("Kafka pounder ${it}") {
        def kp = new KafkaProducer<String,String>(kconf)
        def rnd = new Random()
        def awaiter = new OnNth(1000, {futs ->
            futs*.get()
        })
        cl.countDown()
        cl.await()

        int id = rlimit.getAndIncrement()
        while ( id < rows ) {
            awaiter << kp.send(rec(topic, id,rnd.nextInt(Integer.MAX_VALUE)))
            id = rlimit.getAndIncrement()
        }
        awaiter.flush()
    }
}.each { it.join() }
