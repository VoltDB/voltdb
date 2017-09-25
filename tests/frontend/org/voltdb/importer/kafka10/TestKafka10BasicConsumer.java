/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.importer.kafka10;

import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.importclient.kafka10.Kafka10ConsumerRunner;
import org.voltdb.importclient.kafka10.Kafka10InternalConsumerRunner;
import org.voltdb.importclient.kafka10.Kafka10StreamImporter;
import org.voltdb.importclient.kafka10.Kafka10StreamImporterConfig;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.Invocation;

public class TestKafka10BasicConsumer {

    ArrayList<String> procValues = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);

    class TestImporter extends Kafka10StreamImporter implements Runnable{

        public MockConsumer<ByteBuffer, ByteBuffer> consumer;
        public Kafka10InternalConsumerRunner consumerRunner;

        TestImporter(Kafka10StreamImporterConfig config, MockConsumer<ByteBuffer, ByteBuffer> consumer) {
            super(config);
            this.consumer = consumer;
        }

        @Override
        public boolean callProcedure(Invocation invocation, ProcedureCallback callback) {
          procValues.add(invocation.getParams()[0].toString());
          return true;
        }

        @Override
        public Kafka10ConsumerRunner createConsumerRunner(Properties properties) throws Exception {

            consumerRunner = new Kafka10InternalConsumerRunner(this, m_config, consumer) {
                @Override
                protected void subscribe() {
                    // Use a latch so we don't start producing until the consumer is ready (which is necessary for unit tests so
                    // they don't abort prematurely).
                    consumer.assign((Arrays.asList(new TopicPartition("my_topic", 0))));
                    latch.countDown();
                }
            };
            return consumerRunner;
        }

        @Override
        public void run() {
            accept();
        }

    }


    @Test
    public void testLifecycle() throws Exception {

        Properties props = new Properties();
        props.setProperty("groupid", "myGroupId");
        props.setProperty("topics", "my_topic");
        props.setProperty("brokers", "fake://broker");
        props.setProperty("procedure", "myProc");

        Kafka10StreamImporterConfig config = new Kafka10StreamImporterConfig(props);
        MockConsumer<ByteBuffer, ByteBuffer> consumer = new MockConsumer<ByteBuffer, ByteBuffer>(OffsetResetStrategy.EARLIEST);

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("my_topic", 0), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        TestImporter importer = new TestImporter(config, consumer);
        new Thread(importer).start();

        latch.await();

        ByteBuffer key = ByteBuffer.wrap("mykey".getBytes());
        ByteBuffer val = ByteBuffer.wrap("myvalue0".getBytes());
        consumer.addRecord(new ConsumerRecord<ByteBuffer, ByteBuffer>("my_topic", 0, 0L, key, val));
        assertSize(1, procValues);

        // Stop the importer. This will do a wakeup() on the consumer, which will abort any active poll. In response to the WakeupException,
        // the importer will close the consumer.  So subsequent calls to addRecord() should fail with an "already closed" exception.
        importer.stop();

        //wait for the consumer to be shutdown
        Thread.sleep(2000);
        assert(consumer == null);
    }


    @Test
    public void testBasicOperation() throws Exception {

        Properties props = new Properties();
        props.setProperty("groupid", "myGroupId");
        props.setProperty("topics", "my_topic");
        props.setProperty("brokers", "fake://broker");
        props.setProperty("procedure", "myProc");

        Kafka10StreamImporterConfig config = new Kafka10StreamImporterConfig(props);

        ArrayList<String> procValues = new ArrayList<>();
        AbstractImporter importer = new AbstractImporter() {

            @Override
            public String getName() {
               return "MockImporter";
            }

            @Override
            public URI getResourceID() {
                try {
                    return new URI("fake://uri");
                }
                catch (Exception e) {
                    return null;
                }
            }

            @Override
            protected void accept() {
            }

            @Override
            public void stop() {
            }

            @Override
            public boolean callProcedure(Invocation invocation, ProcedureCallback callback) {
              procValues.add(invocation.getParams()[0].toString());
              return true;
            }

        };


        CountDownLatch latch = new CountDownLatch(1);

        MockConsumer<ByteBuffer, ByteBuffer> consumer = new MockConsumer<ByteBuffer, ByteBuffer>(OffsetResetStrategy.EARLIEST);
        Kafka10InternalConsumerRunner consumerRunner = new Kafka10InternalConsumerRunner(importer, config, consumer) {
            @Override
            protected void subscribe() {
                consumer.assign((Arrays.asList(new TopicPartition("my_topic", 0))));
                latch.countDown();
            }
        };

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("my_topic", 0), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        Thread t = new Thread(consumerRunner);
        t.start();

        latch.await();
        ByteBuffer key = ByteBuffer.wrap("mykey".getBytes());
        ByteBuffer val = ByteBuffer.wrap("myvalue0".getBytes());
        consumer.addRecord(new ConsumerRecord<ByteBuffer, ByteBuffer>("my_topic", 0, 0L, key, val));
        consumer.addRecord(new ConsumerRecord<ByteBuffer, ByteBuffer>("my_topic", 0, 1L, key, val));
        consumer.addRecord(new ConsumerRecord<ByteBuffer, ByteBuffer>("my_topic", 0, 2L, key, val));
        consumer.addRecord(new ConsumerRecord<ByteBuffer, ByteBuffer>("my_topic", 0, 3L, key, val));
        consumer.addRecord(new ConsumerRecord<ByteBuffer, ByteBuffer>("my_topic", 0, 4L, key, val));

        assertSize(5, procValues);

    }

    private void assertSize(int expected,  ArrayList<String> collection) throws Exception {
        for (int i = 0; i < 5; i++) {
            if (expected == collection.size()) {
                return;
            }
            else {
                Thread.sleep(1000);
            }
        }
        Assert.fail("Collection does not contain expected number of items; expected=" + expected + " actual="+collection.size());
    }
}

