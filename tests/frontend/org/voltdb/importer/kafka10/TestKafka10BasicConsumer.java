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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.voltdb.importclient.kafka10.Kafka10StreamImporterConfig;
import org.voltdb.importer.AbstractImporter;
import org.voltdb.importer.Invocation;

public class TestKafka10BasicConsumer {

    @Test
    public void testBasicOperation() throws Exception {

        Properties props = new Properties();
        Kafka10StreamImporterConfig config = new Kafka10StreamImporterConfig(props, null);

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

        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(OffsetResetStrategy.EARLIEST);
        Kafka10ConsumerRunner consumerRunner = new Kafka10ConsumerRunner(importer, config, consumer) {
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
        consumer.addRecord(new ConsumerRecord<byte[], byte[]>("my_topic", 0, 0L, "mykey".getBytes(), "myvalue0".getBytes()));
        consumer.addRecord(new ConsumerRecord<byte[], byte[]>("my_topic", 0, 1L, "mykey".getBytes(), "myvalue1".getBytes()));
        consumer.addRecord(new ConsumerRecord<byte[], byte[]>("my_topic", 0, 2L, "mykey".getBytes(), "myvalue2".getBytes()));
        consumer.addRecord(new ConsumerRecord<byte[], byte[]>("my_topic", 0, 3L, "mykey".getBytes(), "myvalue3".getBytes()));
        consumer.addRecord(new ConsumerRecord<byte[], byte[]>("my_topic", 0, 4L, "mykey".getBytes(), "myvalue4".getBytes()));

        assertSize(5, procValues);
        //Assert.fail("ARRRRRR");
    }

    private void assertSize(int expected, Collection collection) throws Exception {
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

