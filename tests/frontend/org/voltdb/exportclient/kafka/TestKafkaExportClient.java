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

package org.voltdb.exportclient.kafka;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.voltdb.exportclient.ExportClientTestBase;
import org.voltdb.exportclient.ExportDecoderBase.BinaryEncoding;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class TestKafkaExportClient extends ExportClientTestBase {
    @Test
    public void testConfigValidation() throws Exception
    {
        final KafkaExportClient client = new KafkaExportClient();
        final Properties config = new Properties();

        try {
            client.configure(config);
            fail("Empty config");
        } catch (IllegalArgumentException e) {}

        config.setProperty("metadata.broker.list", "fakehost");
        client.configure(config);
    }

    @Test
    public void testDefaultConfig() throws Exception
    {
        final KafkaExportClient client = new KafkaExportClient();
        final Properties config = new Properties();
        config.setProperty("metadata.broker.list", "fakehost");
        client.configure(config);
        assertEquals("fakehost", client.m_producerConfig.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class.getName(), client.m_producerConfig.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals(StringSerializer.class.getName(), client.m_producerConfig.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals("-1", client.m_producerConfig.getProperty(ProducerConfig.ACKS_CONFIG));
        assertEquals("voltdbexport", client.m_topicPrefix);
        assertEquals("voltdb", client.m_producerConfig.getProperty("client.id"));
        assertFalse(client.m_skipInternals);
        assertEquals(BinaryEncoding.HEX, client.m_binaryEncoding);
    }

    @Test
    public void testCustomPartitionColumn() throws Exception
    {
        final KafkaExportClient client = new KafkaExportClient();
        final Properties config = new Properties();
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "fakehost");
        config.setProperty("partition.key", "mytable.mycolumn,yourtable.yourcolumn");
        client.configure(config);
        assertEquals(2, client.m_tablePartitionColumns.size());
        assertEquals("mycolumn", client.m_tablePartitionColumns.get("mytable"));
        assertEquals("yourcolumn", client.m_tablePartitionColumns.get("yourtable"));
    }

    @Test
    public void testTablePartitionColumn() throws Exception
    {
        final KafkaExportClient client = new KafkaExportClient();
        final Properties config = new Properties();
        config.setProperty("metadata.broker.list", "fakehost");
        config.setProperty("partition.key", "tablepartitioncolumn"); //no such property
        try {
            client.configure(config);
            fail("argument check failed");
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void testAvroConfig() throws Exception
    {
        final KafkaExportClient client = new KafkaExportClient();
        final Properties config = new Properties();
        config.setProperty("metadata.broker.list", "fakehost1");
        config.setProperty("type", "avro");
        config.setProperty("schema.registry.url", "fakehost2");
        client.configure(config);
        assertEquals("fakehost1", client.m_producerConfig.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(KafkaAvroSerializer.class.getName(), client.m_producerConfig.getProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals(StringSerializer.class.getName(), client.m_producerConfig.getProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals("fakehost2", client.m_producerConfig.getProperty(KafkaExportClient.SCHEMA_REGISTRY_URL_CONFIG));
    }
}
