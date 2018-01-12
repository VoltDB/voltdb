/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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


package org.voltdb.regressionsuites;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.tools.ant.util.FileUtils;
import org.voltcore.utils.CoreUtils;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.CatalogUtil;

import com.google_voltpatches.common.base.Stopwatch;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * End to end Import tests using the Kafka importer.
 */
public class TestKafkaImportSuite extends RegressionSuite {

    public TestKafkaImportSuite(String name) {
        super(name);
    }

    final static String TEST_TOPIC = "volt_topic";
    static String KAFKA_PORT = "9092";
    static String LOCALHOST_NAME = CoreUtils.getHostnameOrAddress();
    static String KAFKA_HOST_PORT = LOCALHOST_NAME + ":" + KAFKA_PORT;

    KafkaLocalCluster m_kafkaLocalCluster;

    static Charset charset = Charset.forName("UTF-8");
    static CharsetDecoder decoder = charset.newDecoder();

    public static String byteBufferToString(ByteBuffer buffer)
    {
        String data = "";
        try {
            int old_position = buffer.position();
            data = decoder.decode(buffer).toString();
            // reset buffer's position to its original so it is not altered:
            buffer.position(old_position);
        }
        catch (Exception e) {
            return data;
        }
        return data;
    }

    private static void produceOneMessage(String message) {
        //produce message here
        Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_HOST_PORT);
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        // start producer
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        // send one message to local kafkaLocalServer server:
        System.out.println("Producing message: " + message);
        KeyedMessage<String, String> data =
                new KeyedMessage<String, String>(TEST_TOPIC, message);
        producer.send(data);
    }

    public void testImportSimpleData() throws Exception {
        System.out.println("testImportSimpleData");

        List<String> expectedMessageList = new ArrayList<>();
        //produce message here
        Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_HOST_PORT);
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        // start producer
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        // send one message to local kafkaLocalServer server:
        for (int i = 0; i < 10; i++) {
            String message = "test-message-" + i + ", " + i;
            System.out.println("Producing message: " + message);
            expectedMessageList.add(message);
            KeyedMessage<String, String> data =
                    new KeyedMessage<String, String>(TEST_TOPIC, message);
            producer.send(data);
        }
        Client client = getClientToHostId(0);

        // check Kafka importer result
        Stopwatch sw = Stopwatch.createStarted();
        boolean foundImportData = false;
        while (sw.elapsed(TimeUnit.SECONDS) < (10)) {
            VoltTable vt = client.callProcedure("@AdHoc", "Select * from tmap order by val;").getResults()[0];
            System.out.println("Elapsed " + sw.elapsed(TimeUnit.SECONDS) + " seconds, Test table contents: " + vt);
            if (10 == vt.getRowCount()) {
                foundImportData = true;
                break;
            }
            Thread.sleep(1000);
        }

        assertTrue("VoltDB Kafka importer did not import enough data", foundImportData);
        producer.close();
    }

    // example code to use procedure and consumer from kafka cluster
    // this is not testing any VoltDB functionality
    public void thisIsASampleProducerConsumerExample() throws Exception {
        List<String> expectedMessageList = new ArrayList<>();
        //produce message here
        Properties props = new Properties();
        props.put("metadata.broker.list", KAFKA_HOST_PORT);
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        // send one message to local kafkaLocalServer server:
        for (int i = 0; i < 10; i++) {
            String message = "test-message-" + i + ", " + i;
            System.out.println("Producing message: " + message);
            expectedMessageList.add(message);
            KeyedMessage<String, String> data =
                    new KeyedMessage<String, String>(TEST_TOPIC, message);
            producer.send(data);
        }
        producer.close();

        // consumer
        Set<String> actualMessageList = new HashSet<>();
        SimpleConsumer kafkaconsumer = new SimpleConsumer("localhost", 9092, 10000, 1024000, "test_client");
        long offset = 0l;

        while (offset < 1) { // to avoid endless loop
            FetchRequest request = new FetchRequestBuilder().clientId("clientName").addFetch(TEST_TOPIC, 0, offset, 1000000).build();
            kafka.javaapi.FetchResponse fetchResponse = kafkaconsumer.fetch(request);

            for(MessageAndOffset messageAndOffset : fetchResponse.messageSet(TEST_TOPIC, 0)) {
                offset = messageAndOffset.offset();
                String message = byteBufferToString(messageAndOffset.message().payload()).toString();
                System.out.println("Consuming message: " + message);
                actualMessageList.add(message);
            }
        }

        assertEquals(10, actualMessageList.size());
        kafkaconsumer.close();
    }

    @Override
    public void setUp() throws Exception {
        // set up properties
        String KAFKA_LOG_DIR = VOLTDB_TEST_ROOT_DIR + "/kafka_logs";
        String ZOOKEEPER_DIR = VOLTDB_TEST_ROOT_DIR + "/zookeeper";

        FileUtils.delete(new File(KAFKA_LOG_DIR));
        FileUtils.delete(new File(ZOOKEEPER_DIR));

        String ZOOKEEPER_PORT = "2183";

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("broker.id", "0");
        kafkaProperties.setProperty("log.dirs", KAFKA_LOG_DIR);
        kafkaProperties.setProperty("num.partitions", "1");
        kafkaProperties.setProperty("replication", "0");
        // Kafka requires the machine's hostname to be resolveable
        kafkaProperties.setProperty("advertised.host.name", LOCALHOST_NAME);
        kafkaProperties.setProperty("port", KAFKA_PORT);
        kafkaProperties.setProperty("zookeeper.connect", LOCALHOST_NAME + ":" + ZOOKEEPER_PORT);
        kafkaProperties.setProperty("zookeeper.connection.timeout.ms", "6000");
        kafkaProperties.setProperty("kafka.topic", TEST_TOPIC);

        Properties zkProperties = new Properties();
        zkProperties.setProperty("dataDir", ZOOKEEPER_DIR);
        zkProperties.setProperty("clientPort", ZOOKEEPER_PORT);
        // disable the per-ip limit on the number of connections since this is a non-production config
        zkProperties.setProperty("maxClientCnxns", "0");

        //start kafka
        m_kafkaLocalCluster = new KafkaLocalCluster(kafkaProperties, zkProperties);

        // produce a message forcing kafka cluster to be fully initialized
        produceOneMessage("test-message-0");

        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        m_kafkaLocalCluster.shutdown();
    }

    private static String VOLTDB_TEST_ROOT_DIR;

    static public junit.framework.Test suite() {
        Map<String, String> additionalEnv = new HashMap<>();
        //Specify bundle location
        String bundleLocation = System.getProperty("user.dir") + "/bundles";
        System.setProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME, bundleLocation);
        System.out.println("Bundle location is: " + bundleLocation
                + ",  system property bundle: " + System.getProperty(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME));
        additionalEnv.put(CatalogUtil.VOLTDB_BUNDLE_LOCATION_PROPERTY_NAME, bundleLocation);

        LocalCluster config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestKafkaImportSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        try {
            project.addLiteralSchema(
                    "create table tmap (key varchar(30) not null, val bigint not null, primary key(key));  "
                  + "PARTITION TABLE tmap ON COLUMN key; ");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // configure socket importer
        Properties props = buildProperties(
                "brokers", "localhost:" + KAFKA_PORT,
                "topics", TEST_TOPIC,
                "procedure", "TMAP.insert",
                "version", "8");
        project.addImport(true, "kafka", "csv", null, props);

        config = new LocalCluster("kafka-importer.jar", 4, 1, 0, BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING, true, false, additionalEnv);
        config.setHasLocalServer(true);
        boolean compile = config.compile(project);
        assertTrue(compile);
        builder.addServerConfig(config, false);

        VOLTDB_TEST_ROOT_DIR = project.getPathToVoltRoot().getAbsolutePath();

        return builder;
    }
}
