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

package org.voltdb.importer.kafka;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Test;
import org.voltdb.importclient.kafka.KafkaExternalLoaderCLIArguments;
import org.voltdb.importclient.kafka.util.KafkaCommitPolicy;
import org.voltdb.importclient.kafka.util.KafkaConstants;

import junit.framework.Assert;
import static junit.framework.TestCase.assertEquals;

public class TestKafkaLoaderArgumentParsing {

    @Test
    public void testCommitPolicyParsing() throws Exception {

        Assert.assertEquals(KafkaCommitPolicy.NONE, KafkaCommitPolicy.fromString("NONE"));

        Assert.assertEquals(KafkaCommitPolicy.TIME, KafkaCommitPolicy.fromString("3000"));
        Assert.assertEquals(3000, KafkaCommitPolicy.fromStringTriggerValue("3000",KafkaCommitPolicy.TIME));

        Assert.assertEquals(KafkaCommitPolicy.TIME, KafkaCommitPolicy.fromString("3000ms"));
        Assert.assertEquals(3000, KafkaCommitPolicy.fromStringTriggerValue("3000ms",KafkaCommitPolicy.TIME));
    }


    @Test
    public void testHostPortArgsDefault() throws Exception {

        KafkaExternalLoaderCLIArguments args = new KafkaExternalLoaderCLIArguments();
        args.parse("KafaExternalLoader", new String[] { "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        List<String> hosts = args.getVoltHosts();
        Assert.assertEquals(Arrays.asList(new String[]{"localhost:21212"}), hosts);

        StringWriter sw = new StringWriter();
        args = new KafkaExternalLoaderCLIArguments(new PrintWriter(sw));
        args.parse("KafaExternalLoader", new String[] { "--servers", "host1:100,host2:200", "--host", "host3,host4", "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        hosts = args.getVoltHosts();
        Assert.assertEquals(Arrays.asList(new String[]{"host3:21212", "host4:21212"}), hosts);
        Assert.assertTrue(sw.toString().startsWith("Warning: --servers argument is deprecated in favor of --host; value is ignored."));

    }

    @Test
    public void testHostPortArgsServer() throws Exception {


        KafkaExternalLoaderCLIArguments args = new KafkaExternalLoaderCLIArguments(new PrintWriter(new StringWriter()));
        args.parse("KafaExternalLoader", new String[] { "-s", "host1,host2", "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        List<String> hosts = args.getVoltHosts();
        Assert.assertEquals(Arrays.asList(new String[]{"host1:21212", "host2:21212"}), hosts);

        args = new KafkaExternalLoaderCLIArguments(new PrintWriter(new StringWriter()));
        args.parse("KafaExternalLoader", new String[] { "--servers", "host1:100,host2:200", "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        hosts = args.getVoltHosts();
        Assert.assertEquals(Arrays.asList(new String[]{"host1:100", "host2:200"}), hosts);

        args = new KafkaExternalLoaderCLIArguments(new PrintWriter(new StringWriter()));
        args.parse("KafaExternalLoader", new String[] { "--servers", "host1,host2:200", "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        hosts = args.getVoltHosts();
        Assert.assertEquals(Arrays.asList(new String[]{"host1:21212", "host2:200"}), hosts);
    }

    @Test
    public void testHostPortArgsHost() throws Exception {

        KafkaExternalLoaderCLIArguments args = new KafkaExternalLoaderCLIArguments();
        args.parse("KafaExternalLoader", new String[] { "--host", "host1,host2", "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        List<String> hosts = args.getVoltHosts();
        Assert.assertEquals(Arrays.asList(new String[]{"host1:21212", "host2:21212"}), hosts);

        args = new KafkaExternalLoaderCLIArguments();
        args.parse("KafaExternalLoader", new String[] { "--host", "host1:100,host2:200", "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        hosts = args.getVoltHosts();
        Assert.assertEquals(Arrays.asList(new String[]{"host1:100", "host2:200"}), hosts);

        args = new KafkaExternalLoaderCLIArguments();
        args.parse("KafaExternalLoader", new String[] { "--host", "host1,host2:200", "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        hosts = args.getVoltHosts();
        Assert.assertEquals(Arrays.asList(new String[]{"host1:21212", "host2:200"}), hosts);
    }

    @Test
    public void testDefaultPort() throws Exception {

        StringWriter sw = new StringWriter();
        KafkaExternalLoaderCLIArguments args = new KafkaExternalLoaderCLIArguments(new PrintWriter(sw));
        args.parse("KafaExternalLoader", new String[] { "--port", "999", "--host", "host1,host2", "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        List<String> hosts = args.getVoltHosts();
        Assert.assertEquals(Arrays.asList(new String[]{"host1:999", "host2:999"}), hosts);
        Assert.assertTrue(sw.toString().startsWith("Warning: --port argument is deprecated, please use --host with <host:port> URIs instead."));

        sw = new StringWriter();
        args = new KafkaExternalLoaderCLIArguments(new PrintWriter(sw));
        args.parse("KafaExternalLoader", new String[] { "--port", "999", "--host", "host1:100,host2", "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        hosts = args.getVoltHosts();
        Assert.assertEquals(Arrays.asList(new String[]{"host1:100", "host2:999"}), hosts);
        Assert.assertTrue(sw.toString().startsWith("Warning: --port argument is deprecated, please use --host with <host:port> URIs instead."));

        sw = new StringWriter();
        args = new KafkaExternalLoaderCLIArguments(new PrintWriter(sw));
        args.parse("KafaExternalLoader", new String[] { "--port", "999", "--servers", "host1", "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        hosts = args.getVoltHosts();
        Assert.assertEquals(Arrays.asList(new String[]{"host1:999"}), hosts);
        Assert.assertTrue(sw.toString().contains("Warning: --port argument is deprecated, please use --host with <host:port> URIs instead."));
        Assert.assertTrue(sw.toString().startsWith("Warning: --servers argument is deprecated; please use --host instead"));

    }

    @Test
    public void testConfigFile() throws Exception {

        Properties props = new Properties();
        props.setProperty("group.id", "myGroup");
        props.setProperty("ignored", "foo");
        props.setProperty("auto.commit.enable", "true");

        File tempFile = File.createTempFile(this.getClass().getName(), ".properties");
        tempFile.deleteOnExit();
        props.store(new FileOutputStream(tempFile), "KafkaLoaderUnitTest.testDeprecatedConfigFile");

        StringWriter sw = new StringWriter();
        KafkaExternalLoaderCLIArguments args = new KafkaExternalLoaderCLIArguments(new PrintWriter(sw));
        args.parse("KafaExternalLoader", new String[] { "--config", tempFile.getAbsolutePath(), "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );

        assertEquals(args.commitpolicy, "1000ms");
        Assert.assertEquals("myGroup", args.groupid);
    }

    @Test
    public void testConfigFile2() throws Exception {

        // Values in the properties file, make sure we use them by default.
        Properties props = new Properties();
        props.setProperty("socket.timeout.ms", "111");
        props.setProperty("socket.receive.buffer.bytes", "222");

        File tempFile = File.createTempFile(this.getClass().getName(), ".properties");
        tempFile.deleteOnExit();
        props.store(new FileOutputStream(tempFile), "KafkaLoaderUnitTest.testDeprecatedConfigFile2");

        StringWriter sw = new StringWriter();
        KafkaExternalLoaderCLIArguments args = new KafkaExternalLoaderCLIArguments(new PrintWriter(sw));
        args.parse("KafaExternalLoader", new String[] { "--config", tempFile.getAbsolutePath(), "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );

        Assert.assertEquals(111, args.timeout);
        Assert.assertEquals(222, args.buffersize);

        // No config file, make sure we get the defaults
        args = new KafkaExternalLoaderCLIArguments();
        args.parse("KafaExternalLoader", new String[] {  "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );

        Assert.assertEquals(args.timeout, KafkaConstants.KAFKA_TIMEOUT_DEFAULT_MILLIS);
        Assert.assertEquals(args.buffersize, KafkaConstants.KAFKA_BUFFER_SIZE_DEFAULT);
    }

    @Test
    public void testKPartitionsArgumentDeprecated() throws Exception {
        StringWriter sw = new StringWriter();
        KafkaExternalLoaderCLIArguments args = new KafkaExternalLoaderCLIArguments(new PrintWriter(sw));
        args.parse("KafaExternalLoader", new String[] { "-kpartitions", "10", "-z", "localhost:2181", "-t", "volt-topic", "KAFKA_IMPORT" } );
        Assert.assertTrue(sw.toString().contains("Warning: --kpartions argument is deprecated, value is ignored."));
    }
}
