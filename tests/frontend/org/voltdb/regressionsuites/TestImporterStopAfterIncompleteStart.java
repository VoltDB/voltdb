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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.importclient.kafka.KafkaStreamImporterFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/** Tests a specific scenario where the importer never finishes starting up before the cluster is paused and shut down.
 * An error was being thrown to the log for a benign condition.
 * This scenario isn't Kafka specific, but is easiest to reproduce with a Kafka importer but no Kafka.
 */
public class TestImporterStopAfterIncompleteStart {
    private LocalCluster m_cluster = null;

    // This is how we know the appropriate code path was executed
    private static final String CHANNEL_UNREGISTRATION_PATTERN = "Skipping channel un-registration for";
    // Any errors except 'Failed to send topic metadata' fail the test
    private static final String ERRORS_PATTERN = "Failed to send topic metadata request for topic";

    @Before
    public void setUp() throws Exception {
        String schema = "CREATE TABLE test ( val BIGINT NOT NULL );\n"
                      + "PARTITION TABLE test ON COLUMN val;\n";

        m_cluster = new LocalCluster(
                schema,
                null,
                null,
                1, 1, 0, 0,
                BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING,
                false, false, null);

        List<String> logSearchPatterns = new ArrayList<>(1);
        logSearchPatterns.add(CHANNEL_UNREGISTRATION_PATTERN);
        logSearchPatterns.add(ERRORS_PATTERN);
        m_cluster.setHasLocalServer(false);
        m_cluster.setLogSearchPatterns(logSearchPatterns);
        m_cluster.overrideAnyRequestForValgrind();
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        // Verify that the importer we're configuring will use ChannelDistributer
        Assert.assertFalse(new KafkaStreamImporterFactory().isImporterRunEveryWhere());

        // We don't actually want Kafka to be found. Use a non-default port and don't launch Kafka.
        Properties props = RegressionSuite.buildProperties(
                "brokers", "localhost:9999",
                "topics", "T8_KAFKATABLE",
                "procedure", "test.insert",
                "version", "8");
        project.addImport(true, "kafka", "csv", "kafkastream.jar", props);

        m_cluster.compileDeploymentOnly(project);
        new File(project.getPathToDeployment()).deleteOnExit();
        m_cluster.setExpectedToCrash(false);
        m_cluster.startUp();
    }

    @Test
    public void testImporterStopAfterIncompleteStart() throws Exception {
        Thread.sleep(10000);
        Assert.assertTrue("Found ERROR - failing test",
                m_cluster.verifyLogMessage(ERRORS_PATTERN));

        Client client = m_cluster.createAdminClient(new ClientConfig());
        try {
            ClientResponse response = client.callProcedure("@Pause");
            Assert.assertEquals(ClientResponse.SUCCESS, response.getStatus());
            m_cluster.shutDown();
        } finally {
            client.close();
        }
        // With ENG-12070 the above procedure worked, but threw a spurious error in the log
        Assert.assertTrue("Did not find channel unregistration message - perhaps the test is broken?",
                m_cluster.verifyLogMessage(CHANNEL_UNREGISTRATION_PATTERN));
    }
}

