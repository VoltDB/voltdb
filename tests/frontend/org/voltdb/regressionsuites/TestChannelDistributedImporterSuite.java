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

package org.voltdb.regressionsuites;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.importclient.socket.PullSocketImporterFactory;

import java.io.File;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/** Importer tests that exercise ChannelDistributer, but don't inherently require something like Kafka.
 * Created by bshaw on 7/10/17.
 */
public class TestChannelDistributedImporterSuite {
    private LocalCluster m_cluster = null;
    private Thread m_importerSource = null;
    private CountDownLatch m_startupLatch = null;
    private AtomicReference<Throwable> m_importerSourceException = new AtomicReference<>(null);

    private static final String ERRORS_LOG_PATTERN = "ERROR";

    /** Something for the importer to pull from. */
    private class ImporterSource extends Thread {

        final int m_port;

        ImporterSource(int port) {
            m_port = port;
        }

        @Override
        public void run() {
            try (ServerSocket socket = new ServerSocket(m_port)) {
                Assert.assertTrue(socket.isBound());
                m_startupLatch.countDown();
                while (!interrupted()) {
                    Thread.sleep(250);
                }
            } catch (Throwable t) {
                m_importerSourceException.compareAndSet(null, t);
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        final int PORT = 7001;
        m_startupLatch = new CountDownLatch(1);
        m_importerSource = new ImporterSource(PORT);
        m_importerSource.start();
        m_startupLatch.await();

        m_cluster = new LocalCluster(
                "CREATE TABLE test ( val BIGINT NOT NULL );",
                null,
                null,
                1, 1, 0, 0,
                BackendTarget.NATIVE_EE_JNI,
                LocalCluster.FailureState.ALL_RUNNING,
                false, false, null);

        List<String> logSearchPatterns = new ArrayList<>(1);
        logSearchPatterns.add(ERRORS_LOG_PATTERN);
        m_cluster.setHasLocalServer(false);
        m_cluster.setLogSearchPatterns(logSearchPatterns);
        m_cluster.overrideAnyRequestForValgrind();
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        // Verify that the importer we're configuring will use ChannelDistributer
        Assert.assertFalse(new PullSocketImporterFactory().isImporterRunEveryWhere());

        Properties props = RegressionSuite.buildProperties(
                "addresses", "localhost:" + PORT,
                "procedure", "test.insert");
        project.addImport(true, "custom", "csv", "pullsocketimporter.jar", props);

        m_cluster.compileDeploymentOnly(project);
        new File(project.getPathToDeployment()).deleteOnExit();
        m_cluster.setExpectedToCrash(false);
        m_cluster.startUp();
    }

    @After
    public void tearDown() throws Exception {
        m_cluster.shutDown();
        m_importerSource.interrupt();
        m_importerSource.join();
    }

    @Test
    public void testImporterWithNoData() throws Exception {
        Client client = m_cluster.createAdminClient(new ClientConfig());
        try {
            ClientResponse response;
            response = client.callProcedure("@Pause");
            Assert.assertEquals(ClientResponse.SUCCESS, response.getStatus());
            m_cluster.shutDown();
        } finally {
            client.close();
        }
        // With ENG-12070 the above procedure worked, but threw a spurious error in the log
        Assert.assertFalse(m_cluster.verifyLogMessage(ERRORS_LOG_PATTERN));
        Assert.assertTrue(m_cluster.verifyLogMessageNotExist(ERRORS_LOG_PATTERN));
    }
}

