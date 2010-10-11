/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.export;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestExportAndRejoin extends TestCase {

    final int HOST_COUNT = 3;
    final AtomicBoolean globalContinue = new AtomicBoolean(true);

    VoltProjectBuilder getBuilderForTest() throws UnsupportedEncodingException {
        String simpleSchema =
            "create table counter (" +
            "incr bigint default 0 not null, " +
            "clientid smallint default 0 not null, " +
            "PRIMARY KEY(incr,clientid));";

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        String schemaPath = schemaFile.getPath();
        schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(schemaPath);
        builder.addPartitionInfo("counter", "incr");

        builder.addProcedures(InsertEcho.class);
        builder.addStmtProcedure("Check", "select * from counter where incr = ? and clientid = ?;", "counter.incr:0");
        return builder;
    }

    class ClusterRunner extends Thread {

        LocalCluster m_cluster;
        AtomicBoolean m_continue = new AtomicBoolean(true);
        int m_currentHost = 0;
        boolean m_hostAlive = true;
        int m_tryKills = 0;

        ClusterRunner() throws Exception {
            m_cluster = new LocalCluster("exportandrejoin.jar", 1, HOST_COUNT, 1, BackendTarget.NATIVE_EE_JNI);
            m_cluster.setHasLocalServer(false);
            VoltProjectBuilder project = getBuilderForTest();
            boolean compile = m_cluster.compile(project);
            MiscUtils.copyFile(project.getPathToDeployment(),
                    Configuration.getPathToCatalogForTest("export-ddl-sans-nonulls.xml"));
            assertTrue(compile);

            m_cluster.startUp();
        }

        void shutdown() throws InterruptedException {
            synchronized(this) {
                m_continue.set(false);
                m_cluster.shutDown();
            }
        }

        @Override
        public void run() {
            while (m_continue.get()) {
                operation();
            }
        }

        void operation() {
            try {
                if (m_hostAlive) {
                    Thread.sleep(1000);
                    synchronized(this) {
                        if (!m_continue.get()) return;
                        m_cluster.shutDownSingleHost(m_currentHost);
                    }
                    m_hostAlive = false;
                    Thread.sleep(1000);
                }
                else {
                    boolean success = true;
                    synchronized(this) {
                        if (!m_continue.get()) return;
                        success = m_cluster.recoverOne(m_currentHost, null, "localhost");
                    }
                    m_tryKills++;
                    if (success) {
                        m_hostAlive = true;
                        m_currentHost = (m_currentHost + 1) % m_cluster.getNodeCount();
                        m_tryKills = 0;
                    }
                    if (m_tryKills >= 6) {
                        System.exit(-1);
                    }
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    class SpammingClient extends Thread {

        final Client m_client;
        final short m_id;
        final int m_hostId;
        final AtomicBoolean m_continue = new AtomicBoolean(true);
        long m_lastRequestedValue = -1;
        long m_lastConfirmedValue = -1;
        boolean m_up = false;

        SpammingClient(short id, int hostId) throws Exception {
            m_hostId = hostId;
            m_id = id;
            m_client = ClientFactory.createClient();
        }

        long getConfirmedInserts() {
            return m_lastConfirmedValue;
        }

        void shutdown() {
            m_continue.set(false);
            //this.interrupt();
        }

        void runInsert() {
            try {
                m_lastRequestedValue++;
                ClientResponse response = m_client.callProcedure("InsertEcho", m_lastRequestedValue, m_id);
                if (response.getStatus() == ClientResponse.SUCCESS) {
                    long value = response.getResults()[0].asScalarLong();
                    assert ((value % 1000) == m_id);
                    assert ((value / 1000) == m_lastRequestedValue);

                    m_lastConfirmedValue++;
                    if ((m_lastConfirmedValue % 250) == 0)
                        System.out.printf("Spammer id %d has completed %d inserts\n",
                                m_id, m_lastConfirmedValue);
                }
            } catch (NoConnectionsException e) {
                e.printStackTrace();
                assert(false);
            } catch (IOException e) {
                e.printStackTrace();
                assert(false);
            } catch (ProcCallException e) {
                ClientResponse response = e.getClientResponse();
                if (response.getStatusString().equals("Connection to database host (localhost) was lost before a response was received")) {
                    m_up = false;
                    System.out.printf("Host %d unreachable\n", m_hostId);
                }
                else {
                    System.out.printf("UNEXPECTED FAIL: %s\n", response.getStatusString());
                    assert(false);
                }

                try { Thread.sleep(1000); } catch (InterruptedException e1) {}
            }
        }

        boolean runCheckProcedure(long incr) throws NoConnectionsException, IOException, ProcCallException {
            System.out.printf(" - subcheck for incr %d\n", incr);
            ClientResponse response = m_client.callProcedure("Check", incr, m_id);
            if (response.getStatus() == ClientResponse.SUCCESS) {
                VoltTable t = response.getResults()[0];
                //System.out.printf("check table: %s\n", t.toString());
                long value = t.getRowCount();
                if (value == 1) {
                    return true;
                }
                else if (value == 0) {
                    return false;
                }
                else {
                    System.out.println("Oh crap.");
                    assert(false);
                }
            }
            assert(false);
            return false;
        }

        void runCheck() {
            try {
                System.out.println("calling check");
                if (runCheckProcedure(m_lastRequestedValue)) {
                    m_lastConfirmedValue++;
                }
                else {
                    if (m_lastRequestedValue > 0) {
                        boolean prev = runCheckProcedure(m_lastRequestedValue - 1);
                        assert(prev);
                    }
                    m_lastRequestedValue--;
                    System.out.printf("Reseting asked for value to: %d\n", m_lastRequestedValue);
                }
            } catch (NoConnectionsException e) {
                e.printStackTrace();
                assert(false);
            } catch (IOException e) {
                e.printStackTrace();
                assert(false);
            } catch (ProcCallException e) {
                ClientResponse response = e.getClientResponse();
                if (response.getStatusString().equals("Connection to database host (localhost) was lost before a response was received")) {
                    m_up = false;
                    System.out.printf("Host %d unreachable\n", m_hostId);
                }
                else {
                    System.out.printf("UNEXPECTED FAIL: %s\n", response.getStatusString());
                    assert(false);
                }

                try { Thread.sleep(1000); } catch (InterruptedException e1) {}
            }
        }

        void runConnect() {
            try {
                m_client.createConnection("localhost", VoltDB.DEFAULT_PORT + m_hostId);
                m_up = true;
                System.out.printf("Host %d (re)connected\n", m_hostId);
            }
            catch (Exception e) {
                //System.out.printf("Host %d still unreachable\n", m_hostId);
                try { Thread.sleep(10); } catch (InterruptedException e1) {}
            }
        }

        void runOperation() {
            if (m_lastConfirmedValue != m_lastRequestedValue)
                runCheck();
            else
                runInsert();
        }

        @Override
        public void run() {
            try {
                while (m_continue.get()) {
                    if (!m_up) {
                        runConnect();
                    }
                    else {
                        runOperation();
                    }
                }
            }
            catch (AssertionError e) {
                e.printStackTrace();
                globalContinue.set(false);
            }
        }

    }

    class ExportClient extends Thread {

        @Override
        public void run() {
        }

    }


    public void testSimultanious() throws Exception {
        ClusterRunner runner = new ClusterRunner();
        runner.start();

        SpammingClient[] spammers = new SpammingClient[HOST_COUNT * 10];
        for (int i = 0; i < spammers.length; i++) {
            spammers[i] = new SpammingClient((short) i, i % HOST_COUNT);
        }
        for (int i = 0; i < spammers.length; i++) {
            spammers[i].start();
        }

        ExportClient exporter = new ExportClient();
        exporter.start();

        int sleepTime = 1 * 60 * 1000;
        while (sleepTime > 0) {
            if (!globalContinue.get()) {
                fail();
            }
            Thread.sleep(100);
            sleepTime -= 100;
        }

        for (int i = 0; i < spammers.length; i++) {
            spammers[i].shutdown();
        }
        for (int i = 0; i < spammers.length; i++) {
            spammers[i].join();
            assertTrue(spammers[i].getConfirmedInserts() > 0);
        }

        exporter.join();

        runner.shutdown();
        runner.join();
    }

}
