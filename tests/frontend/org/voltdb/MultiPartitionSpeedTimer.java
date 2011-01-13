/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb;

import java.io.IOException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.voltdb.benchmark.overhead.OverheadClient;
import org.voltdb.benchmark.overhead.procedures.measureOverhead;
import org.voltdb.benchmark.overhead.procedures.measureOverheadMultipartition;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalSingleProcessServer;
import org.voltdb.regressionsuites.VoltServerConfig;

public class MultiPartitionSpeedTimer extends TestCase {

    VoltServerConfig m_config;

    AtomicLong m_procsCalled;
    AtomicLong m_procsReturned;
    long m_procCallCount;
    AtomicLong m_outstandingCalls;
    long m_maxOutstandingCalls;

    long m_startTime;
    long m_duration;
    Client m_client;

    class SpeedTestCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (clientResponse.getStatus() == ClientResponse.CONNECTION_LOST){
                return;
            }
            long count = m_procsReturned.incrementAndGet();
            m_outstandingCalls.decrementAndGet();
            if (count >= m_procCallCount)
                m_duration = System.currentTimeMillis() - m_startTime;
        }
    }

    void runATest(long procsToCall, long outstandingMax) throws UnknownHostException, IOException, ProcCallException, InterruptedException {
        // reset all the counters and such
        m_procsCalled = new AtomicLong(0);
        m_procsReturned = new AtomicLong(0);
        m_outstandingCalls = new AtomicLong(0);
        m_procCallCount = procsToCall;
        m_maxOutstandingCalls = outstandingMax;

        // note the time of start
        m_startTime = System.currentTimeMillis();

        // run the benchmark
        while(m_procsCalled.get() < m_procCallCount) {
            while (m_outstandingCalls.get() >= m_maxOutstandingCalls)
                Thread.yield();
            m_outstandingCalls.incrementAndGet();
            m_procsCalled.incrementAndGet();
            m_client.callProcedure(new SpeedTestCallback(), "measureOverheadMultipartition", 0L);
            //m_client.callProcedure(new SpeedTestCallback(), id, "measureOverhead", 0L);
        }
        m_client.drain();

    }

    // procedures used by these tests
    static final Class<?>[] PROCEDURES = {
        measureOverheadMultipartition.class, measureOverhead.class
    };

    public void testBasic() throws InterruptedException, UnknownHostException, IOException, ProcCallException {
        /////////////////////////////////////////////////////////////
        // START THE SERVER WITH THE RIGHT CATALOG/CONFIG
        /////////////////////////////////////////////////////////////

        URL url = OverheadClient.class.getResource("measureoverhead-ddl.sql");
        url.getPath();

        String schemaPath = url.getPath();

        // get a server config for the native backend with one sites/partitions
        m_config = new LocalSingleProcessServer("multisitespeed.jar", 2, BackendTarget.NATIVE_EE_JNI);
        //m_config = new LocalSingleProcessServer("multisitespeed.jar", 1, BackendTarget.NATIVE_EE_IPC);

        //m_config = new LocalCluster("multisitespeed.jar", 1, 1, BackendTarget.NATIVE_EE_JNI);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        //project.setBackendTarget(BackendTarget.NATIVE_EE_IPC);
        project.addSchema(schemaPath);
        project.addPartitionInfo("NEWORDER", "NO_O_ID");
        project.addProcedures(PROCEDURES);
        // build the jarfile
        m_config.compile(project);



        /////////////////////////////////////////////////////////////
        // RUN THE BENCHMARKS
        /////////////////////////////////////////////////////////////

        int totalCount = 10000;
        //for (int i = 1; i <= 1024; i*=2) {
        int i = 1024;
        m_config.startUp();

        // get a client
        String listener = m_config.getListenerAddresses().get(0);

        ClientConfig config = new ClientConfig("user", "pass");
        m_client = ClientFactory.createClient(config);
        m_client.createConnection(listener);

        runATest(totalCount, i);

        System.out.printf("%d OUTSTANDING: Ran %d txns in %.2f seconds at an average of %.1f ms/txn.\n",
                i, totalCount, m_duration / 1000.0, m_duration / (double)totalCount);

        m_config.shutDown();
        m_client.close();
        //}


        /////////////////////////////////////////////////////////////
        // STOP THE SERVER
        /////////////////////////////////////////////////////////////

        //m_config.shutDown();
    }
}
