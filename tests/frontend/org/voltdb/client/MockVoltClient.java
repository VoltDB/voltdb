/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.client;

import static org.mockito.Mockito.doReturn;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.mockito.Mockito;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;

/** Hack subclass of VoltClient that fakes callProcedure. */
public class MockVoltClient implements Client, ReplicaProcCaller{
    public MockVoltClient() {
        super();
    }

    ProcedureCallback m_lastCallback = null;
    LinkedBlockingQueue<ProcedureCallback> m_callbacks = new LinkedBlockingQueue<ProcedureCallback>();
    boolean m_nextReturn = true;

    @Override
    public ClientResponse callProcedure(String procName, Object... parameters) throws ProcCallException {
        numCalls += 1;
        calledName = procName;
        calledParameters = parameters;

        if (abortMessage != null) {
            ProcCallException e = new ProcCallException(null ,abortMessage, null);
            abortMessage = null;
            throw e;
        }

        VoltTable[] candidateResult = null;
        if (!nextResults.isEmpty()) candidateResult = nextResults.get(0);
        final VoltTable[] result = candidateResult;
        if (resetAfterCall && !nextResults.isEmpty()) nextResults.remove(0);
        return new ClientResponse() {

            @Override
            public int getClientRoundtrip() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public int getClusterRoundtrip() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public String getStatusString() {
                return null;
            }

            @Override
            public VoltTable[] getResults() {
                return result;
            }

            @Override
            public byte getStatus() {
                return ClientResponse.SUCCESS;
            }

            @Override
            public byte getAppStatus() {
                return 0;
            }

            @Override
            public String getAppStatusString() {
                return null;
            }

            @Override
            public long getClientRoundtripNanos() {
                // TODO Auto-generated method stub
                return 0;
            }

        };
    }

    public String calledName;
    public Object[] calledParameters;
    public final List<VoltTable[]> nextResults =
            Collections.synchronizedList(new LinkedList<VoltTable[]>());
    public int numCalls = 0;
    public boolean resetAfterCall = true;
    public String abortMessage;
    public long lastOrigTxnId = Long.MIN_VALUE;
    public boolean origTxnIdOrderCorrect = true;
    private long m_startTime;

    @Override
    public boolean callProcedure(ProcedureCallback callback, String procName,
            Object... parameters) throws NoConnectionsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void drain() {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public int calculateInvocationSerializedSize(String procName,
            Object... parameters) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean callProcedure(
            ProcedureCallback callback,
            int expectedSerializedSize,
            String procName,
            Object... parameters)
            throws NoConnectionsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void backpressureBarrier() throws InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    public Object[] getInstanceId() {
        Object[] dumb = new Object[2];
        dumb[0] = m_startTime;
        dumb[1] = 0;
        return dumb;
    }

    @Override
    public String getBuildString() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean blocking() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void configureBlocking(boolean blocking) {
        // TODO Auto-generated method stub

    }

    @Override
    public void createConnection(String host) throws UnknownHostException, IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void createConnection(String host, int port) throws UnknownHostException, IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public ClientResponse updateApplicationCatalog(File catalogPath, File deploymentPath) throws IOException,
                                                                                         NoConnectionsException,
                                                                                         ProcCallException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean updateApplicationCatalog(ProcedureCallback callback,
                                            File catalogPath,
                                            File deploymentPath) throws IOException,
                                                                NoConnectionsException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean callProcedure(long originalTxnId,
                                 long originalTimestamp,
                                 ProcedureCallback callback,
                                 String procName,
                                 Object... parameters) throws IOException,
                                                      NoConnectionsException {
        numCalls += 1;
        calledName = procName;
        calledParameters = parameters;
        m_lastCallback = callback;
        m_callbacks.add(callback);
        if (originalTxnId <= lastOrigTxnId)
        {
            origTxnIdOrderCorrect = false;
        }
        else
        {
            lastOrigTxnId = originalTxnId;
        }

        return m_nextReturn;
    }


    public void pokeLastCallback(final byte status, final String message) throws Exception
    {
        ClientResponse clientResponse = new ClientResponseImpl(status, new VoltTable[0], message);
        m_lastCallback.clientCallback(clientResponse);
    }

    public void pokeAllPendingCallbacks(final byte status, final String message) throws Exception
    {
        ClientResponse clientResponse = new ClientResponseImpl(status, new VoltTable[0], message);
        ProcedureCallback callback = null;
        while ((callback = m_callbacks.poll()) != null) {
            callback.clientCallback(clientResponse);
        }
    }

    public void setNextReturn(boolean retval)
    {
        m_nextReturn = retval;
    }

    public void setInstanceStartTime(long time)
    {
        m_startTime = time;
    }

    @Override
    public ClientResponse callProcedure(long originalTxnId, long originalTimestamp,
                                        String procName, Object... parameters)
    throws IOException, NoConnectionsException, ProcCallException
    {
        numCalls += 1;
        calledName = procName;
        calledParameters = parameters;
        if (originalTxnId <= lastOrigTxnId)
        {
            origTxnIdOrderCorrect = false;
        }
        else
        {
            lastOrigTxnId = originalTxnId;
        }
        return new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[0], "");
    }

    @Override
    public ClientStatsContext createStatsContext() {
        ClientStatsContext mock = Mockito.mock(ClientStatsContext.class);
        doReturn(mock).when(mock).fetchAndResetBaseline();
        doReturn(Mockito.mock(ClientStats.class)).when(mock).getStats();
        return mock;
    }

    @Override
    public int[] getThroughputAndOutstandingTxnLimits() {
        return null;
    }

    @Override
    public void writeSummaryCSV(ClientStats stats, String path) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public List<InetSocketAddress> getConnectedHostList() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VoltBulkLoader getNewBulkLoader(String tableName, int maxBatchSize, BulkLoaderFailureCallBack blfcb) {
        return null;
    }

    @Override
    public ClientResponse updateClasses(File jarPath, String classesToDelete)
            throws IOException, NoConnectionsException, ProcCallException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean updateClasses(ProcedureCallback callback, File jarPath,
            String classesToDelete) throws IOException, NoConnectionsException {
        // TODO Auto-generated method stub
        return false;
    }

}
