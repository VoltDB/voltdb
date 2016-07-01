/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package xdcrSelfCheck;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import xdcrSelfCheck.scenarios.ClientConflictRunner;
import xdcrSelfCheck.scenarios.ClientConflictRunner.ConflictType;

import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ClientThread extends BenchmarkThread {

    final Client m_primaryclient;
    final Client m_secondaryclient;
    final byte m_cid;
    final AtomicLong m_nextRid = new AtomicLong();
    final ClientPayloadProcessor m_processor;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final AtomicLong m_txnsRun;
    final Semaphore m_permits;
    final CountDownLatch m_latch;

    ClientThread(byte cid, AtomicLong txnsRun, Client primaryclient, Client secondaryclient,
                 ClientPayloadProcessor processor, Semaphore permits, CountDownLatch latch)
        throws Exception
    {
        setName("ClientThread(CID=" + String.valueOf(cid) + ")");
        m_cid = cid;
        m_primaryclient = primaryclient;
        m_secondaryclient = secondaryclient;
        m_processor = processor;
        m_txnsRun = txnsRun;
        m_permits = permits;
        m_latch = latch;
        log.info(getName());

        final String sql1 = String.format("select * from xdcr_partitioned where cid = %d order by rid desc limit 1", cid);
        final String sql2 = String.format("select * from xdcr_replicated  where cid = %d order by rid desc limit 1", cid);
        boolean done = false;
        while (! done) {
            try {
                VoltTable t1 = m_primaryclient.callProcedure("@AdHoc", sql1).getResults()[0];
                VoltTable t2 = m_primaryclient.callProcedure("@AdHoc", sql2).getResults()[0];
                long pNextRid = (t1.getRowCount() == 0) ? 1 : t1.fetchRow(0).getLong("rid") + 1;
                long rNextRid = (t2.getRowCount() == 0) ? 1 : t2.fetchRow(0).getLong("rid") + 1;
                m_nextRid.set(pNextRid > rNextRid ? pNextRid : rNextRid); // max
                done = true;
            } catch (Exception e) {
                log.warn("ClientThread threw exception in initialization, will retry", e);
                try {
                    Thread.sleep(3000);
                } catch (Exception e2) {
                }
            }
        }
    }

    static class UserProcCallException extends Exception {
        private static final long serialVersionUID = 1L;

        public ClientResponseImpl cri = null;
        UserProcCallException(ClientResponse cr) {
            cri = (ClientResponseImpl) cr;
        }
    }

    void runOne(String tableName, ConflictType conflictType) throws Throwable {
        try {
            ClientConflictRunner runner = ClientConflictRunner.getInstance(this, conflictType);
            log.info("Running " + runner.getClass().getSimpleName() + " on table " + tableName);
            runner.runScenario(tableName);
        } finally {
            m_nextRid.addAndGet(2);
        }
    }

    void shutdown() {
        m_shouldContinue.set(false);
    }

    public VoltTable[] callStoreProcedure(Client client,
                                          long rid,
                                          String procName,
                                          ClientPayloadProcessor.Pair payload) throws Exception {
        return callStoreProcedure(client, rid, procName, payload, null, null);
    }

    public VoltTable[] callStoreProcedure(Client client,
                                          long rid,
                                          String procName,
                                          ClientPayloadProcessor.Pair payload,
                                          ClientPayloadProcessor.Pair expected,
                                          String scenario) throws Exception {
        ClientResponse response;
        try {
            response = expected != null
                    ? (payload != null
                            ? client.callProcedure(procName, m_cid, rid,
                                    payload.Key.getBytes(), payload.getStoreValue(), expected.Key.getBytes(), expected.getStoreValue(),
                                    (byte) 0, scenario)
                            : client.callProcedure(procName, m_cid, rid,
                                    expected.Key.getBytes(), expected.getStoreValue(),
                                    (byte) 0, scenario))
                    : client.callProcedure(procName, m_cid, rid,
                            payload.Key.getBytes(), payload.getStoreValue(), (byte) 0);
        } catch (Exception e) {
            log.warn("ClientThread threw after " + m_txnsRun.get() +
                    " calls while calling procedure: " + procName +
                    " with args: cid: " + m_cid + ", nextRid: " + m_nextRid +
                    ", payload: " + payload);
            throw e;
        }

        // fake a proc call exception if we think one should be thrown
        if (response.getStatus() != ClientResponse.SUCCESS) {
            throw new ClientThread.UserProcCallException(response);
        }

        VoltTable[] results = response.getResults();

        m_txnsRun.incrementAndGet();

        return results;
    }

    void handleException(ClientResponseImpl cri, Exception e) {
        // this is not an error
        if ((cri.getStatus() == ClientResponse.USER_ABORT) && cri.getStatusString().contains("EXPECTED ROLLBACK")) {
            return;
        }

        // this implies bad data and is fatal
        if ((cri.getStatus() == ClientResponse.GRACEFUL_FAILURE) ||
                (cri.getStatus() == ClientResponse.USER_ABORT)) {
            hardStop("ClientThread had a proc-call exception that indicated bad data", cri);
        }
        // other proc call exceptions are logged, but don't stop the thread
        else {
            log.warn("ClientThread had a proc-call exception that didn't indicate bad data: " + e.getMessage());
            log.warn(cri.toJSONString());

            // take a breather to avoid slamming the LOG (stay paused if no connections)
            do {
                try { Thread.sleep(3000); } catch (Exception e2) {} // sleep for 3s
                // bail on wakeup if we're supposed to bail
                if (! m_shouldContinue.get()) {
                    return;
                }
            }
            while (m_primaryclient.getConnectedHostList().size() == 0
                    || m_secondaryclient.getConnectedHostList().size() == 0);
        }
    }

    @Override
    public void run() {
        while (m_shouldContinue.get()) {
            try {
                m_permits.acquire();

                String tableName = new Random().nextInt(2) == 0 ? "xdcr_partitioned" : "xdcr_replicated";
                List<Integer> ordinals = ConflictType.ordinals();
                Collections.shuffle(ordinals);
                for (int ord : ordinals) {
                    ConflictType conflictType = ConflictType.fromOrdinal(ord);
                    runOne(tableName, conflictType);
                }
            }
            catch (NoConnectionsException e) {
                log.error("ClientThread got NoConnectionsException on proc call. Will sleep.");
                // take a breather to avoid slamming the LOG (stay paused if no connections)
                do {
                    try { Thread.sleep(3000); } catch (Exception e2) {} // sleep for 3s
                    // bail on wakeup if we're supposed to bail
                    if (!m_shouldContinue.get()) {
                        return;
                    }
                }
                while (m_primaryclient.getConnectedHostList().size() == 0
                        || m_secondaryclient.getConnectedHostList().size() == 0);
            }
            catch (ProcCallException e) {
                ClientResponseImpl cri = (ClientResponseImpl) e.getClientResponse();
                handleException(cri, e);
            }
            catch (UserProcCallException e) {
                ClientResponseImpl cri = e.cri;
                handleException(cri, e);
            }
            catch (InterruptedException e) {
                // just need to fall through and get out
            }
            catch (InterruptedIOException e) {
                // just need to fall through and get out
            }
            catch (Throwable r) {
                hardStop("ClientThread had a non proc-call exception", r);
            }
        }

        m_latch.countDown();
    }

    public Client getPrimaryClient() {
        return m_primaryclient;
    }

    public Client getSecondaryClient() {
        return m_secondaryclient;
    }

    public byte getCid() {
        return m_cid;
    }

    public long getAndIncrementRid() {
        return m_nextRid.getAndIncrement();
    }

    public ClientPayloadProcessor getClientPayloadProcessor() {
        return m_processor;
    }
}
