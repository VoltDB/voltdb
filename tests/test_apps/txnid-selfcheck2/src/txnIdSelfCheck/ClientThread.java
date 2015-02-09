/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package txnIdSelfCheck;

import java.io.InterruptedIOException;

import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.VoltProcedure.VoltAbortException;

import txnIdSelfCheck.procedures.UpdateBaseProc;

public class ClientThread extends BenchmarkThread {

    static enum Type {
        PARTITIONED_SP, PARTITIONED_MP, REPLICATED, HYBRID, ADHOC_MP;

        /**
         * Use modulo so the same CID will run the same code
         * across client process lifetimes.
         */
        static Type typeFromId(float mpRatio, boolean allowInProcAdhoc) {
            if (rn.nextDouble() < mpRatio) {
                int r = rn.nextInt(19);
                if (allowInProcAdhoc && (r < 1)) return ADHOC_MP;  // 0% or ~5% of MP workload
                if (r < 7) return PARTITIONED_MP;                  // ~33% or 38%
                if (r < 13) return REPLICATED;                     // ~33%
                if (r < 19) return HYBRID;                         // ~33%
            }
            return PARTITIONED_SP;
        }
    }

    final Client m_client;
    final byte m_cid;
    long m_nextRid;
    final Type m_type;
    final TxnId2PayloadProcessor m_processor;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final AtomicLong m_txnsRun;
    static Random rn = new Random(31); // deterministic sequence
    final Random m_random = new Random();
    final Semaphore m_permits;

    ClientThread(byte cid, AtomicLong txnsRun, Client client, TxnId2PayloadProcessor processor, Semaphore permits,
            boolean allowInProcAdhoc, float mpRatio)
        throws Exception
    {
        setName("ClientThread(CID=" + String.valueOf(cid) + ")");
        m_type = Type.typeFromId(mpRatio, allowInProcAdhoc);
        m_cid = cid;
        m_client = client;
        m_processor = processor;
        m_txnsRun = txnsRun;
        m_permits = permits;
        log.info("ClientThread(CID=" + String.valueOf(cid) + ") " + m_type.toString());

        String sql1 = String.format("select * from partitioned where cid = %d order by rid desc limit 1", cid);
        String sql2 = String.format("select * from replicated  where cid = %d order by rid desc limit 1", cid);
        VoltTable t1;
        VoltTable t2;
        while (true) {
            try {
                  t1 = client.callProcedure("@AdHoc", sql1).getResults()[0];
                  t2 = client.callProcedure("@AdHoc", sql2).getResults()[0];
                  // init the dimension table to have one record for each cid.
                  client.callProcedure("PopulateDimension", cid);
                  break;
            }
            catch (Exception e) {
                log.warn("ClientThread threw exception in initialization, will retry", e);
                try { Thread.sleep(3000); } catch (Exception e2) {}
            }
        }

        long pNextRid = (t1.getRowCount() == 0) ? 1 : t1.fetchRow(0).getLong("rid") + 1;
        long rNextRid = (t2.getRowCount() == 0) ? 1 : t2.fetchRow(0).getLong("rid") + 1;
        m_nextRid = pNextRid > rNextRid ? pNextRid : rNextRid; // max
    }

    class UserProcCallException extends Exception {
        private static final long serialVersionUID = 1L;

        public ClientResponseImpl cri = null;
        UserProcCallException(ClientResponse cr) {
            cri = (ClientResponseImpl) cr;
        }
    }

    void runOne() throws Exception {
        // 1/10th of txns roll back
        byte shouldRollback = (byte) (m_random.nextInt(10) == 0 ? 1 : 0);

        try {
            String procName = null;
            int expectedTables = 4;
            switch (m_type) {
            case PARTITIONED_SP:
                procName = "UpdatePartitionedSP";
                break;
            case PARTITIONED_MP:
                procName = "UpdatePartitionedMP";
                expectedTables = 5;
                break;
            case REPLICATED:
                procName = "UpdateReplicatedMP";
                expectedTables = 5;
                break;
            case HYBRID:
                procName = "UpdateBothMP";
                expectedTables = 5;
                break;
            case ADHOC_MP:
                procName = "UpdateReplicatedMPInProcAdHoc";
                expectedTables = 5;
                break;
            }

            byte[] payload = m_processor.generateForStore().getStoreValue();

            ClientResponse response;
            try {
                response = m_client.callProcedure(procName,
                        m_cid,
                        m_nextRid,
                        payload,
                        shouldRollback);
            } catch (Exception e) {
                if (shouldRollback == 0) {
                    log.warn("ClientThread threw after " + m_txnsRun.get() +
                            " calls while calling procedure: " + procName +
                            " with args: cid: " + m_cid + ", nextRid: " + m_nextRid +
                            ", payload: " + payload +
                            ", shouldRollback: " + shouldRollback);
                }
                throw e;
            }

            // fake a proc call exception if we think one should be thrown
            if (response.getStatus() != ClientResponse.SUCCESS) {
                throw new UserProcCallException(response);
            }

            VoltTable[] results = response.getResults();

            m_txnsRun.incrementAndGet();

            if (results.length != expectedTables) {
                hardStop(String.format(
                        "Client cid %d procedure %s returned %d results instead of %d",
                        m_cid, procName, results.length, expectedTables), response);
            }
            VoltTable data = results[3];
            try {
                UpdateBaseProc.validateCIDData(data, "ClientThread:" + m_cid);
            }
            catch (VoltAbortException vae) {
                log.error("validateCIDData failed on: " + procName + ", shouldRollback: " +
                        shouldRollback + " data: " + data);
                throw vae;
            }
        }
        finally {
            // ensure rid is incremented (if not rolled back intentionally)
            if (shouldRollback == 0) {
                m_nextRid++;
            }
        }
    }

    void shutdown() {
        m_shouldContinue.set(false);
        this.interrupt();
    }

    void handleException(ClientResponseImpl cri, Exception e) {
        // this is not an error
        if ((cri.getStatus() == ClientResponse.USER_ABORT) &&
                cri.getStatusString().contains("EXPECTED ROLLBACK")) {
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

            // take a breather to avoid slamming the log (stay paused if no connections)
            do {
                try { Thread.sleep(3000); } catch (Exception e2) {} // sleep for 3s
                // bail on wakeup if we're supposed to bail
                if (!m_shouldContinue.get()) {
                    return;
                }
            }
            while (m_client.getConnectedHostList().size() == 0);

        }
    }

    @Override
    public void run() {
        while (m_shouldContinue.get()) {
            try {
                m_permits.acquire();
                runOne();
            }
            catch (NoConnectionsException e) {
                log.error("ClientThread got NoConnectionsException on proc call. Will sleep.");
                // take a breather to avoid slamming the log (stay paused if no connections)
                do {
                    try { Thread.sleep(3000); } catch (Exception e2) {} // sleep for 3s
                    // bail on wakeup if we're supposed to bail
                    if (!m_shouldContinue.get()) {
                        return;
                    }
                }
                while (m_client.getConnectedHostList().size() == 0);
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
            catch (Exception e) {
                hardStop("ClientThread had a non proc-call exception", e);
            }
        }
    }
}
