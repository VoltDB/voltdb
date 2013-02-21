/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.VoltProcedure.VoltAbortException;

import txnIdSelfCheck.procedures.UpdateBaseProc;

public class ClientThread extends Thread {

    static VoltLogger log = new VoltLogger("HOST");

    static enum Type {
        PARTITIONED_SP, PARTITIONED_MP, REPLICATED, HYBRID, ADHOC_MP;

        /**
         * Use modulo so the same CID will run the same code
         * across client process lifetimes.
         */
        static Type typeFromId(int id) {
            if (id % 10 == 0) return PARTITIONED_MP;               // 20%
            if (id % 10 == 1) return PARTITIONED_MP;
            if (id % 10 == 2) return REPLICATED;                   // 20%
            if (id % 10 == 3) return REPLICATED;
            if (id % 10 == 4) return HYBRID;                       // 20%
            if (id % 10 == 5) return HYBRID;
            if ((id % 10 == 6) && (id % 20 != 6)) return ADHOC_MP; // 5%
            return PARTITIONED_SP;                                 // 35%
        }
    }

    final Client m_client;
    final byte m_cid;
    long m_nextRid;
    final Type m_type;
    final TxnId2PayloadProcessor m_processor;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final AtomicLong m_txnsRun;
    final Random m_random = new Random();
    final Semaphore m_permits;

    ClientThread(byte cid, AtomicLong txnsRun, Client client, TxnId2PayloadProcessor processor, Semaphore permits) throws Exception {
        setName("ClientThread(CID=" + String.valueOf(cid) + ")");

        m_type = Type.typeFromId(cid);
        m_cid = cid;
        m_client = client;
        m_processor = processor;
        m_txnsRun = txnsRun;
        m_permits = permits;

        String sql1 = String.format("select * from partitioned where cid = %d order by rid desc limit 1", cid);
        String sql2 = String.format("select * from replicated  where cid = %d order by rid desc limit 1", cid);
        VoltTable t1 = client.callProcedure("@AdHoc", sql1).getResults()[0];
        VoltTable t2 = client.callProcedure("@AdHoc", sql2).getResults()[0];

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
            switch (m_type) {
            case PARTITIONED_SP:
                procName = "UpdatePartitionedSP";
                break;
            case PARTITIONED_MP:
                procName = "UpdatePartitionedMP";
                break;
            case REPLICATED:
                procName = "UpdateReplicatedMP";
                break;
            case HYBRID:
                procName = "UpdateBothMP";
                break;
            case ADHOC_MP:
                procName = "UpdateReplicatedMPInProcAdHoc";
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

            if (results.length != 3) {
                log.error(String.format(
                        "Client cid %d procedure %s returned %d results instead of 3",
                        m_cid, procName, results.length));
                log.error(((ClientResponseImpl) response).toJSONString());
                Benchmark.printJStack();
                System.exit(-1);
            }
            VoltTable data = results[2];
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
            log.error("ClientThread had a proc-call exception that indicated bad data", e);
            log.error(cri.toJSONString(), e);
            Benchmark.printJStack();
            System.exit(-1);
        }
        // other proc call exceptions are logged, but don't stop the thread
        else {
            log.warn("ClientThread had a proc-call exception that didn't indicate bad data", e);
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
            catch (Exception e) {
                log.error("ClientThread had a non proc-call exception", e);
                Benchmark.printJStack();
                System.exit(-1);
            }
        }
    }
}
