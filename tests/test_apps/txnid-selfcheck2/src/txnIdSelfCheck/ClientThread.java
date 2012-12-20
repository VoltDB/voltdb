/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import txnIdSelfCheck.procedures.UpdateBaseProc;

public class ClientThread extends Thread {

    static VoltLogger log = new VoltLogger("HOST");

    static enum Type {
        PARTITIONED_SP, PARTITIONED_MP, REPLICATED, HYBRID;

        /**
         * Use modulo so the same CID will run the same code
         * across client process lifetimes.
         */
        static Type typeFromId(int id) {
            if (id % 10 == 0) return PARTITIONED_MP; // 20%
            if (id % 10 == 1) return PARTITIONED_MP;
            if (id % 10 == 2) return REPLICATED;     // 20%
            if (id % 10 == 3) return REPLICATED;
            if (id % 10 == 4) return HYBRID;         // 20%
            if (id % 10 == 5) return HYBRID;
            return PARTITIONED_SP;                   // 40%
        }
    }

    final Client m_client;
    final byte m_cid;
    long m_nextRid;
    final Type m_type;
    final PayloadProcessor m_processor;
    final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
    final AtomicLong m_txnsRun;

    ClientThread(byte cid, AtomicLong txnsRun, Client client, PayloadProcessor processor) throws Exception {
        m_type = Type.typeFromId(cid);
        m_cid = cid;
        m_client = client;
        m_processor = processor;
        m_txnsRun = txnsRun;

        String sql1 = String.format("select * from partitioned where cid = %d order by rid desc limit 1", cid);
        String sql2 = String.format("select * from replicated  where cid = %d order by rid desc limit 1", cid);
        VoltTable t1 = client.callProcedure("@AdHoc", sql1).getResults()[0];
        VoltTable t2 = client.callProcedure("@AdHoc", sql2).getResults()[0];

        long pNextRid = (t1.getRowCount() == 0) ? 1 : t1.fetchRow(0).getLong("rid") + 1;
        long rNextRid = (t2.getRowCount() == 0) ? 1 : t2.fetchRow(0).getLong("rid") + 1;
        m_nextRid = pNextRid > rNextRid ? pNextRid : rNextRid; // max
    }

    void runOne() throws Exception {
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
        }

        byte[] payload = m_processor.generateForStore().getStoreValue();

        VoltTable[] results = m_client.callProcedure(procName,
                m_cid,
                m_nextRid,
                payload).getResults();
        m_txnsRun.incrementAndGet();

        assert(results.length == 3);
        VoltTable data = results[2];
        UpdateBaseProc.validateCIDData(data, "ClientThread:" + m_cid);

        m_nextRid++;
    }

    void shutdown() {
        m_shouldContinue.set(false);
    }

    @Override
    public void run() {
        while (m_shouldContinue.get()) {
            try {
                runOne();
            }
            catch (ProcCallException e) {
                ClientResponseImpl cri = (ClientResponseImpl) e.getClientResponse();
                if ((cri.getStatus() == ClientResponse.GRACEFUL_FAILURE) ||
                        (cri.getStatus() == ClientResponse.USER_ABORT)) {
                    log.error("ClientThread had a proc-call exception that indicated bad data", e);
                    log.error(cri.toJSONString(), e);
                    System.exit(-1);
                }
                else {
                    // other proc call exceptions are logged, but don't stop the thread
                    log.warn("ClientThread had a proc-call exception that didn't indicate bad data", e);
                    log.warn(cri.toJSONString());
                    // take a breather to avoid slamming the log
                    try { Thread.sleep(3000); } catch (InterruptedException e1) {}
                }
            }
            catch (Exception e) {
                log.error("ClientThread had a non proc-call exception", e);
                System.exit(-1);
            }
        }
    }
}
