/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.InitiateResponseMessage;

/**
 * Track responses from each partition. This should be subsumed
 * by proper response tracking for the usual replication case?
 */
public class DuplicateCounter
{
    static final int MISMATCH = 0;
    static final int DONE = 1;
    static final int WAITING = 2;
    static final int ABORT = 3;

    protected static final VoltLogger tmLog = new VoltLogger("TM");

    static final int[] ZERO_HASHES = new int[] { 0, 0, 0 };

    final long m_destinationId;
    int[] m_responseHashes = null;
    protected VoltMessage m_lastResponse = null;
    protected VoltTable m_lastResultTables[] = null;
    // if any response shows the transaction aborted
    boolean m_txnSucceed = false;
    final List<Long> m_expectedHSIds;
    final long m_txnId;
    private final String m_storedProcName;

    DuplicateCounter(
            long destinationHSId,
            long realTxnId,
            List<Long> expectedHSIds, String procName)    {
        m_destinationId = destinationHSId;
        m_txnId = realTxnId;
        m_expectedHSIds = new ArrayList<Long>(expectedHSIds);
        m_storedProcName = procName;
    }

    long getTxnId()
    {
        return m_txnId;
    }

    /**
     * Return stored procedure name for the transaction.
     *
     * @return
     */
    public String getStoredProcedureName() {
        return m_storedProcName;
    }

    int updateReplicas(List<Long> replicas) {
        Set<Long> newSet = new HashSet<Long>(replicas);
        m_expectedHSIds.retainAll(newSet);
        if (m_expectedHSIds.size() == 0) {
            return DONE;
        }
        else {
            return WAITING;
        }
    }

    protected int checkCommon(int[] hashes, boolean rejoining, VoltTable resultTables[], VoltMessage message, boolean txnSucceed)
    {
        if (!rejoining) {
            if (m_responseHashes == null) {
                m_responseHashes = hashes;
                m_txnSucceed = txnSucceed;
            }
            else if (!DeterminismHash.compareHashes(m_responseHashes, hashes)) {
                tmLog.fatal("Stored procedure " + getStoredProcedureName()
                        + " generated different SQL queries at different partitions."
                        + " Shutting down to preserve data integrity.");
                String msg = String.format("HASH MISMATCH COMPARING: %d to %d\n"
                        + "PREV MESSAGE: %s\n"
                        + "CURR MESSAGE: %s\n",
                        hashes[0], m_responseHashes[0],
                        m_lastResponse.toString(), message.toString());
                tmLog.error(msg);
                return MISMATCH;
            }
            else if (m_txnSucceed != txnSucceed) {
                tmLog.fatal("Stored procedure " + getStoredProcedureName()
                        + " succeeded on one partition but failed on another partition."
                        + " Shutting down to preserve data integrity.");
                String msg = String.format("PARTIAL ROLLBACK/ABORT COMPARING: %d to %d\n"
                        + "PREV MESSAGE: %s\n"
                        + "CURR MESSAGE: %s\n",
                        hashes[0], m_responseHashes[0],
                        m_lastResponse.toString(), message.toString());
                tmLog.error(msg);
                return ABORT;
            }
            m_lastResponse = message;
            m_lastResultTables = resultTables;
        }

        /*
         * Set m_lastResponse to a response once at least. It's possible
         * that all responses are dummy responses in the case of elastic
         * join. So only setting m_lastResponse when the message is not
         * a dummy will leave the variable as null.
         */
        if (m_lastResponse == null) {
            m_lastResponse = message;
            m_lastResultTables = resultTables;
        }

        m_expectedHSIds.remove(message.m_sourceHSId);
        if (m_expectedHSIds.size() == 0) {
            return DONE;
        }
        else {
            return WAITING;
        }
    }

    int offer(InitiateResponseMessage message)
    {
        ClientResponseImpl r = message.getClientResponseData();
        // get the hash of sql run
        int[] hashes = r.getHashes();

        boolean txnAbort = true;
        if (ClientResponseImpl.isTransactionallySuccessful(message.getClientResponseData().getStatus())) {
            txnAbort = false;
        }

        return checkCommon(hashes, message.isRecovering(), r.getResults(), message, txnAbort);
    }

    int offer(FragmentResponseMessage message)
    {
        // No check on fragment message
        return checkCommon(ZERO_HASHES, message.isRecovering(), null, message, false);
    }

    VoltMessage getLastResponse()
    {
        return m_lastResponse;
    }

    @Override
    public String toString()
    {
        String msg = String.format("DuplicateCounter: txnId: %s, outstanding HSIds: %s\n", m_txnId,
               CoreUtils.hsIdCollectionToString(m_expectedHSIds));
        return msg;
    }
}
