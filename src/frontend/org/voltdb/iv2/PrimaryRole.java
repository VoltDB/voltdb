/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import org.voltcore.utils.Pair;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

/**
 * Primary initiator role. It assigns transaction IDs to new transactions, then
 * it will execute them. For executed transactions, it waits for responses from
 * all replicas before forwarding the response to the client interface.
 */
public class PrimaryRole implements InitiatorRole {
    // caller of offerInitiateTask() synchronizes on this, so it's okay
    private long txnIdSequence = 0;
    volatile long lastRespondedTxnId = -1;
    private volatile long[] replicas = null;

    @Override
    public void offerInitiateTask(InitiateTaskMessage message)
    {
//        message.setTransactionId(txnIdSequence++);
//        message.setTruncationTxnId(lastRespondedTxnId);
//        int expectedResponses = replicas.length + 1; // plus the leader
//        InFlightTxnState state = new InFlightTxnState(message, expectedResponses);
//        pendingResponses.put(message.getTransactionId(), state);
//
//        ProcedureTask task = new ProcedureTask(a, b, c);
//        m_scheduler.offer(task);
        throw new RuntimeException("IZZY SAYS TO FIX ME, DUMMY!");
    }

    @Override
    public Pair<Long, InitiateResponseMessage> offerResponse(InitiateResponseMessage message)
    {
//        InFlightTxnState state = pendingResponses.get(message.getTxnId());
//        if (state == null) {
//            throw new RuntimeException("Response for transaction " + message.getTxnId() +
//                                       " released before all replicas responded");
//        }
//
//        if (state.addResponse(message)) {
//            pendingResponses.remove(message.getTxnId());
//            if (message.getTxnId() != lastRespondedTxnId + 1) {
//                throw new RuntimeException("Transaction missing, expecting transaction " +
//                                           (lastRespondedTxnId + 1) + ", but got " +
//                                           message.getTxnId());
//            }
//            lastRespondedTxnId = message.getTxnId();
//            return Pair.of(message.getClientInterfaceHSId(), message);
//        }
//        return null;
        throw new RuntimeException("IZZY SAYS TO FIX ME, DUMMY!");
    }

    @Override
    public InitiateTaskMessage poll()
    {
//        InitiateTaskMessage task = outstanding.peek();
//        if (task != null) {
//            outstanding.remove();
//        }
//        return task;
        return null;
    }

    /**
     * Set the HSIds of all the replicas of this partition.
     * @param replicas
     */
    public void setReplicas(long[] replicas)
    {
        this.replicas = replicas;
    }
}

