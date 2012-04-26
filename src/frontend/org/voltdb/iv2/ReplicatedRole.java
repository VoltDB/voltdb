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

import java.util.concurrent.LinkedBlockingDeque;
import org.voltcore.utils.Pair;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

/**
 * Replicated initiator role. It accepts transactions with pre-assigned
 * transaction IDs from the primary initiator, then it immediately execute them.
 * Executed transactions are stored until the primary says it's safe to discard.
 * Responses are forwarded back to the primary immediately on finish.
 */
public class ReplicatedRole implements InitiatorRole {
    private final long hsid;

    private volatile long truncationTxnId = -1;
    private volatile long lastExecutedTxnId = -1;
    private volatile long lastSeenTxnId = -1;

    private LinkedBlockingDeque<InitiateTaskMessage> outstanding =
            new LinkedBlockingDeque<InitiateTaskMessage>();
    private LinkedBlockingDeque<InitiateTaskMessage> inflight =
            new LinkedBlockingDeque<InitiateTaskMessage>();

    /**
     * @param hsid The HSId of the initiator that owns this.
     */
    public ReplicatedRole(long hsid)
    {
        this.hsid = hsid;
    }

    @Override
    public void offerInitiateTask(InitiateTaskMessage message)
    {
//        long txnId = message.getTransactionId();
//        if (txnId != lastSeenTxnId + 1) {
//            throw new RuntimeException("Transaction missing, expecting transaction " +
//                                       (lastSeenTxnId + 1) + ", but got " + txnId);
//        }
//        lastSeenTxnId = message.getTransactionId();
//        message.setInitiatorHSId(hsid);
//        outstanding.offer(message);
//
//        long truncationTxnId = message.getTruncationTxnId();
//        if (truncationTxnId != -1) {
//            truncate(truncationTxnId);
//        }
        throw new RuntimeException("IZZY SAYS TO FIX ME, DUMMY!");
    }

    @Override
    public Pair<Long, InitiateResponseMessage> offerResponse(InitiateResponseMessage message)
    {
        return Pair.of(message.getCoordinatorHSId(), message);
    }

    @Override
    public InitiateTaskMessage poll()
    {
//        InitiateTaskMessage txn = outstanding.poll();
//        if (txn != null) {
//            lastExecutedTxnId = txn.getTransactionId();
//            inflight.offer(txn);
//        }
//        return txn;
        throw new RuntimeException("IZZY SAYS TO FIX ME, DUMMY");
    }

    long getOldestInFlightTxnId()
    {
//        InitiateTaskMessage oldest = inflight.peek();
//        if (oldest != null) {
//            return oldest.getTransactionId();
//        }
//        return -1;
        throw new RuntimeException("IZZY SAYS TO FIX ME, DUMMY");
    }

    /**
     * Remove any transaction responses that are before the truncation point.
     * Replica keeps responses for all transactions until the primary initiator
     * has sent the response back to the client interface, at which time the
     * primary will notify replicas to truncate at that transaction.
     *
     * @param txnId truncation point
     */
    private void truncate(long txnId)
    {
//        truncationTxnId = txnId;
//        long lastExecuted = lastExecutedTxnId;
//        if (truncationTxnId <= lastExecuted) {
//            /*
//             * scan from beginning of executed queue and remove any transaction
//             * that's before the truncation point
//             */
//            InitiateTaskMessage txn;
//            while ((txn = inflight.peek()) != null) {
//                if (txn.getTransactionId() <= truncationTxnId) {
//                    inflight.remove();
//                }
//                else {
//                    break;
//                }
//            }
//        }
        throw new RuntimeException("IZZY SAYS TO FIX ME, DUMMY");
    }
}

