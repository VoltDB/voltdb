/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.concurrent.Future;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;

// Some comments on threading and organization.
//   start() returns a future. Block on this future to get the final answer.
//
//   deliver() runs in the initiator mailbox deliver() context and triggers
//   all repair work.
//
//   it is important that repair work happens with the deliver lock held
//   and that updatereplicas also holds this lock -- replica failure during
//   repair must happen unambiguously before or after each local repair action.
//
//   A RepairAlgo can only be cancelled by initiator mailbox while the deliver
//   lock is held. Repair work must check for cancellation before producing
//   repair actions to the mailbox.
//
//   Note that a RepairAlgo can not prevent messages being delivered post
//   cancellation.  RepairLog requests therefore use a requestId to
//   disambiguate responses for cancelled requests that are filtering in late.


/**
 * A RepairAlgo encapsulates the actions required to converge state across a
 * leader and a set of followers (either SP replicas or MPI/SP leaders).
 */
public interface RepairAlgo
{
    static final VoltLogger repairLogger = new VoltLogger("REPAIR");

    public static class RepairResult
    {
        public final long m_txnId;
        public final long m_repairTruncationHandle;
        RepairResult(long txnId, long repairTruncationHandle) {
            m_txnId = txnId;
            m_repairTruncationHandle = repairTruncationHandle;
        }
    }

    /**
     * Start a new RepairAlgo. Returns a future that is done when the
     * leadership has been fully assumed and all surviving replicas have been
     * repaired.
     *
     * The return value is the largest seen txnid and largest seen unique id
     */
    public Future<RepairResult> start();

    public boolean cancel();

    /** Process a new repair log response */
    public void deliver(VoltMessage message);

    /** Was the future returned by start() cancelled*/
    public boolean isCancelled();
}
