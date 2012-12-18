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
package org.voltdb;

import java.util.ArrayDeque;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public interface CommandLog {
    /**
     *
     * @param context
     * @param txnId
     *            The txnId of the truncation snapshot at the end of restore, or
     *            Long.MIN if there was none.
     */
    public abstract void init(
            CatalogContext context,
            long txnId,
            long perPartitionTxnId[],
            String coreBinding);

    /**
    *
    * @param txnId
    *            The txnId of the truncation snapshot at the end of restore, or
    *            Long.MIN if there was none.
    */
    public abstract void initForRejoin(
            CatalogContext context,
            long txnId,
            long perPartitionTxnId[],
            boolean isRejoin,
            String coreBinding);

    public abstract boolean needsInitialization();

    public abstract void log(InitiateTaskMessage message);

    /*
     * Returns a boolean indicating whether synchronous command logging is enabled.
     *
     * The listener is will be provided with the handle once the message is durable.
     */
    public abstract boolean log(
            Iv2InitiateTaskMessage message,
            long spHandle,
            DurabilityListener listener,
            Object durabilityHandle);

    public abstract void shutdown() throws InterruptedException;

    /**
     * @param failedInitiators
     * @param faultedTxns
     * @return null if the logger is not initialized
     */
    public abstract Semaphore logFault(Set<Long> failedInitiators,
                                       Set<Long> faultedTxns);

    /**
     * IV2-only method.  Write this Iv2FaultLogEntry to the fault log portion of the command log
     */
    public abstract void logIv2Fault(long writerHSId, Set<Long> survivorHSId,
            int partitionId, long spHandle);

    public abstract void logHeartbeat(final long txnId);

    public abstract long getFaultSequenceNumber();

    public interface DurabilityListener {
        public void onDurability(ArrayDeque<Object> durableThings);
    }
}
