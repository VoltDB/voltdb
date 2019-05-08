/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb;

import java.util.Map;
import java.util.Set;

import org.voltdb.iv2.TransactionTask;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

public class DummyCommandLog implements CommandLog {
    @Override
    public void init(int logSize, long txnId, String affinity, Map<Integer, Long> perPartitionTxnId) {}

    @Override
    public boolean needsInitialization() {
        return false;
    }

    @Override
    public void shutdown() throws InterruptedException {}

    @Override
    public void initForRejoin(int logSize, long txnId, boolean isRejoin, String affinity,
            Map<Integer, Long> perPartitionTxnId) {}

    @Override
    public ListenableFuture<Object> log(
            Iv2InitiateTaskMessage message,
            long spHandle,
            int[] involvedPartitions,
            DurabilityListener l,
            TransactionTask handle) {
        return null;
    }

    @Override
    public SettableFuture<Boolean> logIv2Fault(long writerHSId, Set<Long> survivorHSId,
            int partitionId, long spHandle) {
        return null;
    }

    @Override
    public void initializeLastDurableUniqueId(DurabilityListener listener, long uniqueId) {}

    @Override
    public boolean isEnabled()
    {
        // No real command log, obviously not enabled
        return false;
    }

    @Override
    public void requestTruncationSnapshot(final boolean queueIfPending)
    {
        // Don't perform truncation snapshot if Command Logging is disabled
        return;
    }

    @Override
    public void populateCommandLogStats(Map<String, Integer> columnNameToIndex,
            Object[] rowValues) {
        rowValues[columnNameToIndex.get(CommandLogStats.StatName.OUTSTANDING_BYTES.name())] = 0;
        rowValues[columnNameToIndex.get(CommandLogStats.StatName.OUTSTANDING_TXNS.name())] = 0;
        rowValues[columnNameToIndex.get(CommandLogStats.StatName.IN_USE_SEGMENT_COUNT.name())] = 0;
        rowValues[columnNameToIndex.get(CommandLogStats.StatName.SEGMENT_COUNT.name())] = 0;
        rowValues[columnNameToIndex.get(CommandLogStats.StatName.FSYNC_INTERVAL.name())] = 0;
    }

    @Override
    public boolean isSynchronous() {
        return false;
    }

    @Override
    public boolean canOfferTask()
    {
        return true;
    }

    @Override
    public void registerDurabilityListener(DurabilityListener durabilityListener) {
    }
}
