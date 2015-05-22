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

package org.voltdb;

import java.util.Map;
import java.util.Set;

import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google_voltpatches.common.util.concurrent.Futures;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

public class DummyCommandLog implements CommandLog {
    @Override
    public void init(CatalogContext context, long txnId, int partitionCount,
                     String affinity, Map<Integer, Long> perPartitionTxnId) {}

    @Override
    public boolean needsInitialization() {
        return false;
    }

    @Override
    public void shutdown() throws InterruptedException {}

    @Override
    public void initForRejoin(CatalogContext context, long txnId, int partitionCount,
                              boolean isRejoin, String affinity,
                              Map<Integer, Long> perPartitionTxnId) {}

    @Override
    public ListenableFuture<Object> log(
            Iv2InitiateTaskMessage message,
            long spHandle,
            int[] involvedPartitions,
            DurabilityListener l,
            Object handle) {
        return Futures.immediateFuture(null);
    }

    @Override
    public void logIv2Fault(long writerHSId, Set<Long> survivorHSId,
            int partitionId, long spHandle) {
    }

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
        rowValues[columnNameToIndex.get(CommandLogStats.StatName.LOANED_SEGMENT_NUM.name())] = 0;
        rowValues[columnNameToIndex.get(CommandLogStats.StatName.SEGMENT_NUM.name())] = 0;
        rowValues[columnNameToIndex.get(CommandLogStats.StatName.FSYNC_INTERVAL.name())] = 0;
    }

}
