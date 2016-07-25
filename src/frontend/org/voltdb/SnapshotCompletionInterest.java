/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import java.util.concurrent.CountDownLatch;

import org.voltcore.utils.Pair;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;

public interface SnapshotCompletionInterest {

    public static class SnapshotCompletionEvent {
        public final String path;
        public final String nonce;
        // multipartTxnId is the txnId of the snapshot itself.
        // as well as the last snapshotted MP transaction.
        public final long multipartTxnId;
        public final Map<Integer, Long> partitionTxnIds;
        public final boolean truncationSnapshot;
        public final boolean didSucceed;
        public final String requestId;
        public final Map<String, Map<Integer, Pair<Long,Long>>> exportSequenceNumbers;
        public final Map<Integer, Long> drSequenceNumbers;
        public final Map<Integer, Map<Integer, Map<Integer, DRConsumerDrIdTracker>>> drMixedClusterSizeConsumerState;
        public final int drVersion;
        public final long clusterCreateTime;

        public SnapshotCompletionEvent(
                String path,
                SnapshotPathType stype,
                String nonce,
                final long multipartTxnId,
                final Map<Integer, Long> partitionTxnIds,
                final boolean truncationSnapshot,
                final boolean didSucceed,
                final String requestId,
                final Map<String, Map<Integer, Pair<Long,Long>>> exportSequenceNumbers,
                final Map<Integer, Long> drSequenceNumbers,
                final Map<Integer, Map<Integer, Map<Integer, DRConsumerDrIdTracker>>> drMixedClusterSizeConsumerState,
                final int drVersion,
                final long clusterCreateTime) {
            this.path = path;
            this.nonce = nonce;
            this.multipartTxnId = multipartTxnId;
            this.partitionTxnIds = partitionTxnIds;
            this.truncationSnapshot = truncationSnapshot;
            this.didSucceed = didSucceed;
            this.requestId = requestId;
            this.exportSequenceNumbers = exportSequenceNumbers;
            this.drSequenceNumbers = drSequenceNumbers;
            this.drMixedClusterSizeConsumerState = drMixedClusterSizeConsumerState;
            this.drVersion = drVersion;
            this.clusterCreateTime = clusterCreateTime;
        }

        // Factory method for simplified instances used in testing,
        // to avoid repeating this long series of dummy-valued initializers.
        public static SnapshotCompletionEvent newInstanceForTest(
                String path,
                SnapshotPathType stype,
                String nonce,
                long multipartTxnId,
                Map<Integer, Long> partitionTxnIds,
                boolean truncationSnapshot,
                int drVersion,
                long clusterCreateTime) {
            return new SnapshotCompletionEvent(
                    path, stype, nonce, multipartTxnId, partitionTxnIds, truncationSnapshot,
                    true, "", null, null, null, drVersion, clusterCreateTime);
        }
    }

    /**
     * Here's what I think the contract is. This method will be called on all finished snapshots,
     * regardless of whether or not the snapshot succeeded, as long as all nodes have done the
     * work. If node failures happened during the snapshot, this won't fire.
     *
     * To determine if the snapshot actually succeeded, check event.didSucceed.
     *
     * @param event
     * @return
     */
    public CountDownLatch snapshotCompleted(
            SnapshotCompletionEvent event);
}
