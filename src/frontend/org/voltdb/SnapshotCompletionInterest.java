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

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.voltcore.utils.Pair;

public interface SnapshotCompletionInterest {

    public static class SnapshotCompletionEvent {
        public final String nonce;
        public final long multipartTxnId;
        public final long partitionTxnIds[];
        public final boolean truncationSnapshot;
        public final String requestId;
        public final Map<String, Map<Integer, Pair<Long,Long>>> exportSequenceNumbers;

        public SnapshotCompletionEvent(
                String nonce,
                final long multipartTxnId,
                final long partitionTxnIds[],
                final boolean truncationSnapshot,
                final String requestId,
                final Map<String, Map<Integer, Pair<Long,Long>>> exportSequenceNumbers) {
            this.nonce = nonce;
            this.multipartTxnId = multipartTxnId;
            this.partitionTxnIds = partitionTxnIds;
            this.truncationSnapshot = truncationSnapshot;
            this.requestId = requestId;
            this.exportSequenceNumbers = exportSequenceNumbers;
        }
    }

    public CountDownLatch snapshotCompleted(
            SnapshotCompletionEvent event);
}
