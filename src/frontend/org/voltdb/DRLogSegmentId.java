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

package org.voltdb;

import java.io.Serializable;

import org.voltdb.iv2.UniqueIdGenerator;

import com.google_voltpatches.common.base.Preconditions;

public class DRLogSegmentId implements Serializable {
    private static final long serialVersionUID = 2540289527683570995L;

    public static final short MAX_CLUSTER_ID = (1 << 7) - 1;
    public static final long MAX_SEQUENCE_NUMBER = (1L << 55) - 1L;

    public static final long UNINITIALIZED_SEQUENCE_NUMBER = -1L;

    public final long drId;
    public final long spUniqueId;
    public final long mpUniqueId;

    public DRLogSegmentId(long drId, long spUniqueId, long mpUniqueId) {
        this.drId = drId;
        this.spUniqueId = spUniqueId;
        this.mpUniqueId = mpUniqueId;
    }

    @Override
    public String toString() {
        return "[" + getSentinelOrSeqNumFromDRId(drId) + ", " +
                UniqueIdGenerator.toShortString(spUniqueId) + ", " +
                UniqueIdGenerator.toShortString(mpUniqueId) + "]";
    }

    @Override
    public boolean equals(Object obj){
        if (!(obj instanceof DRLogSegmentId)) {
            return false;
        }
        DRLogSegmentId otherLogInfo = (DRLogSegmentId) obj;
        return drId == otherLogInfo.drId && spUniqueId == otherLogInfo.spUniqueId && mpUniqueId == otherLogInfo.mpUniqueId;
    }

    public static long makeDRIdFromComponents(int clusterId, long sequenceNumber) {
        Preconditions.checkArgument(clusterId <= MAX_CLUSTER_ID);
        Preconditions.checkArgument(sequenceNumber >= 0);
        Preconditions.checkArgument(sequenceNumber <= MAX_SEQUENCE_NUMBER);
        return ((long)clusterId << 55) | sequenceNumber;
    }

    public static long makeEmptyDRId(int clusterId) {
        Preconditions.checkArgument(clusterId <= MAX_CLUSTER_ID);
        return ((long)1 << 63) | ((long)clusterId << 55) | MAX_SEQUENCE_NUMBER;
    }

    public static long makeInitialAckDRId(int clusterId) {
        return makeDRIdFromComponents(clusterId, 0L) - 1L;
    }

    public static boolean isMaxDrIdForCluster(long drId, byte clusterId) {
        return (clusterId == getClusterIdFromDRId(drId) &&
                MAX_SEQUENCE_NUMBER == getSequenceNumberFromDRId(drId));
    }

    /*
     * Empty DR Id is used as a sentinel
     * 1) in EndOfSnapshotBuffer to indicate the partition doesn't have any snapshot buffer,
     * 2) in registerLastAckedSegmentId() when the partition has no tracker to recover.
     *
     * Empty DR Id has the highest significant bit been set, but there is one exception that
     * if all bits are set then it belongs to cluster 0's Initial Ack DR Id, not Empty DR Id
     * for cluster 255.
     */
    public static boolean isEmptyDRId (long drId) {
        return ((drId >>> 63) == 1L) && (drId != -1);
    }

    // Constructing InitialAckDRId changes the clusterId in the DrId.
    // To correctly say if a DRId is an initial ack id, we need the clusterId as well.
    public static boolean isInitialAckDRId(long drId, byte clusterId) {
        if (drId == -1 && clusterId == 0) {
            return true;
        }
        drId += 1;
        return (clusterId == getClusterIdFromDRId(drId) && (getSequenceNumberFromDRId(drId) == 0));
    }

    /*
     * Initial Ack DR Id is used as initial value for DR Idempotency filter
     */
    public static boolean isInitialAckDRId(long drId) {
        if (drId == -1) return true;
        int clusterId = getClusterIdFromDRId(drId);
        if (clusterId >= 0 && clusterId <= MAX_CLUSTER_ID) {
            return ((drId >>> 63) != 1L) && (getSequenceNumberFromDRId(drId) == MAX_SEQUENCE_NUMBER);
        }
        return false;
    }

    public static boolean seqIsBeforeZero(long drId, byte clusterId) {
        if (drId == -1 && clusterId == 0) {
            return true;
        } else {
            return getClusterIdFromDRId(drId) == (clusterId - 1);
        }
    }

    public static int getClusterIdFromDRId(long drId) {
        return (int)((drId >> 55) & MAX_CLUSTER_ID);
    }

    public static long getSequenceNumberFromDRId(long drId) {
        return drId & MAX_SEQUENCE_NUMBER;
    }

    public static long getSentinelOrSeqNumFromDRId(long drId) {
        return (drId < 0 ? drId : (drId & MAX_SEQUENCE_NUMBER));
    }

    public static String getDebugStringFromDRId(long drId) {
        if (drId == Long.MAX_VALUE) {
            return "QUEUE EMPTY";
        }
        if (isEmptyDRId(drId)) {
            return String.format("%d:Empty DR ID", getClusterIdFromDRId(drId));
        }
        if (isInitialAckDRId(drId)) {
            if (drId == -1) {
                return "0:Init Ack ID";
            }
            return String.format("%d:Init Ack ID", getClusterIdFromDRId(drId) + 1);
        }
        return String.format("%d:%d", getClusterIdFromDRId(drId), getSequenceNumberFromDRId(drId));
    }

    public static class MutableBinaryLogInfo {
        public long drId;
        public long spUniqueId;
        public long mpUniqueId;

        public MutableBinaryLogInfo() {
            drId = Long.MIN_VALUE;
            spUniqueId = 0;
            mpUniqueId = 0;
        }

        public void reset() {
            drId = Long.MIN_VALUE;
            spUniqueId = 0;
            mpUniqueId = 0;
        }

        public DRLogSegmentId toImmutable() {
            return new DRLogSegmentId(drId, spUniqueId, mpUniqueId);
        }

        @Override
        public String toString() {
            return "[" + getSentinelOrSeqNumFromDRId(drId) + ", " +
                    UniqueIdGenerator.toShortString(spUniqueId) + ", " +
                    UniqueIdGenerator.toShortString(mpUniqueId) + "]";
        }
    }
}
