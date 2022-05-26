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

import com.google_voltpatches.common.base.Preconditions;

public class DRSequenceId implements Serializable, Comparable<DRSequenceId> {
    private static final long serialVersionUID = 1192186486936556377L;
    public static final short MAX_CLUSTER_ID = (1 << 8) - 1;
    public static final long MAX_SEQUENCE_NUMBER = (1L << 55) - 1L;

    public static final long UNINITIALIZED_SEQUENCE_NUMBER = -1L;

    public final long rawDrId;

    public DRSequenceId(long drId) {
        this.rawDrId = drId;
    }

    public DRSequenceId(DRSequenceId drSeqId) {
        this.rawDrId = drSeqId.rawDrId;
    }

    @Override
    public String toString() {
        return Long.toString(getSentinelOrSeqNumFromDRId(this));
    }

    public boolean equals(DRSequenceId drId){
        return rawDrId == drId.rawDrId;
    }

    public static DRSequenceId makeDRIdFromComponents(int clusterId, long sequenceNumber) {
        Preconditions.checkArgument(clusterId <= MAX_CLUSTER_ID);
        Preconditions.checkArgument(sequenceNumber >= 0);
        Preconditions.checkArgument(sequenceNumber <= MAX_SEQUENCE_NUMBER);
        return new DRSequenceId(((long)clusterId << 55) | sequenceNumber);
    }

    public static DRSequenceId makeEmptyDRId(int clusterId) {
        Preconditions.checkArgument(clusterId <= MAX_CLUSTER_ID);
        return new DRSequenceId(((long)1 << 63) | ((long)clusterId << 55) | MAX_SEQUENCE_NUMBER);
    }

    public static DRSequenceId makeInitialAckDRId(int clusterId) {
        return new DRSequenceId(makeDRIdFromComponents(clusterId, 0L).rawDrId - 1L);
    }

    public static boolean isEmptyDRId (DRSequenceId drId) {
        return (drId.rawDrId >>> 63) == 1L;
    }

    public static boolean seqIsBeforeZero(DRSequenceId drId) {
        return ((-1L & MAX_SEQUENCE_NUMBER) == getSequenceNumberFromDRId(drId));
    }

    public static int getClusterIdFromDRId(DRSequenceId drId) {
        return (int)((drId.rawDrId >> 55) & MAX_CLUSTER_ID);
    }

    public static long getSequenceNumberFromDRId(DRSequenceId drId) {
        return drId.rawDrId & MAX_SEQUENCE_NUMBER;
    }

    public static long getSentinelOrSeqNumFromDRId(DRSequenceId drId) {
        return (drId.rawDrId < 0 ? drId.rawDrId : (drId.rawDrId & MAX_SEQUENCE_NUMBER));
    }

    @Override
    public int compareTo(DRSequenceId drId) {
        return Long.compare(rawDrId, drId.rawDrId);
    }
}
