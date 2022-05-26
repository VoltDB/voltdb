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

public class TupleStreamStateInfo {
    public final DRLogSegmentId partitionInfo;
    public final boolean containsReplicatedStreamInfo;
    public final DRLogSegmentId replicatedInfo;
    public final int drVersion;

    public TupleStreamStateInfo(DRLogSegmentId partitionInfo, int drVersion) {
        this(partitionInfo, new DRLogSegmentId(Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE), drVersion);
    }

    public TupleStreamStateInfo(DRLogSegmentId partitionInfo,
            DRLogSegmentId replicatedInfo, int drVersion) {
        this.partitionInfo = partitionInfo;
        this.replicatedInfo = replicatedInfo;
        this.containsReplicatedStreamInfo = (this.replicatedInfo.drId > Long.MIN_VALUE);
        this.drVersion = drVersion;
    }
}
