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

public class TupleStreamStateInfo {
    public final long partitionSequenceNumber;
    public final long partitionUniqueId;
    public final boolean containsReplicatedStreamInfo;
    public final long replicatedSequenceNumber;
    public final long replicatedUniqueId;

    public TupleStreamStateInfo(long partitionSequenceNumber,
                                long partitionUniqueId) {
        this(partitionSequenceNumber, partitionUniqueId, Long.MIN_VALUE, Long.MIN_VALUE);
    }

    public TupleStreamStateInfo(long partitionSequenceNumber,
                                long partitionUniqueId,
                                long replicatedSequenceNumber,
                                long replicatedUniqueId) {
        this.partitionSequenceNumber = partitionSequenceNumber;
        this.partitionUniqueId = partitionUniqueId;
        this.replicatedSequenceNumber = replicatedSequenceNumber;
        this.replicatedUniqueId = replicatedUniqueId;
        this.containsReplicatedStreamInfo = (this.replicatedSequenceNumber > Long.MIN_VALUE);
    }
}
