/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

public class DRProducerNodeStats {
    public static final DRProducerNodeStats DISABLED_NODE_STATS =
        new DRProducerNodeStats((short) -1, DRRoleStats.State.DISABLED, "",
                                -1, -1, 0);
    public static final DRProducerNodeStats PENDING_NODE_STATS =
        new DRProducerNodeStats((short) -1, DRRoleStats.State.PENDING, "",
                                -1, -1, 0);

    public final short consumerClusterId;
    public final DRRoleStats.State state;
    public String syncSnapshotState;
    public final long rowsInSyncSnapshot;
    public final long rowsAckedForSyncSnapshot;
    public final long queueDepth;

    public DRProducerNodeStats(
                              short consumerClusterId,
                              DRRoleStats.State state,
                              String syncSnapshotState,
                              long rowsInSyncSnapshot,
                              long rowsAckedForSyncSnapshot,
                              long queueDepth) {
        this.consumerClusterId = consumerClusterId;
        this.state = state;
        this.syncSnapshotState = syncSnapshotState;
        this.rowsInSyncSnapshot = rowsInSyncSnapshot;
        this.rowsAckedForSyncSnapshot = rowsAckedForSyncSnapshot;
        this.queueDepth = queueDepth;
    }
}
