/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.dr2;

import java.util.Map;
import java.util.TreeMap;

import org.voltcore.utils.Pair;
import org.voltdb.DRProducerStatsBase.DRProducerClusterStatsBase.DRProducerCluster;
import org.voltdb.DRRoleStats;
import org.voltdb.VoltTable;

public class DRProducerClusterStats {
    public final short clusterId;
    public final short consumerClusterId;
    public final DRRoleStats.State state;
    public final byte lastErrorCode;

    private final static byte NO_FAILURE = 0;

    public DRProducerClusterStats(short clusterId,
                               short consumerClusterId,
                               DRRoleStats.State state,
                               byte lastFailure) {
        this.clusterId = clusterId;
        this.consumerClusterId = consumerClusterId;
        this.state = state;
        this.lastErrorCode = lastFailure;
    }

    /**
     * Aggregates DRPRODUCERCLUSTER statistics reported by multiple nodes into
     * a one per-remote-cluster row. The last failure column should be the first
     * non-zero value across entire cluster.
     * The state column may defer slightly and it uses the same logical
     * AND-ish operation to combine the states.
     *
     * This method modifies the VoltTable in place.
     * @param stats Statistics from all cluster nodes. This will be modified in
     *              place. Cannot be null.
     * @return The same VoltTable as in the parameter.
     */
    public static VoltTable aggregateStats(VoltTable stats) {
        stats.resetRowPosition();
        if (stats.getRowCount() == 0) {
            return stats;
        }

        Map<String, Byte> failureMap = new TreeMap<>();
        Map<String, Pair<DRRoleStats.State, Byte>> rowMap = new TreeMap<>();
        while (stats.advanceRow()) {
            final byte clusterId = (byte) stats.getLong(DRProducerCluster.CLUSTER_ID.name());
            final byte remoteClusterId = (byte) stats.getLong(DRProducerCluster.REMOTE_CLUSTER_ID.name());
            String key = clusterId + ":" + remoteClusterId;

            // Remember the first non-zero failure per connection.
            final byte lastFailure = (byte) stats.getLong(DRProducerCluster.LASTFAILURE.name());
            Byte failure = failureMap.get(key);
            if (failure == null) {
                failureMap.put(key, lastFailure);
            } else if (failure == NO_FAILURE && lastFailure != NO_FAILURE){
                failureMap.put(key, lastFailure);
            }

            final DRRoleStats.State state = DRRoleStats.State.valueOf(stats.getString(DRProducerCluster.STATE.name()));
            Pair<DRRoleStats.State, Byte> pair = rowMap.get(key);
            if (pair == null) {
                rowMap.put(key, Pair.of(state, failureMap.get(key)));
            } else {
                rowMap.put(key, Pair.of(
                    state.and(pair.getFirst()), failureMap.get(key)));
            }
        }

        stats.clearRowData();
        for (Map.Entry<String, Pair<DRRoleStats.State, Byte>> e : rowMap.entrySet()) {
            String[] ids = e.getKey().split(":", 2);
            stats.addRow(Byte.parseByte(ids[0]), Byte.parseByte(ids[1]), e.getValue().getFirst().toString(), e.getValue().getSecond());
        }
        return stats;
    }
}
