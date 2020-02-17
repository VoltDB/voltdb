/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class DRRoleStats extends StatsSource {
    public static final String CN_ROLE = "ROLE";
    public static final String CN_STATE = "STATE";
    public static final String CN_REMOTE_CLUSTER_ID = "REMOTE_CLUSTER_ID";

    public enum State {
        DISABLED, // Feature is completely disabled
        ACTIVE,   // Actively exchanging data with remote cluster
        PENDING,  // Waiting to establish connection with remote cluster
        STOPPED;  // Replication broken due to error

        /**
         * Almost like logically ANDing two states together. The precedence is
         * STOPPED->PENDING->ACTIVE->DISABLED. This happened to be the reverse
         * ordinal order.
         *
         * @param other The other state
         * @return The combined state
         */
        public State and(State other)
        {
            if (other == null) {
                return this;
            }

            if (other.ordinal() > this.ordinal()) {
                return other;
            } else {
                return this;
            }
        }
    }

    private final VoltDBInterface m_vdb;

    public DRRoleStats(VoltDBInterface vdb)
    {
        super(false);
        m_vdb = vdb;
    }

    @Override
    protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns)
    {
        columns.add(new VoltTable.ColumnInfo(CN_ROLE, VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo(CN_STATE, VoltType.STRING));
        columns.add(new VoltTable.ColumnInfo(CN_REMOTE_CLUSTER_ID, VoltType.INTEGER));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues)
    {
        @SuppressWarnings("unchecked") Map.Entry<Byte, State> state = (Map.Entry<Byte, State>) rowKey;
        final String role = getRole();

        rowValues[columnNameToIndex.get(CN_ROLE)] = role;
        rowValues[columnNameToIndex.get(CN_STATE)] = state.getValue().name();
        rowValues[columnNameToIndex.get(CN_REMOTE_CLUSTER_ID)] = state.getKey();
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval)
    {
        final ProducerDRGateway producer = m_vdb.getNodeDRGateway();
        final Map<Byte, DRProducerNodeStats> producerStats;
        if (producer != null) {
            producerStats = producer.getNodeDRStats();
        } else {
            producerStats = ImmutableMap.of((byte) -1, DRProducerNodeStats.DISABLED_NODE_STATS);
        }

        final ConsumerDRGateway consumer = m_vdb.getConsumerDRGateway();
        final Map<Byte, State> consumerStates;
        if (consumer != null) {
            consumerStates = consumer.getStates();
        } else {
            consumerStates = ImmutableMap.of((byte) -1, State.DISABLED);
        }

        final Map<Byte, State> states = mergeProducerConsumerStates(producerStats, consumerStates);

        final Iterator<Map.Entry<Byte, State>> iter = states.entrySet().iterator();
        return new Iterator<Object>() {
            @Override
            public boolean hasNext()
            {
                return iter.hasNext();
            }

            @Override
            public Object next()
            {
                return iter.next();
            }
        };
    }

    private String getRole()
    {
        return m_vdb.getCatalogContext().cluster.getDrrole().toUpperCase();
    }

    private static Map<Byte, State> mergeProducerConsumerStates(Map<Byte, DRProducerNodeStats> producerStats,
                                                                Map<Byte, State> consumerStates)
    {
        Map<Byte, State> states = new HashMap<>();
        for (byte clusterId : Sets.union(producerStats.keySet(), consumerStates.keySet())) {
            final DRProducerNodeStats producerNodeStats = producerStats.get(clusterId);
            final State consumerState = consumerStates.get(clusterId);
            State finalState = State.DISABLED;
            if (producerNodeStats != null) {
                finalState = finalState.and(producerNodeStats.state);
            }
            if (consumerState != null) {
                finalState = finalState.and(consumerState);
            }
            states.put(clusterId, finalState);
        }

        // Remove the -1 placeholder if there are real cluster states
        if (states.size() > 1) {
            states.remove((byte) -1);
        }

        return states;
    }

    /**
     * Aggregates DRROLE statistics reported by multiple nodes into a single
     * cluster-wide row. The role column should be the same across all
     * nodes. The state column may defer slightly and it uses the same logical
     * AND-ish operation to combine the states.
     *
     * This method modifies the VoltTable in place.
     * @param stats Statistics from all cluster nodes. This will be modified in
     *              place. Cannot be null.
     * @return The same VoltTable as in the parameter.
     * @throws IllegalArgumentException If the cluster nodes don't agree on the
     * DR role.
     */
    public static VoltTable aggregateStats(VoltTable stats) throws IllegalArgumentException
    {
        stats.resetRowPosition();
        if (stats.getRowCount() == 0) {
            return stats;
        }

        String role = null;
        Map<Byte, State> states = new TreeMap<>();
        while (stats.advanceRow()) {
            final byte clusterId = (byte) stats.getLong(CN_REMOTE_CLUSTER_ID);
            final String curRole = stats.getString(CN_ROLE);
            if (role == null) {
                role = curRole;
            } else if (!role.equals(curRole)) {
                throw new IllegalArgumentException("Inconsistent DR role across cluster nodes: " + stats.toFormattedString(false));
            }

            final State state = State.valueOf(stats.getString(CN_STATE));
            states.put(clusterId, state.and(states.get(clusterId)));
        }

        // Remove the -1 placeholder if there are real cluster states
        if (states.size() > 1) {
            states.remove((byte) -1);
        }

        assert role != null;
        stats.clearRowData();
        for (Map.Entry<Byte, State> e : states.entrySet()) {
            stats.addRow(role, e.getValue().name(), e.getKey());
        }
        return stats;
    }
}
