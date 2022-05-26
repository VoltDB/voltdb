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
package org.voltdb.stats;

import com.google_voltpatches.common.collect.Iterators;
import org.voltdb.StatsSource;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import java.util.ArrayList;
import java.util.Iterator;

public class LimitsStats extends StatsSource {

    private final FileDescriptorsTracker fileDescriptorsTracker;
    private final ClientConnectionsTracker clientConnectionsTracker;

    private int acceptedConnectionsCountLastValue;
    private int droppedConnectionsCountLastValue;

    public enum Column implements StatsColumn {
        FILE_DESCRIPTORS_LIMIT(VoltType.INTEGER),
        FILE_DESCRIPTORS_OPEN(VoltType.INTEGER),
        CLIENT_CONNECTIONS_LIMIT(VoltType.INTEGER),
        CLIENT_CONNECTIONS_OPEN(VoltType.INTEGER),
        ACCEPTED_CONNECTIONS(VoltType.INTEGER),
        DROPPED_CONNECTIONS(VoltType.INTEGER);

        private static final Column[] values = Column.values();

        private final VoltType type;

        Column(VoltType type) {
            this.type = type;
        }

        public static Column[] getValues() {
            return values;
        }

        @Override
        public VoltType getType() {
            return type;
        }
    }

    public LimitsStats(
            FileDescriptorsTracker fileDescriptorsTracker,
            ClientConnectionsTracker clientConnectionsTracker
    ) {
        super(false);

        this.fileDescriptorsTracker = fileDescriptorsTracker;
        this.clientConnectionsTracker = clientConnectionsTracker;
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return Iterators.singletonIterator(interval);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, Column.getValues());
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        boolean intervalCollection = (boolean) rowKey;

        int acceptedConnectionsCount = clientConnectionsTracker.getAcceptedConnectionsCount();
        int droppedConnectionsCount = clientConnectionsTracker.getDroppedConnectionsCount();
        if (intervalCollection) {
            rowValues[offset + Column.ACCEPTED_CONNECTIONS.ordinal()] = acceptedConnectionsCount - acceptedConnectionsCountLastValue;
            rowValues[offset + Column.DROPPED_CONNECTIONS.ordinal()] = droppedConnectionsCount - droppedConnectionsCountLastValue;

            droppedConnectionsCountLastValue = droppedConnectionsCount;
            acceptedConnectionsCountLastValue = acceptedConnectionsCount;
        } else {
            rowValues[offset + Column.ACCEPTED_CONNECTIONS.ordinal()] = acceptedConnectionsCount;
            rowValues[offset + Column.DROPPED_CONNECTIONS.ordinal()] = droppedConnectionsCount;
        }

        rowValues[offset + Column.FILE_DESCRIPTORS_LIMIT.ordinal()] = fileDescriptorsTracker.getOpenFileDescriptorLimit();
        rowValues[offset + Column.FILE_DESCRIPTORS_OPEN.ordinal()] = fileDescriptorsTracker.getOpenFileDescriptorCount();
        rowValues[offset + Column.CLIENT_CONNECTIONS_LIMIT.ordinal()] = clientConnectionsTracker.getMaxNumberOfAllowedConnections();
        rowValues[offset + Column.CLIENT_CONNECTIONS_OPEN.ordinal()] = clientConnectionsTracker.getConnectionsCount();

        return offset + Column.getValues().length;
    }
}
