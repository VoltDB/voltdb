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

package org.voltdb.sysprocs.saverestore;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;

public class DeltaSnapshotRequestConfig extends SnapshotRequestConfig {

    public static class PartitionTimestamp {
        public final int clusterId;
        public final Map<Integer, Long> partitionTimestampMap;

        public PartitionTimestamp(int clusterId, Map<Integer, Long> timestampMap) {
            this.clusterId = clusterId;
            this.partitionTimestampMap = timestampMap;
        }
    }

    public Collection<PartitionTimestamp> m_lastSeenTimestamp;

    public DeltaSnapshotRequestConfig(List<Table> tables, Collection<PartitionTimestamp> timestampMap) {
        super(tables);
        m_lastSeenTimestamp = ImmutableList.copyOf(timestampMap);
    }

    public DeltaSnapshotRequestConfig(JSONObject jsData, Database catalogDatabase) {
        super(jsData, catalogDatabase);
        m_lastSeenTimestamp = parseTimestampMap(jsData);
    }

    private Collection<PartitionTimestamp> parseTimestampMap(JSONObject jsData) {
        if (jsData != null) {
            try {
                JSONObject clusterMap = jsData.getJSONObject("partitiontimestamp");
                Iterator<String> clusterKeys = clusterMap.keys();

                ImmutableList.Builder<PartitionTimestamp> partitionTimestampBuilder =
                        ImmutableList.builder();

                while (clusterKeys.hasNext()) {
                    String clusterStr = clusterKeys.next();
                    JSONObject partitionMapObj = clusterMap.getJSONObject(clusterStr);
                    int clusterId = Integer.parseInt(clusterStr);
                    Iterator<String> partitionKeys = partitionMapObj.keys();

                    ImmutableMap.Builder<Integer, Long> timestampMapBuilder =
                            ImmutableMap.builder();
                    while (partitionKeys.hasNext()) {
                        String timestampStr = partitionKeys.next();
                        int partitionId = Integer.parseInt(timestampStr);
                        long timestamp = partitionMapObj.getLong(timestampStr);
                        timestampMapBuilder.put(partitionId, timestamp);
                    }
                    partitionTimestampBuilder.add(new PartitionTimestamp(clusterId,
                            timestampMapBuilder.build()));
                    return partitionTimestampBuilder.build();
                }
            } catch (JSONException e) {
                SNAP_LOG.warn("Failed to parse partition timestamps", e);
            }
        }
        return null;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);


        stringer.key("partitiontimestamp").object();

        for (PartitionTimestamp pt : m_lastSeenTimestamp) {
            stringer.key(Integer.toString(pt.clusterId)).object();
            for (Map.Entry<Integer, Long> entry : pt.partitionTimestampMap.entrySet()) {
                stringer.key(Integer.toString(entry.getKey())).value(entry.getValue());
            }
            stringer.endObject();
        }

        stringer.endObject();
    }

}
