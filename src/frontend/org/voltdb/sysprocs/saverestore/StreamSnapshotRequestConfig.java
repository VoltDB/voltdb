/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class StreamSnapshotRequestConfig extends SnapshotRequestConfig {
    // src -> (dest1, dest2,...)
    public final Map<Long, Collection<Long>> streamPairs;
    // partitions with the ranges each partition has
    public final Map<Integer, SortedMap<Long, Long>> partitionsToAdd;

    /**
     * @param tables             See {@link #SnapshotRequestConfig(java.util.List)} for more
     * @param streamPairs        This cannot be null for stream snapshot
     * @param partitionsToAdd
     */
    public StreamSnapshotRequestConfig(List<Table> tables,
                                       Map<Long, Collection<Long>> streamPairs,
                                       Map<Integer, Map<Long, Long>> partitionsToAdd)
    {
        super(tables);

        this.streamPairs = ImmutableMap.copyOf(streamPairs);

        if (partitionsToAdd == null) {
            this.partitionsToAdd = null;
        } else {
            ImmutableMap.Builder<Integer, SortedMap<Long, Long>> builder = ImmutableMap.builder();
            for (Map.Entry<Integer, Map<Long, Long>> entry : partitionsToAdd.entrySet()) {
                builder.put(entry.getKey(), ImmutableSortedMap.copyOf(entry.getValue()));
            }
            this.partitionsToAdd = builder.build();
        }
    }

    public StreamSnapshotRequestConfig(JSONObject jsData,
                                       Database catalogDatabase,
                                       Collection<Long> localHSIds)
    {
        super(jsData, catalogDatabase);

        streamPairs = ImmutableMap.copyOf(parseStreamPairs(jsData, localHSIds));
        partitionsToAdd = parsePostSnapshotTasks(jsData);
    }

    private Map<Integer, SortedMap<Long, Long>> parsePostSnapshotTasks(JSONObject jsData)
    {
        ImmutableMap.Builder<Integer, SortedMap<Long, Long>> partitionBuilder =
            ImmutableMap.builder();

        if (jsData != null) {
            try {
                JSONObject cts = jsData.optJSONObject("postSnapshotTasks");
                if (cts != null) {
                    Iterator taskNames = cts.keys();
                    while (taskNames.hasNext()) {
                        String taskName = (String) taskNames.next();
                        if (taskName.equalsIgnoreCase("addPartitions")) {
                            // Get the set of new partitions to add
                            JSONObject newPartitionsObj = cts.getJSONObject(taskName);
                            Iterator partitionKey = newPartitionsObj.keys();
                            while (partitionKey.hasNext()) {
                                String partitionStr = (String) partitionKey.next();
                                int partition = Integer.parseInt(partitionStr);
                                JSONObject rangeObj = newPartitionsObj.getJSONObject(partitionStr);
                                Iterator rangeKey = rangeObj.keys();

                                ImmutableSortedMap.Builder<Long, Long> rangeBuilder =
                                    ImmutableSortedMap.naturalOrder();
                                while (rangeKey.hasNext()) {
                                    String rangeStartStr = (String) rangeKey.next();
                                    long rangeStart = Long.parseLong(rangeStartStr);
                                    long rangeEnd = rangeObj.getLong(rangeStartStr);
                                    rangeBuilder.put(rangeStart, rangeEnd);
                                }

                                partitionBuilder.put(partition, rangeBuilder.build());
                            }
                        }
                    }
                }
            } catch (JSONException e) {
                SNAP_LOG.warn("Failed to parse completion task information", e);
            }
        }

        return partitionBuilder.build();
    }

    private static Map<Long, Collection<Long>> parseStreamPairs(JSONObject jsData,
                                                                Collection<Long> localHSIds)
    {
        ArrayListMultimap<Long, Long> streamPairs = ArrayListMultimap.create();

        if (jsData != null) {
            try {
                JSONObject sp = jsData.getJSONObject("streamPairs");
                @SuppressWarnings("unchecked")
                Iterator<String> it = sp.keys();
                while (it.hasNext()) {
                    String key = it.next();
                    long sourceHSId = Long.valueOf(key);
                    // See whether this source HSID is a local site, if so, we need
                    // the destination HSID
                    if (localHSIds.contains(sourceHSId)) {
                        JSONArray destJSONArray = sp.getJSONArray(key);
                        for (int i = 0; i < destJSONArray.length(); i++) {
                            long destHSId = destJSONArray.getLong(i);
                            streamPairs.put(sourceHSId, destHSId);
                        }
                    }
                }
            } catch (JSONException e) {
                SNAP_LOG.warn("Failed to parse stream pair information", e);
            }
        }

        return streamPairs.asMap();
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException
    {
        super.toJSONString(stringer);

        stringer.key("streamPairs").object();
        for (Map.Entry<Long, Collection<Long>> entry : streamPairs.entrySet()) {
            stringer.key(Long.toString(entry.getKey())).array();
            for (long destHSId : entry.getValue()) {
                stringer.value(destHSId);
            }
            stringer.endArray();
        }
        stringer.endObject();

        stringer.key("postSnapshotTasks").object();
        if (partitionsToAdd != null && !partitionsToAdd.isEmpty()) {
            stringer.key("addPartitions").object();
            for (Map.Entry<Integer, SortedMap<Long, Long>> entry : partitionsToAdd.entrySet()) {
                int partition = entry.getKey();
                Map<Long, Long> ranges = entry.getValue();

                stringer.key(Integer.toString(partition)).object();
                for (Map.Entry<Long, Long> rangeEntry : ranges.entrySet()) {
                    stringer.key(rangeEntry.getKey().toString()).value(rangeEntry.getValue());
                }
                stringer.endObject();
            }
            stringer.endObject();
        }
        stringer.endObject();
    }
}
