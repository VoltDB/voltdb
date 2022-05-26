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

package org.voltdb.sysprocs.saverestore;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.SnapshotTableInfo;
import org.voltdb.catalog.Database;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableSortedMap;

/**
 * Encapsulates all the information needed to initiate the snapshot that will build the elastic
 * index.
 */
public class IndexSnapshotRequestConfig extends SnapshotRequestConfig {
    public static class PartitionRanges {
        public final int partitionId;
        public final SortedMap<Integer, Integer> ranges;

        /**
         * @param partitionId    The partition that currently owns the ranges
         * @param ranges         The ranges to index on this partition
         */
        public PartitionRanges(int partitionId, Map<Integer, Integer> ranges)
        {
            this.partitionId = partitionId;
            this.ranges = ImmutableSortedMap.copyOf(ranges);
        }
    }

    public final Collection<PartitionRanges> partitionRanges;

    public IndexSnapshotRequestConfig(List<SnapshotTableInfo> tables, Collection<PartitionRanges> partitionRanges)
    {
        super(tables);
        this.partitionRanges = ImmutableList.copyOf(partitionRanges);
    }

    public IndexSnapshotRequestConfig(JSONObject jsData, Database catalogDatabase)
    {
        super(jsData, catalogDatabase);
        partitionRanges = parsePartitionRanges(jsData);
    }

    // parse ranges
    private Collection<PartitionRanges> parsePartitionRanges(JSONObject jsData)
    {
        if (jsData != null) {
            try {
                JSONObject partitionObj = jsData.getJSONObject("partitionRanges");
                Iterator<String> partitionKey = partitionObj.keys();

                ImmutableList.Builder<PartitionRanges> partitionRangesBuilder =
                    ImmutableList.builder();

                while (partitionKey.hasNext()) {
                    String pidStr = partitionKey.next();
                    JSONObject rangeObj = partitionObj.getJSONObject(pidStr);
                    Iterator<String> rangeKey = rangeObj.keys();

                    ImmutableSortedMap.Builder<Integer, Integer> rangeBuilder =
                        ImmutableSortedMap.naturalOrder();
                    while (rangeKey.hasNext()) {
                        String rangeStartStr = rangeKey.next();
                        int rangeStart = Integer.parseInt(rangeStartStr);
                        int rangeEnd = rangeObj.getInt(rangeStartStr);
                        rangeBuilder.put(rangeStart, rangeEnd);
                    }

                    partitionRangesBuilder.add(new PartitionRanges(Integer.parseInt(pidStr),
                                                                   rangeBuilder.build()));
                }

                return partitionRangesBuilder.build();
            } catch (JSONException e) {
                SNAP_LOG.warn("Failed to parse partition ranges", e);
            }
        }

        return null;
    }

    // serialize request
    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException
    {
        super.toJSONString(stringer);

        stringer.key("partitionRanges").object();

        for (PartitionRanges partitionRange : partitionRanges) {
            stringer.key(Integer.toString(partitionRange.partitionId)).object();

            for (Map.Entry<Integer, Integer> rangeEntry : partitionRange.ranges.entrySet()) {
                stringer.key(rangeEntry.getKey().toString()).value(rangeEntry.getValue());
            }

            stringer.endObject();
        }

        stringer.endObject();
    }
}
